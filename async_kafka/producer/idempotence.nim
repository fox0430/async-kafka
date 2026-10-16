## Producer module — idempotence: producer ID / epoch lifecycle, the
## per-partition sequence table, and batch encoding under `initLock`.

import std/[tables, options, importutils]

import ../[async_backend, errors, broker, cluster]
import ../protocol/[messages, primitives, compression]
import ../internal/[coordinator_rpc, metrics, retry, safe_logging]

import types

privateAccess(KafkaProducer)

const
  idempotenceInitMaxAttempts = 5
  idempotenceInitBackoffBaseMs = 100'i32

proc resetProducerId*(
    p: KafkaProducer
) {.async: (raises: [KafkaError, CancelledError]).} =
  await p.initLock.acquire()
  try:
    p.producerId = -1'i64
    p.producerEpoch = -1'i16
    p.sequenceNumbers.clear()
    p.idempotenceInitialized = false
    p.resetEpoch.inc
  finally:
    p.initLock.releaseSafely("producer.resetProducerId")

proc initProducerErrorCode(
    r: InitProducerIdResponse
): KafkaErrorCode {.inline, raises: [].} =
  r.errorCode

proc ensureIdempotenceInitialized*(
    p: KafkaProducer
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Lock order: `initLock` is never awaited while `batchLock` is held (see the
  ## producer's locking rules); reversing that order can deadlock against
  ## `resetProducerId` and txn state changes.
  if not p.enableIdempotence or p.idempotenceInitialized:
    return

  await p.initLock.acquire()

  try:
    if p.idempotenceInitialized:
      return
    var b = await p.cluster.anyBroker()
    let req = InitProducerIdRequest(
      transactionalId: none(string),
      transactionTimeoutMs: p.timeoutMs,
      producerId: -1,
      producerEpoch: -1,
    )
    template refreshIdempotenceBroker() =
      b = await p.cluster.anyBroker()

    let resp = runCoordinatorRpc(
      rpcCtx =
        RetryContext(isTransactional: false, isIdempotent: true, isCoordinatorRpc: true),
      maxAttempts = idempotenceInitMaxAttempts,
      backoffBaseMs = idempotenceInitBackoffBaseMs,
      rediscover = refreshIdempotenceBroker(),
      errorCodeOf = initProducerErrorCode,
      rpcCall = await b.initProducerId(req),
    )
    if resp.errorCode != keNone:
      raise newKafkaApiError(
        resp.errorCode, "InitProducerId failed: " & kafkaErrorName(resp.errorCode)
      )
    p.producerId = resp.producerId
    p.producerEpoch = resp.producerEpoch
    p.sequenceNumbers.clear()
    p.idempotenceInitialized = true
  finally:
    p.initLock.releaseSafely("producer.refreshIdempotenceBroker")

proc nextSequence*(p: KafkaProducer, tp: TopicPartition, recordCount: int32): int32 =
  ## Reserve `recordCount` consecutive sequence numbers for `tp`. Synchronous, so
  ## the read-modify-write is atomic on chronos's single thread; callers hold
  ## `initLock` (as `allocateInflight` does) or touch the table only inside a
  ## synchronous block (`applyEpochBump`), so `resetProducerId` cannot interleave.
  result = p.sequenceNumbers.getOrDefault(tp, 0'i32)
  p.sequenceNumbers[tp] =
    int32((int64(result) + int64(recordCount)) and int64(int32.high))

proc batchAttributes(codec: CompressionCodec, isTransactional: bool): int16 =
  ## v2 RecordBatch attributes: low 3 bits = compression codec, bit 4 =
  ## transactional flag; the remaining broker-driven bits are unused here.
  result = int16(codec)
  if isTransactional:
    result = result or 0x10'i16

proc encodeBatch(
    records: seq[Record],
    firstTimestamp: int64,
    codec: CompressionCodec,
    producerId: int64,
    producerEpoch: int16,
    baseSequence: int32,
    isTransactional: bool,
): seq[byte] =
  let batch = RecordBatch(
    baseOffset: 0,
    partitionLeaderEpoch: 0,
    attributes: batchAttributes(codec, isTransactional),
    firstTimestamp: firstTimestamp,
    producerId: producerId,
    producerEpoch: producerEpoch,
    baseSequence: baseSequence,
    records: records,
  )
  var w = initKafkaWriter()
  w.encode(batch)
  result = w.buf

proc allocateInflight*(
    p: KafkaProducer, records: seq[Record], tp: TopicPartition, firstTimestamp: int64
): Future[InflightBatch] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Idempotent path: allocate sequence numbers for `records`, encode, and enqueue
  ## on the per-TP in-flight list. The returned `InflightBatch` carries the encoded
  ## bytes plus the (producerId, epoch, baseSequence) snapshot so retries reuse them
  ## verbatim; all under `initLock`, so `resetProducerId` cannot tear the
  ## identity/sequence pair mid-read. A non-idempotent producer just encodes once.
  if not p.enableIdempotence:
    # Non-idempotent: encode once, no in-flight bookkeeping.
    let encoded = encodeBatch(
      records,
      firstTimestamp = firstTimestamp,
      codec = p.compression,
      producerId = p.producerId,
      producerEpoch = p.producerEpoch,
      baseSequence = -1'i32,
      isTransactional = p.transactionalId.isSome,
    )

    return InflightBatch(
      id: 0'i64,
      records: records,
      firstTimestamp: firstTimestamp,
      baseSequence: -1'i32,
      producerIdAtEncode: p.producerId,
      producerEpochAtEncode: p.producerEpoch,
      resetEpochAtEncode: 0'i64,
      encodedBytes: encoded,
    )
  # Fence against a racing `resetProducerId` that cleared PID/sequences: a bare
  # initLock.acquire() could encode with pid=-1 or seq=0, colliding with the
  # retry's re-sequenced front and silently dropping records.
  while true:
    await p.ensureIdempotenceInitialized()
    await p.initLock.acquire()
    if p.idempotenceInitialized:
      break
    p.initLock.releaseSafely("producer.allocateInflight")

  try:
    let baseSeq = p.nextSequence(tp, int32(records.len))
    let encoded = encodeBatch(
      records,
      firstTimestamp = firstTimestamp,
      codec = p.compression,
      producerId = p.producerId,
      producerEpoch = p.producerEpoch,
      baseSequence = baseSeq,
      isTransactional = p.transactionalId.isSome,
    )
    inc p.nextInflightId
    let batch = InflightBatch(
      id: p.nextInflightId,
      records: records,
      firstTimestamp: firstTimestamp,
      baseSequence: baseSeq,
      producerIdAtEncode: p.producerId,
      producerEpochAtEncode: p.producerEpoch,
      resetEpochAtEncode: p.resetEpoch,
      encodedBytes: encoded,
    )
    p.inflightBatches.mgetOrPut(tp, @[]).add(batch)
    return batch
  finally:
    p.initLock.releaseSafely("producer.allocateInflight")

proc dequeueInflight*(p: KafkaProducer, tp: TopicPartition, id: int64) {.raises: [].} =
  ## Remove the in-flight batch with `id` from `tp`'s queue on a successful or
  ## duplicate-sequence Produce response. id-based, not position-based, so an
  ## out-of-order completion under `maxInFlight > 1` removes *this* coroutine's
  ## batch; the id survives `reassignSequencesAfterReset`. No-op when the queue is
  ## empty, the id is absent, or `tp` is unknown.
  p.inflightBatches.withValue(tp, q):
    for i in 0 ..< q[].len:
      if q[][i].id == id:
        q[].delete(i)
        break
    if q[].len == 0:
      p.inflightBatches.del(tp)

proc reassignSequencesAfterReset*(
    p: KafkaProducer
) {.async: (raises: [CancelledError]).} =
  ## After a producer-id reset or epoch bump, rewrite every per-TP queue
  ## front-to-back so sequences restart at 0 in arrival order, re-encoding with the
  ## bumped (producerId, epoch, baseSequence) so a retry sends bytes the broker
  ## accepts; `sequenceNumbers` advances to each TP's in-flight tail.
  ##
  ## Locking: takes `initLock` so a concurrent `allocateInflight`/`nextSequence`
  ## cannot interleave mid-rewrite.
  await p.initLock.acquire()
  try:
    var totalResequenced = 0
    for tp, batches in p.inflightBatches.mpairs:
      var nextSeq: int32 = 0
      for b in batches.mitems:
        b.baseSequence = nextSeq
        b.producerIdAtEncode = p.producerId
        b.producerEpochAtEncode = p.producerEpoch
        b.resetEpochAtEncode = p.resetEpoch
        try:
          b.encodedBytes = encodeBatch(
            b.records,
            firstTimestamp = b.firstTimestamp,
            codec = p.compression,
            producerId = p.producerId,
            producerEpoch = p.producerEpoch,
            baseSequence = nextSeq,
            isTransactional = p.transactionalId.isSome,
          )
          # Count successful re-encodes only: one whose re-encode failed keeps its
          # stale bytes and is not counted.
          inc totalResequenced
        except KafkaError as e:
          # Unreachable in practice (the batch encoded once already): keep the old
          # encodedBytes and let the next retry surface the underlying error.
          warn "Re-encode after producer-id reset failed for ",
            tp.topic, "-", $tp.partition, ": ", e.msg
        let recordCount = int32(b.records.len)
        let newTail = int64(nextSeq) + int64(recordCount)
        nextSeq = int32(newTail and int64(int32.high))
      if batches.len > 0:
        p.sequenceNumbers[tp] = nextSeq
      else:
        p.sequenceNumbers.del(tp)

    if totalResequenced > 0:
      emitMetric(
        p.metricsHook, meProducerInflightResequenced, noTags, int64(totalResequenced)
      )
  finally:
    p.initLock.releaseSafely("producer.reassignSequences")
