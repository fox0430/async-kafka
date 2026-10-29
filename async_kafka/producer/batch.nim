## Producer module — send pipeline.
## Owns the per-partition batch + linger-timer state machine, the in-flight Produce
## semaphore, and the retry / DLQ path. Entry points: `send`, `flush`,
## `sendFireAndForget`, and the commit/abort orchestration wrapping EndTxn.

import std/[tables, options, sets, strutils, importutils]

import ../[async_backend, errors, broker, cluster]
import ../protocol/messages
import ../internal/[backoff, callback_guard, cancel, metrics, retry, safe_logging]

import types, partitioner, idempotence, transaction, dlq

privateAccess(KafkaProducer)

type
  AttemptCounter = ref object
    n: int32

  CompletionGate = ref object
    ## Per-partition completion-chain markers, installed by `gatedAllocateAndWrite` on the
    ## first attempt only: the caller awaits `prev` before surfacing its result and completes
    ## `mine` once settled, so completions surface in send order (nil when non-idempotent).
    prev: Future[void]
    mine: Future[void]

  FirstAttemptOutcome = ref object
    ## Side channel from `gatedAllocateAndWrite` for the window where allocation succeeded but the
    ## write raised before the caller's `allocated` flag was set: without it the retry allocates a
    ## fresh sequence range (permanent gap → OUT_OF_ORDER_SEQUENCE_NUMBER resets) and re-installs
    ## the gate, orphaning the old `mine`; adopting `allocatedBatch` re-sends frozen bytes instead.
    allocatedBatch: Option[InflightBatch]

  SendAttemptState = ref object
    ## Mutable per-call state carried across the retry loop and its handlers; `ref`
    ## because the async transform captures it in the loop's defer.
    inflight: InflightBatch
    allocated: bool
    dequeuedOnReturn: bool
    lastBroker: Broker
    outcome: FirstAttemptOutcome
    attempt: int32
    deadline: Option[Moment]

const recordBatchV2HeaderBytes* = 61
  ## Fixed RecordBatch v2 header wrapping every produce batch (13 fields, baseOffset ..
  ## recordCount). `estimateRecordSize` excludes it, so byte-cap accounting adds it once per
  ## batch — else framing could push a batch past the broker's message.max.bytes anyway.

func tpTags(
    tp: TopicPartition, extra: varargs[(string, string)]
): seq[(string, string)] =
  ## The `topic`/`partition` dimension pair shared by every per-partition producer
  ## metric, plus call-site `extra` tags. Extra tags use `add`, not `&`: Nim 2.2's
  ## `seq[T] & sink T` mishandles a literal/const tuple (`move` reads from nil), so
  ## `tpTags(tp) & ("reason", "x")` SIGSEGVs.
  result = @[("topic", tp.topic), ("partition", $tp.partition)]
  for pair in extra:
    result.add(pair)

# Forward declaration to break the mutual-recursion cycle sendToDeadLetter → send →
# flushBatch → handleSendFailure → sendToDeadLetter. Only the DLQ hop is declared
# ahead, so `send` keeps its single, documented declaration where the docs index it.
proc sendToDeadLetter(
  p: KafkaProducer, rec: FailedRecord
): Future[bool] {.async: (raises: [CancelledError]).}

proc estimateRecordSize*(rec: Record): int32 {.gcsafe, raises: [].} =
  ## Upper-bound the encoded size of one record in RecordBatch v2 framing; the send
  ## path compares it against `maxBatchBytes`, so it must NOT undercount. Worst-case
  ## varint widths
  ## (length 5, timestampDelta 10, offsetDelta 5, keyLen 5, valueLen 5, headersCount 5,
  ## plus 1 B attributes) total 36 B; each header adds two more varints (10 B).
  const recordFixedOverhead = 36
  const perHeaderVarintOverhead = 10
  var total: int64 = recordFixedOverhead
  if rec.key.isSome:
    total += int64(rec.key.get.len)
  if rec.value.isSome:
    total += int64(rec.value.get.len)
  for h in rec.headers:
    total += perHeaderVarintOverhead
    total += int64(h.key.len)
    if h.value.isSome:
      total += int64(h.value.get.len)
  if total > int64(int32.high):
    int32.high
  else:
    int32(total)

proc getInflightSemaphore(p: KafkaProducer, tp: TopicPartition): AsyncSemaphore =
  ## The per-partition in-flight semaphore, lazily created. Fully synchronous, so
  ## chronos's single-threaded scheduler makes the read-or-create atomic.
  p.inflightSemaphores.withValue(tp, sem):
    return sem[]
  let sem = newAsyncSemaphore(p.maxInFlightRequests)
  p.inflightSemaphores[tp] = sem
  result = sem

proc topicHasLivePartition(p: KafkaProducer, topic: string): bool {.raises: [].} =
  ## True if any partition of `topic` still holds live bookkeeping (buffered batch,
  ## linger timer, in-flight idempotent batch, tracked semaphore). Called after the
  ## caller's own `tp` entries were removed, so that `tp` no longer counts.
  for k in p.batches.keys:
    if k.topic == topic:
      return true
  for k in p.lingerTimers.keys:
    if k.topic == topic:
      return true
  for k in p.inflightBatches.keys:
    if k.topic == topic:
      return true
  for k in p.inflightSemaphores.keys:
    if k.topic == topic:
      return true
  false

proc maybeEvictIdlePartitionState(p: KafkaProducer, tp: TopicPartition) {.raises: [].} =
  ## Reclaim per-partition bookkeeping for a fully-idle partition (called at the tail of every
  ## send-completion path; no-op unless quiescent), so a producer fanning out over an unbounded
  ## partition set doesn't retain one entry per partition ever touched. Synchronous, hence atomic
  ## under chronos: the safety hinge is the semaphore at full permit count (`acquire` takes its
  ## permit with no intervening await, so only then is nobody using it — replacing it can't breach
  ## the in-flight cap, KIP-185's ≤5 bound). `sequenceNumbers` is deliberately NOT evicted (the
  ## broker tracks last sequence per (producerId, partition), so restarting a re-touched partition
  ## at 0 is rejected as OUT_OF_ORDER_SEQUENCE_NUMBER).
  if tp in p.batches or tp in p.lingerTimers or tp in p.inflightBatches:
    return

  var semIdle = false
  p.inflightSemaphores.withValue(tp, sem):
    semIdle = sem[].availableSlots == int(p.maxInFlightRequests)
  do:
    # No semaphore: nothing was ever sent through this partition (or it was evicted).
    return
  if not semIdle:
    return

  p.inflightSemaphores.del(tp)
  # A finished chain tail is inert — the next send skips it as for an absent tail — so dropping
  # it is a no-op for ordering; an unfinished tail still has a batch chained behind it.
  var dropWrite = false
  p.writeChainTail.withValue(tp, t):
    dropWrite = t[].finished
  if dropWrite:
    p.writeChainTail.del(tp)
  var dropCompletion = false
  p.completionChainTail.withValue(tp, t):
    dropCompletion = t[].finished
  if dropCompletion:
    p.completionChainTail.del(tp)
  p.pendingBatchBytes.del(tp)

  # This partition is now fully idle. Reclaim the per-topic round-robin counter only when no live
  # partition is left AND the topic no longer resolves (deleted upstream); otherwise KEEP it — a
  # sendable partition needs its RR position preserved across sends, and an idle-but-resolvable
  # topic is expected to be re-touched. `roundRobinIndex` re-seeds a missing entry (a recreated
  # topic loses only RR history); without this, `partitionCounters` grows a dead entry per topic.
  if not p.topicHasLivePartition(tp.topic) and
      p.cluster.getTopicPartitionsCopy(tp.topic).isNone:
    p.partitionCounters.del(tp.topic)

proc buildProduceReq(
    p: KafkaProducer, tp: TopicPartition, batchBytes: seq[byte]
): ProduceRequest {.raises: [].} =
  ProduceRequest(
    transactionalId: p.transactionalId,
    acks: p.acks,
    timeoutMs: p.timeoutMs,
    topics: @[
      ProduceTopicReq(
        topic: tp.topic,
        partitions:
          @[ProducePartitionReq(partition: tp.partition, recordSet: batchBytes)],
      )
    ],
  )

proc produceResultOf(
    tp: TopicPartition, respOpt: Option[ProduceResponse]
): ProduceResult {.raises: [KafkaError].} =
  ## Extract offset / error from a Produce response. `none` is the acks=0
  ## fire-and-forget case (no response): success with unknown offset.
  if respOpt.isNone:
    return ProduceResult(topic: tp.topic, partition: tp.partition, offset: -1)
  let resp = respOpt.get
  if resp.topics.len > 0 and resp.topics[0].partitions.len > 0:
    let pr = resp.topics[0].partitions[0]
    # buildProduceReq sends exactly one topic/partition, so verify the broker
    # echoed it back rather than blindly attributing slot 0 to this partition.
    if pr.partition != tp.partition:
      raise newException(
        KafkaError,
        "produce response partition mismatch: expected " & $tp.partition & " got " &
          $pr.partition,
      )
    if pr.errorCode != keNone:
      raise
        newKafkaApiError(pr.errorCode, "Produce error: " & kafkaErrorName(pr.errorCode))
    return
      ProduceResult(topic: tp.topic, partition: tp.partition, offset: pr.baseOffset)
  raise newException(KafkaError, "empty produce response")

proc errorCodeLabel(
    e: ref KafkaError, isApi: bool, apiCode: KafkaErrorCode
): string {.raises: [].} =
  ## Canonical `error_code` metric tag for a send failure: the Kafka error name for
  ## broker-reported failures, the `kafkaErrorTag` spelling for client-side ones. Both
  ## are SCREAMING_SNAKE, so one dashboard `GROUP BY error_code` covers every path.
  if isApi:
    kafkaErrorName(apiCode)
  else:
    kafkaErrorTag(e)

proc reasonLabel(
    e: ref KafkaError, isApi: bool, apiCode: KafkaErrorCode
): string {.raises: [].} =
  ## Lowercase `reason` metric tag, the descriptive counterpart of `errorCodeLabel`
  ## for retry/backoff events (`connection_error`, `timeout`, `not_leader_or_follower`);
  ## `error_code` is the dimension to group by, `reason` the per-attempt context.
  toLowerAscii(errorCodeLabel(e, isApi, apiCode))

proc refreshInflightFromQueue(
    p: KafkaProducer, tp: TopicPartition, inflight: var InflightBatch
) {.raises: [].} =
  ## Re-sync the local `inflight` (captured at allocation, so pre-reset) from the per-TP queue
  ## after `reassignSequencesAfterReset` rewrote it. Looked up by the monotonic `id`: with
  ## `maxInFlight > 1` the queue front may not be ours, and it is missing once our defer dequeued.
  if not p.enableIdempotence:
    return

  p.inflightBatches.withValue(tp, q):
    for b in q[]:
      if b.id == inflight.id:
        inflight = b
        return

proc awaitChainMarker(gate: CompletionGate) {.async: (raises: []).} =
  ## Hold this batch's result until every earlier-sequence in-flight batch on the partition
  ## has surfaced its own, else an earlier batch stuck in retry could be acked after a later
  ## success. The predecessor's marker is always completed by its own finally (no deadlock); a
  ## marker that fails is a defect we absorb rather than let it strand the batch behind it.
  if gate.isNil or gate.prev.isNil or gate.prev.finished:
    return
  try:
    await gate.prev
  except CancelledError:
    absorbDuringTeardown(
      "producer::send", "torn down while waiting on the write-chain marker"
    )
  except CatchableError:
    absorbDuringTeardown("producer::send", "write-chain marker failed")

proc gatedAllocateAndWrite(
    p: KafkaProducer,
    tp: TopicPartition,
    records: seq[Record],
    firstTimestamp: int64,
    existing: Option[InflightBatch],
    gate: CompletionGate = nil,
    outcome: FirstAttemptOutcome = nil,
): Future[tuple[inflight: InflightBatch, broker: Broker, handle: ProduceInFlight]] {.
    async: (raises: [KafkaError, CancelledError])
.} =
  ## KIP-185 per-partition write-order gate: on the first attempt, baseSequence is allocated and
  ## the frame written under a per-partition serialization chain (seq=N precedes seq=N+1); the
  ## chain is released right after the write and the caller awaits the response OUTSIDE it, so up
  ## to `maxInFlightRequests` responses pipeline. Retries (`existing.isSome`) do NOT re-enter the
  ## chain (baseSequence is frozen and the slot spent): re-appending would put seq=N behind a
  ## newer first-attempt batch (seq>N), so the retry re-writes its frozen bytes instead — safe
  ## because the broker's idempotent sequence check tolerates retry reordering — and the
  ## completion chain still orders results.
  if not p.enableIdempotence:
    # No sequence numbers → no ordering constraint; skip the chain for full concurrency.
    let inflight =
      if existing.isSome:
        existing.get
      else:
        await p.allocateInflight(records, tp, firstTimestamp)
    let b = await p.cluster.brokerForPartition(tp)
    let h = await b.produceWrite(buildProduceReq(p, tp, inflight.encodedBytes))
    return (inflight, b, h)

  if existing.isSome:
    # Retry: re-write frozen bytes without touching the write/completion chains.
    let inflight = existing.get
    let b = await p.cluster.brokerForPartition(tp)
    let h = await b.produceWrite(buildProduceReq(p, tp, inflight.encodedBytes))
    return (inflight, b, h)

  # First attempt: capture the predecessor and install our write-chain marker (and the
  # completion-chain marker) in one await-free critical section, so chain order == allocation
  # order == baseSequence order.
  #
  # The gate is installed AT MOST ONCE per doSendWithRetry call (`gate.mine` nil-check):
  # re-installing on a retry would orphan the previous `mine` — never completed — wedging every
  # later send awaiting it. Keeping the original (prev, mine) preserves chain integrity; the
  # residual cost is a narrow ordering skew if another send slips in between attempt and retry.
  # The write-chain marker is per-attempt, so a fresh one is safe.
  let prev = p.writeChainTail.getOrDefault(tp)
  let myDone = Future[void].init("kafka.writeChain")
  p.writeChainTail[tp] = myDone
  if gate != nil and gate.mine.isNil:
    gate.prev = p.completionChainTail.getOrDefault(tp)
    gate.mine = Future[void].init("kafka.completionChain")
    p.completionChainTail[tp] = gate.mine

  var inflight: InflightBatch
  var b: Broker
  var h: ProduceInFlight
  try:
    if not prev.isNil and not prev.finished:
      # `prev`'s await widens to CatchableError, but markers are only ever completed:
      # surface a cancel, ignore the rest.
      try:
        await prev
      except CancelledError as e:
        raise e
      except CatchableError:
        absorbDuringTeardown("producer::send", "write-chain marker failed")
    inflight = await p.allocateInflight(records, tp, firstTimestamp)
    # Surface the allocation IMMEDIATELY, before the write can fail: the caller sets `allocated`
    # only after this proc returns, so a failure here would otherwise orphan the enqueued batch
    # (see FirstAttemptOutcome). The retry then re-sends the FROZEN bytes — correct whether the
    # frame never landed (the broker sees the expected sequence) or did land (dedup as
    # DUPLICATE_SEQUENCE_NUMBER); rolling the counter back would let another batch reuse the range.
    if outcome != nil:
      outcome.allocatedBatch = some(inflight)
    b = await p.cluster.brokerForPartition(tp)
    h = await b.produceWrite(buildProduceReq(p, tp, inflight.encodedBytes))
  finally:
    # ALWAYS release the next writer (even on failure/cancel); `myDone` never fails.
    if not myDone.finished:
      myDone.complete()

  return (inflight, b, h)

proc awaitSendBackoff(
    p: KafkaProducer,
    tp: TopicPartition,
    broker: Broker,
    attempt: int32,
    reason: string,
    includeThrottle: bool,
) {.async: (raises: [CancelledError]).} =
  ## Sleep before re-sending a batch: jittered backoff overlapped with the leader's outstanding
  ## quota throttle (`retryDelay` = max(backoff, throttleMs) when `includeThrottle`). Compliance
  ## is owned by writeTypedRequest.awaitThrottle; the fold just makes a retry pay max, not the
  ## sum. `broker` is the already-resolved leader (nil when unresolved), so the read costs no RPC.
  var throttleMs = 0'i32
  if includeThrottle and not broker.isNil:
    throttleMs = broker.throttleRemainingMs()
  let sleepMs = retryDelay(p.retryBackoffMs, int(attempt), throttleMs)
  emitMetric(
    p.metricsHook,
    meProducerSendRetryBackoffMs,
    tpTags(tp, ("reason", reason)),
    int64(sleepMs),
  )
  if throttleMs > 0:
    emitMetric(
      p.metricsHook,
      meProducerSendRetryThrottleMs,
      tpTags(tp, ("reason", reason)),
      int64(throttleMs),
    )
  await sleepAsync(milliseconds(sleepMs))

proc deliveryExpired(state: SendAttemptState): bool {.raises: [].} =
  state.deadline.isSome and Moment.now() >= state.deadline.get

proc emitSendRetryCoded(
    p: KafkaProducer,
    tp: TopicPartition,
    attempt: int32,
    errorCode: string,
    reason: string,
) {.raises: [].} =
  ## One tag shape for every `producer.send.retry`: `error_code` (SCREAMING_SNAKE, what
  ## dashboards GROUP BY) + `reason` (lowercase, per-attempt context).
  emitMetric(
    p.metricsHook,
    meProducerSendRetry,
    tpTags(tp, ("error_code", errorCode), ("reason", reason)),
    int64(attempt),
  )

proc emitSendError(
    p: KafkaProducer,
    tp: TopicPartition,
    attemptCount: int64,
    errorTag: string,
    errorClass: string,
) {.raises: [].} =
  emitMetric(
    p.metricsHook,
    meProducerSendError,
    tpTags(tp, ("error_code", errorTag), ("error_class", errorClass)),
    attemptCount,
  )

proc raiseIfBudgetExhausted(
    p: KafkaProducer,
    tp: TopicPartition,
    state: SendAttemptState,
    e: ref KafkaError,
    facts: ErrorFacts,
    counter: AttemptCounter,
) {.raises: [KafkaError].} =
  ## Bumps `state.attempt` and, if the retry budget is now spent, emits the
  ## terminal metric and raises the original error. Otherwise returns normally.
  state.attempt.inc
  if retryBudgetExhausted(state.attempt, p.maxRetries, deliveryExpired(state)):
    emitSendError(
      p,
      tp,
      int64(state.attempt),
      errorCodeLabel(e, facts.isApi, facts.apiCode),
      $errorClassToCstring(facts.cls),
    )
    if counter != nil:
      counter.n = state.attempt
    raise e

proc runObservedSiblingReset(
    p: KafkaProducer,
    tp: TopicPartition,
    state: SendAttemptState,
    e: ref KafkaError,
    facts: ErrorFacts,
    counter: AttemptCounter,
) {.async: (raises: [KafkaError, CancelledError]).} =
  # A sibling coroutine has already driven the PID reset and republished our frozen
  # bytes under the new (producerId, epoch); just back off, the loop top refreshes.
  raiseIfBudgetExhausted(p, tp, state, e, facts, counter)
  emitSendRetryCoded(
    p,
    tp,
    state.attempt,
    errorCodeLabel(e, facts.isApi, facts.apiCode),
    "producer_id_reset_observed",
  )
  await awaitSendBackoff(
    p, tp, nil, state.attempt, "producer_id_reset_observed", includeThrottle = false
  )

proc runProducerIdResetDrive(
    p: KafkaProducer,
    tp: TopicPartition,
    state: SendAttemptState,
    e: ref KafkaError,
    facts: ErrorFacts,
    counter: AttemptCounter,
) {.async: (raises: [KafkaError, CancelledError]).} =
  # Drive resetProducerId + reassignSequencesAfterReset ourselves. Best effort: the
  # ORIGINAL error must reach the caller if re-init fails (a sibling's observed branch
  # then finds the queue stale and tries again).
  raiseIfBudgetExhausted(p, tp, state, e, facts, counter)
  emitSendRetryCoded(
    p,
    tp,
    state.attempt,
    errorCodeLabel(e, facts.isApi, facts.apiCode),
    "producer_id_reset",
  )
  await p.resetProducerId()
  try:
    await p.ensureIdempotenceInitialized()
    await p.reassignSequencesAfterReset()
  except KafkaError as reinitErr:
    warn "Producer-id re-init during retry failed; surfacing original error: ",
      reinitErr.msg
    if counter != nil:
      counter.n = state.attempt
    raise e
  p.refreshInflightFromQueue(tp, state.inflight)
  await awaitSendBackoff(
    p, tp, nil, state.attempt, "producer_id_reset", includeThrottle = false
  )

proc runTransientRetry(
    p: KafkaProducer,
    tp: TopicPartition,
    state: SendAttemptState,
    e: ref KafkaError,
    facts: ErrorFacts,
) {.async: (raises: [CancelledError]).} =
  state.attempt.inc
  let reason = reasonLabel(e, facts.isApi, facts.apiCode)
  emitSendRetryCoded(
    p, tp, state.attempt, errorCodeLabel(e, facts.isApi, facts.apiCode), reason
  )
  await awaitSendBackoff(
    p, tp, state.lastBroker, state.attempt, reason, includeThrottle = true
  )
  try:
    if facts.isApi and isStaleLeaderCode(facts.apiCode):
      discard await p.cluster.invalidateLeader(tp, facts.apiCode)
    else:
      await p.cluster.refreshMetadata(some(@[tp.topic]))
  except KafkaError as refreshErr:
    warn "Metadata refresh during retry failed: ", refreshErr.msg
  if p.partitionStrategy == psSticky:
    p.stickyPartitions.del(tp.topic)

proc finalizeTerminalFailure(
    p: KafkaProducer,
    tp: TopicPartition,
    state: SendAttemptState,
    e: ref KafkaError,
    facts: ErrorFacts,
    counter: AttemptCounter,
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Non-retriable or retry-budget-exhausted: reset producer state so subsequent
  ## sends don't hit sequence gaps, then raise the original error.
  if p.transactionalId.isSome:
    # resetProducerId is skipped in the txn path: the broker bumps producerId via the
    # EndTxn epoch increment, so it would clobber the in-flight txn's identity.
    if facts.isApi:
      p.transitionToError(facts.apiCode)
    else:
      if not p.tryTransition(tsError):
        warnTransitionFailed(p, tsError, "finalizeTerminalFailure")
  elif p.enableIdempotence:
    # Dequeue THIS batch before reassign so the defer doesn't dequeue it AFTER
    # reassignSequencesAfterReset, leaving a sequence gap where it sat.
    if state.allocated and not state.dequeuedOnReturn:
      p.dequeueInflight(tp, state.inflight.id)
      state.dequeuedOnReturn = true
    try:
      await p.resetProducerId()
      await p.ensureIdempotenceInitialized()
      await p.reassignSequencesAfterReset()
    except KafkaError as reinitErr:
      warn "Producer-id re-init after terminal send failure failed: ", reinitErr.msg
  if p.partitionStrategy == psSticky:
    p.stickyPartitions.del(tp.topic)
  let errorClassLabel =
    if facts.isApi:
      if p.transactionalId.isSome:
        $errorClassToCstring(errorClassForContext(facts.apiCode, true))
      else:
        $errorClassToCstring(facts.cls)
    else:
      $errorClassToCstring(ecUnknown)
  emitSendError(
    p,
    tp,
    int64(state.attempt + 1),
    errorCodeLabel(e, facts.isApi, facts.apiCode),
    errorClassLabel,
  )
  if counter != nil:
    counter.n = state.attempt + 1
  raise e

proc doSendWithRetry(
    p: KafkaProducer,
    tp: TopicPartition,
    records: seq[Record],
    firstTimestamp: int64,
    gate: CompletionGate = nil,
    counter: AttemptCounter = nil,
): Future[ProduceResult] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Encode a batch and send it, retrying retriable/idempotent errors.
  ##
  ## KIP-185: baseSequence is allocated once at first encode and frozen in the per-TP in-flight
  ## queue (retries reuse the same wire bytes); producer-id resets re-sequence every queue
  ## front-to-back via `reassignSequencesAfterReset`, so concurrent retries can't race for seq=0.
  ## Allocation happens INSIDE the write gate (allocation order == wire-write order).
  #
  # delivery.timeout.ms bounds the TOTAL retry time of this send; idempotent/transactional
  # producers have maxRetries = int32.high and could otherwise spin ~2.1B times. `<= 0` disables.
  var state = SendAttemptState(
    outcome:
      if p.enableIdempotence:
        FirstAttemptOutcome()
      else:
        nil,
    deadline:
      if p.deliveryTimeoutMs > 0:
        some(Moment.now() + milliseconds(p.deliveryTimeoutMs.int64))
      else:
        none(Moment),
  )

  template adoptFirstAttempt() =
    ## After gatedAllocateAndWrite allocated/enqueued but the write raised, adopt
    ## the batch so retries re-send frozen bytes and the defer dequeues it.
    if not state.allocated and state.outcome != nil and
        state.outcome.allocatedBatch.isSome:
      state.inflight = state.outcome.allocatedBatch.get
      state.allocated = true

  defer:
    # A CancelledError between allocation and write bypasses the except branch,
    # so an orphaned queue entry is only reachable here.
    adoptFirstAttempt()
    if p.enableIdempotence and state.allocated and not state.dequeuedOnReturn:
      p.dequeueInflight(tp, state.inflight.id)

  let sendCtx = RetryContext(
    isTransactional: p.transactionalId.isSome,
    isIdempotent: p.enableIdempotence,
    isCoordinatorRpc: false,
  )

  while true:
    state.lastBroker = nil
    # A sibling (maxInFlight > 1) may have run resetProducerId +
    # reassignSequencesAfterReset while we were parked, rewriting this batch under a new
    # (producerId, epoch); without this refresh a transient retry re-sends pre-reset
    # bytes the broker rejects as OUT_OF_ORDER_SEQUENCE_NUMBER / INVALID_PRODUCER_EPOCH.
    if state.allocated:
      p.refreshInflightFromQueue(tp, state.inflight)

    try:
      let existing =
        if state.allocated:
          some(state.inflight)
        else:
          none(InflightBatch)
      let written = await p.gatedAllocateAndWrite(
        tp, records, firstTimestamp, existing, gate, state.outcome
      )
      state.inflight = written.inflight
      state.lastBroker = written.broker
      state.allocated = true
      let respOpt = await written.broker.produceAwait(written.handle)
      result = produceResultOf(tp, respOpt)
      emitMetric(p.metricsHook, meProducerSendOk, tpTags(tp), int64(records.len))
      if p.enableIdempotence:
        p.dequeueInflight(tp, state.inflight.id)
        state.dequeuedOnReturn = true
      break
    except KafkaError as e:
      # Adopt BEFORE classifying so a transient retry re-sends frozen bytes via
      # `existing.isSome` instead of re-entering the (corrupting) first-attempt path.
      adoptFirstAttempt()
      let facts = inspectError(e, sendCtx)
      let action = classify(facts, sendCtx)

      if action == raDuplicate:
        # Broker already has the record; treat as success with unknown offset.
        # `.deduped` (not `.ok`) so success dashboards stay clean.
        result = ProduceResult(topic: tp.topic, partition: tp.partition, offset: -1)
        emitMetric(p.metricsHook, meProducerSendDeduped, tpTags(tp), int64(records.len))
        p.dequeueInflight(tp, state.inflight.id)
        state.dequeuedOnReturn = true
        break

      let pastDeadline = deliveryExpired(state)
      if pastDeadline and (
        facts.cls == ecRetriable or facts.cls == ecNeedsProducerIdReset or
        facts.isTransport
      ):
        emitMetric(
          p.metricsHook,
          meProducerSendDeliveryTimeout,
          tpTags(tp, ("error_code", errorCodeLabel(e, facts.isApi, facts.apiCode))),
          int64(state.attempt),
        )

      # Transactional producers can't reset producer-id mid-transaction (the
      # in-flight txn carries the current (producerId, epoch)); they fall through
      # to finalize which transitions txn state to abort.
      if action == raResetProducerId and not pastDeadline:
        # A sibling that observed the same code and already drove the reset republished
        # our bytes; skip a second reset. Fall through to drive ourselves if the queue
        # still shows pre-reset bytes — otherwise we'd zero-backoff hot-loop.
        var observed = false
        if siblingResetLikely(state.inflight.resetEpochAtEncode, p.resetEpoch):
          p.refreshInflightFromQueue(tp, state.inflight)
          observed = state.inflight.resetEpochAtEncode == p.resetEpoch
        if observed:
          await runObservedSiblingReset(p, tp, state, e, facts, counter)
        else:
          await runProducerIdResetDrive(p, tp, state, e, facts, counter)
        continue

      if action == raRetrySame and
          not retryBudgetExhausted(state.attempt + 1, p.maxRetries, pastDeadline):
        await runTransientRetry(p, tp, state, e, facts)
        continue

      await finalizeTerminalFailure(p, tp, state, e, facts, counter)

proc invokeErrorHandler(
    p: KafkaProducer, failedRec: FailedRecord
): ErrorAction {.raises: [].} =
  ## Run the user `errorHandler` under a CatchableError guard: `ErrorHandler` is
  ## `raises: []`, but a buggy implementation can violate that, and a leaked exception
  ## must not corrupt producer state. Falls back to `eaFail`; the metric counts every
  ## failure, the log is rate-limited.
  let raised = p.errorHandlerFailures.guardCallback(
    "errorHandler", "falling back to eaFail"
  ):
    result = p.errorHandler(failedRec)
  if raised != nil:
    emitMetric(
      p.metricsHook,
      meProducerErrorHandlerException,
      [
        ("topic", failedRec.topic),
        ("partition", $failedRec.partition),
        ("exception", $raised.name),
      ],
      1,
    )
    result = eaFail

proc handleSendFailure(
    p: KafkaProducer, failedRec: FailedRecord
): Future[Option[ProduceResult]] {.async: (raises: [CancelledError]).} =
  # A transactional producer must never divert a record out of the transaction via DLQ/drop:
  # resolving the future as pstDropped/pstDeadLettered while the record never landed would let
  # commitTransaction proceed with missing records (silent EOS data loss). Force eaFail so the
  # original error surfaces; doSendWithRetry already moved txnState to tsError, refusing the commit.
  if p.transactionalId.isSome or p.errorHandler == nil:
    return none(ProduceResult)

  let action = p.invokeErrorHandler(failedRec)
  case action
  of eaFail:
    return none(ProduceResult)
  of eaDrop:
    let isApi = failedRec.error of KafkaApiError
    let apiCode =
      if isApi:
        (ref KafkaApiError)(failedRec.error).errorCode
      else:
        keNone
    emitMetric(
      p.metricsHook,
      meProducerDropped,
      [
        ("topic", failedRec.topic),
        ("partition", $failedRec.partition),
        ("reason", reasonLabel(failedRec.error, isApi, apiCode)),
      ],
      1,
    )

    return some(
      ProduceResult(
        topic: failedRec.topic,
        partition: failedRec.partition,
        offset: -1,
        status: pstDropped,
      )
    )
  of eaDeadLetter:
    if p.dlqProducer == nil or p.dlqTopic.len == 0:
      warn "error handler returned eaDeadLetter but dead letter queue is not configured for " &
        failedRec.topic & "[" & $failedRec.partition & "]"
      emitMetric(
        p.metricsHook,
        meProducerDlqError,
        [
          ("topic", failedRec.topic),
          ("partition", $failedRec.partition),
          ("status", "unconfigured"),
        ],
        1,
      )

      return some(
        ProduceResult(
          topic: failedRec.topic,
          partition: failedRec.partition,
          offset: -1,
          status: pstDeadLetterFailed,
        )
      )

    let delivered = await p.sendToDeadLetter(failedRec)
    let status = if delivered: pstDeadLettered else: pstDeadLetterFailed

    return some(
      ProduceResult(
        topic: failedRec.topic,
        partition: failedRec.partition,
        offset: -1,
        status: status,
      )
    )

proc failBatchFutures*(
    p: KafkaProducer, tp: TopicPartition, err: ref CatchableError
) {.gcsafe, raises: [].} =
  ## Fail all pending futures for the partition and remove the batch. Safe with or
  ## without `batchLock` held: no await here, and chronos is single-threaded, so
  ## `p.batches` cannot be concurrently mutated.
  p.batches.withValue(tp, batch):
    for pm in batch[]:
      if not pm.future.finished:
        pm.future.fail(err)
      p.releasePendingRecordPermit()
  p.batches.del(tp)
  p.pendingBatchBytes.del(tp)

proc removeInflightFlush(p: KafkaProducer, marker: Future[void]) {.raises: [].} =
  ## Remove a flushBatch in-flight marker (identity-based, O(n)); idempotent when absent.
  let idx = p.inflightFlushFutures.find(marker)
  if idx >= 0:
    p.inflightFlushFutures.del(idx)

proc awaitInflightFlushes(p: KafkaProducer) {.async: (raises: [CancelledError]).} =
  ## Wait for every extracted-but-not-yet-acked flushBatch to settle, so commit/abort never
  ## race a size-/linger-triggered produce invisible to flush()'s `batches.keys` snapshot.
  ## Loops because a flush starting mid-await adds a fresh marker; markers are only completed.
  while p.inflightFlushFutures.len > 0:
    let pending = p.inflightFlushFutures # snapshot copy
    await allFutures(pending)

proc flushBatch(p: KafkaProducer, tp: TopicPartition) {.async: (raises: []).} =
  # Fail the batch at once in an error state; tsCommitting/tsAborting are valid for flush.
  if p.transactionalId.isSome and p.txnState in {tsError, tsFatal}:
    let err =
      newException(KafkaError, "cannot flush: transaction state is " & $p.txnState)
    p.failBatchFutures(tp, err)
    return

  if p.transactionalId.isSome and p.txnPendingPartitions.len > 0:
    # Register pending txn partitions before sending; hold `txnLock` across the broker
    # round-trip so commit/abort can't tear down `txnPendingPartitions` between consume
    # and the AddPartitionsToTxn reply.
    try:
      await p.txnLock.acquire()
    except CancelledError:
      let cancelErr = newException(
        KafkaError, "cancelled acquiring txnLock for partition registration"
      )
      p.poisonTxnOnFailedBatch(cancelErr)
      p.failBatchFutures(tp, cancelErr)
      return
    try:
      try:
        await p.registerPendingTxnPartitions()
      except KafkaError as e:
        p.poisonTxnOnFailedBatch(e)
        p.failBatchFutures(tp, e)
        return
      except CancelledError:
        let cancelErr =
          newException(KafkaError, "cancelled during txn partition registration")
        p.poisonTxnOnFailedBatch(cancelErr)
        p.failBatchFutures(tp, cancelErr)
        return
    finally:
      p.txnLock.releaseSafely("producer.flushBatch.txn")

  # In-flight flush barrier for ALL producers, registered HERE — after the txn registration
  # section but BEFORE acquiring batchLock — and held via the proc-scope defer until this flush
  # settles. Placement is load-bearing both ways:
  #   * After the txn section: a flushBatch parked on txnLock must NOT hold a marker, or
  #     commit/abort inside awaitInflightFlushes would wait on a marker awaiting that txnLock.
  #   * Before batchLock.acquire: it becomes the next holder on a linger-triggered handoff and
  #     extracts while flush()'s duplicate finds nothing; registering after the extract would
  #     land the marker past flush()'s preInflight snapshot (close() tears down mid-produce).
  # The defer removes it on every exit path; transactionally it is the EOS finalize barrier.
  let flushMarker = newFuture[void]("inflightFlush")
  p.inflightFlushFutures.add(flushMarker)
  defer:
    if not flushMarker.finished:
      flushMarker.complete()
    p.removeInflightFlush(flushMarker)

  # Extract batch under lock, then send outside lock: `batchLock` is innermost — only
  # synchronous ops inside it (no await but the acquire) and no other producer lock taken —
  # keeping lingerTimers/batches/stickyPartitions atomic against `send`.
  var pending: seq[PendingMessage]
  try:
    await p.batchLock.acquire()
  except CancelledError:
    let cancelErr = newException(KafkaError, "cancelled")
    p.poisonTxnOnFailedBatch(cancelErr)
    p.failBatchFutures(tp, cancelErr)
    return
  try:
    p.batches.withValue(tp, batch):
      pending = move batch[]
    p.batches.del(tp)
    p.pendingBatchBytes.del(tp)

    # Cancel any linger timer for this partition; the epoch counter is NOT cleared, so a stale
    # lingerTimeout still sees a higher epoch on wake and knows it was superseded.
    p.lingerTimers.withValue(tp, timer):
      if not timer[].finished:
        timer[].cancelSoon()
    p.lingerTimers.del(tp)

    if p.partitionStrategy == psSticky:
      p.stickyPartitions.withValue(tp.topic, idx):
        if idx[] == tp.partition:
          p.stickyPartitions.del(tp.topic)
  finally:
    p.batchLock.releaseSafely("producer.flushBatch.batchLock")

  if pending.len == 0:
    return

  emitMetric(p.metricsHook, meProducerBatchFlush, tpTags(tp), int64(pending.len))

  # Move records out of `pending` to skip per-record deep copies (the loops read `records[i]`).
  var records = newSeq[Record](pending.len)
  var firstTimestamp = int64(0)
  for i in 0 ..< pending.len:
    records[i] = move pending[i].record
    records[i].offset = int64(i)
  if pending.len > 0:
    firstTimestamp = records[0].timestamp

  var produceResult: ProduceResult
  var produceError: ref KafkaError
  # Cancel between produceWrite (bytes on wire) and produceAwait leaves the outcome UNKNOWN —
  # the broker may already have persisted the batch — so the failure path skips
  # handleSendFailure / DLQ; DLQ-ing it would silently duplicate a record that did land.
  var wasCancelled = false
  # Strict in-order completion gate (idempotence only): we await the predecessor filled in by
  # doSendWithRetry before completing user futures, and release our marker in a finally.
  let gate =
    if p.enableIdempotence:
      CompletionGate()
    else:
      nil

  let sem = p.getInflightSemaphore(tp)
  var semAcquired = false
  try:
    await sem.acquire()
    semAcquired = true
  except CancelledError:
    wasCancelled = true
    produceError = newException(KafkaError, "cancelled")

  let attemptCounter = AttemptCounter()
  if produceError == nil:
    try:
      produceResult =
        await p.doSendWithRetry(tp, records, firstTimestamp, gate, attemptCounter)
    except KafkaError as e:
      produceError = e
    except CancelledError:
      wasCancelled = true
      produceError = newException(KafkaError, "cancelled")
    finally:
      if semAcquired:
        sem.releaseSafely("producer.flushBatch.inflight")

  if produceError != nil:
    # Poison the txn on any batch failure so commitTransaction's post-flush guard refuses to
    # finalize silently-missing records (EOS). Covers the cancel paths that only reach here as
    # KafkaError("cancelled"); idempotent — never downgrades an existing tsFatal.
    p.poisonTxnOnFailedBatch(produceError)

  await awaitChainMarker(gate)

  try:
    # Complete all pending futures; each message consumed one pending-records permit
    # when enqueued (see `send`), so release one per message on every terminal branch.
    if produceError != nil:
      if wasCancelled:
        # Bypass handleSendFailure — see wasCancelled for the duplication hazard.
        for i, pm in pending:
          if not pm.future.finished:
            pm.future.fail(produceError)
          p.releasePendingRecordPermit()
      else:
        # Two passes: start every failure handler before awaiting any, else they run serially.
        var failureFuts =
          newSeq[Future[Option[ProduceResult]].Raising([CancelledError])](pending.len)
        # doSendWithRetry updates attemptCounter on every terminal retry branch;
        # falls back to 1 for errors originating outside the retry loop.
        let attempts = if attemptCounter.n > 0: attemptCounter.n else: 1'i32
        for i, pm in pending:
          if not pm.future.finished:
            let fr = makeFailedRecord(tp, records[i], produceError, attempts)
            failureFuts[i] = p.handleSendFailure(fr)

        for i, pm in pending:
          if failureFuts[i] != nil:
            # Always await a spawned handler so its inner DLQ send is settled (not
            # orphaned) when pm.future was externally cancelled between the two passes;
            # propagate to pm.future only if still open.
            var handled: Option[ProduceResult]
            var handlerCancelled = false
            try:
              handled = await failureFuts[i]
            except CancelledError:
              handlerCancelled = true
            if not pm.future.finished:
              if not handlerCancelled and handled.isSome:
                pm.future.complete(handled.get)
              else:
                pm.future.fail(produceError)
          p.releasePendingRecordPermit()
    else:
      for i, pm in pending:
        if not pm.future.finished:
          let res = ProduceResult(
            topic: tp.topic,
            partition: tp.partition,
            offset:
              if produceResult.offset >= 0:
                produceResult.offset + int64(i)
              else:
                -1,
          )
          pm.future.complete(res)
        p.releasePendingRecordPermit()
  finally:
    # Release the next batch in the completion chain only after our own futures
    # are settled, so completions surface strictly in send order.
    if gate != nil and not gate.mine.isNil and not gate.mine.finished:
      gate.mine.complete()

  # Batch fully settled (semaphore released, completion chain advanced). Reclaim
  # per-partition bookkeeping if the partition is now idle. No-op unless quiescent.
  p.maybeEvictIdlePartitionState(tp)

proc lingerTimeout(
    p: KafkaProducer, tp: TopicPartition, epoch: uint64
) {.async: (raises: []).} =
  try:
    await sleepAsync(milliseconds(p.lingerMs))
  except CancelledError:
    return
  # Bail if superseded: an external flushBatch may have cancelSoon+del'd us while
  # sleepAsync was resolving, and a later send can install a fresh timer at the same key —
  # deleting that entry would strand its batch (double-del) and leak the queued cancel into
  # flushBatch's first await, failing the successor batch's futures with KafkaError("cancelled").
  if p.lingerTimerEpoch.getOrDefault(tp, 0'u64) != epoch:
    return
  p.lingerTimers.del(tp)
  await p.flushBatch(tp)

proc renewLingerTimer*(p: KafkaProducer, tp: TopicPartition) {.gcsafe, raises: [].} =
  ## Replace the linger timer for `tp`, cancelling any stale entry first so it doesn't leak.
  p.lingerTimers.withValue(tp, existing):
    if not existing[].finished:
      existing[].cancelSoon()
  let epoch = p.lingerTimerEpoch.getOrDefault(tp, 0'u64) + 1
  p.lingerTimerEpoch[tp] = epoch
  p.lingerTimers[tp] = lingerTimeout(p, tp, epoch)

proc assertBatchEnqueueable(
    p: KafkaProducer, scope: TxnScope
) {.raises: [KafkaError].} =
  ## Re-check, under an already-held batchLock, that the producer still permits enqueueing a
  ## batched record; on failure it RELEASES the caller's pending-record permit and raises, so the
  ## caller's `finally` only releases the lock.
  ##
  ## Both guards must run in the SAME no-await region as the enqueue: past lsRunning enqueuing
  ## slips a record past failRemaining (leaking its future), past tsInTransaction it stays
  ## invisible to a commit/abort snapshot until it lands in `p.batches`; and once a later txn
  ## is open (`txnScope` replaced), the record would leak into that one.
  if p.state != lsRunning:
    p.releasePendingRecordPermit()
    raise newException(KafkaClosedError, "producer is closed")
  if p.transactionalId.isSome:
    let changed = p.txnChange(scope, "send")
    if changed.isSome:
      p.releasePendingRecordPermit()
      raise newException(KafkaError, changed.get)

proc appendPendingToBatchLocked(
    p: KafkaProducer, tp: TopicPartition, pm: PendingMessage
) {.raises: [].} =
  ## Append `pm` to `tp`'s pending batch and bump `pendingBatchBytes` in lock-step, so the
  ## byte-cap check never re-sums a filling batch (was O(n²)); the one place enqueue accounting
  ## lives (send()'s batched path and the test-only injector both use it).
  p.batches.mgetOrPut(tp, @[]).add(pm)
  p.pendingBatchBytes[tp] =
    p.pendingBatchBytes.getOrDefault(tp, 0'i64) + int64(pm.estimatedBytes)

proc appendRecordLocked(
    p: KafkaProducer, tp: TopicPartition, pm: PendingMessage
): bool {.raises: [].} =
  ## Append `pm` under an already-held batchLock; return whether the batch now needs an immediate
  ## flush (count reached maxBatchRecords). Caller must have run `assertBatchEnqueueable`. The
  ## byte cap is deliberately NOT re-checked: send()'s seal pre-check flushed the in-progress one.
  p.appendPendingToBatchLocked(tp, pm)

  var batchLen = 0
  p.batches.withValue(tp, batch):
    batchLen = batch[].len
  if batchLen >= p.maxBatchRecords:
    return true
  if batchLen == 1:
    p.renewLingerTimer(tp)
  return false

proc spawnNoRaise(fut: Future[void].Raising([])) =
  ## Fire-and-forget a future statically proven to raise nothing: `asyncSpawn` aborts the process
  ## on failure, and the `Raising([])` parameter turns that into a COMPILE-TIME check.
  asyncSpawn fut

proc startFlush(p: KafkaProducer, tp: TopicPartition) {.async: (raises: []).} =
  ## Drive a size-triggered flush from `send()`, dispatched by producer mode.
  ##
  ## Non-transactional: detach as a self-tracked task. flushBatch extracts the WHOLE batch, so an
  ## inline await would let a cancel of THIS caller tear down the shared flush and fail co-located
  ## records of concurrent send()s (mis-DLQ / duplicate); detaching confines it to our own `fut`.
  ## It self-registers in `inflightFlushFutures` (flush()/close() still wait) and — txn sections
  ## skipped and batchLock free on this path — extracts before its first suspension, so the
  ## sealFirst re-append order holds. Transactional: await inline — a cancel then poisons and
  ## aborts the whole txn (atomic EOS).
  if p.transactionalId.isSome:
    await p.flushBatch(tp)
  else:
    spawnNoRaise p.flushBatch(tp)

proc sendScoped(
    p: KafkaProducer,
    scope: TxnScope,
    topic: string,
    value: Option[seq[byte]],
    key: Option[seq[byte]],
    partition: int32,
    headers: seq[RecordHeader],
    timestamp: int64,
): Future[ProduceResult] {.async: (raises: [KafkaError, CancelledError]).} =
  ## `send` for the txn its caller captured (`scope`, nil outside one), so a call that
  ## waited before sending cannot join a txn begun meanwhile.
  requireRunning(p)
  # Before any await, so a send outside a txn fails as such, not as a replaced txn; and
  # before the size check, so not as a rejection the caller may skip either.
  if p.transactionalId.isSome:
    p.requireTxnScope(scope, "must call beginTransaction before send", "send")

  let actualTimestamp = resolveTimestamp(timestamp)
  let rec = Record(
    offset: 0, timestamp: actualTimestamp, key: key, value: value, headers: headers
  )
  # Client-side validation runs before any await, so the caller sees size rejections
  # without a metadata refresh, and no txn-poisoning failure or cancel can preempt them.
  # Raised outside the try below, so the rejection itself never poisons the txn.
  let recBytes = estimateRecordSize(rec)
  if int64(recordBatchV2HeaderBytes) + int64(recBytes) > int64(p.maxBatchBytes):
    raise newException(
      KafkaRecordRejectedError,
      "record estimated size " & $recBytes & " plus batch header " &
        $recordBatchV2HeaderBytes & " exceeds maxBatchBytes " & $p.maxBatchBytes,
    )

  # Set while this send counts toward `scope.unjoined`: from the txn check until its record
  # joins. A failure meanwhile must poison the txn here (see poisonTxnOnUnjoined).
  var unjoined = false
  template leaveUnjoined() =
    # Idempotent: drop this send from the count at most once.
    if unjoined:
      unjoined = false
      dec scope.unjoined

  defer:
    leaveUnjoined()

  # Transactional immediate-send (lingerMs <= 0) barrier marker: that path runs doSendWithRetry
  # outside txnLock, so without it a concurrent commit/abort could see no pending work and send
  # EndTxn while this produce is still on the wire (EOS violation). The defer removes it.
  var txnMarker: Future[void] = nil
  defer:
    if txnMarker != nil:
      if not txnMarker.finished:
        txnMarker.complete()
      p.removeInflightFlush(txnMarker)

  try:
    if p.transactionalId.isSome:
      # Counted from here, so the commit cannot go through while this record may still join.
      inc scope.unjoined
      unjoined = true
      # A deferred epoch bump (broker still finalizing the prior txn) must be retried before this
      # txn's first record, else AddPartitionsToTxn goes out with a stale producerEpoch and is fenced.
      if p.pendingEpochBump:
        await p.ensureEpochBumped()
        requireRunning(p)
        p.requireTxnUnchanged(scope, "send")
    elif p.enableIdempotence:
      await p.ensureIdempotenceInitialized()
      requireRunning(p)

    let part =
      if partition >= 0:
        # Range-check a caller-supplied partition against cached metadata only: an uncached
        # topic would hit the broker (slow UNKNOWN_TOPIC_OR_PARTITION) while a cached one is
        # rejected fast without a refresh (the auto-resolve path already refreshes).
        let partsOpt = p.cluster.getTopicPartitionsCopy(topic)
        # Not a KafkaRecordRejectedError: the cache may lag a topic that has since grown.
        if partsOpt.isSome and partition >= int32(partsOpt.get.len):
          raise newException(
            KafkaError,
            "partition " & $partition & " out of range for topic " & topic &
              " (partitions: 0..<" & $partsOpt.get.len & ")",
          )
        partition
      else:
        let resolved = await p.resolvePartition(topic, key)
        requireRunning(p)
        resolved

    let tp = TopicPartition(topic: topic, partition: part)

    if p.transactionalId.isSome:
      # Hold txnLock from the state re-check through any AddPartitionsToTxn round-trip so a
      # concurrent commit/abort can't tear down partition tracking between mark and register
      # (txnLock is outermost, so awaiting the coordinator request nests no further).
      await p.txnLock.acquire()
      try:
        p.requireTxnUnchanged(scope, "send")
        # Record whether this call added `tp` to the pending set, so a register failure
        # rolls back only our own entry, not those from earlier sends.
        let addedHere = tp notin p.txnPartitions and tp notin p.txnPendingPartitions
        p.markPartitionForTxn(tp)
        if p.lingerMs <= 0:
          # Immediate send: register now; batched sends are registered by flushBatch.
          try:
            await p.registerPendingTxnPartitions()
          except KafkaError as e:
            if addedHere:
              p.txnPendingPartitions.excl(tp)
            raise e
          requireRunning(p)
          # Register the finalize barrier marker while still holding txnLock, so a later
          # commit/abort calling awaitInflightFlushes is guaranteed to see it.
          txnMarker = newFuture[void]("txnImmediateSend")
          p.inflightFlushFutures.add(txnMarker)
      finally:
        p.txnLock.releaseSafely("producer.send.txn")

    if p.lingerMs <= 0:
      let sem = p.getInflightSemaphore(tp)
      await sem.acquire()
      if p.state != lsRunning:
        sem.releaseSafely("producer.send.inflight")
        raise newException(KafkaClosedError, "producer is closed")
      # Strict in-order completion gate (idempotence only); see flushBatch.
      let gate =
        if p.enableIdempotence:
          CompletionGate()
        else:
          nil
      var semReleased = false
      template releaseInflightSem() =
        # Idempotent: release the per-partition in-flight permit at most once.
        if not semReleased:
          semReleased = true
          sem.releaseSafely("producer.releaseInflightSem")

      let attemptCounter = AttemptCounter()
      # Joined: from here doSendWithRetry and the cancel branch below poison the txn on a
      # failure of this record.
      leaveUnjoined()
      try:
        var pendingErr: ref KafkaError = nil
        var sendErr: ref KafkaError = nil
        try:
          result =
            await p.doSendWithRetry(tp, @[rec], actualTimestamp, gate, attemptCounter)
        except KafkaError as e:
          sendErr = e
        except CancelledError as e:
          # doSendWithRetry's terminal transition only covers KafkaError, so poison here to keep a
          # later commit from finalizing a txn missing this record (EOS).
          p.poisonTxnOnFailedBatch(e)
          raise e

        # Release the permit BEFORE failure handling or the completion-chain wait: holding it across
        # `await gate.prev` would let a fast successor pin a slot while blocked on a slow predecessor.
        releaseInflightSem()

        if sendErr == nil and p.partitionStrategy == psSticky:
          # A successful immediate send is its own completed "batch", so rotate the sticky partition
          # as flushBatch does, else a keyless sticky send pins one partition forever; guard on the
          # current value so a concurrent re-stick isn't clobbered.
          p.stickyPartitions.withValue(tp.topic, idx):
            if idx[] == tp.partition:
              p.stickyPartitions.del(tp.topic)

        if sendErr != nil:
          # Route every KafkaError subclass through handleSendFailure / DLQ, as the batched
          # path does; catching only KafkaApiError would let transport errors bypass the DLQ.
          let attempts = if attemptCounter.n > 0: attemptCounter.n else: 1'i32
          let failedRec = makeFailedRecord(tp, rec, sendErr, attempts)
          let handled = await p.handleSendFailure(failedRec)
          if handled.isSome:
            result = handled.get
          else:
            pendingErr = sendErr

        await awaitChainMarker(gate)
        if pendingErr != nil:
          raise pendingErr
      finally:
        # Release the next send in the completion chain only after our result is settled.
        if gate != nil and not gate.mine.isNil and not gate.mine.finished:
          gate.mine.complete()
        # Backstop for the cancel path (release above skipped); no-op if released.
        releaseInflightSem()
        # Reclaim per-partition bookkeeping if this send left the partition idle; after the
        # release above so the idle check sees a full permit count.
        p.maybeEvictIdlePartitionState(tp)
    else:
      # Bound the cross-partition queue: each message holds one permit until its future resolves,
      # acquired BEFORE batchLock so backpressure queues here rather than stalling the flush path.
      await p.pendingRecordsSem.acquire()
      if p.state != lsRunning:
        p.releasePendingRecordPermit()
        raise newException(KafkaClosedError, "producer is closed")

      let fut = Future[ProduceResult].init("kafka.producer.send")
      let pm =
        PendingMessage(record: rec, future: fut, tp: tp, estimatedBytes: recBytes)

      var needsFlush = false
      # If appending this record would push the in-progress batch (records + RecordBatch v2 header)
      # past maxBatchBytes, SEAL it — flush first, then let the record open a fresh batch; matches
      # Kafka Java's RecordAccumulator. A record over the cap on its own already raised at send()
      # entry, so a fresh batch can never itself overflow: sealing suffices, the loop terminates.
      # The check and append must share one synchronous batchLock region while startFlush suspends,
      # so loop and re-check under the lock.
      while true:
        var sealFirst = false
        try:
          await p.batchLock.acquire()
        except CancelledError:
          p.releasePendingRecordPermit()
          raise
        try:
          p.assertBatchEnqueueable(scope)
          let curBytes = p.pendingBatchBytes.getOrDefault(tp, 0'i64)
          var curLen = 0
          p.batches.withValue(tp, batch):
            curLen = batch[].len
          if curLen > 0 and
              int64(recordBatchV2HeaderBytes) + curBytes + int64(pm.estimatedBytes) >
              int64(p.maxBatchBytes):
            sealFirst = true
          else:
            needsFlush = p.appendRecordLocked(tp, pm)
            # Joined: the batch is the txn's now, and its flush poisons on failure.
            leaveUnjoined()
        finally:
          p.batchLock.releaseSafely("producer.releaseInflightSem.batchLock")

        if not sealFirst:
          break

        # flushBatch extracts the batch under batchLock BEFORE any broker await, so awaiting here
        # guarantees it is gone from `p.batches` before the next iteration enqueues this record and
        # the sealed batch keeps an earlier sequence. State may have advanced during the await
        # (close / txn finalize), so the next iteration re-runs the guards.
        await p.startFlush(tp)

      if needsFlush:
        # startFlush (not flushBatch): detach so a cancel of THIS caller can't fail co-located ones.
        await p.startFlush(tp)

      try:
        result = await fut
      except CatchableError as e:
        if e of CancelledError:
          raise (ref CancelledError)(e)
        if e of KafkaError:
          # Preserve the dynamic KafkaError subtype so callers matching on it keep
          # working through the batched-send future hop.
          raise (ref KafkaError)(e)
        raise newException(KafkaError, e.msg)
  except CancelledError as e:
    if unjoined:
      p.poisonTxnOnUnjoined(scope, e)
    raise e
  except KafkaError as e:
    if unjoined:
      p.poisonTxnOnUnjoined(scope, e)
    raise e

proc send*(
    p: KafkaProducer,
    topic: string,
    value: Option[seq[byte]],
    key: Option[seq[byte]] = none(seq[byte]),
    partition: int32 = -1,
    headers: seq[RecordHeader] = @[],
    timestamp: int64 = 0,
): Future[ProduceResult] {.async: (raises: [KafkaError, CancelledError], raw: true).} =
  ## Buffer one record for `topic` and resolve with its `ProduceResult` once acked. A negative
  ## `partition` selects via the configured strategy (key hash when keyed); oversized records
  ## raise before buffering. Inside a transaction the send joins it — commit only after it acked.
  ## A send that fails before its record joins still poisons that transaction, so after a
  ## failed or cancelled send (a timeout included) the commit either carries the record or is
  ## refused; so is a commit made while the send has yet to join (one that `sendFireAndForget`
  ## handed off while `maxPendingRecords` was full, say). The exception is
  ## `KafkaRecordRejectedError` (an oversized record): the transaction stays committable, so
  ## the caller may skip the record. The record never joins a later transaction begun while
  ## the send awaited (nor one whose commit or abort was already called: refused).
  ##
  ## A `none` value sends a tombstone (null value) for compacted topics; pair it
  ## with a key so the broker can match the key to delete. A nil `seq[byte]` passed
  ## to the `seq[byte]` overload stays an empty value, not a tombstone.
  ##
  ## A failure the producer's `errorHandler` diverts does not raise: the result's `status` is
  ## `pstDropped`, `pstDeadLettered`, or `pstDeadLetterFailed` (with `offset = -1`) instead of
  ## `pstSuccess`.
  p.sendScoped(p.currentTxnScope(), topic, value, key, partition, headers, timestamp)

proc send*(
    p: KafkaProducer,
    topic: string,
    value: seq[byte],
    key: Option[seq[byte]] = none(seq[byte]),
    partition: int32 = -1,
    headers: seq[RecordHeader] = @[],
    timestamp: int64 = 0,
): Future[ProduceResult] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Non-null `send`; equivalent to the `Option` overload with `some(value)`.
  ## Use the `Option` overload with `none(seq[byte])` for a tombstone.
  return await p.send(topic, some(value), key, partition, headers, timestamp)

proc sendToDeadLetter(
    p: KafkaProducer, rec: FailedRecord
): Future[bool] {.async: (raises: [CancelledError]).} =
  ## Forward `rec` to the configured DLQ topic; true only on a broker-acked
  ## `pstSuccess`. The caller surfaces false as `pstDeadLetterFailed`, so the original send
  ## sees the failure was not redirected.
  var headers = rec.headers
  headers.add(RecordHeader(key: "dlq.original.topic", value: some(toBytes(rec.topic))))
  headers.add(
    RecordHeader(key: "dlq.original.partition", value: some(toBytes($rec.partition)))
  )
  headers.add(
    RecordHeader(key: "dlq.error.message", value: some(toBytes(rec.error.msg)))
  )

  if rec.error of KafkaApiError:
    let code = (ref KafkaApiError)(rec.error).errorCode
    headers.add(
      RecordHeader(key: "dlq.error.code", value: some(toBytes(kafkaErrorName(code))))
    )

  headers.add(RecordHeader(key: "dlq.attempts", value: some(toBytes($rec.attempts))))

  # Capture the routed producer and topic in this synchronous turn (the caller confirmed
  # `dlqProducer != nil` in the preceding turn, so the capture is non-nil): a concurrent
  # setDeadLetterQueue between the two derefs (both followed by awaits on send) would
  # otherwise split "which producer" from "which topic" across the dispatch.
  let dlq = p.dlqProducer
  let dlqTopic = p.dlqTopic
  var dlqResult: ProduceResult
  try:
    dlqResult = await dlq.send(
      topic = dlqTopic,
      value = rec.value,
      key = rec.key,
      headers = headers,
      timestamp = rec.timestamp,
    )
  except KafkaError as e:
    warn "Failed to send to dead letter queue for " & rec.topic & "[" & $rec.partition &
      "]: " & e.msg
    emitMetric(
      p.metricsHook,
      meProducerDlqError,
      [("topic", rec.topic), ("partition", $rec.partition), ("status", "exception")],
      1,
    )
    return false

  if dlqResult.status != pstSuccess:
    warn "Dead letter queue send failed for ",
      rec.topic, "-", rec.partition, ": status=", $dlqResult.status
    emitMetric(
      p.metricsHook,
      meProducerDlqError,
      [
        ("topic", rec.topic),
        ("partition", $rec.partition),
        ("status", $dlqResult.status),
      ],
      1,
    )
    return false

  emitMetric(
    p.metricsHook,
    meProducerDlqSent,
    [
      ("dlq_topic", dlqTopic),
      ("original_topic", rec.topic),
      ("original_partition", $rec.partition),
    ],
    1,
  )
  return true

proc flush*(p: KafkaProducer) {.async: (raises: []).} =
  ## Flush all pending batches and wait for any already-extracted in-flight batch (a
  ## size-/linger-triggered flushBatch mid round-trip) to ack: it is invisible to the
  ## `p.batches` snapshot below, so close() could otherwise tear the cluster down under it.
  var partitions: seq[TopicPartition]
  try:
    await p.batchLock.acquire()
  except CancelledError:
    return
  try:
    for tp in p.batches.keys:
      partitions.add(tp)
  finally:
    p.batchLock.releaseSafely("producer.flush")

  # Snapshot existing in-flight markers BEFORE starting our own flushBatches — a snapshot,
  # not a drain loop, so a steady stream of new sends can't block flush() indefinitely.
  let preInflight = p.inflightFlushFutures
  var futs: seq[Future[void]]
  for tp in partitions:
    futs.add(p.flushBatch(tp))
  try:
    await allFutures(futs)
    await allFutures(preInflight)
  except CancelledError:
    return

proc runEndTxn(
    p: KafkaProducer, scope: TxnScope, committed: bool
): Future[bool] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Shared body of `commitTransaction` / `abortTransaction`, run with `txnLock` held and the
  ## preconditions checked: drain buffered sends, issue `EndTxn`, settle the deferred epoch bump,
  ## keep the broker-side txn releasable on every failure path. True when it was finalized locally
  ## (coordinator metadata still empty, no EndTxn round-trip); callers own the metrics vocabulary.
  let label = if committed: "commitTransaction" else: "abortTransaction"
  # True once EndTxn succeeded broker-side: a later cancel must still preserve the owed epoch
  # bump, while a cancel during EndTxn leaves this false so we don't claim a bump we aren't owed.
  var endTxnSucceeded = false
  try:
    if committed:
      await p.registerPendingTxnPartitions()
    else:
      # Register pending partitions BEFORE flush, mirroring commitTransaction: this drains
      # `txnPendingPartitions` under `txnLock`, so flushBatch won't re-acquire the non-reentrant
      # `txnLock` (self-deadlock for an abort with buffered records from the lingerMs > 0 path).
      try:
        await p.registerPendingTxnPartitions()
      except KafkaError as regErr:
        # Registration failed. If the coordinator already moved this txn to `Ongoing`
        # (txnStarted) we must still issue EndTxn(abort) to release it rather than orphaning it
        # until transaction.timeout.ms, so drain the pending set first (else flush re-enters
        # registration); when nothing started there is nothing to abort — re-raise.
        if not p.txnStarted:
          raise regErr
        # Fail unregistered-partition batches locally: flush() would otherwise write them without
        # an AddPartitionsToTxn entry, leaving the LSO stuck until transaction.timeout.ms.
        for tp in p.txnPendingPartitions:
          if p.batches.hasKey(tp):
            p.failBatchFutures(tp, regErr)
        p.txnPendingPartitions.clear()
    await p.flush()
    # flush() only awaits batches still in `p.batches`; a size-/linger-triggered flushBatch may be
    # mid round-trip invisibly, so drain those too before EndTxn can race a produce of this txn.
    await p.awaitInflightFlushes()
    if committed and p.txnState in {tsError, tsFatal}:
      # A send or sendOffsetsToTransaction of this txn failed and set tsError/tsFatal; refuse to
      # finalize rather than commit a txn missing records or offsets (EOS).
      raise newException(
        KafkaError,
        "cannot commit: a send or sendOffsetsToTransaction of this transaction failed (state: " &
          $p.txnState & ")",
      )
    if committed and scope.unjoined > 0:
      # A send or sendOffsetsToTransaction of this txn is still short of joining it (waiting on
      # backpressure, txnLock or metadata) and no longer can: committing would finalize the txn
      # without its record or offsets.
      raise newException(
        KafkaError,
        "cannot commit: " & $scope.unjoined &
          " send / sendOffsetsToTransaction call(s) of this transaction have not joined it",
      )
    if not p.txnStarted:
      # Nothing reached the coordinator, so its metadata is still `Empty` and EndTxn would be
      # rejected with INVALID_TXN_STATE; finalize locally (Java's empty-transaction short-circuit).
      # No epoch bump: the coordinator never advanced this transaction.
      if not p.tryTransition(tsReady):
        warnTransitionFailed(p, tsReady, label & " empty-txn finalize")
      p.finalizeTxnTracking(owesEpochBump = false)
      return true
    let req = EndTxnRequest(
      transactionalId: p.transactionalId.get,
      producerId: p.producerId,
      producerEpoch: p.producerEpoch,
      committed: committed,
    )
    let resp = withTxnCoordinatorRetry(p, await p.txnCoordinator.endTxn(req))
    if resp.errorCode != keNone:
      p.transitionToError(resp.errorCode)
      raise newKafkaApiError(
        resp.errorCode,
        "EndTxn (" & (if committed: "commit" else: "abort") & ") failed: " &
          kafkaErrorName(resp.errorCode),
      )
    endTxnSucceeded = true
    await p.bumpEpoch()
    if not p.tryTransition(tsReady):
      warnTransitionFailed(p, tsReady, label & " post-EndTxn")
    p.finalizeTxnTracking(owesEpochBump = false)
    return false
  except CancelledError as e:
    # Only clear txnStarted when EndTxn confirmed the broker left Ongoing: a pre-EndTxn cancel
    # keeps markers so the recovery abort can release it, else it orphans → LSO stall (EOS).
    if endTxnSucceeded:
      p.finalizeTxnTracking(owesEpochBump = true)
    if not p.tryTransition(tsError):
      warnTransitionFailed(p, tsError, label & " cancelled")
    raise e
  except KafkaApiError as e:
    # Same invariant: endTxnSucceeded gates the clear (a pre-EndTxn partial-add or EndTxn
    # response error keeps markers so the recovery abort can release the Ongoing txn).
    p.transitionToError(e.errorCode)
    if endTxnSucceeded:
      p.finalizeTxnTracking(owesEpochBump = true)
    raise e
  except KafkaError as e:
    # Post-flush tsError/tsFatal re-raise: broker still Ongoing, preserve.
    if not p.tryTransition(tsError):
      warnTransitionFailed(p, tsError, label & " transport error")
    if endTxnSucceeded:
      p.finalizeTxnTracking(owesEpochBump = true)
    raise e

proc closeAndLockTxn(
    p: KafkaProducer, abort: bool
): Future[TxnScope] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Shared entry of `commitTransaction` / `abortTransaction`: close the current txn,
  ## then take txnLock; returns the closed scope with the lock held. Closed before
  ## the awaits, which can suspend: a send made meanwhile would otherwise count
  ## against the commit and have it refused.
  result = p.txnScope
  # A finished txn keeps its scope until the next begin; leave how it ended as is.
  if p.currentTxnScope() != nil:
    if abort:
      result.ending = teAbortCalled
    elif result.ending == teOpen:
      result.ending = teCommitCalled
  # Resolve any deferred epoch bump BEFORE taking txnLock (lock order: txnLock outermost): a
  # zero-send transaction would otherwise send EndTxn with a stale producerEpoch and be fenced.
  await p.ensureEpochBumped()
  await p.txnLock.acquire()

proc requireFinalizable(
    p: KafkaProducer, scope: TxnScope, verb: string
) {.raises: [KafkaError].} =
  ## Under txnLock: refuse a fatal producer, or a call whose txn (`scope`) ended
  ## while it waited, so it cannot finalize the one begun since.
  p.requireNotFatal()
  p.requireSameTxn(scope, verb)

proc commitTransaction*(
    p: KafkaProducer
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Flush buffered sends, send `EndTxn(commit)`, and settle the deferred epoch
  ## bump. Refuses — raising without committing — when a buffered send failed, a
  ## `send` or `sendOffsetsToTransaction` has yet to join the transaction, the
  ## transaction is poisoned, or `abortTransaction` was called on it, so a commit
  ## never finalizes missing records. The transaction closes on the call, even if
  ## the commit then fails: abort it, or retry while it is not left abort-only.
  template failCommit(e: ref KafkaError) =
    # Bound once: `e` may be a `newException` call.
    let err = e
    emitMetric(
      p.metricsHook,
      meProducerTxnCommitError,
      txnErrorTags(kafkaErrorTag(err), $errorClassToCstring(ecUnknown)),
      1,
    )
    raise err

  # Only an abort called ahead of this commit, on a txn still in progress, refuses it;
  # refuse before waiting behind it.
  let current = p.currentTxnScope()
  if current != nil and current.ending == teAbortCalled:
    failCommit(
      newException(KafkaError, "cannot commit: abortTransaction was already called")
    )
  # The awaits inside the try, so a commit failing there, the txn closed already,
  # still emits its error metric.
  var locked = false
  try:
    let scope = await p.closeAndLockTxn(abort = false)
    locked = true
    p.requireFinalizable(scope, "commit")
    if not p.tryTransition(tsCommitting):
      raise newException(
        KafkaError, "cannot commit: not in transaction (state: " & $p.txnState & ")"
      )
    # The empty-transaction short-circuit still reports success; it is the
    # deferred-bump shape that says a bump is owed.
    if await runEndTxn(p, scope, committed = true):
      emitMetric(p.metricsHook, meProducerTxnCommitOk, [("empty", "true")], 1)
    else:
      emitMetric(
        p.metricsHook,
        meProducerTxnCommitOk,
        [("deferred_bump", $p.pendingEpochBump)],
        1,
      )
  except CancelledError as e:
    emitMetric(
      p.metricsHook,
      meProducerTxnCommitError,
      txnErrorTags("CANCELLED", $errorClassToCstring(ecUnknown)),
      1,
    )
    raise e
  except KafkaApiError as e:
    emitMetric(
      p.metricsHook,
      meProducerTxnCommitError,
      [
        ("error_code", kafkaErrorName(e.errorCode)),
        ("error_class", $errorClassToCstring(errorClassForContext(e.errorCode, true))),
      ],
      1,
    )
    raise e
  except KafkaError as e:
    failCommit(e)
  finally:
    if locked:
      p.txnLock.releaseSafely("producer.commitTransaction")

proc abortTransaction*(
    p: KafkaProducer
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Flush buffered sends (aborted with the transaction, never visible to
  ## read_committed consumers), send `EndTxn(abort)`, and settle the deferred
  ## epoch bump. Safe from poisoned states; never commits anything. Refused by a
  ## `tsFatal` producer, which only `close` tears down, leaving the broker-side
  ## transaction to the coordinator's timeout. Closes the transaction on the call,
  ## as `commitTransaction` does.
  var locked = false
  try:
    let scope = await p.closeAndLockTxn(abort = true)
    locked = true
    p.requireFinalizable(scope, "abort")
    if p.txnState notin txnInProgressStates:
      raise newException(
        KafkaError, "cannot abort: not in transaction (state: " & $p.txnState & ")"
      )
    if not p.tryTransition(tsAborting):
      raise newException(
        KafkaError, "cannot transition to aborting from state: " & $p.txnState
      )
    if await runEndTxn(p, scope, committed = false):
      emitMetric(p.metricsHook, meProducerTxnAbortOk, [("empty", "true")], 1)
    else:
      emitMetric(
        p.metricsHook, meProducerTxnAbortOk, [("deferred_bump", $p.pendingEpochBump)], 1
      )
  except CancelledError as e:
    emitMetric(
      p.metricsHook,
      meProducerTxnAbortError,
      txnErrorTags("CANCELLED", $errorClassToCstring(ecUnknown)),
      1,
    )
    raise e
  except KafkaApiError as e:
    emitMetric(
      p.metricsHook,
      meProducerTxnAbortError,
      [
        ("error_code", kafkaErrorName(e.errorCode)),
        ("error_class", $errorClassToCstring(errorClassForContext(e.errorCode, true))),
      ],
      1,
    )
    raise e
  except KafkaError as e:
    emitMetric(
      p.metricsHook,
      meProducerTxnAbortError,
      txnErrorTags(kafkaErrorTag(e), $errorClassToCstring(ecUnknown)),
      1,
    )
    raise e
  finally:
    if locked:
      p.txnLock.releaseSafely("producer.abortTransaction")

proc sendFireAndForget*(
    p: KafkaProducer,
    topic: string,
    value: Option[seq[byte]],
    key: Option[seq[byte]] = none(seq[byte]),
    partition: int32 = -1,
    headers: seq[RecordHeader] = @[],
    timestamp: int64 = 0,
    callback: ProduceCallback = nil,
) {.async: (raises: [CancelledError, KafkaClosedError]).} =
  ## Send without awaiting the ack: the result (or failure) goes to `callback`,
  ## or to the logs when `callback` is nil. Returns once the send is handed to
  ## the pipeline, not once it is acked; `close` drains handed-off sends. Inside
  ## a transaction, commit only once `callback` ran. A commit made after this
  ## returned, while the send has yet to join the transaction, is refused; one
  ## made while the call still waits for a `maxPendingFireAndForget` slot is not,
  ## as nothing was handed off yet: the send then fails to `callback` instead.
  ## A `none` value sends a tombstone (null value) for compacted topics. A nil
  ## `seq[byte]` passed to the `seq[byte]` overload stays an empty value.
  proc invokeCallback(res: ProduceResult, err: ref KafkaError) {.raises: [].} =
    if callback == nil:
      if err != nil:
        warn "sendFireAndForget error (no callback): ", err.msg
      return
    # ProduceCallback is `raises: []` but a buggy implementation can violate that; a
    # leaked exception must not kill task()'s finally (the semaphore release).
    let raised = p.produceCallbackFailures.guardCallback(
      "sendFireAndForget callback", ""
    ):
      callback(res, err)
    if raised != nil and err != nil:
      warn "sendFireAndForget error (callback raised): ", err.msg

  # Captured before the slot wait, so a txn begun meanwhile cannot take the record.
  let scope = p.currentTxnScope()
  await p.fireAndForgetSem.acquire()

  # The acquire may have suspended long enough for close() to advance state;
  # re-check before racing close()'s drain of pendingFireAndForget.
  if p.state != lsRunning:
    p.fireAndForgetSem.releaseSafely("producer.callback")
    raise newException(KafkaClosedError, "producer is closed")

  # Lazily drop completed futures. The list is bounded by fireAndForgetSem and
  # fully drained on close().
  var j = 0
  for i in 0 ..< p.pendingFireAndForget.len:
    if not p.pendingFireAndForget[i].finished:
      if j != i:
        p.pendingFireAndForget[j] = p.pendingFireAndForget[i]
      inc j
  p.pendingFireAndForget.setLen(j)

  proc task() {.async: (raises: []).} =
    try:
      let res =
        await p.sendScoped(scope, topic, value, key, partition, headers, timestamp)
      invokeCallback(res, nil)
    except KafkaError as e:
      invokeCallback(ProduceResult(topic: topic, partition: partition, offset: -1), e)
    except CancelledError:
      invokeCallback(
        ProduceResult(topic: topic, partition: partition, offset: -1),
        newException(KafkaError, "cancelled"),
      )
    finally:
      p.fireAndForgetSem.releaseSafely("producer.fireAndForget")

  if p.state != lsRunning:
    # Second state check immediately before publishing the future: it narrows but doesn't
    # eliminate the close() race (task() suspends on its first await, so close() can still
    # see an empty list here); close()'s drain loop re-checks after each wait().
    p.fireAndForgetSem.releaseSafely("producer.fireAndForget")
    raise newException(KafkaClosedError, "producer is closed")

  let fut = task()
  p.pendingFireAndForget.add(fut)

proc sendFireAndForget*(
    p: KafkaProducer,
    topic: string,
    value: seq[byte],
    key: Option[seq[byte]] = none(seq[byte]),
    partition: int32 = -1,
    headers: seq[RecordHeader] = @[],
    timestamp: int64 = 0,
    callback: ProduceCallback = nil,
) {.async: (raises: [CancelledError, KafkaClosedError]).} =
  ## Non-null `sendFireAndForget`; equivalent to the `Option` overload with
  ## `some(value)`. Use the `Option` overload with `none(seq[byte])` for a tombstone.
  await p.sendFireAndForget(
    topic, some(value), key, partition, headers, timestamp, callback
  )
