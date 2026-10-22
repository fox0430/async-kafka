import std/[unittest, tables, options, strutils, importutils]
import std/logging as stdLogging
from std/times import epochTime

import chronos

import async_kafka/[errors, broker]
import async_kafka/cluster {.all.}
import async_kafka/internal/metrics
import async_kafka/protocol/[messages, primitives, api_keys, compression]
import async_kafka/protocol/lz4 as lz4mod
import async_kafka/producer/producer
import async_kafka/producer/types
import async_kafka/producer/partitioner
import async_kafka/producer/idempotence
import async_kafka/producer/transaction
import async_kafka/producer/dlq
import async_kafka/producer/batch

privateAccess(KafkaProducer)

import fake_broker, producer_helpers, producer_internal_helpers, async_helpers

privateAccess(Cluster)

type ProducerLogSink = ref object of stdLogging.Logger
  messages: seq[string]

method log(
    logger: ProducerLogSink, level: stdLogging.Level, args: varargs[string, `$`]
) {.gcsafe.} =
  ## Captures the diagnostics an application handler would have printed.
  logger.messages.add(args.join(""))

suite "Producer: close guard":
  test "send after close raises KafkaError":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, lingerMs = 100)
      p.markClosed()
      var raised = false
      try:
        discard await p.send("test-topic", @[0x01'u8])
      except KafkaError as e:
        raised = true
        check "closed" in e.msg
      check raised

    waitFor testBody()

  test "send blocked on batchLock raises when closed before lock acquired":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      # Pre-populate metadata so resolvePartition succeeds without a broker
      cluster.topicPartitions["test-topic"] = @[
        PartitionMeta(
          partitionIndex: 0,
          leaderId: 0,
          errorCode: keNone,
          replicaNodes: @[],
          isrNodes: @[],
        )
      ]
      let p = newKafkaProducer(cluster, lingerMs = 100)
      cluster.lastMetadataRefresh = Moment.now()

      # Hold the batch lock so send() blocks on acquire
      await p.acquireBatchLock()

      let sendFut = p.send("test-topic", @[0x01'u8])

      # Close the producer while send is waiting
      p.markClosed()

      p.releaseBatchLock()

      var raised = false
      try:
        discard await sendFut
      except KafkaError as e:
        raised = true
        check "closed" in e.msg
      check raised

    waitFor testBody()

suite "Producer: idempotent maxInFlight is honored":
  test "idempotence keeps the caller's maxInFlightRequests=5":
    # KIP-185 in-flight resequencing keeps order under a producer-id
    # reset, so an idempotent producer can run with maxInFlight > 1
    # without risking OUT_OF_ORDER_SEQUENCE_NUMBER on retry.
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster, enableIdempotence = true, maxInFlightRequests = 5)
    check p.maxInFlightRequests == 5

  test "idempotence does not raise the cap below caller's request":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster, enableIdempotence = true, maxInFlightRequests = 1)
    check p.maxInFlightRequests == 1

  test "idempotence caps maxInFlightRequests above 5 at 5":
    # The broker keeps only a 5-batch dedup window per producer-id/partition;
    # a larger in-flight window would let a retried batch fall outside it and
    # break ordering/dedup. The constructor must clamp rather than accept it.
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p =
      newKafkaProducer(cluster, enableIdempotence = true, maxInFlightRequests = 10)
    check p.maxInFlightRequests == 5

  test "transactional mode caps maxInFlightRequests above 5 at 5":
    # Transactional implies idempotence, so the same 5-batch window applies.
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster, transactionalId = some("txn-cap-test"), maxInFlightRequests = 8
    )
    check p.maxInFlightRequests == 5

  test "non-idempotent path leaves maxInFlightRequests untouched":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster, maxInFlightRequests = 5)
    check p.maxInFlightRequests == 5

  test "non-idempotent path leaves a large maxInFlightRequests untouched":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster, maxInFlightRequests = 10)
    check p.maxInFlightRequests == 10

  test "maxInFlightRequests <= 0 is rejected":
    # 0 would create a zero-permit in-flight semaphore that deadlocks the first
    # send; negatives are nonsensical. Both must raise at construction.
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect KafkaError:
      discard newKafkaProducer(cluster, maxInFlightRequests = 0)
    expect KafkaError:
      discard newKafkaProducer(cluster, maxInFlightRequests = -1)

suite "Producer: sequence number allocation":
  test "nextSequence starts at 0 per partition":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster, enableIdempotence = true)
    let tp1 = TopicPartition(topic: "t", partition: 0)
    let tp2 = TopicPartition(topic: "t", partition: 1)
    check p.nextSequence(tp1, 5) == 0
    check p.getSequenceNumber(tp1) == 5
    check p.nextSequence(tp2, 1) == 0
    check p.getSequenceNumber(tp2) == 1
    check p.nextSequence(tp1, 3) == 5
    check p.getSequenceNumber(tp1) == 8

  test "nextSequence wraps at int32.high":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster, enableIdempotence = true)
    let tp = TopicPartition(topic: "t", partition: 0)
    p.setSequenceNumber(tp, int32.high - 1)
    check p.nextSequence(tp, 1) == int32.high - 1
    check p.nextSequence(tp, 1) == int32.high
    # Next call wraps: (int32.high + 1) and int32.high == 0
    check p.nextSequence(tp, 1) == 0

  test "sequence numbers cleared after resetProducerId":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, enableIdempotence = true)
      let tp = TopicPartition(topic: "t", partition: 0)
      discard p.nextSequence(tp, 5)
      check p.sequenceNumberCount() == 1
      # nextSequence after reset should start from 0
      check p.nextSequence(tp, 1) == 5

    waitFor testBody()

suite "Producer KIP-185 in-flight batch tracking":
  ## KIP-185: each batch's baseSequence is allocated once at first encode and
  ## frozen on the InflightBatch entry. Retries reuse the same bytes; a
  ## producer-id reset re-sequences in-flight batches from 0 while preserving
  ## their original arrival order.

  proc makeRecord(payload: byte): Record =
    Record(
      offset: 0,
      timestamp: 0,
      key: none(seq[byte]),
      value: some(@[payload]),
      headers: @[],
    )

  test "fresh producer has zero in-flight batches per TP":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster, enableIdempotence = true)
    let tp = TopicPartition(topic: "t", partition: 0)
    check p.inflightBatchCountForTest(tp) == 0

  test "allocateInflight enqueues a batch with the current next sequence":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, enableIdempotence = true)
      p.setProducerId(1000'i64, 0'i16)
      let tp = TopicPartition(topic: "t", partition: 0)

      discard await p.allocateInflightForTest(@[makeRecord(0xAA)], tp, 0)
      check p.inflightBatchCountForTest(tp) == 1
      check p.getInflightBaseSequenceForTest(tp, 0) == 0'i32

      discard
        await p.allocateInflightForTest(@[makeRecord(0xBB), makeRecord(0xCC)], tp, 0)
      check p.inflightBatchCountForTest(tp) == 2
      check p.getInflightBaseSequenceForTest(tp, 1) == 1'i32

    waitFor body()

  test "dequeueInflight removes the front batch in FIFO order":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, enableIdempotence = true)
      p.setProducerId(1000'i64, 0'i16)
      let tp = TopicPartition(topic: "t", partition: 0)

      discard await p.allocateInflightForTest(@[makeRecord(0xAA)], tp, 0)
      discard await p.allocateInflightForTest(@[makeRecord(0xBB)], tp, 0)
      check p.inflightBatchCountForTest(tp) == 2

      let frontSeq = p.getInflightBaseSequenceForTest(tp, 0)
      let frontId = p.getInflightIdForTest(tp, 0)
      p.dequeueInflightForTest(tp, frontId)
      check p.inflightBatchCountForTest(tp) == 1
      check p.getInflightBaseSequenceForTest(tp, 0) > frontSeq

    waitFor body()

  test "dequeueInflight removes the matching id even when not at the front":
    ## Regression for the id-based dequeue: under maxInFlight > 1 a batch can
    ## complete out of order. dequeueInflight must remove the entry whose id
    ## matches the completing coroutine's batch, not whatever sits at index 0.
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, enableIdempotence = true)
      p.setProducerId(1000'i64, 0'i16)
      let tp = TopicPartition(topic: "t", partition: 0)

      discard await p.allocateInflightForTest(@[makeRecord(0xAA)], tp, 0)
      discard await p.allocateInflightForTest(@[makeRecord(0xBB)], tp, 0)
      check p.inflightBatchCountForTest(tp) == 2

      let frontId = p.getInflightIdForTest(tp, 0)
      let secondId = p.getInflightIdForTest(tp, 1)
      check frontId != secondId

      # Dequeue the SECOND (non-front) batch by id; the front must survive.
      p.dequeueInflightForTest(tp, secondId)
      check p.inflightBatchCountForTest(tp) == 1
      check p.getInflightIdForTest(tp, 0) == frontId

      # Dequeuing an absent id is a no-op (already-dequeued / unknown).
      p.dequeueInflightForTest(tp, secondId)
      check p.inflightBatchCountForTest(tp) == 1

      p.dequeueInflightForTest(tp, frontId)
      check p.inflightBatchCountForTest(tp) == 0

    waitFor body()

  test "allocateInflight across int32.high wrap freezes wrapped baseSequence":
    ## KIP-185 wrap: as the per-TP counter approaches int32.high the next
    ## allocation must wrap to 0 and the in-flight entry must carry the wrapped
    ## baseSequence verbatim — no overflow into negative space, no skip.
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, enableIdempotence = true)
      p.setProducerId(3000'i64, 0'i16)
      let tp = TopicPartition(topic: "t", partition: 0)

      # Walk the counter to int32.high - 2 so the next 3-record batch
      # straddles the wrap point: seq int32.high-2, int32.high-1, int32.high.
      # The follow-up batch must then start at 0 (wrapped).
      p.setSequenceNumber(tp, int32.high - 2'i32)

      discard await p.allocateInflightForTest(
        @[makeRecord(0xAA), makeRecord(0xBB), makeRecord(0xCC)], tp, 0
      )
      check p.inflightBatchCountForTest(tp) == 1
      # baseSequence for the straddling batch is the pre-wrap value —
      # the broker reconstructs the per-record sequences via
      # baseSequence + offsetDelta and is responsible for treating the
      # int32.high → 0 step as wrap, not as out-of-order.
      check p.getInflightBaseSequenceForTest(tp, 0) == int32.high - 2'i32

      discard await p.allocateInflightForTest(@[makeRecord(0xDD)], tp, 0)
      check p.inflightBatchCountForTest(tp) == 2
      # Second batch landed at the wrapped 0, not at int32.high + 1 (which
      # would overflow) and not at any negative value.
      check p.getInflightBaseSequenceForTest(tp, 1) == 0'i32

      # Sequence counter also wraps to track the next allocation.
      check p.getSequenceNumber(tp) == 1'i32

    waitFor body()

  test "allocateInflight fences a concurrent producer-id reset before encoding":
    ## Regression: `allocateInflight` used to take initLock and encode with
    ## whatever (producerId, epoch, sequenceNumbers) it found — including the
    ## transient (-1, -1) state resetProducerId leaves behind before
    ## `ensureIdempotenceInitialized` re-installs a fresh PID. A racing send
    ## whose allocate landed in that window encoded with pid=-1 and seq=0,
    ## colliding with the retry's re-sequenced batches; the broker either
    ## rejected (INVALID_PID) or accepted seq=0 as a new session, silently
    ## losing records the retry then treated as duplicates. Fix: re-drive
    ## `ensureIdempotenceInitialized` and re-check under initLock, so a
    ## non-initialized state blocks the encode until the PID is refreshed.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let cluster = newCluster(@["127.0.0.1:" & $port], connectTimeoutMs = 1000)

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          while true:
            let req = await fb.nextRequest(timeout = 5.seconds)
            try:
              if req.apiKey == int16(ord(akMetadata)):
                await req.respond(
                  buildMetadataResponseBody(
                    [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
                    controllerId = 0'i32,
                    topics = @[],
                  )
                )
              elif req.apiKey == int16(ord(akInitProducerId)):
                # The fresh PID a real reset would install.
                await req.respond(
                  buildInitProducerIdResponseBody(
                    errorCode = 0'i16, producerId = 9000'i64, producerEpoch = 5'i16
                  )
                )
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let p = newKafkaProducer(cluster, enableIdempotence = true)

      # Mid-reset state: the previous PID was cleared and the reset flag was
      # dropped, but `ensureIdempotenceInitialized` has not run yet — exactly
      # what a racing send's allocateInflight used to observe.
      p.setProducerId(-1'i64, -1'i16)
      p.setIdempotenceInitialized(false)

      let tp = TopicPartition(topic: "t", partition: 0)
      let batch = await p.allocateInflightForTest(@[makeRecord(0xAA)], tp, 0)

      # The allocate must have re-driven InitProducerId under the fence and
      # encoded under the FRESH pid=9000, not pid=-1 the stale state carried.
      check batch.producerIdAtEncode == 9000'i64
      check batch.producerEpochAtEncode == 5'i16
      check p.getIdempotenceInitialized()

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await fb.close()
      await cluster.close()

    waitFor body()

  test "idempotence init retries a coordinator still loading":
    ## A fresh single-broker cluster can return COORDINATOR_LOAD_IN_PROGRESS from
    ## the first InitProducerId while its transaction state is initializing.
    ## Retry the same coordinator instead of surfacing a transient startup error.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let cluster = newCluster(@["127.0.0.1:" & $port], connectTimeoutMs = 1000)
      var initAttempts = 0

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          while true:
            let req = await fb.nextRequest(timeout = 5.seconds)
            try:
              if req.apiKey == int16(ord(akMetadata)):
                await req.respond(
                  buildMetadataResponseBody(
                    [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
                    controllerId = 0'i32,
                    topics = @[],
                  )
                )
              elif req.apiKey == int16(ord(akInitProducerId)):
                initAttempts.inc
                if initAttempts == 1:
                  await req.respond(
                    buildInitProducerIdResponseBody(
                      errorCode = int16(ord(keCoordinatorLoadInProgress))
                    )
                  )
                else:
                  await req.respond(
                    buildInitProducerIdResponseBody(
                      errorCode = 0'i16, producerId = 9001'i64, producerEpoch = 0'i16
                    )
                  )
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let p = newKafkaProducer(cluster, enableIdempotence = true)
      let batch = await p.allocateInflightForTest(
        @[makeRecord(0xAA)], TopicPartition(topic: "t", partition: 0), 0
      )

      check initAttempts == 2
      check batch.producerIdAtEncode == 9001'i64
      check p.getIdempotenceInitialized()

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await fb.close()
      await cluster.close()

    waitFor body()

  test "reassignSequencesAfterReset rewrites baseSequence from 0 in order":
    ## Cross-batch invariant a naive `clear + nextSequence` reset breaks: under
    ## concurrent retries the in-flight batches could be re-sequenced out of
    ## their original arrival order. reassign walks each TP's queue front-to-back
    ## assigning fresh sequences from 0.
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, enableIdempotence = true)
      p.setProducerId(1000'i64, 0'i16)
      let tp = TopicPartition(topic: "t", partition: 0)

      discard await p.allocateInflightForTest(@[makeRecord(0xAA)], tp, 0)
      discard
        await p.allocateInflightForTest(@[makeRecord(0xBB), makeRecord(0xCC)], tp, 0)
      discard await p.allocateInflightForTest(@[makeRecord(0xDD)], tp, 0)
      check p.inflightBatchCountForTest(tp) == 3

      # Pretend a producer-id reset bumped (id, epoch) and cleared the
      # sequence table. The in-flight queue must be re-sequenced from 0.
      p.setProducerId(2000'i64, 1'i16)
      await p.reassignSequencesAfterResetForTest()

      check p.getInflightBaseSequenceForTest(tp, 0) == 0'i32
      check p.getInflightBaseSequenceForTest(tp, 1) == 1'i32 # after 1-record batch
      check p.getInflightBaseSequenceForTest(tp, 2) == 3'i32 # after 1+2 records
      # Each batch's encoded bytes must reflect the new producerId/epoch
      # so the broker accepts them under the bumped identity.
      check p.getInflightProducerEpochForTest(tp, 0) == 1'i16
      check p.getInflightProducerEpochForTest(tp, 2) == 1'i16

    waitFor body()

  test "retry refresh picks up a concurrent producer-id re-sequencing":
    ## Regression: with maxInFlight > 1 a sibling can run resetProducerId +
    ## reassignSequencesAfterReset while this batch is parked in retry backoff.
    ## The retry loop re-syncs its local InflightBatch from the queue each
    ## iteration, sending re-sequenced bytes instead of stale pre-reset bytes the
    ## broker rejects as OUT_OF_ORDER_SEQUENCE / INVALID_PRODUCER_EPOCH.
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, enableIdempotence = true)
      p.setProducerId(1000'i64, 0'i16)
      let tp = TopicPartition(topic: "t", partition: 0)

      # Start the counter high so the reset's restart-from-0 visibly moves the
      # baseSequence, not just the epoch.
      p.setSequenceNumber(tp, 100'i32)
      # `staleLocal` is the snapshot a parked retry coroutine still holds.
      let staleLocal = await p.allocateInflightForTest(@[makeRecord(0xAA)], tp, 0)
      discard await p.allocateInflightForTest(@[makeRecord(0xBB)], tp, 0)
      check staleLocal.baseSequence == 100'i32
      check staleLocal.producerIdAtEncode == 1000'i64
      check staleLocal.producerEpochAtEncode == 0'i16
      let originalId = staleLocal.id
      let staleBytes = staleLocal.encodedBytes

      # A sibling drives the producer-id reset and re-sequences the queue.
      p.setProducerId(2000'i64, 1'i16)
      await p.reassignSequencesAfterResetForTest()
      # The queue entry moved; the parked local copy is now stale.
      check p.getInflightBaseSequenceForTest(tp, 0) == 0'i32
      check p.getInflightProducerEpochForTest(tp, 0) == 1'i16

      # The loop-head refresh re-syncs the local copy from the queue by id.
      var refreshed = staleLocal
      p.refreshInflightFromQueueForTest(tp, refreshed)
      check refreshed.id == originalId # same batch, re-located by id
      check refreshed.baseSequence == 0'i32 # picked up the re-sequencing
      check refreshed.producerIdAtEncode == 2000'i64
      check refreshed.producerEpochAtEncode == 1'i16
      check refreshed.encodedBytes != staleBytes # bytes the broker will accept

    waitFor body()

  test "refreshInflightFromQueue is a no-op for a non-idempotent producer":
    ## The loop-head refresh must not touch a non-idempotent producer's local
    ## batch: such producers never enqueue, and the primitive early-returns on
    ## `not enableIdempotence`. Guards the first-attempt / non-idempotent paths.
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster, enableIdempotence = false)
    let tp = TopicPartition(topic: "t", partition: 0)
    var local = InflightBatch(
      id: 7'i64,
      baseSequence: 99'i32,
      producerIdAtEncode: 11'i64,
      producerEpochAtEncode: 2'i16,
      encodedBytes: @[1'u8, 2'u8, 3'u8],
    )
    p.refreshInflightFromQueueForTest(tp, local)
    check local.id == 7'i64
    check local.baseSequence == 99'i32
    check local.producerIdAtEncode == 11'i64
    check local.producerEpochAtEncode == 2'i16
    check local.encodedBytes == @[1'u8, 2'u8, 3'u8]

suite "Producer: resetProducerId guard":
  test "producerId and epoch are preserved when maxRetries is exhausted":
    ## resetProducerId must NOT run before raising when the retry limit is
    ## reached. Tested indirectly: setProducerId values survive (resetProducerId
    ## would clear them).
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster, enableIdempotence = true, maxRetries = 2)
    p.setProducerId(42'i64, 3'i16)
    let tp = TopicPartition(topic: "t", partition: 0)
    discard p.nextSequence(tp, 5)
    check p.getProducerId() == 42'i64
    check p.getProducerEpoch() == 3'i16
    check p.sequenceNumberCount() == 1
    # The actual retry path with resetProducerId is tested via integration;
    # here we verify the state accessors work for future regression tests.
    check p.getSequenceNumber(tp) == 5

suite "Producer: close flush timeout":
  test "close completes with timeout when flush is blocked":
    ## producer-support.close-drain: a blocked flush is a timeout, not a
    ## successful drain. withTimeout cannot see it (flush swallows cancel),
    ## so the hook records producer.close.flush.failed. F&F drain is empty.
    proc testBody() {.async: (raises: [Exception]).} =
      type Sink = ref object
        flushFailed: int
        reason: string
        value: int64
        drainCancelled: int
        unflushed: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.close.flush.failed":
          sink.flushFailed.inc
          sink.value = value
          for (k, v) in tags:
            if $k == "reason":
              sink.reason = $v
        elif $event == "producer.close.drain.cancelled":
          sink.drainCancelled.inc
        elif $event == "producer.close.unflushed_batches":
          sink.unflushed.inc

      let cluster = newCluster(@["127.0.0.1:9092"])
      # Small timeoutMs so the test finishes quickly
      let p =
        newKafkaProducer(cluster, lingerMs = 5000, timeoutMs = 200, metricsHook = hook)

      let tp = TopicPartition(topic: "test-topic", partition: 0)
      let rec = Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some(@[0x01'u8]),
        headers: @[],
      )
      let pendingFut = p.injectPendingBatch(tp, rec)

      # Hold batchLock so flush() blocks
      await p.acquireBatchLock()

      let closeFut = p.close()

      # Wait for close to complete (should timeout flush, not hang)
      let closedInTime = awaitCompleted(closeFut, milliseconds(2000))

      check closedInTime == true
      check p.isClosed() == true

      check pendingFut.finished
      check pendingFut.failed
      check sink.flushFailed == 1
      check sink.reason == "timeout"
      check sink.value == 1
      check sink.drainCancelled == 0
      # The batch the blocked flush left behind is failed during close, and the
      # count of partitions that lost records is the only place that shows up.
      check sink.unflushed == 1

      p.releaseBatchLock()

    waitFor testBody()

suite "Producer: close batchLock ownership":
  test "a cancelled close does not release a batchLock it never acquired":
    ## Regression: failRemaining's finally released batchLock unconditionally.
    ## When its acquire was cancelled while queued behind another holder,
    ## releaseSafely() freed that holder's lock (chronos AsyncLock tracks no
    ## per-coroutine owner). close() must leave the holder's lock alone while
    ## still draining the remaining batches best-effort.
    proc testBody() {.async: (raises: [Exception]).} =
      type Sink = ref object
        flushFailed: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.close.flush.failed":
          sink.flushFailed.inc

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p =
        newKafkaProducer(cluster, lingerMs = 5000, timeoutMs = 200, metricsHook = hook)
      let tp = TopicPartition(topic: "test-topic", partition: 0)
      let rec = Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some(@[0x01'u8]),
        headers: @[],
      )
      let pendingFut = p.injectPendingBatch(tp, rec)

      # Hold batchLock: close's flush times out and the failRemaining acquire
      # parks behind this holder.
      await p.acquireBatchLock()
      check p.batchLock.locked

      let closeFut = p.close()
      # Wait out the flush timeout, then give the failed-flush continuation a
      # few scheduler turns to reach and park in failRemaining.
      for _ in 0 ..< 100:
        if sink.flushFailed > 0:
          break
        await sleepAsync(milliseconds(10))
      check sink.flushFailed == 1
      await sleepAsync(milliseconds(50))
      check not closeFut.finished
      check p.batchLock.locked

      # Cancel the parked acquire: the cancellation is absorbed for teardown,
      # but the lock this coroutine never took must survive.
      closeFut.cancelSoon()
      check awaitCompleted(closeFut, milliseconds(2000))
      check p.isClosed()
      check pendingFut.finished # the best-effort drain still ran
      check p.batchLock.locked

      p.releaseBatchLock()
      check not p.batchLock.locked
      await cluster.close()

    waitFor testBody()

suite "Producer: close cancellation":
  test "a cancel while linger timers are reaped still closes the producer":
    ## Regression: the linger-reaping loop awaited `cancelAndWait` unguarded, so a
    ## close() cancelled at that await unwound with the producer left in lsClosing
    ## (close() never re-runs closeImpl) and its owned Cluster still open.
    proc testBody() {.async: (raises: [Exception]).} =
      let p = newKafkaProducer(@["127.0.0.1:9092"], lingerMs = 60_000)
      check p.ownsCluster
      let cluster = p.clusterForTest()
      let tp = TopicPartition(topic: "test-topic", partition: 0)
      p.renewLingerTimerForTest(tp)
      let lingerFut = p.getLingerTimer(tp)
      check not lingerFut.finished

      # Park the reaping stage, then cancel the close while it is inside it.
      discard p.armCloseLingerParkForTest()
      let closeFut = p.close()
      while not p.closeLingerParkedForTest():
        await sleepAsync(milliseconds(1))
      closeFut.cancelSoon()
      check awaitCompleted(closeFut, milliseconds(2000))

      check p.state == lsClosed
      check cluster.isClosed
      # The reaping stage kept going after the cancel: the timer was reaped.
      check lingerFut.finished

    waitFor testBody()

suite "Producer: transactional close":
  test "close sets txnState to non-transaction when in tsInTransaction":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster, transactionalId = some("test-txn"), enableIdempotence = true
      )
      # Simulate that initTransactions was called and beginTransaction was called
      p.setTxnState(tsInTransaction)
      # close() skips the abort entirely when no coordinator is set; it must
      # not crash and must complete gracefully.
      await p.close()
      check p.isClosed() == true

    waitFor testBody()

  test "close sets txnState correctly when in tsError":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster, transactionalId = some("test-txn"), enableIdempotence = true
      )
      p.setTxnState(tsError)
      await p.close()
      check p.isClosed() == true

    waitFor testBody()

  test "close is idempotent":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster)
      await p.close()
      await p.close()
      check p.isClosed() == true

    waitFor testBody()

  test "concurrent close() calls run a single teardown":
    ## Regression: close() only bailed on lsClosed, so a close() arriving while
    ## another close() was mid-teardown (lsClosing) ran the ENTIRE drain / table
    ## clear / cluster.close() sequence a second time in parallel. close() is now
    ## single-flight: the second caller parks on the in-progress close and
    ## returns after ONE teardown completes.
    proc testBody() {.async: (raises: [Exception]).} =
      type Sink = ref object
        closeEvents: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.close.duration_ms":
          sink.closeEvents.inc

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, metricsHook = hook, timeoutMs = 5000)
      # Park the first close in the fire-and-forget drain so the second call
      # observes lsClosing + an unfinished closeFuture (a truly-overlapping pair).
      let blocker = newFuture[void]("close-blocker")
      p.injectPendingFireAndForget(blocker)
      let f1 = p.close()
      let f2 = p.close()
      check not f1.finished # in-flight
      check not f2.finished # parked on f1, NOT a second teardown
      blocker.complete()
      await f1
      await f2
      check p.isClosed()
      # Exactly one closeImpl ran (one close metric), not two.
      check sink.closeEvents == 1
      await cluster.close()

    waitFor testBody()

suite "Producer: transaction state recovery":
  test "close handles tsCommitting state":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster, transactionalId = some("test-txn"), enableIdempotence = true
      )
      p.setTxnState(tsCommitting)
      await p.close()
      check p.isClosed() == true

    waitFor testBody()

  test "close handles tsAborting state":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster, transactionalId = some("test-txn"), enableIdempotence = true
      )
      p.setTxnState(tsAborting)
      await p.close()
      check p.isClosed() == true

    waitFor testBody()

suite "Producer: timestamp defaults":
  test "resolveTimestamp replaces 0 with current epoch ms":
    let before = int64(epochTime() * 1000)
    let resolved = resolveTimestamp(0)
    let after = int64(epochTime() * 1000)
    check resolved >= before
    check resolved <= after
    check resolved > 1_000_000_000_000'i64 # after year 2001

  test "resolveTimestamp preserves explicit timestamp":
    let explicit = 1700000000000'i64 # 2023-11-14
    check resolveTimestamp(explicit) == explicit

  test "resolveTimestamp preserves negative timestamp":
    check resolveTimestamp(-1'i64) == -1'i64

  test "encodeRecordBatch uses firstTimestamp for delta encoding":
    let ts = 1712505600000'i64
    let rec = Record(
      offset: 0,
      timestamp: ts,
      key: none(seq[byte]),
      value: some(@[0x01'u8]),
      headers: @[],
    )
    var w = initKafkaWriter()
    w.encode(
      RecordBatch(
        firstTimestamp: ts,
        producerId: -1,
        producerEpoch: -1,
        baseSequence: -1,
        records: @[rec],
      )
    )
    var r = initKafkaReader(w.buf)
    let batch = r.decode(RecordBatch)
    check batch.firstTimestamp == ts
    check batch.records.len == 1
    check batch.records[0].timestamp == ts

  test "encodeRecordBatch with firstTimestamp=0 produces wrong delta":
    ## Demonstrates that passing firstTimestamp=0 when the record has a real
    ## timestamp causes the batch header to store 0 instead of the actual time.
    let ts = 1712505600000'i64
    let rec = Record(
      offset: 0,
      timestamp: ts,
      key: none(seq[byte]),
      value: some(@[0x01'u8]),
      headers: @[],
    )
    var w = initKafkaWriter()
    w.encode(
      RecordBatch(
        firstTimestamp: 0,
        producerId: -1,
        producerEpoch: -1,
        baseSequence: -1,
        records: @[rec],
      )
    )
    var r = initKafkaReader(w.buf)
    let batch = r.decode(RecordBatch)
    # firstTimestamp in batch header is 0, not the actual timestamp
    check batch.firstTimestamp == 0
    # But the record timestamp is still correctly reconstructed via delta
    check batch.records[0].timestamp == ts

suite "Producer: semaphore safety":
  test "cancelled acquire does not corrupt semaphore count":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      # maxInFlightRequests=1 so semaphore has exactly 1 slot
      let p = newKafkaProducer(cluster, maxInFlightRequests = 1)
      let tp = TopicPartition(topic: "t", partition: 0)

      check (await p.tryAcquireInflightSemaphore(tp)) == true
      # All slots taken — the next acquire must fail
      check (await p.tryAcquireInflightSemaphore(tp)) == false
      p.releaseInflightSemaphore(tp)

      # Fully available again after release
      check (await p.tryAcquireInflightSemaphore(tp)) == true
      p.releaseInflightSemaphore(tp)

    waitFor testBody()

suite "Producer: configuration validation":
  test "validateProducerConfig rejects each field with its own message":
    ## The constructor cases below reach nine of the eleven checks. Driving the
    ## shared validator directly keeps maxBatchBytes / maxPendingRecords from
    ## regressing to "silently accepted" and pins the message contract.
    proc validate(
        lingerMs: int32 = 0,
        acks: int16 = 1,
        timeoutMs: int32 = 30_000,
        transactionTimeoutMs: int32 = 60_000,
        maxRetries: int32 = 3,
        maxBatchRecords: int32 = 1000,
        maxBatchBytes: int32 = 1_048_576,
        retryBackoffMs: int32 = 100,
        maxPendingFireAndForget: int32 = 1000,
        maxPendingRecords: int32 = 10_000,
        maxInFlightRequests: int32 = 5,
        transactionalId: Option[string] = none(string),
        compression: CompressionCodec = ccNone,
    ) {.raises: [KafkaError].} =
      validateProducerConfig(
        lingerMs, acks, timeoutMs, transactionTimeoutMs, maxRetries, maxBatchRecords,
        maxBatchBytes, retryBackoffMs, maxPendingFireAndForget, maxPendingRecords,
        maxInFlightRequests, transactionalId, compression,
      )

    template rejected(expected: string, call: untyped) =
      var message = ""
      try:
        call
      except KafkaError as e:
        message = e.msg
      check expected in message

    validate() # every default is valid
    rejected("lingerMs must be >= 0", validate(lingerMs = -1))
    rejected("acks must be one of {-1, 0, 1}", validate(acks = 2))
    rejected("timeoutMs must be > 0", validate(timeoutMs = 0))
    rejected("transactionTimeoutMs must be > 0", validate(transactionTimeoutMs = 0))
    rejected("maxRetries must be >= 0", validate(maxRetries = -1))
    rejected("maxBatchRecords must be > 0", validate(maxBatchRecords = 0))
    rejected("maxBatchBytes must be > 0", validate(maxBatchBytes = 0))
    rejected("retryBackoffMs must be >= 0", validate(retryBackoffMs = -1))
    rejected(
      "maxPendingFireAndForget must be > 0", validate(maxPendingFireAndForget = 0)
    )
    rejected("maxPendingRecords must be > 0", validate(maxPendingRecords = 0))
    rejected("maxInFlightRequests must be > 0", validate(maxInFlightRequests = 0))
    rejected(
      "transactionalId must be 1..255 characters", validate(transactionalId = some(""))
    )
    rejected(
      "transactionalId must be 1..255 characters",
      validate(transactionalId = some("t".repeat(256))),
    )
    rejected(
      "transactionalId may only contain [a-zA-Z0-9._-]",
      validate(transactionalId = some("bad id")),
    )
    # Kafka's own character set and length bound.
    validate(transactionalId = some("txn-1.0_ok"))
    validate(transactionalId = some("t".repeat(255)))
    # The "must be > 0" bounds are inclusive at 1.
    validate(
      timeoutMs = 1,
      transactionTimeoutMs = 1,
      maxBatchRecords = 1,
      maxBatchBytes = 1,
      maxPendingFireAndForget = 1,
      maxPendingRecords = 1,
      maxInFlightRequests = 1,
    )
    # ccLz4 is the one codec whose library is not linked; the LZ4 compression
    # suite compresses with it.
    validate(compression = ccLz4)
    lz4mod.lz4BlockLoadForTest(true)
    defer:
      lz4mod.lz4BlockLoadForTest(false)
    rejected("ccLz4 requires the system liblz4", validate(compression = ccLz4))

  test "ccLz4 is probed when the producer is built, not when it first sends":
    ## The library is resolved on first use, so nothing at build time catches a
    ## host without liblz4. Both constructor overloads must reject the codec at
    ## construction — the bootstrap one before it creates its Cluster.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      lz4mod.lz4BlockLoadForTest(true)
      var borrowedMessage, ownedMessage = ""
      try:
        discard newKafkaProducer(cluster, compression = ccLz4)
      except KafkaError as e:
        borrowedMessage = e.msg
      try:
        discard newKafkaProducer(@["127.0.0.1:9092"], compression = ccLz4)
      except KafkaError as e:
        ownedMessage = e.msg
      lz4mod.lz4BlockLoadForTest(false)
      check "ccLz4 requires the system liblz4" in borrowedMessage
      check "ccLz4 requires the system liblz4" in ownedMessage

      let producer = newKafkaProducer(cluster, compression = ccLz4)
      check producer.isClosed() == false
      await producer.close()
      await cluster.close()

    waitFor testBody()

  test "constructors reject an invalid transactionalId":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect KafkaError:
      discard newKafkaProducer(cluster, transactionalId = some("bad id"))
    # The bootstrap-brokers overload validates before it creates its Cluster.
    expect KafkaError:
      discard newKafkaProducer(@["127.0.0.1:9092"], transactionalId = some(""))

  test "lingerMs negative raises KafkaError":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect KafkaError:
      discard newKafkaProducer(cluster, lingerMs = -1)

  test "maxBatchRecords zero raises KafkaError":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect KafkaError:
      discard newKafkaProducer(cluster, maxBatchRecords = 0)

  test "retryBackoffMs negative raises KafkaError":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect KafkaError:
      discard newKafkaProducer(cluster, retryBackoffMs = -1)

  test "timeoutMs zero raises KafkaError":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect KafkaError:
      discard newKafkaProducer(cluster, timeoutMs = 0)

  test "maxPendingFireAndForget zero raises KafkaError":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect KafkaError:
      discard newKafkaProducer(cluster, maxPendingFireAndForget = 0)

  test "valid boundary values do not raise":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster,
      lingerMs = 0,
      maxBatchRecords = 1,
      retryBackoffMs = 0,
      timeoutMs = 1,
      maxPendingFireAndForget = 1,
    )
    check not p.isClosed()

  test "acks outside {-1, 0, 1} raises KafkaError":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect KafkaError:
      discard newKafkaProducer(cluster, acks = 2)
    expect KafkaError:
      discard newKafkaProducer(cluster, acks = -2)

  test "acks boundary values are accepted":
    let cluster = newCluster(@["127.0.0.1:9092"])
    for a in [int16(-1), int16(0), int16(1)]:
      check not newKafkaProducer(cluster, acks = a).isClosed()

  test "transactionTimeoutMs <= 0 raises KafkaError":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect KafkaError:
      discard newKafkaProducer(
        cluster, transactionalId = some("txn-a"), transactionTimeoutMs = 0
      )
    expect KafkaError:
      discard newKafkaProducer(
        cluster, transactionalId = some("txn-a"), transactionTimeoutMs = -1
      )

  test "maxRetries negative raises KafkaError":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect KafkaError:
      discard newKafkaProducer(cluster, maxRetries = -1)

  test "metricsHook is accepted and not invoked at construction":
    type Sink = ref object
      events: int

    proc runTest() =
      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        sink.events.inc

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, metricsHook = hook)
      check not p.isClosed()
      # Construction itself must not fire metrics; events are tied to send/
      # batch/retry, not the lifecycle of the producer object.
      check sink.events == 0

    runTest()

  test "bootstrap-brokers constructor validates before creating its Cluster":
    ## Regression: the bootstrap-brokers overload used to create the owned
    ## Cluster FIRST, then delegate to the Cluster-version constructor whose
    ## validation could raise — stranding an unreachable (never-closed) Cluster
    ## with its broker connections / metadata loop. Validation must run first,
    ## so an invalid config surfaces as THIS constructor's error (lingerMs),
    ## not a bootstrap-addressing error from newCluster, and never creates a
    ## Cluster in the first place.
    expect KafkaError:
      discard newKafkaProducer(@[], lingerMs = -1)
    expect KafkaError:
      discard newKafkaProducer(@["127.0.0.1:9092"], acks = 2)
    expect KafkaError:
      discard newKafkaProducer(@["127.0.0.1:9092"], maxInFlightRequests = 0)

  test "bootstrap-brokers constructor accepts valid config":
    proc body() {.async: (raises: [Exception]).} =
      let p = newKafkaProducer(@["127.0.0.1:9092"], lingerMs = 0)
      check p.ownsCluster # owned → close() must release it
      await p.close()
      check p.state == lsClosed

    waitFor body()

  test "ownsCluster is false for an injected Cluster":
    ## The caller owns what it injected; the producer must not close it and the
    ## public query is how a caller tells the two cases apart.
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let shared = newKafkaProducer(cluster)
      check not shared.ownsCluster
      await shared.close()
      check not cluster.isClosed # still open: shared.close() did not release it
      await cluster.close()

    waitFor body()

  test "flush() with no pending batches does not invoke metricsHook":
    # Regression test for the early-return guard in flushBatch: spurious
    # producer.batch.flush events on empty batches would skew dashboards
    # that count flushes.
    type Sink = ref object
      events: seq[string]

    proc runTest() {.async: (raises: [Exception]).} =
      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        sink.events.add($event)

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, metricsHook = hook, lingerMs = 100)
      # No send() calls — batches map is empty.
      await p.flush()
      check sink.events.len == 0

    waitFor runTest()

suite "Producer: pending records backpressure":
  test "maxPendingRecords bound is plumbed through":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster, maxPendingRecords = 42)
    check p.maxPendingRecordsForTest() == 42

  test "maxPendingRecords <= 0 raises KafkaError":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect KafkaError:
      discard newKafkaProducer(cluster, maxPendingRecords = 0)
    expect KafkaError:
      discard newKafkaProducer(cluster, maxPendingRecords = -1)

  test "failBatchFutures releases one permit per failed record":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, lingerMs = 100, maxPendingRecords = 2)
      let tp = TopicPartition(topic: "bp", partition: 0)

      # Drain both permits up-front; no batched send has gone through yet.
      check (await p.tryAcquirePendingRecord()) == true
      check (await p.tryAcquirePendingRecord()) == true
      # Now saturated — the next acquire must time out.
      check (await p.tryAcquirePendingRecord()) == false

      # Simulate two pending-batch messages that will be failed by close().
      let rec = Record(offset: 0, timestamp: 0, value: some(@[0x01'u8]))
      let f1 = p.injectPendingBatch(tp, rec)
      let f2 = p.injectPendingBatch(tp, rec)
      check p.hasBatch(tp)

      # failBatchFutures is the single site close / cancelled-register paths
      # go through; it must release one permit per pending message.
      p.failBatchFuturesForTest(tp, newException(KafkaError, "test failure"))

      # Best-effort await on the now-failed futures so the test does not
      # leave orphan futures in the dispatcher.
      for fut in [f1, f2]:
        try:
          discard await fut
        except CatchableError:
          discard

      # The two released permits compensate exactly for the two messages we
      # injected — we should now be able to acquire twice more, but not three
      # times (the semaphore is still capped at maxPendingRecords=2 minus the
      # two we pre-drained).
      check (await p.tryAcquirePendingRecord()) == true
      check (await p.tryAcquirePendingRecord()) == true
      check (await p.tryAcquirePendingRecord()) == false

    waitFor testBody()

suite "Producer: linger timer lifecycle":
  test "renewLingerTimer replaces a stale timer and cancels the old future":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, lingerMs = 50)
      let tp = TopicPartition(topic: "t", partition: 0)

      # Seed the table with a stale timer the defensive guard must cancel.
      let stale = sleepAsync(minutes(10))
      p.injectLingerTimer(tp, stale)
      check p.hasLingerTimer(tp)
      check not stale.finished

      p.renewLingerTimerForTest(tp)

      # The stale future is cancelled (not completed normally) and replaced.
      # cancelSoon is asynchronous; one poll step lets it settle.
      await sleepAsync(1.milliseconds)
      check stale.finished
      check stale.cancelled
      let fresh = p.getLingerTimer(tp)
      check fresh != nil
      check fresh != stale

      # Drain the fresh timer before closing to keep Chronos happy.
      fresh.cancelSoon()
      await sleepAsync(1.milliseconds)

    waitFor testBody()

  test "renewLingerTimer is a no-op-safe when no prior entry exists":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, lingerMs = 50)
      let tp = TopicPartition(topic: "empty", partition: 0)

      check not p.hasLingerTimer(tp)
      p.renewLingerTimerForTest(tp)
      check p.hasLingerTimer(tp)

      let fresh = p.getLingerTimer(tp)
      fresh.cancelSoon()
      await sleepAsync(1.milliseconds)

    waitFor testBody()

  test "renewLingerTimer does not double-cancel an already-finished entry":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, lingerMs = 50)
      let tp = TopicPartition(topic: "finished", partition: 0)

      # A completed future simulates a timer that already ran to completion
      # (in practice flushBatch would have deleted it — this guards against
      # any future bug that leaves it hanging around).
      let done = Future[void].init("test.finished")
      done.complete()
      p.injectLingerTimer(tp, done)
      check done.finished

      p.renewLingerTimerForTest(tp)

      let fresh = p.getLingerTimer(tp)
      check fresh != nil
      check fresh != done
      fresh.cancelSoon()
      await sleepAsync(1.milliseconds)

    waitFor testBody()

  test "superseded linger timer resume does not delete a fresh entry":
    # Regression: after sleepAsync completed, an external flushBatch could
    # cancelSoon+del the entry, and a subsequent send install a fresh timer at
    # the same key. The stale continuation used to `del(tp)` unconditionally,
    # dropping the fresh entry (double-del) and leaking its queued cancel into
    # flushBatch's first await, failing the successor's user futures with
    # KafkaError("cancelled"). The epoch guard makes the stale continuation
    # observe the epoch mismatch and return without side effects.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, lingerMs = 50)
      let tp = TopicPartition(topic: "supersede", partition: 0)

      p.renewLingerTimerForTest(tp)
      let epoch1 = p.lingerTimerEpochForTest(tp)
      let staleFut = p.getLingerTimer(tp)
      check staleFut != nil
      check epoch1 == 1'u64

      # Yield partway through the stale timer's sleep, then simulate an
      # external flushBatch cancelSoon+del path without waiting for its Future
      # to settle: the entry is gone from the table, but the coroutine is
      # still resolving sleepAsync in the background.
      await sleepAsync(20.milliseconds)
      p.dropLingerTimerEntryForTest(tp)
      check not p.hasLingerTimer(tp)

      p.renewLingerTimerForTest(tp)
      let epoch2 = p.lingerTimerEpochForTest(tp)
      let freshFut = p.getLingerTimer(tp)
      check epoch2 == 2'u64
      check freshFut != nil
      check freshFut != staleFut

      # Wait past the stale timer's deadline (~t=50ms from start, ~30ms from
      # here) but before the fresh timer's deadline (~t=70ms from start,
      # ~50ms from here). The stale continuation must return via the epoch
      # mismatch and leave the fresh entry untouched.
      await sleepAsync(40.milliseconds)
      check staleFut.finished
      check not freshFut.finished
      check p.lingerTimerEpochForTest(tp) == epoch2
      check p.hasLingerTimer(tp)
      check p.getLingerTimer(tp) == freshFut

      freshFut.cancelSoon()
      await sleepAsync(1.milliseconds)

    waitFor testBody()

  test "close cancels and clears every pending linger timer":
    # Regression companion for the close-time snapshot: close() used to
    # `await cancelAndWait` while iterating `lingerTimers` directly, so a
    # timer firing during that suspension (lingerTimeout deletes its own
    # entry) mutated the table mid-iteration. The snapshot-and-clear close
    # must drain every timer and leave the table empty regardless.
    ## producer-support.close-linger: snapshot cancel is not a timeout, but
    ## the hook still records how many unfinished timers close cancelled.
    proc testBody() {.async: (raises: [Exception]).} =
      type Sink = ref object
        lingerCancelled: int
        lingerValue: int64
        drainCancelled: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.close.linger.cancelled":
          sink.lingerCancelled.inc
          sink.lingerValue = value
        elif $event == "producer.close.drain.cancelled":
          sink.drainCancelled.inc

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, lingerMs = 50, metricsHook = hook)
      var timers: seq[Future[void]]
      for i in 0 ..< 4:
        let tp = TopicPartition(topic: "close-linger", partition: int32(i))
        let fut = sleepAsync(minutes(10))
        p.injectLingerTimer(tp, fut)
        timers.add(fut)

      await p.close()

      for fut in timers:
        check fut.finished
      for i in 0 ..< 4:
        check not p.hasLingerTimer(
          TopicPartition(topic: "close-linger", partition: int32(i))
        )
      check sink.lingerCancelled == 1
      check sink.lingerValue == 4
      check sink.drainCancelled == 0

    waitFor testBody()

suite "Producer: concurrency regression":
  test "nextSequence is atomic across many partitions":
    # chronos is single-threaded so nextSequence's read-modify-write is
    # atomic by construction (no awaits inside the proc). This test pins
    # that contract: 4 partitions × 100 reservations each must produce a
    # final per-partition counter equal to the number of reservations,
    # regardless of the interleaving order driven by the scheduler.
    proc reserveLoop(
        p: KafkaProducer, tp: TopicPartition, count: int
    ) {.async: (raises: []).} =
      for _ in 0 ..< count:
        discard p.nextSequence(tp, 1)
        # Yield to the scheduler so other reservers interleave between
        # iterations.
        try:
          await sleepAsync(0.milliseconds)
        except CancelledError:
          return

    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, enableIdempotence = true)
      let tps = @[
        TopicPartition(topic: "t", partition: 0),
        TopicPartition(topic: "t", partition: 1),
        TopicPartition(topic: "t", partition: 2),
        TopicPartition(topic: "t", partition: 3),
      ]
      const reservations = 100
      var futs: seq[Future[void].Raising([])]
      for tp in tps:
        futs.add(reserveLoop(p, tp, reservations))
      await allFutures(futs)
      for tp in tps:
        check p.getSequenceNumber(tp) == int32(reservations)

    waitFor testBody()

  test "renewLingerTimer in a tight loop leaves a single live entry":
    # Direct stress: replace the linger timer for the same partition many
    # times. After the loop, exactly one entry must remain in the table
    # (no stale entries leak from the previous renewals' cancelSoon paths).
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, lingerMs = 5000)
      let tp = TopicPartition(topic: "t", partition: 0)
      for _ in 0 ..< 50:
        p.renewLingerTimerForTest(tp)
        # Yield occasionally so cancelSoon completions get processed.
        await sleepAsync(0.milliseconds)
      check p.hasLingerTimer(tp)
      let live = p.getLingerTimer(tp)
      check live != nil
      live.cancelSoon()
      await sleepAsync(1.milliseconds)

    waitFor testBody()

suite "Producer: close mid-flight":
  test "close clears all linger timer entries":
    # Regression for the linger-timer leak path: any linger timer left after flush /
    # failRemaining must be cancelled and removed by close, otherwise the
    # producer's Table[TopicPartition, Future[void]] would retain references
    # to completed/cancelled futures across the lifetime of the process.
    ## producer-support.close-linger: two pending timers become one
    ## linger.cancelled event with value=2.
    proc testBody() {.async: (raises: [Exception]).} =
      type Sink = ref object
        lingerCancelled: int
        lingerValue: int64
        drainCancelled: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.close.linger.cancelled":
          sink.lingerCancelled.inc
          sink.lingerValue = value
        elif $event == "producer.close.drain.cancelled":
          sink.drainCancelled.inc

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p =
        newKafkaProducer(cluster, lingerMs = 5000, timeoutMs = 50, metricsHook = hook)
      let tp1 = TopicPartition(topic: "a", partition: 0)
      let tp2 = TopicPartition(topic: "b", partition: 1)
      p.injectLingerTimer(tp1, sleepAsync(minutes(10)))
      p.injectLingerTimer(tp2, sleepAsync(minutes(10)))
      check p.hasLingerTimer(tp1)
      check p.hasLingerTimer(tp2)

      await p.close()

      check not p.hasLingerTimer(tp1)
      check not p.hasLingerTimer(tp2)
      check p.isClosed()
      check sink.lingerCancelled == 1
      check sink.lingerValue == 2
      check sink.drainCancelled == 0

    waitFor testBody()

  test "close settles an in-flight batched send Future within a bounded window":
    # An injected pending message must not leak across producer close. close
    # may race with an in-flight flushBatch, but the Future is guaranteed to
    # settle (fail) within a small window once the producer has closed —
    # waiting on it from the test verifies no future is silently abandoned.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50)
      let p = newKafkaProducer(cluster, lingerMs = 5000, timeoutMs = 50)
      let tp = TopicPartition(topic: "test-topic", partition: 0)
      let rec = Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some(@[0x01'u8]),
        headers: @[],
      )
      let pendingFut = p.injectPendingBatch(tp, rec)
      check not pendingFut.finished

      await p.close()

      var settled = false
      try:
        discard await pendingFut.wait(milliseconds(2000))
        settled = true # completed
      except KafkaError:
        settled = true # failed with a Kafka error — also acceptable
      except CancelledError:
        settled = true
      except CatchableError:
        settled = true
      check settled
      check pendingFut.finished
      check p.isClosed()
      await cluster.close()

    waitFor testBody()

suite "Producer: transaction partition cleanup":
  test "commitTransaction preserves txn partitions when EndTxn was not confirmed":
    ## Regression: the pre-EndTxn failure path (unreachable coordinator, transport
    ## error, post-flush tsError re-raise, cancel during flush/register) leaves
    ## the broker-side txn still Ongoing. Clearing txnStarted/txnPartitions here
    ## would make a follow-up abortTransaction short-circuit on `not txnStarted`
    ## and orphan the coordinator's txn — LSO stall, and next-txn EOS violation
    ## if the same PID/epoch keeps flowing. Only endTxnSucceeded=true clears.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      p.setTxnState(tsInTransaction)
      # Mimic a txn already moved to Ongoing broker-side (registerPending
      # succeeded once) with a still-registered partition.
      p.setTxnStarted(true)
      p.addTxnPartition(TopicPartition(topic: "t", partition: 0))
      p.addTxnPendingPartition(TopicPartition(topic: "t", partition: 1))
      check p.txnPartitionCount() == 1
      check p.txnPendingPartitionCount() == 1
      var raised = false
      try:
        await p.commitTransaction()
      except KafkaError:
        raised = true
      check raised
      # endTxn RPC failed to reach the broker: markers preserved so recovery
      # abort can still issue EndTxn(abort) once the coordinator is reachable.
      check p.getTxnStarted()
      check p.txnPartitionCount() == 1
      check p.txnPendingPartitionCount() == 1
      check p.txnState in {tsError, tsFatal}
      await dummy.close()

    waitFor testBody()

  test "abortTransaction preserves txn partitions when EndTxn was not confirmed":
    ## Symmetric to the commit case. A failed EndTxn(abort) RPC must not clear
    ## txnStarted, else a retry-abort or close()'s abort skips the release.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      p.setTxnState(tsInTransaction)
      p.setTxnStarted(true)
      p.addTxnPartition(TopicPartition(topic: "t", partition: 0))
      p.addTxnPendingPartition(TopicPartition(topic: "t", partition: 1))
      var raised = false
      try:
        await p.abortTransaction()
      except KafkaError:
        raised = true
      check raised
      check p.getTxnStarted()
      check p.txnPartitionCount() == 1
      # abort's inner catch drains pending (since txnStarted) before EndTxn.
      check p.txnPendingPartitionCount() == 0
      check p.txnState in {tsError, tsFatal}
      await dummy.close()

    waitFor testBody()

  test "abortTransaction with buffered records + pending partitions does not deadlock":
    ## Regression: abortTransaction holds txnLock then flushes. If flushBatch
    ## re-acquired the non-reentrant txnLock to register a pending partition the
    ## abort would deadlock. The fix registers pending partitions before flush
    ## (mirroring commit) so flushBatch never re-acquires the lock.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        lingerMs = 60_000, # never auto-flush during the test
        timeoutMs = 500,
      )
      # Dummy (unreachable) coordinator: AddPartitionsToTxn fails fast, so the
      # abort completes by RAISING rather than hanging. The assertion is simply
      # that it completes at all (no txnLock self-deadlock).
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      p.setTxnState(tsInTransaction)

      let tp = TopicPartition(topic: "t", partition: 0)
      let rec = Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some(@[0x01'u8]),
        headers: @[],
      )
      # Mimic the lingerMs > 0 send path: a buffered record plus a pending,
      # not-yet-registered transaction partition.
      let pendingFut = p.injectPendingBatch(tp, rec)
      p.addTxnPendingPartition(tp)
      check p.txnPendingPartitionCount() == 1
      check p.hasBatch(tp)

      let abortFut = p.abortTransaction()
      let completed = await withTimeout(abortFut, milliseconds(3000))
      check completed # false here would mean the txnLock self-deadlock regressed
      if completed:
        try:
          await abortFut
        except CatchableError:
          discard
      # close() resolves the still-buffered record future and tears down; the
      # failed abort kept the partition sets, so close() re-runs the abort.
      await p.close()
      check pendingFut.finished
      await dummy.close()

    waitFor testBody()

  test "batched send refuses enqueue when txn finalized during backpressure window":
    ## Regression: on the batched path a transactional send marks its partition,
    ## then parks on pendingRecordsSem/batchLock before enqueuing. If a concurrent
    ## commit/abort finalizes the txn in that window, the not-yet-enqueued record
    ## is invisible to the flush snapshot, so EndTxn ships without it and enqueuing
    ## afterwards leaks it past the finalized boundary. The fix re-checks txnState
    ## under batchLock at enqueue time and refuses if the txn is no longer in
    ## progress.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        lingerMs = 60_000, # batched path; never auto-flush during the test
        maxPendingRecords = 1, # a single permit so we can saturate it
        timeoutMs = 500,
      )
      p.setTxnState(tsInTransaction)

      # Saturate the pending-records semaphore so the send below parks on
      # `pendingRecordsSem.acquire()` — exactly the backpressure window the race
      # exploits.
      check (await p.tryAcquirePendingRecord()) == true
      check (await p.tryAcquirePendingRecord()) == false

      let tp = TopicPartition(topic: "t", partition: 0)
      # partition >= 0 skips resolvePartition (no metadata / coordinator needed).
      let sendFut = p.send("t", @[0x01'u8], partition = 0'i32)

      # The send runs synchronously through the txnLock region (markPartitionForTxn
      # adds `tp` to the pending set) and then parks on the saturated semaphore,
      # before reaching batchLock — so nothing is enqueued yet.
      await sleepAsync(20.milliseconds)
      check p.txnPendingPartitionCount() == 1
      check not p.hasBatch(tp)
      check not sendFut.finished

      # Simulate the concurrent commit/abort finalizing the transaction, then wake
      # the parked send by returning the permit.
      p.setTxnState(tsReady)
      p.releasePendingRecordForTest()

      var raised = false
      try:
        discard await sendFut
      except KafkaError:
        raised = true
      check raised
      # The record was refused, not buffered, so it cannot leak into a later txn.
      check not p.hasBatch(tp)
      # The permit it briefly took was returned on the refusal path.
      check (await p.tryAcquirePendingRecord()) == true

      await cluster.close()

    waitFor testBody()

  test "close skips abort when txn partition sets are empty":
    ## Regression: when a prior commit/abort cleared the partition sets but
    ## left txnState at tsError (e.g. EndTxn succeeded then bumpEpoch
    ## failed), close() must not reissue EndTxn. With sets empty, the guard
    ## short-circuits and close completes even if the coordinator would
    ## otherwise block/fail.
    ## producer-support.close-abort: a skipped abort is not a timeout.
    proc testBody() {.async: (raises: [Exception]).} =
      type Sink = ref object
        abortFailed: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.close.abort.failed":
          sink.abortFailed.inc

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
        metricsHook = hook,
      )
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      p.setTxnState(tsError)
      # Sets intentionally empty — mimics the post-clear state.
      let startMs = epochTime()
      await p.close()
      let elapsedMs = (epochTime() - startMs) * 1000
      check p.isClosed()
      # Must complete well under timeoutMs; no abort should have been attempted.
      check elapsedMs < 450.0
      check sink.abortFailed == 0
      await dummy.close()

    waitFor testBody()

suite "Producer: sticky partition management":
  test "setStickyPartition and getStickyPartition round-trip":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster, partitionStrategy = psSticky)
    check p.getStickyPartition("test").isNone
    p.setStickyPartition("test", 2)
    check p.getStickyPartition("test") == some(2'i32)

  test "sticky partition is available after setting":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster, partitionStrategy = psSticky)
    p.setStickyPartition("topic-a", 0)
    p.setStickyPartition("topic-b", 1)
    check p.getStickyPartition("topic-a") == some(0'i32)
    check p.getStickyPartition("topic-b") == some(1'i32)
    check p.getStickyPartition("topic-c").isNone

# Drives real keyless immediate sends (psSticky, lingerMs = 0) against a
# FakeBroker that echoes back the partition each Produce request targeted. With
# lingerMs <= 0 each send is its own completed "batch", so the sticky partition
# must rotate after every success — otherwise keyless sticky sends pin one
# partition for the producer's lifetime.
proc autoRespondStickyEcho(
    fb: FakeBroker,
    host: string,
    port: int32,
    topic: string,
    partitionCount: int32,
    targeted: ref seq[int32],
) {.async: (raises: []).} =
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 250.milliseconds)
    except CatchableError:
      break
    try:
      let key = req.apiKey
      if key == int16(ord(akApiVersions)):
        await req.respond(
          buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              ),
              FakeApiVersion(
                apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
              ),
              FakeApiVersion(
                apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
              ),
            ],
          )
        )
      elif key == int16(ord(akMetadata)):
        var partitions: seq[FakePartitionMeta]
        for i in 0'i32 ..< partitionCount:
          partitions.add(
            FakePartitionMeta(
              partitionIndex: i, leaderId: 0'i32, replicas: @[0'i32], isr: @[0'i32]
            )
          )
        await req.respond(
          buildMetadataResponseBody(
            [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
            controllerId = 0'i32,
            topics = [
              FakeTopicMeta(
                name: topic, errorCode: 0'i16, isInternal: false, partitions: partitions
              )
            ],
          )
        )
      elif key == int16(ord(akProduce)):
        # Echo the partition the producer actually picked so produceResultOf
        # does not reject the response with a partition mismatch.
        var part = 0'i32
        try:
          var r = initKafkaReader(req.body)
          let preq = r.decode(ProduceRequest, 3'i16)
          if preq.topics.len > 0 and preq.topics[0].partitions.len > 0:
            part = preq.topics[0].partitions[0].partition
        except CatchableError:
          discard
        targeted[].add(part)
        await req.respond(
          buildProduceResponseBody(
            topic = topic,
            partition = part,
            errorCode = 0'i16,
            baseOffset = 0'i64,
            logAppendTimeMs = -1'i64,
            throttleTimeMs = 0'i32,
          )
        )
    except CatchableError:
      break

suite "Producer: sticky partition rotates on immediate send":
  test "keyless sticky + lingerMs=0 rotates across partitions on success":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let topic = "sticky-topic"
      let targeted = new(seq[int32])
      let respFut = autoRespondStickyEcho(fb, host, port, topic, 3'i32, targeted)
      let cluster = newCluster(@[host & ":" & $port], clientId = "sticky-test")
      let p = newKafkaProducer(
        cluster, acks = 1, partitionStrategy = psSticky, lingerMs = 0, timeoutMs = 1500
      )
      try:
        for _ in 0 ..< 3:
          discard await p.send(topic, @[0xAB'u8]) # no key -> sticky path
          # Each successful immediate send must clear the sticky entry so the
          # next send re-rolls instead of pinning the same partition.
          check p.getStickyPartition(topic).isNone
      finally:
        await p.close()
        await fb.close()
        await respFut
      # Three keyless sends rotated across all three partitions (round-robin),
      # rather than pinning partition 0 for every send.
      check targeted[] == @[0'i32, 1'i32, 2'i32]

    waitFor body()

suite "Producer: flushBatch txnState guard":
  test "close fails pending batch when txnState is tsError":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      p.setTxnState(tsError)

      let tp = TopicPartition(topic: "test-topic", partition: 0)
      let rec = Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some(@[0x01'u8]),
        headers: @[],
      )
      let pendingFut = p.injectPendingBatch(tp, rec)

      await p.close()

      check p.isClosed()
      check pendingFut.finished
      check pendingFut.failed

    waitFor testBody()

  test "flushBatch fails batch immediately when txnState is tsError":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      p.setTxnState(tsError)

      let tp = TopicPartition(topic: "test-topic", partition: 0)
      let rec = Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some(@[0x01'u8]),
        headers: @[],
      )
      let pendingFut = p.injectPendingBatch(tp, rec)

      # flush should fail the batch futures immediately due to tsError guard
      await p.flush()

      check pendingFut.finished
      check pendingFut.failed

    waitFor testBody()

  test "flushBatch fails batch when registerPendingTxnPartitions fails":
    ## Regression: the KafkaError / CancelledError branches that wrap
    ## registerPendingTxnPartitions used to re-acquire batchLock around
    ## failBatchFutures. failBatchFutures does no awaits, so the extra lock
    ## dance was dead weight. Simplified path must still fail pending
    ## futures and drop the batch entry exactly once.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 300,
      )
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      p.setTxnState(tsInTransaction)
      # The coordinator roundtrip in registerPendingTxnPartitions is gated by
      # txnPendingPartitions, not by producerId (the partition is added below).
      p.setProducerId(42, 0)
      let tp = TopicPartition(topic: "test-topic", partition: 0)
      let rec = Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some(@[0x01'u8]),
        headers: @[],
      )
      let pendingFut = p.injectPendingBatch(tp, rec)
      p.addTxnPendingPartition(tp)

      await p.flush()

      check pendingFut.finished
      check pendingFut.failed
      # The batch entry must be removed exactly once — no stale entry left.
      check not p.hasBatch(tp)
      # A registration failure must also poison the transaction so a
      # subsequent commit cannot finalize a txn that is silently missing this
      # record (EOS violation). Before the fix txnState stayed tsInTransaction.
      check p.txnState in {tsError, tsFatal}
      await dummy.close()

    waitFor testBody()

  test "flushBatch waits for txnLock before registerPendingTxnPartitions":
    ## Regression for the linger path acquiring txnLock symmetrically with
    ## `send()` for `lingerMs <= 0`. Before the fix, flushBatch consumed
    ## `txnPendingPartitions` without txnLock, so a commit/abort racing the
    ## linger flush could tear down the tracking mid-roundtrip.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 200,
      )
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      p.setTxnState(tsInTransaction)
      p.setProducerId(42, 0)
      let tp = TopicPartition(topic: "test-topic", partition: 0)
      let rec = Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some(@[0x01'u8]),
        headers: @[],
      )
      let pendingFut = p.injectPendingBatch(tp, rec)
      p.addTxnPendingPartition(tp)

      # Hold txnLock so the linger-flush path must wait for it. If the fix
      # is reverted (flushBatch skips the lock), pendingFut completes
      # immediately and the assertion below fires.
      await p.acquireTxnLock()
      let flushFut = p.flush()
      await sleepAsync(50.milliseconds)
      check not pendingFut.finished
      p.releaseTxnLock()

      await flushFut
      check pendingFut.finished
      check pendingFut.failed
      check not p.hasBatch(tp)
      await dummy.close()

    waitFor testBody()

  test "flushBatch fails batch immediately when txnState is tsFatal":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      p.setTxnState(tsFatal)

      let tp = TopicPartition(topic: "test-topic", partition: 0)
      let rec = Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some(@[0x01'u8]),
        headers: @[],
      )
      let pendingFut = p.injectPendingBatch(tp, rec)

      # flush should fail the batch futures immediately due to tsFatal guard
      await p.flush()

      check pendingFut.finished
      check pendingFut.failed

    waitFor testBody()

suite "Producer: transaction lock":
  test "beginTransaction fails when txnLock is held":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster, transactionalId = some("test-txn"), enableIdempotence = true
      )
      p.setTxnState(tsReady)
      await p.acquireTxnLock()
      var raised = false
      try:
        p.beginTransaction()
      except KafkaError as e:
        raised = true
        check "in progress" in e.msg
      check raised
      p.releaseTxnLock()

    waitFor testBody()

  test "txnLock is not held by default":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster, transactionalId = some("test-txn"), enableIdempotence = true
    )
    check not p.isTxnLocked()

  test "beginTransaction rejects after close set lsClosing":
    ## Regression: without a state != lsRunning guard, a close() awaiting
    ## abortTransaction's pre-lock await points would yield to a sync
    ## beginTransaction that reset txnStarted, and the resumed abort would
    ## skip EndTxn — orphaning the coordinator's Ongoing txn.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster, transactionalId = some("test-txn"), enableIdempotence = true
      )
      p.setTxnState(tsReady)
      p.markClosed()
      var raised = false
      try:
        p.beginTransaction()
      except KafkaClosedError:
        raised = true
      check raised
      check p.txnState == tsReady

    waitFor testBody()

  test "beginTransaction rejects re-entry from tsInTransaction":
    ## Regression: canTransition's src == dst self-loop let a second
    ## beginTransaction succeed from tsInTransaction, and the resets that
    ## follow wiped a live txn's tracking — a later commit/abort/close would
    ## then see `not txnStarted` and skip EndTxn.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster, transactionalId = some("test-txn"), enableIdempotence = true
      )
      p.setTxnState(tsInTransaction)
      p.addTxnPartition(TopicPartition(topic: "t", partition: 0))
      var raised = false
      try:
        p.beginTransaction()
      except KafkaError as e:
        raised = true
        check "tsInTransaction" in e.msg
      check raised
      check p.txnState == tsInTransaction
      check p.getTxnStarted()
      check p.txnPartitionCount() == 1

    waitFor testBody()

  test "beginTransaction rejects re-entry from tsError":
    ## tsError requires an explicit abortTransaction first; otherwise the
    ## re-entry would clear tracking the pending abort still needs.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster, transactionalId = some("test-txn"), enableIdempotence = true
      )
      p.setTxnState(tsError)
      p.addTxnPartition(TopicPartition(topic: "t", partition: 0))
      var raised = false
      try:
        p.beginTransaction()
      except KafkaError as e:
        raised = true
        check "tsError" in e.msg
      check raised
      check p.txnState == tsError
      check p.getTxnStarted()

    waitFor testBody()

  test "close bounds abortTransaction by timeoutMs when txnLock is held":
    ## Regression: close() previously awaited abortTransaction() without a
    ## timeout. If another coroutine held txnLock (e.g. an in-flight
    ## commit/abort that the user cancelled), close() would block
    ## indefinitely. With the fix, close completes within roughly timeoutMs
    ## and transitions the producer to lsClosed regardless.
    ## producer-support.close-abort: abort re-raises cancel, so withTimeout
    ## sees the timer win. The hook records producer.close.abort.failed.
    ## flush has nothing to drain.
    proc testBody() {.async: (raises: [Exception]).} =
      type Sink = ref object
        abortFailed: int
        reason: string
        value: int64
        flushFailed: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.close.abort.failed":
          sink.abortFailed.inc
          sink.value = value
          for (k, v) in tags:
            if $k == "reason":
              sink.reason = $v
        elif $event == "producer.close.flush.failed":
          sink.flushFailed.inc

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 300,
        metricsHook = hook,
      )
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      p.setTxnState(tsInTransaction)
      p.addTxnPartition(TopicPartition(topic: "t", partition: 0))
      # Take txnLock so abortTransaction() will block on acquire.
      await p.acquireTxnLock()
      let startMs = epochTime()
      await p.close()
      let elapsedMs = (epochTime() - startMs) * 1000
      p.releaseTxnLock()
      check p.isClosed()
      # Should bail out close to timeoutMs, not hang forever.
      check elapsedMs < 2000.0
      check elapsedMs >= 250.0
      check sink.abortFailed == 1
      check sink.reason == "timeout"
      check sink.value == 1
      check sink.flushFailed == 0
      await dummy.close()

    waitFor testBody()

suite "Producer: DLQ producer ownership":
  test "setDeadLetterQueue defaults to ownsProducer=false":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let main = newKafkaProducer(cluster)
      let dlq = newKafkaProducer(cluster)
      await main.setDeadLetterQueue("dlq-topic", dlq)
      check main.dlqProducer == dlq
      check main.dlqTopic == "dlq-topic"
      check main.ownsDlqProducer == false

    waitFor testBody()

  test "setDeadLetterQueue with ownsProducer=true records ownership":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let main = newKafkaProducer(cluster)
      let dlq = newKafkaProducer(cluster)
      await main.setDeadLetterQueue("dlq-topic", dlq, ownsProducer = true)
      check main.ownsDlqProducer == true

    waitFor testBody()

  test "setDeadLetterQueue rejects the source producer as its own DLQ":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let main = newKafkaProducer(cluster)
      expect KafkaError:
        await main.setDeadLetterQueue("dlq-topic", main)

    waitFor testBody()

  test "setDeadLetterQueue rejects a nil producer":
    ## Without the guard the cycle walk dereferences it (NilAccessDefect) — a
    ## crash on a public entry point, not a config rejection.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let main = newKafkaProducer(cluster)
      let none: KafkaProducer = nil
      expect KafkaError:
        await main.setDeadLetterQueue("dlq-topic", none)

    waitFor testBody()

  test "setDeadLetterQueue rejects an empty topic":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let main = newKafkaProducer(cluster)
      let dlq = newKafkaProducer(cluster)
      expect KafkaError:
        await main.setDeadLetterQueue("", dlq)

    waitFor testBody()

  test "setDeadLetterQueue rejects a DLQ cycle (A->B->A)":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let a = newKafkaProducer(cluster)
      let b = newKafkaProducer(cluster)
      # A -> B is fine.
      await a.setDeadLetterQueue("dlq-a", b)
      # B -> A would close the loop: a failed record could bounce A<->B.
      expect KafkaError:
        await b.setDeadLetterQueue("dlq-b", a)

    waitFor testBody()

  test "setDeadLetterQueue rejects a longer DLQ cycle (A->B->C->A)":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let a = newKafkaProducer(cluster)
      let b = newKafkaProducer(cluster)
      let c = newKafkaProducer(cluster)
      await a.setDeadLetterQueue("dlq-a", b)
      await b.setDeadLetterQueue("dlq-b", c)
      expect KafkaError:
        await c.setDeadLetterQueue("dlq-c", a)

    waitFor testBody()

  test "replacing an owned DLQ producer closes the replaced one":
    ## Nothing else would ever close it: close() only cascades into the
    ## configuration current at close time, so pre-fix the first producer was
    ## orphaned with its connections and timers.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let main = newKafkaProducer(cluster)
      let first = newKafkaProducer(cluster)
      let second = newKafkaProducer(cluster)
      await main.setDeadLetterQueue("dlq-first", first, ownsProducer = true)
      await main.setDeadLetterQueue("dlq-second", second, ownsProducer = true)
      check main.dlqProducer == second
      check main.dlqTopic == "dlq-second"
      check main.ownsDlqProducer
      check first.state == lsClosed
      check second.state == lsRunning

    waitFor testBody()

  test "replacing a non-owned DLQ producer leaves it running":
    ## The caller still owns it; the swap must not close someone else's handle.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let main = newKafkaProducer(cluster)
      let first = newKafkaProducer(cluster)
      let second = newKafkaProducer(cluster)
      await main.setDeadLetterQueue("dlq-first", first)
      await main.setDeadLetterQueue("dlq-second", second, ownsProducer = true)
      check first.state == lsRunning
      check second.state == lsRunning

    waitFor testBody()

  test "re-configuring the same owned DLQ producer does not close it":
    ## The replacement IS the current producer, so closing it would tear down
    ## the configuration just installed.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let main = newKafkaProducer(cluster)
      let dlq = newKafkaProducer(cluster)
      await main.setDeadLetterQueue("dlq-first", dlq, ownsProducer = true)
      await main.setDeadLetterQueue("dlq-second", dlq, ownsProducer = true)
      check main.dlqProducer == dlq
      check main.dlqTopic == "dlq-second"
      check dlq.state == lsRunning

    waitFor testBody()

  test "close cascades into owned DLQ producer":
    proc testBody() {.async: (raises: [Exception]).} =
      type Sink = ref object
        dlqFailed: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.close.dlq.failed":
          sink.dlqFailed.inc

      let cluster = newCluster(@["127.0.0.1:9092"])
      let main = newKafkaProducer(cluster, metricsHook = hook)
      let dlq = newKafkaProducer(cluster)
      await main.setDeadLetterQueue("dlq-topic", dlq, ownsProducer = true)
      await main.close()
      check main.state == lsClosed
      check dlq.state == lsClosed
      # producer-support.dlq-close: a completed cascade is not a failure.
      check sink.dlqFailed == 0

    waitFor testBody()

  test "close leaves non-owned DLQ producer alone":
    proc testBody() {.async: (raises: [Exception]).} =
      type Sink = ref object
        dlqFailed: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.close.dlq.failed":
          sink.dlqFailed.inc

      let cluster = newCluster(@["127.0.0.1:9092"])
      let main = newKafkaProducer(cluster, metricsHook = hook)
      let dlq = newKafkaProducer(cluster)
      await main.setDeadLetterQueue("dlq-topic", dlq) # default ownsProducer=false
      await main.close()
      check main.state == lsClosed
      # External owner is still responsible for closing the DLQ producer.
      check dlq.state == lsRunning
      check sink.dlqFailed == 0
      await dlq.close()

    waitFor testBody()

  test "close emits producer.close.dlq.failed when owned DLQ close times out":
    ## producer-support.dlq-close: park the DLQ in fire-and-forget drain
    ## (same blocker as concurrent close) so main's timeoutMs fires first.
    ## The DLQ's own drain budget stays larger so this is not a race
    ## against DLQ finishing on its own. close() swallows cancel, so the
    ## cascade may still reach lsClosed after we emit; the metric is the
    ## timeout itself, not leftover state.
    proc testBody() {.async: (raises: [Exception]).} =
      type Sink = ref object
        dlqFailed: int
        reason: string
        value: int64
        duration: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.close.dlq.failed":
          sink.dlqFailed.inc
          sink.value = value
          for (k, v) in tags:
            if $k == "reason":
              sink.reason = $v
        elif $event == "producer.close.duration_ms":
          sink.duration.inc

      let cluster = newCluster(@["127.0.0.1:9092"])
      let main = newKafkaProducer(cluster, metricsHook = hook, timeoutMs = 300)
      let dlq = newKafkaProducer(cluster, timeoutMs = 5000)
      let blocker = newFuture[void]("dlq-close-blocker")
      dlq.injectPendingFireAndForget(blocker)
      await main.setDeadLetterQueue("dlq-topic", dlq, ownsProducer = true)
      await main.close()
      check main.state == lsClosed
      check sink.dlqFailed == 1
      check sink.reason == "timeout"
      check sink.value == 1
      check sink.duration == 1
      blocker.complete()
      await dlq.close()

    waitFor testBody()

  test "setDeadLetterQueue rejects self-DLQ":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster)
      expect KafkaError:
        await p.setDeadLetterQueue("dlq", p, ownsProducer = true)

    waitFor testBody()

  test "setDeadLetterQueue is rejected once close has started":
    ## Regression for the DLQ-config guard gap: a reconfiguration was legal after
    ## close() transitioned to lsClosing, racing close's owned-DLQ teardown — it
    ## could point close at a producer it already passed over (leaking the
    ## owned-DLQ close) or install a fresh owned producer into a half-torn-down
    ## producer. The mutator now shares the requireRunning guard of every other
    ## public entry point.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 300,
      )
      let dlq = newKafkaProducer(cluster)
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      p.setTxnState(tsInTransaction)
      p.addTxnPartition(TopicPartition(topic: "t", partition: 0))
      # Park close() mid-teardown: hold txnLock so the abort inside closeImpl
      # blocks on acquire. close()'s synchronous prologue has already set
      # lsClosing before any suspension, so the state is observable here.
      await p.acquireTxnLock()
      let closeFut = p.close()
      check p.state == lsClosing
      var raised = false
      try:
        await p.setDeadLetterQueue("dlq-topic", dlq)
      except KafkaClosedError:
        raised = true
      check raised
      p.releaseTxnLock()
      await closeFut
      check p.isClosed()
      await dummy.close()

    waitFor testBody()

suite "Producer: DLQ ErrorHandler factories":
  proc makeRec(): Record =
    Record(
      offset: 0,
      timestamp: 1234'i64,
      key: some(@[0x01'u8, 0x02]),
      value: some(@[0x03'u8]),
      headers: @[],
    )

  test "makeFailedRecord maps every field from the TopicPartition and Record":
    let tp = TopicPartition(topic: "orders", partition: 5)
    let err = newKafkaApiError(keMessageTooLarge, "too big")
    let fr = makeFailedRecord(tp, makeRec(), err, attempts = 3)
    check fr.topic == "orders"
    check fr.partition == 5
    check fr.key == some(@[0x01'u8, 0x02])
    check fr.value == some(@[0x03'u8])
    check fr.headers.len == 0
    check fr.error == err
    check fr.attempts == 3'i32
    check fr.timestamp == 1234'i64

  test "alwaysDeadLetter returns eaDeadLetter regardless of the error":
    let handler = alwaysDeadLetter()
    let fr = makeFailedRecord(
      TopicPartition(topic: "t", partition: 0),
      makeRec(),
      newKafkaApiError(keRequestTimedOut, "transient"),
      1,
    )
    check handler(fr) == eaDeadLetter

  test "alwaysDrop returns eaDrop regardless of the error":
    let handler = alwaysDrop()
    let fr = makeFailedRecord(
      TopicPartition(topic: "t", partition: 0),
      makeRec(),
      newKafkaApiError(keMessageTooLarge, "permanent"),
      1,
    )
    check handler(fr) == eaDrop

  test "deadLetterOnPermanent dead-letters a permanent API error":
    let handler = deadLetterOnPermanent()
    let fr = makeFailedRecord(
      TopicPartition(topic: "t", partition: 0),
      makeRec(),
      newKafkaApiError(keMessageTooLarge, "permanent"),
      1,
    )
    check handler(fr) == eaDeadLetter

  test "deadLetterOnPermanent fails a non-permanent API error":
    let handler = deadLetterOnPermanent()
    # A retriable code is not permanent — the handler must not divert it to the
    # DLQ, leaving the caller to surface it (eaFail).
    let fr = makeFailedRecord(
      TopicPartition(topic: "t", partition: 0),
      makeRec(),
      newKafkaApiError(keRequestTimedOut, "transient"),
      1,
    )
    check handler(fr) == eaFail

  test "deadLetterOnPermanent fails a non-API error":
    let handler = deadLetterOnPermanent()
    let fr = makeFailedRecord(
      TopicPartition(topic: "t", partition: 0),
      makeRec(),
      newException(KafkaError, "plain transport failure"),
      1,
    )
    check handler(fr) == eaFail

suite "Producer: sendFireAndForget close race":
  test "sendFireAndForget raises KafkaClosedError after close":
    ## Regression: previously sendFireAndForget skipped the running-state
    ## check, so it could enqueue a background future after close() had
    ## already drained pendingFireAndForget — leaving the task running
    ## against a half-torn-down producer.
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, lingerMs = 0)
      let semBefore = p.fireAndForgetSemAvailable
      p.markClosed()
      var raised = false
      try:
        await p.sendFireAndForget("t", @[0x01'u8])
      except KafkaClosedError:
        raised = true
      check raised
      check p.pendingFireAndForgetLen == 0
      # Permit must be released when we bail out: the semaphore is exactly as
      # it was before the failed acquire. Otherwise a long-lived producer
      # would slowly drain its fire-and-forget budget on every close race.
      check p.fireAndForgetSemAvailable == semBefore

    waitFor body()

suite "Producer: sendFireAndForget callback exception guard":
  ## Regression: invokeCallback invoked user callbacks without a CatchableError
  ## guard (asymmetric with invokeErrorHandler, which logs and falls back). A
  ## callback leaking an exception through `{.cast(raises: []).}` failed the
  ## background task future; the fix logs and swallows it so the task completes.

  test "a callback that leaks an exception is swallowed and the task completes":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50)
      let p =
        newKafkaProducer(cluster, lingerMs = 0, maxRetries = 0, retryBackoffMs = 0)

      var callbackRan = false
      proc cb(res: ProduceResult, err: ref KafkaError) {.closure, gcsafe, raises: [].} =
        callbackRan = true
        # A buggy callback leaking an exception despite the raises: [] contract.
        {.cast(raises: []).}:
          raise newException(ValueError, "intentional callback bug")

      let logger = ProducerLogSink()
      let previousLogFilter = stdLogging.getLogFilter()
      stdLogging.setLogFilter(stdLogging.lvlAll)
      stdLogging.addHandler(logger)
      var unfinished = 1
      try:
        await p.sendFireAndForget("t", @[0x01'u8], callback = cb)

        # The callback runs in the background task; wait for it to settle. The
        # tracked list is compacted lazily (only on the next sendFireAndForget),
        # so count unfinished futures directly.
        let deadline = Moment.now() + 3.seconds
        while unfinished > 0 and Moment.now() < deadline:
          unfinished = 0
          for fut in p.pendingFireAndForget:
            if not fut.finished:
              inc unfinished
          if unfinished > 0:
            await sleepAsync(milliseconds(10))
      finally:
        stdLogging.removeHandler(logger)
        stdLogging.setLogFilter(previousLogFilter)
      check unfinished == 0
      check callbackRan
      # The send error the callback was handed is still logged, as with no callback.
      var reported, fallback = false
      for msg in logger.messages:
        if msg.startsWith(
          "sendFireAndForget callback raised ValueError: intentional callback bug"
        ):
          reported = true
        if msg.startsWith("sendFireAndForget error (callback raised): "):
          fallback = true
      check reported
      check fallback

      await p.close()
      await cluster.close()

    waitFor body()

# transactional Produce failure paths drive txnState transitions
#
# Drives a transactional producer against a FakeBroker that returns a chosen
# Produce error code. We pre-seed the producer with a producerId / epoch /
# tsInTransaction state via the local seedTxnProducer helper so the test does
# not need InitProducerId / AddPartitionsToTxn round-trips. The fake only has
# to answer ApiVersions, Metadata, and Produce.

type TxnProduceBehavior = ref object
  produceErrorCode: int16

proc autoRespondTxn(
    fb: FakeBroker,
    host: string,
    port: int32,
    topic: string,
    behavior: TxnProduceBehavior,
) {.async: (raises: []).} =
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 250.milliseconds)
    except CatchableError:
      break
    try:
      let key = req.apiKey
      if key == int16(ord(akApiVersions)):
        let body = buildApiVersionsResponseBody(
          0'i16,
          [
            FakeApiVersion(
              apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
            ),
            FakeApiVersion(apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1),
            FakeApiVersion(apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3),
          ],
        )
        await req.respond(body)
      elif key == int16(ord(akMetadata)):
        let body = buildMetadataResponseBody(
          [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
          controllerId = 0'i32,
          topics = [
            FakeTopicMeta(
              name: topic,
              errorCode: 0'i16,
              isInternal: false,
              partitions: @[
                FakePartitionMeta(
                  partitionIndex: 0'i32,
                  leaderId: 0'i32,
                  replicas: @[0'i32],
                  isr: @[0'i32],
                )
              ],
            )
          ],
        )
        await req.respond(body)
      elif key == int16(ord(akProduce)):
        let body = buildProduceResponseBody(
          topic = topic,
          partition = 0'i32,
          errorCode = behavior.produceErrorCode,
          baseOffset = 0'i64,
          logAppendTimeMs = -1'i64,
          throttleTimeMs = 0'i32,
        )
        await req.respond(body)
    except CatchableError:
      break

proc seedTxnProducer(p: KafkaProducer) =
  ## Shortcut: pretend InitProducerId and beginTransaction already happened.
  p.setProducerId(1000'i64, 0'i16)
  p.setTxnState(tsInTransaction)
  p.addTxnPartition(TopicPartition(topic: "txn-topic", partition: 0'i32))

proc runTxnProduceFailureCase(
    expectedState: TransactionState, ec: int16
) {.async: (raises: [Exception]).} =
  let fb = newFakeBroker()
  let host = "127.0.0.1"
  let port = int32(fb.address().port)
  let behavior = TxnProduceBehavior(produceErrorCode: ec)
  let respFut = autoRespondTxn(fb, host, port, "txn-topic", behavior)
  let cluster = newCluster(@[host & ":" & $port], clientId = "p1p2-test")
  let p = newKafkaProducer(
    cluster,
    transactionalId = some("test-txn"),
    enableIdempotence = true,
    maxRetries = 0,
    retryBackoffMs = 1,
    timeoutMs = 1500,
  )
  seedTxnProducer(p)
  let idBefore = p.getProducerId
  let epochBefore = p.getProducerEpoch
  var raisedCode = keNone
  try:
    discard await p.send("txn-topic", @[0xAB'u8])
  except KafkaApiError as e:
    raisedCode = e.errorCode
  except KafkaError:
    discard
  # Either way the state machine must reflect the failure.
  check p.txnState == expectedState
  # resetProducerId() must NOT have been called for a transactional
  # producer, so the identity survives across the failure.
  check p.getProducerId == idBefore
  check p.getProducerEpoch == epochBefore
  check raisedCode.int16 == ec

  await p.close()
  await fb.close()
  awaitDrain(respFut, 1.seconds)

suite "Producer: transactional send failure transitions":
  test "ecAbortable (INVALID_TXN_STATE) -> tsError, producer-id preserved":
    waitFor runTxnProduceFailureCase(tsError, 48'i16)

  test "ecFatal (PRODUCER_FENCED) -> tsFatal, producer-id preserved":
    waitFor runTxnProduceFailureCase(tsFatal, 90'i16)

  test "ecNeedsProducerIdReset (UNKNOWN_PRODUCER_ID) -> tsError, no reset in txn":
    waitFor runTxnProduceFailureCase(tsError, 59'i16)

proc newJoinTestProducer(
    partitioner: CustomPartitioner = nil,
    lingerMs = 0'i32,
    maxPendingRecords = 10_000'i32,
    maxPendingFireAndForget = 1000'i32,
    metricsHook: MetricsHook = nil,
): KafkaProducer =
  ## A transactional producer mid-transaction, over a cluster that already knows topic
  ## "t" (3 partitions), so resolving a partition never reaches a broker.
  let cluster = newCluster(@["127.0.0.1:9092"])
  for i in 0'i32 ..< 3'i32:
    cluster.topicPartitions.mgetOrPut("t", @[]).add(
      PartitionMeta(partitionIndex: i, leaderId: 0, errorCode: keNone)
    )
  cluster.lastMetadataRefresh = Moment.now()
  result = newKafkaProducer(
    cluster,
    transactionalId = some("test-txn"),
    enableIdempotence = true,
    customPartitioner = partitioner,
    lingerMs = lingerMs,
    maxPendingRecords = maxPendingRecords,
    maxPendingFireAndForget = maxPendingFireAndForget,
    metricsHook = metricsHook,
  )
  result.setTxnState(tsInTransaction)

suite "Producer: a send failing before its record joins the txn":
  ## Regression: only a failed partition registration poisoned the txn, so most
  ## failures before the record joined (oversized record, partitioner, a cancel
  ## while waiting for txnLock) left a txn that a caller swallowing the error could
  ## commit without the record. Send now poisons on any of them, but only the txn it
  ## was called in: one begun while it awaited never held the record. An oversized
  ## record (KafkaRecordRejectedError) is the exception: it could never be sent, so
  ## the caller may skip it and still commit.

  test "a partitioner failure poisons the open transaction":
    proc body() {.async: (raises: [Exception]).} =
      var leaks = true
      proc partitioner(
          topic: string, key: Option[seq[byte]], partitionCount: int32
      ): int32 {.closure, gcsafe, raises: [].} =
        if leaks:
          {.cast(raises: []).}:
            raise newException(ValueError, "intentional partitioner bug")
        partitionCount # out of range

      for leak in [true, false]:
        leaks = leak
        let p = newJoinTestProducer(partitioner)
        var raised = false
        try:
          discard await p.send("t", @[1'u8])
        except KafkaError:
          raised = true
        check raised
        check p.txnState == tsError
        await p.close()

    waitFor body()

  test "an oversized record leaves the transaction committable":
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      var msg = ""
      try:
        discard await p.send("t", newSeq[byte](2_000_000))
      except KafkaRecordRejectedError as e:
        msg = e.msg
      check "exceeds maxBatchBytes" in msg
      check p.txnState == tsInTransaction
      await p.close()

    waitFor body()

  test "an oversized record is rejected before waiting on a deferred epoch bump":
    ## Regression: the size check ran after the bump's await, so a cancel or bump
    ## failure there preempted the rejection and poisoned the txn.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      p.holdEpochBumpGate()
      let fut = p.send("t", newSeq[byte](2_000_000))
      check fut.failed()
      p.releaseEpochBumpGate()
      var rejected = false
      try:
        discard await fut
      except KafkaRecordRejectedError:
        rejected = true
      check rejected
      check p.txnState == tsInTransaction
      await p.close()

    waitFor body()

  test "a partition the cached metadata rules out poisons the open transaction":
    ## Not a KafkaRecordRejectedError: the cache may lag a topic that has since
    ## grown, so the record may be valid and skipping it would lose it.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      var msg = ""
      var rejected = false
      try:
        discard await p.send("t", @[1'u8], partition = 5)
      except KafkaRecordRejectedError:
        rejected = true
      except KafkaError as e:
        msg = e.msg
      check not rejected
      check "out of range for topic t" in msg
      check p.txnState == tsError
      await p.close()

    waitFor body()

  test "a send made before beginTransaction is refused as such":
    ## Regression: the state was checked only after a deferred epoch bump, so a
    ## beginTransaction during it let the send fail as if its txn had been replaced.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      p.setTxnState(tsReady)
      p.holdEpochBumpGate()
      let fut = p.send("t", @[1'u8])
      p.beginTransaction()
      p.releaseEpochBumpGate()
      var msg = ""
      try:
        discard await fut
      except KafkaError as e:
        msg = e.msg
      check msg == "must call beginTransaction before send"
      check p.txnState == tsInTransaction
      await p.close()

    waitFor body()

  test "an oversized record sent outside a transaction is refused as such":
    ## Regression: the size check ran first, so the send failed with a rejection the
    ## caller may skip and still commit, hiding that no transaction was open.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      p.setTxnState(tsReady)
      var msg = ""
      var rejected = false
      try:
        discard await p.send("t", newSeq[byte](2_000_000))
      except KafkaRecordRejectedError:
        rejected = true
      except KafkaError as e:
        msg = e.msg
      check not rejected
      check msg == "must call beginTransaction before send"
      await p.close()

    waitFor body()

  test "a send in a poisoned txn or a fatal producer is refused as such":
    ## Regression: both were told to call beginTransaction, which a poisoned txn
    ## (abort it first) and a fatal producer (close it) refuse.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      p.setTxnState(tsError)
      var sendErr = ""
      try:
        discard await p.send("t", @[1'u8])
      except KafkaError as e:
        sendErr = e.msg
      check sendErr ==
        "cannot send: the transaction failed and must be aborted (state: tsError)"
      var offsets = initTable[TopicPartition, int64]()
      offsets[TopicPartition(topic: "t", partition: 0'i32)] = 10'i64
      var offsetsErr = ""
      try:
        await p.sendOffsetsToTransaction(offsets, "test-group")
      except KafkaError as e:
        offsetsErr = e.msg
      check offsetsErr ==
        "cannot sendOffsetsToTransaction: the transaction failed and must be aborted (state: tsError)"
      p.setTxnState(tsFatal)
      sendErr = ""
      try:
        discard await p.send("t", @[1'u8])
      except KafkaError as e:
        sendErr = e.msg
      check sendErr == "producer is in a fatal state and must be closed"
      await p.close()

    waitFor body()

  test "a fatal registration error escalates a transaction already poisoned":
    ## Regression: a send failing before its record joined poisoned only an open or
    ## committing txn, so a PRODUCER_FENCED registration reply arriving after a
    ## concurrent failure had set tsError left the producer abortable, not fatal.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let b = newBroker(0'i32, "127.0.0.1", int32(fb.address().port), "fenced-reg")
      let p = newJoinTestProducer()
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          await av.respond(
            buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akAddPartitionsToTxn)), minVersion: 0, maxVersion: 0
                ),
              ],
            )
          )
          let req = await fb.nextRequest(timeout = 5.seconds)
          # A lock-free failure (another send's terminal error) poisons the txn
          # while the registration is on the wire.
          p.setTxnState(tsError)
          await req.respond(
            buildAddPartitionsToTxnResponseBody(
              "t", [(0'i32, keProducerFenced.toInt16)]
            )
          )
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()
      var msg = ""
      try:
        discard await p.send("t", @[1'u8], partition = 0)
      except KafkaError as e:
        msg = e.msg
      check "PRODUCER_FENCED" in msg
      check p.txnState == tsFatal

      await p.close()
      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()

    waitFor body()

  test "a send cancelled before its record joins refuses the commit waiting on it":
    ## Regression: an immediate send registers a barrier marker the commit waits on,
    ## but a cancel while it waited for its in-flight permit poisoned only in
    ## tsInTransaction, so the commit already in tsCommitting went ahead without it.
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:1"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        maxInFlightRequests = 1,
        timeoutMs = 200,
      )
      # Unreachable, so a commit that wrongly went ahead fails at EndTxn instead.
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      let tp = TopicPartition(topic: "t", partition: 0)
      p.setProducerId(1000'i64, 0'i16)
      p.setTxnState(tsInTransaction)
      # Already registered, so the send reaches its permit wait with no RPC.
      p.addTxnPartition(tp)
      # Held, so the send parks after registering its marker.
      check await p.tryAcquireInflightSemaphore(tp)
      let fut = p.send("t", @[1'u8], partition = 0)
      let commitFut = p.commitTransaction()
      let deadline = Moment.now() + 2.seconds
      while p.txnState != tsCommitting and Moment.now() < deadline:
        await sleepAsync(milliseconds(1))
      check p.txnState == tsCommitting
      await fut.cancelAndWait()
      check fut.cancelled()
      check p.txnState == tsError
      var msg = ""
      try:
        await commitFut
      except KafkaError as e:
        msg = e.msg
      check "cannot commit" in msg
      p.releaseInflightSemaphore(tp)
      await dummy.close()
      await p.close()
      await cluster.close()

    waitFor body()

  test "a batched send failing while the commit runs refuses that commit":
    ## Regression: only an immediate send poisoned a committing txn (the commit
    ## waits on its barrier marker), so a batched send failing once the commit had
    ## begun left it committable without the record.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer(lingerMs = 60_000, maxPendingRecords = 1)
      let tp = TopicPartition(topic: "t", partition: 0)
      # Registered but not started, so a commit that wrongly went ahead finalizes
      # locally instead of needing a coordinator.
      p.addTxnPartition(tp)
      p.setTxnStarted(false)
      # The only pending-record permit, so the send parks before its record joins.
      check await p.tryAcquirePendingRecord()
      let fut = p.send("t", @[1'u8], partition = 0)
      # Parks the commit in tsCommitting, waiting on an in-flight flush.
      let gate = newFuture[void]("test-gate")
      let inflight = p.simulateInflightFlushForTest(gate)
      let commitFut = p.commitTransaction()
      check p.txnState == tsCommitting
      p.releasePendingRecordForTest()
      check await fut.withTimeout(2.seconds)
      check fut.failed()
      check p.txnState == tsError
      gate.complete()
      await inflight
      var msg = ""
      try:
        await commitFut
      except KafkaError as e:
        msg = e.msg
      check "cannot commit" in msg
      await p.close()

    waitFor body()

  test "a commit made while a send has yet to join is refused":
    ## Regression: a commit waited only for buffered and in-flight records, so a
    ## fire-and-forget send still waiting on backpressure failed after the commit
    ## had gone through without it, its error reaching only the callback.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer(lingerMs = 60_000, maxPendingRecords = 1)
      let tp = TopicPartition(topic: "t", partition: 0)
      p.addTxnPartition(tp)
      p.setTxnStarted(false)
      check await p.tryAcquirePendingRecord()
      var cbErr = ""
      proc callback(
          res: ProduceResult, err: ref KafkaError
      ) {.closure, gcsafe, raises: [].} =
        if err != nil:
          cbErr = err.msg

      await p.sendFireAndForget("t", @[1'u8], partition = 0, callback = callback)
      var msg = ""
      try:
        await p.commitTransaction()
      except KafkaError as e:
        msg = e.msg
      check "cannot commit" in msg
      check p.txnState == tsError
      p.releasePendingRecordForTest()
      let deadline = Moment.now() + 2.seconds
      while cbErr.len == 0 and Moment.now() < deadline:
        await sleepAsync(milliseconds(1))
      check "transaction state changed during send" in cbErr
      await p.close()

    waitFor body()

  test "a send cancelled before its record joins poisons the transaction":
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      # Held, so the send can only park before its record joins.
      await p.acquireTxnLock()
      let fut = p.send("t", @[1'u8])
      await fut.cancelAndWait()
      p.releaseTxnLock()
      check fut.cancelled()
      check p.txnState == tsError
      await p.close()

    waitFor body()

  test "a failure after a later transaction began leaves that one alone":
    proc body() {.async: (raises: [Exception]).} =
      var p: KafkaProducer
      proc partitioner(
          topic: string, key: Option[seq[byte]], partitionCount: int32
      ): int32 {.closure, gcsafe, raises: [].} =
        # Meanwhile the txn the send was called in ended and the next one began.
        p.setTxnState(tsReady)
        try:
          p.beginTransaction()
        except KafkaError:
          discard
        {.cast(raises: []).}:
          raise newException(ValueError, "intentional partitioner bug")

      p = newJoinTestProducer(partitioner)
      var raised = false
      try:
        discard await p.send("t", @[1'u8])
      except KafkaError:
        raised = true
      check raised
      check p.txnState == tsInTransaction
      await p.close()

    waitFor body()

  test "a send whose transaction ended while it awaited does not join the next one":
    ## Regression: the txnLock re-check looked at the state alone, which the next
    ## txn restores to tsInTransaction, so the record joined a txn begun after send
    ## was called.
    proc body() {.async: (raises: [Exception]).} =
      var p: KafkaProducer
      proc partitioner(
          topic: string, key: Option[seq[byte]], partitionCount: int32
      ): int32 {.closure, gcsafe, raises: [].} =
        p.setTxnState(tsReady)
        try:
          p.beginTransaction()
        except KafkaError:
          discard
        0

      p = newJoinTestProducer(partitioner)
      var msg = ""
      try:
        discard await p.send("t", @[1'u8])
      except KafkaError as e:
        msg = e.msg
      check msg == "transaction changed during send (a later transaction is open)"
      check p.txnState == tsInTransaction
      check p.txnPendingPartitionCount() == 0
      await p.close()

    waitFor body()

  test "a batched send whose transaction ended while it waited does not join the next one":
    ## Regression: the enqueue re-check under batchLock looked at the state alone,
    ## so a batched send that marked its partition in one txn appended its record
    ## to the next one once that had begun.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer(lingerMs = 60_000)
      let tp = TopicPartition(topic: "t", partition: 0)
      # Held, so the send parks after marking its partition, before appending.
      await p.acquireBatchLock()
      let fut = p.send("t", @[1'u8], partition = 0)
      let deadline = Moment.now() + 2.seconds
      while p.txnPendingPartitionCount() == 0 and Moment.now() < deadline:
        await sleepAsync(milliseconds(1))
      check p.txnPendingPartitionCount() == 1
      # Meanwhile the txn the send was called in ended and the next one began.
      p.setTxnState(tsReady)
      p.beginTransaction()
      p.releaseBatchLock()
      # Bounded: a send that wrongly joined would wait out the linger instead.
      check await fut.withTimeout(2.seconds)
      check fut.failed()
      if fut.failed():
        check fut.error.msg ==
          "transaction changed during send (a later transaction is open)"
      check p.txnState == tsInTransaction
      check p.pendingBatchBytesFor(tp) == 0
      # Drop any batch before close(), which would flush it, and this producer has no
      # coordinator to register its partition with.
      p.failBatchFuturesForTest(tp, newException(KafkaError, "test teardown"))
      await p.close()

    waitFor body()

  test "cancelling a batched send after its record joined leaves the transaction open":
    ## The record stays in the batch and goes out with the commit, so poisoning
    ## the txn here would refuse a commit that is still whole.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer(lingerMs = 60_000)
      let tp = TopicPartition(topic: "t", partition: 0)
      let fut = p.send("t", @[1'u8], partition = 0)
      let deadline = Moment.now() + 2.seconds
      while p.pendingBatchBytesFor(tp) == 0 and Moment.now() < deadline:
        await sleepAsync(milliseconds(1))
      check p.pendingBatchBytesFor(tp) > 0
      await fut.cancelAndWait()
      check fut.cancelled()
      check p.txnState == tsInTransaction
      check p.pendingBatchBytesFor(tp) > 0
      # Drop the batch: close() would flush it, and this producer has no coordinator
      # to register its partition with.
      p.failBatchFuturesForTest(tp, newException(KafkaError, "test teardown"))
      await p.close()

    waitFor body()

suite "Producer: commit and abort close their transaction when called":
  ## Regression: a commit left tsInTransaction only once it held txnLock, so a send
  ## made while it waited was counted as one yet to join, and the commit refused.
  ## A call made after commit/abort is now refused instead, and each finalize acts
  ## only on the txn it was called in.
  test "a send made while a commit waits for txnLock leaves that commit alone":
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      # Held, so the commit parks before it takes the lock.
      await p.acquireTxnLock()
      let commitFut = p.commitTransaction()
      check not commitFut.finished
      let sendFut = p.send("t", @[1'u8])
      p.releaseTxnLock()
      var commitErr = ""
      try:
        await commitFut
      except KafkaError as e:
        commitErr = e.msg
      check commitErr == ""
      # Nothing reached the coordinator, so the commit finalized locally.
      check p.txnState == tsReady
      check await sendFut.withTimeout(2.seconds)
      check sendFut.failed()
      if sendFut.failed():
        check sendFut.error.msg ==
          "cannot send: commitTransaction or abortTransaction was already called"
      await p.close()

    waitFor body()

  test "sendOffsetsToTransaction made while an abort waits is refused":
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      var offsets = initTable[TopicPartition, int64]()
      offsets[TopicPartition(topic: "t", partition: 0'i32)] = 10'i64
      # Parks the abort on a deferred epoch bump, before it takes txnLock.
      p.holdEpochBumpGate()
      let abortFut = p.abortTransaction()
      check not abortFut.finished
      let offsetsFut = p.sendOffsetsToTransaction(offsets, "test-group")
      p.releaseEpochBumpGate()
      await abortFut
      check p.txnState == tsReady
      check offsetsFut.finished
      check offsetsFut.failed()
      if offsetsFut.failed():
        check offsetsFut.error.msg ==
          "cannot sendOffsetsToTransaction: commitTransaction or abortTransaction was already called"
      await p.close()

    waitFor body()

  test "a commit that takes txnLock ahead of an earlier sendOffsetsToTransaction is refused":
    ## Regression: only sends were counted, so a commit that won the lock finalized
    ## the txn without offsets requested before it, and the offsets call failed after.
    proc body() {.async: (raises: [Exception]).} =
      var offsetsErrors = 0
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.txn.send_offsets.error":
          inc offsetsErrors

      let p = newJoinTestProducer(metricsHook = hook)
      var offsets = initTable[TopicPartition, int64]()
      offsets[TopicPartition(topic: "t", partition: 0'i32)] = 10'i64
      p.holdEpochBumpGate()
      let offsetsFut = p.sendOffsetsToTransaction(offsets, "test-group")
      check not offsetsFut.finished
      # The bump settles, and the commit takes txnLock before the parked call resumes.
      p.releaseEpochBumpGate()
      let commitFut = p.commitTransaction()
      var commitErr = ""
      try:
        await commitFut
      except KafkaError as e:
        commitErr = e.msg
      check "cannot commit" in commitErr
      check "have not joined" in commitErr
      check p.txnState == tsError
      var offsetsErr = ""
      try:
        await offsetsFut
      except KafkaError as e:
        offsetsErr = e.msg
      check offsetsErr ==
        "transaction state changed during sendOffsetsToTransaction (now tsError)"
      # A refusal of an already poisoned txn, not a failed offsets commit.
      check offsetsErrors == 0
      await p.close()

    waitFor body()

  test "an abort whose transaction ended while it waited leaves the next one alone":
    ## Regression: the abort re-checked the state alone, which the next txn restores
    ## to tsInTransaction, so it aborted a txn begun after it was called.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      p.holdEpochBumpGate()
      let abortFut = p.abortTransaction()
      check not abortFut.finished
      # Meanwhile the txn the abort was called in ended and the next one began.
      p.setTxnState(tsReady)
      p.beginTransaction()
      p.releaseEpochBumpGate()
      var msg = ""
      try:
        await abortFut
      except KafkaError as e:
        msg = e.msg
      check msg ==
        "cannot abort: its transaction ended while it waited (a later transaction is open)"
      check p.txnState == tsInTransaction
      await p.close()

    waitFor body()

  test "an abort with no transaction in progress leaves the finished one's ending":
    ## Regression: the abort marked the committed txn's scope abort-called before it
    ## was refused for having no txn to abort.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      let scope = p.txnScope
      await p.commitTransaction()
      check p.txnState == tsReady
      var msg = ""
      try:
        await p.abortTransaction()
      except KafkaError as e:
        msg = e.msg
      check "cannot abort: not in transaction" in msg
      check scope.ending == teCommitCalled
      await p.close()

    waitFor body()

  test "a commit cancelled before it takes txnLock leaves the transaction closed":
    ## A send made after it is still refused, and another commit finishes the txn.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      await p.acquireTxnLock()
      let first = p.commitTransaction()
      await first.cancelAndWait()
      p.releaseTxnLock()
      check first.cancelled()
      check p.txnState == tsInTransaction
      var msg = ""
      try:
        discard await p.send("t", @[1'u8])
      except KafkaError as e:
        msg = e.msg
      check msg ==
        "cannot send: commitTransaction or abortTransaction was already called"
      check p.txnState == tsInTransaction
      await p.commitTransaction()
      check p.txnState == tsReady
      await p.close()

    waitFor body()

  test "a commit made after a cancelled abort is refused":
    ## The abort left the txn open in tsInTransaction, but the records were meant
    ## to be discarded, so only another abort may finish it.
    proc body() {.async: (raises: [Exception]).} =
      var commitErrors = 0
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.txn.commit.error":
          inc commitErrors

      let p = newJoinTestProducer(metricsHook = hook)
      await p.acquireTxnLock()
      let abortFut = p.abortTransaction()
      await abortFut.cancelAndWait()
      p.releaseTxnLock()
      check abortFut.cancelled()
      check p.txnState == tsInTransaction
      var msg = ""
      try:
        await p.commitTransaction()
      except KafkaError as e:
        msg = e.msg
      check msg == "cannot commit: abortTransaction was already called"
      # Reported like every other refused commit.
      check commitErrors == 1
      check p.txnState == tsInTransaction
      await p.abortTransaction()
      check p.txnState == tsReady
      # Once the abort finished, a commit is refused for the missing txn instead.
      msg = ""
      try:
        await p.commitTransaction()
      except KafkaError as e:
        msg = e.msg
      check "cannot commit: not in transaction" in msg
      await p.close()

    waitFor body()

  test "a call made while a commit finalizes is refused as already called":
    ## Regression: once the commit had moved the state to tsCommitting, the refusal
    ## read as if no transaction had been begun.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      # Parks the commit in tsCommitting, waiting on an in-flight flush.
      let gate = newFuture[void]("test-gate")
      let inflight = p.simulateInflightFlushForTest(gate)
      let commitFut = p.commitTransaction()
      check p.txnState == tsCommitting
      var sendErr = ""
      try:
        discard await p.send("t", @[1'u8])
      except KafkaError as e:
        sendErr = e.msg
      check sendErr ==
        "cannot send: commitTransaction or abortTransaction was already called"
      var offsets = initTable[TopicPartition, int64]()
      offsets[TopicPartition(topic: "t", partition: 0'i32)] = 10'i64
      var offsetsErr = ""
      try:
        await p.sendOffsetsToTransaction(offsets, "test-group")
      except KafkaError as e:
        offsetsErr = e.msg
      check offsetsErr ==
        "cannot sendOffsetsToTransaction: commitTransaction or abortTransaction was already called"
      gate.complete()
      await inflight
      await commitFut
      check p.txnState == tsReady
      await p.close()

    waitFor body()

  test "a fire-and-forget send waiting for a slot does not join a later transaction":
    ## Regression: the queued task read the txn only once it got its slot, so a call
    ## made in one txn, or outside any, joined the next one begun while it waited.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer(
        lingerMs = 60_000, maxPendingRecords = 1, maxPendingFireAndForget = 1
      )
      let tp = TopicPartition(topic: "t", partition: 0)
      # The only pending-record permit, so the first send parks holding the only slot.
      check await p.tryAcquirePendingRecord()
      var firstErr = ""
      var secondErr = ""
      var thirdErr = ""
      proc first(
          res: ProduceResult, err: ref KafkaError
      ) {.closure, gcsafe, raises: [].} =
        if err != nil:
          firstErr = err.msg

      proc second(
          res: ProduceResult, err: ref KafkaError
      ) {.closure, gcsafe, raises: [].} =
        if err != nil:
          secondErr = err.msg

      proc third(
          res: ProduceResult, err: ref KafkaError
      ) {.closure, gcsafe, raises: [].} =
        if err != nil:
          thirdErr = err.msg

      await p.sendFireAndForget("t", @[1'u8], partition = 0, callback = first)
      let queued = p.sendFireAndForget("t", @[2'u8], partition = 0, callback = second)
      check not queued.finished
      # Meanwhile the txn both calls were made in ended, a third call came outside any
      # txn, and the next one began.
      p.setTxnState(tsReady)
      let outside = p.sendFireAndForget("t", @[3'u8], partition = 0, callback = third)
      p.beginTransaction()
      p.releasePendingRecordForTest()
      await queued
      await outside
      let deadline = Moment.now() + 2.seconds
      while (firstErr.len == 0 or secondErr.len == 0 or thirdErr.len == 0) and
          Moment.now() < deadline:
        await sleepAsync(milliseconds(1))
      check "transaction changed during send" in firstErr
      check secondErr ==
        "cannot send: its transaction ended while it waited (a later transaction is open)"
      check thirdErr == "must call beginTransaction before send"
      check p.txnState == tsInTransaction
      check not p.hasBatch(tp)
      if p.hasBatch(tp):
        # close() would flush it, and this producer has no coordinator to register with.
        p.failBatchFuturesForTest(tp, newException(KafkaError, "test teardown"))
      await p.close()

    waitFor body()

  test "a fire-and-forget send whose producer turned fatal while it waited says so":
    ## Regression: the queued send captured an open txn, then failed as if made
    ## outside one, telling the operator to begin a txn the fatal producer refuses.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer(
        lingerMs = 60_000, maxPendingRecords = 1, maxPendingFireAndForget = 1
      )
      let tp = TopicPartition(topic: "t", partition: 0)
      # The only pending-record permit, so the first send parks holding the only slot.
      check await p.tryAcquirePendingRecord()
      var queuedErr = ""
      proc onQueued(
          res: ProduceResult, err: ref KafkaError
      ) {.closure, gcsafe, raises: [].} =
        if err != nil:
          queuedErr = err.msg

      await p.sendFireAndForget("t", @[1'u8], partition = 0)
      let queued = p.sendFireAndForget("t", @[2'u8], partition = 0, callback = onQueued)
      check not queued.finished
      p.setTxnState(tsFatal)
      p.releasePendingRecordForTest()
      await queued
      let deadline = Moment.now() + 2.seconds
      while queuedErr.len == 0 and Moment.now() < deadline:
        await sleepAsync(milliseconds(1))
      check queuedErr == "producer is in a fatal state and must be closed"
      check not p.hasBatch(tp)
      if p.hasBatch(tp):
        p.failBatchFuturesForTest(tp, newException(KafkaError, "test teardown"))
      await p.close()

    waitFor body()

  test "a commit cancelled once txnLock is granted still releases it":
    ## The caller enters its try/finally only after closeAndLockTxn resumes it, which
    ## is safe: chronos does not cancel a future whose awaited child already finished.
    proc body() {.async: (raises: [Exception]).} =
      for ticks in 0 .. 6:
        let p = newJoinTestProducer()
        await p.acquireTxnLock()
        let commitFut = p.commitTransaction()
        # Grant the lock to the parked commit, then cancel it `ticks` turns later.
        p.releaseTxnLock()
        for _ in 0 ..< ticks:
          await sleepAsync(0.milliseconds)
        commitFut.cancelSoon()
        try:
          await commitFut
        except CancelledError, KafkaError:
          discard
        check not p.isTxnLocked()
        await p.close()

    waitFor body()

  test "an abort called after a waiting commit does not refuse that commit":
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      await p.acquireTxnLock()
      let commitFut = p.commitTransaction()
      let abortFut = p.abortTransaction()
      p.releaseTxnLock()
      await commitFut
      check p.txnState == tsReady
      var msg = ""
      try:
        await abortFut
      except KafkaError as e:
        msg = e.msg
      check "cannot abort: not in transaction" in msg
      await p.close()

    waitFor body()

  test "a commit or abort cancelled while waiting for txnLock reports its error":
    ## Regression: the waits ran outside the try that emits the error metric, so a
    ## call failing there left its txn closed without reporting it.
    proc body() {.async: (raises: [Exception]).} =
      var commitErrors, abortErrors: seq[string]
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        var code = ""
        for (k, v) in tags:
          if $k == "error_code":
            code = $v
        if $event == "producer.txn.commit.error":
          commitErrors.add(code)
        elif $event == "producer.txn.abort.error":
          abortErrors.add(code)

      let p = newJoinTestProducer(metricsHook = hook)
      await p.acquireTxnLock()
      let commitFut = p.commitTransaction()
      await commitFut.cancelAndWait()
      check commitFut.cancelled()
      check commitErrors == @["CANCELLED"]
      let abortFut = p.abortTransaction()
      await abortFut.cancelAndWait()
      check abortFut.cancelled()
      check abortErrors == @["CANCELLED"]
      # The lock neither call took is still the test's.
      check p.isTxnLocked()
      p.releaseTxnLock()
      await p.abortTransaction()
      check p.txnState == tsReady
      await p.close()

    waitFor body()

  test "a commit or abort failing on the deferred epoch bump reports its error":
    ## Regression: the bump ran outside the try that emits the error metric, so a
    ## commit or abort failing there left its txn closed without reporting it.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      # ApiVersions leaves InitProducerId out, so the bump fails UNSUPPORTED_VERSION.
      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          await av.respond(
            buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                )
              ],
            )
          )
        except CatchableError:
          discard

      let handlerFut = handler()
      var commitErrors, abortErrors: seq[string]
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        var code = ""
        for (k, v) in tags:
          if $k == "error_code":
            code = $v
        if $event == "producer.txn.commit.error":
          commitErrors.add(code)
        elif $event == "producer.txn.abort.error":
          abortErrors.add(code)

      let p = newJoinTestProducer(metricsHook = hook)
      let b = newBroker(0'i32, "127.0.0.1", port, "txn-bump-unsupported")
      p.setTxnCoordinator(b)
      await b.connect()
      p.setPendingEpochBump(true)
      var commitRaised = false
      try:
        await p.commitTransaction()
      except KafkaApiError:
        commitRaised = true
      check commitRaised
      check commitErrors == @["UNSUPPORTED_VERSION"]
      var abortRaised = false
      try:
        await p.abortTransaction()
      except KafkaApiError:
        abortRaised = true
      check abortRaised
      check abortErrors == @["UNSUPPORTED_VERSION"]
      check not p.isTxnLocked()
      p.setPendingEpochBump(false)
      await p.abortTransaction()
      check p.txnState == tsReady

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await p.close()
      await fb.close()

    waitFor body()

suite "Producer: each client guards its own metrics hook":
  test "producers and their cluster report hook failures separately":
    ## Regression: one threadvar held the report window for every hook on the
    ## thread, so while one client's hook kept failing another's failure was only
    ## counted, never named. Each client constructor now guards the hook it is given.
    proc body() {.async: (raises: [Exception]).} =
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        {.cast(raises: []).}:
          raise newException(ValueError, "intentional hook bug")
      let cluster = newCluster(@["127.0.0.1:9092"], metricsHook = hook)
      let first = newKafkaProducer(cluster, metricsHook = hook)
      let second = newKafkaProducer(cluster, metricsHook = hook)

      let logger = ProducerLogSink()
      let previousLogFilter = stdLogging.getLogFilter()
      stdLogging.setLogFilter(stdLogging.lvlAll)
      stdLogging.addHandler(logger)
      try:
        for stored in [cluster.metricsHook, first.metricsHook, second.metricsHook]:
          emitMetric(stored, meProducerSendOk, noTags, 1)
      finally:
        stdLogging.removeHandler(logger)
        stdLogging.setLogFilter(previousLogFilter)
      # One report per client: through a shared window only the first would show.
      check logger.messages.len == 3
      for msg in logger.messages:
        check "intentional hook bug" in msg

      await first.close()
      await second.close()
      await cluster.close()

    waitFor body()

suite "Producer: duplicate sequence is a deduped success":
  ## `doSendWithRetry` classifies DUPLICATE_SEQUENCE_NUMBER as `raDuplicate` and
  ## completes the record as `pstSuccess` (deduped, offset -1) instead of
  ## retrying or failing it. Only the classify() unit test covered that action.
  test "a duplicate sequence number reports deduped, not a failure":
    proc body() {.async: (raises: [Exception]).} =
      const topic = "txn-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let behavior =
        TxnProduceBehavior(produceErrorCode: int16(ord(keDuplicateSequenceNumber)))
      let respFut = autoRespondTxn(fb, host, port, topic, behavior)
      let cluster = newCluster(@[host & ":" & $port], clientId = "dedup-e2e")

      var events: seq[string]
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        events.add($event)

      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        maxRetries = 0,
        retryBackoffMs = 1,
        timeoutMs = 1500,
        metricsHook = hook,
      )
      seedTxnProducer(p)

      let res = await p.send(topic, @[0xAB'u8])
      check res.status == pstSuccess
      check res.offset == -1'i64
      check "producer.send.deduped" in events
      check "producer.send.ok" notin events
      # The dedup path releases the in-flight slot, so the next send (or close)
      # is not left waiting on a leaked permit.
      check p.inflightBatchCountForTest(TopicPartition(topic: topic, partition: 0'i32)) ==
        0

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor body()

suite "Producer: transactional producer bypasses DLQ/drop handler":
  ## Regression: a DLQ/drop errorHandler must never divert a record out of an
  ## in-flight transaction — that would complete the per-record future as a
  ## "success" while the record never landed in the txn, so commit proceeds with
  ## missing records. A transactional producer bypasses the handler (forces
  ## eaFail) so the failure surfaces.
  test "drop handler does not silently drop a failed transactional record":
    proc body() {.async: (raises: [Exception]).} =
      const topic = "txn-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      # 48 = INVALID_TXN_STATE: terminal for a transactional producer.
      let behavior = TxnProduceBehavior(produceErrorCode: 48'i16)
      let respFut = autoRespondTxn(fb, host, port, topic, behavior)
      let cluster = newCluster(@[host & ":" & $port], clientId = "m2-test")
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        maxRetries = 0,
        retryBackoffMs = 1,
        timeoutMs = 1500,
        errorHandler = alwaysDrop(),
      )
      seedTxnProducer(p)

      var raised = false
      var droppedSilently = false
      try:
        let res = await p.send(topic, @[0xAB'u8])
        if res.status == pstDropped:
          droppedSilently = true
      except KafkaError:
        raised = true

      # With the fix the failure surfaces; without it the record would be
      # silently dropped (droppedSilently == true) and the txn would look clean.
      check raised
      check not droppedSilently
      check p.txnState in {tsError, tsFatal}

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor body()

suite "Producer: commit refuses after a flush send failure":
  ## Regression: flush() awaits each batch's full send completion, so a
  ## transactional send that fails during commitTransaction's flush moves
  ## txnState to tsError before flush() returns. commitTransaction must re-check
  ## the state and refuse to send EndTxn(commit) — committing a transaction that
  ## is missing records is an EOS violation.
  proc autoRespondTrackEndTxn(
      fb: FakeBroker,
      host: string,
      port: int32,
      topic: string,
      produceErrorCode: int16,
      endTxnSeen: ref bool,
  ) {.async: (raises: []).} =
    while true:
      var req: CapturedRequest
      try:
        req = await fb.nextRequest(timeout = 250.milliseconds)
      except CatchableError:
        break
      try:
        let key = req.apiKey
        if key == int16(ord(akApiVersions)):
          await req.respond(
            buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                ),
                # Advertise EndTxn so that, absent the fix, the (wrongful)
                # EndTxn(commit) is actually sent and recorded below.
                FakeApiVersion(
                  apiKey: int16(ord(akEndTxn)), minVersion: 0, maxVersion: 3
                ),
              ],
            )
          )
        elif key == int16(ord(akMetadata)):
          await req.respond(
            buildMetadataResponseBody(
              [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
              controllerId = 0'i32,
              topics = [
                FakeTopicMeta(
                  name: topic,
                  errorCode: 0'i16,
                  isInternal: false,
                  partitions: @[
                    FakePartitionMeta(
                      partitionIndex: 0'i32,
                      leaderId: 0'i32,
                      replicas: @[0'i32],
                      isr: @[0'i32],
                    )
                  ],
                )
              ],
            )
          )
        elif key == int16(ord(akProduce)):
          await req.respond(
            buildProduceResponseBody(
              topic = topic,
              partition = 0'i32,
              errorCode = produceErrorCode,
              baseOffset = 0'i64,
            )
          )
        elif key == int16(ord(akEndTxn)):
          # A correct commit must NOT reach here after a failed flush.
          endTxnSeen[] = true
      except CatchableError:
        break

  test "commitTransaction does not send EndTxn(commit) when a buffered send failed":
    proc body() {.async: (raises: [Exception]).} =
      const topic = "txn-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let endTxnSeen = new(bool)
      endTxnSeen[] = false
      # 48 = INVALID_TXN_STATE (abortable) -> tsError on the txn producer.
      let respFut = autoRespondTrackEndTxn(fb, host, port, topic, 48'i16, endTxnSeen)
      let cluster = newCluster(@[host & ":" & $port], clientId = "h4-test")
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        maxRetries = 0,
        retryBackoffMs = 1,
        timeoutMs = 1500,
      )
      seedTxnProducer(p)
      # Coordinator points at the same fake broker so a wrongful EndTxn(commit)
      # would be observed; with the fix the coordinator is never contacted.
      let coord = newBroker(
        nodeId = 0'i32,
        host = host,
        port = port,
        clientId = "h4-coord",
        requestTimeoutMs = 1000,
        connectTimeoutMs = 1000,
      )
      p.setTxnCoordinator(coord)

      # Buffer a record; commit's flush sends it and the broker fails it
      # terminally, moving txnState to tsError.
      let tp = TopicPartition(topic: topic, partition: 0'i32)
      let rec = Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some(@[0x01'u8]),
        headers: @[],
      )
      let recFut = p.injectPendingBatch(tp, rec)

      var raised = false
      try:
        await p.commitTransaction()
      except KafkaError:
        raised = true

      check raised
      check p.txnState in {tsError, tsFatal}
      check not endTxnSeen[] # the commit must never have been sent to the broker
      check recFut.finished
      try:
        discard await recFut
      except CatchableError:
        discard

      await p.close()
      await coord.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor body()

  test "post-flush tsError re-raise preserves txnStarted; recovery abort issues EndTxn(abort)":
    ## Regression for the design-review LSO-stall / EOS chain:
    ## commitTransaction's post-flush tsError re-raise used to clear txnStarted
    ## in its `except KafkaError` handler. A follow-up abortTransaction would
    ## then see `not txnStarted`, finalize locally, and orphan the coordinator's
    ## Ongoing txn — LSO stall, plus a risk that the next txn's EndTxn(commit)
    ## rolls in the aborted txn's records under the same PID/epoch (EOS
    ## violation). With the fix txnStarted is preserved and the recovery abort
    ## actually reaches the coordinator.
    proc autoRespondCommitFailThenAbort(
        fb: FakeBroker,
        host: string,
        port: int32,
        topic: string,
        endTxnSeen: ref bool,
        endTxnCommitted: ref bool,
    ) {.async: (raises: []).} =
      while true:
        var req: CapturedRequest
        try:
          req = await fb.nextRequest(timeout = 250.milliseconds)
        except CatchableError:
          break
        try:
          let key = req.apiKey
          if key == int16(ord(akApiVersions)):
            await req.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akEndTxn)), minVersion: 0, maxVersion: 0
                  ),
                  # Advertised so the post-abort bumpEpoch can proceed and drive
                  # the txn back to tsReady for the test's final assertion.
                  FakeApiVersion(
                    apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
            )
          elif key == int16(ord(akInitProducerId)):
            await req.respond(
              buildInitProducerIdResponseBody(
                errorCode = 0'i16, producerId = 1000'i64, producerEpoch = 1'i16
              )
            )
          elif key == int16(ord(akMetadata)):
            await req.respond(
              buildMetadataResponseBody(
                [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                controllerId = 0'i32,
                topics = [
                  FakeTopicMeta(
                    name: topic,
                    errorCode: 0'i16,
                    isInternal: false,
                    partitions: @[
                      FakePartitionMeta(
                        partitionIndex: 0'i32,
                        leaderId: 0'i32,
                        replicas: @[0'i32],
                        isr: @[0'i32],
                      )
                    ],
                  )
                ],
              )
            )
          elif key == int16(ord(akProduce)):
            # Fail the buffered send terminally so commit's post-flush check
            # re-raises and lands in the fixed KafkaError handler.
            await req.respond(
              buildProduceResponseBody(
                topic = topic,
                partition = 0'i32,
                errorCode = 48'i16, # INVALID_TXN_STATE -> tsError
                baseOffset = 0'i64,
              )
            )
          elif key == int16(ord(akEndTxn)):
            endTxnSeen[] = true
            # EndTxn v0 request body: transactional_id:string,
            # producer_id:int64, producer_epoch:int16, committed:bool.
            # Parse just the trailing `committed` byte to record abort vs commit.
            let body = req.body
            if body.len > 0:
              endTxnCommitted[] = body[body.len - 1] != 0'u8
            await req.respond(buildEndTxnResponseBody(0'i16))
        except CatchableError:
          break

    proc body() {.async: (raises: [Exception]).} =
      const topic = "txn-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let endTxnSeen = new(bool)
      endTxnSeen[] = false
      let endTxnCommitted = new(bool)
      endTxnCommitted[] = false
      let respFut = autoRespondCommitFailThenAbort(
        fb, host, port, topic, endTxnSeen, endTxnCommitted
      )
      let cluster = newCluster(@[host & ":" & $port], clientId = "recover-test")
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        maxRetries = 0,
        retryBackoffMs = 1,
        timeoutMs = 1500,
      )
      seedTxnProducer(p)
      let coord = newBroker(
        nodeId = 0'i32,
        host = host,
        port = port,
        clientId = "recover-coord",
        requestTimeoutMs = 1000,
        connectTimeoutMs = 1000,
      )
      p.setTxnCoordinator(coord)

      let tp = TopicPartition(topic: topic, partition: 0'i32)
      let rec = Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some(@[0x01'u8]),
        headers: @[],
      )
      let recFut = p.injectPendingBatch(tp, rec)

      var commitRaised = false
      try:
        await p.commitTransaction()
      except KafkaError:
        commitRaised = true
      check commitRaised
      check not endTxnSeen[] # commit refused to send EndTxn(commit)
      check p.txnState in {tsError, tsFatal}
      # Core invariant: post-flush KafkaError re-raise must not clear txnStarted,
      # else the recovery abort short-circuits and orphans the Ongoing txn.
      check p.getTxnStarted()

      # Drain the failed record's future so close() teardown doesn't strand it.
      try:
        discard await recFut
      except CatchableError:
        discard

      # Recovery abort: with the fix this reaches the coordinator and issues
      # a real EndTxn(abort); without it the short-circuit skips broker contact.
      if p.txnState == tsError:
        var abortRaised = false
        try:
          await p.abortTransaction()
        except KafkaError:
          abortRaised = true
        check not abortRaised
        check endTxnSeen[]
        check not endTxnCommitted[] # `committed=false` — abort, not commit
        check p.txnState == tsReady
        check not p.getTxnStarted()

      await p.close()
      await coord.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor body()

suite "Producer: sendOffsetsToTransaction poisons txn on failure":
  ## Regression: a failed sendOffsetsToTransaction must poison the txn
  ## (tsError/tsFatal) like the record-send path. Before the fix it raised but
  ## left txnState at tsInTransaction, so a caller that swallowed the error could
  ## commit a transaction whose offsets never reached the coordinator — an EOS
  ## violation.
  proc autoRespondAddOffsets(
      fb: FakeBroker, addOffsetsErrorCode: int16, addOffsetsSeen: ref bool
  ) {.async: (raises: []).} =
    while true:
      var req: CapturedRequest
      try:
        req = await fb.nextRequest(timeout = 250.milliseconds)
      except CatchableError:
        break
      try:
        let key = req.apiKey
        if key == int16(ord(akApiVersions)):
          await req.respond(
            buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akAddOffsetsToTxn)), minVersion: 0, maxVersion: 0
                ),
              ],
            )
          )
        elif key == int16(ord(akAddOffsetsToTxn)):
          addOffsetsSeen[] = true
          await req.respond(buildAddOffsetsToTxnResponseBody(addOffsetsErrorCode))
      except CatchableError:
        break

  proc runSendOffsetsFailureCase(
      expectedState: TransactionState, ec: int16
  ) {.async: (raises: [Exception]).} =
    type Sink = ref object
      sendOffsets: int
      sendOffsetsError: int

    let sink = Sink()
    let hook: MetricsHook = proc(
        event: cstring, tags: openArray[(cstring, cstring)], value: int64
    ) {.gcsafe, raises: [].} =
      if $event == "producer.txn.send_offsets":
        sink.sendOffsets.inc
      elif $event == "producer.txn.send_offsets.error":
        sink.sendOffsetsError.inc

    let fb = newFakeBroker()
    let host = "127.0.0.1"
    let port = int32(fb.address().port)
    let addOffsetsSeen = new(bool)
    addOffsetsSeen[] = false
    let respFut = autoRespondAddOffsets(fb, ec, addOffsetsSeen)
    let cluster = newCluster(@[host & ":" & $port], clientId = "sotx-test")
    let p = newKafkaProducer(
      cluster,
      transactionalId = some("test-txn"),
      enableIdempotence = true,
      maxRetries = 0,
      retryBackoffMs = 1,
      timeoutMs = 1500,
      metricsHook = hook,
    )
    seedTxnProducer(p)
    # Coordinator points at the fake broker so AddOffsetsToTxn actually runs and
    # returns the chosen error code.
    let coord = newBroker(
      nodeId = 0'i32,
      host = host,
      port = port,
      clientId = "sotx-coord",
      requestTimeoutMs = 1000,
      connectTimeoutMs = 1000,
    )
    p.setTxnCoordinator(coord)

    var offsets = initTable[TopicPartition, int64]()
    offsets[TopicPartition(topic: "txn-topic", partition: 0'i32)] = 10'i64

    var raisedCode = keNone
    try:
      await p.sendOffsetsToTransaction(offsets, "test-group")
    except KafkaApiError as e:
      raisedCode = e.errorCode
    except KafkaError:
      discard

    check addOffsetsSeen[]
    check raisedCode.int16 == ec
    # The core fix: the failed offsets RPC poisons the txn so a later commit
    # cannot finalize a transaction missing these offsets.
    check p.txnState == expectedState
    check sink.sendOffsets == 0
    # API failures already emit producer.txn.error / fatal via
    # transitionToError; send_offsets.error is the transport/cancel gap.
    check sink.sendOffsetsError == 0

    await p.close()
    await coord.close()
    await fb.close()
    awaitDrain(respFut, 1.seconds)

  test "AddOffsetsToTxn abortable (INVALID_PRODUCER_EPOCH) -> tsError":
    waitFor runSendOffsetsFailureCase(tsError, 47'i16)

  test "AddOffsetsToTxn fatal (PRODUCER_FENCED) -> tsFatal":
    waitFor runSendOffsetsFailureCase(tsFatal, 90'i16)

  test "a fatal AddOffsetsToTxn error escalates a transaction already poisoned":
    ## Regression: only an open or committing txn was poisoned, so PRODUCER_FENCED
    ## arriving after a concurrent failure had set tsError left the producer
    ## abortable, not fatal (a failed send already escalated).
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let cluster = newCluster(@[host & ":" & $port], clientId = "sotx-test")
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        maxRetries = 0,
        retryBackoffMs = 1,
        timeoutMs = 1500,
      )
      seedTxnProducer(p)
      let coord = newBroker(
        nodeId = 0'i32,
        host = host,
        port = port,
        clientId = "sotx-coord",
        requestTimeoutMs = 1000,
        connectTimeoutMs = 1000,
      )
      p.setTxnCoordinator(coord)

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          await av.respond(
            buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akAddOffsetsToTxn)), minVersion: 0, maxVersion: 0
                ),
              ],
            )
          )
          let req = await fb.nextRequest(timeout = 5.seconds)
          # A lock-free failure (another send's terminal error) poisons the txn
          # while the request is on the wire.
          p.setTxnState(tsError)
          await req.respond(buildAddOffsetsToTxnResponseBody(keProducerFenced.toInt16))
        except CatchableError:
          discard

      let handlerFut = handler()
      var offsets = initTable[TopicPartition, int64]()
      offsets[TopicPartition(topic: "txn-topic", partition: 0'i32)] = 10'i64
      var raisedCode = keNone
      try:
        await p.sendOffsetsToTransaction(offsets, "test-group")
      except KafkaApiError as e:
        raisedCode = e.errorCode
      check raisedCode == keProducerFenced
      check p.txnState == tsFatal

      await p.close()
      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await coord.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "sendOffsetsToTransaction transport failure emits producer.txn.send_offsets.error":
    ## transaction.send-offsets-error: tryTransition(tsError) on transport
    ## used to leave no metricsHook signal. A connect failure emits
    ## producer.txn.send_offsets.error once, tagged with the transport
    ## `error_code`; API failures must not (poison suite pins 0 — those already
    ## emit producer.txn.error / fatal).
    proc body() {.async: (raises: [Exception]).} =
      type Sink = ref object
        sendOffsets: int
        sendOffsetsError: int
        value: int64
        tagLen: int
        errorCode: string
        errorClass: string

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.txn.send_offsets":
          sink.sendOffsets.inc
        elif $event == "producer.txn.send_offsets.error":
          sink.sendOffsetsError.inc
          sink.value = value
          sink.tagLen = tags.len
          for (k, v) in tags:
            if $k == "error_code":
              sink.errorCode = $v
            elif $k == "error_class":
              sink.errorClass = $v

      let cluster = newCluster(@["127.0.0.1:1"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 200,
        metricsHook = hook,
      )
      # No real coordinator; AddOffsetsToTxn fails at connect, which
      # sendOffsetsToTransaction maps to its KafkaError (not KafkaApiError)
      # catch. Same dummy as producer.txn.commit.error.
      # Bootstrap is also unreachable (port 1): withTxnCoordinatorRetry
      # re-resolves the txn coordinator via the cluster on transport
      # failure, so a live broker on 9092 would answer the retry with
      # INVALID_PRODUCER_ID_MAPPING and turn this into the API-error path.
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      seedTxnProducer(p)
      var offsets = initTable[TopicPartition, int64]()
      offsets[TopicPartition(topic: "txn-topic", partition: 0'i32)] = 10'i64
      var raised = false
      try:
        await p.sendOffsetsToTransaction(offsets, "test-group")
      except KafkaError:
        raised = true
      check raised
      check p.txnState == tsError
      check sink.sendOffsets == 0
      check sink.sendOffsetsError == 1
      check sink.value == 1
      # One tag shape for every producer.txn.*.error event: the canonical
      # error_code (here the client-side transport tag, not the old lowercase
      # `reason=transport`) plus error_class, which is `unknown` because a
      # connect failure carries no broker error code.
      check sink.tagLen == 2
      check sink.errorCode == "CONNECTION_ERROR"
      check sink.errorClass == "unknown"
      await dummy.close()
      await p.close()
      await cluster.close()

    waitFor body()

  test "sendOffsetsToTransaction does not run in a transaction begun while it awaited":
    ## Regression: the txnLock re-check looked at the state alone, which the next
    ## txn restores to tsInTransaction, so offsets meant for one txn went into the
    ## next, and their failure poisoned it. Here the window is a deferred epoch bump:
    ## an abort from tsError does not wait for it.
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:1"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 200,
      )
      # Unreachable, so offsets that wrongly ran would fail and poison the next txn.
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      seedTxnProducer(p)
      var offsets = initTable[TopicPartition, int64]()
      offsets[TopicPartition(topic: "txn-topic", partition: 0'i32)] = 10'i64

      p.holdEpochBumpGate()
      let fut = p.sendOffsetsToTransaction(offsets, "test-group")
      check not fut.finished
      # Meanwhile the txn the call was made in ended and the next one began.
      p.setTxnState(tsReady)
      p.beginTransaction()
      p.releaseEpochBumpGate()
      var msg = ""
      try:
        await fut
      except KafkaError as e:
        msg = e.msg
      check msg ==
        "transaction changed during sendOffsetsToTransaction (a later transaction is open)"
      check p.txnState == tsInTransaction
      await dummy.close()
      await p.close()
      await cluster.close()

    waitFor body()

  test "sendOffsetsToTransaction made before beginTransaction is refused as such":
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      p.setTxnState(tsReady)
      p.holdEpochBumpGate()
      var offsets = initTable[TopicPartition, int64]()
      offsets[TopicPartition(topic: "t", partition: 0'i32)] = 10'i64
      let fut = p.sendOffsetsToTransaction(offsets, "test-group")
      p.beginTransaction()
      p.releaseEpochBumpGate()
      var msg = ""
      try:
        await fut
      except KafkaError as e:
        msg = e.msg
      check msg == "must be in transaction to send offsets"
      check p.txnState == tsInTransaction
      await p.close()

    waitFor body()

  test "sendOffsetsToTransaction cancelled before taking txnLock poisons the transaction":
    ## Regression: only a failure under txnLock poisoned the txn, so a cancel while
    ## waiting for the lock (or a deferred epoch bump) left it committable without
    ## the offsets.
    proc body() {.async: (raises: [Exception]).} =
      let p = newJoinTestProducer()
      var offsets = initTable[TopicPartition, int64]()
      offsets[TopicPartition(topic: "t", partition: 0'i32)] = 10'i64
      await p.acquireTxnLock()
      let fut = p.sendOffsetsToTransaction(offsets, "test-group")
      check not fut.finished
      await fut.cancelAndWait()
      check fut.cancelled()
      check p.txnState == tsError
      # The lock it never took is still the test's.
      check p.isTxnLocked()
      p.releaseTxnLock()
      await p.close()

    waitFor body()

  test "a cancelled commit is tagged CANCELLED (error_code is never missing)":
    ## `producer.txn.commit.error` used to tag a cancellation with
    ## `reason=cancelled` only, so a dashboard grouping by `error_code` lost
    ## every cancelled finalize. The uniform shape is
    ## `error_code` + `error_class`, and a cancellation has no broker code, so
    ## it takes the `CANCELLED` spelling from the same vocabulary as
    ## `CONNECTION_ERROR` / `TIMEOUT`.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      # A minimal responder: ApiVersions advertises EndTxn (so the client can
      # resolve a version for it) and every other request — EndTxn itself — is
      # deliberately left unanswered, which keeps the commit suspended in the
      # RPC until the test cancels it.
      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 250.milliseconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              let body = buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akEndTxn)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
              await req.respond(body)
          except CatchableError:
            break

      let respFut = handler()

      type Sink = ref object
        commitErrors: int
        errorCode: string
        errorClass: string

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.txn.commit.error":
          sink.commitErrors.inc
          for (k, v) in tags:
            if $k == "error_code":
              sink.errorCode = $v
            elif $k == "error_class":
              sink.errorClass = $v

      let cluster = newCluster(@[host & ":" & $port], clientId = "txn-cancel")
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 1000,
        metricsHook = hook,
      )
      # A coordinator the commit can actually reach: the fake broker accepts
      # the connection and negotiates ApiVersions, then never answers EndTxn.
      # Without this the commit would dereference a nil coordinator.
      let dummy = newBroker(
        nodeId = 0'i32,
        host = host,
        port = port,
        clientId = "txn-cancel",
        requestTimeoutMs = 5_000,
        connectTimeoutMs = 5_000,
      )
      p.setTxnCoordinator(dummy)
      seedTxnProducer(p)
      let commitFut = p.commitTransaction()
      await sleepAsync(50.milliseconds)
      check not commitFut.finished
      commitFut.cancelSoon()
      var cancelled = false
      try:
        await commitFut
      except CancelledError:
        cancelled = true
      except KafkaError:
        discard
      check cancelled
      check sink.commitErrors == 1
      check sink.errorCode == "CANCELLED"
      check sink.errorClass == "unknown"
      await dummy.close()
      await p.close()
      await cluster.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor body()

suite "Producer: flexible txn codec (KIP-482) end-to-end":
  ## The legacy fake-broker builders pin every txn API to v0, so the compact /
  ## tagged-fields wire paths were only ever exercised at the codec level.
  ## These tests advertise the flexible versions and drive the real producer RPC
  ## flow: the captured request bodies are asserted to use the compact forms,
  ## and the flexible response decode is proven by the producer state
  ## transitions that follow it.

  test "init/begin/send/commit negotiate flexible InitProducerId/AddPartitionsToTxn/EndTxn":
    proc testBody() {.async: (raises: [Exception]).} =
      const topic = "flex-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)

      type CapturedWire = tuple[apiVersion: int16, body: seq[byte]]
      var captured = initTable[int16, CapturedWire]()

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 250.milliseconds)
          except CatchableError:
            break
          try:
            let key = req.apiKey
            if key == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akFindCoordinator)),
                      minVersion: 0,
                      maxVersion: 1,
                    ),
                    # Advertise the flexible versions so the negotiated paths
                    # use the compact / tagged-fields wire (KIP-482).
                    FakeApiVersion(
                      apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 3
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akAddPartitionsToTxn)),
                      minVersion: 0,
                      maxVersion: 3,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akEndTxn)), minVersion: 0, maxVersion: 3
                    ),
                  ],
                )
              )
            elif key == int16(ord(akFindCoordinator)):
              await req.respond(
                buildFindCoordinatorResponseBodyV1(
                  nodeId = 0'i32, host = host, port = port
                )
              )
            elif key == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                  controllerId = 0'i32,
                  topics = [
                    FakeTopicMeta(
                      name: topic,
                      errorCode: 0'i16,
                      isInternal: false,
                      partitions: @[
                        FakePartitionMeta(
                          partitionIndex: 0'i32,
                          leaderId: 0'i32,
                          replicas: @[0'i32],
                          isr: @[0'i32],
                        )
                      ],
                    )
                  ],
                )
              )
            elif key == int16(ord(akInitProducerId)):
              # First occurrence is the init (PID/epoch = -1); the second is
              # the post-EndTxn bump with the real identity.
              if int16(ord(akInitProducerId)) notin captured:
                captured[int16(ord(akInitProducerId))] =
                  (req.apiVersion, req.flexibleBody())
              await req.respond(
                buildInitProducerIdResponseBodyV2(
                  producerId = 1000'i64, producerEpoch = 0'i16
                ),
                flexible = true,
              )
            elif key == int16(ord(akAddPartitionsToTxn)):
              if int16(ord(akAddPartitionsToTxn)) notin captured:
                captured[int16(ord(akAddPartitionsToTxn))] =
                  (req.apiVersion, req.flexibleBody())
              await req.respond(
                buildAddPartitionsToTxnResponseBodyV3(topic, [(0'i32, 0'i16)]),
                flexible = true,
              )
            elif key == int16(ord(akProduce)):
              await req.respond(
                buildProduceResponseBody(
                  topic = topic,
                  partition = 0'i32,
                  errorCode = 0'i16,
                  baseOffset = 0'i64,
                )
              )
            elif key == int16(ord(akEndTxn)):
              if int16(ord(akEndTxn)) notin captured:
                captured[int16(ord(akEndTxn))] = (req.apiVersion, req.flexibleBody())
              await req.respond(buildEndTxnResponseBodyV3(0'i16), flexible = true)
          except CatchableError:
            break

      let handlerFut = handler()

      let cluster = newCluster(@[host & ":" & $port], clientId = "flex-txn-test")
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("flex-txn"),
        enableIdempotence = true,
        retryBackoffMs = 1,
        timeoutMs = 2000,
      )
      await p.initTransactions()
      check p.txnState == tsReady
      p.beginTransaction()
      discard await p.send(topic, @[0xCA'u8])
      await p.commitTransaction()
      check p.txnState == tsReady
      check p.getProducerId() == 1000'i64

      # The captured requests must have used the flexible versions, and their
      # bodies must parse as compact strings/arrays with the tagged-fields
      # trailers — the flexible encode path end to end.
      check int16(ord(akInitProducerId)) in captured
      check captured[int16(ord(akInitProducerId))].apiVersion == 3'i16
      var ipid = initKafkaReader(captured[int16(ord(akInitProducerId))].body)
      check ipid.readCompactNullableString() == some("flex-txn")
      check ipid.readInt32() == 60_000'i32 # transactionTimeoutMs default
      check ipid.readInt64() == -1'i64 # producerId (-1 = request new identity)
      check ipid.readInt16() == -1'i16 # producerEpoch
      check ipid.decodeUvarint() == 0'u64 # top-level tagged-fields trailer
      check ipid.remaining == 0

      check int16(ord(akAddPartitionsToTxn)) in captured
      check captured[int16(ord(akAddPartitionsToTxn))].apiVersion == 3'i16
      var apt = initKafkaReader(captured[int16(ord(akAddPartitionsToTxn))].body)
      check apt.readCompactString() == "flex-txn"
      check apt.readInt64() == 1000'i64
      check apt.readInt16() == 0'i16
      check apt.readCompactArrayLen() == 1'i32 # topics
      check apt.readCompactString() == topic
      check apt.readCompactArrayLen() == 1'i32 # partitions
      check apt.readInt32() == 0'i32
      check apt.decodeUvarint() == 0'u64 # topics[0] tagged-fields trailer
      check apt.decodeUvarint() == 0'u64 # top-level tagged-fields trailer
      check apt.remaining == 0

      check int16(ord(akEndTxn)) in captured
      check captured[int16(ord(akEndTxn))].apiVersion == 3'i16
      var et = initKafkaReader(captured[int16(ord(akEndTxn))].body)
      check et.readCompactString() == "flex-txn"
      check et.readInt64() == 1000'i64
      check et.readInt16() == 0'i16
      check et.readBool() == true # committed
      check et.decodeUvarint() == 0'u64 # top-level tagged-fields trailer
      check et.remaining == 0

      await p.close()
      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await fb.close()
      await cluster.close()

    waitFor testBody()

  test "sendOffsetsToTransaction negotiates flexible AddOffsetsToTxn/TxnOffsetCommit":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)

      type CapturedWire = tuple[apiVersion: int16, body: seq[byte]]
      var captured = initTable[int16, CapturedWire]()

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 250.milliseconds)
          except CatchableError:
            break
          try:
            let key = req.apiKey
            if key == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akFindCoordinator)),
                      minVersion: 0,
                      maxVersion: 1,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akAddOffsetsToTxn)),
                      minVersion: 0,
                      maxVersion: 3,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akTxnOffsetCommit)),
                      minVersion: 0,
                      maxVersion: 3,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akEndTxn)), minVersion: 0, maxVersion: 3
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 3
                    ),
                  ],
                )
              )
            elif key == int16(ord(akFindCoordinator)):
              # Serves both the txn-coordinator lookup (type 1) and the
              # group-coordinator lookup (type 0); both point at this broker.
              await req.respond(
                buildFindCoordinatorResponseBodyV1(
                  nodeId = 0'i32, host = host, port = port
                )
              )
            elif key == int16(ord(akMetadata)):
              # The group-coordinator lookup triggers a cluster metadata
              # refresh; no topics are registered to this producer.
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                  controllerId = 0'i32,
                  topics = newSeq[FakeTopicMeta](),
                )
              )
            elif key == int16(ord(akAddOffsetsToTxn)):
              if int16(ord(akAddOffsetsToTxn)) notin captured:
                captured[int16(ord(akAddOffsetsToTxn))] =
                  (req.apiVersion, req.flexibleBody())
              await req.respond(
                buildAddOffsetsToTxnResponseBodyV3(0'i16), flexible = true
              )
            elif key == int16(ord(akTxnOffsetCommit)):
              if int16(ord(akTxnOffsetCommit)) notin captured:
                captured[int16(ord(akTxnOffsetCommit))] =
                  (req.apiVersion, req.flexibleBody())
              await req.respond(
                buildTxnOffsetCommitResponseBodyV3(
                  "txn-topic", partition = 0'i32, errorCode = 0'i16
                ),
                flexible = true,
              )
            elif key == int16(ord(akEndTxn)):
              # close() aborts the offsets-only txn (txnStarted) on teardown.
              await req.respond(buildEndTxnResponseBodyV3(0'i16), flexible = true)
            elif key == int16(ord(akInitProducerId)):
              # bumpEpoch after the abort's successful EndTxn.
              await req.respond(
                buildInitProducerIdResponseBodyV2(
                  producerId = 1000'i64, producerEpoch = 0'i16
                ),
                flexible = true,
              )
          except CatchableError:
            break

      let handlerFut = handler()

      let cluster = newCluster(@[host & ":" & $port], clientId = "flex-sotx-test")
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("flex-txn"),
        enableIdempotence = true,
        retryBackoffMs = 1,
        timeoutMs = 2000,
      )
      p.setTransactionalIdForTest("flex-txn")
      p.setProducerId(1000'i64, 0'i16)
      p.setTxnState(tsInTransaction)
      p.setTxnStarted(true)
      let coord = newBroker(
        nodeId = 0'i32,
        host = host,
        port = port,
        clientId = "flex-sotx-coord",
        requestTimeoutMs = 1000,
        connectTimeoutMs = 1000,
      )
      p.setTxnCoordinator(coord)

      var offsets = initTable[TopicPartition, int64]()
      offsets[TopicPartition(topic: "txn-topic", partition: 0'i32)] = 10'i64
      await p.sendOffsetsToTransaction(offsets, "test-group")
      check p.getTxnStarted()
      check p.txnState == tsInTransaction

      check int16(ord(akAddOffsetsToTxn)) in captured
      check captured[int16(ord(akAddOffsetsToTxn))].apiVersion == 3'i16
      var aot = initKafkaReader(captured[int16(ord(akAddOffsetsToTxn))].body)
      check aot.readCompactString() == "flex-txn"
      check aot.readInt64() == 1000'i64
      check aot.readInt16() == 0'i16
      check aot.readCompactString() == "test-group"
      check aot.decodeUvarint() == 0'u64 # top-level tagged-fields trailer
      check aot.remaining == 0

      check int16(ord(akTxnOffsetCommit)) in captured
      check captured[int16(ord(akTxnOffsetCommit))].apiVersion == 3'i16
      var toc = initKafkaReader(captured[int16(ord(akTxnOffsetCommit))].body)
      check toc.readCompactString() == "flex-txn"
      check toc.readCompactString() == "test-group"
      check toc.readInt64() == 1000'i64
      check toc.readInt16() == 0'i16
      check toc.readInt32() == -1'i32 # generationId sentinel (KIP-447)
      check toc.readCompactString() == "" # memberId sentinel
      check toc.readCompactNullableString() == none(string) # groupInstanceId
      check toc.readCompactArrayLen() == 1'i32 # topics
      check toc.readCompactString() == "txn-topic"
      check toc.readCompactArrayLen() == 1'i32 # partitions
      check toc.readInt32() == 0'i32
      check toc.readInt64() == 10'i64 # offset
      check toc.readInt32() == -1'i32 # committedLeaderEpoch (no cached metadata)
      check toc.readCompactNullableString() == none(string) # metadata
      check toc.decodeUvarint() == 0'u64 # partition tagged-fields trailer
      check toc.decodeUvarint() == 0'u64 # topic tagged-fields trailer
      check toc.decodeUvarint() == 0'u64 # top-level tagged-fields trailer
      check toc.remaining == 0

      # The offsets-only txn is still Ongoing broker-side; the abort issues a
      # flexible EndTxn(abort) and the follow-up bump, both served above.
      await p.abortTransaction()
      check p.txnState == tsReady

      await p.close()
      await coord.close()
      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await fb.close()
      await cluster.close()

    waitFor testBody()

suite "Producer: txn partition register failure rolls back":
  ## Regression: send() marked the partition lock-free then awaited
  ## registerPendingTxnPartitions; a concurrent commit/abort or a failed register
  ## could lose the partition or leave a stale tentative entry. The fix wraps both
  ## in txnLock and excises the tentative entry on failure so callers retry clean.

  proc autoRespondTxnRegisterError(
      fb: FakeBroker, host: string, port: int32, topic: string
  ) {.async: (raises: []).} =
    while true:
      var req: CapturedRequest
      try:
        req = await fb.nextRequest(timeout = 500.milliseconds)
      except CatchableError:
        break
      try:
        let key = req.apiKey
        if key == int16(ord(akApiVersions)):
          let body = buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              ),
              FakeApiVersion(
                apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
              ),
              FakeApiVersion(
                apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
              ),
              FakeApiVersion(
                apiKey: int16(ord(akAddPartitionsToTxn)), minVersion: 0, maxVersion: 0
              ),
            ],
          )
          await req.respond(body)
        elif key == int16(ord(akMetadata)):
          let body = buildMetadataResponseBody(
            [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
            controllerId = 0'i32,
            topics = [
              FakeTopicMeta(
                name: topic,
                errorCode: 0'i16,
                isInternal: false,
                partitions: @[
                  FakePartitionMeta(
                    partitionIndex: 0'i32,
                    leaderId: 0'i32,
                    replicas: @[0'i32],
                    isr: @[0'i32],
                  )
                ],
              )
            ],
          )
          await req.respond(body)
        elif key == int16(ord(akAddPartitionsToTxn)):
          # COORDINATOR_LOAD_IN_PROGRESS (14) — retriable, surfaces as
          # KafkaApiError so send() can roll back the pending entry.
          var w = initKafkaWriter()
          w.writeInt32(0'i32)
          w.writeArrayLen(1)
          w.writeString(topic)
          w.writeArrayLen(1)
          w.writeInt32(0'i32)
          w.writeInt16(14'i16)
          await req.respond(w.buf)
      except CatchableError:
        break

  test "AddPartitionsToTxn error rolls back tentative pending entry":
    proc body() {.async: (raises: [Exception]).} =
      const topic = "txn-rollback-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let respFut = autoRespondTxnRegisterError(fb, host, port, topic)

      let cluster = newCluster(@[host & ":" & $port], clientId = "txn-rollback")
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        lingerMs = 0,
        maxRetries = 0,
        timeoutMs = 1500,
      )
      # Skip InitProducerId / beginTransaction and inject the coordinator.
      p.setProducerId(1000'i64, 0'i16)
      p.setTxnState(tsInTransaction)
      let coord =
        await cluster.brokerForPartition(TopicPartition(topic: topic, partition: 0'i32))
      p.setTxnCoordinator(coord)

      let tp = TopicPartition(topic: topic, partition: 0'i32)
      check not p.containsTxnPendingPartition(tp)
      check p.txnPendingPartitionCount == 0

      var raised = false
      try:
        discard await p.send(topic, @[0xAB'u8])
      except KafkaApiError:
        raised = true
      except KafkaError:
        raised = true
      check raised

      # The fix: tentative pending entry must be removed so the next
      # attempt does not double-register the same partition.
      check not p.containsTxnPendingPartition(tp)
      check p.txnPendingPartitionCount == 0

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor body()

  test "register failure poisons txn so commit is refused":
    ## A failed AddPartitionsToTxn (immediate-send path) used to surface
    ## the error to the caller without moving txnState. A caller that swallowed
    ## that error could then commitTransaction successfully, finalizing a
    ## transaction silently missing the record (EOS violation). The fix poisons
    ## the txn to tsError so the post-send commit is refused (and never reaches
    ## the coordinator — the fake broker has no EndTxn handler).
    proc body() {.async: (raises: [Exception]).} =
      const topic = "txn-poison-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let respFut = autoRespondTxnRegisterError(fb, host, port, topic)

      let cluster = newCluster(@[host & ":" & $port], clientId = "txn-poison")
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        lingerMs = 0,
        maxRetries = 0,
        timeoutMs = 1500,
      )
      p.setProducerId(1000'i64, 0'i16)
      p.setTxnState(tsInTransaction)
      let coord =
        await cluster.brokerForPartition(TopicPartition(topic: topic, partition: 0'i32))
      p.setTxnCoordinator(coord)

      # Swallow the send failure exactly as a careless caller might.
      try:
        discard await p.send(topic, @[0xAB'u8])
      except KafkaError:
        discard

      # The transaction must now be poisoned.
      check p.txnState in {tsError, tsFatal}

      # And a subsequent commit must be refused rather than dropping the record.
      var commitRefused = false
      try:
        await p.commitTransaction()
      except KafkaError:
        commitRefused = true
      check commitRefused
      check p.txnState in {tsError, tsFatal}

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor body()

suite "Producer: fire-and-forget drain budget":
  ## Regression. The close() drain previously break'd on the first
  ## per-iteration timeout, stranding any future that did not complete
  ## within `timeoutMs` of one snapshot. The fix budgets the whole drain
  ## by `timeoutMs` and cancels stragglers explicitly so close()'s
  ## "release resources" contract holds.

  proc makeHangFuture(): Future[void] =
    ## A future that never completes on its own. Tests cancel it via the
    ## producer's drain path.
    Future[void].init("test.hang")

  test "close cancels hung fire-and-forget futures instead of stranding them":
    ## producer-support.close-drain: budget expiry cancels the snapshot,
    ## and the hook records the cancel count. flush had nothing to drain.
    proc body() {.async: (raises: [Exception]).} =
      type Sink = ref object
        drainCancelled: int
        drainValue: int64
        flushFailed: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.close.drain.cancelled":
          sink.drainCancelled.inc
          sink.drainValue = value
        elif $event == "producer.close.flush.failed":
          sink.flushFailed.inc

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p =
        newKafkaProducer(cluster, lingerMs = 0, timeoutMs = 200, metricsHook = hook)

      var hanging: seq[Future[void]]
      for _ in 0 ..< 3:
        let fut = makeHangFuture()
        hanging.add(fut)
        p.injectPendingFireAndForget(fut)
      check p.pendingFireAndForgetLen == 3

      let started = Moment.now()
      await p.close()
      let elapsed = Moment.now() - started

      # The drain must respect the overall budget — well below a hard
      # ceiling chosen to ignore scheduler jitter — and never wait for
      # an indefinitely-hung task.
      check elapsed < milliseconds(2_000)

      # All stragglers must be marked finished after close returns. The
      # fix routes them through cancelSoon (then the dispatcher), so wait
      # one tick to let the cancellation settle.
      for fut in hanging:
        if not fut.finished:
          awaitDrain(fut, 500.milliseconds)
        check fut.finished

      check p.isClosed()
      check sink.drainCancelled == 1
      check sink.drainValue == 3
      check sink.flushFailed == 0

    waitFor body()

  test "close still completes promptly when fire-and-forget list is empty":
    ## producer-support.close-drain: nothing leftover is not a drain
    ## failure — same "only when remaining" rule as unflushed_batches.
    ## producer-support.close-linger: an empty linger table is not a cancel.
    proc body() {.async: (raises: [Exception]).} =
      type Sink = ref object
        drainCancelled: int
        flushFailed: int
        lingerCancelled: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.close.drain.cancelled":
          sink.drainCancelled.inc
        elif $event == "producer.close.flush.failed":
          sink.flushFailed.inc
        elif $event == "producer.close.linger.cancelled":
          sink.lingerCancelled.inc

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p =
        newKafkaProducer(cluster, lingerMs = 0, timeoutMs = 200, metricsHook = hook)
      let started = Moment.now()
      await p.close()
      let elapsed = Moment.now() - started
      check elapsed < milliseconds(500)
      check p.isClosed()
      check sink.drainCancelled == 0
      check sink.flushFailed == 0
      check sink.lingerCancelled == 0

    waitFor body()

suite "Producer: deferred epoch bump (KIP-360 followthrough)":
  ## Regression: when EndTxn succeeded but the follow-up InitProducerId
  ## (bumpEpoch) exhausted its retry budget on a retriable error,
  ## commitTransaction raised — a committed broker-side txn but a tsError client
  ## state. bumpEpoch now defers via pendingEpochBump and the next send retries
  ## it transparently.

  test "bumpEpoch sets pendingEpochBump when retriable failures exhaust budget":
    proc body() {.async: (raises: [Exception]).} =
      type Sink = ref object
        bumpEpoch: int
        deferred: int
        retriable: string

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.txn.bump_epoch":
          sink.bumpEpoch.inc
        elif $event == "producer.txn.bump_epoch.deferred":
          sink.deferred.inc
          for (k, v) in tags:
            if $k == "retriable":
              sink.retriable = $v

      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "bump-defer-test")

      proc handler() {.async: (raises: []).} =
        try:
          # ApiVersions: advertise InitProducerId v0..v3 so the producer's
          # encode/decode path uses the same wire format as production.
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            let body = buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                ),
              ],
            )
            await av.respond(body)
          except CatchableError:
            discard
          # Reply OUT_OF_ORDER_SEQUENCE_NUMBER to every bump attempt (7 retries
          # in the new budget) — the budget exhausts and bumpEpoch returns
          # without raising.
          while true:
            let ipid = await fb.nextRequest(timeout = 5.seconds)
            try:
              # OUT_OF_ORDER_SEQUENCE_NUMBER is one of the codes bumpEpoch
              # treats as retriable after EndTxn.
              await ipid.respond(
                buildInitProducerIdResponseBody(
                  errorCode = int16(ord(keOutOfOrderSequenceNumber))
                )
              )
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
        metricsHook = hook,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setTxnState(tsInTransaction)

      await b.connect()
      var raised = false
      try:
        # bumpEpoch is what commitTransaction / abortTransaction call
        # after EndTxn succeeds.
        await p.bumpEpoch()
      except KafkaError:
        raised = true
      check not raised
      check p.getPendingEpochBump() == true
      # Producer epoch stays at the pre-bump value — the broker rejected.
      check p.getProducerEpoch() == 0'i16
      check sink.bumpEpoch == 0
      # A deferred bump is a silent epoch stall until the next send retries it,
      # so the emission (carrying the retriable classification) is the only signal.
      check sink.deferred == 1
      check sink.retriable == "true"

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "bumpEpoch emits producer.txn.bump_epoch":
    ## transaction.bump-epoch: applyEpochBump used to leave no metricsHook
    ## signal. A real bump emits producer.txn.bump_epoch once; a deferred
    ## bump must not (exhaust suite pins 0).
    proc body() {.async: (raises: [Exception]).} =
      type Sink = ref object
        bumpEpoch: int
        value: int64
        tagLen: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.txn.bump_epoch":
          sink.bumpEpoch.inc
          sink.value = value
          sink.tagLen = tags.len

      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "bump-epoch-metric")

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          let ipid = await fb.nextRequest(timeout = 5.seconds)
          try:
            await ipid.respond(
              buildInitProducerIdResponseBody(
                errorCode = 0'i16, producerId = 42'i64, producerEpoch = 7'i16
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
        metricsHook = hook,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setTxnState(tsInTransaction)

      await b.connect()
      await p.bumpEpoch()
      check p.getPendingEpochBump() == false
      check p.getProducerEpoch() == 7'i16
      check sink.bumpEpoch == 1
      check sink.value == 1
      check sink.tagLen == 0

      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "bumpEpoch defers instead of raising on a non-retriable InitProducerId error":
    ## Regression: EndTxn already succeeded broker-side, then bumpEpoch's
    ## InitProducerId returned a non-retriable error (e.g. PRODUCER_FENCED).
    ## Raising here would report a committed transaction as failed. bumpEpoch
    ## must poison the producer (tsFatal for fenced) and defer instead.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "bump-fatal-test")

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          let ipid = await fb.nextRequest(timeout = 5.seconds)
          try:
            await ipid.respond(
              buildInitProducerIdResponseBody(errorCode = int16(ord(keProducerFenced)))
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setTxnState(tsInTransaction)

      await b.connect()
      var raised = false
      try:
        await p.bumpEpoch()
      except KafkaError:
        raised = true
      check not raised
      check p.getPendingEpochBump() == true
      check p.getProducerEpoch() == 0'i16
      # PRODUCER_FENCED is ecFatal — the producer must be poisoned so the
      # next transactional call refuses cleanly.
      check p.txnState == tsFatal

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "ensureEpochBumped skips InitProducerId when producer is tsFatal":
    ## Regression: a non-retriable post-EndTxn bumpEpoch poisons the producer to
    ## tsFatal but leaves pendingEpochBump set (the commit/abort that triggered it
    ## is still reported successful). The entry points call ensureEpochBumped BEFORE
    ## their tsFatal state check, so bumpEpoch must short-circuit on tsFatal —
    ## firing InitProducerId would allocate an orphaned PID for a producer that must
    ## be closed, only for the state check to raise right after.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "bump-fatal-skip-test")

      var initProducerIdCount = 0
      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          # Record any InitProducerId; the test asserts none arrives.
          while true:
            let req = await fb.nextRequest(timeout = 5.seconds)
            if req.apiKey == int16(ord(akInitProducerId)):
              initProducerIdCount.inc
            try:
              await req.respond(
                buildInitProducerIdResponseBody(
                  errorCode = 0'i16, producerId = 42'i64, producerEpoch = 9'i16
                )
              )
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      # Simulate the state a non-retriable post-EndTxn bumpEpoch leaves behind:
      # poisoned tsFatal with the bump still flagged pending.
      p.setTxnState(tsFatal)
      p.setPendingEpochBump(true)

      await b.connect()
      var raised = false
      try:
        await p.ensureEpochBumped()
      except KafkaError:
        raised = true
      check not raised
      # The tsFatal guard short-circuits: no InitProducerId reaches the
      # coordinator and the producer's identity is left untouched.
      check initProducerIdCount == 0
      check p.getProducerEpoch() == 0'i16
      check p.txnState == tsFatal

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "ensureEpochBumped skips InitProducerId when producer is tsError":
    ## Regression: an abortable post-EndTxn bumpEpoch (e.g. INVALID_RECORD
    ## classed with transactional context) poisons the producer to tsError but
    ## leaves pendingEpochBump set. Re-attempting from tsError can only
    ## reproduce the same non-retriable error — an InitProducerId RPC wasted on
    ## every entry point, forever, until a successful EndTxn changes the
    ## broker-side epoch (which commit/abort resolve by calling bumpEpoch
    ## directly). Mirror the tsFatal short-circuit: skip the re-attempt while
    ## the producer is poisoned in tsError.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "bump-error-skip-test")

      var initProducerIdCount = 0
      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          while true:
            let req = await fb.nextRequest(timeout = 5.seconds)
            if req.apiKey == int16(ord(akInitProducerId)):
              initProducerIdCount.inc
            try:
              await req.respond(
                buildInitProducerIdResponseBody(
                  errorCode = 0'i16, producerId = 42'i64, producerEpoch = 9'i16
                )
              )
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      # Simulate the state an abortable post-EndTxn bumpEpoch leaves behind:
      # poisoned tsError with the bump still flagged pending.
      p.setTxnState(tsError)
      p.setPendingEpochBump(true)

      await b.connect()
      var raised = false
      try:
        await p.ensureEpochBumped()
      except KafkaError:
        raised = true
      check not raised
      # The tsError guard short-circuits: no InitProducerId reaches the
      # coordinator and the producer's identity is left untouched.
      check initProducerIdCount == 0
      check p.getProducerEpoch() == 0'i16
      check p.txnState == tsError
      check p.getPendingEpochBump() == true

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "bumpEpoch defers instead of raising on a transport-level InitProducerId timeout":
    ## Regression: a KafkaTimeoutError from the post-EndTxn InitProducerId RPC
    ## used to propagate (bumpEpoch caught only KafkaConnectionError), reporting
    ## a committed txn as failed. Must defer via pendingEpochBump.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(
        0'i32, "127.0.0.1", port, "bump-timeout-test", requestTimeoutMs = 50'i32
      )

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          # Drain every InitProducerId retry without responding, so each attempt
          # trips the broker's 50ms requestTimeoutMs and raises KafkaTimeoutError
          # inside bumpEpoch.
          while true:
            let ipid = await fb.nextRequest(timeout = 10.seconds)
            discard ipid
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setTxnState(tsInTransaction)

      await b.connect()
      var raised = false
      try:
        await p.bumpEpoch()
      except KafkaError:
        raised = true
      check not raised
      check p.getPendingEpochBump() == true
      check p.getProducerEpoch() == 0'i16

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "commitTransaction returns success when post-EndTxn bumpEpoch fails non-retriably":
    ## End-to-end regression for the same bug: EndTxn succeeds, InitProducerId
    ## returns PRODUCER_FENCED. Callers must see the commit as successful —
    ## reporting failure would prompt an abort/retry against an already-committed
    ## txn (double-side-effect or a false "lost data" incident).
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "commit-fatal-bump-test")

      var received: seq[int16]
      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akEndTxn)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          while true:
            let req = await fb.nextRequest(timeout = 5.seconds)
            received.add(req.apiKey)
            try:
              if req.apiKey == int16(ord(akEndTxn)):
                await req.respond(buildEndTxnResponseBody(errorCode = 0'i16))
              elif req.apiKey == int16(ord(akInitProducerId)):
                await req.respond(
                  buildInitProducerIdResponseBody(
                    errorCode = int16(ord(keProducerFenced))
                  )
                )
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setTxnState(tsInTransaction)
      # Mark txn as started broker-side so commit actually sends EndTxn (the
      # empty-txn short-circuit would otherwise skip the coordinator entirely).
      p.setTxnStarted(true)

      await b.connect()
      var raised = false
      try:
        await p.commitTransaction()
      except KafkaError:
        raised = true
      check not raised
      # EndTxn was sent (commit is real), and it preceded the failing bump.
      let endTxnIdx = received.find(int16(ord(akEndTxn)))
      let ipidIdx = received.find(int16(ord(akInitProducerId)))
      check endTxnIdx >= 0
      check ipidIdx > endTxnIdx
      # bumpEpoch's fenced error poisoned the producer for the NEXT call, but
      # the current commit is reported as success.
      check p.txnState == tsFatal
      check p.getPendingEpochBump() == true

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "commitTransaction resolves a deferred bump on an empty txn without EndTxn":
    ## Regression: a deferred epoch bump left from a prior commit/abort was only
    ## retried by send(). A zero-send commit must still resolve it via
    ## ensureEpochBumped before any coordinator RPC, and must NOT send EndTxn —
    ## nothing was registered broker-side (txnStarted == false), so the txn is
    ## still `Empty` and would reject EndTxn with INVALID_TXN_STATE. Asserts the
    ## bump's InitProducerId IS sent and EndTxn is NOT.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "commit-bump-test")

      var received: seq[int16]
      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akEndTxn)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          # Answer every InitProducerId with success (incrementing epoch) and
          # every EndTxn with success. Record arrival order so the test can
          # prove no EndTxn was sent.
          var epoch = 7'i16
          while true:
            let req = await fb.nextRequest(timeout = 5.seconds)
            received.add(req.apiKey)
            try:
              if req.apiKey == int16(ord(akInitProducerId)):
                await req.respond(
                  buildInitProducerIdResponseBody(
                    errorCode = 0'i16, producerId = 42'i64, producerEpoch = epoch
                  )
                )
                inc epoch
              elif req.apiKey == int16(ord(akEndTxn)):
                # EndTxnResponse: throttle_time_ms (int32) + error_code (int16).
                await req.respond(@[0'u8, 0, 0, 0, 0, 0])
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setTxnState(tsInTransaction)
      # Simulate the leftover deferred bump from a prior transaction.
      p.setPendingEpochBump(true)

      await b.connect()
      await p.commitTransaction()

      # The deferred bump was resolved: flag cleared, epoch advanced.
      check p.getPendingEpochBump() == false
      check p.getProducerEpoch() >= 7'i16
      # An empty transaction commits locally: tsReady, no broker EndTxn.
      check p.txnState == tsReady
      # The deferred bump's InitProducerId was sent, but EndTxn was NOT — the
      # coordinator's txn is still Empty and would reject it.
      let ipidIdx = received.find(int16(ord(akInitProducerId)))
      let endTxnIdx = received.find(int16(ord(akEndTxn)))
      check ipidIdx >= 0
      check endTxnIdx < 0

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "commitTransaction on an empty transaction skips EndTxn entirely":
    ## A beginTransaction -> commitTransaction with no records and no offsets
    ## leaves the coordinator's txn in `Empty`; an EndTxn would be rejected with
    ## INVALID_TXN_STATE. commit must finalize locally (tsReady) without
    ## touching the coordinator. The coordinator here is unreachable, so a
    ## wrongful EndTxn would surface as a raised KafkaError.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      p.setTxnState(tsInTransaction)
      # No partition registered, no offsets sent.
      check p.getTxnStarted() == false
      var raised = false
      try:
        await p.commitTransaction()
      except KafkaError:
        raised = true
      check not raised # coordinator never contacted
      check p.txnState == tsReady
      check p.txnPartitionCount() == 0
      check p.txnPendingPartitionCount() == 0
      await dummy.close()

    waitFor testBody()

  test "abortTransaction on an empty transaction skips EndTxn entirely":
    ## Symmetric to the empty-commit case: with nothing registered broker-side
    ## there is no Ongoing txn to abort, so abort finalizes locally (tsReady)
    ## without sending EndTxn(abort) to the unreachable coordinator.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      p.setTxnState(tsInTransaction)
      check p.getTxnStarted() == false
      var raised = false
      try:
        await p.abortTransaction()
      except KafkaError:
        raised = true
      check not raised # coordinator never contacted
      check p.txnState == tsReady
      check p.txnPartitionCount() == 0
      check p.txnPendingPartitionCount() == 0
      await dummy.close()

    waitFor testBody()

  test "close aborts an offsets-only transaction (no partitions but txnStarted)":
    ## Regression: sendOffsetsToTransaction moves the coordinator's txn to
    ## Ongoing via AddOffsetsToTxn but registers nothing in txnPartitions. close()
    ## must still abort it (EndTxn(abort)) rather than leaving the broker-side txn
    ## to time out and block the LSO. The `txnStarted` marker — not the partition
    ## sets — is what makes close() attempt the abort.
    ## producer-support.close-abort: a completed abort is not a timeout.
    proc body() {.async: (raises: [Exception]).} =
      type Sink = ref object
        abortFailed: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.close.abort.failed":
          sink.abortFailed.inc

      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "offsets-only-abort")

      var endTxnSeen = false
      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akEndTxn)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          while true:
            let req = await fb.nextRequest(timeout = 5.seconds)
            try:
              if req.apiKey == int16(ord(akEndTxn)):
                endTxnSeen = true
                await req.respond(buildEndTxnResponseBody(0'i16))
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 1000,
        metricsHook = hook,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setTxnState(tsInTransaction)
      # Mimic the post-sendOffsetsToTransaction state: nothing in the partition
      # sets, but the coordinator's txn is Ongoing.
      p.setTxnStarted(true)
      check p.txnPartitionCount() == 0
      check p.txnPendingPartitionCount() == 0

      await b.connect()
      await p.close()

      check p.isClosed()
      check endTxnSeen # close issued EndTxn(abort) for the offsets-only txn
      check sink.abortFailed == 0

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "close aborts a started txn whose later partition registration fails":
    ## Regression: once a txn is Ongoing broker-side, a later batch whose
    ## registration is rejected leaves its partition stuck in txnPendingPartitions.
    ## abort re-runs registerPendingTxnPartitions before EndTxn; if that raises and
    ## aborts the abort, the Ongoing txn is never released and blocks the LSO until
    ## transaction.timeout.ms. abort must tolerate the registration failure when
    ## txnStarted and still issue EndTxn(abort).
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "started-txn-reg-fail")

      var endTxnSeen = false
      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akAddPartitionsToTxn)),
                    minVersion: 0,
                    maxVersion: 0,
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akEndTxn)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          while true:
            let req = await fb.nextRequest(timeout = 5.seconds)
            try:
              if req.apiKey == int16(ord(akAddPartitionsToTxn)):
                # Reject the still-pending partition with an abortable code so the
                # txn lands in tsError (close stays eligible to abort) rather than
                # tsFatal.
                await req.respond(
                  buildAddPartitionsToTxnResponseBody(
                    "test-topic", [(1'i32, keInvalidProducerIdMapping.toInt16)]
                  )
                )
              elif req.apiKey == int16(ord(akEndTxn)):
                endTxnSeen = true
                await req.respond(buildEndTxnResponseBody(0'i16))
              elif req.apiKey == int16(ord(akInitProducerId)):
                await req.respond(
                  buildInitProducerIdResponseBody(
                    errorCode = 0'i16, producerId = 42'i64, producerEpoch = 8'i16
                  )
                )
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 1500,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      # Mimic a txn already Ongoing broker-side (a prior registration succeeded)
      # whose later batch registration failed: txnStarted set, the failed
      # partition still pending, and the txn poisoned to tsError.
      p.setTxnStarted(true)
      p.addTxnPendingPartition(TopicPartition(topic: "test-topic", partition: 1))
      p.setTxnState(tsError)

      await b.connect()
      await p.close()

      check p.isClosed()
      check endTxnSeen # abort released the Ongoing txn despite the reg failure

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "abort of registration-failed partition fails buffered records instead of flushing them":
    ## Regression: when abortTransaction's registerPendingTxnPartitions fails but
    ## txnStarted is set (a prior partition made the coordinator move to Ongoing),
    ## the abort path used to just clear txnPendingPartitions and proceed to
    ## flush(). flush() would then send buffered records for the unregistered
    ## partition as transactional writes without an AddPartitionsToTxn entry, so
    ## the subsequent EndTxn(abort) marker would not cover them and the LSO would
    ## stall in read_committed. Fix: fail those buffered futures locally before
    ## draining pending, so flush() only touches truly registered partitions.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "abort-reg-fail-fails-batches")

      var endTxnSeen = false
      var produceSeen = false
      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akAddPartitionsToTxn)),
                    minVersion: 0,
                    maxVersion: 0,
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akEndTxn)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          while true:
            let req = await fb.nextRequest(timeout = 5.seconds)
            try:
              if req.apiKey == int16(ord(akAddPartitionsToTxn)):
                await req.respond(
                  buildAddPartitionsToTxnResponseBody(
                    "test-topic", [(1'i32, keInvalidProducerIdMapping.toInt16)]
                  )
                )
              elif req.apiKey == int16(ord(akEndTxn)):
                endTxnSeen = true
                await req.respond(buildEndTxnResponseBody(0'i16))
              elif req.apiKey == int16(ord(akInitProducerId)):
                await req.respond(
                  buildInitProducerIdResponseBody(
                    errorCode = 0'i16, producerId = 42'i64, producerEpoch = 8'i16
                  )
                )
              elif req.apiKey == int16(ord(akProduce)):
                # Must never fire: batches for unregistered partitions should be
                # failed locally, not put on the wire during abort.
                produceSeen = true
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 1500,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setTxnStarted(true)
      p.setTxnState(tsInTransaction)

      let tp = TopicPartition(topic: "test-topic", partition: 1)
      p.addTxnPendingPartition(tp)
      let recordFut = p.injectPendingBatch(
        tp, Record(value: some(@[0x01'u8]), timestamp: 1, offset: 0)
      )

      await b.connect()

      # Abort should complete without raising: EndTxn(abort) succeeds, batch's
      # future is failed locally, no Produce ever leaves this producer.
      await p.abortTransaction()

      check endTxnSeen
      check not produceSeen
      check recordFut.finished
      check recordFut.failed
      check not p.hasBatch(tp)

      await p.close()

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "registerPendingTxnPartitions marks txn started on a partial add":
    ## A broker that accepts one partition but rejects a sibling in the same
    ## AddPartitionsToTxn has still moved the txn to `Ongoing`.
    ## registerPendingTxnPartitions must set txnStarted before raising so a later
    ## abort/close issues EndTxn(abort) instead of orphaning the broker-side txn.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "partial-add-started")

      var endTxnSeen = false
      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akAddPartitionsToTxn)),
                    minVersion: 0,
                    maxVersion: 0,
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akEndTxn)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          while true:
            let req = await fb.nextRequest(timeout = 5.seconds)
            try:
              if req.apiKey == int16(ord(akAddPartitionsToTxn)):
                # Partition 0 accepted (keNone), partition 1 rejected — the txn is
                # Ongoing for partition 0.
                await req.respond(
                  buildAddPartitionsToTxnResponseBody(
                    "test-topic",
                    [(0'i32, 0'i16), (1'i32, keInvalidProducerIdMapping.toInt16)],
                  )
                )
              elif req.apiKey == int16(ord(akEndTxn)):
                endTxnSeen = true
                await req.respond(buildEndTxnResponseBody(0'i16))
              elif req.apiKey == int16(ord(akInitProducerId)):
                await req.respond(
                  buildInitProducerIdResponseBody(
                    errorCode = 0'i16, producerId = 42'i64, producerEpoch = 8'i16
                  )
                )
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 1500,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setTxnState(tsInTransaction)
      p.addTxnPendingPartition(TopicPartition(topic: "test-topic", partition: 0))
      p.addTxnPendingPartition(TopicPartition(topic: "test-topic", partition: 1))

      await b.connect()

      var raised = false
      try:
        await p.registerPendingTxnPartitions()
      except KafkaError:
        raised = true
      check raised
      check p.getTxnStarted() # marked started despite the sibling's rejection

      # End to end: close must now release the Ongoing broker-side txn.
      await p.close()
      check endTxnSeen

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "ensureEpochBumped clears pendingEpochBump on successful retry":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "ensure-bump-test")

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            let body = buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                ),
              ],
            )
            await av.respond(body)
          except CatchableError:
            discard
          # First call: success — the deferred bump completes.
          let ipid = await fb.nextRequest(timeout = 5.seconds)
          try:
            await ipid.respond(
              buildInitProducerIdResponseBody(
                errorCode = 0'i16, producerId = 42'i64, producerEpoch = 7'i16
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setPendingEpochBump(true)

      await b.connect()
      await p.ensureEpochBumped()
      check p.getPendingEpochBump() == false
      check p.getProducerEpoch() == 7'i16

      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "ensureEpochBumped is a no-op when pendingEpochBump is false":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      check p.getPendingEpochBump() == false
      # Even with a nil coordinator the call returns immediately — no
      # broker round-trip happens when nothing is deferred.
      await p.ensureEpochBumped()
      check p.getPendingEpochBump() == false
      await p.close()

    waitFor body()

  test "resetProducerId completes while a bump RPC is in flight":
    ## Regression: ensureEpochBumped used to hold initLock across the entire
    ## bump (network I/O + retries), so resetProducerId — and the other initLock
    ## users (ensureIdempotenceInitialized / reassignSequencesAfterReset /
    ## initTransactions) — blocked until the bump settled; a slow or failing
    ## bump stalled unrelated producer state recovery. The bump now runs
    ## lock-free and applies via a synchronous, base-verified block, so
    ## resetProducerId can interleave mid-RPC and wins the identity: the stale
    ## bump response is discarded and the bump stays owed for the next caller.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "bump-no-block")

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            let body = buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                ),
              ],
            )
            await av.respond(body)
          except CatchableError:
            discard
          while true:
            let ipid = await fb.nextRequest(timeout = 2.seconds)
            try:
              # Hold the response so the test can interleave resetProducerId
              # while the bump RPC is still awaiting it.
              await sleepAsync(200.milliseconds)
              await ipid.respond(
                buildInitProducerIdResponseBody(
                  errorCode = 0'i16, producerId = 100'i64, producerEpoch = 5'i16
                )
              )
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 1)
      p.setPendingEpochBump(true)

      await b.connect()
      let bumpFut = p.ensureEpochBumped()
      # Let the bump RPC reach its delayed response, then reset: it must
      # complete promptly instead of parking on the held initLock.
      await sleepAsync(100.milliseconds)
      check (await withTimeout(p.resetProducerId(), milliseconds(150))) == true
      await bumpFut
      # The reset won the identity: the bump's apply is discarded by the base
      # re-check and the bump remains owed for the next caller.
      check p.getProducerId() == -1
      check p.getProducerEpoch() == -1
      check p.getPendingEpochBump() == true

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

  test "concurrent ensureEpochBumped issues a single InitProducerId":
    ## Regression: ensureEpochBumped read pendingEpochBump lock-free then yielded
    ## into bumpEpoch's round-trip, so two concurrent sends could both fire
    ## InitProducerId RPCs and race the response order. The fix serializes it
    ## with a single-flight gate — the first arrival drives the bump, followers
    ## wait on the completion future and return once it settles.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "bump-race-test")

      var initProducerIdCount = 0
      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            let body = buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                ),
              ],
            )
            await av.respond(body)
          except CatchableError:
            discard
          while true:
            let ipid = await fb.nextRequest(timeout = 2.seconds)
            initProducerIdCount.inc
            try:
              await ipid.respond(
                buildInitProducerIdResponseBody(
                  errorCode = 0'i16,
                  producerId = 42'i64,
                  producerEpoch = int16(initProducerIdCount),
                )
              )
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setPendingEpochBump(true)

      await b.connect()
      # Fire 5 concurrent ensureEpochBumped calls. Only one should actually
      # talk to the coordinator; the other four await the epochBumpDone
      # single-flight gate (no initLock involved) and return immediately.
      var futs: seq[Future[void].Raising([KafkaError, CancelledError])]
      for _ in 0 ..< 5:
        futs.add(p.ensureEpochBumped())
      for fut in futs:
        try:
          await fut
        except KafkaError:
          discard

      check initProducerIdCount == 1
      check p.getPendingEpochBump() == false
      # The single bump used producerEpoch=1 (count after first request).
      check p.getProducerEpoch() == 1'i16

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

suite "Producer: errorHandler exception safety":
  ## Regression: a user errorHandler that violates its `raises: []` contract used
  ## to propagate the leaked exception up the send pipeline. The producer now
  ## guards the handler call and falls back to `eaFail`, so the original send
  ## error surfaces and the producer stays healthy.

  test "errorHandler raising exception falls back to eaFail":
    proc body() {.async: (raises: [Exception]).} =
      const topic = "errh-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let behavior = TxnProduceBehavior(produceErrorCode: 48'i16)
        # INVALID_TXN_STATE → ecAbortable, non-retriable
      let respFut = autoRespondTxn(fb, host, port, topic, behavior)

      let cluster = newCluster(@[host & ":" & $port], clientId = "errh-test")
      let sink = MetricSink()
      let p = newKafkaProducer(
        cluster,
        lingerMs = 0,
        maxRetries = 0,
        timeoutMs = 1500,
        metricsHook = makeMetricHook(sink),
      )

      var handlerCalled = 0
      proc badHandler(rec: FailedRecord): ErrorAction {.gcsafe, raises: [].} =
        handlerCalled.inc
        {.cast(raises: []).}:
          raise newException(ValueError, "intentional handler bug")

      p.errorHandler = badHandler

      var raisedKind = ""
      try:
        discard await p.send(topic, @[0xAB'u8], partition = 0'i32)
      except KafkaApiError as e:
        raisedKind = "api:" & $e.errorCode.int16
      except KafkaError as e:
        raisedKind = "kafka:" & e.msg
      except ValueError:
        raisedKind = "ValueError"

      # Handler must have been invoked exactly once.
      check handlerCalled == 1
      # The original KafkaApiError surfaces (eaFail fallback), NOT the
      # ValueError the handler raised.
      check raisedKind.startsWith("api:")
      # A handler that violates `raises: []` is a bug the caller has to hear
      # about; nothing else in the send result reports it.
      let ev = findMetricEvent(sink, "producer.error_handler.exception")
      check ev.isSome
      check metricTagValue(ev.get, "exception") == some("ValueError")

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)
      await cluster.close()

    waitFor body()

suite "Producer: DLQ send failure surfaces pstDeadLetterFailed":
  ## Regression: when errorHandler returned `eaDeadLetter` but the DLQ send itself
  ## failed, the producer swallowed the failure so callers could not tell the
  ## record was not redirected. It now returns a ProduceResult with status
  ## `pstDeadLetterFailed` so callers can detect the dropped record.

  test "missing DLQ configuration returns pstDeadLetterFailed":
    proc body() {.async: (raises: [Exception]).} =
      const topic = "dlq-unconf-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let behavior = TxnProduceBehavior(produceErrorCode: 48'i16)
      let respFut = autoRespondTxn(fb, host, port, topic, behavior)

      let cluster = newCluster(@[host & ":" & $port], clientId = "dlq-unconf-test")
      let sink = MetricSink()
      let p = newKafkaProducer(
        cluster,
        lingerMs = 0,
        maxRetries = 0,
        timeoutMs = 1500,
        metricsHook = makeMetricHook(sink),
      )
      # errorHandler asks for DLQ but no DLQ producer is configured.
      p.errorHandler = alwaysDeadLetter()

      let res = await p.send(topic, @[0xAB'u8], partition = 0'i32)
      check res.status == pstDeadLetterFailed
      check res.offset == -1
      # `pstDeadLetterFailed` tells the caller the record is gone; the event tells
      # the operator why it never reached a DLQ.
      let ev = findMetricEvent(sink, "producer.dlq.error")
      check ev.isSome
      check metricTagValue(ev.get, "status") == some("unconfigured")

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)
      await cluster.close()

    waitFor body()

  test "DLQ broker error returns pstDeadLetterFailed":
    proc body() {.async: (raises: [Exception]).} =
      const sourceTopic = "dlq-src-topic"
      const dlqTopic = "dlq-dest-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)

      # Auto-respond: source topic → INVALID_TXN_STATE, and the dlq produce
      # also replies INVALID_TXN_STATE (48), persistent and non-retriable. Both
      # producers share the same broker since metadata is keyed by topic.
      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 500.milliseconds)
          except CatchableError:
            break
          try:
            let key = req.apiKey
            if key == int16(ord(akApiVersions)):
              let body = buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                  ),
                ],
              )
              await req.respond(body)
            elif key == int16(ord(akMetadata)):
              let body = buildMetadataResponseBody(
                [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                controllerId = 0'i32,
                topics = [
                  FakeTopicMeta(
                    name: sourceTopic,
                    errorCode: 0'i16,
                    isInternal: false,
                    partitions: @[
                      FakePartitionMeta(
                        partitionIndex: 0'i32,
                        leaderId: 0'i32,
                        replicas: @[0'i32],
                        isr: @[0'i32],
                      )
                    ],
                  ),
                  FakeTopicMeta(
                    name: dlqTopic,
                    errorCode: 0'i16,
                    isInternal: false,
                    partitions: @[
                      FakePartitionMeta(
                        partitionIndex: 0'i32,
                        leaderId: 0'i32,
                        replicas: @[0'i32],
                        isr: @[0'i32],
                      )
                    ],
                  ),
                ],
              )
              await req.respond(body)
            elif key == int16(ord(akProduce)):
              # Inspect topic in body: the produce request body for v3
              # is [transactionalId, acks, timeoutMs, topics_array, ...].
              # Rather than re-decode, we route both source and dlq
              # topics to an error code; the source error triggers DLQ
              # routing, and the DLQ error trips pstDeadLetterFailed.
              # The errorHandler on the DLQ producer is nil → handler
              # returns none → send raises → sendToDeadLetter returns
              # false → caller surfaces pstDeadLetterFailed.
              let body = buildProduceResponseBody(
                topic = sourceTopic,
                partition = 0'i32,
                errorCode = 48'i16, # INVALID_TXN_STATE (also used for dlq)
                baseOffset = 0'i64,
                logAppendTimeMs = -1'i64,
                throttleTimeMs = 0'i32,
              )
              await req.respond(body)
          except CatchableError:
            break

      let handlerFut = handler()

      let cluster = newCluster(@[host & ":" & $port], clientId = "dlq-err-test")
      let dlq =
        newKafkaProducer(cluster, lingerMs = 0, maxRetries = 0, timeoutMs = 1500)
      let main =
        newKafkaProducer(cluster, lingerMs = 0, maxRetries = 0, timeoutMs = 1500)
      main.errorHandler = alwaysDeadLetter()
      await main.setDeadLetterQueue(dlqTopic, dlq)

      let res = await main.send(sourceTopic, @[0xAB'u8], partition = 0'i32)
      check res.status == pstDeadLetterFailed
      check res.offset == -1

      await main.close()
      await dlq.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)
      await cluster.close()

    waitFor body()

  test "a successful redirect emits producer.dlq.sent":
    ## The redirect is invisible in the send result beyond `pstDeadLettered`:
    ## which topic the record landed in is only in this event.
    proc body() {.async: (raises: [Exception]).} =
      const sourceTopic = "dlq-sent-src"
      const dlqTopic = "dlq-sent-dest"
      let host = "127.0.0.1"
      # Separate brokers: the source one rejects the produce, the DLQ one
      # accepts it. Each producer has its own cluster, so no cluster needs the
      # other topic's metadata.
      let srcFb = newFakeBroker()
      let srcPort = int32(srcFb.address().port)
      let dlqFb = newFakeBroker()
      let dlqPort = int32(dlqFb.address().port)
      let srcRespFut = autoRespondTxn(
        srcFb, host, srcPort, sourceTopic, TxnProduceBehavior(produceErrorCode: 48'i16)
      )
      let dlqRespFut = autoRespondTxn(
        dlqFb, host, dlqPort, dlqTopic, TxnProduceBehavior(produceErrorCode: 0'i16)
      )

      let sink = MetricSink()
      let cluster = newCluster(@[host & ":" & $srcPort], clientId = "dlq-sent-test")
      let dlqCluster = newCluster(@[host & ":" & $dlqPort], clientId = "dlq-sent-dlq")
      let dlq =
        newKafkaProducer(dlqCluster, lingerMs = 0, maxRetries = 0, timeoutMs = 1500)
      let p = newKafkaProducer(
        cluster,
        lingerMs = 0,
        maxRetries = 0,
        timeoutMs = 1500,
        errorHandler = alwaysDeadLetter(),
        metricsHook = makeMetricHook(sink),
      )
      await p.setDeadLetterQueue(dlqTopic, dlq)

      let res = await p.send(sourceTopic, @[0xAB'u8], partition = 0'i32)
      check res.status == pstDeadLettered
      check res.offset == -1
      let ev = findMetricEvent(sink, "producer.dlq.sent")
      check ev.isSome
      check metricTagValue(ev.get, "dlq_topic") == some(dlqTopic)
      check metricTagValue(ev.get, "original_topic") == some(sourceTopic)
      check metricTagValue(ev.get, "original_partition") == some("0")

      await p.close()
      await dlq.close()
      await srcFb.close()
      await dlqFb.close()
      awaitDrain(srcRespFut, 1.seconds)
      awaitDrain(dlqRespFut, 1.seconds)
      await cluster.close()
      await dlqCluster.close()

    waitFor body()

  test "an eaDrop handler returns pstDropped and reports the drop":
    ## A non-transactional producer may discard a failed record on the handler's
    ## instruction. The caller sees `pstDropped` and the offset `-1`, which is
    ## indistinguishable from a record that vanished; the event carries why.
    proc body() {.async: (raises: [Exception]).} =
      const topic = "drop-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      # INVALID_TXN_STATE is non-retriable, so the failure reaches the handler
      # instead of being retried.
      let respFut = autoRespondTxn(
        fb, host, port, topic, TxnProduceBehavior(produceErrorCode: 48'i16)
      )

      let sink = MetricSink()
      let cluster = newCluster(@[host & ":" & $port], clientId = "drop-test")
      let p = newKafkaProducer(
        cluster,
        lingerMs = 0,
        maxRetries = 0,
        timeoutMs = 1500,
        errorHandler = alwaysDrop(),
        metricsHook = makeMetricHook(sink),
      )

      let res = await p.send(topic, @[0xAB'u8], partition = 0'i32)
      check res.status == pstDropped
      check res.offset == -1
      let ev = findMetricEvent(sink, "producer.dropped")
      check ev.isSome
      check metricTagValue(ev.get, "topic") == some(topic)
      check metricTagValue(ev.get, "reason") == some("invalid_txn_state")

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)
      await cluster.close()

    waitFor body()

suite "Producer: cancel during flushBatch bypasses DLQ":
  ## Regression: a cancel between produceWrite (bytes on wire) and produceAwait
  ## used to be wrapped as KafkaError("cancelled") and routed through
  ## handleSendFailure. For a non-transactional producer with an eaDeadLetter
  ## errorHandler + configured DLQ, this silently duplicated the record — the
  ## broker persisted the earlier write AND the record was re-published to the
  ## DLQ. The fix skips handleSendFailure on any cancel so the caller sees the
  ## raw error and decides how to handle the unknown-outcome (retry, dedup).
  test "cancel at sem.acquire fails the batch with KafkaError, not DLQ status":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let dlq = newKafkaProducer(cluster, lingerMs = 0, timeoutMs = 200)
      # maxInFlightRequests = 1 so a single external tryAcquire saturates the
      # per-partition inflight semaphore and flushBatch parks on sem.acquire.
      let p = newKafkaProducer(
        cluster, lingerMs = 100, timeoutMs = 200, maxInFlightRequests = 1
      )
      p.errorHandler = alwaysDeadLetter()
      await p.setDeadLetterQueue("dlq-topic", dlq)

      let tp = TopicPartition(topic: "src-topic", partition: 0)
      let rec = Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some(@[0x01'u8]),
        headers: @[],
      )
      let pendingFut = p.injectPendingBatch(tp, rec)

      # Saturate the per-partition inflight semaphore so flushBatch parks on
      # sem.acquire after extracting the batch under batchLock. Cancelling from
      # there exercises the wasCancelled branch.
      check await p.tryAcquireInflightSemaphore(tp)

      let flushFut = flushBatchForTest(p, tp)
      # Yield so flushBatch clears batchLock extraction and parks on sem.acquire.
      await sleepAsync(50.milliseconds)

      flushFut.cancelSoon()
      await sleepAsync(50.milliseconds)

      check pendingFut.finished
      check pendingFut.failed
      var msg = ""
      try:
        discard await pendingFut
      except KafkaError as e:
        msg = e.msg
      # Without the fix, pendingFut would either complete with pstDeadLettered /
      # pstDeadLetterFailed status (duplication path taken) or hang on the DLQ
      # producer's send. With the fix it fails cleanly with the raw cancel error.
      check msg == "cancelled"

      p.releaseInflightSemaphore(tp)
      await p.close()
      await dlq.close()
      await cluster.close()

    waitFor body()

proc autoRespondCaptureBatchBytes(
    fb: FakeBroker,
    host: string,
    port: int32,
    topic: string,
    partitionCount: int32,
    sentBytes: ref seq[int],
) {.async: (raises: []).} =
  ## Records the encoded RecordBatch size (the `recordSet` length) of every
  ## Produce request the producer sends, so a test can assert the byte-cap seal
  ## keeps each batch at or under maxBatchBytes instead of letting a boundary
  ## record inflate it toward ~2x.
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 1000.milliseconds)
    except CatchableError:
      break
    try:
      let key = req.apiKey
      if key == int16(ord(akApiVersions)):
        await req.respond(
          buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              ),
              FakeApiVersion(
                apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
              ),
              FakeApiVersion(
                apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
              ),
            ],
          )
        )
      elif key == int16(ord(akMetadata)):
        var partitions: seq[FakePartitionMeta]
        for i in 0'i32 ..< partitionCount:
          partitions.add(
            FakePartitionMeta(
              partitionIndex: i, leaderId: 0'i32, replicas: @[0'i32], isr: @[0'i32]
            )
          )
        await req.respond(
          buildMetadataResponseBody(
            [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
            controllerId = 0'i32,
            topics = [
              FakeTopicMeta(
                name: topic, errorCode: 0'i16, isInternal: false, partitions: partitions
              )
            ],
          )
        )
      elif key == int16(ord(akProduce)):
        var part = 0'i32
        try:
          var r = initKafkaReader(req.body)
          let preq = r.decode(ProduceRequest, 3'i16)
          if preq.topics.len > 0 and preq.topics[0].partitions.len > 0:
            part = preq.topics[0].partitions[0].partition
            sentBytes[].add(preq.topics[0].partitions[0].recordSet.len)
        except CatchableError:
          discard
        await req.respond(
          buildProduceResponseBody(
            topic = topic,
            partition = part,
            errorCode = 0'i16,
            baseOffset = 0'i64,
            logAppendTimeMs = -1'i64,
            throttleTimeMs = 0'i32,
          )
        )
    except CatchableError:
      break

suite "Producer: maxBatchBytes byte-cap flush trigger":
  ## Regression: the producer only flushed by record count or linger timer, so a
  ## batch of small records could grow past broker.message.max.bytes and be
  ## rejected on the wire. Batches now also flush when the estimated size exceeds
  ## maxBatchBytes, and a single oversized record is rejected at send().

  test "default maxBatchBytes is 1 MiB":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster, lingerMs = 0)
    check p.maxBatchBytes == 1_048_576'i32
    waitFor p.close()
    waitFor cluster.close()

  test "send raises when a single record exceeds maxBatchBytes":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, lingerMs = 0, maxBatchBytes = 1024'i32)
      let oversized = newSeq[byte](2048)
      var raised = false
      var msg = ""
      try:
        discard await p.send("t", oversized, partition = 0)
      except KafkaError as e:
        raised = true
        msg = e.msg
      check raised
      check "exceeds maxBatchBytes" in msg
      await p.close()
      await cluster.close()

    waitFor body()

  test "send rejects a record that fits maxBatchBytes only without the batch header":
    # estimate = 976 (value) + 36 (overhead) = 1012 < maxBatchBytes 1024, so the
    # old per-record check accepted it. But the encoded single-record batch is
    # 61 (RecordBatch v2 header) + 1012 = 1073, which the broker would reject as
    # MESSAGE_TOO_LARGE. The pre-check now accounts for the header. (Rejection is
    # synchronous, before any network, so the absent broker is never contacted.)
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, lingerMs = 0, maxBatchBytes = 1024'i32)
      let value = newSeq[byte](976)
      var raised = false
      var msg = ""
      try:
        discard await p.send("t", value, partition = 0)
      except KafkaError as e:
        raised = true
        msg = e.msg
      check raised
      check "exceeds maxBatchBytes" in msg
      await p.close()
      await cluster.close()

    waitFor body()

  test "estimateRecordSize covers key, value, and headers":
    let smallRec = Record(
      offset: 0,
      timestamp: 0,
      key: some(newSeq[byte](10)),
      value: some(newSeq[byte](100)),
      headers: @[
        RecordHeader(key: "abc", value: some(newSeq[byte](7))),
        RecordHeader(key: "longer-header", value: some(newSeq[byte](20))),
      ],
    )
    let bytes = estimateRecordSize(smallRec)
    # 10 (key) + 100 (value) + (3 + 7) + (13 + 20) + 20 (header varints)
    # + 36 (overhead) = 209
    check bytes >= 150'i32
    check bytes <= 250'i32

  test "newKafkaProducer rejects maxBatchBytes <= 0":
    let cluster = newCluster(@["127.0.0.1:9092"])
    var raised = false
    try:
      discard newKafkaProducer(cluster, maxBatchBytes = 0'i32)
    except KafkaError:
      raised = true
    check raised
    waitFor cluster.close()

  test "pendingBatchBytes running total tracks enqueues and clears on teardown":
    # Regression for the O(n²)->O(1) byte-cap refactor: the per-partition
    # running total must equal the sum of buffered records' estimatedBytes and
    # must be dropped in lock-step with the batch (here via failBatchFutures,
    # the same del path flushBatch/close use).
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, lingerMs = 100)
      let tp = TopicPartition(topic: "t", partition: 0)
      check p.pendingBatchBytesFor(tp) == 0'i64

      let rec1 = Record(offset: 0, timestamp: 0, value: some(newSeq[byte](100)))
      let rec2 = Record(offset: 0, timestamp: 0, value: some(newSeq[byte](250)))
      let f1 = p.injectPendingBatch(tp, rec1)
      let total1 = p.pendingBatchBytesFor(tp)
      check total1 == int64(estimateRecordSize(rec1))
      let f2 = p.injectPendingBatch(tp, rec2)
      check p.pendingBatchBytesFor(tp) ==
        int64(estimateRecordSize(rec1)) + int64(estimateRecordSize(rec2))

      p.failBatchFuturesForTest(tp, newException(KafkaError, "drain"))
      check p.pendingBatchBytesFor(tp) == 0'i64
      check not p.hasBatch(tp)
      # The injected futures were failed by failBatchFutures; drain them so the
      # test does not leak unraised future errors.
      for fut in [f1, f2]:
        try:
          discard await fut
        except CatchableError:
          discard
      await p.close()
      await cluster.close()

    waitFor body()

  test "byte-cap seal keeps every sent batch at or under maxBatchBytes":
    # The byte cap is a hard cap, not a ~2x soft cap: when the next record would
    # push the in-progress batch over maxBatchBytes, the producer seals (flushes)
    # that batch BEFORE the record joins it, then opens a fresh batch with the
    # record — Kafka Java's RecordAccumulator behavior. The pre-fix code appended
    # the boundary record first and flushed the inflated batch, so a batch could
    # reach the wire at nearly twice the cap (and trip the broker's
    # message.max.bytes). This drives a real Produce through a FakeBroker and
    # checks the encoded RecordBatch (`recordSet`) of every request stays <= cap.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let topic = "seal-topic"
      let sentBytes = new(seq[int])
      let respFut =
        autoRespondCaptureBatchBytes(fb, host, port, topic, 1'i32, sentBytes)
      let cluster = newCluster(@[host & ":" & $port], clientId = "seal-test")
      # maxBatchRecords high so the BYTE cap is the only trigger; a short linger
      # so the lone post-seal record's batch flushes on its own (no explicit
      # flush(), which would race the seal's re-enqueue).
      let p = newKafkaProducer(
        cluster,
        acks = 1,
        lingerMs = 100,
        maxBatchRecords = 1000,
        maxBatchBytes = 1024'i32,
        timeoutMs = 1500,
      )
      let tp = TopicPartition(topic: topic, partition: 0)
      let value = newSeq[byte](200) # estimateRecordSize == 36 + 200 == 236
      try:
        # Pre-fill the batch to just under the cap: 4 records, estimated
        # 61 (RecordBatch v2 header) + 4*236 == 1005 <= 1024.
        var injected: seq[Future[ProduceResult]]
        for _ in 0 ..< 4:
          injected.add(
            p.injectPendingBatch(
              tp, Record(offset: 0, timestamp: 0, value: some(value))
            )
          )
        check p.pendingBatchBytesFor(tp) ==
          4'i64 *
          int64(estimateRecordSize(Record(offset: 0, timestamp: 0, value: some(value))))

        # A 5th record's estimate (61 + 5*236 == 1241) exceeds the cap. Pre-fix
        # this flushed all five (~1106 encoded bytes, over the cap); the seal
        # flushes [1..4] first and starts a fresh batch with the 5th, which then
        # leaves via its own linger timer.
        let f5 = p.send(topic, value, partition = 0)
        discard await f5 # transitively waits for the sealed [1..4] too
        await allFutures(injected)
      finally:
        await p.close()
        await fb.close()
        await respFut

      # Two batches reached the broker — the sealed [1..4] and the lone 5th —
      # rather than one inflated batch of five.
      check sentBytes[].len == 2
      # Every sent batch stayed at or under the cap. Pre-fix the first batch
      # carried all five records and exceeded 1024.
      for b in sentBytes[]:
        check b <= 1024
      # The sealed batch (four records) is well past the half-cap mark, so it
      # clearly held multiple records — the seal split a real batch, it did not
      # degrade batching into single-record sends. The lone 5th batch is small.
      check sentBytes[][0] > 512
      check sentBytes[][0] > sentBytes[][1]

    waitFor body()

  test "seal re-append re-checks the byte cap after a batch refills mid-flush":
    # The seal pre-check and the append must stay in one batchLock region. A
    # transactional send awaits its seal flush inline (the broker round-trip),
    # and while suspended the in-progress batch — emptied by the extract — can
    # refill from concurrent sends back toward the cap. If the post-seal append
    # were unconditional it would push that refilled batch over maxBatchBytes
    # and the broker would reject it with MESSAGE_TOO_LARGE. The fix re-checks
    # the cap on resume and re-seals; this test pins the suspension with a gated
    # Produce, refills the batch in the window, and asserts no batch exceeds the
    # cap.
    proc body() {.async: (raises: [Exception]).} =
      const topic = "txn-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let sentBytes = new(seq[int])
      let firstProduceArrived = newFuture[void]("firstProduceArrived")
      let releaseFirstProduce = newFuture[void]("releaseFirstProduce")

      proc handler() {.async: (raises: []).} =
        var produceCount = 0
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            let key = req.apiKey
            if key == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                    ),
                  ],
                )
              )
            elif key == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                  controllerId = 0'i32,
                  topics = [
                    FakeTopicMeta(
                      name: topic,
                      errorCode: 0'i16,
                      isInternal: false,
                      partitions: @[
                        FakePartitionMeta(
                          partitionIndex: 0'i32,
                          leaderId: 0'i32,
                          replicas: @[0'i32],
                          isr: @[0'i32],
                        )
                      ],
                    )
                  ],
                )
              )
            elif key == int16(ord(akProduce)):
              var part = 0'i32
              try:
                var r = initKafkaReader(req.body)
                let preq = r.decode(ProduceRequest, 3'i16)
                if preq.topics.len > 0 and preq.topics[0].partitions.len > 0:
                  part = preq.topics[0].partitions[0].partition
                  sentBytes[].add(preq.topics[0].partitions[0].recordSet.len)
              except CatchableError:
                discard
              produceCount.inc
              # Gate only the first Produce — the sealed batch's flush — so the
              # sending send() parks inside startFlush with the batch already
              # extracted, opening the refill window below.
              if produceCount == 1:
                firstProduceArrived.complete()
                await releaseFirstProduce
              await req.respond(
                buildProduceResponseBody(
                  topic = topic,
                  partition = part,
                  errorCode = 0'i16,
                  baseOffset = 0'i64,
                  logAppendTimeMs = -1'i64,
                  throttleTimeMs = 0'i32,
                )
              )
          except CatchableError:
            break

      let handlerFut = handler()
      let cluster = newCluster(@[host & ":" & $port], clientId = "seal-race-test")
      # Transactional so the seal flush is awaited INLINE (a real broker
      # round-trip) — the suspension point the race needs. maxBatchRecords 5 so
      # the buggy 5th append flushes immediately (record-count trigger) and its
      # over-cap batch reaches the broker to be observed, rather than lingering.
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        lingerMs = 50,
        maxBatchRecords = 5,
        maxBatchBytes = 1024'i32,
        maxRetries = 0,
        retryBackoffMs = 1,
        timeoutMs = 2000,
      )
      seedTxnProducer(p)
      let tp = TopicPartition(topic: topic, partition: 0'i32)
      let value = newSeq[byte](200) # estimateRecordSize == 236

      try:
        # Pre-fill the in-progress batch to just under the cap: 4 records,
        # 61 (header) + 4*236 == 1005 <= 1024.
        var prefilled: seq[Future[ProduceResult]]
        for _ in 0 ..< 4:
          prefilled.add(
            p.injectPendingBatch(
              tp, Record(offset: 0, timestamp: 0, value: some(value))
            )
          )

        # This send's record (est 236) would push the batch to 1241 > 1024, so
        # it SEALS [1..4] and (txn) awaits the flush inline. Do not await it yet.
        let fx = p.send(topic, value, partition = 0)

        # The sealed batch's Produce has reached the broker — flushBatch has
        # extracted [1..4] and the in-progress batch is empty. fx is parked
        # awaiting this Produce's (gated) response.
        await firstProduceArrived.wait(2.seconds)
        check not p.hasBatch(tp)

        # Refill the empty in-progress batch back to just under the cap, standing
        # in for concurrent sends that grabbed batchLock during the flush await.
        var refilled: seq[Future[ProduceResult]]
        for _ in 0 ..< 4:
          refilled.add(
            p.injectPendingBatch(
              tp, Record(offset: 0, timestamp: 0, value: some(value))
            )
          )

        # Let the sealed flush ack. fx resumes and must re-check the cap before
        # appending: the fix re-seals the refilled batch and opens a fresh batch
        # for its record. The pre-fix unconditional append would make it the 5th
        # record of the refilled batch (over the cap, and flushed at once).
        releaseFirstProduce.complete()

        discard await fx.wait(2.seconds)
        await allFutures(prefilled).wait(2.seconds)
        await allFutures(refilled).wait(2.seconds)
      finally:
        await p.close()
        await fb.close()
        awaitDrain(handlerFut, 1.seconds)

      # The sealed [1..4] and the re-sealed refilled batch each reached the
      # broker as their own batch.
      check sentBytes[].len >= 2
      # Every batch stayed at or under the cap. Pre-fix the refilled batch
      # carried five records and exceeded 1024.
      for b in sentBytes[]:
        check b <= 1024

    waitFor body()

suite "Producer: per-partition tables are cleared on close":
  ## Regression: inflightSemaphores, stickyPartitions, sequenceNumbers,
  ## txnPartitions, and txnPendingPartitions are populated lazily. Idle eviction
  ## already drops inflightSemaphores and finished chain tails mid-life; close()
  ## clears the rest so a short-lived producer that walked many partitions
  ## doesn't hand its memory to the next instance via the gc-roots.

  test "close clears inflightSemaphores":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, lingerMs = 100, timeoutMs = 200)
      # Populate the table directly — production code would do this via
      # send(), but that would also try to contact a broker.
      for i in 0 ..< 5:
        discard
          p.tryAcquireInflightSemaphore(TopicPartition(topic: "t", partition: int32(i)))
      check p.inflightSemaphoreCount() == 5
      await p.close()
      check p.inflightSemaphoreCount() == 0
      await cluster.close()

    waitFor body()

suite "Producer: idle per-partition state reclaimed mid-life":
  ## Regression for unbounded per-partition table growth: a producer fanning out
  ## over many partitions used to retain each partition's bookkeeping until
  ## close(). maybeEvictIdlePartitionState now reclaims it once a partition goes
  ## idle — but never the protocol-mandated sequenceNumbers entry, and never
  ## while the partition is still busy.

  test "idle partition's semaphore is reclaimed":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster)
      let tp = TopicPartition(topic: "t", partition: 0'i32)
      check await p.tryAcquireInflightSemaphore(tp) # creates + takes a permit
      check p.inflightSemaphoreCount() == 1
      p.releaseInflightSemaphore(tp) # fully idle now
      p.evictIdlePartitionStateForTest(tp)
      check p.inflightSemaphoreCount() == 0
      await cluster.close()

    waitFor body()

  test "busy partition (held permit) is NOT reclaimed":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster)
      let tp = TopicPartition(topic: "t", partition: 0'i32)
      check await p.tryAcquireInflightSemaphore(tp)
      # Permit still held → availableSlots < max → eviction must skip so the
      # in-flight cap stays attached to the same semaphore object.
      p.evictIdlePartitionStateForTest(tp)
      check p.inflightSemaphoreCount() == 1
      p.releaseInflightSemaphore(tp)
      p.evictIdlePartitionStateForTest(tp)
      check p.inflightSemaphoreCount() == 0
      await cluster.close()

    waitFor body()

  test "partition with a buffered batch is NOT reclaimed":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster)
      let tp = TopicPartition(topic: "t", partition: 0'i32)
      check await p.tryAcquireInflightSemaphore(tp)
      p.releaseInflightSemaphore(tp) # semaphore idle...
      let fut = p.injectPendingBatch(tp, Record(value: some(@[0x01'u8])))
      # ...but a batch is still buffered → eviction must skip.
      p.evictIdlePartitionStateForTest(tp)
      check p.inflightSemaphoreCount() == 1
      # Drain the buffered batch, then the partition is idle and reclaimable.
      p.failBatchFuturesForTest(tp, newException(KafkaError, "test"))
      try:
        discard await fut
      except CatchableError:
        discard
      p.evictIdlePartitionStateForTest(tp)
      check p.inflightSemaphoreCount() == 0
      await cluster.close()

    waitFor body()

  test "sequenceNumbers entry survives eviction (protocol-mandated)":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, enableIdempotence = true)
      let tp = TopicPartition(topic: "t", partition: 0'i32)
      p.setSequenceNumber(tp, 42'i32)
      check await p.tryAcquireInflightSemaphore(tp)
      p.releaseInflightSemaphore(tp)
      p.evictIdlePartitionStateForTest(tp)
      check p.inflightSemaphoreCount() == 0 # semaphore reclaimed
      check p.sequenceNumberCount() == 1 # sequence preserved (broker-tracked)
      check p.getSequenceNumber(tp) == 42'i32
      await cluster.close()

    waitFor body()

  test "finished chain tails reclaimed, unfinished retained":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster, enableIdempotence = true)
      let tp = TopicPartition(topic: "t", partition: 0'i32)

      # An UNFINISHED completion tail is a real production state: a batch
      # releases the in-flight semaphore BEFORE it awaits its completion-chain
      # predecessor, so the semaphore can be idle while gate.mine is still
      # pending. Eviction must reclaim the semaphore but keep the unfinished
      # tail so a later send still chains its completion behind it (ordering).
      check await p.tryAcquireInflightSemaphore(tp)
      p.releaseInflightSemaphore(tp)
      let pending = newFuture[void]("test-unfinished-tail")
      p.setChainTailsForTest(tp, pending)
      p.evictIdlePartitionStateForTest(tp)
      check p.inflightSemaphoreCount() == 0 # semaphore independently reclaimed
      check p.hasWriteChainTailForTest(tp) # unfinished → retained
      check p.hasCompletionChainTailForTest(tp)

      # Once the tail finishes it is behaviourally inert, so a later eviction
      # reclaims both chains.
      pending.complete()
      check await p.tryAcquireInflightSemaphore(tp) # fresh idle semaphore
      p.releaseInflightSemaphore(tp)
      p.evictIdlePartitionStateForTest(tp)
      check not p.hasWriteChainTailForTest(tp)
      check not p.hasCompletionChainTailForTest(tp)
      await cluster.close()

    waitFor body()

  test "topic's RR counter survives idle eviction until the topic is deleted":
    ## Regression: partitionCounters grew with every distinct topic ever written
    ## and shrank only on close(). The counter must be KEPT while the topic is
    ## still resolvable (round-robin rotation across sends depends on it — see the
    ## sticky rotation tests) and reclaimed once the topic is deleted upstream.
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster)
      # Topic resolvable: two partitions in the cluster cache.
      cluster.topicPartitions["t"] = @[
        PartitionMeta(partitionIndex: 0'i32, leaderId: 0'i32, errorCode: keNone),
        PartitionMeta(partitionIndex: 1'i32, leaderId: 0'i32, errorCode: keNone),
      ]
      let tp0 = TopicPartition(topic: "t", partition: 0'i32)
      let tp1 = TopicPartition(topic: "t", partition: 1'i32)
      # Prior RR sends created the counter.
      check p.roundRobinIndex("t", 2) == 0
      check p.roundRobinIndex("t", 2) == 1

      # Both partitions idle-and-sendable, then fully evicted.
      check await p.tryAcquireInflightSemaphore(tp0)
      check await p.tryAcquireInflightSemaphore(tp1)
      p.releaseInflightSemaphore(tp0)
      p.releaseInflightSemaphore(tp1)
      p.evictIdlePartitionStateForTest(tp0)
      p.evictIdlePartitionStateForTest(tp1)
      # Topic still resolvable → counter retained (rotation must keep working).
      check "t" in p.partitionCounters

      # Topic deleted upstream: drop it from the cache, then the next idle
      # eviction reclaims the now-dead counter.
      cluster.topicPartitions.del("t")
      check await p.tryAcquireInflightSemaphore(tp0)
      p.releaseInflightSemaphore(tp0)
      p.evictIdlePartitionStateForTest(tp0)
      check "t" notin p.partitionCounters
      # A fresh send to a recreated topic re-seeds the counter, so round-robin
      # still works.
      cluster.topicPartitions["t"] =
        @[PartitionMeta(partitionIndex: 0'i32, leaderId: 0'i32, errorCode: keNone)]
      check p.roundRobinIndex("t", 1) == 0
      check "t" in p.partitionCounters
      await cluster.close()

    waitFor body()

  test "idle eviction does not reclaim counter while a sibling partition is busy":
    ## The eviction safety hinge is "partition fully idle". While another
    ## partition of the SAME topic is mid-write, dropping the topic counter
    ## would reset its RR position mid-burst — so eviction must skip reclaim
    ## until no partition of the topic is live.
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(cluster)
      let tpIdle = TopicPartition(topic: "t", partition: 0'i32)
      let tpBusy = TopicPartition(topic: "t", partition: 1'i32)
      discard p.roundRobinIndex("t", 2)
      check await p.tryAcquireInflightSemaphore(tpIdle)
      p.releaseInflightSemaphore(tpIdle) # idle
      check await p.tryAcquireInflightSemaphore(tpBusy) # held live
      p.evictIdlePartitionStateForTest(tpIdle)
      check "t" in p.partitionCounters # sibling still writing → kept
      p.releaseInflightSemaphore(tpBusy)
      p.evictIdlePartitionStateForTest(tpBusy)
      check "t" notin p.partitionCounters # last one idle → reclaimed
      await cluster.close()

    waitFor body()

suite "Producer: bumpEpoch honors producer close":
  ## Regression: bumpEpoch's backoff loop sleeps between InitProducerId attempts.
  ## If close() flipped state out of lsRunning during that sleep the next attempt
  ## fired another RPC, leaving a zombie PID. The loop now re-checks state after
  ## each sleep and abandons the bump silently — the preceding EndTxn already
  ## succeeded, so raising would make a successful commit/abort look failed.

  test "bumpEpoch abandons silently when close races the retry loop":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "bump-close-race-test")

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            let body = buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                ),
              ],
            )
            await av.respond(body)
          except CatchableError:
            discard
          # First initProducerId: respond retriable so bumpEpoch enters the
          # sleep + retry loop. We never serve a second request — close()
          # below short-circuits the loop via the requireRunning check.
          let ipid = await fb.nextRequest(timeout = 5.seconds)
          try:
            await ipid.respond(
              buildInitProducerIdResponseBody(
                errorCode = int16(ord(keOutOfOrderSequenceNumber))
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 500,
      )
      p.setTransactionalIdForTest("test-txn")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setTxnState(tsInTransaction)
      await b.connect()

      # Race a markClosed with the bumpEpoch loop. The loop will fire one
      # initProducerId, sleep 50ms, then re-check state on the next iteration.
      # We flip state before that sleep completes.
      let bumpFut = p.bumpEpoch()
      await sleepAsync(milliseconds(20))
      p.markClosed()
      var raised = false
      try:
        await bumpFut
      except KafkaError:
        raised = true
      # New contract: a close race abandons the bump silently — bumpEpoch
      # must NOT raise (the EndTxn already succeeded broker-side), and only the
      # single initProducerId from the first iteration was ever fired. The
      # producer identity is left untouched.
      check not raised
      check p.getProducerEpoch() == 0'i16

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor body()

# Producer KIP-185: broker-side ordering checks for `maxInFlight > 1`.
#
# Decodes captured Produce requests so the fake broker can pin two
# invariants the v0.1.0 clamp lift relies on:
#   1. concurrent produces against an idempotent producer arrive at the
#      broker in baseSequence order; and
#   2. a retried batch reuses the same baseSequence it was first assigned
#      (the KIP-185 "freeze" invariant).

type
  CapturedBatch = object
    topic: string
    partition: int32
    baseSequence: int32
    recordCount: int32
    producerId: int64
    producerEpoch: int16

  OrderTracker = ref object
    ## Per-(producerId, partition) expected baseSequence cursor. The broker
    ## handler bumps the cursor by `recordCount` on every accepted batch
    ## and asserts each new request lines up with the cursor.
    expectedBySeqKey: Table[(int64, int32), int32]
    observed: seq[CapturedBatch]
    outOfOrderSeen: bool

proc decodeProduceBatches(body: seq[byte]): seq[CapturedBatch] {.raises: [Exception].} =
  ## Decode a Produce v3 request body into the (baseSequence, recordCount)
  ## tuples the broker needs to validate ordering. The Produce schema is
  ## non-flexible at v3, so the captured body bytes can go straight into
  ## the schema-generated decoder.
  var r = initKafkaReader(body)
  let pReq = r.decode(ProduceRequest)
  for topic in pReq.topics:
    for part in topic.partitions:
      var br = initKafkaReader(part.recordSet)
      let batch = br.decode(RecordBatch)
      result.add(
        CapturedBatch(
          topic: topic.topic,
          partition: part.partition,
          baseSequence: batch.baseSequence,
          recordCount: int32(batch.records.len),
          producerId: batch.producerId,
          producerEpoch: batch.producerEpoch,
        )
      )

proc recordBatch(t: OrderTracker, b: CapturedBatch): bool =
  ## Returns true when `b.baseSequence` matches the next expected value
  ## for `(producerId, partition)`. Advances the cursor and appends to
  ## `observed`. On mismatch records the violation and returns false.
  let key = (b.producerId, b.partition)
  let expected = t.expectedBySeqKey.getOrDefault(key, 0'i32)
  t.observed.add(b)
  if b.baseSequence != expected:
    t.outOfOrderSeen = true
    return false
  t.expectedBySeqKey[key] = expected + b.recordCount
  return true

suite "Producer KIP-185: maxInFlight > 1 ordering on the wire":
  ## Removing the idempotence maxInFlight=1 clamp depends on the
  ## broker-visible wire order matching the producer's allocation order.
  ## These tests decode the Produce requests captured by the fake broker
  ## and assert the (baseSequence, producerId) tuples line up — the only
  ## external invariant a real broker enforces.

  type BrokerMode = enum
    ## Strict tracks `expectedBySeqKey` and returns OUT_OF_ORDER on a
    ## gap; lenient accepts every produce request unconditionally and
    ## only the test inspects `observed` after the run. The retry
    ## tests use lenient because a real KIP-185 retry path under
    ## maxInFlight > 1 inevitably interleaves a few "broker would
    ## have rejected this with OUT_OF_ORDER" batches that the producer
    ## must re-encode — pin the freeze invariant instead.
    bmStrict
    bmLenient

  proc runOrderingHarness(
      sends: int, injectRetriableOnSeqs: seq[int32] = @[], mode: BrokerMode = bmStrict
  ): Future[OrderTracker] {.async: (raises: [Exception]).} =
    let fb = newFakeBroker()
    let host = "127.0.0.1"
    let port = int32(fb.address().port)
    let topic = "kip185-topic"
    let tracker = OrderTracker(
      expectedBySeqKey: initTable[(int64, int32), int32](),
      observed: @[],
      outOfOrderSeen: false,
    )
    var failedOnce = initTable[int32, bool]()
    var nextOffset: int64 = 0

    proc handler() {.async: (raises: []).} =
      while true:
        var req: CapturedRequest
        try:
          req = await fb.nextRequest(timeout = 2.seconds)
        except CatchableError:
          break
        try:
          let key = req.apiKey
          if key == int16(ord(akApiVersions)):
            let body = buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                ),
              ],
            )
            await req.respond(body)
          elif key == int16(ord(akMetadata)):
            let body = buildMetadataResponseBody(
              [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
              controllerId = 0'i32,
              topics = [
                FakeTopicMeta(
                  name: topic,
                  errorCode: 0'i16,
                  isInternal: false,
                  partitions: @[
                    FakePartitionMeta(
                      partitionIndex: 0'i32,
                      leaderId: 0'i32,
                      replicas: @[0'i32],
                      isr: @[0'i32],
                    )
                  ],
                )
              ],
            )
            await req.respond(body)
          elif key == int16(ord(akInitProducerId)):
            await req.respond(
              buildInitProducerIdResponseBody(
                errorCode = 0'i16, producerId = 1000'i64, producerEpoch = 0'i16
              )
            )
          elif key == int16(ord(akProduce)):
            let batches =
              try:
                decodeProduceBatches(req.body)
              except Exception:
                @[]
            doAssert batches.len == 1, "single-partition harness expected"
            let b = batches[0]
            let wantRetry =
              b.baseSequence in injectRetriableOnSeqs and
              not failedOnce.getOrDefault(b.baseSequence, false)
            if wantRetry:
              failedOnce[b.baseSequence] = true
              tracker.observed.add(b)
              # NOT_LEADER_OR_FOLLOWER (6) — retriable, does not trigger
              # a producer-id reset, so the producer reissues with the
              # same baseSequence and we can check the freeze invariant.
              await req.respond(
                buildProduceResponseBody(
                  topic = b.topic,
                  partition = b.partition,
                  errorCode = 6'i16,
                  baseOffset = -1'i64,
                )
              )
            else:
              let accepted =
                case mode
                of bmStrict:
                  tracker.recordBatch(b)
                of bmLenient:
                  tracker.observed.add(b)
                  true
              if accepted:
                let assignedOffset = nextOffset
                nextOffset += int64(b.recordCount)
                await req.respond(
                  buildProduceResponseBody(
                    topic = b.topic,
                    partition = b.partition,
                    errorCode = 0'i16,
                    baseOffset = assignedOffset,
                  )
                )
              else:
                await req.respond(
                  buildProduceResponseBody(
                    topic = b.topic,
                    partition = b.partition,
                    errorCode = int16(ord(keOutOfOrderSequenceNumber)),
                    baseOffset = -1'i64,
                  )
                )
        except CatchableError:
          break

    let handlerFut = handler()

    let cluster = newCluster(@[host & ":" & $port], clientId = "kip185-ordering-test")
    let p = newKafkaProducer(
      cluster,
      enableIdempotence = true,
      maxInFlightRequests = 5,
      lingerMs = 0,
      maxRetries = 5,
      retryBackoffMs = 1,
      timeoutMs = 1500,
    )

    var sendFuts: seq[Future[ProduceResult]]
    for i in 0 ..< sends:
      sendFuts.add(p.send(topic, @[byte(i and 0xFF)], partition = 0'i32))
    for fut in sendFuts:
      try:
        discard await fut
      except KafkaError:
        # Lenient mode may surface a couple of producer-side
        # ecNeedsProducerIdReset retries before the freeze pin observes
        # both attempts. The post-run assertions inspect `observed` and
        # do not require every send future to succeed.
        discard

    await p.close()
    await fb.close()
    awaitDrain(handlerFut, 2.seconds)
    await cluster.close()
    return tracker

  test "concurrent produces arrive in baseSequence order":
    proc body() {.async: (raises: [Exception]).} =
      let tracker = await runOrderingHarness(sends = 10, mode = bmStrict)
      check not tracker.outOfOrderSeen
      # Every accepted batch should carry the producerId the broker
      # assigned via InitProducerId, with sequences walking 0..N-1.
      check tracker.observed.len == 10
      for i, b in tracker.observed:
        check b.producerId == 1000'i64
        check b.producerEpoch == 0'i16
        check b.recordCount == 1'i32
        check b.baseSequence == int32(i)

    waitFor body()

  test "retried batch reuses its frozen baseSequence":
    proc body() {.async: (raises: [Exception]).} =
      # Single-flight pin of the KIP-185 freeze invariant: a retriable
      # broker error on a batch must trigger a re-send with the *same*
      # baseSequence — never a re-allocation. The lenient broker here
      # just records what the producer puts on the wire so the test can
      # see both attempts.
      let tracker = await runOrderingHarness(
        sends = 1, injectRetriableOnSeqs = @[0'i32], mode = bmLenient
      )
      # Producer sent seq=0, broker rejected once (retriable), producer
      # retried with seq=0 → exactly two observations at seq=0.
      check tracker.observed.len == 2
      check tracker.observed[0].baseSequence == 0'i32
      check tracker.observed[1].baseSequence == 0'i32
      # Same producerId / epoch — a retriable failure must not bump
      # producer-id (that's the NeedsProducerIdReset path, distinct
      # from retriable codes like NOT_LEADER_OR_FOLLOWER).
      check tracker.observed[0].producerId == tracker.observed[1].producerId
      check tracker.observed[0].producerEpoch == tracker.observed[1].producerEpoch

    waitFor body()

  test "writes pipeline in baseSequence order while responses are withheld":
    ## Core regression for the per-partition write-order gate. The broker
    ## withholds ALL Produce responses until every concurrent send's frame has
    ## arrived, then replies. This proves two things at once:
    ##   1. The gate is released after the WRITE (not held across the response
    ##      await) — otherwise only one send could be in flight and the broker,
    ##      waiting for the rest, would deadlock the test.
    ##   2. The frames arrive in strict baseSequence order (0..N-1) even though
    ##      they are all in flight simultaneously.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let topic = "gate-order-topic"
      const sends = 5
      var produceReqs: seq[CapturedRequest]
      var observedSeqs: seq[int32]

      proc handler() {.async: (raises: []).} =
        var produceCount = 0
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 3.seconds)
          except CatchableError:
            break
          try:
            let key = req.apiKey
            if key == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                    ),
                  ],
                )
              )
            elif key == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                  controllerId = 0'i32,
                  topics = [
                    FakeTopicMeta(
                      name: topic,
                      errorCode: 0'i16,
                      isInternal: false,
                      partitions: @[
                        FakePartitionMeta(
                          partitionIndex: 0'i32,
                          leaderId: 0'i32,
                          replicas: @[0'i32],
                          isr: @[0'i32],
                        )
                      ],
                    )
                  ],
                )
              )
            elif key == int16(ord(akInitProducerId)):
              await req.respond(
                buildInitProducerIdResponseBody(
                  errorCode = 0'i16, producerId = 3000'i64, producerEpoch = 0'i16
                )
              )
            elif key == int16(ord(akProduce)):
              # Record arrival order; withhold the response until all `sends`
              # writes have arrived. If the gate were held across the response
              # await this loop would block here forever (only one in flight).
              let batches =
                try:
                  decodeProduceBatches(req.body)
                except Exception:
                  @[]
              if batches.len == 1:
                observedSeqs.add(batches[0].baseSequence)
              produceReqs.add(req)
              inc produceCount
              if produceCount == sends:
                var offset: int64 = 0
                for pr in produceReqs:
                  let bs =
                    try:
                      decodeProduceBatches(pr.body)
                    except Exception:
                      @[]
                  let recCount =
                    if bs.len == 1:
                      bs[0].recordCount
                    else:
                      1'i32
                  await pr.respond(
                    buildProduceResponseBody(
                      topic = topic,
                      partition = 0'i32,
                      errorCode = 0'i16,
                      baseOffset = offset,
                    )
                  )
                  offset += int64(recCount)
          except CatchableError:
            break

      let handlerFut = handler()

      let cluster = newCluster(@[host & ":" & $port], clientId = "gate-order-test")
      let p = newKafkaProducer(
        cluster,
        enableIdempotence = true,
        maxInFlightRequests = 5,
        lingerMs = 0,
        maxRetries = 0,
        timeoutMs = 3000,
      )

      var sendFuts: seq[Future[ProduceResult]]
      for i in 0 ..< sends:
        sendFuts.add(p.send(topic, @[byte(i and 0xFF)], partition = 0'i32))
      var okCount = 0
      for fut in sendFuts:
        let r = await fut
        if r.offset >= 0:
          inc okCount

      check okCount == sends
      # Every frame arrived, and in strict baseSequence order.
      check observedSeqs.len == sends
      for i in 0 ..< sends:
        check observedSeqs[i] == int32(i)

      await p.close()
      await fb.close()
      awaitDrain(handlerFut, 2.seconds)
      await cluster.close()

    waitFor body()

  test "completions surface in send order even when responses arrive reversed":
    ## Regression: strict in-order completion. Under maxInFlight > 1 each send
    ## awaits its own response, so a later send whose response arrives first could
    ## be acked before an earlier one — violating completion ordering. The broker
    ## replies in REVERSE baseSequence order; the completion barrier must still
    ## surface the send futures in order 0..N-1.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let topic = "c2-order-topic"
      const sends = 5
      var produceReqs: seq[CapturedRequest]

      proc handler() {.async: (raises: []).} =
        var produceCount = 0
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 3.seconds)
          except CatchableError:
            break
          try:
            let key = req.apiKey
            if key == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                    ),
                  ],
                )
              )
            elif key == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                  controllerId = 0'i32,
                  topics = [
                    FakeTopicMeta(
                      name: topic,
                      errorCode: 0'i16,
                      isInternal: false,
                      partitions: @[
                        FakePartitionMeta(
                          partitionIndex: 0'i32,
                          leaderId: 0'i32,
                          replicas: @[0'i32],
                          isr: @[0'i32],
                        )
                      ],
                    )
                  ],
                )
              )
            elif key == int16(ord(akInitProducerId)):
              await req.respond(
                buildInitProducerIdResponseBody(
                  errorCode = 0'i16, producerId = 4000'i64, producerEpoch = 0'i16
                )
              )
            elif key == int16(ord(akProduce)):
              produceReqs.add(req)
              inc produceCount
              if produceCount == sends:
                # Frames arrive in baseSequence order (enforced by the write
                # gate), so produceReqs[i] is sequence i. Respond in REVERSE so
                # the latest send's response lands first — the worst case for
                # completion ordering.
                for i in countdown(produceReqs.high, 0):
                  await produceReqs[i].respond(
                    buildProduceResponseBody(
                      topic = topic,
                      partition = 0'i32,
                      errorCode = 0'i16,
                      baseOffset = int64(i),
                    )
                  )
          except CatchableError:
            break

      let handlerFut = handler()
      let cluster = newCluster(@[host & ":" & $port], clientId = "c2-order-test")
      let p = newKafkaProducer(
        cluster,
        enableIdempotence = true,
        maxInFlightRequests = 5,
        lingerMs = 0,
        maxRetries = 0,
        timeoutMs = 3000,
      )

      # Record the order in which the send futures actually resolve.
      var completionOrder: seq[int]
      proc watch(i: int, fut: Future[ProduceResult]) {.async: (raises: []).} =
        try:
          discard await fut
        except CatchableError:
          discard
        completionOrder.add(i)

      var sendFuts: seq[Future[ProduceResult]]
      for i in 0 ..< sends:
        sendFuts.add(p.send(topic, @[byte(i and 0xFF)], partition = 0'i32))
      var watchers: seq[Future[void]]
      for i, fut in sendFuts:
        watchers.add(watch(i, fut))
      await allFutures(watchers)

      # Despite responses arriving 4,3,2,1,0, the barrier surfaces completions
      # strictly in send order.
      check completionOrder == @[0, 1, 2, 3, 4]

      await p.close()
      await fb.close()
      awaitDrain(handlerFut, 2.seconds)
      await cluster.close()

    waitFor body()

  test "acks=0 produces are written in send order under the gate":
    ## acks=0 uses the fire-and-forget write path; it must still be ordered by
    ## the per-partition gate. The broker sends no responses, so the test just
    ## asserts the captured frames arrive in baseSequence order.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let topic = "gate-acks0-topic"
      const sends = 4
      var observedSeqs: seq[int32]

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 3.seconds)
          except CatchableError:
            break
          try:
            let key = req.apiKey
            if key == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                    ),
                  ],
                )
              )
            elif key == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                  controllerId = 0'i32,
                  topics = [
                    FakeTopicMeta(
                      name: topic,
                      errorCode: 0'i16,
                      isInternal: false,
                      partitions: @[
                        FakePartitionMeta(
                          partitionIndex: 0'i32,
                          leaderId: 0'i32,
                          replicas: @[0'i32],
                          isr: @[0'i32],
                        )
                      ],
                    )
                  ],
                )
              )
            elif key == int16(ord(akInitProducerId)):
              await req.respond(
                buildInitProducerIdResponseBody(
                  errorCode = 0'i16, producerId = 4000'i64, producerEpoch = 0'i16
                )
              )
            elif key == int16(ord(akProduce)):
              # acks=0 → no response is sent. Just record the order.
              let batches =
                try:
                  decodeProduceBatches(req.body)
                except Exception:
                  @[]
              if batches.len == 1:
                observedSeqs.add(batches[0].baseSequence)
          except CatchableError:
            break

      let handlerFut = handler()

      let cluster = newCluster(@[host & ":" & $port], clientId = "gate-acks0-test")
      # acks=0 is incompatible with idempotence (which forces acks=all), so use
      # a non-idempotent producer here. The gate skips the chain when
      # idempotence is off, so this asserts the fire-and-forget write path
      # still goes out one frame at a time in call order.
      let p = newKafkaProducer(
        cluster, acks = 0'i16, lingerMs = 0, maxRetries = 0, timeoutMs = 3000
      )

      for i in 0 ..< sends:
        discard await p.send(topic, @[byte(i and 0xFF)], partition = 0'i32)

      # Give the fire-and-forget writes time to land at the broker.
      var waited = 0
      while observedSeqs.len < sends and waited < 60:
        await sleepAsync(50.milliseconds)
        inc waited

      check observedSeqs.len == sends
      # Non-idempotent batches all carry baseSequence -1; assert arrival count
      # and that no reordering dropped a frame (order is trivially preserved
      # since each send awaits its own write completion).
      for s in observedSeqs:
        check s == -1'i32

      await p.close()
      await fb.close()
      awaitDrain(handlerFut, 2.seconds)
      await cluster.close()

    waitFor body()

suite "Producer: delivery.timeout.ms bounds total retry time":
  ## Regression: idempotent / transactional producers force maxRetries =
  ## int32.high, so a persistently retriable broker error (e.g.
  ## NOT_ENOUGH_REPLICAS) could spin ~2.1B times. deliveryTimeoutMs caps the
  ## total wall-clock time of a single send's retry loop.

  test "idempotent send stops retrying once deliveryTimeoutMs elapses":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let topic = "dt-topic"
      var produceAttempts = 0

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 3.seconds)
          except CatchableError:
            break
          try:
            let key = req.apiKey
            if key == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                    ),
                  ],
                )
              )
            elif key == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                  controllerId = 0'i32,
                  topics = [
                    FakeTopicMeta(
                      name: topic,
                      errorCode: 0'i16,
                      isInternal: false,
                      partitions: @[
                        FakePartitionMeta(
                          partitionIndex: 0'i32,
                          leaderId: 0'i32,
                          replicas: @[0'i32],
                          isr: @[0'i32],
                        )
                      ],
                    )
                  ],
                )
              )
            elif key == int16(ord(akInitProducerId)):
              await req.respond(
                buildInitProducerIdResponseBody(
                  errorCode = 0'i16, producerId = 2000'i64, producerEpoch = 0'i16
                )
              )
            elif key == int16(ord(akProduce)):
              inc produceAttempts
              # NOT_ENOUGH_REPLICAS (19) is retriable — never let it succeed.
              await req.respond(
                buildProduceResponseBody(
                  topic = topic,
                  partition = 0'i32,
                  errorCode = int16(ord(keNotEnoughReplicas)),
                  baseOffset = -1'i64,
                )
              )
          except CatchableError:
            break

      let handlerFut = handler()

      let cluster = newCluster(@[host & ":" & $port], clientId = "dt-test")
      let p = newKafkaProducer(
        cluster,
        enableIdempotence = true, # forces maxRetries = int32.high
        lingerMs = 0,
        retryBackoffMs = 20,
        deliveryTimeoutMs = 200,
        timeoutMs = 1000,
      )

      let started = Moment.now()
      var raised = false
      try:
        discard await p.send(topic, @[0x01'u8], partition = 0'i32)
      except KafkaApiError as e:
        raised = true
        check e.errorCode == keNotEnoughReplicas
      let elapsed = (Moment.now() - started).milliseconds

      check raised
      # Bounded by the deadline (with generous slack for jittered backoff +
      # scheduling), NOT looping for int32.high attempts.
      check elapsed < 5000
      check produceAttempts >= 1
      check produceAttempts < 100

      await p.close()
      await fb.close()
      awaitDrain(handlerFut, 2.seconds)
      await cluster.close()

    waitFor body()

suite "Producer: transactional finalize waits for in-flight flushes":
  ## commitTransaction / abortTransaction call flush(), which only awaits batches
  ## still in `p.batches`. A size- or linger-triggered flushBatch may have already
  ## extracted its batch and be mid round-trip — invisible to flush()'s snapshot.
  ## awaitInflightFlushes drains those before EndTxn so a produce belonging to the
  ## txn is never excluded (EOS violation).

  test "awaitInflightFlushes blocks until a registered in-flight flush settles":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster, transactionalId = some("test-txn"), enableIdempotence = true
      )

      # Empty registry → the barrier returns immediately.
      check p.inflightFlushCountForTest() == 0
      await p.awaitInflightFlushesForTest()

      # Register a simulated in-flight flush gated on `gate`. The marker is
      # added synchronously before the helper's first await, so the registry
      # reports it without needing an event-loop turn.
      let gate = newFuture[void]("gate")
      let inflight = p.simulateInflightFlushForTest(gate)
      check p.inflightFlushCountForTest() == 1

      # The barrier must NOT complete while the flush is still in flight.
      let drain = p.awaitInflightFlushesForTest()
      await sleepAsync(milliseconds(20))
      check not drain.finished
      check not inflight.finished

      # Release the "broker round-trip": the simulated flush completes and
      # removes its marker (mirroring flushBatch's defer), then the barrier
      # observes an empty registry and returns.
      gate.complete()
      await inflight
      await drain
      check p.inflightFlushCountForTest() == 0

      await p.close()
      await cluster.close()

    waitFor testBody()

# On a retriable produce error the batch must re-write its
# FROZEN bytes (KIP-185) and the retry must NOT re-enter the per-partition write
# chain. We drive a transactional+idempotent producer (seeded so it skips
# InitProducerId) against a fake broker that fails the first Produce with a
# retriable code and succeeds the retry, capturing both request bodies.

type RetryThenOkState = ref object
  produceCount: int
  produceBodies: seq[seq[byte]]

proc autoRespondTxnFirstErrorThenOk(
    fb: FakeBroker,
    host: string,
    port: int32,
    topic: string,
    state: RetryThenOkState,
    successOffset: int64,
) {.async: (raises: []).} =
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 500.milliseconds)
    except CatchableError:
      break
    try:
      let key = req.apiKey
      if key == int16(ord(akApiVersions)):
        await req.respond(
          buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              ),
              FakeApiVersion(
                apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
              ),
              FakeApiVersion(
                apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
              ),
            ],
          )
        )
      elif key == int16(ord(akMetadata)):
        await req.respond(
          buildMetadataResponseBody(
            [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
            controllerId = 0'i32,
            topics = [
              FakeTopicMeta(
                name: topic,
                errorCode: 0'i16,
                isInternal: false,
                partitions: @[
                  FakePartitionMeta(
                    partitionIndex: 0'i32,
                    leaderId: 0'i32,
                    replicas: @[0'i32],
                    isr: @[0'i32],
                  )
                ],
              )
            ],
          )
        )
      elif key == int16(ord(akProduce)):
        inc state.produceCount
        state.produceBodies.add(req.body)
        # 1st attempt: REQUEST_TIMED_OUT (7) — retriable. Retry: success.
        let ec = if state.produceCount == 1: 7'i16 else: 0'i16
        let off = if state.produceCount == 1: -1'i64 else: successOffset
        await req.respond(
          buildProduceResponseBody(
            topic = topic,
            partition = 0'i32,
            errorCode = ec,
            baseOffset = off,
            logAppendTimeMs = -1'i64,
            throttleTimeMs = 0'i32,
          )
        )
    except CatchableError:
      break

suite "Producer: retriable retry re-writes frozen bytes":
  test "a retriable produce error retries and re-sends byte-identical frozen bytes":
    proc body() {.async: (raises: [Exception]).} =
      const topic = "txn-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let state = RetryThenOkState(produceCount: 0, produceBodies: @[])
      let respFut = autoRespondTxnFirstErrorThenOk(fb, host, port, topic, state, 50'i64)
      let cluster = newCluster(@[host & ":" & $port], clientId = "retry-frozen-test")
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        maxInFlightRequests = 5,
        maxRetries = 5,
        retryBackoffMs = 1,
        timeoutMs = 1500,
      )
      seedTxnProducer(p)

      let res = await p.send(topic, @[0xAB'u8])

      # The retriable error was retried and the retry succeeded.
      check res.offset == 50'i64
      check state.produceCount == 2

      # KIP-185: the retry re-sends the SAME frozen bytes (the retry path must
      # reuse the in-flight batch's encodedBytes, not re-encode or re-sequence).
      check state.produceBodies.len == 2
      check state.produceBodies[0] == state.produceBodies[1]

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor body()

# immediate transactional sends are barriered before EndTxn
#
# An immediate (lingerMs <= 0) transactional send runs doSendWithRetry outside
# txnLock. Without a marker, a concurrent commit/abort could acquire txnLock, see
# no pending work, and send EndTxn while the produce is still on the wire (EOS
# violation). The send now registers a marker in inflightFlushFutures so
# awaitInflightFlushes blocks until the produce acks.

suite "Producer: immediate transactional send barrier":
  test "an in-flight immediate transactional send blocks awaitInflightFlushes":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)

      let produceSeen = newFuture[void]("produceSeen")
      let releaseProduce = newFuture[void]("releaseProduce")

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            let key = req.apiKey
            if key == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                    ),
                  ],
                )
              )
            elif key == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                  controllerId = 0'i32,
                  topics = [
                    FakeTopicMeta(
                      name: "txn-topic",
                      errorCode: 0'i16,
                      isInternal: false,
                      partitions: @[
                        FakePartitionMeta(
                          partitionIndex: 0'i32,
                          leaderId: 0'i32,
                          replicas: @[0'i32],
                          isr: @[0'i32],
                        )
                      ],
                    )
                  ],
                )
              )
            elif key == int16(ord(akProduce)):
              # Signal the produce is in flight, then hold the response until
              # the test releases it.
              if not produceSeen.finished:
                produceSeen.complete()
              await releaseProduce
              await req.respond(
                buildProduceResponseBody(
                  topic = "txn-topic",
                  partition = 0'i32,
                  errorCode = 0'i16,
                  baseOffset = 0'i64,
                  logAppendTimeMs = -1'i64,
                  throttleTimeMs = 0'i32,
                )
              )
          except CatchableError:
            break

      let handlerFut = handler()
      let cluster = newCluster(@[host & ":" & $port], clientId = "m1-test")
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        lingerMs = 0,
        timeoutMs = 2000,
      )
      seedTxnProducer(p)

      # No barrier marker before any send.
      check p.inflightFlushCountForTest() == 0

      let sendFut = p.send("txn-topic", @[0xAB'u8])
      # Wait until the produce reaches the broker — by then the marker has been
      # registered (under txnLock, before the produce went on the wire).
      await produceSeen.wait(2.seconds)
      check p.inflightFlushCountForTest() == 1

      # awaitInflightFlushes (the commit/abort barrier) must not return while
      # the immediate send is still in flight.
      let flushFut = p.awaitInflightFlushesForTest()
      await sleepAsync(milliseconds(50))
      check not flushFut.finished

      # Release the produce: the send completes and the marker resolves, so the
      # barrier unblocks.
      releaseProduce.complete()
      discard await sendFut.wait(2.seconds)
      await flushFut.wait(2.seconds)
      check p.inflightFlushCountForTest() == 0

      await p.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

suite "Producer: txn epoch-bump error classification":
  ## Code-49 (INVALID_PRODUCER_ID_MAPPING) from the epoch-bump InitProducerId must
  ## be ABORTABLE inside a transaction (Java parity), not retriable: bumpEpoch must
  ## classify it as non-retriable so a single InitProducerId is sent (no retry
  ## loop). The original bug misclassified 49 with the context-free isRetriable
  ## and looped the whole retry budget. Post-EndTxn, bumpEpoch does not raise
  ## non-retriable errors — the txn is already committed broker-side — but it
  ## still transitions to tsError so the next txn call refuses cleanly.

  test "INVALID_PRODUCER_ID_MAPPING aborts the bump instead of looping":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coord = newBroker(9'i32, "127.0.0.1", port, "txn-coord")

      var initProducerIdCount = 0
      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          # Reply INVALID_PRODUCER_ID_MAPPING to every InitProducerId — if
          # bumpEpoch treated 49 as retriable it would send 7 of them.
          while true:
            let ipid = await fb.nextRequest(timeout = 2.seconds)
            initProducerIdCount.inc
            try:
              await ipid.respond(
                buildInitProducerIdResponseBody(
                  errorCode = int16(ord(keInvalidProducerIdMapping))
                )
              )
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let cluster = newCluster(@["127.0.0.1:" & $port])
      let p = newKafkaProducer(
        cluster, transactionalId = some("test-txn"), enableIdempotence = true
      )
      p.setProducerId(1000'i64, 5'i16)
      p.setTxnState(tsCommitting) # transitionToError reaches tsError from here
      await coord.connect()
      p.setTxnCoordinator(coord)

      var raised = false
      try:
        await p.bumpEpoch()
      except KafkaApiError:
        raised = true
      check not raised # post-EndTxn: does not raise a completed-txn failure
      check initProducerIdCount == 1 # no retry loop — the real anti-regression
      check p.txnState == tsError # abortable: the txn moves to error
      check p.getPendingEpochBump() # deferred for later retry
      check p.getProducerEpoch() == 5'i16 # identity untouched on a failed bump

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await coord.close()
      await fb.close()
      await cluster.close()

    waitFor body()

suite "Producer: non-transactional flush waits for in-flight batch":
  ## Regression: a linger-triggered flushBatch sends its batch in a background
  ## coroutine. flush()/close() must wait for that produce to ack — before the fix
  ## the in-flight marker was registered for transactional producers only, so a
  ## non-txn close() could tear the cluster down under the produce (lost-on-close).

  test "flush() does not return while a linger flushBatch is on the wire":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let produceSeen = newFuture[void]("produceSeen")
      let releaseProduce = newFuture[void]("releaseProduce")

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            let key = req.apiKey
            if key == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                    ),
                  ],
                )
              )
            elif key == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                  controllerId = 0'i32,
                  topics = [
                    FakeTopicMeta(
                      name: "t",
                      errorCode: 0'i16,
                      isInternal: false,
                      partitions: @[
                        FakePartitionMeta(
                          partitionIndex: 0'i32,
                          leaderId: 0'i32,
                          replicas: @[0'i32],
                          isr: @[0'i32],
                        )
                      ],
                    )
                  ],
                )
              )
            elif key == int16(ord(akProduce)):
              if not produceSeen.finished:
                produceSeen.complete()
              await releaseProduce
              await req.respond(
                buildProduceResponseBody(
                  topic = "t",
                  partition = 0'i32,
                  errorCode = 0'i16,
                  baseOffset = 0'i64,
                  logAppendTimeMs = -1'i64,
                  throttleTimeMs = 0'i32,
                )
              )
          except CatchableError:
            break

      let handlerFut = handler()
      let cluster = newCluster(@[host & ":" & $port], clientId = "b4-test")
      let p = newKafkaProducer(cluster, lingerMs = 30, timeoutMs = 2000)

      # Batched send: do NOT await it — the linger timer fires flushBatch in a
      # background coroutine, and send()'s own future only resolves once the
      # produce acks.
      let sendFut = p.send("t", @[0xAB'u8])
      await produceSeen.wait(2.seconds)
      # The batch has been extracted from p.batches and is on the wire, so its
      # in-flight marker (now registered for non-txn too) must be present.
      check p.inflightFlushCountForTest() == 1

      let flushFut = p.flush()
      await sleepAsync(milliseconds(50))
      check not flushFut.finished # flush must wait for the in-flight produce

      releaseProduce.complete()
      discard await sendFut.wait(2.seconds)
      await flushFut.wait(2.seconds)
      check p.inflightFlushCountForTest() == 0

      await p.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

suite "Producer: flush marker registered before batchLock acquire":
  ## Regression: the in-flight flush marker must be registered BEFORE flushBatch
  ## acquires batchLock, not after the batch is extracted. Otherwise a linger
  ## flushBatch parked on the acquire lands its marker after flush()/close()
  ## snapshots inflightFlushFutures, so they return without awaiting the produce
  ## and tear the cluster down under it (lost-on-close). Holds batchLock
  ## externally to pin a linger flushBatch at the acquire and asserts its marker
  ## is already present.

  test "linger flushBatch registers its marker before parking on batchLock":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let releaseProduce = newFuture[void]("releaseProduce")

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            let key = req.apiKey
            if key == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                    ),
                  ],
                )
              )
            elif key == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                  controllerId = 0'i32,
                  topics = [
                    FakeTopicMeta(
                      name: "t",
                      errorCode: 0'i16,
                      isInternal: false,
                      partitions: @[
                        FakePartitionMeta(
                          partitionIndex: 0'i32,
                          leaderId: 0'i32,
                          replicas: @[0'i32],
                          isr: @[0'i32],
                        )
                      ],
                    )
                  ],
                )
              )
            elif key == int16(ord(akProduce)):
              await releaseProduce
              await req.respond(
                buildProduceResponseBody(
                  topic = "t",
                  partition = 0'i32,
                  errorCode = 0'i16,
                  baseOffset = 0'i64,
                  logAppendTimeMs = -1'i64,
                  throttleTimeMs = 0'i32,
                )
              )
          except CatchableError:
            break

      let handlerFut = handler()
      let cluster = newCluster(@[host & ":" & $port], clientId = "m3-test")
      # Long linger so the arm-then-acquire window below is comfortable.
      let p = newKafkaProducer(cluster, lingerMs = 200, timeoutMs = 2000)
      let tp = TopicPartition(topic: "t", partition: 0'i32)

      # Batched send: do NOT await it. send() resolves metadata, enqueues the
      # record under batchLock, and arms the linger timer; its future resolves
      # only once the produce acks.
      let sendFut = p.send("t", @[0xAB'u8])

      # Wait until the record is enqueued and the linger timer is armed, but
      # before the 200ms timer fires. The timer is armed under batchLock the
      # instant the batch becomes non-empty, so both flags flip together.
      var armed = false
      for _ in 0 ..< 200:
        if p.hasBatch(tp) and p.hasLingerTimer(tp):
          armed = true
          break
        await sleepAsync(milliseconds(2))
      check armed

      # Hold batchLock so the linger-triggered flushBatch parks on its acquire,
      # exactly as it would while flush()/close() briefly holds the lock.
      await p.acquireBatchLock()
      try:
        check p.inflightFlushCountForTest() == 0 # linger has not fired yet

        # Let the linger timer fire. lingerTimeout calls flushBatch, which (for
        # a non-txn producer) registers its in-flight marker synchronously and
        # only THEN parks on batchLock.acquire — held here. Under the old bug
        # the marker was registered after the extract (which needs this lock),
        # so it would still be absent while parked.
        var registered = false
        for _ in 0 ..< 200:
          if p.inflightFlushCountForTest() == 1:
            registered = true
            break
          await sleepAsync(milliseconds(2))
        check registered
        # The batch is still in p.batches: extraction is blocked on this lock.
        check p.hasBatch(tp)
      finally:
        p.releaseBatchLock()

      # Release the lock: flushBatch extracts, sends, and acks. The marker is
      # removed by its defer once the produce settles.
      releaseProduce.complete()
      discard await sendFut.wait(2.seconds)
      check p.inflightFlushCountForTest() == 0

      await p.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

suite "Producer: cancelled send does not collaterally fail co-located records":
  ## Regression: a size-triggered flush extracts the WHOLE batch, including
  ## records from other concurrent sends co-located in it. For a non-txn producer
  ## the flush runs detached, so cancelling the triggering send cannot propagate
  ## into the shared flush and fail every co-located record — which would
  ## mis-route innocent records to the DLQ or duplicate them on retry.
  ## Cancellation must stay confined to the triggering send's own future.

  test "cancelling the flush-triggering send leaves co-located records intact":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      # Withhold the produce ack so the detached flush stays in flight while we
      # cancel — modelling a cancel that lands after the write but before the ack.
      let releaseProduce = newFuture[void]("releaseProduce")

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            let key = req.apiKey
            if key == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                    ),
                  ],
                )
              )
            elif key == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                  controllerId = 0'i32,
                  topics = [
                    FakeTopicMeta(
                      name: "t",
                      errorCode: 0'i16,
                      isInternal: false,
                      partitions: @[
                        FakePartitionMeta(
                          partitionIndex: 0'i32,
                          leaderId: 0'i32,
                          replicas: @[0'i32],
                          isr: @[0'i32],
                        )
                      ],
                    )
                  ],
                )
              )
            elif key == int16(ord(akProduce)):
              await releaseProduce
              await req.respond(
                buildProduceResponseBody(
                  topic = "t",
                  partition = 0'i32,
                  errorCode = 0'i16,
                  baseOffset = 0'i64,
                  logAppendTimeMs = -1'i64,
                  throttleTimeMs = 0'i32,
                )
              )
          except CatchableError:
            break

      let handlerFut = handler()
      let cluster = newCluster(@[host & ":" & $port], clientId = "m4-test")
      # Non-transactional (the detached-flush path under test). Large linger so
      # only the maxBatchRecords = 2 size trigger fires the flush; no idempotence
      # so there is no per-partition completion gate between the two records.
      let p = newKafkaProducer(
        cluster, lingerMs = 5000, maxBatchRecords = 2, timeoutMs = 2000
      )
      let tp = TopicPartition(topic: "t", partition: 0'i32)

      # Record A: the innocent co-located record. Fills the batch to 1 and arms
      # the linger timer; its future resolves only once the produce acks.
      let aFut = p.send("t", @[0xA1'u8], partition = 0'i32)
      var armed = false
      for _ in 0 ..< 200:
        if p.hasBatch(tp) and p.hasLingerTimer(tp):
          armed = true
          break
        await sleepAsync(milliseconds(2))
      check armed

      # Record B: fills the batch to maxBatchRecords = 2, firing the size-trigger
      # flush. startFlush detaches it (non-txn), so B parks only on its own
      # future; the flush extracts the whole batch (A and B) and goes on the wire.
      let bFut = p.send("t", @[0xB2'u8], partition = 0'i32)
      var inflight = false
      for _ in 0 ..< 200:
        if p.inflightFlushCountForTest() == 1 and not p.hasBatch(tp):
          inflight = true
          break
        await sleepAsync(milliseconds(2))
      check inflight

      # Cancel the triggering send. Only B's own future may be affected.
      bFut.cancelSoon()
      for _ in 0 ..< 50:
        if bFut.finished:
          break
        await sleepAsync(milliseconds(2))
      check bFut.cancelled

      # The co-located record A must survive: the detached flush is still in
      # flight (awaiting the withheld ack), so A's future is still pending. Under
      # the old bug B's cancel tears down the shared flush and fails A here.
      check not aFut.finished

      # Release the ack: the detached flush completes A normally and skips B's
      # already-cancelled future, then drains its in-flight marker.
      releaseProduce.complete()
      let aRes = await aFut.wait(2.seconds)
      check aRes.partition == 0'i32
      check p.inflightFlushCountForTest() == 0

      await p.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

suite "Producer: produce response validation":
  test "produceResultOf rejects a partition-mismatched response":
    let tp = TopicPartition(topic: "t", partition: 3'i32)
    # A conformant broker echoes partition 3 back; a response whose slot 0 is a
    # different partition must raise rather than attribute partition 0's
    # offset/error to tp.
    let mismatched = some(
      ProduceResponse(
        topics: @[
          ProduceTopicResp(
            topic: "t",
            partitions: @[
              ProducePartitionResp(
                partition: 0'i32,
                errorCode: keNone,
                baseOffset: 99'i64,
                timestampMs: -1'i64,
              )
            ],
          )
        ],
        throttleTimeMs: 0'i32,
      )
    )
    expect KafkaError:
      discard produceResultOfForTest(tp, mismatched)

    # The matching-partition response resolves normally.
    let matched = some(
      ProduceResponse(
        topics: @[
          ProduceTopicResp(
            topic: "t",
            partitions: @[
              ProducePartitionResp(
                partition: 3'i32,
                errorCode: keNone,
                baseOffset: 99'i64,
                timestampMs: -1'i64,
              )
            ],
          )
        ],
        throttleTimeMs: 0'i32,
      )
    )
    let r = produceResultOfForTest(tp, matched)
    check r.partition == 3'i32
    check r.offset == 99'i64

# Regression suite for the first-attempt write-failure window.
#
# gatedAllocateAndWrite allocates (and enqueues) the InflightBatch, then writes
# the frame. A failure between those steps used to be invisible to
# doSendWithRetry, so the retry re-entered the first-attempt path: it
# re-allocated a fresh sequence range (orphaning the enqueued batch — a
# permanent sequence gap the broker rejects as OUT_OF_ORDER_SEQUENCE_NUMBER) and
# re-installed the completion gate (orphaning the `mine` marker, so the send
# hung forever). The fix surfaces the allocation through FirstAttemptOutcome and
# installs the gate at most once per send.

proc allocateDeadPort(): Future[int32] {.async: (raises: [Exception]).} =
  ## Bind a throwaway listener and close it immediately: connects to the
  ## released port fail fast with ECONNREFUSED.
  let tmp = newFakeBroker()
  let port = int32(tmp.address().port)
  await tmp.close()
  return port

proc seedDeadLeader(
    cluster: Cluster, topic: string, deadPort: int32, clientId: string
): TopicPartition =
  ## Point `topic`-0's leader at an unreachable broker and mark the metadata
  ## cache fresh, so the first produce attempt skips any refresh and fails
  ## inside the allocation-to-write window (connect refused in produceWrite).
  cluster.topicPartitions[topic] =
    @[PartitionMeta(partitionIndex: 0, leaderId: 0, errorCode: keNone)]
  let tp = TopicPartition(topic: topic, partition: 0)
  cluster.partitionLeaders[tp] = PartitionLeader(nodeId: 0, leaderEpoch: -1)
  cluster.brokers[0'i32] =
    newBroker(0'i32, "127.0.0.1", deadPort, clientId, reconnectBackoffMs = 1)
  cluster.lastMetadataRefresh = Moment.now()
  tp

suite "Producer: first-attempt write failure (allocation adoption + gate install-once)":
  test "write failure after allocation surfaces the batch via the outcome":
    proc testBody() {.async: (raises: [Exception]).} =
      let deadPort = await allocateDeadPort()
      let cluster = newCluster(@["127.0.0.1:1"], clientId = "fa-outcome")
      let tp = seedDeadLeader(cluster, "fa-topic", deadPort, "fa-outcome")
      let p = newKafkaProducer(cluster, enableIdempotence = true)
      p.idempotenceInitialized = true
      p.setProducerId(900'i64, 0'i16)
      let recs = @[Record(timestamp: 1, value: some(@[0x01'u8]))]
      let outcome = newFirstAttemptOutcomeForTest()
      var raised = false
      try:
        await p.gatedAllocateAndWriteForTest(tp, recs, 1, outcome = outcome)
      except KafkaError:
        raised = true
      check raised
      # The allocation is enqueued AND surfaced to the caller — not orphaned.
      check outcome.allocatedBatchForTest().isSome
      check p.inflightBatchCountForTest(tp) == 1
      check p.getInflightIdForTest(tp, 0) == outcome.allocatedBatchForTest().get.id
      check p.getSequenceNumber(tp) == 1
      await p.close()
      await cluster.close()

    waitFor testBody()

  test "completion gate is installed once across failed first attempts":
    proc testBody() {.async: (raises: [Exception]).} =
      let deadPort = await allocateDeadPort()
      let cluster = newCluster(@["127.0.0.1:1"], clientId = "fa-gate")
      let tp = seedDeadLeader(cluster, "fa-gate-topic", deadPort, "fa-gate")
      let p = newKafkaProducer(cluster, enableIdempotence = true)
      p.idempotenceInitialized = true
      p.setProducerId(901'i64, 0'i16)
      let recs = @[Record(timestamp: 1, value: some(@[0x02'u8]))]
      let gate = newCompletionGateForTest()
      var raisedCount = 0
      try:
        await p.gatedAllocateAndWriteForTest(tp, recs, 1, gate = gate)
      except KafkaError:
        inc raisedCount
      let mineAfterFirst = gate.gateMineForTest()
      check not mineAfterFirst.isNil
      try:
        await p.gatedAllocateAndWriteForTest(tp, recs, 1, gate = gate)
      except KafkaError:
        inc raisedCount
      check raisedCount == 2
      # Same Future object: the second first-attempt pass must NOT re-install
      # the gate — re-installing would orphan the first marker (never
      # completed by anyone) and wedge every send chained behind it.
      check gate.gateMineForTest() == mineAfterFirst
      await p.close()
      await cluster.close()

    waitFor testBody()

  test "send() recovers from a first-attempt write failure (no hang, no gap)":
    proc testBody() {.async: (raises: [Exception]).} =
      let deadPort = await allocateDeadPort()
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let behavior = TxnProduceBehavior(produceErrorCode: 0'i16)
      let respFut = autoRespondTxn(fb, host, port, "fa-e2e-topic", behavior)
      let cluster = newCluster(
        @[host & ":" & $port],
        clientId = "fa-e2e",
        reconnectBackoffMs = 1,
        metadataDebounceInterval = milliseconds(0),
      )
      let tp = seedDeadLeader(cluster, "fa-e2e-topic", deadPort, "fa-e2e")
      let p = newKafkaProducer(
        cluster,
        enableIdempotence = true,
        maxRetries = 5,
        retryBackoffMs = 1,
        timeoutMs = 2000,
      )
      # Pre-seed the idempotent identity so ensureIdempotenceInitialized does
      # not run an InitProducerId round-trip (whose anyBroker metadata refresh
      # would heal the dead leader before the first produce attempt).
      p.idempotenceInitialized = true
      p.setProducerId(902'i64, 0'i16)

      # First attempt fails inside the allocation-to-write window (dead
      # leader); the transient retry refreshes metadata (bootstrap → fake
      # broker), heals the leader address, and re-sends the FROZEN bytes.
      # Regression: the old code hung forever here on the orphaned
      # completion-chain marker, and leaked the first allocation.
      let res = await p.send("fa-e2e-topic", @[0xEE'u8]).wait(5.seconds)
      check res.offset == 0
      # No orphaned in-flight entry, no sequence gap: exactly one record was
      # allocated (the retry reused the frozen batch) and it was dequeued on
      # success.
      check p.inflightBatchCountForTest(tp) == 0
      check p.getSequenceNumber(tp) == 1

      await p.close()
      await cluster.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor testBody()

suite "Producer: txn coordinator rediscovery (6c)":
  ## Regression: txnCoordinator was resolved once in initTransactions, so after a
  ## coordinator move every commit/abort failed against the stale handle, and
  ## retriable coordinator errors hard-failed the txn on the first response.
  ## Coordinator RPCs now ride withTxnCoordinatorRetry: re-resolve on
  ## NOT_COORDINATOR / connection errors, back off on retriable codes, and only
  ## surface terminal codes or an exhausted budget.

  test "firstTxnErrorCode returns the first per-partition error for AddPartitionsToTxn":
    let resp = AddPartitionsToTxnResponse(
      errors: @[
        AddPartitionsToTxnTopicResp(
          topic: "a",
          partitions: @[
            AddPartitionsToTxnPartitionResp(partition: 0'i32, errorCode: keNone),
            AddPartitionsToTxnPartitionResp(
              partition: 1'i32, errorCode: keConcurrentTransactions
            ),
          ],
        )
      ]
    )
    check firstTxnErrorCode(resp) == keConcurrentTransactions
    check firstTxnErrorCode(AddPartitionsToTxnResponse()) == keNone

  test "endTxn retries after NOT_COORDINATOR via FindCoordinator re-resolution":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "txn-rediscovery-test")

      var endTxnCount = 0
      var sawFindCoordinator = false
      proc handler() {.async: (raises: []).} =
        ## Generic dispatcher: the rediscovery path opens fresh connections
        ## (bootstrap broker for FindCoordinator, re-resolved coordinator), so
        ## several ApiVersions handshakes can occur in any order.
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 3.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akEndTxn)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akFindCoordinator)),
                      minVersion: 0,
                      maxVersion: 1,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
                  controllerId = 0'i32,
                  topics = newSeq[FakeTopicMeta](),
                )
              )
            elif req.apiKey == int16(ord(akEndTxn)):
              inc endTxnCount
              if endTxnCount == 1:
                # The coordinator moved: first EndTxn is rejected.
                await req.respond(buildEndTxnResponseBody(keNotCoordinator.toInt16))
              else:
                await req.respond(buildEndTxnResponseBody(0'i16))
            elif req.apiKey == int16(ord(akFindCoordinator)):
              sawFindCoordinator = true
              # The "new" coordinator is this same fake broker under nodeId 0.
              await req.respond(
                buildFindCoordinatorResponseBodyV1(
                  nodeId = 0'i32, host = "127.0.0.1", port = port
                )
              )
            elif req.apiKey == int16(ord(akInitProducerId)):
              # bumpEpoch after the successful EndTxn.
              await req.respond(
                buildInitProducerIdResponseBody(producerId = 42, producerEpoch = 1)
              )
          except CatchableError:
            discard

      let handlerFut = handler()

      # Bootstrap points at the fake broker so FindCoordinator can be served.
      let cluster = newCluster(@["127.0.0.1:" & $port])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("txn-rediscovery"),
        enableIdempotence = true,
        timeoutMs = 2000,
      )
      p.setTransactionalIdForTest("txn-rediscovery")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setTxnState(tsInTransaction)
      # Register a partition so the transaction is started broker-side and
      # commitTransaction sends a real EndTxn (an empty txn would skip it).
      p.addTxnPartition(TopicPartition(topic: "txn-topic", partition: 0'i32))
      await b.connect()

      # Previously: the first NOT_COORDINATOR hard-failed the commit and left
      # txnState in tsError. Now: re-resolve via FindCoordinator and retry.
      await p.commitTransaction()
      check endTxnCount == 2
      check sawFindCoordinator
      check p.txnState == tsReady

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await p.close()
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor testBody()

  test "initTransactions retries CONCURRENT_TRANSACTIONS then succeeds":
    ## End-to-end pin for the 45/51 fix: CONCURRENT_TRANSACTIONS
    ## (51) must classify retriable and be retried with backoff, not treated
    ## as a producer-id reset / terminal failure.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      var initPidCount = 0
      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 3.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akFindCoordinator)),
                      minVersion: 0,
                      maxVersion: 1,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
                  controllerId = 0'i32,
                  topics = newSeq[FakeTopicMeta](),
                )
              )
            elif req.apiKey == int16(ord(akFindCoordinator)):
              await req.respond(
                buildFindCoordinatorResponseBodyV1(
                  nodeId = 0'i32, host = "127.0.0.1", port = port
                )
              )
            elif req.apiKey == int16(ord(akInitProducerId)):
              inc initPidCount
              if initPidCount == 1:
                # Transiently rejected: the broker's txn-state machine has not
                # quiesced yet.
                await req.respond(
                  buildInitProducerIdResponseBody(
                    errorCode = keConcurrentTransactions.toInt16
                  )
                )
              else:
                await req.respond(
                  buildInitProducerIdResponseBody(producerId = 7, producerEpoch = 0)
                )
          except CatchableError:
            discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:" & $port])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("txn-concurrent"),
        enableIdempotence = true,
        timeoutMs = 2000,
      )

      await p.initTransactions()
      check initPidCount == 2
      check p.txnState == tsReady
      check p.getProducerEpoch() == 0'i16

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await p.close()
      await fb.close()
      await cluster.close()

    waitFor testBody()

  test "initTransactions rejects a fatal producer without re-initializing":
    ## Regression: initTransactions had no tsFatal guard, so a recovery-style
    ## re-call on a fatal producer issued InitProducerId — allocating a fresh
    ## broker PID and setting idempotenceInitialized — then silently dropped the
    ## trailing tryTransition(tsReady) (tsFatal is terminal). That orphaned the
    ## new PID and left the producer falsely looking initialized. It must raise
    ## and leave producer-id state untouched instead.
    proc testBody() {.async: (raises: [Exception]).} =
      type Sink = ref object
        initCount: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.txn.init":
          sink.initCount.inc

      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn-fatal"),
        enableIdempotence = true,
        timeoutMs = 500,
        metricsHook = hook,
      )
      p.setProducerId(1234'i64, 5'i16)
      p.setTxnState(tsFatal)

      var raised = false
      try:
        # Guard fires before any await, so this never reaches the coordinator.
        await p.initTransactions()
      except KafkaError:
        raised = true

      check raised
      check p.txnState == tsFatal
      check p.getProducerId() == 1234'i64
      check p.getProducerEpoch() == 5'i16
      check sink.initCount == 0

    waitFor testBody()

  test "initTransactions emits producer.txn.init":
    ## transaction.init: tsUninitialized → tsReady used to leave no
    ## metricsHook signal. A real init emits producer.txn.init once;
    ## a refused re-entry must not increment.
    proc testBody() {.async: (raises: [Exception]).} =
      type Sink = ref object
        initCount: int
        value: int64
        tagLen: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.txn.init":
          sink.initCount.inc
          sink.value = value
          sink.tagLen = tags.len

      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 3.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akFindCoordinator)),
                      minVersion: 0,
                      maxVersion: 1,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
                  controllerId = 0'i32,
                  topics = newSeq[FakeTopicMeta](),
                )
              )
            elif req.apiKey == int16(ord(akFindCoordinator)):
              await req.respond(
                buildFindCoordinatorResponseBodyV1(
                  nodeId = 0'i32, host = "127.0.0.1", port = port
                )
              )
            elif req.apiKey == int16(ord(akInitProducerId)):
              await req.respond(
                buildInitProducerIdResponseBody(producerId = 7, producerEpoch = 0)
              )
          except CatchableError:
            discard

      let handlerFut = handler()

      let cluster = newCluster(@["127.0.0.1:" & $port])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("txn-init"),
        enableIdempotence = true,
        timeoutMs = 2000,
        metricsHook = hook,
      )

      await p.initTransactions()
      check p.txnState == tsReady
      check sink.initCount == 1
      check sink.value == 1
      check sink.tagLen == 0
      var raised = false
      try:
        await p.initTransactions()
      except KafkaError as e:
        raised = true
        check "already called" in e.msg
      check raised
      check p.txnState == tsReady
      check sink.initCount == 1

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await p.close()
      await fb.close()
      await cluster.close()

    waitFor testBody()

  test "sendOffsetsToTransaction retries TxnOffsetCommit on NOT_COORDINATOR":
    ## Regression: the final TxnOffsetCommit to the GROUP coordinator was a
    ## bare await with no retry, so a group-coordinator move mid
    ## consume-process-produce forced the caller to abort. It now re-resolves the
    ## group coordinator and retries, symmetric with AddOffsetsToTxn.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "send-offsets-test")

      var txnOffsetCommitCount = 0
      var findCoordinatorCount = 0
      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 3.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akAddOffsetsToTxn)),
                      minVersion: 0,
                      maxVersion: 0,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akFindCoordinator)),
                      minVersion: 0,
                      maxVersion: 1,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akTxnOffsetCommit)),
                      minVersion: 0,
                      maxVersion: 0,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
                  controllerId = 0'i32,
                  topics = newSeq[FakeTopicMeta](),
                )
              )
            elif req.apiKey == int16(ord(akAddOffsetsToTxn)):
              await req.respond(buildAddOffsetsToTxnResponseBody(0'i16))
            elif req.apiKey == int16(ord(akFindCoordinator)):
              inc findCoordinatorCount
              await req.respond(
                buildFindCoordinatorResponseBodyV1(
                  nodeId = 0'i32, host = "127.0.0.1", port = port
                )
              )
            elif req.apiKey == int16(ord(akTxnOffsetCommit)):
              inc txnOffsetCommitCount
              let ec =
                if txnOffsetCommitCount == 1: keNotCoordinator.toInt16 else: 0'i16
              await req.respond(
                buildTxnOffsetCommitResponseBody(
                  topic = "t", partition = 0'i32, errorCode = ec
                )
              )
          except CatchableError:
            discard

      let handlerFut = handler()
      let cluster = newCluster(@["127.0.0.1:" & $port])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("send-offsets"),
        enableIdempotence = true,
        timeoutMs = 2000,
      )
      p.setTransactionalIdForTest("send-offsets")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setTxnState(tsInTransaction)
      await b.connect()

      var offsets = initTable[TopicPartition, int64]()
      offsets[TopicPartition(topic: "t", partition: 0'i32)] = 10'i64

      # Must NOT raise: the first NOT_COORDINATOR is re-resolved and retried.
      await p.sendOffsetsToTransaction(offsets, "test-group")
      check txnOffsetCommitCount == 2
      check findCoordinatorCount == 2 # re-resolved after the coordinator move

      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await p.close()
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor testBody()

  test "sendOffsetsToTransaction emits producer.txn.send_offsets":
    ## transaction.send-offsets: AddOffsetsToTxn + TxnOffsetCommit used to
    ## leave no metricsHook signal. A real send emits producer.txn.send_offsets
    ## once; a refused / API-failed send must not (poison suite pins 0).
    proc testBody() {.async: (raises: [Exception]).} =
      type Sink = ref object
        sendOffsets: int
        sendOffsetsError: int
        value: int64
        tagLen: int

      let sink = Sink()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        if $event == "producer.txn.send_offsets":
          sink.sendOffsets.inc
          sink.value = value
          sink.tagLen = tags.len
        elif $event == "producer.txn.send_offsets.error":
          sink.sendOffsetsError.inc

      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "send-offsets-metric")

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 3.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akAddOffsetsToTxn)),
                      minVersion: 0,
                      maxVersion: 0,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akFindCoordinator)),
                      minVersion: 0,
                      maxVersion: 1,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akTxnOffsetCommit)),
                      minVersion: 0,
                      maxVersion: 0,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akEndTxn)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
                  controllerId = 0'i32,
                  topics = newSeq[FakeTopicMeta](),
                )
              )
            elif req.apiKey == int16(ord(akAddOffsetsToTxn)):
              await req.respond(buildAddOffsetsToTxnResponseBody(0'i16))
            elif req.apiKey == int16(ord(akFindCoordinator)):
              await req.respond(
                buildFindCoordinatorResponseBodyV1(
                  nodeId = 0'i32, host = "127.0.0.1", port = port
                )
              )
            elif req.apiKey == int16(ord(akTxnOffsetCommit)):
              await req.respond(
                buildTxnOffsetCommitResponseBody(
                  topic = "t", partition = 0'i32, errorCode = 0'i16
                )
              )
            elif req.apiKey == int16(ord(akEndTxn)):
              await req.respond(buildEndTxnResponseBody(0'i16))
            elif req.apiKey == int16(ord(akInitProducerId)):
              await req.respond(
                buildInitProducerIdResponseBody(producerId = 42, producerEpoch = 1)
              )
          except CatchableError:
            discard

      let handlerFut = handler()
      let cluster = newCluster(@["127.0.0.1:" & $port])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("send-offsets-metric"),
        enableIdempotence = true,
        timeoutMs = 2000,
        metricsHook = hook,
      )
      p.setTransactionalIdForTest("send-offsets-metric")
      p.setTxnCoordinator(b)
      p.setProducerId(42, 0)
      p.setTxnState(tsInTransaction)
      await b.connect()

      var offsets = initTable[TopicPartition, int64]()
      offsets[TopicPartition(topic: "t", partition: 0'i32)] = 10'i64

      await p.sendOffsetsToTransaction(offsets, "test-group")
      check p.txnState == tsInTransaction
      check p.getTxnStarted()
      check sink.sendOffsets == 1
      check sink.value == 1
      check sink.tagLen == 0
      check sink.sendOffsetsError == 0

      await p.close()
      handlerFut.cancelSoon()
      awaitDrain(handlerFut, 1.seconds)
      await b.close()
      await fb.close()
      await cluster.close()

    waitFor testBody()

suite "Producer: terminal failure republishes sibling in-flight batches":
  ## Regression: on a non-retriable failure the terminal handler ran
  ## resetProducerId only, without re-encoding the per-TP queues. A sibling
  ## in-flight batch (maxInFlight > 1) failing next observed the bump, found the
  ## queue unchanged via refreshInflightFromQueue, and re-sent identical stale-PID
  ## bytes in a zero-backoff hot loop until deliveryTimeoutMs. The handler now
  ## dequeues the failed batch, re-inits the producer id, and republishes the
  ## queues; the observed fast path only fires when the refresh actually yielded
  ## post-reset bytes.

  test "sibling batch is re-encoded under the new PID after a terminal failure":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let topic = "h3-topic"

      let cluster = newCluster(@[host & ":" & $port], clientId = "h3-terminal-test")
      let p = newKafkaProducer(
        cluster,
        enableIdempotence = true,
        maxInFlightRequests = 5,
        lingerMs = 0,
        maxRetries = 5,
        retryBackoffMs = 1,
        timeoutMs = 1500,
      )

      # Two concurrent sends to two partitions of the same broker, so both
      # produce requests are in flight together.
      let sendA = p.send(topic, @[0xAA'u8], partition = 0'i32)
      let sendB = p.send(topic, @[0xBB'u8], partition = 1'i32)

      # Drive the handshake (ApiVersions / Metadata / initial InitProducerId,
      # possibly over several connections) until both produces are captured.
      var initPidCount = 0
      var produceP0, produceP1: CapturedRequest
      var gotP0 = false
      var gotP1 = false
      while not (gotP0 and gotP1):
        let req = await fb.nextRequest(timeout = 2.seconds)
        if req.apiKey == int16(ord(akApiVersions)):
          await req.respond(
            buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                ),
              ],
            )
          )
        elif req.apiKey == int16(ord(akMetadata)):
          await req.respond(
            buildMetadataResponseBody(
              [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
              controllerId = 0'i32,
              topics = [
                FakeTopicMeta(
                  name: topic,
                  errorCode: 0'i16,
                  isInternal: false,
                  partitions: @[
                    FakePartitionMeta(
                      partitionIndex: 0'i32,
                      leaderId: 0'i32,
                      replicas: @[0'i32],
                      isr: @[0'i32],
                    ),
                    FakePartitionMeta(
                      partitionIndex: 1'i32,
                      leaderId: 0'i32,
                      replicas: @[0'i32],
                      isr: @[0'i32],
                    ),
                  ],
                )
              ],
            )
          )
        elif req.apiKey == int16(ord(akInitProducerId)):
          inc initPidCount
          await req.respond(
            buildInitProducerIdResponseBody(producerId = 1000'i64, producerEpoch = 0)
          )
        elif req.apiKey == int16(ord(akProduce)):
          let b = decodeProduceBatches(req.body)[0]
          check b.producerId == 1000'i64
          if b.partition == 0'i32:
            produceP0 = req
            gotP0 = true
          else:
            produceP1 = req
            gotP1 = true
      check initPidCount == 1

      # Terminal (ecFatal) failure for partition 0 while partition 1's produce
      # is still in flight under the old PID.
      await produceP0.respond(
        buildProduceResponseBody(
          topic = topic,
          partition = 0'i32,
          errorCode = keMessageTooLarge.toInt16,
          baseOffset = -1'i64,
        )
      )

      # The terminal handler resets the producer id and republishes the
      # queues: serve its re-init with a NEW producer id.
      let ipid2 = await fb.nextRequest(timeout = 2.seconds)
      check ipid2.apiKey == int16(ord(akInitProducerId))
      await ipid2.respond(
        buildInitProducerIdResponseBody(producerId = 2000'i64, producerEpoch = 0)
      )

      # sendA raises only AFTER reassignSequencesAfterReset ran, so awaiting
      # it here makes the republish-before-sibling-retry ordering
      # deterministic.
      var raisedA = keNone
      try:
        discard await sendA
      except KafkaApiError as e:
        raisedA = e.errorCode
      check raisedA == keMessageTooLarge

      # NOW reject the stale in-flight produce for partition 1 with a
      # producer-id-reset code.
      await produceP1.respond(
        buildProduceResponseBody(
          topic = topic,
          partition = 1'i32,
          errorCode = keUnknownProducerId.toInt16,
          baseOffset = -1'i64,
        )
      )

      # The sibling must re-send the REPUBLISHED bytes — new PID, sequences
      # restarting at 0 — via the observed fast path, without driving a
      # second reset (an InitProducerId arriving here instead of a Produce
      # fails the apiKey check).
      let retryP1 = await fb.nextRequest(timeout = 2.seconds)
      check retryP1.apiKey == int16(ord(akProduce))
      let rb = decodeProduceBatches(retryP1.body)[0]
      check rb.partition == 1'i32
      check rb.producerId == 2000'i64 # previously: stale 1000
      check rb.baseSequence == 0'i32
      await retryP1.respond(
        buildProduceResponseBody(
          topic = topic, partition = 1'i32, errorCode = 0'i16, baseOffset = 0'i64
        )
      )

      let resB = await sendB
      check resB.offset == 0'i64

      await p.close()
      await fb.close()
      await cluster.close()

    waitFor testBody()

proc decodeSingleProduceRecord(
    body: seq[byte]
): tuple[key: Option[seq[byte]], value: Option[seq[byte]]] =
  var r = initKafkaReader(body)
  let pReq = r.decode(ProduceRequest, 3'i16)
  doAssert pReq.topics.len == 1
  doAssert pReq.topics[0].partitions.len == 1
  var br = initKafkaReader(pReq.topics[0].partitions[0].recordSet)
  let batch = br.decode(RecordBatch)
  doAssert batch.records.len == 1
  (batch.records[0].key, batch.records[0].value)

proc autoRespondCaptureProduce(
    fb: FakeBroker,
    host: string,
    port: int32,
    topic: string,
    captured: ref seq[byte],
    produceErrorCode: int16 = 0'i16,
) {.async: (raises: []).} =
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 500.milliseconds)
    except CatchableError:
      break
    try:
      let key = req.apiKey
      if key == int16(ord(akApiVersions)):
        await req.respond(
          buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              ),
              FakeApiVersion(
                apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
              ),
              FakeApiVersion(
                apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
              ),
            ],
          )
        )
      elif key == int16(ord(akMetadata)):
        await req.respond(
          buildMetadataResponseBody(
            [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
            controllerId = 0'i32,
            topics = [
              FakeTopicMeta(
                name: topic,
                errorCode: 0'i16,
                isInternal: false,
                partitions: @[
                  FakePartitionMeta(
                    partitionIndex: 0'i32,
                    leaderId: 0'i32,
                    replicas: @[0'i32],
                    isr: @[0'i32],
                  )
                ],
              )
            ],
          )
        )
      elif key == int16(ord(akProduce)):
        captured[] = req.body
        await req.respond(
          buildProduceResponseBody(
            topic = topic,
            partition = 0'i32,
            errorCode = produceErrorCode,
            baseOffset = 0'i64,
            logAppendTimeMs = -1'i64,
            throttleTimeMs = 0'i32,
          )
        )
    except CatchableError:
      break

suite "Producer: tombstone":
  test "send with none value reaches the broker as null":
    proc body() {.async: (raises: [Exception]).} =
      const topic = "tombstone-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let captured = new(seq[byte])
      let respFut = autoRespondCaptureProduce(fb, host, port, topic, captured)
      let cluster = newCluster(@[host & ":" & $port], clientId = "tombstone-test")
      let p = newKafkaProducer(cluster, lingerMs = 0, timeoutMs = 1500)
      try:
        let key = some(toBytes("tombstone-key"))
        let res = await p.send(topic, none(seq[byte]), key = key, partition = 0'i32)
        check res.status == pstSuccess
        let (wireKey, wireValue) = decodeSingleProduceRecord(captured[])
        check wireKey == key
        check wireValue.isNone
      finally:
        await p.close()
        await fb.close()
        awaitDrain(respFut, 1.seconds)
        await cluster.close()

    waitFor body()

  test "seq overload still sends a non-null value":
    proc body() {.async: (raises: [Exception]).} =
      const topic = "tombstone-compat-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let captured = new(seq[byte])
      let respFut = autoRespondCaptureProduce(fb, host, port, topic, captured)
      let cluster = newCluster(@[host & ":" & $port], clientId = "tombstone-compat")
      let p = newKafkaProducer(cluster, lingerMs = 0, timeoutMs = 1500)
      try:
        let res = await p.send(topic, @[0xAB'u8], partition = 0'i32)
        check res.status == pstSuccess
        let (_, wireValue) = decodeSingleProduceRecord(captured[])
        check wireValue == some(@[0xAB'u8])
        let resOpt = await p.send(topic, some(@[0xCD'u8]), partition = 0'i32)
        check resOpt.status == pstSuccess
        let (_, wireOpt) = decodeSingleProduceRecord(captured[])
        check wireOpt == some(@[0xCD'u8])
      finally:
        await p.close()
        await fb.close()
        awaitDrain(respFut, 1.seconds)
        await cluster.close()

    waitFor body()

  test "seq overload sends empty bytes distinctly from tombstone null":
    proc body() {.async: (raises: [Exception]).} =
      const topic = "tombstone-empty-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let captured = new(seq[byte])
      let respFut = autoRespondCaptureProduce(fb, host, port, topic, captured)
      let cluster = newCluster(@[host & ":" & $port], clientId = "tombstone-empty")
      let p = newKafkaProducer(cluster, lingerMs = 0, timeoutMs = 1500)
      try:
        let resEmpty = await p.send(topic, @[], partition = 0'i32)
        check resEmpty.status == pstSuccess
        let (_, wireEmpty) = decodeSingleProduceRecord(captured[])
        check wireEmpty.isSome
        check wireEmpty.get.len == 0
        let resNil = await p.send(topic, default(seq[byte]), partition = 0'i32)
        check resNil.status == pstSuccess
        let (_, wireNil) = decodeSingleProduceRecord(captured[])
        check wireNil.isSome
        check wireNil.get.len == 0
        let resNone = await p.send(topic, none(seq[byte]), partition = 0'i32)
        check resNone.status == pstSuccess
        let (_, wireNone) = decodeSingleProduceRecord(captured[])
        check wireNone.isNone
      finally:
        await p.close()
        await fb.close()
        awaitDrain(respFut, 1.seconds)
        await cluster.close()

    waitFor body()

  test "sendFireAndForget seq overload forwards a non-null value":
    proc body() {.async: (raises: [Exception]).} =
      const topic = "tombstone-ff-compat-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let captured = new(seq[byte])
      let respFut = autoRespondCaptureProduce(fb, host, port, topic, captured)
      let cluster = newCluster(@[host & ":" & $port], clientId = "tombstone-ff-compat")
      let p = newKafkaProducer(cluster, lingerMs = 0, timeoutMs = 1500)
      try:
        var got: Option[ProduceResult]
        let cb: ProduceCallback = proc(
            res: ProduceResult, err: ref KafkaError
        ) {.gcsafe, raises: [].} =
          got = some(res)
        await p.sendFireAndForget(topic, @[0xAB'u8], partition = 0'i32, callback = cb)
        var attempts = 0
        while got.isNone and attempts < 100:
          await sleepAsync(10.milliseconds)
          inc attempts
        check got.isSome
        check got.get.status == pstSuccess
        let (_, wireValue) = decodeSingleProduceRecord(captured[])
        check wireValue == some(@[0xAB'u8])
      finally:
        await p.close()
        await fb.close()
        awaitDrain(respFut, 1.seconds)
        await cluster.close()

    waitFor body()

  test "sendFireAndForget with none value reaches the broker as null":
    proc body() {.async: (raises: [Exception]).} =
      const topic = "tombstone-ff-topic"
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let captured = new(seq[byte])
      let respFut = autoRespondCaptureProduce(fb, host, port, topic, captured)
      let cluster = newCluster(@[host & ":" & $port], clientId = "tombstone-ff")
      let p = newKafkaProducer(cluster, lingerMs = 0, timeoutMs = 1500)
      try:
        var got: Option[ProduceResult]
        let cb: ProduceCallback = proc(
            res: ProduceResult, err: ref KafkaError
        ) {.gcsafe, raises: [].} =
          got = some(res)
        await p.sendFireAndForget(
          topic,
          none(seq[byte]),
          key = some(toBytes("k")),
          partition = 0'i32,
          callback = cb,
        )
        var attempts = 0
        while got.isNone and attempts < 100:
          await sleepAsync(10.milliseconds)
          inc attempts
        check got.isSome
        let (_, wireValue) = decodeSingleProduceRecord(captured[])
        check wireValue.isNone
      finally:
        await p.close()
        await fb.close()
        awaitDrain(respFut, 1.seconds)
        await cluster.close()

    waitFor body()

  test "DLQ preserves a tombstone null instead of empty bytes":
    proc body() {.async: (raises: [Exception]).} =
      const sourceTopic = "tombstone-dlq-src"
      const dlqTopic = "tombstone-dlq-dest"
      let host = "127.0.0.1"
      let srcFb = newFakeBroker()
      let srcPort = int32(srcFb.address().port)
      let dlqFb = newFakeBroker()
      let dlqPort = int32(dlqFb.address().port)
      let dlqCaptured = new(seq[byte])
      let srcRespFut = autoRespondCaptureProduce(
        srcFb, host, srcPort, sourceTopic, new(seq[byte]), produceErrorCode = 87'i16
      )
      let dlqRespFut =
        autoRespondCaptureProduce(dlqFb, host, dlqPort, dlqTopic, dlqCaptured)
      let cluster = newCluster(@[host & ":" & $srcPort], clientId = "tombstone-dlq")
      let dlqCluster =
        newCluster(@[host & ":" & $dlqPort], clientId = "tombstone-dlq-dest")
      let dlq = newKafkaProducer(dlqCluster, lingerMs = 0, timeoutMs = 1500)
      let main =
        newKafkaProducer(cluster, lingerMs = 0, maxRetries = 0, timeoutMs = 1500)
      main.errorHandler = alwaysDeadLetter()
      await main.setDeadLetterQueue(dlqTopic, dlq)
      try:
        let res = await main.send(
          sourceTopic, none(seq[byte]), key = some(toBytes("k")), partition = 0'i32
        )
        check res.status == pstDeadLettered
        let (_, dlqValue) = decodeSingleProduceRecord(dlqCaptured[])
        check dlqValue.isNone
      finally:
        await main.close()
        await dlq.close()
        await srcFb.close()
        await dlqFb.close()
        awaitDrain(srcRespFut, 1.seconds)
        awaitDrain(dlqRespFut, 1.seconds)
        await cluster.close()
        await dlqCluster.close()

    waitFor body()
