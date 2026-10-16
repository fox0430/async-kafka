## Test-only accessors for `Fetcher` internals (the private fetch-session table
## and the top-level error classifier). Private procs/types via the `{.all.}`
## import; private fields via `std/importutils`' `privateAccess`.

import std/[importutils, options, sets, tables]

import chronos

import async_kafka/[errors, broker]
import async_kafka/protocol/messages
import async_kafka/consumer/fetcher {.all.}

proc registerPartitionForTest*(
    f: Fetcher,
    tp: TopicPartition,
    offset: int64,
    broker: Broker = nil,
    paused: bool = false,
) =
  ## Test-only: append a partition with a pre-resolved broker (or nil) and
  ## pre-set offsets, bypassing Cluster lookup. Covers the common
  ## `PartitionState` construction; a hand-rolled assignment builds one inline.
  let idx = f.partitions.len
  f.partitions.add(
    PartitionState(
      tp: tp,
      fetchOffset: offset,
      consumedOffset: offset,
      consumedLeaderEpoch: -1'i32,
      broker: broker,
      paused: paused,
      lastFetchedEpoch: -1'i32,
      awaitingValidation: false,
    )
  )
  f.partitionIndex[tp] = idx

proc applyOffsetResetForTest*(f: Fetcher, tp: TopicPartition, newOffset: int64) =
  ## Exposes the OFFSET_OUT_OF_RANGE recovery `fetchFromBroker` applies so a test
  ## can verify both the fetch cursor and the committable position move to the
  ## reset offset without a fake broker / ListOffsets round-trip.
  f.applyOffsetReset(tp, newOffset)

proc posEpochForTest*(f: Fetcher, tp: TopicPartition): int64 =
  ## Test-only: read a partition's position epoch so regression tests can
  ## assert seek / offset reset bump it (the in-flight-fetch staleness guard).
  f.partitionIndex.withValue(tp, idx):
    return f.partitions[idx[]].posEpoch
  -1

proc lastLeaderEpochForTest*(f: Fetcher, tp: TopicPartition): int32 =
  ## Test-only: read the monotone leaderEpoch ceiling for `tp`. Returns -1
  ## if the partition has never sent a fetch carrying a real epoch.
  privateAccess(Fetcher)
  f.lastLeaderEpoch.getOrDefault(tp, -1'i32)

proc setLastLeaderEpochForTest*(f: Fetcher, tp: TopicPartition, epoch: int32) =
  ## Test-only: prime the leaderEpoch ceiling so a unit test can verify the
  ## clamp + cleanup paths without driving a real broker round-trip.
  privateAccess(Fetcher)
  f.lastLeaderEpoch[tp] = epoch

proc enqueueRecordForTest*(f: Fetcher, record: FetchedRecord) {.raises: [].} =
  ## Test-only: push a record onto the queue tagged via the production
  ## `stampEnqueuedRecord` (the same generation / posEpoch stamping
  ## enqueuePartition applies), so generation- and reposition-filter tests
  ## exercise the real stamping instead of a test-side copy.
  var tagged = record
  let tp = TopicPartition(topic: record.topic, partition: record.partition)
  f.stampEnqueuedRecord(tagged, tp)
  try:
    f.records.addLastNoWait(tagged)
  except AsyncQueueFullError:
    # Tests should size the queue large enough; fail fast instead of dropping.
    raise (ref Defect)(msg: "enqueueRecordForTest: queue full")

proc enqueueRecordRawForTest*(f: Fetcher, record: FetchedRecord) {.raises: [].} =
  ## Test-only: push a record verbatim (no generation / posEpoch stamping) so
  ## a test can inject a stale-posEpoch record to exercise the delivery-side
  ## filter.
  try:
    f.records.addLastNoWait(record)
  except AsyncQueueFullError:
    raise (ref Defect)(msg: "enqueueRecordRawForTest: queue full")

proc fetchSessionCountForTest*(f: Fetcher): int =
  privateAccess(Fetcher)
  f.fetchSessions.len

proc getFetchSessionForTest*(
    f: Fetcher, brokerId: int32
): tuple[sessionId, sessionEpoch: int32, exists: bool] =
  privateAccess(Fetcher)
  privateAccess(FetchSession)
  f.fetchSessions.withValue(brokerId, s):
    return (s[].sessionId, s[].sessionEpoch, true)
  (0'i32, 0'i32, false)

proc setFetchSessionForTest*(
    f: Fetcher, brokerId: int32, sessionId: int32, sessionEpoch: int32
) =
  privateAccess(Fetcher)
  privateAccess(FetchSession)
  f.fetchSessions[brokerId] =
    FetchSession(sessionId: sessionId, sessionEpoch: sessionEpoch)

proc handleTopLevelFetchErrorForTest*(
    f: Fetcher, code: KafkaErrorCode, brokerId: int32
): bool {.raises: [KafkaError].} =
  f.handleTopLevelFetchError(code, brokerId)

proc fetchOnceForTest*(
    f: Fetcher
): Future[void] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Test-only: drive one round of the private `fetchOnce` so a test can
  ## exercise the full fetchFromBroker response processing (record extraction,
  ## position filtering, fetchOffset advancement) against a fake broker.
  await f.fetchOnce()

proc enqueuePartitionForTest*(
    f: Fetcher,
    tp: TopicPartition,
    filteredBatches: seq[RecordBatch],
    batches: seq[RecordBatch],
): Future[bool] {.async: (raises: [CancelledError]).} =
  ## Test-only: drive the private per-partition enqueue + fetchOffset advance
  ## (no broker), so a test can park the enqueue on a full-queue `put`, interleave
  ## a concurrent seek, and assert the mid-enqueue posEpoch re-check protects it.
  await f.enqueuePartition(tp, filteredBatches, batches)

proc lastFetchedEpochForTest*(f: Fetcher, tp: TopicPartition): int32 =
  ## Test-only: read the KIP-320 epoch a partition's position is known-valid
  ## under. -1 if unknown or the partition is not assigned.
  f.partitionIndex.withValue(tp, idx):
    return f.partitions[idx[]].lastFetchedEpoch
  -1'i32

proc consumedLeaderEpochForTest*(f: Fetcher, tp: TopicPartition): int32 =
  ## Test-only: read the epoch attached to the committable position (sent as
  ## OffsetCommit's committedLeaderEpoch). -1 means commit falls back to
  ## metadata — expected after seek / offset reset / truncation rewind.
  f.partitionIndex.withValue(tp, idx):
    return f.partitions[idx[]].consumedLeaderEpoch
  -1'i32

proc setLastFetchedEpochForTest*(f: Fetcher, tp: TopicPartition, epoch: int32) =
  ## Test-only: prime the position's known-valid leader epoch so a validation
  ## test can drive markPartitionsForValidation / validateOffsets without a real
  ## consume round.
  f.partitionIndex.withValue(tp, idx):
    f.partitions[idx[]].lastFetchedEpoch = epoch

proc awaitingValidationForTest*(f: Fetcher, tp: TopicPartition): bool =
  ## Test-only: whether a partition is currently flagged for OffsetForLeaderEpoch
  ## validation.
  f.partitionIndex.withValue(tp, idx):
    return f.partitions[idx[]].awaitingValidation
  false

proc setAwaitingValidationForTest*(f: Fetcher, tp: TopicPartition, on: bool) =
  ## Test-only: flag/unflag a partition for validation directly.
  f.partitionIndex.withValue(tp, idx):
    f.partitions[idx[]].awaitingValidation = on

proc validationAttemptsForTest*(f: Fetcher, tp: TopicPartition): int =
  ## Test-only: read a partition's consecutive-failed-validation-round counter
  ## (the escalation budget recordValidationFailure consumes).
  f.partitionIndex.withValue(tp, idx):
    return f.partitions[idx[]].validationAttempts
  0

proc recordValidationFailureForTest*(f: Fetcher, tp: TopicPartition) =
  ## Test-only: expose the private failure accounting so unit tests can drive
  ## the escalation budget without a broker round-trip.
  f.recordValidationFailure(tp)

proc markPartitionsForValidationForTest*(f: Fetcher) =
  ## Test-only: run the private leader-change scan that flags partitions whose
  ## cached leaderEpoch advanced past their known-valid epoch.
  f.markPartitionsForValidation()

proc validateOffsetsForTest*(
    f: Fetcher
): Future[void] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Test-only: drive one round of the private OffsetForLeaderEpoch validation
  ## against a fake broker.
  await f.validateOffsets()

proc markBrokerEpochValidationUnsupportedForTest*(f: Fetcher, brokerId: int32) =
  ## Test-only: prime the broker-level OffsetForLeaderEpoch unsupported set so a
  ## unit test can verify markPartitionsForValidation / validateOffsets skip the
  ## broker without driving a real keUnsupportedVersion round-trip.
  privateAccess(Fetcher)
  f.epochValidationUnsupportedBrokers.incl(brokerId)

proc brokerEpochValidationUnsupportedForTest*(f: Fetcher, brokerId: int32): bool =
  ## Test-only: whether `brokerId` is currently in the OffsetForLeaderEpoch
  ## unsupported set (populated when a broker returns keUnsupportedVersion).
  privateAccess(Fetcher)
  brokerId in f.epochValidationUnsupportedBrokers

proc isRunningForTest*(f: Fetcher): bool =
  ## Test-only: whether the fetch loop is armed. False on a stopped or
  ## never-started fetcher; used to assert the restart normalization keeps a
  ## running loop from being restarted.
  privateAccess(Fetcher)
  f.running

proc setRunningForTest*(f: Fetcher, running: bool) =
  ## Test-only: force the running flag so a test can pin the alive/dead-loop
  ## split tryRestartFetcher uses to decide between real restart and error
  ## surface (see the divergence guard in consumer.tryRestartFetcher).
  privateAccess(Fetcher)
  f.running = running

proc injectFetchLoopFutForTest*(f: Fetcher, fut: Future[void]) =
  ## Test-only: pin a synthetic fetchLoopFut so a test can drive the
  ## "loop alive vs. dead" branch in tryRestartFetcher without a live
  ## fetch loop / broker.
  privateAccess(Fetcher)
  f.fetchLoopFut = fut

proc hasExitStopFutForTest*(f: Fetcher): bool =
  ## Test-only: whether spawnStop() has a stop() future that joinExitStop()
  ## still needs to reclaim. False after joinExitStop() or when spawnStop
  ## was never called.
  privateAccess(Fetcher)
  f.exitStopFut != nil

proc applyValidationResultForTest*(
    f: Fetcher,
    tp: TopicPartition,
    validatedEpoch: int32,
    errorCode: KafkaErrorCode,
    leaderEpoch: int32,
    endOffset: int64,
): Future[void] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Test-only: feed a synthetic OffsetForLeaderEpoch partition result into the
  ## private response handler, so truncation / valid-position / orpNone branches
  ## can be exercised without a broker round-trip.
  await f.applyValidationResult(
    tp,
    validatedEpoch,
    OffsetForLeaderPartitionResp(
      errorCode: errorCode,
      partition: tp.partition,
      leaderEpoch: leaderEpoch,
      endOffset: endOffset,
    ),
  )

proc partitionBrokerForTest*(f: Fetcher, tp: TopicPartition): Broker =
  ## Test-only: the broker reference a partition currently fetches from. The
  ## broker-failure recovery path reassigns this, so asserting it is how a test
  ## observes that recovery actually happened (the metadata refresh it drives is
  ## otherwise invisible from outside the fetcher).
  f.partitionIndex.withValue(tp, idx):
    return f.partitions[idx[]].broker
  nil

proc brokerFetchErrorsForTest*(f: Fetcher): Table[int32, int] =
  ## Test-only: the per-broker consecutive fetch-failure counters that arm the
  ## recovery path. Asserts a broker's counter accumulates, clears on success,
  ## and is dropped once its partitions are reassigned.
  privateAccess(Fetcher)
  f.brokerFetchErrors

proc retriableFetchAttemptsForTest*(f: Fetcher): int =
  ## Test-only: consecutive fetch rounds whose response carried a retriable
  ## per-partition error, which scales the fetch retry backoff.
  privateAccess(Fetcher)
  f.retriableFetchAttempts

proc resolveOffsetsForTest*(
    f: Fetcher, partitions: seq[TopicPartition]
): Future[Table[TopicPartition, int64]] {.
    async: (raises: [KafkaError, CancelledError])
.} =
  ## Test-only: drive the private ListOffsets resolution directly, so a test can
  ## assert the retry of a retriable per-partition answer without the rest of the
  ## setPartitions flow.
  await f.resolveOffsets(partitions)

proc reassignBrokerPartitionsForTest*(
    f: Fetcher, partIndices: seq[int]
): Future[void] {.async: (raises: [CancelledError]).} =
  ## Test-only: drive the private per-broker recovery directly. Used to assert
  ## the guard that keeps a stopped fetcher from starting a metadata refresh on
  ## its way down (stop() clears `running` before cancelling in-flight fetches,
  ## and those fail as KafkaConnectionError rather than CancelledError).
  await f.reassignBrokerPartitions(partIndices)
