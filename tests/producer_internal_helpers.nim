## Producer test-only accessors that need producer-PRIVATE procs
## (`awaitInflightFlushes`, `maybeEvictIdlePartitionState`), reached through
## `import batch {.all.}`.

import std/[options, importutils]

import chronos

import async_kafka/errors
import async_kafka/producer/types
import async_kafka/protocol/messages

import async_kafka/producer/batch {.all.}

type
  CompletionGateForTest* = CompletionGate
    ## Re-export of the batch-private CompletionGate so tests can drive
    ## `gatedAllocateAndWrite` with a real gate and assert the install-once
    ## invariant across a failed first attempt.
  FirstAttemptOutcomeForTest* = FirstAttemptOutcome
    ## Re-export of the batch-private FirstAttemptOutcome so tests can assert
    ## that a write failure after allocation still surfaces the enqueued batch
    ## to the caller (orphaned-batch regression).

proc produceResultOfForTest*(
    tp: TopicPartition, respOpt: Option[ProduceResponse]
): ProduceResult {.raises: [KafkaError].} =
  ## Test helper. Drives the private `produceResultOf` so a regression test can
  ## assert it verifies the response partition matches the request partition.
  produceResultOf(tp, respOpt)

proc awaitInflightFlushesForTest*(
    p: KafkaProducer
): Future[void] {.async: (raises: [CancelledError]).} =
  ## Test helper. Drives the private `awaitInflightFlushes` so a test can
  ## assert the transactional finalize barrier waits for in-flight flushBatch
  ## markers and returns once they are completed + removed.
  await p.awaitInflightFlushes()

proc evictIdlePartitionStateForTest*(p: KafkaProducer, tp: TopicPartition) =
  ## Test helper. Drives the private `maybeEvictIdlePartitionState` to assert
  ## idle-partition reclamation frees a quiescent partition's bookkeeping, honours
  ## the busy-partition guards, and never evicts the mandated `sequenceNumbers`
  ## entry.
  p.maybeEvictIdlePartitionState(tp)

proc refreshInflightFromQueueForTest*(
    p: KafkaProducer, tp: TopicPartition, inflight: var InflightBatch
) =
  ## Test helper. Drives the private `refreshInflightFromQueue` to assert a parked
  ## retry re-syncs its local `InflightBatch` (baseSequence / epoch / encodedBytes)
  ## from the per-TP queue after a concurrent producer-id reset re-sequenced it —
  ## the same refresh the retry loop performs each iteration.
  p.refreshInflightFromQueue(tp, inflight)

proc newCompletionGateForTest*(): CompletionGateForTest =
  CompletionGate()

proc gateMineForTest*(g: CompletionGateForTest): Future[void] =
  ## The gate's own completion-chain marker (nil until first installed).
  privateAccess(CompletionGate)
  g.mine

proc newFirstAttemptOutcomeForTest*(): FirstAttemptOutcomeForTest =
  FirstAttemptOutcome()

proc allocatedBatchForTest*(o: FirstAttemptOutcomeForTest): Option[InflightBatch] =
  ## The batch a failed first attempt allocated, if any.
  privateAccess(FirstAttemptOutcome)
  o.allocatedBatch

proc gatedAllocateAndWriteForTest*(
    p: KafkaProducer,
    tp: TopicPartition,
    records: seq[Record],
    firstTimestamp: int64,
    gate: CompletionGateForTest = nil,
    outcome: FirstAttemptOutcomeForTest = nil,
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Test helper. Drives a FIRST-ATTEMPT `gatedAllocateAndWrite` (existing = none)
  ## so failure-path tests can exercise the allocation side channel and gate
  ## install-once guard without the full send pipeline. Failure scenarios only:
  ## on success the returned in-flight handle is discarded, leaking the slot.
  discard await p.gatedAllocateAndWrite(
    tp, records, firstTimestamp, none(InflightBatch), gate, outcome
  )

proc flushBatchForTest*(
    p: KafkaProducer, tp: TopicPartition
): Future[void] {.async: (raises: []).} =
  ## Test helper. Drive `flushBatch` directly (bypass send / flush / close) so a
  ## test can cancel it at a chosen await point and observe the resulting
  ## behavior on pending futures.
  await p.flushBatch(tp)

proc injectPendingBatch*(
    p: KafkaProducer, tp: TopicPartition, rec: Record
): Future[ProduceResult] =
  ## Test helper. Inject a pending message and return its future. Drives the
  ## same `appendPendingToBatchLocked` accounting (batch membership +
  ## `pendingBatchBytes`) that send()'s batched path uses, so byte-cap and
  ## close tests assert against the real bookkeeping rather than a test-side
  ## copy of it.
  result = Future[ProduceResult].init("kafka.test.injectPendingBatch")
  let pm = PendingMessage(
    record: rec, future: result, tp: tp, estimatedBytes: estimateRecordSize(rec)
  )
  p.appendPendingToBatchLocked(tp, pm)
