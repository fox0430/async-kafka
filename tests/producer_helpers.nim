## Producer test-only internal accessors for the producer test suites.
## These deliberately import concrete producer implementation modules instead
## of the public facade: the facade is an allowlist, while these helpers need
## selected state-machine and batching internals for regression coverage.

import std/[tables, options, sets, importutils]

import async_kafka/async_backend

import async_kafka/[errors, broker, cluster]
import async_kafka/protocol/messages

import async_kafka/producer/types
import async_kafka/producer/idempotence
import async_kafka/producer/transaction
import async_kafka/producer/batch
import async_kafka/producer/lifecycle

# The facade exports the lifecycle entry points but not the shared validator,
# so the test suites reach it through this helper module.
export lifecycle.validateProducerConfig

privateAccess(KafkaProducer)

when defined(kafkaCloseTestHooks):
  # The close-teardown parks live in the shipped modules, compile-time gated; the
  # suites reach them through this helper like the other internals.
  proc armCloseLingerParkForTest*(
      p: KafkaProducer
  ): Future[void].Raising([CancelledError]) =
    ## Test helper. Arm a pending future the linger-reaping stage of close() waits
    ## on, so a test can cancel a close that is inside that stage.
    p.closeLingerPark =
      Future[void].Raising([CancelledError]).init("producer.close.linger.park")
    p.closeLingerPark

  proc closeLingerParkedForTest*(p: KafkaProducer): bool =
    ## Test helper. True while close() waits on the future armed above.
    p.closeLingerParked

proc acquireBatchLock*(
    p: KafkaProducer
): Future[void] {.async: (raises: [CancelledError]).} =
  ## Test helper. Acquire the internal batch lock.
  await p.batchLock.acquire()

proc releaseBatchLock*(p: KafkaProducer) {.raises: [].} =
  ## Test helper. Release the internal batch lock.
  p.batchLock.releaseSafely("tests.helpers.acquireBatchLock")

proc markClosed*(p: KafkaProducer) =
  ## Test helper. Mark the producer as closed without performing cleanup.
  p.state = lsClosed

proc clusterForTest*(p: KafkaProducer): Cluster =
  ## Test helper. The Cluster this producer borrowed or owns, so a test can assert
  ## close() released an owned one.
  p.cluster

proc setTxnState*(p: KafkaProducer, state: TransactionState) =
  ## Test helper. Override transaction state for testing close behavior.
  p.txnState = state

proc tryTransitionForTest*(
    p: KafkaProducer, dst: TransactionState
): bool {.discardable.} =
  ## Test helper. Wraps the `tryTransition` state-machine gate for unit tests.
  p.tryTransition(dst)

proc getSequenceNumber*(p: KafkaProducer, tp: TopicPartition): int32 =
  ## Test helper. Get the current sequence number for a partition.
  p.sequenceNumbers.getOrDefault(tp, 0'i32)

proc setSequenceNumber*(p: KafkaProducer, tp: TopicPartition, seq: int32) =
  ## Test helper. Set the sequence number for a partition.
  p.sequenceNumbers[tp] = seq

proc sequenceNumberCount*(p: KafkaProducer): int =
  ## Test helper. Get the number of partitions with sequence numbers.
  p.sequenceNumbers.len

proc getStickyPartition*(p: KafkaProducer, topic: string): Option[int32] =
  ## Test helper. Get the sticky partition for a topic, if any.
  if topic in p.stickyPartitions:
    some(p.stickyPartitions[topic])
  else:
    none(int32)

proc setStickyPartition*(p: KafkaProducer, topic: string, partition: int32) =
  ## Test helper. Set the sticky partition for a topic.
  p.stickyPartitions[topic] = partition

proc isTxnLocked*(p: KafkaProducer): bool =
  ## Test helper. Check if the transaction lock is held.
  p.txnLock.locked

proc acquireTxnLock*(
    p: KafkaProducer
): Future[void] {.async: (raises: [CancelledError]).} =
  ## Test helper. Acquire the transaction lock.
  await p.txnLock.acquire()

proc releaseTxnLock*(p: KafkaProducer) {.raises: [].} =
  ## Test helper. Release the transaction lock.
  p.txnLock.releaseSafely("tests.helpers.acquireTxnLock")

proc pendingBatchBytesFor*(p: KafkaProducer, tp: TopicPartition): int64 =
  ## Test helper. Running byte total the byte-cap flush trigger consults for
  ## `tp`; 0 when the partition has no buffered batch.
  p.pendingBatchBytes.getOrDefault(tp, 0'i64)

proc tryAcquireInflightSemaphore*(
    p: KafkaProducer, tp: TopicPartition
): Future[bool] {.async: (raises: []).} =
  ## Test helper. Tries to acquire the in-flight semaphore for tp within 50ms.
  ## Returns true if acquired (slot was available), false on timeout.
  let sem = p.inflightSemaphores.mgetOrPut(tp, newAsyncSemaphore(p.maxInFlightRequests))
  try:
    await sem.acquire().wait(milliseconds(50))
    return true
  except AsyncTimeoutError:
    return false
  except CancelledError:
    return false

proc releaseInflightSemaphore*(p: KafkaProducer, tp: TopicPartition) =
  ## Test helper. Release the in-flight semaphore for tp.
  p.inflightSemaphores.withValue(tp, sem):
    try:
      sem[].release()
    except AsyncSemaphoreError:
      discard

proc getProducerId*(p: KafkaProducer): int64 =
  ## Test helper. Get the current producer ID.
  p.producerId

proc getProducerEpoch*(p: KafkaProducer): int16 =
  ## Test helper. Get the current producer epoch.
  p.producerEpoch

proc setProducerId*(p: KafkaProducer, id: int64, epoch: int16) =
  ## Test helper. Set the producer ID and epoch. Also marks
  ## `idempotenceInitialized` true so `allocateInflight`'s PID-reset fence sees
  ## the primed identity as ready instead of driving a broker round-trip. Tests
  ## that want to simulate a mid-reset state should call
  ## `setIdempotenceInitialized(false)` after this helper.
  p.producerId = id
  p.producerEpoch = epoch
  p.idempotenceInitialized = true

proc setIdempotenceInitialized*(p: KafkaProducer, value: bool) =
  ## Test helper. Simulate a mid-reset state (false) so a subsequent
  ## `allocateInflight` must drive `ensureIdempotenceInitialized` before
  ## encoding — the PID-reset fencing regression.
  p.idempotenceInitialized = value

proc getIdempotenceInitialized*(p: KafkaProducer): bool =
  ## Test helper. Read `idempotenceInitialized`.
  p.idempotenceInitialized

proc txnPartitionCount*(p: KafkaProducer): int =
  ## Test helper. Number of partitions enrolled in the current transaction.
  p.txnPartitions.len

proc txnPendingPartitionCount*(p: KafkaProducer): int =
  ## Test helper. Number of partitions pending registration.
  p.txnPendingPartitions.len

proc addTxnPartition*(p: KafkaProducer, tp: TopicPartition) =
  ## Test helper. Add a partition to the transaction set. A registered partition
  ## means AddPartitionsToTxn succeeded broker-side, so mark the transaction
  ## started — commit/abort must then send a real EndTxn.
  p.txnPartitions.incl(tp)
  p.txnStarted = true

proc getTxnStarted*(p: KafkaProducer): bool =
  ## Test helper. Whether the current transaction has been started broker-side.
  ## commit/abort skip EndTxn when this is false.
  p.txnStarted

proc setTxnStarted*(p: KafkaProducer, value: bool) =
  ## Test helper. Force the broker-side "transaction started" marker to drive the
  ## empty-transaction short-circuit (false) or a normal EndTxn (true) without the
  ## full AddPartitionsToTxn handshake.
  p.txnStarted = value

proc getPendingEpochBump*(p: KafkaProducer): bool =
  ## Test helper. Read the deferred-bump flag (set when a bump fails, retriable
  ## or not; cleared when a later bump completes).
  p.pendingEpochBump

proc setPendingEpochBump*(p: KafkaProducer, value: bool) =
  ## Test helper. Force the deferred-bump flag to simulate the "EndTxn ok,
  ## bumpEpoch deferred" state without driving the full InitProducerId failure.
  p.pendingEpochBump = value

proc holdEpochBumpGate*(p: KafkaProducer) =
  ## Test helper. Mark a deferred bump as in flight, so ensureEpochBumped callers
  ## park on its gate until `releaseEpochBumpGate`.
  p.pendingEpochBump = true
  p.epochBumpInflight = true
  p.epochBumpDone = newFuture[void]("tests.helpers.epochBump")

proc releaseEpochBumpGate*(p: KafkaProducer) =
  ## Test helper. Finish a bump held by `holdEpochBumpGate`, as a successful one does.
  let done = p.epochBumpDone
  p.pendingEpochBump = false
  p.epochBumpInflight = false
  p.epochBumpDone = nil
  if done != nil and not done.finished:
    done.complete()

proc inflightSemaphoreCount*(p: KafkaProducer): int =
  ## Test helper. Number of entries in the inflightSemaphores table. Verifies
  ## close() clears the per-partition tracking instead of letting it grow
  ## unbounded across short-lived producers.
  p.inflightSemaphores.len

proc setTransactionalIdForTest*(p: KafkaProducer, id: string) =
  ## Test helper. Pretend the producer was constructed with a
  ## transactionalId so transactional code paths are reachable from
  ## tests that bypass the public constructor.
  p.transactionalId = some(id)

proc addTxnPendingPartition*(p: KafkaProducer, tp: TopicPartition) =
  ## Test helper. Add a partition to the pending registration set.
  p.txnPendingPartitions.incl(tp)

proc containsTxnPendingPartition*(p: KafkaProducer, tp: TopicPartition): bool =
  ## Test helper. Whether the partition sits in the pending-AddPartitionsToTxn
  ## set. Verifies a failed register rolls the tentative entry back instead of
  ## leaking it to the next call.
  tp in p.txnPendingPartitions

proc setTxnCoordinator*(p: KafkaProducer, coord: Broker) =
  ## Test helper. Inject a transaction coordinator without running InitProducerId,
  ## so tests can exercise close/abort guard logic without a real broker.
  p.txnCoordinator = coord

proc hasBatch*(p: KafkaProducer, tp: TopicPartition): bool =
  ## Test helper. Whether a pending batch entry exists for the partition.
  p.batches.hasKey(tp)

proc hasLingerTimer*(p: KafkaProducer, tp: TopicPartition): bool =
  ## Test helper. Whether a linger timer entry currently exists for the partition.
  p.lingerTimers.hasKey(tp)

proc getLingerTimer*(p: KafkaProducer, tp: TopicPartition): Future[void] =
  ## Test helper. Returns the linger timer future, or nil if none.
  if p.lingerTimers.hasKey(tp):
    p.lingerTimers[tp]
  else:
    nil

proc injectLingerTimer*(p: KafkaProducer, tp: TopicPartition, fut: Future[void]) =
  ## Test helper. Inject an external future into the linger timer table so
  ## tests can verify that the defensive cleanup in send() cancels a stale
  ## entry.
  p.lingerTimers[tp] = fut

proc renewLingerTimerForTest*(p: KafkaProducer, tp: TopicPartition) =
  ## Test helper. Runs the same defensive cleanup + re-register that send()'s
  ## first-message-in-batch path performs, without a live broker.
  p.renewLingerTimer(tp)

proc lingerTimerEpochForTest*(p: KafkaProducer, tp: TopicPartition): uint64 =
  ## Test helper. Reports the current linger timer epoch for `tp`, or 0 if none.
  p.lingerTimerEpoch.getOrDefault(tp, 0'u64)

proc dropLingerTimerEntryForTest*(p: KafkaProducer, tp: TopicPartition) =
  ## Test helper. Simulate an external flushBatch that dropped the linger timer
  ## entry (cancelSoon + del) while its Future is still resolving in the
  ## background, so a stale lingerTimeout resume can be observed under test.
  ## Leaves `lingerTimerEpoch` untouched — production flushBatch does the same
  ## so the stale continuation observes a strictly higher successor epoch.
  p.lingerTimers.del(tp)

proc maxPendingRecordsForTest*(p: KafkaProducer): int32 =
  ## Test helper. Expose the configured pending-records bound.
  p.maxPendingRecords

proc tryAcquirePendingRecord*(
    p: KafkaProducer, timeoutMs: int = 50
): Future[bool] {.async: (raises: []).} =
  ## Test helper. Try to acquire one pending-records permit within `timeoutMs`;
  ## true if a slot was free, false on timeout — verifies backpressure has
  ## saturated the semaphore.
  try:
    await p.pendingRecordsSem.acquire().wait(milliseconds(timeoutMs))
    return true
  except AsyncTimeoutError:
    return false
  except CancelledError:
    return false

proc releasePendingRecordForTest*(p: KafkaProducer) {.raises: [].} =
  ## Test helper. Release one permit back to the pending-records semaphore.
  p.releasePendingRecordPermit()

proc failBatchFuturesForTest*(
    p: KafkaProducer, tp: TopicPartition, err: ref CatchableError
) =
  ## Test helper. Drive the private failBatchFutures path directly so tests
  ## can verify its permit-release behavior without a live broker.
  p.failBatchFutures(tp, err)

proc pendingFireAndForgetLen*(p: KafkaProducer): int =
  ## Test helper. Number of fire-and-forget futures still tracked. Verifies
  ## close-time guards prevent futures from being added during shutdown.
  p.pendingFireAndForget.len

proc fireAndForgetSemAvailable*(p: KafkaProducer): int =
  ## Test helper. Free permits on the fire-and-forget semaphore — verifies the
  ## close-race guards release the permit they took before raising KafkaClosedError.
  p.fireAndForgetSem.availableSlots

proc injectPendingFireAndForget*(p: KafkaProducer, fut: Future[void]) =
  ## Test helper. Drop a future into the pending-fire-and-forget list so close()'s
  ## drain loop sees it without a live broker. The test owns `fut`.
  p.pendingFireAndForget.add(fut)

proc inflightBatchCountForTest*(p: KafkaProducer, tp: TopicPartition): int =
  ## Test helper. Number of in-flight batches queued for `tp` (KIP-185 in-flight
  ## tracking); 0 when the partition has no queue entry.
  if tp in p.inflightBatches:
    p.inflightBatches[tp].len
  else:
    0

proc getInflightBaseSequenceForTest*(
    p: KafkaProducer, tp: TopicPartition, index: int
): int32 =
  ## Test helper. Read the `baseSequence` field of the in-flight batch
  ## at position `index` in `tp`'s queue. Index is bounded by
  ## `inflightBatchCountForTest`.
  p.inflightBatches[tp][index].baseSequence

proc getInflightProducerEpochForTest*(
    p: KafkaProducer, tp: TopicPartition, index: int
): int16 =
  ## Test helper. Read `producerEpochAtEncode` of the in-flight batch at `index`.
  ## Verifies reassignSequencesAfterReset rebuilds each batch under the new
  ## (producerId, epoch) tuple.
  p.inflightBatches[tp][index].producerEpochAtEncode

proc allocateInflightForTest*(
    p: KafkaProducer, records: seq[Record], tp: TopicPartition, firstTimestamp: int64
): Future[InflightBatch] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Test helper. Public wrapper around the private `allocateInflight`
  ## so unit tests can drive the in-flight tracking without going
  ## through the full send pipeline.
  await p.allocateInflight(records, tp, firstTimestamp)

proc dequeueInflightForTest*(p: KafkaProducer, tp: TopicPartition, id: int64) =
  ## Test helper. Drives `dequeueInflight` so tests can simulate a
  ## successful Produce response (id-based, matching the production
  ## call sites) without driving a real broker.
  p.dequeueInflight(tp, id)

proc getInflightIdForTest*(p: KafkaProducer, tp: TopicPartition, index: int): int64 =
  ## Test helper. Read the monotonic `id` of the in-flight batch at
  ## position `index` in `tp`'s queue. Lets tests dequeue a specific
  ## (possibly non-front) batch to exercise out-of-order completion.
  p.inflightBatches[tp][index].id

proc reassignSequencesAfterResetForTest*(
    p: KafkaProducer
) {.async: (raises: [CancelledError]).} =
  ## Test helper. Public wrapper around the private
  ## `reassignSequencesAfterReset` so unit tests can verify the
  ## re-sequencing invariant after a simulated producer-id bump.
  await p.reassignSequencesAfterReset()

proc clearInflightForTest*(p: KafkaProducer) =
  ## Test helper. Drop all per-TP in-flight queues, as `close()` does when it
  ## tears the producer down.
  p.inflightBatches.clear()

proc inflightFlushCountForTest*(p: KafkaProducer): int =
  ## Test helper. Number of live flushBatch markers: registered by every producer
  ## before the batch leaves `batches`, drained by flush/commit/abort.
  p.inflightFlushFutures.len

proc hasWriteChainTailForTest*(p: KafkaProducer, tp: TopicPartition): bool =
  ## Test helper. Whether a write-order chain tail entry exists for `tp`.
  p.writeChainTail.hasKey(tp)

proc hasCompletionChainTailForTest*(p: KafkaProducer, tp: TopicPartition): bool =
  ## Test helper. Whether a completion chain tail entry exists for `tp`.
  p.completionChainTail.hasKey(tp)

proc setChainTailsForTest*(p: KafkaProducer, tp: TopicPartition, fut: Future[void]) =
  ## Test helper. Seed both per-partition chain tails for `tp` with `fut` so the
  ## idle-eviction test can verify a FINISHED tail is reclaimed while an
  ## UNFINISHED one is retained.
  p.writeChainTail[tp] = fut
  p.completionChainTail[tp] = fut

proc simulateInflightFlushForTest*(p: KafkaProducer, gate: Future[void]): Future[void] =
  ## Test helper. Mimic flushBatch's in-flight window without a broker: register a
  ## marker in `inflightFlushFutures`, await `gate` (standing in for the broker
  ## round-trip), then complete + remove the marker in a defer as flushBatch does.
  ## Lets a test drive `awaitInflightFlushesForTest` against a controllable flush.
  proc run() {.async: (raises: []).} =
    let marker = newFuture[void]("test-inflight-flush")
    p.inflightFlushFutures.add(marker)
    defer:
      if not marker.finished:
        marker.complete()
      let idx = p.inflightFlushFutures.find(marker)
      if idx >= 0:
        p.inflightFlushFutures.del(idx)
    try:
      await gate
    except CatchableError:
      # `gate` is a plain Future the test only completes; a failure/cancel just
      # ends the round-trip early — the defer still completes + removes the marker.
      discard

  run()
