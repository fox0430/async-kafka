## Test-only accessors for `KafkaConsumer` internals. Private procs are reached
## via the `{.all.}` import; private fields via `std/importutils`' `privateAccess`.

import std/[importutils, options, tables]

import chronos

import async_kafka/[errors, broker, cluster]
import async_kafka/protocol/messages
import async_kafka/consumer/[group, fetcher]
import async_kafka/consumer/consumer {.all.}

when defined(kafkaCloseTestHooks):
  # The close-teardown park lives in the shipped module, compile-time gated; the
  # suites reach it through this helper like the other internals.
  proc armCloseFetcherParkForTest*(
      c: KafkaConsumer
  ): Future[void].Raising([CancelledError]) =
    ## Test helper. Arm a pending future that close()'s fetcher-stop stage waits on
    ## before stopping the fetcher, so a test can cancel the close inside that stage.
    privateAccess(KafkaConsumer)
    c.closeFetcherPark =
      Future[void].Raising([CancelledError]).init("consumer.close.fetcher.park")
    c.closeFetcherPark

  proc closeFetcherParkedForTest*(c: KafkaConsumer): bool =
    ## Test helper. True while close() waits on the future armed above.
    privateAccess(KafkaConsumer)
    c.closeFetcherParked

proc markRunning*(c: KafkaConsumer) =
  ## Test helper. Force state to lsRunning without going through start()
  ## so tests can exercise poll paths (e.g. deadline handling during
  ## rebalance) without a real broker.
  privateAccess(KafkaConsumer)
  c.state = lsRunning

proc markClosed*(c: KafkaConsumer) =
  ## Test helper. Force state to lsClosed without driving a real close()
  ## so tests can exercise the closing/closed rejection paths.
  privateAccess(KafkaConsumer)
  c.state = lsClosed

proc requestRebalance*(c: KafkaConsumer) =
  ## Test helper. Set the rebalance-requested flag on the consumer group.
  privateAccess(KafkaConsumer)
  c.group.rebalanceRequested = true

proc rebalanceRequestedForTest*(c: KafkaConsumer): bool =
  ## Test helper. Read the rebalance-requested flag. Asserts the "commitOffsets
  ## is pure" change leaves the flag where expected.
  privateAccess(KafkaConsumer)
  c.group.rebalanceRequested

proc rejoinInFlightForTest*(c: KafkaConsumer): bool =
  ## Test helper. True when a JoinGroup+SyncGroup is parked in flight after a
  ## deadline-bounded poll ran out of budget mid-rejoin. Used to assert the
  ## rejoin is suspended (not cancelled) across the poll deadline.
  privateAccess(KafkaConsumer)
  c.rejoinFut != nil

proc injectRejoinFutForTest*(
    c: KafkaConsumer,
    fut: Future[seq[TopicPartition]].Raising([KafkaError, CancelledError]),
) =
  ## Test helper. Pin a synthetic in-flight rejoin future so a regression test
  ## can drive handleRebalanceIfNeeded down a chosen join/sync outcome (e.g. a
  ## permanent KafkaApiError) without scripting a live coordinator handshake.
  privateAccess(KafkaConsumer)
  c.rejoinFut = fut

proc setAssignedForTest*(c: KafkaConsumer, partitions: seq[TopicPartition]) =
  ## Test helper. Pre-populate the consumer group's assigned partitions so
  ## tests can verify that revoked / lost listeners receive the snapshot
  ## without needing a successful join.
  privateAccess(KafkaConsumer)
  c.group.assignedPartitions = partitions

proc fetcherLastError*(c: KafkaConsumer): ref KafkaError =
  ## Test helper. Exposes the fetcher's lastError so regression tests can
  ## verify that rebalance failure arms the next poll's auto-restart.
  privateAccess(KafkaConsumer)
  c.fetcher.lastError

proc fetcherForTest*(c: KafkaConsumer): Fetcher =
  ## Test helper. Returns the fetcher reference so regression tests can
  ## poke at its public mutators (setLastError, consecutiveRestartAttempts,
  ## lastRestartAttempt) without having to expose them on the consumer.
  privateAccess(KafkaConsumer)
  c.fetcher

proc clusterForTest*(c: KafkaConsumer): Cluster =
  ## Test helper. The Cluster this consumer borrowed or owns, so a test can assert
  ## close() released an owned one.
  privateAccess(KafkaConsumer)
  c.cluster

proc configForTest*(c: KafkaConsumer): ConsumerConfig =
  ## Test helper. Returns the normalized ConsumerConfig stored on the consumer.
  privateAccess(KafkaConsumer)
  c.config

proc groupHeartbeatTimeoutForTest*(c: KafkaConsumer): int32 =
  ## Test helper. The effective per-heartbeat-request timeout (ms) installed on
  ## the consumer group, after the div-3 auto-default has been applied.
  privateAccess(KafkaConsumer)
  c.group.heartbeatTimeoutMs()

proc groupMaxRediscoverAttemptsForTest*(c: KafkaConsumer): int =
  ## Test helper. The coordinator rediscovery attempt budget installed on the
  ## consumer group.
  privateAccess(KafkaConsumer)
  c.group.maxCoordinatorRediscoverAttempts

proc injectCommitLoopFut*(c: KafkaConsumer, fut: Future[void]) =
  ## Test helper. Lets a regression test pin a synthetic commitLoopFut
  ## to verify close() drains it cooperatively instead of cancelling
  ## mid-commit (KIP-320 Schrödinger fix in close()).
  privateAccess(KafkaConsumer)
  c.commitLoopFut = fut

proc appendPendingCommitForTest*(c: KafkaConsumer, fut: Future[void]) =
  ## Test helper. Mirrors the commitAsync queue without going through
  ## the public API (which requires a live coordinator broker).
  privateAccess(KafkaConsumer)
  c.pendingCommits.add(fut)

proc pendingCommitsForTest*(c: KafkaConsumer): seq[Future[void]] =
  ## Test helper. The fire-and-forget commit futures spawned by commitAsync,
  ## so a regression test can await their completion deterministically.
  privateAccess(KafkaConsumer)
  c.pendingCommits

proc acquireCommitLockForTest*(c: KafkaConsumer) {.async: (raises: [CancelledError]).} =
  ## Test helper. Hold the commit serialization lock so a spawned commitAsync
  ## parks at `commitLock.acquire()` before its OffsetCommit RPC — letting a
  ## test mutate the live position and prove commitAsync sent the call-time
  ## snapshot, not the later value.
  privateAccess(KafkaConsumer)
  await c.commitLock.acquire()

proc releaseCommitLockForTest*(c: KafkaConsumer) {.raises: [].} =
  ## Test helper. Release the lock taken by `acquireCommitLockForTest`.
  privateAccess(KafkaConsumer)
  try:
    c.commitLock.release()
  except AsyncLockError:
    discard

proc commitLockHeldForTest*(c: KafkaConsumer): bool =
  ## Test helper. True while a commit coroutine holds the serialization lock.
  ## A timed-out final commit that close() failed to reap would keep it held
  ## past close() — the orphan check for the close reap regression test.
  privateAccess(KafkaConsumer)
  c.commitLock.locked()

proc setGroupGenerationForTest*(c: KafkaConsumer, generationId: int32) =
  ## Test helper. Sets the consumer group's generationId so behavioral
  ## tests of the poll-side stale-generation filter can exercise the
  ## drop path without driving a real JoinGroup.
  privateAccess(KafkaConsumer)
  c.group.generationId = generationId

proc setPartialMembershipForTest*(
    c: KafkaConsumer, coordinator: Broker, memberId: string
) =
  ## Test helper. Simulate a start() that joined the group (coordinator + memberId
  ## installed) but failed before reaching lsRunning, so close()'s zombie-member
  ## cleanup can be exercised without a full FindCoordinator/JoinGroup handshake.
  privateAccess(KafkaConsumer)
  c.group.coordinator = coordinator
  c.group.memberId = memberId

proc setGroupStateForTest*(c: KafkaConsumer, state: GroupState) =
  ## Test helper. Forces the consumer group's state so the commit gate (which
  ## must commit on any valid generation, not only gsStable) can be exercised
  ## without driving a real rebalance.
  privateAccess(KafkaConsumer)
  c.group.state = state

proc setGroupCoordinatorForTest*(c: KafkaConsumer, coord: Broker) =
  ## Test helper. Injects a coordinator broker so commit paths attempt a real
  ## OffsetCommit RPC instead of short-circuiting on a nil coordinator.
  privateAccess(KafkaConsumer)
  c.group.coordinator = coord

proc setPollIntervalExceededForTest*(c: KafkaConsumer, v: bool) =
  ## Test helper. Simulates the heartbeat loop having self-left on a
  ## max.poll.interval.ms timeout so the poll-surfacing path can be exercised
  ## without driving a live heartbeat + coordinator.
  privateAccess(KafkaConsumer)
  c.group.pollIntervalExceeded = v

proc pollIntervalExceededForTest*(c: KafkaConsumer): bool =
  ## Test helper. Reads the poll-interval-exceeded flag so a test can assert
  ## poll() cleared it after surfacing the error once.
  privateAccess(KafkaConsumer)
  c.group.pollIntervalExceeded

proc pendingAdvanceLenForTest*(c: KafkaConsumer): int =
  ## Test helper. Number of records whose position has not yet been
  ## advanced. Used by pollBatch position-update regression tests.
  privateAccess(KafkaConsumer)
  c.pendingAdvance.len

proc injectPendingAdvanceForTest*(c: KafkaConsumer, rec: FetchedRecord) =
  ## Test helper. Queue a record as if a prior poll had returned it, so a
  ## test can verify how a later seek / flush treats the deferred position
  ## update without driving a full fetch round.
  privateAccess(KafkaConsumer)
  c.pendingAdvance.add(rec)

proc tryRestartFetcherForTest*(c: KafkaConsumer) {.raises: [KafkaError].} =
  ## Test helper. Drives tryRestartFetcher directly so regression tests
  ## can assert the escalation behaviour without needing a real poll
  ## loop / broker.
  c.tryRestartFetcher()

proc fetchInitialOffsetsForTest*(
    c: KafkaConsumer, partitions: seq[TopicPartition]
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Test helper. Drives the private `fetchInitialOffsets` so regression
  ## tests can assert how top-level / per-partition OffsetFetch errors are
  ## surfaced without scripting a full join + rebalance.
  await c.fetchInitialOffsets(partitions)

proc handleRebalanceForTest*(
    c: KafkaConsumer, deadline = none(Moment)
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Test helper. Drives the private `handleRebalanceIfNeeded` to assert the
  ## join-vs-offset-fetch retry split: a transient OffsetFetch failure must retry
  ## only the offset fetch, never rejoin.
  await c.handleRebalanceIfNeeded(deadline)

proc commitOffsetsForTest*(
    c: KafkaConsumer
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Test helper. Drives the bare `commitOffsets` the auto-commit loop uses, to
  ## assert it does NOT flush pendingAdvance (the position advance is the
  ## explicit-commit / poll caller's job).
  await c.commitOffsets()

proc commitSnapshotForTest*(
    c: KafkaConsumer,
    snapshotOffsets: Table[TopicPartition, int64],
    snapshotGeneration: int32,
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Test helper. Drives `commitOffsets` with an explicit caller-time snapshot
  ## (the commitAsync path) to assert the snapshot-generation guard skips a commit
  ## whose generation no longer matches the live one — committing it would rewind
  ## the new owner.
  await c.commitOffsets(some(snapshotOffsets), snapshotGeneration)
