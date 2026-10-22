import std/[tables, options, importutils]

import ../[async_backend, errors, cluster, broker, connection, sasl, group_metadata]
import ../protocol/messages
import ../internal/[backoff, callback_guard, cancel, metrics, retry, safe_logging]
import assignment, group, fetcher

privateAccess(FetchedRecord)

export fetcher.OffsetResetPolicy
export assignment.AssignmentStrategy
export messages.IsolationLevel
export metrics.MetricsHook
export
  group_metadata.ConsumerGroupMetadata, group_metadata.initConsumerGroupMetadata,
  group_metadata.initUnfencedConsumerGroupMetadata

type
  RebalanceCallback* =
    proc(partitions: seq[TopicPartition]) {.gcsafe, raises: [CatchableError].}
    ## Fired when the consumer's partition assignment changes.
    ## May raise: bookkeeping always completes first, then the driving
    ## poll()/start() re-raises wrapped in KafkaRebalanceCallbackError (Java
    ## parity); if the rebalance itself failed, its error wins and this one is only
    ## logged. Runs inline and synchronously on the consumer's event loop, so keep it
    ## short, non-blocking, and free of calls back into the client. `gcsafe` because
    ## the consumer runs in a chronos async context the runtime treats as a thread
    ## root.

  ConsumerConfig* = object
    ## Build with `initConsumerConfig` and tune fields before `newKafkaConsumer`;
    ## validation happens there, not at construction.
    bootstrapBrokers*: seq[string]
      ## Brokers for the Cluster that `newKafkaConsumer(config)` builds; the overload
      ## taking a `Cluster` dials that cluster's list instead.
    groupId*: string
      ## The consumer group this member joins; assignments and offset commits are
      ## scoped to it.
    topics*: seq[string]
      ## Topics the group subscribes to; the assignment covers their partitions.
    clientId*: string
      ## Reported to the broker by the owned Cluster; an injected Cluster keeps its
      ## own id.
    sessionTimeoutMs*: int32
      ## Group session timeout (ms): a member that goes this long without a successful
      ## heartbeat is evicted and its partitions reassigned. Must be > 0. 0 = default
      ## 30_000.
    heartbeatIntervalMs*: int32
      ## Heartbeat cadence (ms). Must be > 0 and < `sessionTimeoutMs`. 0 = default 3_000.
    heartbeatTimeoutMs*: int32
      ## Per-heartbeat-request timeout (ms); a slower Heartbeat response is treated
      ## as an unreachable coordinator. 0 selects max(sessionTimeoutMs div 3, 1);
      ## positive values must be <= sessionTimeoutMs.
    maxCoordinatorRediscoverAttempts*: int
      ## Max FindCoordinator retries after a moved/still-loading coordinator, shared
      ## by the heartbeat loop and OffsetCommit escalation. 0 = default 3.
    rebalanceTimeoutMs*: int32
      ## Budget (ms) for one rebalance, bounding both the JoinGroup body and the
      ## SyncGroup wait. Must be >= `sessionTimeoutMs`, since the coordinator can expire
      ## the member while it rejoins. 0 = default 60_000.
    maxPollIntervalMs*: int32
      ## max.poll.interval.ms (Java parity). When > 0, the heartbeat loop leaves
      ## the group if no poll happens within the window so the coordinator reassigns
      ## its partitions; the next poll raises once, the one after rejoins. 0 disables;
      ## negative is rejected.
    metadataMaxAgeMs*: int32
      ## metadata.max.age.ms (Java parity): cadence at which the heartbeat loop
      ## refreshes metadata and arms a rebalance when a topic gained partitions.
      ## 0 disables; negative is rejected.
    maxWaitMs*: int32
      ## How long (ms) the broker may hold a fetch open waiting for `minFetchBytes`
      ## (fetch.max.wait.ms). 0 = default 500.
    minFetchBytes*: int32
      ## Bytes a fetch response should carry before the broker answers
      ## (fetch.min.bytes). 0 = default 1.
    maxFetchBytes*: int32
      ## Bytes per fetch response, across all its partitions (fetch.max.bytes).
      ## 0 = default 52_428_800 (50 MiB).
    maxPartitionFetchBytes*: int32
      ## Bytes per partition in one fetch response (max.partition.fetch.bytes).
      ## 0 = default 1_048_576 (1 MiB).
    maxTotalDecompressedBytes*: int
      ## Decompressed-byte budget for one fetch response, across all its partitions
      ## (0 = default 1GiB). Bounds decompression amplification from a malicious
      ## broker; exceeding it stops the fetcher with KafkaDecompressionLimitError.
    assignmentStrategy*: AssignmentStrategy
      ## How the group spreads partitions across its members. Defaults to `asRange`.
    autoCommit*: bool
      ## Commit consumed offsets periodically in the background, every
      ## `autoCommitIntervalMs`; `commitSync` and `commitAsync` stay available. Defaults
      ## to true. This knob covers the periodic commit only: a rebalance and `close`
      ## commit the consumed positions regardless (see `close`), so an application whose
      ## offsets belong to a transaction must commit them there rather than rely on this
      ## being false.
    autoCommitIntervalMs*: int32 ## Auto-commit cadence (ms). 0 = default 5_000.
    offsetResetPolicy*: OffsetResetPolicy
      # from fetcher module
      ## Where a partition starts when it has no committed offset, or its committed
      ## offset is out of range. Defaults to `orpLatest`; `orpNone` raises instead.
    isolationLevel*: IsolationLevel
      ## Whether fetches see transactional records before their transaction commits.
      ## Defaults to `ilReadUncommitted`.
    maxFetcherRestartAttempts*: int
      ## Max consecutive tryRestartFetcher() attempts without a successful fetch
      ## before poll() raises: prevents an infinite restart loop on a permanent error
      ## (e.g. AUTHORIZATION_FAILED). 0 disables the cap.
    maxValidationAttempts*: int
      ## Consecutive failed OffsetForLeaderEpoch rounds after which a partition's
      ## KIP-320 validation is abandoned so fetching resumes (an unreachable leader
      ## must not stall it silently). 0 = default 10; negative is rejected. Re-armed
      ## on the next leader epoch change.
    recordQueueSize*: int
      ## Capacity of the fetcher's internal AsyncQueue[FetchedRecord]. Backpressure:
      ## fetchOnce stops once full, so a slow consumer cannot accumulate unbounded
      ## records. 0 = default 1000; negative is rejected.
    tlsConfig*: TlsConfig
      ## TLS for the owned Cluster. The overload taking a `Cluster` raises on a non-nil
      ## value instead of ignoring it; set TLS where that cluster is built.
    hostAllowlist*: HostAllowlist
      ## Bounds which advertised broker addresses the owned Cluster will dial
      ## (plaintext included; see `HostAllowlist`); `nil` disables. As `tlsConfig`, the
      ## overload taking a `Cluster` raises on a non-nil value.
    saslMechanism*: SaslMechanism
      ## SASL for the owned Cluster; as `tlsConfig`, rejected by the overload taking a
      ## `Cluster`.
    metricsHook*: MetricsHook
      ## Optional callback for this consumer's events (`consumer.fetch.error`, ...);
      ## `nil` makes each emit a no-op.
    onPartitionsAssigned*: RebalanceCallback
      ## Runs with the new assignment once it is in effect and before that poll returns
      ## records.
    onPartitionsRevoked*: RebalanceCallback
      ## Runs with the old assignment while the membership is still valid, after the
      ## pre-rebalance offset commit. Exactly one of this and `onPartitionsLost` fires
      ## per rebalance.
    onPartitionsLost*: RebalanceCallback
      ## Runs with the old assignment when the membership was already invalidated (the
      ## heartbeat was fenced), so no commit is attempted and the partitions are lost
      ## rather than handed over.

  KafkaConsumer* = ref object
    ## Single-threaded like Java's consumer: `poll`, `tryPoll`, `pollBatch`,
    ## `pollBatchStrict`, `seek`, `pause`/`resume`, and `commitSync` mutate shared
    ## state unlocked, from one coroutine only.
    ##
    ## The heartbeat, fetch, and auto-commit loops share the caller's event loop, so
    ## they advance only while the caller awaits. Work between polls that awaits is
    ## bounded by `maxPollIntervalMs`; synchronous work that never yields (CPU-bound
    ## processing, a blocking call) stops heartbeats too, and once it outlasts
    ## `sessionTimeoutMs` the coordinator evicts the member and reassigns its
    ## partitions, whose new owner resumes from the last committed offset and so
    ## re-delivers the uncommitted records. Keep each such stretch below
    ## `sessionTimeoutMs` minus `heartbeatIntervalMs` (the last heartbeat may already be
    ## that old when it starts), or move the work to another thread and await its
    ## completion, e.g. through chronos's `ThreadSignalPtr`.
    ##
    ## The auto-commit loop and `commitAsync` tasks are the only self-managed
    ## concurrency; `commitLock` serializes them through `commitOffsets` so two
    ## commits cannot race on the coordinator handle, and they never touch poll-owned
    ## state.
    ##
    ## Group membership is dynamic and rebalancing is eager, as in Java's consumer
    ## before KIP-345 / KIP-429: static membership (`group.instance.id`) is not
    ## implemented, so `groupInstanceId` stays unset on JoinGroup / LeaveGroup /
    ## OffsetCommit and a restarting member rejoins as a new member instead of
    ## resuming its assignment; and every rebalance revokes all currently assigned
    ## partitions, rather than keeping the unchanged subset.
    config: ConsumerConfig
    cluster: Cluster
    group: ConsumerGroup
    fetcher: Fetcher
    state: LifecycleState
    closeSignal: AsyncEvent
    clusterOwned: bool ## True when this consumer built its Cluster and must close it.
    closeFuture: Future[void]
      ## The in-progress close() coroutine, so racing close() calls resolve to one
      ## teardown instead of a second drain, LeaveGroup, and final commit.
    when defined(kafkaCloseTestHooks):
      closeFetcherPark: Future[void].Raising([CancelledError])
        ## Test-only. When armed, close()'s fetcher-stop stage waits on it before
        ## stopping the fetcher, so a test can cancel the close inside that stage.
      closeFetcherParked: bool
        ## Test-only. True while close() waits on `closeFetcherPark`.
    commitLoopFut: Future[void]
    pendingCommits: seq[Future[void]]
    commitLock: AsyncLock
      ## Serializes every `commitOffsets` call, held across the broker round-trip and
      ## any coordinator rediscovery, so two commits cannot interleave and race on the
      ## coordinator handle `rediscoverCoordinator` repoints. Commits never touch
      ## poll-owned state, so they cannot interleave with poll. Never held across
      ## close()'s drain phases; the final commit bounds its wait, so the chronos
      ## AsyncLock hand-off hazard (a cancelled waiter can leave the lock held by a
      ## coroutine that unwinds) cannot wedge close().
    pendingAdvance: seq[FetchedRecord]
      ## Records from the most recent poll* call whose `consumedOffset` has not yet
      ## advanced. Java moves the position on the NEXT poll, not while collecting, so
      ## a cancelled/timed-out poll cannot commit records the caller never observed.
      ## Flushed on entry to every poll/commit path.
    rejoinFut: Future[seq[TopicPartition]].Raising([KafkaError, CancelledError])
      ## A JoinGroup+SyncGroup started by handleRebalanceIfNeeded but not finished. A
      ## deadline-bounded poll out of budget mid-rejoin leaves it running here —
      ## cancelling SyncGroup mid-flight would advance the generation — to resume on
      ## the next poll.
    rejoinPrevAssigned: seq[TopicPartition]
      ## Assignment snapshotted when the in-flight `rejoinFut` started, so the
      ## revoked/lost listeners report what was actually assigned across a multi-poll
      ## rejoin.
    rejoinListenerError: ref KafkaRebalanceCallbackError
      ## First listener exception of this rebalance cycle, parked so a deadline-parked
      ## rejoin keeps it; re-raised on completion, or warn-superseded if the rejoin
      ## fails.
    rejoinPrepareDone: bool
      ## Java's `needsJoinPrepare`: teardown listener + pre-rebalance commit already
      ## fired for this episode, cleared on a successful rejoin — without it, a
      ## retry-exhaustion raise would re-fire the revoked/lost callback for the same
      ## assignment.
    commitCallbackFailures: CallbackFailureLog
      ## Reports a `commitAsync` callback that raises despite its `raises: []` type.

  OffsetFetchAction* = enum
    ofaUseOffset ## errorCode == keNone and offset >= 0 — committed offset present
    ofaNoCommitted ## errorCode == keNone and offset == -1 — fall back to reset policy
    ofaInvalidateCoordinator
      ## coordinator moved (NOT_COORDINATOR / GROUP_COORDINATOR_NOT_AVAILABLE) —
      ## drop cached coordinator then raise
    ofaRebalance ## ecRebalanceRequired — arm rebalanceRequested for the next poll
    ofaRetriable ## other retriable codes — raise so handleRebalanceIfNeeded retries
    ofaFatal ## fatal / abortable / unknown — raise immediately

  CommitCallback* = proc(err: ref KafkaError) {.gcsafe, raises: [].}
    ## Async-commit completion: `nil` on success, the failure otherwise. Runs on
    ## the commit path, so it must be cheap and never raise (a `CatchableError`
    ## raised anyway is logged along with the commit error; a `Defect` is not caught).

type ConsumerConfigDefaults = object
  sessionTimeoutMs: int32
  heartbeatIntervalMs: int32
  rebalanceTimeoutMs: int32
  maxPollIntervalMs: int32
  metadataMaxAgeMs: int32
  maxWaitMs: int32
  minFetchBytes: int32
  maxFetchBytes: int32
  maxPartitionFetchBytes: int32
  maxTotalDecompressedBytes: int
  assignmentStrategy: AssignmentStrategy
  autoCommit: bool
  autoCommitIntervalMs: int32
  offsetResetPolicy: OffsetResetPolicy
  isolationLevel: IsolationLevel
  maxFetcherRestartAttempts: int
  recordQueueSize: int
  maxValidationAttempts: int
  maxCoordinatorRediscoverAttempts: int

const consumerConfigDefaults = ConsumerConfigDefaults(
  sessionTimeoutMs: 30_000,
  heartbeatIntervalMs: 3_000,
  rebalanceTimeoutMs: 60_000,
  maxPollIntervalMs: 300_000,
  metadataMaxAgeMs: 300_000,
  maxWaitMs: 500,
  minFetchBytes: 1,
  maxFetchBytes: 52_428_800,
  maxPartitionFetchBytes: 1_048_576,
  maxTotalDecompressedBytes: defaultMaxTotalDecompressedBytes,
  assignmentStrategy: asRange,
  autoCommit: true,
  autoCommitIntervalMs: 5_000,
  offsetResetPolicy: orpLatest,
  isolationLevel: ilReadUncommitted,
  maxFetcherRestartAttempts: 10,
  recordQueueSize: 1_000,
  maxValidationAttempts: 10,
  maxCoordinatorRediscoverAttempts: 3,
)

proc initConsumerConfig*(
    bootstrapBrokers: seq[string],
    groupId: string,
    topics: seq[string],
    clientId: string = "async-kafka-consumer",
): ConsumerConfig =
  ## Tested defaults; only the broker list, group id, and topics are required. Tune
  ## the rest on the returned value before `newKafkaConsumer`.
  # Keep maxTotalDecompressedBytes at its zero sentinel; the shared table resolves it.
  ConsumerConfig(
    bootstrapBrokers: bootstrapBrokers,
    groupId: groupId,
    topics: topics,
    clientId: clientId,
    sessionTimeoutMs: consumerConfigDefaults.sessionTimeoutMs,
    heartbeatIntervalMs: consumerConfigDefaults.heartbeatIntervalMs,
    rebalanceTimeoutMs: consumerConfigDefaults.rebalanceTimeoutMs,
    maxPollIntervalMs: consumerConfigDefaults.maxPollIntervalMs,
    metadataMaxAgeMs: consumerConfigDefaults.metadataMaxAgeMs,
    maxWaitMs: consumerConfigDefaults.maxWaitMs,
    minFetchBytes: consumerConfigDefaults.minFetchBytes,
    maxFetchBytes: consumerConfigDefaults.maxFetchBytes,
    maxPartitionFetchBytes: consumerConfigDefaults.maxPartitionFetchBytes,
    assignmentStrategy: consumerConfigDefaults.assignmentStrategy,
    autoCommit: consumerConfigDefaults.autoCommit,
    autoCommitIntervalMs: consumerConfigDefaults.autoCommitIntervalMs,
    offsetResetPolicy: consumerConfigDefaults.offsetResetPolicy,
    isolationLevel: consumerConfigDefaults.isolationLevel,
    maxFetcherRestartAttempts: consumerConfigDefaults.maxFetcherRestartAttempts,
    recordQueueSize: consumerConfigDefaults.recordQueueSize,
    maxValidationAttempts: consumerConfigDefaults.maxValidationAttempts,
    maxCoordinatorRediscoverAttempts:
      consumerConfigDefaults.maxCoordinatorRediscoverAttempts,
  )

proc withDefaults(config: ConsumerConfig): ConsumerConfig {.raises: [].} =
  # A direct `ConsumerConfig(...)` leaves numeric fields at 0, which breaks
  # JoinGroup / heartbeat / fetch / auto-commit; fill them from the same table
  # initConsumerConfig uses. Fields documented as "0 disables" stay untouched.
  result = config
  if result.sessionTimeoutMs == 0:
    result.sessionTimeoutMs = consumerConfigDefaults.sessionTimeoutMs
  if result.heartbeatIntervalMs == 0:
    result.heartbeatIntervalMs = consumerConfigDefaults.heartbeatIntervalMs
  if result.rebalanceTimeoutMs == 0:
    result.rebalanceTimeoutMs = consumerConfigDefaults.rebalanceTimeoutMs
  if result.maxWaitMs == 0:
    result.maxWaitMs = consumerConfigDefaults.maxWaitMs
  if result.minFetchBytes == 0:
    result.minFetchBytes = consumerConfigDefaults.minFetchBytes
  if result.maxFetchBytes == 0:
    result.maxFetchBytes = consumerConfigDefaults.maxFetchBytes
  if result.maxPartitionFetchBytes == 0:
    result.maxPartitionFetchBytes = consumerConfigDefaults.maxPartitionFetchBytes
  if result.maxTotalDecompressedBytes == 0:
    result.maxTotalDecompressedBytes = consumerConfigDefaults.maxTotalDecompressedBytes
  if result.autoCommitIntervalMs == 0:
    result.autoCommitIntervalMs = consumerConfigDefaults.autoCommitIntervalMs
  if result.recordQueueSize == 0:
    result.recordQueueSize = consumerConfigDefaults.recordQueueSize
  if result.maxValidationAttempts == 0:
    result.maxValidationAttempts = consumerConfigDefaults.maxValidationAttempts
  if result.maxCoordinatorRediscoverAttempts == 0:
    result.maxCoordinatorRediscoverAttempts =
      consumerConfigDefaults.maxCoordinatorRediscoverAttempts

proc newKafkaConsumerOnCluster(
    config: ConsumerConfig, cluster: Cluster
): KafkaConsumer {.raises: [KafkaError].} =
  ## Shared body of both public overloads. The Cluster's transport settings are
  ## already fixed by the time it runs, so `config`'s transport fields are not read.
  var config = withDefaults(config)
  # Guarded here rather than in `newKafkaConsumer(config)`, which hands the raw hook
  # to the Cluster it builds: each client guards its own copy.
  config.metricsHook = guardMetricsHook(config.metricsHook)
  let group = newConsumerGroup(
    cluster,
    config.groupId,
    config.topics,
    config.sessionTimeoutMs,
    config.heartbeatIntervalMs,
    heartbeatTimeoutMs = config.heartbeatTimeoutMs,
    rebalanceTimeoutMs = config.rebalanceTimeoutMs,
    strategy = config.assignmentStrategy,
    maxCoordinatorRediscoverAttempts = config.maxCoordinatorRediscoverAttempts,
    maxPollIntervalMs = config.maxPollIntervalMs,
    metadataMaxAgeMs = config.metadataMaxAgeMs,
  )

  let fetch = newFetcher(
    cluster,
    config.maxWaitMs,
    config.minFetchBytes,
    config.maxFetchBytes,
    config.maxPartitionFetchBytes,
    queueSize = config.recordQueueSize,
    offsetResetPolicy = config.offsetResetPolicy,
    isolationLevel = config.isolationLevel,
    rebalanceCheck = proc(): bool {.raises: [].} =
      group.rebalanceRequested,
    maxTotalDecompressedBytes = config.maxTotalDecompressedBytes,
    metricsHook = config.metricsHook,
    maxValidationAttempts = config.maxValidationAttempts,
  )

  let c = KafkaConsumer(
    config: config,
    cluster: cluster,
    group: group,
    fetcher: fetch,
    state: lsUninitialized,
    closeSignal: newAsyncEvent(),
    clusterOwned: false,
    commitLock: newAsyncLock(),
  )

  # A coordinator-terminated session must stop fetching. The callback runs
  # synchronously inside heartbeatLoop, so the stop is dispatched, not awaited.
  group.onHeartbeatExit = proc() {.gcsafe, raises: [].} =
    # Synthetic error first, so the next poll() escalates instead of silently
    # observing a stopped fetcher with no diagnostic.
    let err = newException(
      KafkaError, "heartbeat exited due to permanent broker error; fetcher stopped"
    )
    fetch.setLastError(err)
    # Retain the dispatched stop() (not fire-and-forget) so close() can await it via
    # joinExitStop and no stop coroutine outlives the consumer. Idempotent w.r.t.
    # close()'s own fetcher.stop().
    fetch.spawnStop()
  c

proc newKafkaConsumer*(
    config: ConsumerConfig, cluster: Cluster
): KafkaConsumer {.raises: [KafkaError].} =
  ## Share connections with an existing Cluster. Configuration is validated eagerly,
  ## raising `KafkaError` on misuse. The caller must close the Cluster after the
  ## consumer is closed.
  ##
  ## The Cluster owns the transport settings, so `tlsConfig`, `saslMechanism`, and
  ## `hostAllowlist` are rejected here rather than ignored: a consumer that dials
  ## plaintext while its config asks for TLS reads as protected and is not.
  ## `bootstrapBrokers` and `clientId` come from the Cluster too.
  if config.tlsConfig != nil:
    raise newException(
      KafkaError,
      "config.tlsConfig is only read by newKafkaConsumer(config); set TLS on the Cluster",
    )
  if config.saslMechanism != nil:
    raise newException(
      KafkaError,
      "config.saslMechanism is only read by newKafkaConsumer(config); set SASL on the Cluster",
    )
  if config.hostAllowlist != nil:
    raise newException(
      KafkaError,
      "config.hostAllowlist is only read by newKafkaConsumer(config); set it on the Cluster",
    )
  newKafkaConsumerOnCluster(config, cluster)

template requireRunning(c: KafkaConsumer) =
  if c.state != lsRunning:
    raise newException(KafkaClosedError, "consumer is closed")

proc invokeListener(
    cb: RebalanceCallback, partitions: seq[TopicPartition]
): ref CatchableError {.gcsafe, raises: [].} =
  ## Runs a user rebalance callback, returning any exception instead of raising it so
  ## the caller finishes bookkeeping first and re-raises at a safe point (Java's
  ## ConsumerCoordinator contract).
  if cb == nil:
    return nil
  try:
    cb(partitions)
    nil
  except CatchableError as e:
    e

proc dropListenerError(c: KafkaConsumer, why: string) {.raises: [].} =
  ## Drop a parked listener exception because a more important error replaces it;
  ## warn so the user error is not silent.
  if c.rejoinListenerError != nil:
    warn "Rebalance listener error superseded by ", why, ": ", c.rejoinListenerError.msg
    c.rejoinListenerError = nil

proc isClosed*(c: KafkaConsumer): bool =
  ## True once close has started or finished.
  c.state.isClosingOrClosed

proc state*(c: KafkaConsumer): LifecycleState =
  ## Current lifecycle state: `lsUninitialized` until `start` succeeds, so a consumer
  ## that was just built reports that rather than `lsRunning`; then `lsRunning`,
  ## `lsClosing` while `close` runs, `lsClosed` after it.
  c.state

proc ownsCluster*(c: KafkaConsumer): bool {.raises: [].} =
  ## True when this consumer created its own Cluster (`newKafkaConsumer(config)`)
  ## and closes it in `close()`. When the Cluster was injected, the caller owns it
  ## and must close it after the consumer.
  c.clusterOwned

proc groupMetadata*(c: KafkaConsumer): ConsumerGroupMetadata {.raises: [KafkaError].} =
  ## Group identity snapshot (groupId, generationId, memberId) for the EOS path:
  ## `sendOffsetsToTransaction` passes it so the broker fences the offset commit
  ## against a stale generation (KIP-447). Before the first join there is nothing to
  ## fence, so the unfenced form is returned (`generationId = -1`, `memberId = ""`),
  ## else fenced.
  let snap = c.group.snapshotMembership()
  if snap.generationId < 0:
    initUnfencedConsumerGroupMetadata(c.config.groupId)
  else:
    initConsumerGroupMetadata(
      groupId = c.config.groupId,
      generationId = snap.generationId,
      memberId = snap.memberId,
    )

proc newKafkaConsumer*(config: ConsumerConfig): KafkaConsumer {.raises: [KafkaError].} =
  ## Create a consumer that owns its own Cluster: close() closes it, and
  ## `ownsCluster(c)` reports the ownership.
  let cluster = newCluster(
    config.bootstrapBrokers,
    config.clientId,
    tlsConfig = config.tlsConfig,
    hostAllowlist = config.hostAllowlist,
    saslMechanism = config.saslMechanism,
    metricsHook = config.metricsHook,
  )
  result = newKafkaConsumerOnCluster(config, cluster)
  result.clusterOwned = true

proc flushPendingAdvance*(c: KafkaConsumer) {.raises: [].} =
  ## Apply and clear the pending position updates of the previous poll batch, on entry
  ## to every poll path and inside `commitAsync` (whose snapshot must include them): a
  ## position advances only after the user could process the record AND a poll or
  ## commit is requested (Java semantics). `commitSync` flushes after the broker
  ## accepts, and `commitOffsets` never calls this, so auto-commit cannot advance a
  ## batch still being processed.
  if c.pendingAdvance.len == 0:
    return
  c.fetcher.advanceConsumedOffsets(c.pendingAdvance)
  c.pendingAdvance.setLen(0)

proc commitOffsetsLocked(
    c: KafkaConsumer,
    snapshotOffsets: Option[Table[TopicPartition, int64]] =
      none(Table[TopicPartition, int64]),
    snapshotGeneration: int32 = -1'i32,
    snapshotLeaderEpochs: Table[TopicPartition, int32] =
      initTable[TopicPartition, int32](),
) {.async: (raises: [KafkaError, CancelledError]).} =
  # Deliberately does not flush pendingAdvance (the caller's responsibility):
  # auto-commit must only commit offsets the application already moved past, or a
  # timer tick could commit a batch still being processed — an at-most-once loss
  # window beyond Java's.

  let offsets =
    if snapshotOffsets.isSome:
      snapshotOffsets.get
    else:
      c.fetcher.getConsumedOffsets()
  # Java parity: committing an empty map is a no-op (no RPC, no CommitSkipped).
  # Short-circuit BEFORE the generation gates so a caller with nothing to commit is
  # not spuriously rejected mid-rebalance — writing nothing cannot rewind anyone.
  if offsets.len == 0:
    return

  let snap = c.group.snapshotMembership()

  if c.group.coordinator == nil or snap.generationId < 0:
    # Gate on a valid generation, not gsStable: heartbeat flips the group to
    # gsRebalancing on REBALANCE_IN_PROGRESS while the OLD generationId stays valid
    # (the broker fences it with ILLEGAL_GENERATION if not).
    let msg =
      "commit skipped: no active generation (coordinator=" &
      $(c.group.coordinator != nil) & ", generationId=" & $snap.generationId & ")"
    warn msg
    raise newKafkaCommitSkippedError(msg)

  if c.fetcher.assignmentGeneration != snap.generationId:
    # Rebalance window: the group generation advanced but the fetcher has not been
    # re-based by the next poll. getConsumedOffsets still returns the previous
    # assignment's offsets; committing them under the new generation would silently
    # rewind the new owner (classic OffsetCommit does not verify ownership).
    let msg =
      "commit skipped: fetcher assignmentGeneration (" & $c.fetcher.assignmentGeneration &
      ") lags group generationId (" & $snap.generationId & ")"
    warn msg
    raise newKafkaCommitSkippedError(msg)

  if snapshotOffsets.isSome and snap.generationId != snapshotGeneration:
    # Caller-time snapshot (commitAsync) is only valid under its own generation.
    let msg =
      "commit skipped: snapshot generation (" & $snapshotGeneration &
      ") != live generationId (" & $snap.generationId & ")"
    warn msg
    raise newKafkaCommitSkippedError(msg)

  # Prefer the epoch under which the consumed record was written; fall back to cluster
  # metadata only when the fetcher has none. Sending the current metadata leader after
  # an unclean election would save an epoch the consumer never reached, suppressing
  # KIP-320 truncation detection on restart.
  let liveLeaderEpochs =
    if snapshotOffsets.isSome:
      snapshotLeaderEpochs
    else:
      c.fetcher.getConsumedLeaderEpochs()

  # `committedLeaderEpoch` is gated at v6+; older brokers see the v2-v4 layout.
  var topicReqs = initTable[string, seq[OffsetCommitPartitionReq]]()
  for tp, offset in offsets:
    let liveEpoch = liveLeaderEpochs.getOrDefault(tp, low(int32))
    let leaderEpoch =
      if liveEpoch != low(int32):
        liveEpoch
      else:
        let leaderInfo = c.cluster.getPartitionLeader(tp)
        if leaderInfo.isSome: leaderInfo.get.leaderEpoch else: -1'i32
    topicReqs.mgetOrPut(tp.topic, @[]).add(
      OffsetCommitPartitionReq(
        partition: tp.partition,
        offset: offset,
        committedLeaderEpoch: leaderEpoch,
        metadata: none(string),
      )
    )

  var commitTopics: seq[OffsetCommitTopicReq]
  for topic, parts in topicReqs:
    commitTopics.add(OffsetCommitTopicReq(topic: topic, partitions: parts))

  let req = OffsetCommitRequest(
    groupId: c.group.groupId,
    generationId: snap.generationId,
    memberId: snap.memberId,
    groupInstanceId: none(string),
    retentionTimeMs: -1,
    topics: commitTopics,
  )

  template isCoordinatorAvailErr(code: KafkaErrorCode): bool =
    ## Java's ConsumerCoordinator retries OffsetCommit when the coordinator is
    ## unavailable or moved: rediscover and reissue up to
    ## `maxCoordinatorRediscoverAttempts` times. Other errors surface via
    ## armRebalanceFromCommitError; LOAD_IN_PROGRESS is retried too.
    isCoordinatorMoved(code) or code == keCoordinatorLoadInProgress

  var resp: OffsetCommitResponse
  block obtainResp:
    let maxAttempts = max(c.group.maxCoordinatorRediscoverAttempts, 1)
    for attempt in 1 .. maxAttempts:
      resp = await c.group.coordinator.offsetCommit(req)
      var sawCoordinatorErr = false
      for topicResp in resp.topics:
        for partResp in topicResp.partitions:
          if isCoordinatorAvailErr(partResp.errorCode):
            sawCoordinatorErr = true
            break
        if sawCoordinatorErr:
          break
      if not sawCoordinatorErr:
        break obtainResp
      if attempt == maxAttempts:
        break obtainResp
      c.cluster.invalidateCoordinator(c.group.groupId)
      warn "OffsetCommit coordinator-unavailable (attempt ",
        attempt, "/", maxAttempts, "); rediscovering"
      let rd = await c.group.rediscoverCoordinator()
      case rd
      of rdSuccess:
        discard
      of rdCancelled:
        raise newException(CancelledError, "commit cancelled during rediscover")
      of rdFailed:
        # Rediscovery exhausted; fall through with the last response so the
        # caller sees the broker's actual error rather than a synthetic one.
        break obtainResp

  var partErrors: seq[PartitionCommitError]
  var firstCode = keNone
  var firstMsg = ""
  for topicResp in resp.topics:
    for partResp in topicResp.partitions:
      if isCoordinatorMoved(partResp.errorCode):
        c.cluster.invalidateCoordinator(c.group.groupId)
      if partResp.errorCode != keNone:
        # Include rebalance-required codes (ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID)
        # so the caller can arm rebalanceRequested itself; this proc stays pure.
        let msg =
          "OffsetCommit error for " & topicResp.topic & "-" & $partResp.partition & ": " &
          kafkaErrorName(partResp.errorCode)
        warn msg
        partErrors.add(
          PartitionCommitError(
            topic: topicResp.topic,
            partition: partResp.partition,
            errorCode: partResp.errorCode,
          )
        )
        if firstCode == keNone:
          firstCode = partResp.errorCode
          firstMsg = msg

  if partErrors.len > 0:
    if partErrors.len > 1:
      firstMsg = firstMsg & " (and " & $(partErrors.len - 1) & " more error(s))"
    raise newKafkaCommitError(firstCode, firstMsg, partErrors)

proc commitOffsets(
    c: KafkaConsumer,
    snapshotOffsets: Option[Table[TopicPartition, int64]] =
      none(Table[TopicPartition, int64]),
    snapshotGeneration: int32 = -1'i32,
    snapshotLeaderEpochs: Table[TopicPartition, int32] =
      initTable[TopicPartition, int32](),
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Serializes every commit behind `commitLock` (released on every exit path) so
  ## concurrent commits cannot race on the coordinator handle `rediscoverCoordinator`
  ## repoints.
  ##
  ## `snapshotOffsets` / `snapshotGeneration` (commitAsync) pin the offsets and
  ## generation to the caller's instant instead of re-reading the live table after the
  ## lock is acquired; `snapshotLeaderEpochs` pairs with them (KIP-320 committed
  ## leader epochs).
  await c.commitLock.acquire()
  try:
    await c.commitOffsetsLocked(
      snapshotOffsets, snapshotGeneration, snapshotLeaderEpochs
    )
  finally:
    c.commitLock.releaseSafely("consumer.commitOffsets")

proc armRebalanceFromCommitError*(
    c: KafkaConsumer, e: ref KafkaCommitError
) {.raises: [].} =
  ## Arm `rebalanceRequested` when a KafkaCommitError partition error signals stale
  ## membership (ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID, other ecRebalanceRequired
  ## codes).
  for pe in e.partitionErrors:
    if isRebalanceRequired(pe.errorCode):
      c.group.rebalanceRequested = true
      return

proc autoCommitLoop(c: KafkaConsumer) {.async: (raises: []).} =
  ## Sleeps between commits, racing the sleep against `closeSignal` so close()
  ## wakes us promptly instead of waiting out the full interval.
  while c.state == lsRunning and c.config.autoCommit:
    let sleepFut = sleepAsync(milliseconds(c.config.autoCommitIntervalMs))
    let signalFut = c.closeSignal.wait()

    try:
      discard await race(sleepFut, signalFut)
    except ValueError:
      discard
    except CancelledError:
      await sleepFut.cancelAndWait()
      await signalFut.cancelAndWait()
      return

    if not sleepFut.finished:
      await sleepFut.cancelAndWait()
    if not signalFut.finished:
      await signalFut.cancelAndWait()

    if c.state != lsRunning:
      return
    try:
      await c.commitOffsets()
    except KafkaCommitSkippedError:
      # Skip already logged by commitOffsetsLocked. Move on to the next tick.
      discard
    except KafkaCommitError as ce:
      c.armRebalanceFromCommitError(ce)
      warn "Auto-commit failed: ", ce.msg
    except KafkaError as e:
      warn "Auto-commit failed: ", e.msg
    except CancelledError:
      # A cancel while still lsRunning means an outside force (parent cancel chain)
      # ended an active commit mid-flight: it may or may not have reached the broker.
      # A close-driven cancel leaves lsRunning first and stays silent.
      if c.state == lsRunning:
        warn "auto-commit loop cancelled while consumer still running; pending offsets may be lost"
      return

proc classifyOffsetFetchError*(
    code: KafkaErrorCode, offset: int64
): OffsetFetchAction {.inline, raises: [].} =
  if code == keNone:
    return if offset >= 0: ofaUseOffset else: ofaNoCommitted
  if isCoordinatorMoved(code):
    # Both NOT_COORDINATOR and GROUP_COORDINATOR_NOT_AVAILABLE invalidate: treating
    # the latter as plain-retriable cached the stale mapping across a failover until
    # the rebalance path re-resolved it.
    return ofaInvalidateCoordinator
  case errorClass(code)
  of ecRebalanceRequired: ofaRebalance
  of ecRetriable: ofaRetriable
  else: ofaFatal

proc applyOffsetFetchRecovery*(
    c: KafkaConsumer, e: ref KafkaOffsetFetchError
) {.raises: [].} =
  ## Cheap idempotent side effects of an OffsetFetch failure (invalidateCoordinator,
  ## rebalance-arm); full rediscovery and membership reset stay in
  ## `handleRebalanceIfNeeded`.
  var needsInvalidate = false
  var needsRebalance = false
  template inspect(code: KafkaErrorCode) =
    if isCoordinatorMoved(code):
      needsInvalidate = true
    elif isRebalanceRequired(code):
      needsRebalance = true

  if e.partitionErrors.len == 0:
    inspect(e.errorCode)
  else:
    for pe in e.partitionErrors:
      inspect(pe.errorCode)
  if needsInvalidate:
    c.cluster.invalidateCoordinator(c.group.groupId)
  if needsRebalance:
    c.group.rebalanceRequested = true

proc fetchInitialOffsets(
    c: KafkaConsumer, partitions: seq[TopicPartition]
) {.async: (raises: [KafkaError, CancelledError]).} =
  var topicReqs = initTable[string, seq[int32]]()
  for tp in partitions:
    topicReqs.mgetOrPut(tp.topic, @[]).add(tp.partition)

  var fetchTopics: seq[OffsetFetchTopicReq]
  for topic, parts in topicReqs:
    fetchTopics.add(OffsetFetchTopicReq(topic: topic, partitions: parts))

  let req = OffsetFetchRequest(groupId: c.group.groupId, topics: fetchTopics)

  let resp = await c.group.coordinator.offsetFetch(req)

  if resp.errorCode != keNone:
    # OffsetFetch v2+ carries a top-level errorCode with an EMPTY topics array on
    # group-level failures (e.g. NOT_COORDINATOR during failover). Without this check
    # every partition looks like "no committed offset" and the reset policy jumps to
    # latest/earliest — data loss or mass replay.
    let topLevel = newKafkaOffsetFetchError(
      resp.errorCode,
      "OffsetFetch top-level error: " & kafkaErrorName(resp.errorCode),
      @[],
    )
    c.applyOffsetFetchRecovery(topLevel)
    raise topLevel

  var offsets = initTable[TopicPartition, int64]()
  # KIP-320: committedLeaderEpoch seeds lastFetchedEpoch so a restart validates
  # against the committed position before the first fetch. Broker-normalized to -1 at
  # versions < 5.
  var epochs = initTable[TopicPartition, int32]()
  # The raised error's `errorCode` stays the first non-keNone code (API/test
  # compatibility); `partitionErrors` carries every code so handleRebalanceIfNeeded
  # can aggregate recovery across a mixed batch instead of flattening to the winner.
  var partErrors: seq[PartitionCommitError]
  for topicResp in resp.topics:
    for partResp in topicResp.partitions:
      let tp = TopicPartition(topic: topicResp.topic, partition: partResp.partition)
      case classifyOffsetFetchError(partResp.errorCode, partResp.offset)
      of ofaUseOffset:
        offsets[tp] = partResp.offset
        if partResp.committedLeaderEpoch >= 0:
          epochs[tp] = partResp.committedLeaderEpoch
      of ofaNoCommitted:
        discard # fall through to fetcher's offsetResetPolicy
      of ofaInvalidateCoordinator, ofaRebalance, ofaRetriable:
        partErrors.add(
          PartitionCommitError(
            topic: topicResp.topic,
            partition: partResp.partition,
            errorCode: partResp.errorCode,
          )
        )
      of ofaFatal:
        raise newKafkaOffsetFetchError(
          partResp.errorCode,
          "OffsetFetch error for " & tp.topic & "-" & $tp.partition & ": " &
            kafkaErrorName(partResp.errorCode),
          @[
            PartitionCommitError(
              topic: topicResp.topic,
              partition: partResp.partition,
              errorCode: partResp.errorCode,
            )
          ],
        )
  if partErrors.len > 0:
    let firstError = partErrors[0].errorCode
    let e = newKafkaOffsetFetchError(
      firstError,
      "OffsetFetch returned " & kafkaErrorName(firstError) &
        " for one or more partitions",
      partErrors,
    )
    c.applyOffsetFetchRecovery(e)
    raise e

  await c.fetcher.setPartitions(partitions, offsets, epochs)

proc start*(c: KafkaConsumer) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Join the group, fetch initial offsets, and start the fetch/heartbeat
  ## loops. A no-op once running; raises `KafkaClosedError` after close.
  if c.state == lsRunning:
    return
  if c.state.isClosingOrClosed:
    raise newException(KafkaClosedError, "consumer is closed")

  let partitions = await c.group.joinAndSync()

  # joinAndSync made us a live member but no heartbeat runs yet: if startup fails we
  # must leave, else we are a heartbeat-less zombie stalling the group's rebalance
  # until session timeout. On cancellation we stay lsUninitialized and let close()
  # send LeaveGroup — awaiting a round-trip while our own future is cancelled is
  # unreliable.
  try:
    await c.fetchInitialOffsets(partitions)
  except KafkaError as e:
    await c.group.leave()
    raise e

  # Java contract: onPartitionsAssigned sees a consistent consumer and runs before
  # any record is delivered, so a listener seek() cannot race the fetcher. Fire after
  # lsRunning is set, before the fetcher/heartbeat loops spawn.
  c.state = lsRunning
  let listenerErr = invokeListener(c.config.onPartitionsAssigned, partitions)
  # Sync the fetcher's generation before the fetch loop spawns so the first round's
  # records carry the live generationId and a later rebalance can drop leftovers.
  c.fetcher.bumpGeneration(c.group.snapshotMembership().generationId)
  c.fetcher.start()
  c.group.startHeartbeat()

  if c.config.autoCommit:
    c.commitLoopFut = autoCommitLoop(c)

  if listenerErr != nil:
    # Startup completed (fetcher + heartbeat running) — the consumer is usable
    # despite the listener failure. Surface it to the caller, Java-style.
    raise newKafkaRebalanceCallbackError("onPartitionsAssigned", listenerErr)

proc rebalanceRetryBackoff(
    attempt, maxAttempts: int, deadline: Option[Moment], e: ref KafkaError
): Future[bool] {.async: (raises: [CancelledError]).} =
  ## Inter-attempt backoff for handleRebalanceIfNeeded: true to retry, false at the
  ## final attempt or once the deadline elapsed. Sleeps a jittered 500ms-base backoff
  ## clamped to the remaining deadline.
  if attempt >= maxAttempts:
    warn "Rebalance attempt ", attempt, "/", maxAttempts, " failed; giving up: ", e.msg
    return false
  warn "Rebalance attempt ", attempt, "/", maxAttempts, " failed: ", e.msg
  var backoff = milliseconds(jitteredBackoff(500'i32, attempt))
  if deadline.isSome:
    let remaining = deadline.get - Moment.now()
    if remaining <= nanoseconds(0):
      return false
    if remaining < backoff:
      backoff = remaining
  await sleepAsync(backoff)
  if deadline.isSome and (deadline.get - Moment.now()) <= nanoseconds(0):
    return false
  return true

proc onJoinPrepare(c: KafkaConsumer) {.async: (raises: [CancelledError]).} =
  ## Rebalance teardown (Java's ConsumerCoordinator.onJoinPrepare): stop the fetcher,
  ## run the pre-rebalance commit iff membership is still valid, and fire exactly ONE
  ## of onPartitionsRevoked / onPartitionsLost for the old assignment. Listener
  ## exceptions are parked on `c.rejoinListenerError` and re-raised by the caller.
  # Snapshot before teardown so the callbacks see the actual assignment even
  # when the rejoin spans several polls.
  c.rejoinPrevAssigned = @(c.group.assignedPartitions)
  c.rejoinListenerError = nil
  # Java order: stop fetching → commit what we've seen → notify the user.
  # Callback-first would race the fetcher and hand out pre-commit offsets.
  await c.fetcher.stop()
  if c.group.snapshotMembership().generationId < 0:
    # Membership already invalidated (heartbeat fenced / self-left): assignment
    # is LOST, not revoked. Skip the commit — a fenced generation cannot commit.
    let cbErr = invokeListener(c.config.onPartitionsLost, c.rejoinPrevAssigned)
    if cbErr != nil:
      c.rejoinListenerError = newKafkaRebalanceCallbackError("onPartitionsLost", cbErr)
  else:
    try:
      await c.commitOffsets()
    except KafkaCommitSkippedError:
      discard
    except KafkaCommitError as ce:
      # Surface fencing so the caller's retry loop can resetMembership.
      c.armRebalanceFromCommitError(ce)
      warn "Offset commit before rebalance failed: ", ce.msg
    except KafkaError as e:
      warn "Offset commit before rebalance failed: ", e.msg
    # Heartbeat may have fenced us mid-commit; re-check so the exclusive
    # revoked/lost contract holds.
    if c.group.snapshotMembership().generationId < 0:
      let cbErr = invokeListener(c.config.onPartitionsLost, c.rejoinPrevAssigned)
      if cbErr != nil:
        c.rejoinListenerError =
          newKafkaRebalanceCallbackError("onPartitionsLost", cbErr)
    else:
      let cbErr = invokeListener(c.config.onPartitionsRevoked, c.rejoinPrevAssigned)
      if cbErr != nil:
        c.rejoinListenerError =
          newKafkaRebalanceCallbackError("onPartitionsRevoked", cbErr)
  c.rejoinPrepareDone = true

proc handleRebalanceIfNeeded(
    c: KafkaConsumer, deadline: Option[Moment] = none(Moment)
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Handles a rebalance requested by the heartbeat loop: retries up to 3 times on
  ## transient errors, raises immediately on a permanent one, and on retry exhaustion
  ## raises the last error with `rebalanceRequested` still set for the next poll retry
  ## (only the fetcher stays stopped while retrying; the heartbeat keeps running so
  ## the session survives a slow rejoin).
  ##
  ## Listener contract (Java parity): teardown fires exactly ONE of
  ## onPartitionsRevoked (graceful, after the pre-rebalance commit) or
  ## onPartitionsLost (invalidated membership, nothing committed). A parked listener
  ## error is re-raised as KafkaRebalanceCallbackError after the rebalance (warn if
  ## that failed).
  ##
  ## With `deadline` set (poll/pollBatch) the JoinGroup+SyncGroup races the remaining
  ## time; if the deadline wins the rejoin is NOT cancelled (that would advance the
  ## broker generation and force another group-wide rebalance) but parked on
  ## `c.rejoinFut` and resumed by the next poll.
  ##
  ## (rebalanceRequested, rejoinFut, rejoinPrepareDone): (false, nil, false) idle;
  ## (true, nil, false) fresh episode → onJoinPrepare → joinAndSync;
  ## (true, some, true) join parked by deadline, resume the wait;
  ## (true, nil, true) join done, OffsetFetch retrying (or retry-exhausted,
  ## re-armed for the next poll). Success clears all three.
  if c.state != lsRunning:
    return
  if not c.group.rebalanceRequested and c.rejoinFut == nil:
    return

  # Tear down only when STARTING a fresh rebalance, not when resuming a parked
  # rejoin (fetcher stopped, offsets committed). `rejoinPrepareDone` is Java's
  # needsJoinPrepare: it guards against re-firing after retry exhaustion re-armed
  # `rebalanceRequested`.
  if c.rejoinFut == nil and not c.rejoinPrepareDone:
    await c.onJoinPrepare()
    # close() can run during the prepare (it awaits the pre-rebalance commit) and
    # tears down whatever rejoin it finds, so a join started now would outlive the
    # consumer and could re-register the membership LeaveGroup just dropped.
    if c.state != lsRunning:
      return

  const maxRebalanceRetries = 3
  var lastErr: ref KafkaError
  # Once JoinGroup+SyncGroup succeeds we hold the assignment, so a transient
  # OffsetFetch failure retries ONLY the offset fetch: re-running JoinGroup would
  # advance the generation and ripple a rebalance storm across the group on a
  # coordinator failover (every member's OffsetFetch fails at once). Java parity.
  var joined = none(seq[TopicPartition])
  for attempt in 1 .. maxRebalanceRetries:
    if joined.isNone:
      # Start a fresh JoinGroup+SyncGroup unless one is already in flight from a
      # previous deadline-bounded poll.
      if c.rejoinFut == nil:
        c.rejoinFut = c.group.joinAndSync()
      # Hold the in-flight join in a local for the rest of this attempt: close()
      # clears the field while we are parked below (finished rejoins are dropped
      # without cancelling) and may be mid-teardown after the prepare await, so
      # re-reading it can deref nil.
      let joinFut = c.rejoinFut

      # Wait for the rejoin, bounded by the caller's deadline. `race` does NOT cancel
      # the loser, so a timer win leaves the join running for the next poll, not
      # aborted mid-handshake.
      if deadline.isSome:
        let remaining = deadline.get - Moment.now()
        if remaining <= nanoseconds(0):
          return # out of budget; rejoinFut stays alive for the next poll
        let timer = sleepAsync(remaining)
        discard await race(FutureBase(joinFut), FutureBase(timer))
        if not joinFut.finished:
          return # deadline hit mid-rejoin; resume on the next poll
        await timer.cancelAndWait()

      # Rejoin settled. Detach before awaiting so an error leaves no stale
      # in-flight future and the next attempt starts a fresh one.
      c.rejoinFut = nil
      try:
        joined = some(await joinFut)
        # close() cancels the rejoin future above and stops fetcher/heartbeat
        # while we resume; skip restart if we raced through the cancel window.
        if c.state != lsRunning:
          return
      except KafkaError as e:
        lastErr = e
        if e of KafkaApiError:
          let apiErr = (ref KafkaApiError)(e)
          # joinAndSync already reset the membership on UNKNOWN_MEMBER_ID /
          # ILLEGAL_GENERATION, so the next rejoin starts fresh; only a permanent code
          # stops the retries. Join/Sync responses carry group-level codes, where
          # isInstanceFatal and isPermanent coincide — a record-level ecFatal would
          # not.
          if apiErr.errorCode.isInstanceFatal():
            # Mark the fetcher failed so its stopped state is observable instead of
            # nil after fetcher.stop(); tryRestartFetcher won't restart a permanent
            # error, so this cannot spin. No lost listener: the old assignment was
            # already surfaced at teardown.
            c.fetcher.setLastError(e)
            c.dropListenerError("permanent rebalance error")
            raise e
          if isCoordinatorMoved(apiErr.errorCode):
            c.cluster.invalidateCoordinator(c.group.groupId)
        if not (await rebalanceRetryBackoff(attempt, maxRebalanceRetries, deadline, e)):
          break
        continue

    # OffsetFetch + start phase (JoinGroup already succeeded)
    let partitions = joined.get
    try:
      await c.fetchInitialOffsets(partitions)
      # A concurrent close() may have stopped fetcher/heartbeat and sent LeaveGroup
      # while we awaited OffsetFetch; restarting them would spawn a zombie member.
      # Bail before touching listener or fetcher.
      if c.state != lsRunning:
        return
      # Mirror `start()`: the listener runs before fetcher/heartbeat spawn, so a user
      # `seek()` in onPartitionsAssigned wins the race against the first fetch round.
      let assignedErr = invokeListener(c.config.onPartitionsAssigned, partitions)
      if assignedErr != nil:
        if c.rejoinListenerError == nil:
          c.rejoinListenerError =
            newKafkaRebalanceCallbackError("onPartitionsAssigned", assignedErr)
        else:
          # First listener error wins (Java's firstException); log the loser.
          warn "onPartitionsAssigned listener raised after an earlier ",
            "listener error: ", assignedErr.msg
      # Sync the fetcher's generation before the fetch loop restarts (the queue was
      # drained by fetcher.stop(); the new generation is in group.generationId).
      c.fetcher.bumpGeneration(c.group.snapshotMembership().generationId)
      c.fetcher.start()
      c.group.startHeartbeat()
      # doSyncGroup cleared rebalanceRequested on gsStable, but the pre-rebalance
      # commit or the heartbeat path may have re-armed it: clear here so the next poll
      # does not immediately re-enter rebalance.
      c.group.rebalanceRequested = false
      # Episode complete; a fresh rebalance will re-run the teardown listener.
      c.rejoinPrepareDone = false
      lastErr = nil
      break
    except KafkaError as e:
      lastErr = e
      if e of KafkaApiError:
        let apiErr = (ref KafkaApiError)(e)
        # OffsetFetch returns coordinator-level codes, where the instance-fatal and
        # permanent sets coincide; a record-level ecFatal (e.g. MESSAGE_TOO_LARGE)
        # cannot surface here and must not terminate the instance.
        if apiErr.errorCode.isInstanceFatal():
          c.fetcher.setLastError(e)
          c.dropListenerError("permanent rebalance error")
          raise e
        # Aggregate recovery across every partition code: a mixed batch needs both
        # actions, and first-wins flattening would let the runner-up slip to a later
        # retry. rediscoverCoordinator re-points c.group.coordinator — a bare
        # invalidate would leave every retry on the pre-failover broker.
        var needsReset = false
        var needsRediscover = false
        template inspect(code: KafkaErrorCode) =
          if code == keUnknownMemberId or code == keIllegalGeneration:
            needsReset = true
          elif isCoordinatorMoved(code):
            needsRediscover = true

        if e of KafkaOffsetFetchError:
          let ofe = (ref KafkaOffsetFetchError)(e)
          if ofe.partitionErrors.len == 0:
            inspect(ofe.errorCode)
          else:
            for pe in ofe.partitionErrors:
              inspect(pe.errorCode)
        else:
          inspect(apiErr.errorCode)
        if needsReset:
          c.group.resetMembership()
          joined = none(seq[TopicPartition])
        if needsRediscover:
          case await c.group.rediscoverCoordinator()
          of rdCancelled:
            raise newException(
              CancelledError, "coordinator rediscover cancelled during offset fetch"
            )
          of rdSuccess, rdFailed:
            discard
      if not (await rebalanceRetryBackoff(attempt, maxRebalanceRetries, deadline, e)):
        break
      continue

  if lastErr != nil:
    if joined.isSome:
      # SyncGroup succeeded but OffsetFetch exhausted its retries, and doSyncGroup
      # already cleared rebalanceRequested. Without re-arming, the next poll
      # early-returns and tryRestartFetcher restarts under the OLD generation
      # (bumpGeneration never ran): every record is dropped by the stale-generation
      # filter, a silent no-delivery livelock that also lets auto-commit write stale
      # offsets. The join-phase path recovers on its own.
      c.group.rebalanceRequested = true
    # Mark the fetcher failed so the next poll's tryRestartFetcher fires;
    # fetcher.stop() otherwise leaves lastError nil and the consumer silently stopped.
    c.fetcher.setLastError(lastErr)
    c.dropListenerError("rebalance failure")
    raise lastErr

  if c.rejoinListenerError != nil:
    # Bookkeeping complete (fetcher + heartbeat restarted under the new generation):
    # now surface the listener exception to the poll caller.
    let cbErr = c.rejoinListenerError
    c.rejoinListenerError = nil
    raise cbErr

proc tryRestartFetcher(c: KafkaConsumer) {.raises: [KafkaError].} =
  ## Restart the fetcher after an error, with a 5-second cooldown against tight loops
  ## and escalation after `maxFetcherRestartAttempts` consecutive failures, so a
  ## permanent error surfaces instead of looping forever.
  ##
  ## Fast-path: an ecFatal KafkaApiError (AUTHORIZATION_FAILED, etc.) cannot be
  ## recovered by restarting, so it skips the cooldown and surfaces on the next poll.
  if c.fetcher.lastError == nil:
    return

  if c.fetcher.lastError of KafkaApiError:
    let apiErr = (ref KafkaApiError)(c.fetcher.lastError)
    # Only codes that leave the instance unusable skip escalation; a record-level
    # fatal (MESSAGE_TOO_LARGE, INVALID_RECORD) still gets the cooldown +
    # maxFetcherRestartAttempts path, surfacing with progress, not failing poll #1.
    if apiErr.errorCode.isInstanceFatal:
      error "Fetcher stopped on permanent error; not restarting: " & apiErr.msg
      raise apiErr

  # start() no-ops while running=true; surface the persistent-failure signal
  # without counting a fake restart if the loop is still alive.
  if c.fetcher.isLoopAlive:
    raise newException(KafkaError, "fetcher error: " & c.fetcher.lastError.msg)

  # Loop returned without clearing running (permanent-error path, or a
  # CancelledError aborted the retry sleep after lastError was set).
  c.fetcher.forceRunningFalse()

  let cap = c.config.maxFetcherRestartAttempts
  if cap > 0 and c.fetcher.consecutiveRestartAttempts >= cap:
    let msg =
      "fetcher unrecoverable after " & $c.fetcher.consecutiveRestartAttempts &
      " restart attempts: " & c.fetcher.lastError.msg
    error msg
    raise newException(KafkaError, msg)

  let now = Moment.now()
  let minRestartInterval = seconds(5)
  if now - c.fetcher.lastRestartAttempt < minRestartInterval:
    # Within the cooldown: surface the error (with progress, so a caller can tell a
    # transient retry from an imminent give-up) instead of hanging; escalation
    # advances on the next post-cooldown poll.
    let progress =
      if cap > 0:
        " (restart " & $c.fetcher.consecutiveRestartAttempts & "/" & $cap &
          ", cooling down)"
      else:
        " (restart " & $c.fetcher.consecutiveRestartAttempts & ", cooling down)"
    raise newException(
      KafkaError, "fetcher stopped" & progress & ": " & c.fetcher.lastError.msg
    )

  warn "Fetcher stopped with error, attempting restart (",
    c.fetcher.consecutiveRestartAttempts + 1,
    if cap > 0:
      "/" & $cap
    else:
      "",
    "): ",
    c.fetcher.lastError.msg

  c.fetcher.lastRestartAttempt = now
  c.fetcher.consecutiveRestartAttempts.inc
  c.fetcher.start()

proc dropIfStalePosEpoch(c: KafkaConsumer, record: FetchedRecord): bool {.raises: [].} =
  ## Drop records superseded by a seek / OOR reset after enqueue.
  let tp = TopicPartition(topic: record.topic, partition: record.partition)
  let live = c.fetcher.currentPosEpoch(tp)
  if not isRecordStalePosEpoch(record, live):
    return false

  let partStr = $record.partition
  let recEpochStr = $record.posEpoch
  let curEpochStr = $(if live.isSome: live.get else: -1'i64)
  emitMetric(
    c.config.metricsHook,
    meConsumerRepositionStaleRecordsDropped,
    [
      (cstring"topic", cstring(record.topic)),
      (cstring"partition", cstring(partStr)),
      (cstring"record_pos_epoch", cstring(recEpochStr)),
      (cstring"current_pos_epoch", cstring(curEpochStr)),
    ],
    1,
  )
  true

proc dropIfStaleGeneration(
    c: KafkaConsumer, record: FetchedRecord
): bool {.raises: [].} =
  ## True (and a metric) when `record` carries a previous group generation: the poll
  ## path swallows these so the caller never sees data from a revoked assignment.
  let current = c.group.snapshotMembership().generationId
  if not isRecordStaleGeneration(record, current):
    return false

  let partStr = $record.partition
  let recGenStr = $record.generation
  let curGenStr = $current
  emitMetric(
    c.config.metricsHook,
    meConsumerRebalanceStaleRecordsDropped,
    [
      (cstring"topic", cstring(record.topic)),
      (cstring"partition", cstring(partStr)),
      (cstring"record_generation", cstring(recGenStr)),
      (cstring"current_generation", cstring(curGenStr)),
    ],
    1,
  )
  true

proc dropStaleGenerationRecords(
    c: KafkaConsumer, records: var seq[FetchedRecord]
) {.raises: [].} =
  ## Re-filter a collected batch against the live generation. The pollBatch* loop
  ## re-enters handleRebalanceIfNeeded per iteration, so a rebalance can complete
  ## mid-call and stale records would be returned with their offsets advanced via
  ## pendingAdvance — a stale offset+1 clobbering the reset position and silently
  ## skipping records. poll() returns after one record, so it lacks that window.
  ## Pre-reposition records that slipped past the enqueue-side post-put re-check are
  ## filtered too.
  var kept: seq[FetchedRecord]
  for rec in records:
    if c.dropIfStaleGeneration(rec):
      continue
    if c.dropIfStalePosEpoch(rec):
      continue
    kept.add(rec)
  records = kept

proc drainBufferedRecords(c: KafkaConsumer, limit: int): seq[FetchedRecord] =
  ## Pop up to `limit` enqueued records without blocking, skipping stale ones.
  ## Delivers what a healthy broker buffered before a permanent fetcher error surfaced
  ## (Java parity): popped records cannot be re-queued, so raising would strand them;
  ## the error re-raises on the next poll once the buffer drains.
  if limit <= 0:
    return

  while result.len < limit:
    var rec: FetchedRecord
    try:
      rec = c.fetcher.records.popFirstNoWait()
    except AsyncQueueEmptyError:
      break
    if c.dropIfStaleGeneration(rec):
      continue
    if c.dropIfStalePosEpoch(rec):
      continue
    result.add(rec)

proc checkPollInterval(c: KafkaConsumer) {.raises: [KafkaError].} =
  ## If the heartbeat loop self-left because the app missed max.poll.interval.ms,
  ## surface it exactly once: clear the flag and raise. Membership is already reset
  ## and `rebalanceRequested` armed, so the NEXT poll rejoins. Runs at the entry of
  ## every poll variant, before notePoll re-stamps the clock.
  if c.group.pollIntervalExceeded:
    c.group.pollIntervalExceeded = false
    raise newException(
      KafkaError,
      "max.poll.interval.ms exceeded; the consumer left the group and its " &
        "partitions were reassigned. The next poll rejoins the group.",
    )

proc poll*(
    c: KafkaConsumer, timeout: Duration = milliseconds(500)
): Future[FetchedRecord] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Poll for a single record; raises KafkaTimeoutError if none arrives within
  ## `timeout`.
  ##
  ## Position semantics (Java's KafkaConsumer.poll()): the previously returned
  ## record's `consumedOffset` advances on entry and this call's record is queued for
  ## advance on the NEXT poll/commit, so a cancel or timeout leaves it un-consumed for
  ## re-delivery.
  ##
  ## Records tagged with a previous generation are dropped, so the caller never sees
  ## data from a revoked assignment.
  ##
  ## A legacy MessageSet (pre-0.11, magic 0/1) batch in the fetch stream has its
  ## records dropped, and the position advances by one offset past the batch's
  ## `baseOffset` only even when it spans many offsets: the resulting gap is reported
  ## by a `warn` log and the `consumer.batch.legacy_skipped` metric.
  ##
  ## A rebalance in progress shares the `timeout` budget with fetching and can surface
  ## as KafkaTimeoutError (the next poll resumes); give rebalance-prone polls more
  ## timeout.
  requireRunning(c)
  c.checkPollInterval()
  c.group.notePoll()
  c.flushPendingAdvance()

  let deadline = Moment.now() + timeout
  while true:
    await c.handleRebalanceIfNeeded(some(deadline))
    requireRunning(c)

    try:
      c.tryRestartFetcher()
    except KafkaError as e:
      # A permanent fetcher error must not strand records a healthy broker already
      # enqueued: deliver one before surfacing the error, and keep delivering one
      # per poll until the buffer drains — only then re-raise.
      let buffered = c.drainBufferedRecords(1)
      if buffered.len > 0:
        result = buffered[0]
        c.pendingAdvance.add(result)
        return
      raise e

    let remaining = deadline - Moment.now()

    if remaining <= nanoseconds(0):
      raise newException(KafkaTimeoutError, "poll timed out")

    try:
      let candidate = await c.fetcher.records.get().wait(remaining)
      if c.dropIfStaleGeneration(candidate):
        # Loop on the same deadline so one stale record cannot eat the whole timeout
        # budget.
        continue
      if c.dropIfStalePosEpoch(candidate):
        continue
      result = candidate
      c.pendingAdvance.add(result)
      return
    except AsyncTimeoutError:
      requireRunning(c)
      raise newException(KafkaTimeoutError, "poll timed out")

proc tryPoll*(
    c: KafkaConsumer, timeout: Duration = milliseconds(500)
): Future[Option[FetchedRecord]] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Like poll, but returns none(FetchedRecord) on timeout instead of raising.
  try:
    result = some(await c.poll(timeout))
  except KafkaTimeoutError:
    result = none(FetchedRecord)

proc pollBatch*(
    c: KafkaConsumer, maxRecords: int, timeout: Duration
): Future[seq[FetchedRecord]] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Poll up to `maxRecords` records, returning whatever was collected when the
  ## deadline elapses (empty seq if none) — use `pollBatchStrict` when you need a
  ## KafkaTimeoutError to distinguish "no records" from "0 is the answer". `maxRecords
  ## <= 0` returns an empty seq immediately. Legacy MessageSet batches are skipped as
  ## described in `poll`.
  ##
  ## Position semantics (Java's Consumer.poll(Duration)): collected records are NOT
  ## advanced immediately; the next poll\* / commitSync / commitAsync flushes the
  ## queue.
  ##
  ## Rebalance and timeout: same caveat as `poll` — a rebalance shares the budget and
  ## the call still returns what was collected. Error semantics: an error from a LATER
  ## iteration after records were collected returns the partial batch instead of
  ## raising (popped records can't be re-queued); the persistent error re-raises on
  ## the next poll\*.
  requireRunning(c)
  c.checkPollInterval()
  c.group.notePoll()
  c.flushPendingAdvance()

  if maxRecords <= 0:
    return @[]

  var records: seq[FetchedRecord]
  let deadline = Moment.now() + timeout

  while records.len < maxRecords:
    let remaining = deadline - Moment.now()
    if remaining <= nanoseconds(0):
      break

    try:
      await c.handleRebalanceIfNeeded(some(deadline))
      requireRunning(c)
      c.tryRestartFetcher()
    except KafkaError as e:
      # KafkaRebalanceCallbackError is one-shot: rejoinListenerError was nulled
      # before raise, so swallowing it would lose the listener exception.
      if e of KafkaRebalanceCallbackError:
        raise e
      # Other errors above are persistent (re-raise on the next poll*), so
      # deliver records the broker already buffered before surfacing them.
      records.add(c.drainBufferedRecords(maxRecords - records.len))
      if records.len == 0:
        raise e
      break

    let remainingAfter = deadline - Moment.now()
    if remainingAfter <= nanoseconds(0):
      break

    try:
      let rec = await c.fetcher.records.get().wait(remainingAfter)
      if c.dropIfStaleGeneration(rec):
        continue
      if c.dropIfStalePosEpoch(rec):
        continue
      records.add(rec)
    except AsyncTimeoutError:
      break

  # A rebalance that completed between loop iterations supersedes records
  # collected earlier under the prior generation; drop them so a stale
  # offset+1 cannot clobber the freshly reset position.
  c.dropStaleGenerationRecords(records)
  if records.len > 0:
    c.pendingAdvance.add(records)
  result = records

proc pollBatchStrict*(
    c: KafkaConsumer, maxRecords: int, timeout: Duration
): Future[seq[FetchedRecord]] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Strict `pollBatch`: raises `KafkaTimeoutError` when the deadline elapses with
  ## zero records collected instead of returning an empty seq; partial batches are
  ## returned as-is. `maxRecords <= 0` returns an empty seq (not a timeout); position
  ## and error semantics match `pollBatch`.
  ##
  ## Rebalance and timeout: same caveat as `poll`/`pollBatch`, except that a batch
  ## emptied by a mid-collection rebalance returns an empty seq rather than raising —
  ## a rebalance, not a timeout; the next poll resumes.
  requireRunning(c)
  c.checkPollInterval()
  c.group.notePoll()
  c.flushPendingAdvance()

  if maxRecords <= 0:
    return @[]

  var records: seq[FetchedRecord]
  let deadline = Moment.now() + timeout

  while records.len < maxRecords:
    let remaining = deadline - Moment.now()
    if remaining <= nanoseconds(0):
      break

    try:
      await c.handleRebalanceIfNeeded(some(deadline))
      requireRunning(c)
      c.tryRestartFetcher()
    except KafkaError as e:
      # See pollBatch: one-shot listener error must always surface.
      if e of KafkaRebalanceCallbackError:
        raise e
      records.add(c.drainBufferedRecords(maxRecords - records.len))
      if records.len == 0:
        raise e
      break

    let remainingAfter = deadline - Moment.now()
    if remainingAfter <= nanoseconds(0):
      break

    try:
      let rec = await c.fetcher.records.get().wait(remainingAfter)
      if c.dropIfStaleGeneration(rec):
        continue
      if c.dropIfStalePosEpoch(rec):
        continue
      records.add(rec)
    except AsyncTimeoutError:
      break

  # Drop records superseded by a mid-collection rebalance (see pollBatch).
  let collectedBeforeFilter = records.len
  c.dropStaleGenerationRecords(records)
  if records.len == 0:
    requireRunning(c)
    # A batch emptied by a mid-collection rebalance is not a timeout: return an empty
    # seq (the next poll resumes from the new assignment) rather than a misleading
    # KafkaTimeoutError.
    if collectedBeforeFilter > 0:
      return @[]
    raise newException(KafkaTimeoutError, "pollBatchStrict timed out")
  c.pendingAdvance.add(records)
  result = records

proc commitSync*(c: KafkaConsumer) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Commit the caller's consumed position (handed-out batches included) and
  ## advance the live position only after the broker accepts. A failure raises
  ## without moving the position, so nothing unacked is ever committed.
  ##
  ## When the group generation makes the commit unsafe (typically mid-rebalance) it
  ## raises the non-fatal `KafkaCommitSkippedError`: catch it and retry after the
  ## next poll.
  requireRunning(c)
  # Commit the position the caller reached (committable offsets + the deferred advance
  # of the batch just handed out), applying the advance ONLY after the broker accepts:
  # flushing first then raising would advance consumedOffset with no rollback, letting
  # auto-commit write un-acked offsets. The snapshot generation lets a racing
  # rebalance skip it.
  let snapshotOffsets = c.fetcher.consumedOffsetsAfter(c.pendingAdvance)
  let snapshotLeaderEpochs = c.fetcher.consumedLeaderEpochsAfter(c.pendingAdvance)
  let snapshotGeneration = c.group.snapshotMembership().generationId
  try:
    await c.commitOffsets(
      some(snapshotOffsets), snapshotGeneration, snapshotLeaderEpochs
    )
  except KafkaCommitError as ce:
    # Mirror autoCommitLoop / onJoinPrepare / commitAsync: ILLEGAL_GENERATION /
    # UNKNOWN_MEMBER_ID means the broker rejected our membership. Arm the rebalance so
    # the next poll rejoins without waiting out the heartbeat period, then re-raise.
    c.armRebalanceFromCommitError(ce)
    raise ce
  c.flushPendingAdvance()

proc pause*(c: KafkaConsumer, tps: openArray[TopicPartition]) {.raises: [KafkaError].} =
  ## Pause fetching for the given partitions. Records already queued are still
  ## delivered by `poll`/`pollBatch`; later fetch rounds skip these partitions until
  ## `resume`. A pause survives a rebalance for partitions re-assigned to this
  ## consumer; new assignments default to unpaused.
  requireRunning(c)
  c.fetcher.pause(tps)

proc resume*(
    c: KafkaConsumer, tps: openArray[TopicPartition]
) {.raises: [KafkaError].} =
  ## Resume fetching for previously-paused partitions. The next fetch round
  ## resumes from the partition's current `fetchOffset`.
  requireRunning(c)
  c.fetcher.resume(tps)

proc seek*(
    c: KafkaConsumer, tp: TopicPartition, offset: int64
) {.raises: [KafkaError].} =
  ## Set the next fetch position for `tp` to `offset`, dropping `tp`'s buffered
  ## records so the caller cannot receive stale data (other partitions are preserved).
  ## The next `commitOffsets` commits `offset` for `tp` until further records are
  ## polled.
  requireRunning(c)

  # Validate via fetcher.seek first: an invalid seek (negative offset, unassigned tp)
  # raises with `pendingAdvance` untouched, so consumed records remain committable.
  c.fetcher.seek(tp, offset)

  if c.pendingAdvance.len > 0:
    var kept: seq[FetchedRecord]
    for rec in c.pendingAdvance:
      if not (rec.topic == tp.topic and rec.partition == tp.partition):
        kept.add(rec)
    c.pendingAdvance = kept

proc isPaused*(c: KafkaConsumer, tp: TopicPartition): bool {.raises: [].} =
  ## True while fetches for `tp` are suspended by `pause` (position retained,
  ## assignment untouched).
  c.fetcher.isPaused(tp)

proc pausedPartitions*(c: KafkaConsumer): seq[TopicPartition] {.raises: [].} =
  ## Partitions currently suspended by `pause`.
  c.fetcher.pausedPartitions()

proc isNonRetriableCommitError(err: ref KafkaError): bool {.raises: [].} =
  ## A broker rejection that retrying will not fix. A KafkaCommitError counts when any
  ## partition failed that way, not only the first one its `errorCode` carries.
  proc nonRetriable(code: KafkaErrorCode): bool =
    not (code.isRetriable or code.isRebalanceRequired)

  if err == nil or not (err of KafkaApiError):
    return false
  if nonRetriable((ref KafkaApiError)(err).errorCode):
    return true
  if err of KafkaCommitError:
    for pe in (ref KafkaCommitError)(err).partitionErrors:
      if nonRetriable(pe.errorCode):
        return true
  false

proc logCommitError(err: ref KafkaError, why: string) {.raises: [].} =
  ## Log a commitAsync failure that no callback took, at `error` when non-retriable
  ## so error-level alerting catches a commit that keeps failing.
  if isNonRetriableCommitError(err):
    error "commitAsync non-retriable error (" & why & "): ", err.msg
  else:
    warn "commitAsync error (" & why & "): ", err.msg

proc reportCommitResult(
    c: KafkaConsumer, cb: CommitCallback, err: ref KafkaError
) {.raises: [].} =
  ## Hand a commitAsync outcome (`nil` on success) to the callback, or log a failure
  ## when there is none or it raises. The guard keeps such a raise out of
  ## commitAsync's caller and the chronos dispatcher.
  if cb == nil:
    if err != nil:
      logCommitError(err, "no callback")
    return
  let raised = c.commitCallbackFailures.guardCallback("commitAsync callback", ""):
    cb(err)
  if raised != nil and err != nil:
    logCommitError(err, "callback raised")

proc commitAsync*(c: KafkaConsumer, callback: CommitCallback = nil) {.raises: [].} =
  ## Fire-and-forget offset commit. Optional callback for error notification.
  ## Never raises: on a closed consumer the error goes to the callback, as it does
  ## inside the spawned commit task — including a non-fatal
  ## `KafkaCommitSkippedError` when a rebalance outdates the commit.
  if c.state != lsRunning:
    # Closing/closed: honour the fire-and-forget contract and route the error through
    # the callback instead of raising synchronously (a raise would bypass the callback
    # the caller registered, on a call it never awaits). No task is spawned.
    c.reportCommitResult(callback, newException(KafkaClosedError, "consumer is closed"))
    return
  # Snapshot the position AND offset VALUES on the caller's coroutine before spawning,
  # so the commit reflects exactly the records handed out by this call, not whatever a
  # later poll or seek moved before commitLock was acquired. flushPendingAdvance comes
  # first because commitOffsets no longer flushes: without it the last poll's batch
  # would be missing from the snapshot. The generation is captured too, so a rebalance
  # in between skips the stale snapshot.
  c.flushPendingAdvance()
  let snapshotOffsets = c.fetcher.getConsumedOffsets()
  let snapshotLeaderEpochs = c.fetcher.getConsumedLeaderEpochs()
  let snapshotGeneration = c.group.snapshotMembership().generationId

  proc task() {.async: (raises: []).} =
    try:
      await c.commitOffsets(
        some(snapshotOffsets), snapshotGeneration, snapshotLeaderEpochs
      )
      c.reportCommitResult(callback, nil)
    except KafkaCommitError as ce:
      # Mirror autoCommitLoop: ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID means the broker
      # rejected our membership and the next poll must rebalance.
      c.armRebalanceFromCommitError(ce)
      c.reportCommitResult(callback, ce)
    except KafkaError as e:
      c.reportCommitResult(callback, e)
    except CancelledError:
      c.reportCommitResult(callback, newException(KafkaError, "commit cancelled"))

  let fut = task()
  var i = 0
  while i < c.pendingCommits.len:
    if c.pendingCommits[i].finished:
      c.pendingCommits.del(i)
    else:
      i.inc
  c.pendingCommits.add(fut)

proc closeImpl(c: KafkaConsumer) {.async: (raises: []).} =
  ## The teardown behind `close`, which documents the contract. Reachable only
  ## through it, so the single-flight claim and the phase list stay in one place.
  if c.state == lsClosed:
    return

  if c.state == lsUninitialized:
    # Never fully started, but a previous start() may have joined (registered a
    # memberId) then failed before lsRunning. Leaving avoids a heartbeat-less
    # membership stalling the group's rebalance until the session times out.
    if c.group.memberId != "":
      let leaveTimeoutMs = min(c.config.sessionTimeoutMs, 5000'i32)
      try:
        discard await withTimeout(c.group.leave(), milliseconds(leaveTimeoutMs))
      except CancelledError:
        absorbDuringTeardown(
          "consumer::close", "cancel waiting for LeaveGroup after a failed start"
        )

    c.state = lsClosed
    if c.clusterOwned:
      await c.cluster.close()
    return

  c.state = lsClosing
  c.closeSignal.fire()

  # Drain background commits cooperatively: closeSignal.fire() wakes the autoCommit
  # sleep, but the loop may already be inside `await commitOffsets()` — cancelling
  # there leaves the commit accepted by the broker but reported as failed to the
  # client, so wait and cancel only as a fallback.
  #
  # All four phases share one budget bounded by sessionTimeoutMs, so a hung broker on
  # the first drain cannot starve the final commit until the session expires and
  # fences the offset write.
  let closeStart = Moment.now()
  let totalBudgetMs = max(c.config.sessionTimeoutMs - 1000'i32, 2000'i32)

  template remainingBudgetMs(minMs: int32): int32 =
    let elapsed = (Moment.now() - closeStart).milliseconds.int32
    let remaining = totalBudgetMs - elapsed
    if remaining <= 0:
      0'i32
    else:
      max(remaining, minMs)

  if c.commitLoopFut != nil and not c.commitLoopFut.finished:
    let drainTimeoutMs = remainingBudgetMs(500'i32)
    try:
      await c.commitLoopFut.wait(milliseconds(drainTimeoutMs))
    except AsyncTimeoutError:
      warn "autoCommitLoop did not drain within ", drainTimeoutMs, "ms; cancelling"
      try:
        await c.commitLoopFut.cancelAndWait()
      except CancelledError:
        absorbDuringTeardown(
          "consumer::close", "cancel reaping a timed-out autoCommitLoop"
        )
    except CancelledError:
      absorbDuringTeardown("consumer::close", "cancel draining autoCommitLoop")
    except CatchableError as e:
      debug "autoCommitLoop drain swallowed exception: ", e.msg

  # Drain in-flight commitAsync tasks the same way.
  let pendingFuts = c.pendingCommits
  c.pendingCommits.setLen(0)
  for fut in pendingFuts:
    if fut.finished:
      continue
    let drainTimeoutMs = remainingBudgetMs(100'i32)
    try:
      await fut.wait(milliseconds(drainTimeoutMs))
    except AsyncTimeoutError:
      warn "commitAsync future did not drain within ", drainTimeoutMs, "ms; cancelling"
      try:
        await fut.cancelAndWait()
      except CancelledError:
        absorbDuringTeardown(
          "consumer::close", "cancel reaping a timed-out commitAsync"
        )
    except CancelledError:
      absorbDuringTeardown("consumer::close", "cancel draining commitAsync")
    except CatchableError as e:
      debug "commitAsync drain swallowed exception: ", e.msg

  if c.rejoinFut != nil and not c.rejoinFut.finished:
    # Tear down a rejoin parked by a deadline-bounded poll so it cannot outlive the
    # consumer. Safe to cancel here (unlike from poll): we are shutting down and will
    # LeaveGroup, so a half-finished SyncGroup no longer matters.
    try:
      await c.rejoinFut.cancelAndWait()
    except CancelledError:
      absorbDuringTeardown("consumer::close", "cancel reaping an in-flight rejoin")
  c.rejoinFut = nil
  c.rejoinPrepareDone = false

  # Stop the fetcher (no more enqueued records), ordered after the autoCommit drain so
  # the final commit sees the latest consumed offset. joinExitStop reclaims a stop()
  # spawned by the heartbeat permanent-error hook. Both are idempotent, and the stage
  # is restartable: a cancellation delivered here must not abort the teardown, since
  # close() never re-runs closeImpl — the consumer would stay in lsClosing with the
  # owned Cluster open and the fetcher loop running.
  var fetcherStageDone = false
  while not fetcherStageDone:
    try:
      when defined(kafkaCloseTestHooks):
        if c.closeFetcherPark != nil:
          let park = c.closeFetcherPark
          c.closeFetcherPark = nil
          c.closeFetcherParked = true
          await park
      await c.fetcher.stop()
      await c.fetcher.joinExitStop()
      fetcherStageDone = true
    except CancelledError:
      absorbDuringTeardown("consumer::close", "cancel stopping the fetcher")
  when defined(kafkaCloseTestHooks):
    c.closeFetcherParked = false

  # Stop heartbeat – prevents rebalance during final commit. stopHeartbeat() absorbs
  # its own cancellation; the guard keeps a cancel delivered at this boundary from
  # skipping the stages below.
  try:
    await c.group.stopHeartbeat()
  except CancelledError:
    absorbDuringTeardown("consumer::close", "cancel stopping the heartbeat")

  # Final commit while the coordinator is alive (retry once on transient error), using
  # what remains of the close budget. close() is an explicit "I am done", so flush the
  # last batch's position (commitOffsets does not flush by itself).
  c.flushPendingAdvance()
  block finalCommit:
    for attempt in 0 .. 1:
      let commitTimeoutMs = remainingBudgetMs(500'i32)
      let commitFut = c.commitOffsets()
      try:
        await commitFut.wait(milliseconds(commitTimeoutMs))
        break finalCommit
      except AsyncTimeoutError:
        warn "Final offset commit on close timed out after ", commitTimeoutMs, "ms"
        # Reap the timed-out commit instead of orphaning it (it still holds commitLock
        # against a cluster about to close), so no coroutine outlives the consumer —
        # the invariant the drain phases above enforce.
        try:
          await commitFut.cancelAndWait()
        except CancelledError:
          absorbDuringTeardown(
            "consumer::close", "cancel reaping a timed-out final commit"
          )
        break finalCommit
      except KafkaApiError as e:
        if attempt == 0 and e.errorCode.isRetriable:
          warn "Final offset commit on close failed (retrying): ", e.msg
          try:
            await sleepAsync(milliseconds(jitteredBackoff(500'i32, 1)))
          except CancelledError:
            debug "Final offset commit retry cancelled during close"
            break finalCommit
        else:
          # Non-retriable (e.g. ILLEGAL_GENERATION) or the second attempt: retrying
          # keeps failing and we are about to LeaveGroup anyway, so stop.
          warn "Final offset commit on close failed: ", e.msg
          break finalCommit
      except KafkaError as e:
        warn "Final offset commit on close failed: ", e.msg
        break finalCommit
      except CancelledError:
        debug "Final offset commit cancelled during close"
        break finalCommit

  # LeaveGroup so the coordinator reassigns immediately instead of waiting out
  # session.timeout.ms; the wait is bounded by the timeout below.
  let leaveTimeoutMs = min(c.config.sessionTimeoutMs, 5000'i32)
  try:
    discard await withTimeout(c.group.leave(), milliseconds(leaveTimeoutMs))
  except CancelledError:
    absorbDuringTeardown("consumer::close", "cancel waiting for LeaveGroup")

  if c.clusterOwned:
    try:
      await c.cluster.close()
    except CancelledError:
      # A cancel delivered at this boundary must not skip the state transition
      # below. Cluster.close() absorbs its own cancellation and still reaches
      # lsClosed, so the Cluster is not left half-open.
      absorbDuringTeardown("consumer::close", "cancel closing the owned Cluster")

  c.state = lsClosed

proc close*(c: KafkaConsumer) {.async: (raises: []).} =
  ## Single-flight close: a concurrent second close() parks on the in-progress
  ## teardown instead of re-running it. Chronos is single-threaded, so the
  ## check-and-claim below is atomic.
  ##
  ## Phases:
  ##   1. lsClosing — poll()/commitSync() raise KafkaClosedError; commitAsync()
  ##      reports it via callback (or logs).
  ##   2. fire closeSignal, then drain background commits with a bounded wait
  ##      (cancel as last resort) so an in-flight OffsetCommit isn't cut off
  ##      mid-write (the broker would still apply it while the client saw
  ##      CancelledError).
  ##   3. stop fetcher + heartbeat, final commit.
  ##   4. LeaveGroup, release an owned Cluster.
  if c.closeFuture != nil:
    if not c.closeFuture.finished:
      try:
        await c.closeFuture
      except CatchableError:
        absorbDuringTeardown("consumer::close", "a concurrent close() failed")
    return
  if c.state == lsClosed:
    return
  c.closeFuture = closeImpl(c)
  try:
    await c.closeFuture
  except CatchableError:
    absorbDuringTeardown("consumer::close", "closeImpl failed")
