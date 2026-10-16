import std/[tables, options]

import ../[async_backend, errors, broker, cluster]
import ../protocol/messages
import ../internal/[backoff, cancel, retry, safe_logging]
import assignment

type
  GroupState* = enum
    ## Consumer group state machine; `tryTransition` is its only mutator and
    ## enforces the legal edges:
    ##
    ##   gsUnjoined    → gsJoining (joinAndSync starts)
    ##   gsJoining     → gsSyncing (join ok) | gsRebalancing (failure) | gsUnjoined
    ##   gsSyncing     → gsStable (sync ok) | gsRebalancing (failure) | gsUnjoined
    ##   gsStable      → gsRebalancing (heartbeat signal / session expired) |
    ##                   gsJoining (rejoin) | gsUnjoined (leave)
    ##   gsRebalancing → gsJoining (caller drives recovery) | gsUnjoined (leave)
    ##
    ## Failed (re)joins land in gsRebalancing, NOT gsUnjoined: the member has not
    ## left, so the heartbeat keeps running across the retry. gsUnjoined is only for
    ## the real "stop heartbeating" paths — leave(), max.poll.interval self-leave,
    ## a permanent heartbeat error, and a cancelled join.
    gsUnjoined
    gsJoining
    gsSyncing
    gsStable
    gsRebalancing

  ConsumerGroup* = ref object
    cluster*: Cluster
    groupId*: string
    memberId*: string
    generationId*: int32
    state*: GroupState
    coordinator*: Broker
    assignedPartitions*: seq[TopicPartition]
    topics*: seq[string]
    sessionTimeoutMs*: int32
    heartbeatIntervalMs*: int32
    heartbeatTimeoutMs: int32
    rebalanceTimeoutMs*: int32
    strategy*: AssignmentStrategy
    heartbeatFut: Future[void]
    rebalanceRequested*: bool
    maxCoordinatorRediscoverAttempts*: int
    onHeartbeatExit*: proc() {.gcsafe, raises: [].}
      ## Fired exactly once when `heartbeatLoop` exits on a permanent broker error
      ## (e.g. AUTHORIZATION_FAILED), letting the consumer stop the fetcher in
      ## lock-step so it does not fetch against a terminated session. Runs
      ## synchronously inside the heartbeat coroutine and MUST NOT block or re-enter
      ## the group; `asyncSpawn` heavier work.
    maxPollIntervalMs*: int32
      ## max.poll.interval.ms. When > 0, `heartbeatLoop` leaves the group if the
      ## application has not polled within the window, so the coordinator reassigns
      ## our partitions instead of letting a heartbeating-but-not-polling zombie
      ## stall the whole group. 0 disables it.
    lastPollAt: Moment
      ## Monotonic time of the most recent poll(); re-stamped when the heartbeat
      ## loop (re)starts. Written by `notePoll`; read by `heartbeatLoop` to enforce
      ## `maxPollIntervalMs`.
    pollIntervalExceeded*: bool
      ## Set by `heartbeatLoop` when it self-leaves on a poll-interval timeout. The
      ## next poll() raises once and clears it; membership is already reset and
      ## `rebalanceRequested` armed, so the poll after that rejoins.
    metadataMaxAgeMs*: int32
      ## metadata.max.age.ms (Java parity): cadence at which `heartbeatLoop`
      ## refreshes subscribed-topic metadata and arms a rebalance when a topic has
      ## gained partitions. 0 disables the check.
    knownPartitionCounts: Table[string, int]
      ## Partition count per subscribed topic as of the current assignment;
      ## `checkPartitionGrowth` compares the live count against it, and
      ## `captureKnownPartitionCounts` refreshes it on every (re)join so a
      ## growth-triggered rebalance re-baselines and does not re-fire.
    lastPartitionCheck: Moment
      ## Monotonic time of the last `checkPartitionGrowth` dispatch, spacing checks
      ## by `metadataMaxAgeMs`.
    partitionCheckFut: Future[void]
      ## The in-flight `checkPartitionGrowth` dispatched off `heartbeatLoop`. Tracked
      ## (not fire-and-forget) so `stopHeartbeat` can cancel and await it: no refresh
      ## coroutine outlives the group or touches a closing cluster.

  RediscoverResult* = enum
    rdSuccess
    rdFailed
    rdCancelled

proc newConsumerGroup*(
    cluster: Cluster,
    groupId: string,
    topics: seq[string],
    sessionTimeoutMs: int32 = 30000,
    heartbeatIntervalMs: int32 = 3000,
    heartbeatTimeoutMs: int32 = 0,
    rebalanceTimeoutMs: int32 = 60000,
    strategy: AssignmentStrategy = asRange,
    maxCoordinatorRediscoverAttempts: int = 3,
    maxPollIntervalMs: int32 = 300000,
    metadataMaxAgeMs: int32 = 300000,
): ConsumerGroup {.raises: [KafkaError].} =
  if sessionTimeoutMs <= 0:
    raise
      newException(KafkaError, "sessionTimeoutMs must be > 0, got " & $sessionTimeoutMs)
  if heartbeatIntervalMs <= 0:
    raise newException(
      KafkaError, "heartbeatIntervalMs must be > 0, got " & $heartbeatIntervalMs
    )
  if heartbeatIntervalMs >= sessionTimeoutMs:
    raise newException(
      KafkaError,
      "heartbeatIntervalMs (" & $heartbeatIntervalMs & ") must be < sessionTimeoutMs (" &
        $sessionTimeoutMs & ")",
    )
  if heartbeatTimeoutMs < 0:
    raise newException(
      KafkaError, "heartbeatTimeoutMs must be >= 0, got " & $heartbeatTimeoutMs
    )
  let effectiveHeartbeatTimeoutMs =
    if heartbeatTimeoutMs > 0:
      heartbeatTimeoutMs
    else:
      max(sessionTimeoutMs div 3, 1)
  if effectiveHeartbeatTimeoutMs > sessionTimeoutMs:
    raise newException(
      KafkaError,
      "heartbeatTimeoutMs (" & $effectiveHeartbeatTimeoutMs &
        ") must be <= sessionTimeoutMs (" & $sessionTimeoutMs & ")",
    )
  # JoinGroup body and SyncGroup wait both use this value; <= 0 would saturate the
  # broker-layer wait near int32.low or fall back to requestTimeoutMs (0 is already
  # mapped by withDefaults in newKafkaConsumer).
  if rebalanceTimeoutMs <= 0:
    raise newException(
      KafkaError, "rebalanceTimeoutMs must be > 0, got " & $rebalanceTimeoutMs
    )
  # The coordinator can expire the member on the session timer while it rejoins, so
  # the rebalance budget must cover the full session window.
  if rebalanceTimeoutMs < sessionTimeoutMs:
    raise newException(
      KafkaError,
      "rebalanceTimeoutMs (" & $rebalanceTimeoutMs & ") must be >= sessionTimeoutMs (" &
        $sessionTimeoutMs & ")",
    )
  # 0 is a documented off switch for both checks, but a negative interval would
  # silently disable them and hold the assignment forever.
  if maxPollIntervalMs < 0:
    raise newException(
      KafkaError, "maxPollIntervalMs must be >= 0, got " & $maxPollIntervalMs
    )
  if metadataMaxAgeMs < 0:
    raise newException(
      KafkaError, "metadataMaxAgeMs must be >= 0, got " & $metadataMaxAgeMs
    )
  ConsumerGroup(
    cluster: cluster,
    groupId: groupId,
    topics: topics,
    sessionTimeoutMs: sessionTimeoutMs,
    heartbeatIntervalMs: heartbeatIntervalMs,
    heartbeatTimeoutMs: effectiveHeartbeatTimeoutMs,
    rebalanceTimeoutMs: rebalanceTimeoutMs,
    strategy: strategy,
    state: gsUnjoined,
    memberId: "",
    generationId: -1,
    rebalanceRequested: false,
    maxCoordinatorRediscoverAttempts: maxCoordinatorRediscoverAttempts,
    maxPollIntervalMs: maxPollIntervalMs,
    lastPollAt: Moment.now(),
    pollIntervalExceeded: false,
    metadataMaxAgeMs: metadataMaxAgeMs,
    knownPartitionCounts: initTable[string, int](),
    lastPartitionCheck: Moment.now(),
  )

proc heartbeatTimeoutMs*(g: ConsumerGroup): int32 {.inline, raises: [].} =
  ## Effective per-heartbeat-request timeout selected at construction.
  g.heartbeatTimeoutMs

proc canTransition(src, dst: GroupState): bool =
  ## True when the src → dst edge is legal (see the GroupState table above).
  if src == dst:
    return true
  case src
  of gsUnjoined:
    dst == gsJoining
  of gsJoining:
    dst in {gsSyncing, gsUnjoined, gsRebalancing}
  of gsSyncing:
    dst in {gsStable, gsUnjoined, gsRebalancing}
  of gsStable:
    dst in {gsRebalancing, gsUnjoined, gsJoining}
  of gsRebalancing:
    dst in {gsJoining, gsUnjoined}

proc tryTransition*(g: ConsumerGroup, dst: GroupState): bool {.discardable.} =
  ## The single point of mutation for ConsumerGroup.state.
  if canTransition(g.state, dst):
    g.state = dst
    true
  else:
    false

template warnTransitionFailed(g: ConsumerGroup, dst: GroupState, context: string) =
  warn context & ": cannot transition to " & $dst & " from " & $g.state

proc snapshotMembership*(
    g: ConsumerGroup
): tuple[generationId: int32, memberId: string, state: GroupState] =
  ## Captures (generationId, memberId, state) together so one heartbeat iteration
  ## sees a consistent triple: chronos is single-threaded, but re-reading the fields
  ## later let a rebalance change memberId between the request build and its send.
  (g.generationId, g.memberId, g.state)

proc notePoll*(g: ConsumerGroup) {.raises: [].} =
  ## Stamp the latest poll() time for `maxPollIntervalMs`. Await-free, so it cannot
  ## interleave with the heartbeat read; called at the entry of every poll variant.
  g.lastPollAt = Moment.now()

proc resetMembership*(
    g: ConsumerGroup, memberId: string = "", generationId: int32 = -1
) =
  ## Resets membership after the broker invalidates it (UNKNOWN_MEMBER_ID,
  ## ILLEGAL_GENERATION) or on leave.
  g.memberId = memberId
  g.generationId = generationId

proc captureKnownPartitionCounts(g: ConsumerGroup) =
  ## Baseline each subscribed topic's partition count from the cluster cache so
  ## `checkPartitionGrowth` can detect later increases; refreshed on every (re)join
  ## so a growth-triggered rebalance re-baselines and does not re-fire.
  for topic in g.topics:
    let partsOpt = g.cluster.getTopicPartitionsCopy(topic)
    if partsOpt.isSome:
      g.knownPartitionCounts[topic] = partsOpt.get.len

proc checkPartitionGrowth*(g: ConsumerGroup) {.async: (raises: []).} =
  ## Refresh metadata and arm a rebalance if a subscribed topic gained partitions
  ## since the current assignment (Java parity: the consumer rejoins so they get
  ## assigned). Best-effort — failures are swallowed until the next cadence; runs
  ## spawned off heartbeatLoop so a slow refresh never delays a heartbeat.
  if g.topics.len == 0:
    return

  try:
    # force = true: fires on the metadata.max.age cadence and must read a fresh
    # view, not be debounced by an unrelated recent refresh.
    await g.cluster.refreshMetadata(some(g.topics), force = true)
  except KafkaError:
    return
  except CancelledError:
    return

  for topic in g.topics:
    let partsOpt = g.cluster.getTopicPartitionsCopy(topic)

    if partsOpt.isNone:
      continue

    let current = partsOpt.get.len
    let known = g.knownPartitionCounts.getOrDefault(topic, -1)
    if known < 0:
      # First observation: establish the baseline without a rebalance.
      g.knownPartitionCounts[topic] = current
    elif current > known:
      info "Subscribed topic '" & topic & "' grew from " & $known & " to " & $current &
        " partitions; requesting rebalance"
      g.knownPartitionCounts[topic] = current
      # Mirror signalRebalance: only a stable member flips state; an in-flight
      # rejoin owns the transition.
      if g.state == gsStable:
        if not g.tryTransition(gsRebalancing):
          warnTransitionFailed(g, gsRebalancing, "partition-growth rebalance")
      g.rebalanceRequested = true
    elif current < known:
      # Partition counts never shrink in Kafka; lower the baseline anyway so a later
      # regrowth is still detected.
      g.knownPartitionCounts[topic] = current

proc performAssignment(
    g: ConsumerGroup, joinResp: JoinGroupResponse
): Future[seq[SyncGroupAssignment]] {.async: (raises: [KafkaError, CancelledError]).} =
  var memberSubs: seq[MemberSubscription]
  for m in joinResp.members:
    let topics =
      try:
        decodeSubscription(m.metadata)
      except KafkaProtocolError as e:
        # One member's corrupt metadata must not take down the group: skip it (empty
        # assignment this round) so the rebalance keeps progressing — raising would
        # re-fail every JoinGroup until that member leaves.
        warn "Skipping member " & m.memberId & " with undecodable subscription: " & e.msg
        continue
    memberSubs.add(MemberSubscription(memberId: m.memberId, topics: topics))

  var allTopics: seq[string]
  for ms in memberSubs:
    for t in ms.topics:
      if t notin allTopics:
        allTopics.add(t)

  # force = true bypasses the debounce so a rebalance triggered by a partition-count
  # change sees the new partitions in the same round.
  await g.cluster.refreshMetadata(some(allTopics), force = true)

  var topicPartitions = initTable[string, seq[int32]]()
  for topic in allTopics:
    let partsOpt = g.cluster.getTopicPartitionsCopy(topic)
    if partsOpt.isSome:
      var partIds: seq[int32]
      for p in partsOpt.get:
        partIds.add(p.partitionIndex)
      topicPartitions[topic] = partIds
    else:
      # No metadata even after the refresh (topic missing / unauthorized /
      # propagation lag): no partitions, empty assignment for its subscribers — warn
      # so "0 records, no error" is diagnosable.
      warn "No metadata for subscribed topic '" & topic &
        "'; it will receive no partition assignment this round"

  # Use the strategy selected by the broker (from protocolName in JoinGroupResponse)
  let actualStrategy =
    case joinResp.protocolName
    of "range": asRange
    of "roundrobin": asRoundRobin
    else: g.strategy
  let assigned = assignPartitions(actualStrategy, memberSubs, topicPartitions)

  for ma in assigned:
    let assignmentBytes =
      try:
        encodeAssignment(ma.partitions)
      except KafkaProtocolError as e:
        raise newException(KafkaProtocolError, "failed to encode assignment: " & e.msg)
    result.add(SyncGroupAssignment(memberId: ma.memberId, assignment: assignmentBytes))

proc doSyncGroup(
    g: ConsumerGroup, joinResp: JoinGroupResponse
): Future[seq[TopicPartition]] {.async: (raises: [KafkaError, CancelledError]).} =
  if not g.tryTransition(gsSyncing):
    # The JoinGroup await above left us teardown-cancelled (gsUnjoined): syncing
    # anyway would install a membership the coordinator heartbeats for while the
    # local group does not — a zombie until session timeout.
    warnTransitionFailed(g, gsSyncing, "sync after join")
    raise newException(KafkaError, "cannot sync group in state: " & $g.state)
  # The transition must gate the membership write: stamping memberId/generationId
  # first would leave a zombie membership installed when it fails, defeating the
  # guard above.
  g.memberId = joinResp.memberId
  g.generationId = joinResp.generationId

  try:
    var assignments: seq[SyncGroupAssignment]
    if joinResp.leader == g.memberId:
      assignments = await performAssignment(g, joinResp)

    let syncReq = SyncGroupRequest(
      groupId: g.groupId,
      generationId: g.generationId,
      memberId: g.memberId,
      assignments: assignments,
    )
    let syncResp = await g.coordinator.syncGroup(syncReq, g.rebalanceTimeoutMs)
    if syncResp.errorCode != keNone:
      raise newKafkaApiError(
        syncResp.errorCode, "SyncGroup error: " & kafkaErrorName(syncResp.errorCode)
      )

    g.assignedPartitions =
      try:
        decodeAssignment(syncResp.assignment.get(@[]))
      except KafkaProtocolError as e:
        raise newException(KafkaProtocolError, "failed to decode assignment: " & e.msg)
    if not g.tryTransition(gsStable):
      # Teardown won while SyncGroup was in flight (gsUnjoined): the assignment is
      # moot and must not be applied against a dead membership.
      warnTransitionFailed(g, gsStable, "stable after sync")
      raise newException(KafkaError, "cannot reach stable state: " & $g.state)

    g.rebalanceRequested = false
    # Re-baseline the partition counts the growth check compares against.
    g.captureKnownPartitionCounts()
    info "Successfully joined group ",
      g.groupId, " with generation ", g.generationId, " (memberId ", g.memberId,
      "): assigned ", g.assignedPartitions.len, " partition(s)"
    result = g.assignedPartitions
  except KafkaError:
    # Still a member: gsRebalancing keeps the heartbeat loop alive across rejoin
    # retries.
    if not g.tryTransition(gsRebalancing):
      warnTransitionFailed(g, gsRebalancing, "rebalancing after sync error")
    raise
  except CancelledError:
    # Cancellation here means teardown (leave/close), which sets gsUnjoined itself
    # after cancelling the heartbeat — keep "cancelled = stopping".
    if not g.tryTransition(gsUnjoined):
      warnTransitionFailed(g, gsUnjoined, "unjoined after sync cancellation")
    raise

proc joinAndSync*(
    g: ConsumerGroup
): Future[seq[TopicPartition]] {.async: (raises: [KafkaError, CancelledError]).} =
  g.coordinator = await g.cluster.coordinatorBroker(g.groupId)
  if not g.tryTransition(gsJoining):
    raise newException(KafkaError, "cannot join group in state: " & $g.state)

  try:
    let subBytes =
      try:
        encodeSubscription(g.topics)
      except KafkaProtocolError as e:
        raise
          newException(KafkaProtocolError, "failed to encode subscription: " & e.msg)

    let preferredName = if g.strategy == asRange: "range" else: "roundrobin"
    let fallbackName = if g.strategy == asRange: "roundrobin" else: "range"

    let joinReq = JoinGroupRequest(
      groupId: g.groupId,
      sessionTimeoutMs: g.sessionTimeoutMs,
      rebalanceTimeoutMs: g.rebalanceTimeoutMs,
      memberId: g.memberId,
      protocolType: "consumer",
      protocols: @[
        JoinGroupProtocol(name: preferredName, metadata: subBytes),
        JoinGroupProtocol(name: fallbackName, metadata: subBytes),
      ],
    )

    var joinResp = await g.coordinator.joinGroup(joinReq)

    if joinResp.errorCode == keMemberIdRequired:
      g.memberId = joinResp.memberId
      let joinReq2 = JoinGroupRequest(
        groupId: g.groupId,
        sessionTimeoutMs: g.sessionTimeoutMs,
        rebalanceTimeoutMs: g.rebalanceTimeoutMs,
        memberId: g.memberId,
        protocolType: "consumer",
        protocols: joinReq.protocols,
      )
      joinResp = await g.coordinator.joinGroup(joinReq2)

    if joinResp.errorCode != keNone:
      raise newKafkaApiError(
        joinResp.errorCode, "JoinGroup error: " & kafkaErrorName(joinResp.errorCode)
      )

    result = await doSyncGroup(g, joinResp)
  except KafkaError as e:
    # UNKNOWN_MEMBER_ID / ILLEGAL_GENERATION mean the broker invalidated our
    # membership: reset here so every caller of this public API re-joins with an
    # empty memberId (Java's ConsumerCoordinator owns the same reset), not a rejected
    # one.
    if e of KafkaApiError:
      let apiErr = (ref KafkaApiError)(e)
      if apiErr.errorCode == keUnknownMemberId or apiErr.errorCode == keIllegalGeneration:
        g.resetMembership()
    # Still a member: gsRebalancing keeps the heartbeat loop alive across rejoin
    # retries.
    if not g.tryTransition(gsRebalancing):
      warnTransitionFailed(g, gsRebalancing, "rebalancing after join error")
    raise
  except CancelledError:
    # Cancellation here means teardown (leave/close), which sets gsUnjoined itself
    # after cancelling the heartbeat — keep "cancelled = stopping".
    if not g.tryTransition(gsUnjoined):
      warnTransitionFailed(g, gsUnjoined, "unjoined after join cancellation")
    raise

proc rediscoverCoordinator*(
    g: ConsumerGroup
): Future[RediscoverResult] {.async: (raises: []).} =
  ## Try to re-discover the group coordinator with retry and backoff.
  g.cluster.invalidateCoordinator(g.groupId)
  for attempt in 1 .. g.maxCoordinatorRediscoverAttempts:
    try:
      g.coordinator = await g.cluster.coordinatorBroker(g.groupId)
      return rdSuccess
    except CancelledError:
      return rdCancelled
    except KafkaError as e:
      warn "Coordinator re-discovery attempt ",
        attempt, "/", g.maxCoordinatorRediscoverAttempts, " failed: ", e.msg
      if attempt < g.maxCoordinatorRediscoverAttempts:
        try:
          await sleepAsync(milliseconds(jitteredBackoff(500'i32, attempt)))
        except CancelledError:
          return rdCancelled

  return rdFailed

proc pollIntervalExpired*(
    maxPollIntervalMs: int32, lastPollAt, now: Moment
): bool {.raises: [].} =
  ## True when max.poll.interval.ms enforcement fires: enabled and more than that
  ## many ms since the last poll. Split out for unit-testing the time math.
  maxPollIntervalMs > 0 and (now - lastPollAt) > milliseconds(maxPollIntervalMs)

proc sessionTimerExpired*(
    sessionTimeoutMs: int32, lastSuccessfulHeartbeat, now: Moment
): bool {.raises: [].} =
  ## True when the coordinator has very likely expired our session: more than
  ## sessionTimeoutMs since the last proof of liveness (successful heartbeat or fresh
  ## rejoin). Unlike the consecutive-failure counter (reset on every rediscovery),
  ## this wall-clock bound still fires when heartbeats keep failing transiently.
  sessionTimeoutMs > 0 and
    (now - lastSuccessfulHeartbeat) > milliseconds(sessionTimeoutMs)

proc heartbeatResponseIsCurrent*(
    snapState: GroupState, snapGeneration, liveGeneration: int32
): bool {.raises: [].} =
  ## True when a rebalance-required heartbeat response still pertains to the
  ## membership it was sent for: the group was stable at snapshot AND no rejoin has
  ## advanced the generation since.
  ##
  ## The loop keeps running across a rejoin, so a rejoin can complete (fresh
  ## generation/memberId, back to gsStable) while a response for the OLD generation
  ## is still awaited; acting on it would clobber the fresh membership and force a
  ## spurious rejoin. Generations are monotonic, so equality means no rejoin since
  ## the snapshot.
  snapState == gsStable and snapGeneration == liveGeneration

proc heartbeatLoop(g: ConsumerGroup) {.async: (raises: []).} =
  ## Send periodic heartbeats across every joined state (gsStable/gsRebalancing/
  ## gsJoining/gsSyncing) so the broker keeps the session alive while a rejoin is in
  ## flight — stopping would risk a sessionTimeoutMs eviction on a slow rejoin. Only
  ## a heartbeat issued while the snapshot was gsStable may flip into gsRebalancing.
  var consecutiveFailures = 0
  let maxRetries = max(1, g.sessionTimeoutMs div g.heartbeatIntervalMs - 1)

  # Wall-clock session liveness, independent of consecutiveFailures (which a
  # flapping coordinator resets). Refreshed on success and on each fresh generation.
  var lastSuccessfulHeartbeat = Moment.now()
  var lastStableGeneration = g.generationId

  template signalRebalance(snap: untyped) =
    if snap.state == gsStable:
      if not g.tryTransition(gsRebalancing):
        # The group already left (gsUnjoined) since the snapshot: the rejoin owns
        # the transition — skip the flip and the flag.
        warnTransitionFailed(g, gsRebalancing, "heartbeat rebalance signal")
      else:
        g.rebalanceRequested = true
    consecutiveFailures = 0

  while g.state != gsUnjoined:
    try:
      await sleepAsync(milliseconds(g.heartbeatIntervalMs))
    except CancelledError:
      return

    let snap = g.snapshotMembership()
    if snap.state == gsUnjoined:
      return

    # Enforce max.poll.interval.ms (Java parity), only while stable: if the app has
    # not polled, LeaveGroup so the coordinator reassigns our partitions, then arm a
    # rejoin and stop heartbeating. Must NOT call leave()/stopHeartbeat() here —
    # this IS the heartbeat coroutine, so cancelling our own future would deadlock.
    if snap.state == gsStable and
        pollIntervalExpired(g.maxPollIntervalMs, g.lastPollAt, Moment.now()):
      warn "max.poll.interval.ms (" & $g.maxPollIntervalMs &
        "ms) exceeded; leaving group " & g.groupId & " so its partitions can " &
        "be reassigned"

      if g.coordinator != nil and g.memberId != "":
        let leaveReq = LeaveGroupRequest(groupId: g.groupId, memberId: g.memberId)
        try:
          discard await g.coordinator.leaveGroup(leaveReq)
        except KafkaError:
          discard
        except CancelledError:
          return

      if not g.tryTransition(gsUnjoined):
        # A rejoin advanced the state while LeaveGroup was in flight: keep
        # heartbeating so the in-flight rejoin's session stays alive.
        warnTransitionFailed(g, gsUnjoined, "max.poll.interval self-leave")
        continue
      g.resetMembership()
      g.pollIntervalExceeded = true
      g.rebalanceRequested = true
      # Unlike the permanent-error exit (which calls onHeartbeatExit), self-leave
      # leaves the fetcher alone: rebalanceRequested already pauses fetching, and the
      # next poll surfaces pollIntervalExceeded once, then rejoins under a fresh
      # generation — old-generation records are dropped by the poll-side stale
      # filter.
      return

    if snap.state == gsStable and snap.generationId != lastStableGeneration:
      # A completed rejoin (new stable generation) re-confirms our membership, so it
      # counts as a successful heartbeat — otherwise a slow rejoin could re-fire the
      # session check right after recovery.
      lastStableGeneration = snap.generationId
      lastSuccessfulHeartbeat = Moment.now()

    if snap.state == gsStable and g.metadataMaxAgeMs > 0 and
        (Moment.now() - g.lastPartitionCheck) >= milliseconds(g.metadataMaxAgeMs) and
        (g.partitionCheckFut == nil or g.partitionCheckFut.finished):
      # Periodically check for topic growth and arm a rebalance (Java parity),
      # spawned off this loop so a slow refresh never delays heartbeats; the cadence
      # stamp is set before dispatch and the finished-guard keeps at most one check
      # per metadata.max.age window.
      g.lastPartitionCheck = Moment.now()
      g.partitionCheckFut = g.checkPartitionGrowth()

    if snap.state == gsStable and
        sessionTimerExpired(g.sessionTimeoutMs, lastSuccessfulHeartbeat, Moment.now()):
      # Enforce session.timeout.ms by wall clock: consecutiveFailures never trips
      # when rediscovery keeps succeeding between failing heartbeats, yet the broker
      # has already expired the session and reassigned our partitions. Arm a
      # rebalance so the next poll revokes and rejoins (stable only — a rejoin owns
      # it otherwise).
      warn "session.timeout.ms (" & $g.sessionTimeoutMs &
        "ms) elapsed with no successful heartbeat for group " & g.groupId &
        "; the coordinator has likely reassigned our partitions — rejoining"

      signalRebalance(snap)
      continue

    let req = HeartbeatRequest(
      groupId: g.groupId, generationId: snap.generationId, memberId: snap.memberId
    )

    try:
      let heartbeatTimeout = milliseconds(g.heartbeatTimeoutMs)
      let resp = await g.coordinator.heartbeat(req).wait(heartbeatTimeout)
      if resp.errorCode == keNone:
        consecutiveFailures = 0
        lastSuccessfulHeartbeat = Moment.now()
      elif isRebalanceRequired(resp.errorCode):
        # A rejoin already in progress means the broker tells us what we know, and a
        # rejoin that completed during this await makes the response stale: ignore it
        # so it cannot clobber the fresh membership (see heartbeatResponseIsCurrent).
        if heartbeatResponseIsCurrent(snap.state, snap.generationId, g.generationId):
          if resp.errorCode == keIllegalGeneration or resp.errorCode == keUnknownMemberId:
            g.resetMembership()
          signalRebalance(snap)
      elif isRetriable(resp.errorCode):
        consecutiveFailures.inc
        if isCoordinatorMoved(resp.errorCode):
          let rd = await g.rediscoverCoordinator()

          case rd
          of rdCancelled:
            return
          of rdSuccess:
            consecutiveFailures = 0
            continue
          of rdFailed:
            warn "Coordinator rediscovery exhausted after ",
              kafkaErrorName(resp.errorCode), "; triggering rebalance"
            signalRebalance(snap)
            continue

        warn "Heartbeat retriable error (attempt ",
          consecutiveFailures, "/", maxRetries, "): ", kafkaErrorName(resp.errorCode)
        if consecutiveFailures > maxRetries:
          signalRebalance(snap)
      else:
        # Neither success, retriable, nor a rebalance signal: permanent/fatal plus
        # unrecognized codes. A permanent error won't recover, so heartbeating stops
        # for good — one of the reserved gsUnjoined paths above. `rebalanceRequested`
        # stays armed so the next poll attempts a rejoin that fails with the same
        # code and surfaces it via handleRebalanceIfNeeded; onHeartbeatExit stops the
        # fetcher.
        if resp.errorCode.isInstanceFatal():
          if not g.tryTransition(gsUnjoined):
            warnTransitionFailed(g, gsUnjoined, "permanent heartbeat error")
          g.rebalanceRequested = true
          if g.onHeartbeatExit != nil:
            g.onHeartbeatExit()
          return

        # A non-fatal unknown code must NOT arm a rebalance (a needless revoke/rejoin
        # would drain the un-polled queue); log and keep the session alive.
        warn "Heartbeat returned unhandled non-fatal code " &
          kafkaErrorName(resp.errorCode) & "; continuing to heartbeat"
        continue
    except AsyncTimeoutError:
      consecutiveFailures.inc
      warn "Heartbeat timed out (attempt ", consecutiveFailures, "/", maxRetries, ")"
      let rd = await g.rediscoverCoordinator()
      case rd
      of rdCancelled:
        return
      of rdSuccess:
        consecutiveFailures = 0
        continue
      of rdFailed:
        warn "Coordinator rediscovery exhausted after heartbeat timeout; triggering rebalance"
        signalRebalance(snap)
        continue
    except KafkaError as e:
      consecutiveFailures.inc
      warn "Heartbeat error (attempt ",
        consecutiveFailures, "/", maxRetries, "): ", e.msg
      let rd = await g.rediscoverCoordinator()
      case rd
      of rdCancelled:
        return
      of rdSuccess:
        consecutiveFailures = 0
        continue
      of rdFailed:
        warn "Coordinator rediscovery exhausted after heartbeat error; triggering rebalance"
        signalRebalance(snap)
        continue
    except CancelledError:
      return

proc startHeartbeat*(g: ConsumerGroup) =
  if g.heartbeatFut != nil and not g.heartbeatFut.finished:
    return
  # Re-stamp the poll clock when the heartbeat loop (re)starts, so a slow app that
  # has not polled since the previous generation is not immediately kicked.
  g.lastPollAt = Moment.now()
  g.heartbeatFut = heartbeatLoop(g)

proc stopHeartbeat*(g: ConsumerGroup) {.async: (raises: []).} =
  # Both reaps absorb a cancellation instead of unwinding: this runs from
  # `close()`, which never re-runs its teardown, so a cancel landing here would
  # leave the consumer in `lsClosing` with a loop still running against a cluster
  # being closed. Each future is already scheduled for cancellation and never
  # fails, so waiting again cannot raise and cannot lose the cancel.
  if g.heartbeatFut != nil and not g.heartbeatFut.finished:
    let beatFut = g.heartbeatFut
    while not beatFut.finished:
      try:
        await beatFut.cancelAndWait()
      except CancelledError:
        absorbDuringTeardown(
          "group::stopHeartbeat", "cancel reaping the heartbeat loop"
        )
    g.heartbeatFut = nil
  # Reclaim a checkPartitionGrowth dispatched off the stopped loop so it cannot
  # refresh metadata against a cluster being closed.
  if g.partitionCheckFut != nil and not g.partitionCheckFut.finished:
    let checkFut = g.partitionCheckFut
    while not checkFut.finished:
      try:
        await checkFut.cancelAndWait()
      except CancelledError:
        absorbDuringTeardown(
          "group::stopHeartbeat", "cancel reaping the partition check"
        )
  g.partitionCheckFut = nil

proc leave*(g: ConsumerGroup) {.async: (raises: []).} =
  await g.stopHeartbeat()

  if g.coordinator != nil and g.memberId != "":
    let req = LeaveGroupRequest(groupId: g.groupId, memberId: g.memberId)
    try:
      discard await g.coordinator.leaveGroup(req)
    except KafkaError:
      absorbDuringTeardown("group::leave", "LeaveGroup rejected")
    except CancelledError:
      absorbDuringTeardown("group::leave", "cancel waiting for LeaveGroup")

  if not g.tryTransition(gsUnjoined):
    warnTransitionFailed(g, gsUnjoined, "leave")
  g.resetMembership()
  g.assignedPartitions = @[]
