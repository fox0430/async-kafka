import std/[algorithm, tables, sets, options, strutils, sysrand]

import async_backend, errors, broker, connection, sasl
import internal/[backoff, cancel, metrics, safe_logging]
import protocol/messages

export metrics.MetricsHook

## Locking model
## -------------
## `stateLock` serializes metadata refreshes; other mutations (coordinator
## invalidation, broker pruning) rely on chronos being single-threaded: mutations
## between await points are atomic.
##
## Never iterate a live table across an await — `for k in c.brokers: ... await ...`
## invalidates the iterator if another coroutine mutates it. Use the snapshot
## accessors below, which return a copy the caller owns.
##
## Caches and constructor-selected settings are private: production code must use
## the public lifecycle/snapshot API; tests reach private fields via `{.all.}` +
## `privateAccess`.

type
  CoordinatorKind* = enum
    ## Coordinator role a request targets; `toWire` below is the only place it
    ## crosses to the FindCoordinator v1+ wire int8 (0=group, 1=transaction).
    ckGroup
    ckTxn

  PartitionLeader* = object
    ## Cached leader for a TopicPartition. Below MetadataResponse v7 the wire
    ## carries no `leaderEpoch` (KIP-320 added it), so the field keeps its Nim
    ## default 0 rather than a known epoch; callers send it in
    ## FetchRequest.currentLeaderEpoch / OffsetCommitRequest.committedLeaderEpoch
    ## so the broker fences stale views.
    nodeId*: int32
    leaderEpoch*: int32

  Cluster* = ref object
    ## Pooled broker connections, discovery, version negotiation and the metadata
    ## cache. Configuration is fixed at construction and never mutated through this
    ## handle; shared by producers and consumers — close it after they are closed.
    bootstrapBrokers: seq[tuple[host: string, port: int]]
    clientId: string
    tlsConfig: TlsConfig
    hostAllowlist: HostAllowlist
      ## Cluster-wide policy: every broker address a metadata response advertises
      ## must match before a socket opens, plaintext included. `nil` disables it.
    saslMechanism: SaslMechanism
    saslMechanisms: seq[SaslMechanism]
    brokers: Table[int32, Broker]
    topicPartitions: Table[string, seq[PartitionMeta]]
    partitionLeaders: Table[TopicPartition, PartitionLeader]
    coordinators: Table[string, int32]
    txnCoordinators: Table[string, int32]
    metadataMaxAge: Duration
    lastMetadataRefresh: Moment
    stateLock: AsyncLock
    state: LifecycleState
    allowAutoTopicCreation: bool
      ## When true, metadata requests set the v4+ allowAutoTopicCreation flag so
      ## the broker materializes referenced topics; false by default so a routine
      ## refresh cannot silently create a mistyped topic.
    metadataDebounceInterval: Duration
    pendingRefreshes: Table[string, Future[void]]
      ## Per-topic RPC coalescing for `invalidateLeader`: joiners share one
      ## MetadataRequest instead of stacking N forced refreshes.
    pendingCoordinatorLookups: Table[string, Future[void]]
      ## Per-(kind, key) RPC coalescing for `findCoordinatorBroker`, keyed by
      ## `"g:" & groupId` / `"t:" & transactionalId`.
    reconnectBackoffMs: int32
    reconnectBackoffMaxMs: int32
    connectTimeoutMs: int32
    requestTimeoutMs: int32
      ## Per-request timeout. Non-positive values wait (almost) indefinitely,
      ## bounded by the connection layer's ten-minute ceiling.
    metadataRefreshTimeoutMs: int32
      ## Wall-clock cap for one refresh pass, bootstrap fallbacks included: the
      ## positive value when set, else 3x the effective per-request timeout.
    metricsHook: MetricsHook
      ## Optional metrics callback, propagated to every Broker it constructs.

let neverRefreshed = Moment.init(0'i64, nanoseconds(1))

func saturatingMultiplyNonNegative(value, factor: int64): int64 =
  ## `value` is a resolved timeout (`effectiveTimeoutMs` never returns a negative)
  ## and `factor` is a literal at every call site, so the guard below cannot fire.
  ## It saturates to 0 instead of raising because one caller, `pruneStaleBrokers`,
  ## is deliberately `{.raises: [].}`.
  if value <= 0 or factor <= 0:
    return 0
  if value > int64.high div factor:
    int64.high
  else:
    value * factor

proc metadataRefreshTimeoutBudgetMs(c: Cluster): int64 =
  ## A positive override wins; else 3x the effective per-request timeout.
  if c.metadataRefreshTimeoutMs > 0:
    c.metadataRefreshTimeoutMs.int64
  else:
    saturatingMultiplyNonNegative(effectiveTimeoutMs(c.requestTimeoutMs).int64, 3'i64)

proc parsePort(
    addr_str: string, portStr: string
): int {.raises: [KafkaConnectionError].} =
  ## Ports are 1..65535. Range-check before the later `int32` narrowing so a wrap
  ## cannot silently dial a different port (e.g. 2^32+9092 → 9092).
  let port =
    try:
      parseInt(portStr)
    except ValueError:
      raise newException(KafkaConnectionError, "invalid port in address: " & addr_str)
  if port < 1 or port > 65535:
    raise newException(KafkaConnectionError, "invalid port in address: " & addr_str)
  port

proc parseAddress(
    addr_str: string
): tuple[host: string, port: int] {.raises: [KafkaConnectionError].} =
  if addr_str.startsWith("["):
    let closeBracket = addr_str.find(']')
    if closeBracket < 0 or closeBracket + 2 >= addr_str.len or
        addr_str[closeBracket + 1] != ':':
      raise newException(
        KafkaConnectionError,
        "invalid IPv6 bootstrap address: " & addr_str & " (expected [host]:port)",
      )
    let host = addr_str[1 ..< closeBracket]
    result = (host: host, port: parsePort(addr_str, addr_str[closeBracket + 2 .. ^1]))
  else:
    let colonPos = addr_str.rfind(':')
    if colonPos < 0:
      raise newException(
        KafkaConnectionError,
        "invalid bootstrap address: " & addr_str & " (expected host:port)",
      )
    let host = addr_str[0 ..< colonPos]
    result = (host: host, port: parsePort(addr_str, addr_str[colonPos + 1 .. ^1]))

proc newCluster*(
    bootstrapAddrs: seq[string],
    clientId: string = "async-kafka",
    metadataMaxAge: Duration = 5.minutes,
    tlsConfig: TlsConfig = nil,
    hostAllowlist: HostAllowlist = nil,
    saslMechanism: SaslMechanism = nil,
    reconnectBackoffMs: int32 = 50,
    reconnectBackoffMaxMs: int32 = 10_000,
    connectTimeoutMs: int32 = 30_000,
    requestTimeoutMs: int32 = 30_000,
    metadataDebounceInterval: Duration = seconds(1),
    metadataRefreshTimeoutMs: int32 = 0,
    metricsHook: MetricsHook = nil,
    saslMechanisms: seq[SaslMechanism] = @[],
    allowAutoTopicCreation: bool = false,
): Cluster {.raises: [KafkaError].} =
  ## Create a cluster handle over `bootstrapAddrs` (`host:port` strings). Configuration
  ## is validated eagerly (raising `KafkaError`); connections are lazy. Release with
  ## `close`.
  ##
  ## `hostAllowlist` bounds which broker addresses may be dialed (see `HostAllowlist` /
  ## `newHostAllowlist`): an advertised name is resolved first and every answer checked,
  ## so it cannot reach past the rules, on plaintext connections as well as TLS. `nil`
  ## (default) disables the check.
  ##
  ## Timeouts and backoff: `metadataMaxAge` must be positive — it is how long cached
  ## metadata counts as fresh, not a caching switch. `metadataDebounceInterval` coalesces
  ## the refresh requests that arrive inside the window, and 0 refreshes immediately. One
  ## refresh pass is capped by `metadataRefreshTimeoutMs` when positive, else by three
  ## times `requestTimeoutMs`. `connectTimeoutMs` bounds the TCP connect and the TLS
  ## handshake; `requestTimeoutMs` bounds one request/response exchange, and a
  ## non-positive value waits until the connection layer's ten-minute ceiling. Reconnects
  ## back off from `reconnectBackoffMs`, doubling up to `reconnectBackoffMaxMs`.
  ##
  ## Transport: `tlsConfig` (`nil` = plaintext) covers every connection. `saslMechanism`
  ## (singular) authenticates every connection with that one mechanism, while
  ## `saslMechanisms` (plural) offers its entries in order, retrying the next candidate on
  ## a fresh connection when the broker answers UNSUPPORTED_SASL_MECHANISM; the plural
  ## wins when both are given, and neither runs a SASL handshake. KIP-368 re-authentication
  ## is not implemented: the broker-advertised `sessionLifetimeMs` is ignored, so a broker
  ## with `connections.max.reauth.ms > 0` closes the connection at the session boundary and
  ## the next request pays a fresh connect and handshake. `clientId` is the name
  ## the broker sees in request headers and logs. `allowAutoTopicCreation` (false by
  ## default) lets a metadata request materialize the topics it names, so a routine
  ## refresh cannot create a mistyped one. `metricsHook` receives cluster and
  ## per-connection events; `nil` makes each emit a no-op.
  ##
  ## A broker advertised by host name is resolved with a synchronous resolver call
  ## (chronos has no asynchronous resolver), so a slow lookup stalls the event loop for
  ## its duration.
  # Validate up front: a negative or out-of-order backoff would produce a degenerate
  # delay, and KafkaError makes every config-time failure catchable as one type.
  if bootstrapAddrs.len == 0:
    raise newException(
      KafkaError, "bootstrapAddrs must contain at least one host:port address"
    )
  validateBrokerConfig(saslMechanisms, reconnectBackoffMs, reconnectBackoffMaxMs)
  if saslMechanism != nil:
    validateMechanism(saslMechanism)
  # Non-positive TTL makes the "metadata is fresh" fast path never succeed, so every
  # lookup pays a broker round-trip; 0 is not a caching off switch.
  if metadataMaxAge <= milliseconds(0):
    raise newException(KafkaError, "metadataMaxAge must be > 0, got " & $metadataMaxAge)
  # 0 is meaningful (no debounce window); a negative one makes the comparison always
  # false and is almost certainly a mistake.
  if metadataDebounceInterval < milliseconds(0):
    raise newException(
      KafkaError,
      "metadataDebounceInterval must be >= 0, got " & $metadataDebounceInterval,
    )

  var addrs: seq[tuple[host: string, port: int]]
  for a in bootstrapAddrs:
    addrs.add(parseAddress(a))

  Cluster(
    bootstrapBrokers: addrs,
    clientId: clientId,
    tlsConfig: tlsConfig,
    hostAllowlist: hostAllowlist,
    saslMechanism: saslMechanism,
    saslMechanisms: saslMechanisms,
    brokers: initTable[int32, Broker](),
    topicPartitions: initTable[string, seq[PartitionMeta]](),
    partitionLeaders: initTable[TopicPartition, PartitionLeader](),
    coordinators: initTable[string, int32](),
    txnCoordinators: initTable[string, int32](),
    metadataMaxAge: metadataMaxAge,
    lastMetadataRefresh: neverRefreshed,
    stateLock: newAsyncLock(),
    state: lsRunning,
    metadataDebounceInterval: metadataDebounceInterval,
    pendingRefreshes: initTable[string, Future[void]](),
    pendingCoordinatorLookups: initTable[string, Future[void]](),
    reconnectBackoffMs: reconnectBackoffMs,
    reconnectBackoffMaxMs: reconnectBackoffMaxMs,
    connectTimeoutMs: connectTimeoutMs,
    requestTimeoutMs: requestTimeoutMs,
    metadataRefreshTimeoutMs: metadataRefreshTimeoutMs,
    metricsHook: metricsHook,
    allowAutoTopicCreation: allowAutoTopicCreation,
  )

proc isClosed*(c: Cluster): bool =
  ## True once close has started or finished.
  c.state.isClosingOrClosed

proc state*(c: Cluster): LifecycleState =
  ## Current lifecycle state: `lsRunning` from construction (connections are lazy, so
  ## there is no start step), `lsClosing` while `close` runs, `lsClosed` after it.
  c.state

template requireRunning(c: Cluster) =
  if c.state != lsRunning:
    raise newException(KafkaClosedError, "cluster is closed")

# Snapshot accessors: each returns a synchronous copy of the underlying table/seq,
# so callers can iterate it across awaits while a concurrent refresh mutates the
# live tables.

proc snapshotBrokers*(c: Cluster): seq[Broker] {.raises: [].} =
  ## Stable copy of the current broker list.
  result = newSeqOfCap[Broker](c.brokers.len)
  for _, b in c.brokers:
    result.add(b)

proc getBrokerById*(c: Cluster, nodeId: int32): Option[Broker] {.raises: [].} =
  c.brokers.withValue(nodeId, b):
    return some(b[])
  none(Broker)

proc getPartitionLeader*(
    c: Cluster, tp: TopicPartition
): Option[PartitionLeader] {.raises: [].} =
  ## Cached (nodeId, leaderEpoch) for `tp`, or none if not yet refreshed;
  ## `leaderEpoch` stays 0 below MetadataResponse v7, where KIP-320 added the field.
  c.partitionLeaders.withValue(tp, leader):
    return some(leader[])

proc getPartitionLeaderId*(
    c: Cluster, tp: TopicPartition
): Option[int32] {.raises: [].} =
  ## Leader broker id only; thin wrapper over `getPartitionLeader`.
  let leaderOpt = c.getPartitionLeader(tp)
  if leaderOpt.isSome:
    return some(leaderOpt.get.nodeId)

proc applyPartitionLeaderMonotonic(
    c: Cluster,
    tp: TopicPartition,
    candidate: PartitionLeader,
    prev: Option[PartitionLeader],
    topic: string,
) {.raises: [].} =
  ## Writes `candidate` into `partitionLeaders[tp]` unless `prev` has a strictly
  ## higher leaderEpoch, in which case the prior (nodeId, epoch) is kept; -1 is
  ## "unknown" (pre-KIP-320) and never wins. Emits a metric on a stale rejection.
  var nodeId = candidate.nodeId
  var leaderEpoch = candidate.leaderEpoch
  if prev.isSome and prev.get.leaderEpoch >= 0 and leaderEpoch < prev.get.leaderEpoch:
    let partStr = $tp.partition
    let staleStr = $leaderEpoch
    let keptStr = $prev.get.leaderEpoch
    emitMetric(
      c.metricsHook,
      meClusterMetadataStaleLeaderEpoch,
      [
        (cstring"topic", cstring(topic)),
        (cstring"partition", cstring(partStr)),
        (cstring"stale", cstring(staleStr)),
        (cstring"kept", cstring(keptStr)),
      ],
      1,
    )
    debug "Ignoring stale leader epoch for ",
      topic,
      "/",
      $tp.partition,
      ": got ",
      $staleStr,
      " but kept epoch ",
      $keptStr,
      " on node ",
      $nodeId
    nodeId = prev.get.nodeId
    leaderEpoch = prev.get.leaderEpoch
  c.partitionLeaders[tp] = PartitionLeader(nodeId: nodeId, leaderEpoch: leaderEpoch)

proc getCachedCoordinator*(
    c: Cluster, key: string, txn: bool = false
): Option[int32] {.raises: [].} =
  if txn:
    c.txnCoordinators.withValue(key, nid):
      return some(nid[])
  else:
    c.coordinators.withValue(key, nid):
      return some(nid[])

proc getTopicPartitionsCopy*(
    c: Cluster, topic: string
): Option[seq[PartitionMeta]] {.raises: [].} =
  ## Stable copy of the partition metadata for `topic`, or none if not cached.
  c.topicPartitions.withValue(topic, parts):
    return some(parts[])

proc getOrCreateBroker(
    c: Cluster, nodeId: int32, host: string, port: int32
): Broker {.raises: [KafkaError].} =
  c.brokers.withValue(nodeId, existing):
    if existing[].host != host or existing[].port != port:
      # Address moved: detach the old conn so in-flight requests fail rather than
      # wait on whatever broker still answers at the old host:port.
      existing[].detachConn()
      existing[].host = host
      existing[].port = port
      existing[].markAddressDirty()
    # Active again: clear any stale-since prune marker.
    existing[].setFirstSkippedPruneAt(Moment.init(0'i64, nanoseconds(1)))
    return existing[]

  let b = newBroker(
    nodeId,
    host,
    port,
    c.clientId,
    c.tlsConfig,
    c.hostAllowlist,
    c.saslMechanism,
    c.reconnectBackoffMs,
    c.reconnectBackoffMaxMs,
    c.connectTimeoutMs,
    c.requestTimeoutMs,
    metricsHook = c.metricsHook,
    saslMechanisms = c.saslMechanisms,
  )
  c.brokers[nodeId] = b
  result = b

proc pruneStaleBrokers*(
    c: Cluster, activeNodeIds: HashSet[int32]
) {.async: (raises: []).} =
  ## Prunes brokers absent from metadata, but skips any with callers still awaiting
  ## a response (closing them would cancel in-flight requests against a reference
  ## the producer/consumer still holds); such a broker stays registered until
  ## in-flight drains and the next refresh reassesses.
  ##
  ## Backstop for a request that never completes (buggy peer, bad timeout): once a
  ## broker is stale longer than stalePruneGraceMs (2x the per-request timeout, so a
  ## normally-progressing request always wins) it is force-closed. No hard floor:
  ## lowering requestTimeoutMs deliberately tightens this grace too.
  ##
  ## A caller parked in `awaitThrottle` (waiting out a quota throttle) is NOT
  ## in-flight: the wait precedes the write and the `inFlightCount` bump, so its
  ## broker counts as idle and is retired at once. That parked call then fails with
  ## a connection error — nothing was written, so retrying is safe.
  let now = Moment.now()
  let stalePruneGraceMs =
    saturatingMultiplyNonNegative(effectiveTimeoutMs(c.requestTimeoutMs).int64, 2'i64)
  let unsetMoment = Moment.init(0'i64, nanoseconds(1))
  var staleIds: seq[int32]

  for nodeId in c.brokers.keys:
    if nodeId notin activeNodeIds:
      staleIds.add(nodeId)

  for nodeId in staleIds:
    var victim: Broker = nil
    var forced = false
    c.brokers.withValue(nodeId, b):
      # `isHandshaking` covers the uncounted ApiVersions negotiation
      # (conn.sendRequest directly, no inFlightCount bump): treat it like pending
      # requests, skip this cycle.
      #
      # "Wedgeable" = a response-awaiting request or an unfinished handshake — the only
      # states the force-prune backstop may reclaim, since a stuck response-waiter
      # resurfaces through its caller's retry loop. acks=0 writes are NOT wedgeable (no
      # response, no retry, so a force-close drops the record): patient skip path only.
      let wedgeable = b[].inFlightRequests > 0 or b[].isHandshaking
      let busy = wedgeable or b[].fireAndForgetInFlight > 0
      if not busy:
        victim = b[]
      elif not wedgeable:
        # Busy only with fire-and-forget writes: keep this cycle and let them
        # drain. Don't stamp firstSkippedPruneAt — there's no wedge clock.
        discard
      else:
        if b[].firstSkippedPruneAt == unsetMoment:
          b[].setFirstSkippedPruneAt(now)
        elif (now - b[].firstSkippedPruneAt).milliseconds >= stalePruneGraceMs and
            b[].latestInFlightDeadline <= now:
          # In-flight hung past the grace window AND every widened-timeout deadline
          # past: assume wedged and reclaim — the caller surfaces the failure through
          # its retry loop. The tracked deadline keeps a legitimately waiting JoinGroup
          # / SyncGroup (rebalanceTimeoutMs + requestTimeoutMs) alive.
          victim = b[]
          forced = true

    if victim != nil:
      # Remove from the registry before awaiting close() so a concurrent caller
      # cannot fish out a half-closed broker during the close await.
      c.brokers.del(nodeId)
      if forced:
        let nidStr = $nodeId
        info "Pruning broker ",
          nidStr, " (stale past the grace window with requests in flight)"
        emitMetric(
          c.metricsHook,
          meClusterBrokerForcePruned,
          [(cstring"nodeId", cstring(nidStr))],
          1,
        )
      # retire(), not close(): a caller that resolved this broker before the prune must
      # not revive a registry-less, leaked connection via ensureConnected. The retired
      # flag is set synchronously, before the close await, so a racing connect sees it.
      await victim.retire()

  var staleCoords: seq[string]

  for key, nid in c.coordinators:
    if nid notin activeNodeIds:
      staleCoords.add(key)

  for key in staleCoords:
    c.coordinators.del(key)

  var staleTxnCoords: seq[string]
  for key, nid in c.txnCoordinators:
    if nid notin activeNodeIds:
      staleTxnCoords.add(key)
  for key in staleTxnCoords:
    c.txnCoordinators.del(key)

const maxCandidateCauseLen = 200

const
  coordinatorLookupMaxAttempts = 3
    ## Attempts against the broker that answered FindCoordinator with a retriable
    ## error code (COORDINATOR_LOAD_IN_PROGRESS while __consumer_offsets loads, a
    ## coordinator election in flight). One attempt is the original RPC.
  coordinatorLookupBackoffMs = 100'i32
    ## Base delay between those attempts, jittered and escalated. Matches Java's
    ## `retry.backoff.ms` default; the point is to ride out a coordinator becoming
    ## ready rather than failing `start()` on a transient answer.

type
  CandidateFailure = object
    endpoint: string
    err: ref KafkaError

  CandidateFailures = object
    ## Per-candidate failures of a fallback sweep (metadata refresh, coordinator
    ## lookup), so the aggregate error names the most diagnostic cause instead of
    ## leaving a wrong SASL password, an unreachable port and a TLS refusal alike.
    last: CandidateFailure
    answered: CandidateFailure
      ## First candidate the broker itself rejected (`KafkaApiError`): a broker that
      ## answered is more diagnostic than one that never did, so it outranks `last`.

proc oneLineBounded(s: string, maxLen = maxCandidateCauseLen): string {.raises: [].} =
  ## Single-line, length-bounded copy of `s`: error text can embed broker-controlled
  ## bytes that must not forge log lines or inflate the errors a caller surfaces.
  for ch in s:
    if ch < ' ' or ch == '\x7f':
      result.add(' ')
    else:
      result.add(ch)
  if result.len > maxLen:
    result.setLen(maxLen)
    result.add("...")

proc recordCandidateFailure(
    f: var CandidateFailures, host: string, port: int32, e: ref KafkaError
) {.raises: [].} =
  f.last = CandidateFailure(endpoint: host & ":" & $port, err: e)
  if f.answered.err == nil and e of KafkaApiError:
    f.answered = f.last

proc renderCandidateCause(f: CandidateFailures): string {.raises: [].} =
  ## `host:port: LABEL: detail` for the most diagnostic recorded failure; LABEL is
  ## the broker's error name and code when one answered, else the client-side tag.
  let c = if f.answered.err != nil: f.answered else: f.last
  if c.err == nil:
    return ""
  result =
    if c.err of KafkaApiError:
      let code = (ref KafkaApiError)(c.err).errorCode
      c.endpoint & ": " & $code & " (" & $int16(code) & "): "
    else:
      c.endpoint & ": " & kafkaErrorTag(c.err) & ": "
  result.add(oneLineBounded(c.err.msg))

proc withCandidateCause(base, cause: string): string {.raises: [].} =
  ## Append the candidate cause; kept in one place so the warn line and the
  ## exception a sweep raises stay identical.
  if cause.len == 0:
    base
  else:
    base & " (cause: " & cause & ")"

proc refreshMetadataUnlocked(
    c: Cluster, topics: Option[seq[string]] = none(seq[string])
) {.async: (raises: [KafkaError, CancelledError]).} =
  requireRunning(c)
  var resp: MetadataResponse
  var success = false
  var failures: CandidateFailures

  # Snapshot before iterating: the fetch awaits and a parallel refresh must not
  # invalidate the iterator.
  let knownBrokers = c.snapshotBrokers()
  for b in knownBrokers:
    try:
      resp = await b.metadata(topics, c.allowAutoTopicCreation)
      success = true
      break
    except KafkaError as e:
      # Per-candidate counterpart to cluster.metadata.refresh.failed, which does not
      # say which broker died. CancelledError is not a candidate skip, so no emit.
      debug "Metadata request to ", b.host, ":", $b.port, " failed: ", e.msg
      failures.recordCandidateFailure(b.host, b.port, e)
      let portStr = $b.port
      emitMetric(
        c.metricsHook,
        meClusterMetadataRefreshAttemptFailed,
        [(cstring"host", cstring(b.host)), (cstring"port", cstring(portStr))],
        1,
      )
      continue

  var tempBroker: Broker = nil
  if not success:
    for addrTuple in c.bootstrapBrokers:
      let b = newBroker(
        -1,
        addrTuple.host,
        int32(addrTuple.port),
        c.clientId,
        c.tlsConfig,
        c.hostAllowlist,
        c.saslMechanism,
        c.reconnectBackoffMs,
        c.reconnectBackoffMaxMs,
        c.connectTimeoutMs,
        c.requestTimeoutMs,
        metricsHook = c.metricsHook,
        saslMechanisms = c.saslMechanisms,
      )
      try:
        resp = await b.metadata(topics, c.allowAutoTopicCreation)
        success = true
        tempBroker = b
        break
      except KafkaError as e:
        debug "Metadata request to bootstrap ",
          addrTuple.host, ":", $addrTuple.port, " failed: ", e.msg
        failures.recordCandidateFailure(addrTuple.host, int32(addrTuple.port), e)
        let portStr = $addrTuple.port
        emitMetric(
          c.metricsHook,
          meClusterMetadataRefreshAttemptFailed,
          [(cstring"host", cstring(addrTuple.host)), (cstring"port", cstring(portStr))],
          1,
        )
        await b.close()
        continue
      except CancelledError:
        # Shield the teardown from the incoming cancellation so the socket does not leak.
        await noCancel(b.close())
        raise

  if not success:
    let cause = renderCandidateCause(failures)
    warn withCandidateCause(
      "Metadata refresh failed: no broker answered (topics: " &
        (if topics.isSome: $topics.get else: "<all>") & ")",
      cause,
    )
    emitMetric(
      c.metricsHook,
      meClusterMetadataRefreshFailed,
      [(cstring"reason", cstring"no_broker")],
      1,
    )
    raise newException(
      KafkaConnectionError,
      withCandidateCause("failed to fetch metadata from any broker", cause),
    )

  # Degenerate response: Kafka always populates the brokers list (we just talked to one),
  # so an empty one means misconfiguration or corruption — applying it would retire every
  # known broker and orphan the topic leaders. Refuse it; the caller retries next tick.
  if resp.brokers.len == 0:
    warn "Metadata refresh rejected: the response listed no brokers (refusing to " &
      "retire every known broker)"
    if tempBroker != nil:
      await noCancel(tempBroker.close())
    emitMetric(
      c.metricsHook,
      meClusterMetadataRefreshFailed,
      [(cstring"reason", cstring"empty_brokers")],
      1,
    )
    raise newException(
      KafkaProtocolError, "MetadataResponse contained an empty brokers list"
    )

  # Adopt the bootstrap connection when it matches a real broker in the response,
  # avoiding a redundant TCP reconnect on cold start.
  var adopted = false
  var retiredNodeIds: HashSet[int32]
  # `noCancel` shields the apply phase from `refreshMetadata`'s `withTimeout`:
  # cancelling mid-apply would leave the caches half-updated (e.g. a pruned nodeId
  # still referenced from partitionLeaders → "leader broker N not known").
  try:
    for bm in resp.brokers:
      if not adopted and tempBroker != nil and tempBroker.isConnected and
          tempBroker.host == bm.host and tempBroker.port == bm.port and
          bm.nodeId notin c.brokers:
        tempBroker.nodeId = bm.nodeId
        c.brokers[bm.nodeId] = tempBroker
        adopted = true
      else:
        discard c.getOrCreateBroker(bm.nodeId, bm.host, bm.port)

    if topics.isNone:
      var activeIds: HashSet[int32]
      for bm in resp.brokers:
        activeIds.incl(bm.nodeId)
      # Snapshot broker keys before prune: the prevLeaders restore below skips
      # nodeIds this refresh retired.
      var beforePrune: HashSet[int32]
      for nodeId in c.brokers.keys:
        beforePrune.incl(nodeId)
      await noCancel(c.pruneStaleBrokers(activeIds))
      for nodeId in beforePrune:
        if nodeId notin c.brokers:
          retiredNodeIds.incl(nodeId)
  finally:
    if tempBroker != nil and not adopted:
      await noCancel(tempBroker.close())

  # KIP-320 leaderEpoch monotonicity: snapshot existing (nodeId, epoch) pairs so the
  # write-back loop can reject a backwards move. Without it, a stale response during
  # a leader election would make later commits/fetches send a smaller
  # currentLeaderEpoch (fenced) or route to a stale leader unfenced.
  var respTopicNames: HashSet[string]
  if topics.isSome:
    for tm in resp.topics:
      respTopicNames.incl(tm.name)

  var prevLeaders: Table[TopicPartition, PartitionLeader]
  for tp, leader in c.partitionLeaders:
    if topics.isNone or tp.topic in respTopicNames:
      prevLeaders[tp] = leader

  if topics.isNone:
    c.partitionLeaders.clear()
  else:
    for tm in resp.topics:
      var toRemove: seq[TopicPartition]
      for tp in c.partitionLeaders.keys:
        if tp.topic == tm.name:
          toRemove.add(tp)
      for tp in toRemove:
        c.partitionLeaders.del(tp)

  for tm in resp.topics:
    if tm.errorCode != keNone:
      # Topic-level error (e.g. LEADER_NOT_AVAILABLE during an election): drop the
      # stale `topicPartitions` entry, but restore partitionLeaders from
      # `prevLeaders` so the KIP-320 (nodeId, leaderEpoch) baseline survives —
      # otherwise the next commit/fetch sends committedLeaderEpoch=-1 (unfenced).
      c.topicPartitions.del(tm.name)
      for tp, leader in prevLeaders:
        # Skip pruned leader nodeIds: restoring them would leave
        # brokerForPartition raising "leader broker N not known".
        if tp.topic == tm.name and leader.nodeId notin retiredNodeIds:
          c.partitionLeaders[tp] = leader
      continue
    # Sort by partitionIndex: MetadataResponse does not guarantee ordering, but the
    # key-hash partitioner indexes this list as `parts[hash mod partCount]`, so an
    # unstable order would map the same key to different partitions across refreshes
    # and differently from Java's `toPositive(murmur2) % count`.
    var sortedPartitions = tm.partitions
    sortedPartitions.sort(
      proc(a, b: PartitionMeta): int =
        cmp(a.partitionIndex, b.partitionIndex)
    )
    c.topicPartitions[tm.name] = sortedPartitions
    for pm in sortedPartitions:
      let tp = TopicPartition(topic: tm.name, partition: pm.partitionIndex)
      if pm.errorCode != keNone or pm.leaderId < 0:
        # No usable leader now: a transient per-partition error, or keNone with
        # leaderId<0 (a mid-election gap some brokers report without a code). Caching
        # -1 is worse than nothing — getPartitionLeader would return some(-1),
        # brokerForPartition would skip the forced refresh and raise "leader broker -1
        # not known". Restore the prior (nodeId, leaderEpoch) if known (keeping the
        # KIP-320 baseline), else leave it absent so the next lookup forces a refresh.
        prevLeaders.withValue(tp, p):
          if p[].nodeId notin retiredNodeIds:
            c.partitionLeaders[tp] = p[]
        continue
      let candidate = PartitionLeader(nodeId: pm.leaderId, leaderEpoch: pm.leaderEpoch)
      var prev = none(PartitionLeader)
      prevLeaders.withValue(tp, p):
        prev = some(p[])
      c.applyPartitionLeaderMonotonic(tp, candidate, prev, tm.name)

  if topics.isNone:
    # A full refresh returns every topic, so a cached topic absent from the response
    # was deleted upstream; its partitionLeaders were already cleared above.
    var presentTopics: HashSet[string]
    for tm in resp.topics:
      presentTopics.incl(tm.name)
    var deletedTopics: seq[string]
    for name in c.topicPartitions.keys:
      if name notin presentTopics:
        deletedTopics.add(name)
    for name in deletedTopics:
      c.topicPartitions.del(name)

  c.lastMetadataRefresh = Moment.now()
  let brokersStr = $c.brokers.len
  let topicsStr = $c.topicPartitions.len
  emitMetric(
    c.metricsHook,
    meClusterMetadataRefresh,
    [(cstring"brokers", cstring(brokersStr)), (cstring"topics", cstring(topicsStr))],
    1,
  )

proc refreshMetadata*(
    c: Cluster, topics: Option[seq[string]] = none(seq[string]), force: bool = false
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## `force = true` bypasses the debounce so a caller needing a fresh view (e.g.
  ## `brokerForPartition` with no leader) is not no-op'd by a recent refresh.
  await c.stateLock.acquire()

  defer:
    c.stateLock.releaseSafely("cluster.refreshMetadata")

  let now = Moment.now()
  if not force and c.lastMetadataRefresh != neverRefreshed and
      (now - c.lastMetadataRefresh) < c.metadataDebounceInterval:
    return

  let overallTimeoutMs = c.metadataRefreshTimeoutBudgetMs()
  let innerFut = c.refreshMetadataUnlocked(topics)
  let completed = await withTimeout(innerFut, milliseconds(overallTimeoutMs))

  if not completed:
    await innerFut.cancelAndWait()
    raise newException(
      KafkaTimeoutError, "metadata refresh timed out after " & $overallTimeoutMs & "ms"
    )
  # withTimeout reports only whether innerFut finished, not its exception: re-await
  # so a connection/API error from the refresh surfaces to the caller.
  await innerFut

proc maybeRefreshMetadata*(
    c: Cluster, topics: Option[seq[string]] = none(seq[string])
): Future[bool] {.async: (raises: [KafkaError, CancelledError]).} =
  ## True when this call drove a refresh; false when metadata was already fresh.
  # Lock-free fast path: skip the refresh if metadata is fresh. It can still go stale
  # between this check and the send, so on a response `isStaleLeaderCode` accepts, call
  # `invalidateLeader(tp, code)` — it classifies, drops the leader and coalesces a
  # refresh — rather than re-implementing the sequence; for a stale coordinator use
  # `invalidateCoordinator` / `invalidateTxnCoordinator`.
  let now = Moment.now()
  if c.lastMetadataRefresh != neverRefreshed and
      (now - c.lastMetadataRefresh) <= c.metadataMaxAge:
    return false

  await c.stateLock.acquire()

  defer:
    c.stateLock.releaseSafely("cluster.maybeRefreshMetadata")

  # Re-check under the lock: another caller may have refreshed while we queued.
  let nowInner = Moment.now()
  if c.lastMetadataRefresh != neverRefreshed and
      (nowInner - c.lastMetadataRefresh) <= c.metadataMaxAge:
    return false
  let overallTimeoutMs = c.metadataRefreshTimeoutBudgetMs()
  let innerFut = c.refreshMetadataUnlocked(topics)
  let completed = await withTimeout(innerFut, milliseconds(overallTimeoutMs))
  if not completed:
    await innerFut.cancelAndWait()
    raise newException(
      KafkaTimeoutError, "metadata refresh timed out after " & $overallTimeoutMs & "ms"
    )
  # Surface inner failures — see refreshMetadata.
  await innerFut

  return true

proc brokerForPartition*(
    c: Cluster, tp: TopicPartition
): Future[Broker] {.async: (raises: [KafkaError, CancelledError]).} =
  let refreshed = await c.maybeRefreshMetadata(some(@[tp.topic]))

  var leaderOpt = c.getPartitionLeader(tp)
  if leaderOpt.isNone:
    if not refreshed:
      # maybeRefresh was skipped on TTL: force past the debounce, else a plain
      # refreshMetadata could itself be debounced and leave the leader unresolved.
      await c.refreshMetadata(some(@[tp.topic]), force = true)
    leaderOpt = c.getPartitionLeader(tp)
    if leaderOpt.isNone:
      raise newException(
        KafkaError, "no leader found for " & tp.topic & "-" & $tp.partition
      )

  let leaderId = leaderOpt.get.nodeId
  let brokerOpt = c.getBrokerById(leaderId)
  if brokerOpt.isNone:
    raise newException(KafkaError, "leader broker " & $leaderId & " not known")
  result = brokerOpt.get

proc isStaleLeaderCode*(code: KafkaErrorCode): bool {.raises: [].} =
  ## Per-partition codes that only recover after a metadata refresh: stale leader
  ## view, KIP-320 fenced/unknown epoch, and the transient leader/broker
  ## not-available codes of an election (without a refresh we would keep routing to
  ## the cached nodeId until the TTL expires).
  code == keNotLeaderOrFollower or code == keUnknownTopicOrPartition or
    code == keFencedLeaderEpoch or code == keUnknownLeaderEpoch or
    code == keLeaderNotAvailable or code == keBrokerNotAvailable

proc joinSharedSlot(
    slot: Future[void], failureLabel: string
): Future[void] {.async: (raises: [KafkaError, CancelledError]).} =
  # `await slot` sets the caller's `internalChild`, so chronos' `tryCancel` walks that
  # chain: one joiner's cancel would cancel the shared slot and spuriously fail every
  # other joiner. Interpose a per-joiner proxy so only the joiner detaches.
  let localFut = newInternalRaisesFuture[void, (KafkaError, CancelledError)](
    "cluster.joinSharedSlot"
  )
  proc onSlot(udata: pointer) {.gcsafe, raises: [].} =
    if localFut.finished:
      return
    if slot.cancelled or (slot.failed and slot.error of CancelledError):
      localFut.cancelAndSchedule()
    elif slot.failed:
      let e = slot.error
      if e of KafkaError:
        localFut.fail((ref KafkaError)(e))
      else:
        localFut.fail(newException(KafkaError, failureLabel & ": " & e.msg))
    else:
      localFut.complete()

  if slot.finished:
    onSlot(nil)
  else:
    slot.addCallback(onSlot)

  try:
    await localFut
  except CancelledError as e:
    if not slot.finished:
      slot.removeCallback(onSlot)
    raise e

proc refreshRunner(
    c: Cluster, slot: Future[void], topic: string
) {.async: (raises: []).} =
  # Detached from awaiters so one caller's cancel doesn't kill the refresh for the
  # other joiners, which await via `joinSharedSlot`.
  var err: ref CatchableError = nil
  try:
    await c.refreshMetadata(some(@[topic]), force = true)
  except KafkaError as e:
    err = e
  except CancelledError as e:
    # Wrap runner-side cancel as KafkaError before slot.fail so joiners see an internal
    # failure, not a spurious CancelledError they never invoked.
    err = newException(KafkaError, "coalesced metadata refresh cancelled: " & e.msg)

  # Settle before del(): a late joiner between complete() and del() finds the
  # settled slot instead of firing a second RPC.
  if not slot.finished:
    if err == nil:
      slot.complete()
    else:
      slot.fail(err)

  var owned = false
  c.pendingRefreshes.withValue(topic, existing):
    if existing[] == slot:
      owned = true
  if owned:
    c.pendingRefreshes.del(topic)

proc invalidateLeader*(
    c: Cluster, tp: TopicPartition, code: KafkaErrorCode
): Future[bool] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Sole owner of "stale response → invalidate cached leader → refresh"; returns
  ## true iff `code` was stale and a refresh was driven. Concurrent calls for one topic
  ## share an RPC via `pendingRefreshes` — do not open-code this at call sites.
  if not isStaleLeaderCode(code):
    return false

  var slot: Future[void] = nil
  c.pendingRefreshes.withValue(tp.topic, existing):
    if not existing[].finished:
      slot = existing[]
    elif existing[].completed:
      # A previous refresh already succeeded (its runner hasn't del'd the entry yet),
      # so the leader it wrote is fresh: leave it and don't drive a duplicate refresh,
      # which would also delete that leader below.
      return true
    # else: finished but failed/cancelled — retry below.

  # Delete the leader only once a refresh is committed to: deleting first would
  # discard a fresh leader a just-finished refresh wrote.
  c.partitionLeaders.del(tp)

  if slot == nil:
    slot = newFuture[void]("cluster.invalidateLeader")
    c.pendingRefreshes[tp.topic] = slot
    asyncSpawn c.refreshRunner(slot, tp.topic)

  await joinSharedSlot(slot, "coalesced refresh failed")
  return true

proc getTopicPartitionsRefreshing*(
    c: Cluster, topic: string
): Future[tuple[parts: seq[PartitionMeta], refreshed: bool]] {.
    async: (raises: [KafkaError, CancelledError])
.} =
  ## Partitions for `topic`, refreshing on cache miss. `refreshed` is true when a refresh
  ## actually ran (TTL check or forced refresh), so callers can drop partition-count-
  ## sensitive state such as a sticky pick that no longer fits. Raises if the topic is
  ## still unknown.
  var refreshed = await c.maybeRefreshMetadata(some(@[topic]))
  var partsOpt = c.getTopicPartitionsCopy(topic)
  if partsOpt.isNone:
    await c.refreshMetadata(some(@[topic]), force = true)
    refreshed = true
    partsOpt = c.getTopicPartitionsCopy(topic)
    if partsOpt.isNone:
      raise newException(KafkaError, "unknown topic: " & topic)
  return (partsOpt.get, refreshed)

proc invalidateCoordinator*(c: Cluster, groupId: string) {.raises: [].} =
  ## Clear cached coordinator for a group, forcing re-discovery on next lookup.
  if c.coordinators.hasKey(groupId):
    debug "Invalidating coordinator cache for group: " & groupId
    c.coordinators.del(groupId)

proc invalidateTxnCoordinator*(c: Cluster, transactionalId: string) {.raises: [].} =
  ## Clear cached transaction coordinator, forcing re-discovery on next lookup.
  if c.txnCoordinators.hasKey(transactionalId):
    debug "Invalidating txn coordinator cache for: " & transactionalId
    c.txnCoordinators.del(transactionalId)

proc toWire(kind: CoordinatorKind): int8 {.inline, raises: [].} =
  # The single place CoordinatorKind crosses to the FindCoordinator v1 wire int8
  # (0=group, 1=transaction): a new coordinator role only lands here.
  case kind
  of ckGroup: 0'i8
  of ckTxn: 1'i8

proc findCoordinatorRunner(
    c: Cluster,
    slot: Future[void],
    key: string,
    kind: CoordinatorKind,
    coalesceKey: string,
) {.async: (raises: []).} =
  # Detached from awaiters so one caller's cancel doesn't kill the RPC for the other
  # joiners (mirrors `refreshRunner`).
  var err: ref CatchableError = nil
  try:
    var coordResp: FindCoordinatorResponse
    var found = false
    var tempBroker: Broker = nil
    var answeringBroker: Broker = nil
    var failures: CandidateFailures
    let candidates = c.snapshotBrokers()
    for b in candidates:
      try:
        await b.connect()
        coordResp = await b.findCoordinator(key, kind.toWire)
        found = true
        answeringBroker = b
        break
      except KafkaError as e:
        # Per-candidate counterpart to a silent continue: joiners see only the final
        # KafkaError, which names this candidate's cause. CancelledError is not a
        # candidate skip.
        debug "Coordinator lookup for ",
          key, " via ", b.host, ":", $b.port, " failed: ", e.msg
        failures.recordCandidateFailure(b.host, b.port, e)
        let portStr = $b.port
        emitMetric(
          c.metricsHook,
          meClusterCoordinatorLookupAttemptFailed,
          [(cstring"host", cstring(b.host)), (cstring"port", cstring(portStr))],
          1,
        )
        continue

    # Fall back to bootstrap addresses (mirroring refreshMetadataUnlocked): a stale
    # registry whose brokers are all unreachable must still locate a coordinator
    # (e.g. broker restarts shuffled the registry while bootstrap stayed up).
    if not found:
      for addrTuple in c.bootstrapBrokers:
        let b = newBroker(
          -1,
          addrTuple.host,
          int32(addrTuple.port),
          c.clientId,
          c.tlsConfig,
          c.hostAllowlist,
          c.saslMechanism,
          c.reconnectBackoffMs,
          c.reconnectBackoffMaxMs,
          c.connectTimeoutMs,
          c.requestTimeoutMs,
          metricsHook = c.metricsHook,
          saslMechanisms = c.saslMechanisms,
        )
        try:
          await b.connect()
          coordResp = await b.findCoordinator(key, kind.toWire)
          found = true
          tempBroker = b
          answeringBroker = b
          break
        except KafkaError as e:
          failures.recordCandidateFailure(addrTuple.host, int32(addrTuple.port), e)
          let portStr = $addrTuple.port
          emitMetric(
            c.metricsHook,
            meClusterCoordinatorLookupAttemptFailed,
            [
              (cstring"host", cstring(addrTuple.host)),
              (cstring"port", cstring(portStr)),
            ],
            1,
          )
          await b.close()
          continue
        except CancelledError:
          await b.close()
          raise

    if not found:
      # Whole-lookup counterpart to attempt.failed; CancelledError never reaches this
      # raise, and a FindCoordinator errorCode != keNone is handled later.
      let cause = renderCandidateCause(failures)
      warn withCandidateCause(
        "Coordinator lookup failed: no broker answered for " & key, cause
      )
      emitMetric(
        c.metricsHook,
        meClusterCoordinatorLookupFailed,
        [(cstring"reason", cstring"no_broker")],
        1,
      )
      let label =
        case kind
        of ckGroup: "group"
        of ckTxn: "transaction"
      raise newException(
        KafkaError,
        withCandidateCause(
          "failed to find " & label & " coordinator for: " & key, cause
        ),
      )

    # A retriable error code here means the coordinator is not ready yet, not that
    # the lookup is wrong: COORDINATOR_LOAD_IN_PROGRESS while __consumer_offsets
    # loads, or a coordinator election still settling. Ride it out on the broker
    # that answered instead of failing start()/join on a transient answer (Java
    # retries these with retry.backoff.ms).
    var attempt = 1
    while coordResp.errorCode != keNone and attempt < coordinatorLookupMaxAttempts and
        coordResp.errorCode.isRetriable and answeringBroker != nil:
      inc attempt
      let delayMs = jitteredBackoff(coordinatorLookupBackoffMs, attempt - 1)
      warn "Coordinator lookup for ",
        key,
        " not ready (",
        kafkaErrorName(coordResp.errorCode),
        "); retrying in ",
        delayMs,
        "ms"
      emitMetric(
        c.metricsHook,
        meClusterCoordinatorLookupFailed,
        [(cstring"reason", cstring"not_ready")],
        1,
      )
      try:
        await sleepAsync(milliseconds(delayMs))
      except CancelledError:
        raise
      coordResp = await answeringBroker.findCoordinator(key, kind.toWire)

    if coordResp.errorCode != keNone:
      if tempBroker != nil:
        await noCancel(tempBroker.close())
      # RPC succeeded; not a candidate skip. Same event as no_broker, mirroring
      # refresh.failed / empty_brokers.
      warn "Coordinator lookup for ",
        key, " rejected: ", kafkaErrorName(coordResp.errorCode)
      emitMetric(
        c.metricsHook,
        meClusterCoordinatorLookupFailed,
        [(cstring"reason", cstring"api_error")],
        1,
      )
      raise newKafkaApiError(
        coordResp.errorCode,
        "FindCoordinator error: " & kafkaErrorName(coordResp.errorCode),
      )

    # Serialize the cache write against close(): otherwise a runner resuming after
    # close() cleared the tables would resurrect entries nothing can reach to close —
    # a zombie coordinator connection outliving the cluster.
    var adopted = false
    var stateLocked = false
    try:
      await c.stateLock.acquire()
      stateLocked = true
    except CancelledError as ce:
      err =
        newException(KafkaError, "coalesced coordinator lookup cancelled: " & ce.msg)
    if stateLocked:
      try:
        if c.state == lsRunning:
          if tempBroker != nil and tempBroker.isConnected and
              tempBroker.host == coordResp.host and tempBroker.port == coordResp.port and
              coordResp.nodeId notin c.brokers:
            # Adopt the bootstrap connection when it is the coordinator itself
            # (mirrors refreshMetadataUnlocked): no redundant cold-start reconnect.
            tempBroker.nodeId = coordResp.nodeId
            c.brokers[coordResp.nodeId] = tempBroker
            adopted = true
          discard c.getOrCreateBroker(coordResp.nodeId, coordResp.host, coordResp.port)
          case kind
          of ckGroup:
            c.coordinators[key] = coordResp.nodeId
          of ckTxn:
            c.txnCoordinators[key] = coordResp.nodeId
        else:
          err = (ref KafkaClosedError)(msg: "cluster is closed")
      finally:
        c.stateLock.releaseSafely("cluster.findCoordinator")
    if tempBroker != nil and not adopted:
      await noCancel(tempBroker.close())
  except KafkaError as e:
    err = e
  except CancelledError as e:
    err = newException(KafkaError, "coalesced coordinator lookup cancelled: " & e.msg)

  # Settle before del(): a late joiner between complete() and del() finds the
  # settled slot instead of firing a second RPC.
  if not slot.finished:
    if err == nil:
      slot.complete()
    else:
      slot.fail(err)

  var owned = false
  c.pendingCoordinatorLookups.withValue(coalesceKey, existing):
    if existing[] == slot:
      owned = true
  if owned:
    c.pendingCoordinatorLookups.del(coalesceKey)

proc findCoordinatorBroker(
    c: Cluster, key: string, kind: CoordinatorKind
): Future[Broker] {.async: (raises: [KafkaError, CancelledError]).} =
  requireRunning(c)

  proc invalidate(c: Cluster, key: string, kind: CoordinatorKind) {.raises: [].} =
    case kind
    of ckGroup:
      c.invalidateCoordinator(key)
    of ckTxn:
      c.invalidateTxnCoordinator(key)

  let cached = c.getCachedCoordinator(key, txn = kind == ckTxn)
  if cached.isSome:
    let cachedNodeId = cached.get
    let brokerOpt = c.getBrokerById(cachedNodeId)
    if brokerOpt.isSome:
      let b = brokerOpt.get
      if b != nil and b.isConnected and not b.isAddressDirty():
        return b
      if b != nil:
        try:
          # connect() honours addressDirty: a coordinator whose address changed is
          # redialed rather than returned stale.
          await b.connect()
          return b
        except KafkaError:
          discard
    c.invalidate(key, kind)

  discard await c.maybeRefreshMetadata()

  # Coalesce: N concurrent lookups for the same (kind, key) share one FindCoordinator
  # RPC, else a cold-started fleet stampedes the bootstrap broker.
  let coalesceKey =
    case kind
    of ckGroup:
      "g:" & key
    of ckTxn:
      "t:" & key

  var slot: Future[void] = nil
  c.pendingCoordinatorLookups.withValue(coalesceKey, existing):
    if not existing[].finished:
      slot = existing[]

  if slot == nil:
    slot = newFuture[void]("cluster.findCoordinatorBroker")
    c.pendingCoordinatorLookups[coalesceKey] = slot
    asyncSpawn c.findCoordinatorRunner(slot, key, kind, coalesceKey)

  await joinSharedSlot(slot, "coalesced coordinator lookup failed")

  let cachedAfter = c.getCachedCoordinator(key, txn = kind == ckTxn)
  if cachedAfter.isNone:
    raise newException(KafkaError, "coordinator lookup returned no result for: " & key)
  let coordBrokerOpt = c.getBrokerById(cachedAfter.get)
  if coordBrokerOpt.isNone:
    raise
      newException(KafkaError, "coordinator broker " & $cachedAfter.get & " not known")
  let coordBroker = coordBrokerOpt.get
  try:
    await coordBroker.connect()
  except KafkaError:
    c.invalidate(key, kind)
    raise
  info "Resolved ",
    (if kind == ckTxn: "transaction coordinator" else: "group coordinator"),
    " for ",
    key,
    ": broker ",
    coordBroker.host,
    ":",
    coordBroker.port
  result = coordBroker

proc coordinatorBroker*(
    c: Cluster, groupId: string
): Future[Broker] {.async: (raises: [KafkaError, CancelledError]).} =
  await c.findCoordinatorBroker(groupId, ckGroup)

proc txnCoordinatorBroker*(
    c: Cluster, transactionalId: string
): Future[Broker] {.async: (raises: [KafkaError, CancelledError]).} =
  await c.findCoordinatorBroker(transactionalId, ckTxn)

proc anyBroker*(
    c: Cluster
): Future[Broker] {.async: (raises: [KafkaError, CancelledError]).} =
  requireRunning(c)

  discard await c.maybeRefreshMetadata()

  var brokerList = c.snapshotBrokers()
  if brokerList.len > 0:
    # Random start index so load isn't always on the first broker.
    var startIdx = 0
    if brokerList.len > 1:
      var buf: array[2, byte]
      if urandom(buf):
        startIdx = (int(buf[0]) or (int(buf[1]) shl 8)) mod brokerList.len
    for i in 0 ..< brokerList.len:
      let b = brokerList[(startIdx + i) mod brokerList.len]
      try:
        await b.connect()
        return b
      except KafkaError:
        continue

  # All known brokers failed or the list is empty: force a refresh (bypassing the TTL)
  # so it falls back to bootstrap. force = true is required — a debounced refresh
  # would no-op and surface "no broker available" without trying bootstrap.
  try:
    await c.refreshMetadata(force = true)
  except KafkaError as e:
    # The refresh already names the failed candidate and why; flattening it to a bare
    # "no broker available" would erase the SASL-vs-port distinction.
    raise newException(
      KafkaConnectionError, "no broker available: " & oneLineBounded(e.msg)
    )

  brokerList = c.snapshotBrokers()

  var failures: CandidateFailures
  for b in brokerList:
    try:
      await b.connect()
      return b
    except KafkaError as e:
      failures.recordCandidateFailure(b.host, b.port, e)
      continue

  raise newException(
    KafkaConnectionError,
    withCandidateCause("no broker available", renderCandidateCause(failures)),
  )

proc close*(c: Cluster) {.async: (raises: []).} =
  ## Retire/close every broker, clear the tables, and end in lsClosed.
  if c.state == lsClosed:
    return
  # Serialize teardown against an in-flight refresh (which holds stateLock across its
  # awaits): a refresh parked in the bootstrap fallback could resume after the tables are
  # cleared and re-populate c.brokers via adoption, leaking a socket + readLoop past this
  # close(). The lock also fail-fasts later refreshes on requireRunning().
  var locked = false
  try:
    await c.stateLock.acquire()
    locked = true
  except CancelledError:
    absorbDuringTeardown(
      "cluster::close", "stateLock acquire cancelled; tearing down unlocked"
    )
  defer:
    if locked:
      c.stateLock.releaseSafely("cluster.close")

  if c.state == lsClosed:
    return # a racing close() finished while we waited for the lock

  c.state = lsClosing
  var closeFuts: seq[Future[void]]
  for b in c.snapshotBrokers():
    # retire() marks each broker before closing, so a coroutine racing teardown (e.g.
    # findCoordinatorBroker connecting on a pre-close snapshot) fast-fails in
    # connectImpl instead of reopening a connection the cleared registry can't close.
    closeFuts.add(b.retire())
  if closeFuts.len > 0:
    try:
      let completed = await withTimeout(allFutures(closeFuts), seconds(10))
      if not completed:
        warn "Cluster close timed out; cancelling remaining broker connections"
        for fut in closeFuts:
          if not fut.finished:
            fut.cancelSoon()
    except CancelledError:
      for fut in closeFuts:
        if not fut.finished:
          fut.cancelSoon()

  c.brokers.clear()
  c.topicPartitions.clear()
  c.partitionLeaders.clear()
  c.coordinators.clear()
  c.txnCoordinators.clear()
  for k, slot in c.pendingRefreshes:
    if not slot.finished:
      slot.fail((ref KafkaClosedError)(msg: "cluster is closed"))
  c.pendingRefreshes.clear()
  # Fail parked FindCoordinator joiners so they observe the close regardless of how
  # far the detached runner has progressed.
  for k, slot in c.pendingCoordinatorLookups:
    if not slot.finished:
      slot.fail((ref KafkaClosedError)(msg: "cluster is closed"))
  c.pendingCoordinatorLookups.clear()
  c.state = lsClosed
