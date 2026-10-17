import std/[tables, options, sequtils, importutils]

import async_backend, errors, connection, sasl
import internal/[backoff, metrics, safe_logging]
import protocol/[primitives, api_keys, messages]

privateAccess(SaslMechanism)

export metrics.MetricsHook

type
  Broker* = ref object
    nodeId*: int32
    host*: string
    port*: int32
    conn: BrokerConnection
    apiVersions: Table[int16, ApiVersionRange]
      ## Ranges advertised by ApiVersions negotiation, trusted for the life of the
      ## connection. Private so a caller cannot re-target the version later requests
      ## resolve against; reader: resolveVersion.
    negotiated: bool
    clientId: string
    tlsConfig: TlsConfig
    hostAllowlist: HostAllowlist
      ## Cluster-wide dial policy enforced by `connectImpl` before any socket opens,
      ## plaintext included. An advertised name is resolved first and the answers
      ## filtered, so it cannot point at a denied address. `nil` disables it.
    saslMechanism: SaslMechanism
    saslMechanisms: seq[SaslMechanism]
      ## Fallback list: when non-empty, `connectImpl` tries each candidate on a fresh
      ## TCP connection (KIP-43: the broker closes it on UNSUPPORTED_SASL_MECHANISM)
      ## and takes the first accepted; the singular `saslMechanism` is then ignored.
    addressDirty: bool
      ## Set when a Metadata response carries a new host:port for this nodeId; cleared
      ## by connectImpl after redialing, and the connect fast paths refuse a dirty
      ## broker. Access via the mark/is pair.
    connectLock: AsyncLock
    when defined(kafkaConnTestHooks):
      dnsResolver: proc(host: string, port: Port): seq[TransportAddress] {.
        gcsafe, raises: [TransportAddressError]
      .}
        ## Test-only: replaces the system resolver on the name dial path, so tests can
        ## drive multi-address/empty answers and resolver failures without real DNS.
    reconnectBackoffMs*: int32 ## Reconnection backoff state
    reconnectBackoffMaxMs*: int32
    connectTimeoutMs*: int32
    requestTimeoutMs*: int32
      ## Per-request timeout. Non-positive values wait (almost) indefinitely,
      ## bounded by the connection layer's ten-minute ceiling.
    consecutiveFailures: int
    lastConnectAttempt: Moment
    currentBackoff: Duration
    throttleUntil: Moment
      ## Per-broker quota throttle deadline: a response's throttle_time_ms > 0 asks for
      ## further pacing. An absolute deadline lets multiple in-flight responses collapse
      ## to the latest rather than summing.
    inFlightCount: int
      ## sendTypedRequest calls currently awaiting a response; pruneStaleBrokers reads
      ## it so a vanished broker stays registered until in-flight drains. Private; only
      ## writeTypedRequest, awaitTypedResponse and produceAbandon mutate it.
    fireAndForgetCount: int
      ## acks=0 produce writes on the wire, mutated only by the acks=0 branches of
      ## produce / produceWrite. They bypass sendTypedRequest and register no response
      ## slot, so they are tracked separately: pruneStaleBrokers keeps the broker
      ## registered while non-zero but never *force*-prunes on it, since a force-close
      ## would silently drop a record with no response and no retry.
    handshaking: bool
      ## True while `connectImpl` is between `connectTcp` and the end of ApiVersions
      ## negotiation + SASL auth. ApiVersions traffic uses `conn.sendRequest` directly
      ## and does not bump `inFlightCount`, so this flag is all that stops
      ## pruneStaleBrokers from closing the broker mid-handshake (conn would go nil and
      ## crash); SASL steps do bump the counter, but the flag covers the gaps between
      ## them. Mutated under `connectLock`; pruneStaleBrokers reads it lock-free by
      ## design, so at worst it sees a value one prune cycle stale. Access via
      ## isHandshaking / setHandshaking.
    firstSkippedPruneAt: Moment
      ## Stale-since timestamp: when pruneStaleBrokers first saw this broker absent from
      ## the active set with an in-flight request, so the prune loop can force-close one
      ## whose in-flight never drains. Reset when it reappears as active.
    latestInFlightDeadline: Moment
      ## Largest response deadline recorded for a `sendTypedRequest` that widened
      ## `timeoutMs` past `requestTimeoutMs` (JoinGroup / SyncGroup). Bumped
      ## monotonically: a past value no longer extends the prune grace, so no reset is
      ## needed when in-flight drains. Used by pruneStaleBrokers to defer a force-prune.
    retired: bool
      ## Set once the cluster permanently removes this broker (prune, cluster.close).
      ## `connectImpl` refuses to reconnect it, so a caller holding a stale reference
      ## across an await cannot revive an untracked connection and leak its readLoop.
      ## Not set by the transient closes in connectImpl (SASL cycling, address redial),
      ## which must leave the broker reusable. Set via retire(); read via isRetired.
    metricsHook*: MetricsHook

  BrokerConnState* = enum
    ## Derived view over the connection flags (`conn`, `conn.closed`, `negotiated`,
    ## `handshaking`), which stay the source of truth: deriving on read cannot drift out
    ## of sync the way a fifth stored flag could.
    bcsDisconnected ## no live TCP connection
    bcsHandshaking ## TCP up; ApiVersions negotiation / SASL auth not finished
    bcsReady ## negotiated (and authenticated when SASL is configured)

  ProduceInFlight* = ref object
    ## Handle for a pipelined Produce written by `produceWrite` but not yet awaited.
    ## Exactly one of `produceAwait` / `produceAbandon` may consume it; consuming twice
    ## raises (await after abandon) or is a no-op (abandon after await). A handle that is
    ## neither leaks its in-flight slot and its `Broker.inFlightCount` bump — connection
    ## teardown clears the slot but never the broker-level count — so the caller must
    ## always consume it.
    handle: InFlightHandle
    consumed: bool

const maxThrottleMs* = 600_000'i32
  ## Upper bound on one throttle signal (10 minutes): clamps an out-of-range value that
  ## would otherwise park the client for up to int32.high ms (~24 days). Real quotas sit
  ## well below this.

proc validateBrokerConfig*(
    saslMechanisms: seq[SaslMechanism], reconnectBackoffMs, reconnectBackoffMaxMs: int32
) {.raises: [KafkaError].} =
  ## Shared by `newCluster`, so a bad value fails while the cluster is built, and by
  ## `newBroker`, which has to reject it for a direct construction too. A nil
  ## candidate would be dereferenced by the SASL handshake (a NilAccessDefect), and a
  ## negative backoff would make `calculateBackoff` negative and silently disable the
  ## wait.
  for mech in saslMechanisms:
    if mech == nil:
      raise newException(KafkaError, "saslMechanisms must not contain nil entries")
  if reconnectBackoffMs < 0:
    raise newException(
      KafkaError, "reconnectBackoffMs must be >= 0, got " & $reconnectBackoffMs
    )
  if reconnectBackoffMaxMs < reconnectBackoffMs:
    raise newException(
      KafkaError,
      "reconnectBackoffMaxMs (" & $reconnectBackoffMaxMs &
        ") must be >= reconnectBackoffMs (" & $reconnectBackoffMs & ")",
    )

proc newBroker*(
    nodeId: int32,
    host: string,
    port: int32,
    clientId: string,
    tlsConfig: TlsConfig = nil,
    hostAllowlist: HostAllowlist = nil,
    saslMechanism: SaslMechanism = nil,
    reconnectBackoffMs: int32 = 50,
    reconnectBackoffMaxMs: int32 = 10_000,
    connectTimeoutMs: int32 = 30_000,
    requestTimeoutMs: int32 = 30_000,
    metricsHook: MetricsHook = nil,
    saslMechanisms: seq[SaslMechanism] = @[],
): Broker =
  validateBrokerConfig(saslMechanisms, reconnectBackoffMs, reconnectBackoffMaxMs)
  Broker(
    nodeId: nodeId,
    host: host,
    port: port,
    clientId: clientId,
    apiVersions: initTable[int16, ApiVersionRange](),
    negotiated: false,
    connectLock: AsyncLock.new(),
    tlsConfig: tlsConfig,
    hostAllowlist: hostAllowlist,
    saslMechanism: saslMechanism,
    saslMechanisms: saslMechanisms,
    reconnectBackoffMs: reconnectBackoffMs,
    reconnectBackoffMaxMs: reconnectBackoffMaxMs,
    connectTimeoutMs: connectTimeoutMs,
    requestTimeoutMs: requestTimeoutMs,
    consecutiveFailures: 0,
    lastConnectAttempt: Moment.init(0'i64, nanoseconds(1)),
    currentBackoff: Duration.default(),
    throttleUntil: Moment.init(0'i64, nanoseconds(1)),
    firstSkippedPruneAt: Moment.init(0'i64, nanoseconds(1)),
    latestInFlightDeadline: Moment.init(0'i64, nanoseconds(1)),
    metricsHook: metricsHook,
  )

proc inFlightRequests*(b: Broker): int {.inline, raises: [].} =
  ## Read-only accessor for the in-flight request count.
  b.inFlightCount

proc fireAndForgetInFlight*(b: Broker): int {.inline, raises: [].} =
  ## Count of acks=0 writes on the wire; see the field doc for the prune semantics.
  b.fireAndForgetCount

proc isHandshaking*(b: Broker): bool {.inline, raises: [].} =
  ## True while connectImpl is mid-handshake; pruneStaleBrokers reads it so the broker
  ## isn't torn down while the uncounted ApiVersions negotiation is live.
  b.handshaking

proc setHandshaking*(b: Broker, on: bool) {.inline, raises: [].} =
  ## Set/clear the in-handshake flag; the production caller is connectImpl only, under
  ## connectLock. Tests use it to simulate "TCP up, handshake in progress". A setter
  ## (not a public field) keeps call sites greppable.
  b.handshaking = on

proc isAddressDirty*(b: Broker): bool {.inline, raises: [].} =
  ## True while a new host:port is flagged and the stale connection has not been
  ## redialed. The connect fast paths refuse a dirty broker; only connectImpl clears it.
  b.addressDirty

proc markAddressDirty*(b: Broker) {.inline, raises: [].} =
  ## Flag the address as changed; production caller is getOrCreateBroker, on a Metadata
  ## response with a different host:port. Only connectImpl clears the flag.
  b.addressDirty = true

proc firstSkippedPruneAt*(b: Broker): Moment {.inline, raises: [].} =
  ## Stale-since timestamp set by pruneStaleBrokers on the first in-flight skip; unset is
  ## newBroker's sentinel value.
  b.firstSkippedPruneAt

proc setFirstSkippedPruneAt*(b: Broker, at: Moment) {.inline, raises: [].} =
  ## Update the stale-since marker: pruneStaleBrokers stamps the first observation,
  ## getOrCreateBroker resets it on reappearance. Tests fast-forward the grace window.
  b.firstSkippedPruneAt = at

proc latestInFlightDeadline*(b: Broker): Moment {.inline, raises: [].} =
  ## Largest response deadline recorded for a widened-timeout request through
  ## sendTypedRequest; newBroker's sentinel means none was ever recorded.
  b.latestInFlightDeadline

proc isRetired*(b: Broker): bool {.inline, raises: [].} =
  ## True once the cluster permanently removed this broker; it refuses to reconnect.
  b.retired

proc applyThrottle*(b: Broker, throttleTimeMs: int32) {.gcsafe, raises: [].} =
  ## Record a broker-quota throttle: `throttleTimeMs` <= 0 is a no-op, otherwise the
  ## deadline is extended (max(), so a fresher smaller throttle cannot shorten it).
  if throttleTimeMs <= 0:
    return

  var clamped = throttleTimeMs
  if clamped > maxThrottleMs:
    let portStr = $b.port
    let reqStr = $throttleTimeMs
    emitMetric(
      b.metricsHook,
      meBrokerThrottleClamped,
      [
        (cstring"host", cstring(b.host)),
        (cstring"port", cstring(portStr)),
        (cstring"requested_ms", cstring(reqStr)),
      ],
      int64(throttleTimeMs),
    )
    clamped = maxThrottleMs
  let until = Moment.now() + milliseconds(clamped.int64)
  if until > b.throttleUntil:
    b.throttleUntil = until

proc throttleRemainingMs*(b: Broker): int32 {.gcsafe, raises: [].} =
  ## Milliseconds remaining on the active broker-quota throttle, clamped to non-negative.
  let now = Moment.now()
  if b.throttleUntil <= now:
    return 0'i32
  let remaining = (b.throttleUntil - now).milliseconds
  if remaining <= 0:
    0'i32
  elif remaining > int32.high:
    int32.high
  else:
    int32(remaining)

proc awaitThrottle*(
    b: Broker, timeoutMs: int32 = -1
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Sleep until the broker's throttle deadline elapses (no-op if none), bounded by a
  ## budget with response-timeout semantics: `< 0` = the broker's `requestTimeoutMs`,
  ## `0` = wait (almost) indefinitely, positive = that many ms (non-positive budgets
  ## share the ten-minute cap of response waits). A throttle outlasting the budget
  ## raises KafkaTimeoutError at the boundary, so a request is never parked past its
  ## configured timeout.
  ##
  ## Loops rather than sleeping once: applyThrottle() runs lock-free from response
  ## handlers and can extend `throttleUntil` while we're suspended, so a single sleep
  ## would wake on a stale deadline and let the caller re-violate the quota; the
  ## caller's write then follows with no await, so the deadline cannot move first.
  ##
  ## A completed wait emits `broker.throttle.waited_ms` with wall-clock milliseconds;
  ## KafkaTimeoutError and CancelledError leave through the loop and do not emit.
  let selectedBudgetMs = if timeoutMs < 0: b.requestTimeoutMs else: timeoutMs
  let budgetMs = effectiveTimeoutMs(selectedBudgetMs)
  let budgetDeadline =
    if budgetMs > 0:
      some(Moment.now() + milliseconds(budgetMs.int64))
    else:
      none(Moment)
  let waitStart = Moment.now()
  var slept = false
  while true:
    let remaining = b.throttleRemainingMs()
    if remaining <= 0:
      break
    var sleepMs = remaining.int64
    if budgetDeadline.isSome:
      let budgetLeft = (budgetDeadline.get() - Moment.now()).milliseconds
      if budgetLeft <= 0:
        raise newException(
          KafkaTimeoutError,
          "broker throttle outlasts the request timeout (" & $budgetMs & "ms): " &
            $remaining & "ms still pending",
        )
      if sleepMs > budgetLeft:
        sleepMs = budgetLeft
    slept = true
    await sleepAsync(milliseconds(sleepMs))
  if slept:
    let portStr = $b.port
    emitMetric(
      b.metricsHook,
      meBrokerThrottleWaitedMs,
      [(cstring"host", cstring(b.host)), (cstring"port", cstring(portStr))],
      (Moment.now() - waitStart).milliseconds,
    )

proc isConnected*(b: Broker): bool =
  b.conn != nil and not b.conn.closed

proc connState*(b: Broker): BrokerConnState {.inline, raises: [].} =
  if not b.isConnected:
    bcsDisconnected
  elif b.handshaking or not b.negotiated:
    # conn-alive ∧ ¬negotiated ∧ ¬handshaking is unreachable in production (handshaking
    # spans connectImpl, close() nils conn); "not ready" is the safe reading for tests.
    bcsHandshaking
  else:
    bcsReady

proc isReady*(b: Broker): bool {.inline, raises: [].} =
  ## True when the connection is fully usable for typed requests (TCP up, negotiated,
  ## SASL done). Contrast `isConnected`, plain conn-aliveness, true mid-handshake.
  b.connState == bcsReady

proc resolveVersion(b: Broker, apiKey: ApiKey): int16 {.raises: [].} =
  let clientRange = supportedVersion(apiKey)

  if not b.negotiated:
    return clientRange.maxVersion

  let key = int16(ord(apiKey))
  b.apiVersions.withValue(key, brokerRange):
    return negotiateVersion(clientRange, brokerRange[])
  return -1

proc writeTypedRequest(
    b: Broker,
    apiKey: ApiKey,
    bodyBytes: seq[byte],
    flexible: bool = false,
    version: int16 = -1'i16,
    timeoutMs: int32 = -1,
): Future[InFlightHandle] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Resolve the version, write the request frame, and return the in-flight handle.
  ## Bumps `inFlightCount` on success (the paired `awaitTypedResponse` decrements it)
  ## and decrements it here on a write failure, since no response is coming.
  ##
  ## Honours any active throttle BEFORE issuing, not just on retry paths, else the first
  ## happy-path request after a throttle re-violates the quota. The wait shares this
  ## call's timeout budget (`< 0` = `requestTimeoutMs`), so a longer throttle fails with
  ## KafkaTimeoutError instead of parking the caller.
  ##
  ## `version >= 0` pins the header to the caller-resolved value so a reconnect landing
  ## in awaitThrottle cannot desync header/body versions.
  await b.awaitThrottle(timeoutMs)
  let resolvedVersion =
    if version >= 0:
      version
    else:
      b.resolveVersion(apiKey)
  if resolvedVersion < 0:
    # Carry a real error code: a bare KafkaApiError leaves errorCode = keNone, which
    # callers branching on errorClass(e.errorCode) would read as success.
    raise newKafkaApiError(keUnsupportedVersion, "unsupported API: " & $apiKey)
  if b.conn == nil or b.conn.closed:
    raise newException(
      KafkaConnectionError, "broker connection lost before request could be sent"
    )
  inc b.inFlightCount
  var ok = false
  try:
    result = await b.conn.writeRequest(apiKey, resolvedVersion, bodyBytes, flexible)
    ok = true
  finally:
    if not ok:
      dec b.inFlightCount

proc awaitTypedResponse(
    b: Broker, handle: InFlightHandle, timeoutMs: int32 = -1
): Future[ResponsePayload] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Await the response for a `writeTypedRequest` handle and drop the in-flight count
  ## once it resolves (success, timeout, or failure).
  ##
  ## Awaits on the connection the request was WRITTEN on (`handle.conn`), not `b.conn`:
  ## a concurrent reconnect or force-close can swap/nil `b.conn` between the write and
  ## here, nil-derefing or releasing a slot on the wrong connection.
  ##
  ## Rejects the acks=0 sentinel (`corrId < 0`, nil conn/fut) — no response, no in-flight
  ## slot — since awaiting it would nil-deref and spuriously decrement `inFlightCount`;
  ## `produceAwait` is its only legitimate consumer.
  if handle.corrId < 0:
    raise newException(
      KafkaError,
      "cannot await a fire-and-forget sentinel handle (" & $handle.apiKey &
        "); acks=0 produce has no response",
    )
  try:
    result = await handle.conn.awaitResponse(handle, timeoutMs)
  finally:
    dec b.inFlightCount

proc sendTypedRequest(
    b: Broker,
    apiKey: ApiKey,
    bodyBytes: seq[byte],
    flexible: bool = false,
    timeoutMs: int32 = -1,
    version: int16 = -1'i16,
): Future[ResponsePayload] {.async: (raises: [KafkaError, CancelledError]).} =
  # In-flight tracking (so pruneStaleBrokers won't tear down a broker with a caller
  # awaiting it) is split across writeTypedRequest / awaitTypedResponse but nets out the
  # same on every exit path, exceptions and cancellation included. `version >= 0` pins
  # the header version — see writeTypedRequest.
  #
  # Record the widened response deadline so pruneStaleBrokers cannot force-prune a broker
  # mid-JoinGroup / SyncGroup: default requests fit the grace floor, but a widened timeout
  # would otherwise wedge before the caller times out. Monotonic max(), so a past value is
  # a no-op at the prune site.
  let selectedTimeoutMs = if timeoutMs < 0: b.requestTimeoutMs else: timeoutMs
  let effectiveCallTimeoutMs = effectiveTimeoutMs(selectedTimeoutMs)
  if effectiveCallTimeoutMs > effectiveTimeoutMs(b.requestTimeoutMs):
    let deadline = Moment.now() + milliseconds(effectiveCallTimeoutMs.int64)
    if deadline > b.latestInFlightDeadline:
      b.latestInFlightDeadline = deadline
  let handle =
    await b.writeTypedRequest(apiKey, bodyBytes, flexible, version, timeoutMs)
  result = await b.awaitTypedResponse(handle, timeoutMs)

proc close*(b: Broker) {.async: (raises: []).} =
  ## Tear down the current connection. The field is claimed with a local reference and
  ## only cleared after the await, and only if it still points at the connection this call
  ## closed: a concurrent connect may install a fresh connection while this close is
  ## suspended, and an unconditional `b.conn = nil` would orphan the new socket fd and
  ## readLoop with no owner left to close them. Leaving the field in place during the
  ## await also keeps mid-handshake readers on the closed connection (a catchable error).
  let conn = b.conn
  if conn != nil:
    await conn.close()
    if b.conn == conn:
      b.conn = nil
      b.negotiated = false

proc retire*(b: Broker) {.async: (raises: []).} =
  ## Permanently remove this broker: mark it retired so a later connect refuses to
  ## reconnect (connectImpl raises KafkaConnectionError), then close the connection.
  ## Used by pruneStaleBrokers and cluster.close; the flag is set before the close await
  ## so a racing connect sees it. Use plain close() for transient teardown that must leave
  ## the broker reusable (connectImpl's SASL cycling and address redial).
  b.retired = true
  await b.close()

proc detachConn*(b: Broker) {.raises: [].} =
  ## Mark the current connection unusable and detach its teardown so the caller stays
  ## sync. Used when the address changes: in-flight requests would otherwise wait on
  ## responses from the wrong broker. Nills `b.conn` so a later connect never reaches the
  ## detached connection (in-flight handles pin the object and observe its failAll).
  ##
  ## A real detach emits `broker.connection.detached` with the address still on the broker
  ## (getOrCreateBroker overwrites host/port after this returns); nil/already-closed is a
  ## silent no-op whose asyncSpawn'd close is not a metric.
  let old = b.conn
  if old == nil or old.closed:
    return
  old.closed = true
  b.negotiated = false
  b.conn = nil
  let portStr = $b.port
  emitMetric(
    b.metricsHook,
    meBrokerConnectionDetached,
    [(cstring"host", cstring(b.host)), (cstring"port", cstring(portStr))],
    1,
  )
  asyncSpawn old.close()

proc decodeSaslAuthResponse(
    payload: ResponsePayload, version: int16, mechanism: SaslMechanism
): SaslAuthenticateResponse {.raises: [KafkaError].} =
  ## Decode one SaslAuthenticate response, raising the broker's rejection with the
  ## mechanism name so a multi-mechanism client can tell which candidate failed.
  ## Shared by the initial and step round trips so they cannot report differently.
  var r = initKafkaReader(payload)
  result = r.decode(SaslAuthenticateResponse, version)
  if result.errorCode != keNone:
    let msg =
      if result.errorMessage.isSome:
        result.errorMessage.get
      else:
        kafkaErrorName(result.errorCode)
    raise newKafkaApiError(
      result.errorCode, "SASL auth failed for " & mechanism.name & ": " & msg
    )

proc performSaslAuthOne(
    b: Broker, mechanism: SaslMechanism
) {.async: (raises: [KafkaError, CancelledError]).} =
  # Guard credential-leaking mechanisms (e.g. PLAIN) against non-TLS and unverified-TLS
  # connections, before any auth bytes go on the wire.
  if mechanism.requireTls:
    if not b.conn.isTls():
      raise newException(
        KafkaConnectionError,
        "SASL mechanism " & mechanism.name & " requires a TLS connection but broker " &
          b.host & ":" & $b.port &
          " was reached over plaintext. Configure TlsConfig, or pass " &
          "allowPlaintext = true to opt out.",
      )
    if b.tlsConfig != nil and b.tlsConfig.skipVerify:
      raise newException(
        KafkaConnectionError,
        "SASL mechanism " & mechanism.name &
          " rejects TlsConfig.skipVerify = true (credentials would be " &
          "exposed to MITM). Provide trust anchors or pass " &
          "allowPlaintext = true to opt out.",
      )

  let session = mechanism.newSession()
  # Zero any per-session secret on every exit path: a credential must not be left in
  # memory when auth fails before the mechanism's own burn points run.
  defer:
    if session.dispose != nil:
      session.dispose()
  let handshakeVersion = b.resolveVersion(akSaslHandshake)
  let authVersion = b.resolveVersion(akSaslAuthenticate)
  # Guard -1 like every other RPC path: the broker demands SASL but advertises neither
  # SaslHandshake nor SaslAuthenticate.
  if handshakeVersion < 0 or authVersion < 0:
    raise newKafkaApiError(
      keUnsupportedVersion,
      "broker " & b.host & ":" & $b.port &
        " demands SASL but does not advertise SaslHandshake / SaslAuthenticate",
    )

  var w = initKafkaWriter()
  w.encode(SaslHandshakeRequest(mechanism: mechanism.name), handshakeVersion)
  let handshakeResp = await b.sendTypedRequest(akSaslHandshake, w.buf)
  var r = initKafkaReader(handshakeResp)
  let hs = r.decode(SaslHandshakeResponse, handshakeVersion)
  if hs.errorCode != keNone:
    # Surface the mechanism list and error code so the multi-mechanism caller can tell
    # "try the next candidate" (UNSUPPORTED_SASL_MECHANISM) from a hard auth failure.
    raise newKafkaApiError(
      hs.errorCode,
      "SASL handshake rejected mechanism " & mechanism.name & ": broker supports " &
        $hs.mechanisms,
    )

  # PLAIN completes in 1 round, SCRAM-SHA-* in 2; the tight maxSaslSteps cap stops a
  # misbehaving broker stretching it. The call is awaited: SCRAM's `challenge` derives
  # the key in event-loop slices.
  const maxSaslSteps = 3
  var serverData = block:
    var authBytes = session.initialResponse()
    var ww = initKafkaWriter()
    ww.encode(SaslAuthenticateRequest(authBytes: authBytes), authVersion)
    var authResp: ResponsePayload
    # Zero the payload copies on every exit, not just success: for PLAIN `authBytes` and
    # `ww.buf` both hold the plaintext password until GC (a harmless zeroize for SCRAM's
    # already-hashed payload), and a failed or cancelled send is when they'd linger.
    try:
      authResp = await b.sendTypedRequest(akSaslAuthenticate, ww.buf)
    finally:
      burnBytes(authBytes)
      burnBytes(ww.buf)
    decodeSaslAuthResponse(authResp, authVersion, mechanism).authBytes

  var (state, response) = await session.challenge(serverData)
  var saslStep = 0
  while state == ssInProgress:
    if saslStep >= maxSaslSteps:
      raise
        newException(KafkaProtocolError, "SASL authentication exceeded maximum steps")
    inc saslStep
    var ww = initKafkaWriter()
    ww.encode(SaslAuthenticateRequest(authBytes: response), authVersion)
    var stepResp: ResponsePayload
    try:
      stepResp = await b.sendTypedRequest(akSaslAuthenticate, ww.buf)
    finally:
      burnBytes(response)
      burnBytes(ww.buf)
    let ar = decodeSaslAuthResponse(stepResp, authVersion, mechanism)
    (state, response) = await session.challenge(ar.authBytes)

proc resetBackoff*(b: Broker) =
  b.consecutiveFailures = 0
  b.currentBackoff = Duration.default()

proc calculateBackoff(b: Broker): Duration =
  ## Exponential backoff with jitter via `jitteredBackoff`
  ## (base × 2^attempt capped at ×32, ±25% jitter), clamped to `reconnectBackoffMaxMs`
  ## so per-broker settings still bound the sleep.
  let attempt = b.consecutiveFailures + 1
  let raw = jitteredBackoff(b.reconnectBackoffMs, attempt)
  let capped = min(int32(b.reconnectBackoffMaxMs), raw)
  milliseconds(capped)

proc recordFailure(b: Broker, stage: string) {.raises: [].} =
  ## Bump `consecutiveFailures` (capped at 14 — the backoff helper saturates well before
  ## that), recompute backoff, emit `broker.reconnect.failed` tagged with `stage`, warn.
  b.consecutiveFailures = min(b.consecutiveFailures + 1, 14)
  b.currentBackoff = b.calculateBackoff()
  let portStr = $b.port
  emitMetric(
    b.metricsHook,
    meBrokerReconnectFailed,
    [
      (cstring"host", cstring(b.host)),
      (cstring"port", cstring(portStr)),
      (cstring"stage", cstring(stage)),
    ],
    int64(b.consecutiveFailures),
  )
  warn "Broker " & b.host & ":" & $b.port & " " & stage & " failed (attempt " &
    $b.consecutiveFailures & ", next backoff " & $b.currentBackoff.milliseconds & "ms)"

when defined(kafkaConnTestHooks):
  proc armDnsResolverForTest*(
      b: Broker,
      resolver: proc(host: string, port: Port): seq[TransportAddress] {.
        gcsafe, raises: [TransportAddressError]
      .},
  ) =
    ## Replace the resolver the name dial path uses. See `dnsResolver`.
    b.dnsResolver = resolver

  proc clearDnsResolverForTest*(b: Broker) =
    b.dnsResolver = nil

proc resolveHost(b: Broker): seq[TransportAddress] {.raises: [TransportAddressError].} =
  when defined(kafkaConnTestHooks):
    if not b.dnsResolver.isNil:
      return b.dnsResolver(b.host, Port(b.port))
  resolveTAddress(b.host, Port(b.port))

proc resolveBrokerAddress(
    b: Broker
): seq[TransportAddress] {.raises: [KafkaConnectionError].} =
  ## Addresses `b.host` resolves to; only called for names (`initTAddress` handles
  ## literals). A failure is recorded so the reconnect backoff grows — a down resolver is
  ## transient, unlike a malformed address. The lookup is synchronous (chronos has no
  ## async resolver), so it stalls the event loop; the caller applies the allowlist.
  var failure = ""
  try:
    result = resolveHost(b)
  except TransportAddressError as e:
    failure = ": " & e.msg
  if result.len == 0:
    b.recordFailure("dns_resolve")
    raise newException(
      KafkaConnectionError,
      "cannot resolve broker address " & b.host & ":" & $b.port & failure,
    )

proc connectTcp(
    b: Broker, address: TransportAddress
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Open the TCP (and optionally TLS) connection and install the orphan-response hook.
  ## Caller handles failure metrics / counters.
  b.conn = await connection.connect(
    address, b.clientId, b.host, b.tlsConfig, b.hostAllowlist, b.connectTimeoutMs,
    b.requestTimeoutMs,
  )
  # Capture host/port at install time so the callback need not walk back to the broker.
  # It fires synchronously from the chronos readLoop, so emitMetric (and the user's
  # MetricsHook) must stay cheap.
  let hook = b.metricsHook
  let host = b.host
  let port = $b.port
  b.conn.setOnOrphan(
    proc(corrId: int32) {.closure, gcsafe, raises: [].} =
      emitMetric(
        hook,
        meConnectionOrphanResponse,
        [(cstring"host", cstring(host)), (cstring"port", cstring(port))],
        int64(corrId),
      )
  )

proc negotiateAndAuth(
    b: Broker, mech: Option[SaslMechanism]
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## On the already-open `b.conn`: run ApiVersions negotiation and, when `mech` is
  ## `some`, one SASL handshake + authenticate round. Caller handles failure metrics and
  ## the multi-mechanism reconnect loop.
  var w = initKafkaWriter()
  let respBytes = await b.conn.sendRequest(akApiVersions, 0'i16, w.buf)

  var r = initKafkaReader(respBytes)
  let resp = r.decode(ApiVersionsResponse)

  if resp.errorCode != keNone:
    raise newKafkaApiError(
      resp.errorCode, "ApiVersions error: " & kafkaErrorName(resp.errorCode)
    )

  b.apiVersions.clear()
  for key in resp.apiKeys:
    b.apiVersions[key.apiKey] =
      ApiVersionRange(minVersion: key.minVersion, maxVersion: key.maxVersion)
  b.negotiated = true
  let portStr2 = $b.port
  emitMetric(
    b.metricsHook,
    meBrokerApiVersionsNegotiated,
    [(cstring"host", cstring(b.host)), (cstring"port", cstring(portStr2))],
    int64(b.apiVersions.len),
  )

  # SaslHandshake v1+ requires API version negotiation first, done above.
  if mech.isSome:
    await b.performSaslAuthOne(mech.get)
    let mechName = mech.get.name
    let portStr = $b.port
    emitMetric(
      b.metricsHook,
      meBrokerSaslAuthenticated,
      [
        (cstring"host", cstring(b.host)),
        (cstring"port", cstring(portStr)),
        (cstring"mechanism", cstring(mechName)),
      ],
      1,
    )

proc connectImpl(b: Broker) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Internal: connect, negotiate version, and run SASL auth. Caller holds connectLock.
  ##
  ## When `saslMechanisms` (plural) is populated, each candidate is tried on a fresh
  ## connection (per KIP-43 the broker may close after each UNSUPPORTED_SASL_MECHANISM);
  ## the backoff sleep and reconnect.attempt metric fire once per call, not per candidate.
  ##
  ## A retired broker must never reconnect: a caller holding a stale reference across an
  ## await would otherwise revive a connection the registry no longer tracks, leaking its
  ## readLoop with no owner left to close it.
  if b.retired:
    raise newException(
      KafkaConnectionError,
      "broker " & b.host & ":" & $b.port &
        " has been retired (removed from the cluster) and will not reconnect",
    )
  if b.isConnected:
    return

  # A dead-readLoop connection is left non-nil but `closed` (readLoop flags closed +
  # failAll but never disposes the transport; only close() does). Since isConnected is
  # false here, any surviving b.conn is that stale connection — tear it down before
  # connectTcp overwrites b.conn, or its socket fd and streams leak on every reconnect.
  if b.conn != nil:
    await b.close()

  # Mark in-handshake so pruneStaleBrokers doesn't tear the broker down while
  # the ApiVersions negotiation flies via conn.sendRequest (no inFlightCount bump).
  # Under connectLock; the lock-free prune read may lag one cycle behind, and the
  # defer drops the flag even on cancellation or partial handshake failure.
  b.setHandshaking(true)
  defer:
    b.setHandshaking(false)

  # Backoff is consumed outside connectLock by the public entry points, so a sleep
  # doesn't block other callers' fast-path isConnected checks. The attempt-stamp is
  # recorded here so the next failure's backoff stays accurate on the SASL loop too.
  b.lastConnectAttempt = Moment.now()
  let portStr3 = $b.port
  emitMetric(
    b.metricsHook,
    meBrokerReconnectAttempt,
    [(cstring"host", cstring(b.host)), (cstring"port", cstring(portStr3))],
    int64(b.consecutiveFailures + 1),
  )

  # Range-check before `Port`: metadata decodes the port as an unchecked int32, and
  # `Port(70000'i32)` truncates instead of raising — dialing a different port than the
  # broker named.
  if b.port < 1 or b.port > 65535:
    raise newException(
      KafkaConnectionError,
      "invalid address " & b.host & ":" & $b.port & ": port out of range",
    )

  var addresses: seq[TransportAddress]
  try:
    addresses = @[initTAddress(b.host, int(b.port))]
  except TransportAddressError:
    # Kafka often advertises a host name (a single-node broker defaults to `localhost`)
    # and `initTAddress` accepts only IP literals. Resolution happens here because TLS
    # verification needs the name: dialing the raw address fails the certificate check.
    addresses = resolveBrokerAddress(b)

  # Bound where the broker may send us: the rules match resolved addresses, never the
  # advertised name — a metadata response can carry any string, and only the address
  # says where the socket goes.
  if b.hostAllowlist != nil:
    let resolvedCount = addresses.len
    addresses = addresses.filterIt(addressAllowed(b.hostAllowlist, it))
    if addresses.len == 0:
      raise newException(
        KafkaConnectionError,
        "broker address " & b.host & ":" & $b.port &
          " is not permitted by the configured host allowlist (resolved " &
          $resolvedCount & " addresses, " & $(resolvedCount - addresses.len) &
          " excluded by allowlist)",
      )
    elif addresses.len < resolvedCount:
      debug "Broker ",
        b.host,
        ":",
        $b.port,
        ": ",
        $(resolvedCount - addresses.len),
        " of ",
        $resolvedCount,
        " resolved addresses excluded by host allowlist"

  # The seq form wins, else the singular field, else `none` (skip SASL entirely).
  var candidates: seq[Option[SaslMechanism]]
  if b.saslMechanisms.len > 0:
    candidates = b.saslMechanisms.mapIt(some(it))
  elif b.saslMechanism != nil:
    candidates = @[some(b.saslMechanism)]
  else:
    candidates = @[none(SaslMechanism)]

  var lastUnsupportedErr: ref KafkaApiError
  var ok = false
  for mech in candidates:
    # Re-check `retired` per candidate: a retire landing during a prior candidate's
    # close/backoff await would otherwise leak a fresh socket + readLoop.
    if b.retired:
      raise newException(
        KafkaConnectionError,
        "broker " & b.host & ":" & $b.port &
          " was retired mid-handshake and will not reconnect",
      )

    # TCP connect — failure here is terminal (a mechanism cannot fix a network
    # problem). Try every resolved address in order: one unreachable family (an
    # unrouted IPv6 answer, say) must not take the broker down with it. Only an
    # all-addresses failure counts as a reconnect failure.
    var tcpFailure: ref KafkaConnectionError
    var connected = false
    for address in addresses:
      try:
        await b.connectTcp(address)
        connected = true
        break
      except KafkaConnectionError as e:
        tcpFailure = e
      except CancelledError:
        raise
    if not connected:
      b.recordFailure("tcp_connect")
      if addresses.len > 1:
        raise newException(
          KafkaConnectionError,
          "failed to connect to broker " & b.host & ":" & $b.port & " (" & $addresses.len &
            " addresses tried): " & tcpFailure.msg,
        )
      raise tcpFailure

    # Retire may have landed during connectTcp: close the fresh socket rather than
    # authenticate and abandon it.
    if b.retired:
      await b.close()
      raise newException(
        KafkaConnectionError,
        "broker " & b.host & ":" & $b.port &
          " was retired mid-handshake and will not reconnect",
      )

    var rejected = false
    try:
      await b.negotiateAndAuth(mech)
      ok = true
    except KafkaApiError as e:
      if candidates.len > 1 and e.errorCode == keUnsupportedSaslMechanism:
        # KIP-43: the broker may close the connection after this response. Drop our
        # side, remember the error, and try the next candidate on a fresh socket.
        await b.close()
        let mechName = if mech.isSome: mech.get.name else: "<none>"
        info "Broker ",
          b.host,
          ":",
          $b.port,
          " rejected SASL mechanism ",
          mechName,
          " (",
          kafkaErrorName(e.errorCode),
          "); trying the next candidate"
        let portStr4 = $b.port
        emitMetric(
          b.metricsHook,
          meBrokerSaslMechanismRejected,
          [
            (cstring"host", cstring(b.host)),
            (cstring"port", cstring(portStr4)),
            (cstring"mechanism", cstring(mechName)),
          ],
          int64(e.errorCode),
        )
        lastUnsupportedErr = e
        rejected = true
      else:
        await b.close()
        b.recordFailure("handshake")
        warn "Handshake with broker ",
          b.host,
          ":",
          $b.port,
          " failed for SASL ",
          "mechanism ",
          (if mech.isSome: mech.get.name else: "<none>"),
          ": ",
          kafkaErrorName(e.errorCode)
        raise
    except KafkaError:
      # Includes requireTls failures: switching mechanisms can't help (TLS posture is
      # identical for all), so fail fast.
      await b.close()
      b.recordFailure("handshake")
      raise
    except CancelledError:
      await b.close()
      raise

    if rejected:
      continue
    break

  if not ok:
    # Every candidate was rejected: surface the most recent rejection so the caller sees
    # the broker-supplied mechanism list.
    b.recordFailure("sasl_no_mechanism")
    if lastUnsupportedErr != nil:
      raise lastUnsupportedErr
    raise newException(KafkaError, "no SASL mechanism configured")

  # Success — reset backoff and clear addressDirty: the connection is against the
  # current b.host/b.port, and a broker that changed address while disconnected would
  # otherwise keep the flag set, so the next ensureConnected would tear down this good
  # connection and fail unrelated in-flight requests.
  if b.consecutiveFailures > 0:
    info "Reconnected to broker " & b.host & ":" & $b.port & " after " &
      $b.consecutiveFailures & " failures"
  b.resetBackoff()
  b.addressDirty = false

proc awaitReconnectBackoff(b: Broker) {.async: (raises: [CancelledError]).} =
  ## If a prior connect failed, sleep the remaining backoff before contending for
  ## `connectLock`; holding the lock across the sleep would block fast-path checks.
  if b.consecutiveFailures > 0:
    let elapsed = Moment.now() - b.lastConnectAttempt
    if elapsed < b.currentBackoff:
      await sleepAsync(b.currentBackoff - elapsed)

proc connectSlowPath(b: Broker) {.async: (raises: [KafkaError, CancelledError]).} =
  # Shared slow path for connect/ensureConnected: consume backoff outside the lock,
  # re-check the fast path after the sleep (another caller may have connected), then hold
  # the lock across the whole connect to close the dirty-address TOCTOU gap.
  await b.awaitReconnectBackoff()
  if b.isReady and not b.addressDirty and not b.retired:
    return
  await b.connectLock.acquire()
  try:
    if b.isReady and not b.addressDirty and not b.retired:
      return
    if b.addressDirty and b.isConnected:
      # Honour a pending address change rather than return a connection to the stale
      # host/port; connectImpl clears the flag on success.
      await b.close()
      b.resetBackoff()
    await b.connectImpl()
  finally:
    b.connectLock.releaseSafely("broker.connect")

proc connect*(b: Broker) {.async: (raises: [KafkaError, CancelledError]).} =
  # Public connect entry: same contract as ensureConnected. A retired broker falls
  # through to connectImpl, which raises.
  await b.connectSlowPath()

proc ensureConnected(b: Broker) {.async: (raises: [KafkaError, CancelledError]).} =
  # Fast path: `isReady` (not `isConnected`) and address not dirty — no lock needed.
  # connectImpl assigns b.conn before ApiVersions/SASL run, so conn-aliveness alone would
  # admit a request onto a half-authenticated connection. A caller arriving mid-handshake
  # parks on connectLock below.
  #
  # `not b.retired` too: retire() sets the flag before awaiting close(), so a broker can
  # still be isReady yet retired. Falling through routes to connectImpl, which raises.
  if b.isReady and not b.addressDirty and not b.retired:
    return
  await b.connectSlowPath()

template roundTrip(
    b: Broker,
    apiKey: ApiKey,
    req: typed,
    Resp: typedesc,
    flexThreshold: int16 = int16.high,
    timeoutMs: int32 = -1,
): untyped =
  ## Shared skeleton for the version-negotiated, throttle-aware per-API methods: ensure
  ## the connection, resolve the negotiated version (raising keUnsupportedVersion if the
  ## API is not on offer), encode `req`, send, decode `Resp`, and record the quota
  ## throttle — so no RPC drifts out of throttle accounting.
  ##
  ## `flexThreshold` selects the flexible (KIP-482) compact form from that version on.
  ## `timeoutMs` overrides the await deadline (JoinGroup and SyncGroup, which park on the
  ## rebalance, need it).
  ##
  ## A template, not a generic proc: chronos's async transform cannot prove the {.raises.}
  ## set of a generic decoding a type parameter, so inlining lets effect analysis run
  ## against the concrete `Resp` at each call site.
  block:
    await b.ensureConnected()

    let version = b.resolveVersion(apiKey)
    if version < 0:
      raise newKafkaApiError(keUnsupportedVersion, "unsupported API: " & $apiKey)

    let flexible = version >= flexThreshold
    var w = initKafkaWriter()
    w.encode(req, version)
    # Pin `version` through send so a reconnect in awaitThrottle cannot diverge the
    # header from what we encoded/decoded.
    let respBytes =
      await b.sendTypedRequest(apiKey, w.buf, flexible, timeoutMs, version)
    var r = initKafkaReader(respBytes)
    let resp = r.decode(Resp, version)
    b.applyThrottle(resp.throttleTimeMs)
    resp

proc metadata*(
    b: Broker, topics: Option[seq[string]], allowAutoTopicCreation: bool = false
): Future[MetadataResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Metadata API. Wraps each topic name in a MetadataRequestTopic (v10+ topicId stays
  ## the zero UUID — no caller identifies topics by UUID yet).
  ##
  ## `allowAutoTopicCreation` maps to the v4+ field, false by default so a routine
  ## refresh never materializes a mistyped topic; pre-v4 brokers auto-created
  ## unconditionally, so true restores that where supported.
  let reqTopics =
    if topics.isNone:
      none(seq[MetadataRequestTopic])
    else:
      var ts = newSeq[MetadataRequestTopic](topics.get.len)
      for i, name in topics.get:
        ts[i] = MetadataRequestTopic(name: name)
      some(ts)
  result = roundTrip(
    b,
    akMetadata,
    MetadataRequest(topics: reqTopics, allowAutoTopicCreation: allowAutoTopicCreation),
    MetadataResponse,
    flexThreshold = 9'i16,
  )

proc findCoordinator*(
    b: Broker, key: string, coordinatorType: int8 = 0
): Future[FindCoordinatorResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## FindCoordinator API. Serializes as v0 or v1 (v1+ for transaction coordinators).
  await b.ensureConnected()

  let version = b.resolveVersion(akFindCoordinator)
  if version < 0:
    raise newKafkaApiError(keUnsupportedVersion, "unsupported API: FindCoordinator")
  if coordinatorType != 0 and version < 1:
    raise newException(
      KafkaError,
      "transaction coordinator requires FindCoordinator v1+, but broker only supports v0",
    )

  var w = initKafkaWriter()
  w.encode(FindCoordinatorRequest(key: key, coordinatorType: coordinatorType), version)
  let respBytes = await b.sendTypedRequest(akFindCoordinator, w.buf, version = version)
  var r = initKafkaReader(respBytes)
  result = r.decode(FindCoordinatorResponse, version)
  b.applyThrottle(result.throttleTimeMs)

proc listOffsets*(
    b: Broker, req: ListOffsetsRequest
): Future[ListOffsetsResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## ListOffsets API. v1..v2; v2 adds isolation_level to the request and
  ## throttle_time_ms to the response.
  result = roundTrip(b, akListOffsets, req, ListOffsetsResponse)

proc offsetForLeaderEpoch*(
    b: Broker, req: OffsetForLeaderEpochRequest
): Future[OffsetForLeaderEpochResponse] {.
    async: (raises: [KafkaError, CancelledError])
.} =
  ## OffsetForLeaderEpoch API (KIP-320). v2..v3; v3 adds replicaId (consumers send -2).
  ## Detects log truncation on a leader change: returns the end offset of the largest
  ## epoch <= the requested one.
  result = roundTrip(b, akOffsetForLeaderEpoch, req, OffsetForLeaderEpochResponse)

proc leaveGroup*(
    b: Broker, req: LeaveGroupRequest
): Future[LeaveGroupResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## LeaveGroup API; v1 prepends throttle_time_ms to the response (same request wire).
  result = roundTrip(b, akLeaveGroup, req, LeaveGroupResponse)

proc fetch*(
    b: Broker, req: FetchRequest
): Future[FetchResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Fetch API. Per-partition record bytes return as `recordsRaw: Option[seq[byte]]`;
  ## the consumer's fetcher walks them into RecordBatches.
  result = roundTrip(b, akFetch, req, FetchResponse)

proc satAddInt32(a, b: int32): int32 {.inline, raises: [].} =
  ## Saturating int32 add, clamping instead of raising OverflowDefect: the widened
  ## rebalance+request timeout must survive an extreme user-supplied rebalanceTimeoutMs.
  let s = int64(a) + int64(b)
  if s > int64(int32.high):
    int32.high
  elif s < int64(int32.low):
    int32.low
  else:
    int32(s)

proc joinGroup*(
    b: Broker, req: JoinGroupRequest
): Future[JoinGroupResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## JoinGroup API; v2 prepends throttle_time_ms (same request wire through v2).
  ##
  ## The broker parks the call until every member rejoins, up to `rebalanceTimeoutMs`;
  ## the default 30s timeout would fire first, so widen this request to
  ## rebalanceTimeoutMs + requestTimeoutMs (Java convention).
  result = roundTrip(
    b,
    akJoinGroup,
    req,
    JoinGroupResponse,
    timeoutMs =
      satAddInt32(req.rebalanceTimeoutMs, effectiveTimeoutMs(b.requestTimeoutMs)),
  )

proc syncGroup*(
    b: Broker, req: SyncGroupRequest, rebalanceTimeoutMs: int32 = 0
): Future[SyncGroupResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## SyncGroup API; v1 prepends throttle_time_ms to the response (same request wire).
  ##
  ## Followers park until the leader submits the assignment, so a slow leader (metadata
  ## refresh, large partition set, custom partitioner) can exceed the default
  ## requestTimeoutMs, timing every follower out and bumping the generation into a
  ## rebalance storm: widen to rebalanceTimeoutMs + requestTimeoutMs like joinGroup.
  ## rebalanceTimeoutMs <= 0 keeps the default requestTimeoutMs.
  let timeoutMs =
    if rebalanceTimeoutMs > 0:
      satAddInt32(rebalanceTimeoutMs, effectiveTimeoutMs(b.requestTimeoutMs))
    else:
      effectiveTimeoutMs(b.requestTimeoutMs)
  result = roundTrip(b, akSyncGroup, req, SyncGroupResponse, timeoutMs = timeoutMs)

proc heartbeat*(
    b: Broker, req: HeartbeatRequest
): Future[HeartbeatResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Heartbeat API; v1 prepends throttle_time_ms to the response (same request wire).
  result = roundTrip(b, akHeartbeat, req, HeartbeatResponse)

proc offsetFetch*(
    b: Broker, req: OffsetFetchRequest
): Future[OffsetFetchResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## OffsetFetch API. v1..v5, decoded via the schema-gen procs, which gate
  ## throttle_time_ms (v3+ prefix), the top-level error_code (v2+ trailer) and
  ## per-partition committedLeaderEpoch (v5+, KIP-320) by negotiated version.
  ##
  ## Inlined rather than routed through roundTrip so the version gating the decode is the
  ## same one gating the committedLeaderEpoch clamp below: a reconnect landing in
  ## awaitThrottle can renegotiate `b.apiVersions`, and re-resolving after the round trip
  ## would let the clamp diverge from what the decoder saw.
  await b.ensureConnected()

  let version = b.resolveVersion(akOffsetFetch)
  if version < 0:
    raise newKafkaApiError(keUnsupportedVersion, "unsupported API: " & $akOffsetFetch)

  var w = initKafkaWriter()
  w.encode(req, version)
  let respBytes = await b.sendTypedRequest(akOffsetFetch, w.buf, version = version)
  var r = initKafkaReader(respBytes)
  result = r.decode(OffsetFetchResponse, version)
  b.applyThrottle(result.throttleTimeMs)

  # Below v5 the wire carries no committedLeaderEpoch and the decoder leaves it at Nim's
  # 0, a legitimate epoch that would masquerade as known-good. Clamp to -1 (Kafka's
  # KIP-320 sentinel) so callers distinguish unknown from epoch 0.
  if version < 5:
    for i in 0 ..< result.topics.len:
      for j in 0 ..< result.topics[i].partitions.len:
        result.topics[i].partitions[j].committedLeaderEpoch = -1'i32

proc offsetCommit*(
    b: Broker, req: OffsetCommitRequest
): Future[OffsetCommitResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## OffsetCommit API. v2..v8 via the schema-gen procs, which gate
  ## retentionTimeMs (v2-v4), throttle_time_ms (v3+), committedLeaderEpoch (v6+, KIP-320),
  ## groupInstanceId (v7+, KIP-345) and the flexible/tagged_fields trailer (v8+, KIP-482)
  ## by negotiated version.
  result =
    roundTrip(b, akOffsetCommit, req, OffsetCommitResponse, flexThreshold = 8'i16)

proc initProducerId*(
    b: Broker, req: InitProducerIdRequest
): Future[InitProducerIdResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## InitProducerId API. v0..v3 (flexible since v2); the decoder handles tagged_fields.
  result =
    roundTrip(b, akInitProducerId, req, InitProducerIdResponse, flexThreshold = 2'i16)

proc addPartitionsToTxn*(
    b: Broker, req: AddPartitionsToTxnRequest
): Future[AddPartitionsToTxnResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## AddPartitionsToTxn API. v0..v3, flexible at v3; capped there — KIP-890 (v4)
  ## reshaped the request for broker-side verification and is not a client send path.
  result = roundTrip(
    b, akAddPartitionsToTxn, req, AddPartitionsToTxnResponse, flexThreshold = 3'i16
  )

proc addOffsetsToTxn*(
    b: Broker, req: AddOffsetsToTxnRequest
): Future[AddOffsetsToTxnResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## AddOffsetsToTxn API. Negotiates v0..v3; v3 is flexible (KIP-482).
  result =
    roundTrip(b, akAddOffsetsToTxn, req, AddOffsetsToTxnResponse, flexThreshold = 3'i16)

proc endTxn*(
    b: Broker, req: EndTxnRequest
): Future[EndTxnResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## EndTxn API. Negotiates v0..v3; v3 is flexible (KIP-482).
  result = roundTrip(b, akEndTxn, req, EndTxnResponse, flexThreshold = 3'i16)

proc txnOffsetCommit*(
    b: Broker, req: TxnOffsetCommitRequest
): Future[TxnOffsetCommitResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## TxnOffsetCommit API. Negotiates v0..v3; v2 adds committedLeaderEpoch and
  ## v3 adds the KIP-447 fencing fields + is flexible (KIP-482).
  result =
    roundTrip(b, akTxnOffsetCommit, req, TxnOffsetCommitResponse, flexThreshold = 3'i16)

proc writeFireAndForget(
    b: Broker, apiKey: ApiKey, apiVersion: int16, body: seq[byte], timeoutMs: int32 = -1
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Send a request that expects no response (acks=0 produce).
  ##
  ## Bypasses sendTypedRequest and registers no response slot, so it is tracked on
  ## `fireAndForgetCount` instead: pruneStaleBrokers keeps the broker registered mid-write
  ## but never force-prunes on it (an acks=0 write has no response and no retry).
  ##
  ## Honours any active throttle here too, else a throttled broker is hammered by
  ## fire-and-forget writes; the wait shares this call's timeout budget.
  await b.awaitThrottle(timeoutMs)
  if b.conn == nil or b.conn.closed:
    raise newException(
      KafkaConnectionError, "broker connection lost before fire-and-forget request"
    )
  inc b.fireAndForgetCount
  try:
    await b.conn.sendRequestFireAndForget(apiKey, apiVersion, body)
  finally:
    dec b.fireAndForgetCount

proc produce*(
    b: Broker, req: ProduceRequest
): Future[Option[ProduceResponse]] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Produce API. v3..v7 (non-flexible through v8, legacy header/body); the response gains
  ## logStartOffset at v5, so the negotiated version must reach the decoder.
  await b.ensureConnected()

  let version = b.resolveVersion(akProduce)
  if version < 0:
    raise newKafkaApiError(keUnsupportedVersion, "unsupported API: " & $akProduce)

  var w = initKafkaWriter()
  w.encode(req, version)
  if req.acks == 0:
    await b.writeFireAndForget(akProduce, version, w.buf)
    return none(ProduceResponse)

  let respBytes = await b.sendTypedRequest(akProduce, w.buf, version = version)
  var r = initKafkaReader(respBytes)
  let resp = r.decode(ProduceResponse, version)
  b.applyThrottle(resp.throttleTimeMs)
  result = some(resp)

proc produceWrite*(
    b: Broker, req: ProduceRequest
): Future[ProduceInFlight] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Pipelined Produce: write the request frame and return a handle without awaiting the
  ## reply, so the producer can release its per-partition write-order gate while up to
  ## `maxInFlight` responses stay outstanding (KIP-185). Consume the handle with exactly
  ## one of `produceAwait` / `produceAbandon`.
  ##
  ## For acks=0 there is no response: the handle wraps a sentinel (`corrId = -1`, nil
  ## `conn`/`fut`), `produceAwait` resolves to none and `produceAbandon` releases nothing.
  ## It cannot reach `awaitTypedResponse` / `awaitResponse` — `ProduceInFlight` is not an
  ## `InFlightHandle`, so those consumers reject it at compile time.
  await b.ensureConnected()

  let version = b.resolveVersion(akProduce)
  if version < 0:
    raise newKafkaApiError(keUnsupportedVersion, "unsupported API: " & $akProduce)

  var w = initKafkaWriter()
  w.encode(req, version)
  if req.acks == 0:
    await b.writeFireAndForget(akProduce, version, w.buf)
    return ProduceInFlight(
      handle:
        InFlightHandle(corrId: -1'i32, fut: nil, apiKey: akProduce, apiVersion: version),
      consumed: false,
    )
  result = ProduceInFlight(
    handle: await b.writeTypedRequest(akProduce, w.buf, version = version),
    consumed: false,
  )

proc produceAwait*(
    b: Broker, h: ProduceInFlight
): Future[Option[ProduceResponse]] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Await the response for a `produceWrite` handle, `none` for the acks=0 sentinel.
  ## Consumes the handle: a later produceAwait/produceAbandon raises or is a no-op.
  if h.consumed:
    raise newException(KafkaError, "produce handle already consumed")
  try:
    if h.handle.corrId < 0:
      return none(ProduceResponse)

    let respBytes = await b.awaitTypedResponse(h.handle)
    var r = initKafkaReader(respBytes)
    let resp = r.decode(ProduceResponse, h.handle.apiVersion)
    b.applyThrottle(resp.throttleTimeMs)
    result = some(resp)
  finally:
    h.consumed = true

proc produceAbandon*(b: Broker, h: ProduceInFlight) {.raises: [].} =
  ## Release a `produceWrite` handle without awaiting its response: drops the in-flight
  ## slot and the `inFlightCount` bump, so an abandoned handle cannot leak a pipeline slot
  ## or stall pruneStaleBrokers. Consumes the handle like `produceAwait`.
  ##
  ## A late response counts as an orphan on the connection (see `orphanResponseCount`).
  ## A no-op on an already-consumed handle and on the acks=0 sentinel.
  if h.consumed:
    return
  h.consumed = true
  if h.handle.corrId < 0:
    return
  let released = h.handle.conn.releaseSlot(h.handle.corrId)
  if released or h.handle.fut.finished:
    dec b.inFlightCount
