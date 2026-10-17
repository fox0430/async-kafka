import std/[tables, options, strutils]
when defined(posix):
  from std/posix import setsockopt, SOL_SOCKET, SO_KEEPALIVE, SockLen, SocketHandle

import pkg/stew/endians2

import protocol/[primitives, api_keys]
import internal/[cancel, safe_logging]
import errors, async_backend

export tlsstream.TrustAnchorStore

type
  HostRuleKind = enum
    hrkExact
    hrkIp4Prefix
    hrkIp6Prefix
    hrkPrivateNetworks

  HostRule* = object
    ## One entry of an allowlist. Carries the address as bytes rather than a
    ## string because one host has several valid spellings (`::ffff:10.0.0.5`
    ## and `10.0.0.5` are the same host), and a string comparison would let the
    ## first satisfy — or evade — a rule written as the second.
    kind: HostRuleKind
    address: array[16, byte]
    prefixLen: int # bits, counted from the first byte
    networks: seq[tuple[base: uint32, bits: int]]
      # hrkPrivateNetworks: IPv4 ranges held at once.
    v6Networks: seq[tuple[base: array[16, byte], bits: int]]
      # hrkPrivateNetworks only: IPv6 ranges held alongside `networks`.

  HostAllowlist* = ref object
    ## Rules the address a broker is actually dialed at must match, built by
    ## `newHostAllowlist`; `nil` (none configured) means "no restriction", and an
    ## advertised host name is resolved with every answer checked. A cluster-level
    ## network policy for plaintext too, not a TLS setting: a broker's advertised
    ## `host` is both the dial target and the certificate name, so a hostile
    ## metadata response can redirect the client to an address whose certificate
    ## TLS would then verify.
    rules: seq[HostRule]

  TlsConfig* = ref object
    ## Connection-level TLS configuration.
    ##
    ## `skipVerify` disables certificate and hostname verification (a warning is
    ## logged per handshake); dev/self-signed paths only — it removes MITM
    ## protection, so **never use it in production**.
    ##
    ## The name verified is the broker's advertised `host`, matched against the
    ## certificate's `dNSName` SANs only (BearSSL): a listener advertising an IP
    ## literal cannot pass verification even with an `iPAddress` SAN for it, so
    ## advertise a DNS name.
    ##
    ## `trustAnchors` — explicit trusted roots; `nil` (default) uses the Mozilla CA
    ## bundle compiled into the TLS backend, which covers public CAs but not a private
    ## one. Supply an explicit `TrustAnchorStore` for those.
    skipVerify*: bool
    trustAnchors*: TrustAnchorStore

  ResponsePayload* = object
    ## Full response frame plus the body-start offset: avoids re-slicing an
    ## MB-scale Fetch response just to skip the header (see `initKafkaReader`).
    frame*: seq[byte]
    bodyStart*: int

  InFlightSlot = object
    future: Future[ResponsePayload]
    flexible: bool
    apiKey: ApiKey

  InFlightHandle* = object
    ## Returned by `writeRequest` after the frame is written but before the
    ## response arrives; the caller awaits it via `awaitResponse`, which is what
    ## lets a producer release a per-partition write-order gate without blocking
    ## on the reply (KIP-185 pipelining).
    ##
    ## `conn` pins the connection so the await resolves the slot on the SAME
    ## connection: the broker layer must not re-read `b.conn` between write and
    ## await, since a concurrent reconnect / force-prune can swap or nil it, which
    ## would nil-deref or release a slot on an unrelated window.
    ##
    ## `corrId < 0` is the fire-and-forget sentinel (acks=0 produce): nil `conn`,
    ## no response. Only `produceAwait` may consume it; `awaitResponse` /
    ## `awaitTypedResponse` raise KafkaError.
    corrId*: int32
    fut*: Future[ResponsePayload]
    apiKey*: ApiKey
    apiVersion*: int16
    conn*: BrokerConnection

  OrphanResponseCallback* = proc(corrId: int32) {.closure, gcsafe, raises: [].}

  InFlightWindow* = ref object
    ## Owns the per-connection request bookkeeping: the slot table keyed by
    ## correlation id, a counting semaphore capping concurrent requests, and
    ## collision-free correlation id allocation.
    ##
    ## Lifecycle: `await available.acquire()` → `assign(...)` → write → await future.
    ## Whichever side resolves the slot first removes it from the table AND releases
    ## the semaphore in one step: `dispatchResponse` (readLoop), `releaseSlot`
    ## (sendRequest error/cancel/timeout), or `failAll` (teardown). `releaseSlot` is
    ## idempotent, so writeRequest's finally block calls it blindly as a safety net.
    slots: OrderedTable[int32, InFlightSlot]
    orphanResponses*: int64
      ## Response frames with no live slot (timed out, cancelled, or never
      ## tracked); a sustained non-zero rate suggests clock skew or a bad broker.
    doubleReleases*: int64
      ## Semaphore double-release attempts caught by `releaseSemaphore`; non-zero
      ## means a slot was resolved twice (a bug) and the connection is degraded.
    onOrphan*: OrphanResponseCallback
      ## Invoked from `dispatchResponse` per dropped orphan — emit a metric or
      ## trigger a reconnect when the rate is high.
    available*: AsyncSemaphore
    nextCorrId: int32
    nextFafCorrId: int32
      ## Separate counter walking int32.low..-1 for fire-and-forget requests, so a
      ## spurious frame for a past fire-and-forget id can only land as an orphan.
    maxInFlight: int
    when defined(kafkaConnTestHooks):
      acquirePark: Future[void].Raising([CancelledError])

  BrokerConnection* = ref object
    transport: StreamTransport
    tlsStream: TLSAsyncStream
    baseReader: AsyncStreamReader
    baseWriter: AsyncStreamWriter
    reader: AsyncStreamReader
    writer: AsyncStreamWriter
    clientId*: string
    inFlight: InFlightWindow
    writeLock: AsyncLock
    readLoopFut: Future[void]
    closed*: bool
      ## Set once the connection is logically unusable (readLoop error paths and
      ## close(); sendRequest short-circuits on it). Does NOT imply resources were
      ## released — see `disposed`.
    disposed*: bool
      ## Set once close() tore down reader / writer / transport. Separate from
      ## `closed` so a readLoop error flipping `closed` does not make a later
      ## close() skip cleanup and leak the socket and chronos streams.
    requestTimeoutMs*: int32
      ## Per-request timeout; non-positive values wait (almost) indefinitely,
      ## bounded by `maxIndefiniteWaitMs`.
    when defined(kafkaConnTestHooks):
      acquirePark: Future[void].Raising([CancelledError])
      readErrorInjector: proc() {.gcsafe, raises: [TransportError, AsyncStreamError].}
        ## Test-only: one-shot failure raised inside the readLoop try boundary.

const
  defaultMaxInFlight* = 5
  defaultMaxFrameSize = 104_857_600 # 100MB
  maxIndefiniteWaitMs = 600_000'i32
    # Even an indefinite wait must be finite: a broker that accepts a request but
    # never replies (half-open TCP, no RST/FIN) would hang the future forever
    # (readLoop blocks in readExactly, failAll never fires). Ten minutes outlasts
    # legitimate broker-side waits such as a slow rebalance.

func effectiveTimeoutMs*(timeoutMs: int32): int32 =
  ## Resolve a configured or per-request timeout to the actual wait budget:
  ## non-positive values are bounded by `maxIndefiniteWaitMs`, positive ones kept.
  if timeoutMs <= 0: maxIndefiniteWaitMs else: timeoutMs

# readLoop's frame-size cast assumes the uint32 wire value fits in `int`. On a
# 32-bit host `int` is `int32`, so bumping defaultMaxFrameSize past int32.high
# would silently wrap the allocation size. Guard at compile time.
static:
  doAssert defaultMaxFrameSize <= int(int32.high)

proc initKafkaReader*(payload: sink ResponsePayload): KafkaReader =
  ## Adopt a `ResponsePayload` as a reader starting at `bodyStart` — no slice
  ## of the frame.
  result.data = payload.frame
  result.pos = payload.bodyStart

proc bodyBytes*(payload: ResponsePayload): seq[byte] =
  ## Copy the response body out as a fresh `seq[byte]`. Tests/diagnostics only —
  ## production decodes via `initKafkaReader(payload)`.
  payload.frame[payload.bodyStart .. payload.frame.high]

proc newInFlightWindow*(maxInFlight: int = defaultMaxInFlight): InFlightWindow =
  if maxInFlight <= 0:
    # KafkaConnectionError rather than KafkaError so `connect`'s narrower raises
    # contract stays intact: the window is built while a connection is set up.
    raise
      newException(KafkaConnectionError, "maxInFlight must be > 0, got " & $maxInFlight)
  InFlightWindow(
    slots: initOrderedTable[int32, InFlightSlot](),
    available: newAsyncSemaphore(maxInFlight),
    nextCorrId: 0,
    nextFafCorrId: -1,
    maxInFlight: maxInFlight,
  )

proc inFlightCount*(w: InFlightWindow): int =
  w.slots.len

proc maxInFlight*(w: InFlightWindow): int =
  w.maxInFlight

proc allocateCorrId(w: InFlightWindow): int32 {.raises: [KafkaProtocolError].} =
  ## Linear scan from `nextCorrId` for an unused id. The semaphore caps the table
  ## at `maxInFlight`, so the scan terminates in `maxInFlight` steps — unlike a
  ## naive `(corrId + 1) and int32.high`, which can collide on wrap-around.
  ## Fire-and-forget ids instead come from the separate negative counter
  ## `allocateFireAndForgetCorrId` walks, so the two id spaces never collide.
  var candidate = w.nextCorrId
  # Defensive; unreachable while nextCorrId is kept in 0..int32.high by the wrap.
  if candidate < 0:
    candidate = 0
  # Route the +1 through int64 so the wrap at int32.high doesn't trip Nim's
  # checked int32 add before the mask lands.
  var attempts = 0
  while w.slots.contains(candidate):
    candidate = int32((int64(candidate) + 1) and int64(int32.high))
    inc attempts
    if attempts > w.maxInFlight:
      raise newException(
        KafkaProtocolError, "could not allocate correlation id (window exhausted)"
      )
  w.nextCorrId = int32((int64(candidate) + 1) and int64(int32.high))
  candidate

proc allocateFireAndForgetCorrId*(w: InFlightWindow): int32 {.raises: [].} =
  ## Correlation id for a fire-and-forget request (no slot, e.g. `acks=0`
  ## produce). The disjoint negative range means a spurious response for a past
  ## fire-and-forget id lands as an orphan, never in a slot a later slotted
  ## request now owns.
  let id = w.nextFafCorrId
  w.nextFafCorrId =
    if id == int32.low:
      -1'i32
    else:
      id - 1'i32
  id

proc assign*(
    w: InFlightWindow, flexible: bool, apiKey: ApiKey
): tuple[corrId: int32, future: Future[ResponsePayload]] {.
    raises: [KafkaProtocolError]
.} =
  ## The caller MUST have acquired `w.available` first; registers the slot and
  ## returns it with the future to await.
  let corrId = w.allocateCorrId()
  let fut = Future[ResponsePayload].init("BrokerConnection.inFlight")
  w.slots[corrId] = InFlightSlot(future: fut, flexible: flexible, apiKey: apiKey)
  (corrId, fut)

proc releaseSemaphore(w: InFlightWindow) {.raises: [].} =
  # A double-release is a caller bug; swallow it rather than crash — the
  # connection is already degraded by then (see `doubleReleases`).
  try:
    w.available.release()
  except AsyncSemaphoreError:
    w.doubleReleases.inc
    warn "semaphore double-release detected (doubleReleases=" & $w.doubleReleases &
      "); connection is degraded"

proc releaseSlot*(w: InFlightWindow, corrId: int32): bool =
  ## Idempotent: removes the slot if present and releases the semaphore. The
  ## writeRequest finally block calls it so cancellation/exception paths never leak.
  var slot: InFlightSlot
  if not w.slots.pop(corrId, slot):
    return false
  w.releaseSemaphore()
  true

when defined(kafkaConnTestHooks):
  # Compile-time-gated park between a synchronous lock/permit take and the await
  # of that completed future, so tests can cancel on the `fut.completed()`
  # self-release path without racing the scheduler. Never enabled in production.
  proc armConnAcquirePark*(conn: BrokerConnection) =
    ## Arm a pending future that `acquireWriteLock` / `acquireInflight` await after
    ## taking the lock or permit. Cancel the acquire while it is pending to hit the
    ## completed-future self-release branch.
    let park = Future[void].Raising([CancelledError]).init("connection.acquirePark")
    conn.acquirePark = park
    conn.inFlight.acquirePark = park

  proc clearConnAcquirePark*(conn: BrokerConnection) =
    ## Drop a leftover park so later acquires are not stranded. Completes a
    ## still-pending park; a cancelled/finished one is just forgotten.
    template drop(p: untyped) =
      if not p.isNil and not p.finished:
        p.complete()
      p = nil

    drop(conn.acquirePark)
    if conn.inFlight != nil:
      drop(conn.inFlight.acquirePark)

template awaitAcquiredOrRelease(
    fut: Future[void], park: untyped, releaseBody: untyped
) =
  ## Await a lock/semaphore acquire future. If cancellation lands after a
  ## synchronous take (`fut.completed()`), run `releaseBody` so the take is
  ## not leaked. A queued waiter is `cancelled()` rather than `completed()`,
  ## so it does not release a permit or lock it never held.
  try:
    when defined(kafkaConnTestHooks):
      if not park.isNil and not park.finished:
        await park
    await fut
  except CancelledError:
    if fut.completed():
      releaseBody
    raise

proc acquireInflight(w: InFlightWindow) {.async: (raises: [CancelledError]).} =
  ## Acquire one in-flight permit. chronos' `AsyncSemaphore.acquire` can take the
  ## permit synchronously and return a completed future with no cancel callback, so
  ## a cancellation at the await point can land *after* the permit was taken —
  ## before the caller registered a slot to release it — leaking the permit until
  ## the window deadlocks. Release it ourselves then; a cancelled queued waiter
  ## yields a `cancelled()` (not `completed()`) future.
  let fut = w.available.acquire()
  awaitAcquiredOrRelease(fut, w.acquirePark):
    w.releaseSemaphore()

proc acquireWriteLock(conn: BrokerConnection) {.async: (raises: [CancelledError]).} =
  ## Acquire the connection's `writeLock`, serializing wire writes per connection.
  ## Same cancellation edge as `acquireInflight`: chronos' `AsyncLock.acquire` can
  ## take the lock synchronously and return a completed future with no cancel
  ## callback, so a cancel at the await point can land *after* the lock was taken —
  ## leaking it and hanging every later write, prune, metadata refresh and close(),
  ## which all block on it. Release it ourselves before propagating.
  let fut = conn.writeLock.acquire()
  awaitAcquiredOrRelease(fut, conn.acquirePark):
    conn.writeLock.releaseSafely("connection.acquireWriteLock")

proc orphanResponseCount*(w: InFlightWindow): int64 {.raises: [].} =
  ## Cumulative count of orphan responses observed on this connection. Reset
  ## when the connection is re-established (a fresh InFlightWindow is created).
  w.orphanResponses

proc doubleReleaseCount*(w: InFlightWindow): int64 {.raises: [].} =
  ## Cumulative count of semaphore double-release attempts caught on this
  ## connection. Reset when the connection is re-established.
  w.doubleReleases

proc setOnOrphan*(w: InFlightWindow, cb: OrphanResponseCallback) {.raises: [].} =
  ## Install (or remove with `nil`) the per-orphan callback. Runs synchronously
  ## inside `dispatchResponse`, so it must not raise or re-enter the connection.
  w.onOrphan = cb

proc dispatchResponse*(
    w: InFlightWindow, corrId: int32, frame: sink seq[byte]
) {.raises: [].} =
  ## readLoop calls this with a full frame body. If a slot exists, parses the
  ## response header (skipping flexible request-header tagged_fields) and completes
  ## its future with the frame + body offset (no slice copy); otherwise the frame is
  ## dropped, the orphan counter incremented and `onOrphan` invoked. A parse error
  ## fails ONLY that slot's future — it never propagates, so one bad response cannot
  ## drag down a multiplexed connection.
  var slot: InFlightSlot
  if not w.slots.pop(corrId, slot):
    w.orphanResponses.inc
    if w.onOrphan != nil:
      w.onOrphan(corrId)
    return

  w.releaseSemaphore()

  if slot.future.finished:
    return
  var bodyStart = 4
  if slot.flexible:
    try:
      bodyStart = skipResponseHeaderTaggedFields(frame, 4)
    except KafkaProtocolError as e:
      warn(
        "flexible response header decode failed corrId=",
        corrId,
        " api=",
        $slot.apiKey,
        ": ",
        e.msg,
      )
      slot.future.fail(
        newException(
          KafkaProtocolError,
          "flexible response header decode failed corrId=" & $corrId & " api=" &
            $slot.apiKey & ": " & e.msg,
        )
      )
      return

  slot.future.complete(ResponsePayload(frame: frame, bodyStart: bodyStart))

proc failAll*(w: InFlightWindow, reason: string) =
  ## Fails every outstanding slot and releases the semaphore once per slot, waking
  ## tasks blocked in `acquire()` so they observe `conn.closed` and bail out.
  var pending: seq[InFlightSlot]
  for _, slot in w.slots.pairs:
    pending.add(slot)
  w.slots.clear()
  for slot in pending:
    w.releaseSemaphore()
    if not slot.future.finished:
      slot.future.fail(newException(KafkaConnectionError, reason))

proc toIp4UInt(ip: IpAddress): uint32 =
  ## An IPv4 address as one big-endian integer, the form both the prefix rules
  ## and `allowPrivateHosts`' network constants are written in.
  let a = ip.address_v4
  (uint32(a[0]) shl 24) or (uint32(a[1]) shl 16) or (uint32(a[2]) shl 8) or uint32(a[3])

proc toIp6Bytes(ip: IpAddress): array[16, byte] =
  ip.address_v6

proc isIp4MappedV6(ip: IpAddress): bool =
  ## True for the `::ffff:a.b.c.d` family, which chronos accepts as a dial
  ## target and which is the same host as the IPv4 address it encodes. Without
  ## this, a string-shaped allowlist has two spellings for one host.
  ip.family == IpAddressFamily.IPv6 and ip.address_v6[0] == 0 and ip.address_v6[1] == 0 and
    ip.address_v6[2] == 0 and ip.address_v6[3] == 0 and ip.address_v6[4] == 0 and
    ip.address_v6[5] == 0 and ip.address_v6[6] == 0 and ip.address_v6[7] == 0 and
    ip.address_v6[8] == 0 and ip.address_v6[9] == 0 and ip.address_v6[10] == 0xff and
    ip.address_v6[11] == 0xff

proc ip4MappedToUInt(ip: IpAddress): uint32 =
  (uint32(ip.address_v6[12]) shl 24) or (uint32(ip.address_v6[13]) shl 16) or
    (uint32(ip.address_v6[14]) shl 8) or uint32(ip.address_v6[15])

proc ip4MappedToIpv4(ip: IpAddress): IpAddress =
  IpAddress(
    family: IpAddressFamily.IPv4,
    address_v4:
      [ip.address_v6[12], ip.address_v6[13], ip.address_v6[14], ip.address_v6[15]],
  )

proc parseIpAddressOrThrow(address, what: string): IpAddress =
  let ip =
    try:
      parseIpAddress(address)
    except CatchableError:
      raise newException(KafkaError, what & " is not a literal IP address: " & address)
  # An IPv4-mapped literal is stored as the IPv4 address it encodes: left in v6
  # form it would compare unequal to the v4 rule an operator wrote, which is the
  # whole reason matching is on parsed bytes.
  if ip.family == IpAddressFamily.IPv6 and ip.isIp4MappedV6():
    return ip.ip4MappedToIpv4()
  ip

proc ipv4Bytes(v: uint32): array[16, byte] =
  ## An IPv4 address in the first 4 bytes of a 16-byte buffer, zero elsewhere,
  ## so IPv4 and IPv6 rules can share one comparison path.
  result[0] = byte((v shr 24) and 0xFF)
  result[1] = byte((v shr 16) and 0xFF)
  result[2] = byte((v shr 8) and 0xFF)
  result[3] = byte(v and 0xFF)

proc ipv4BytesAt(a: array[16, byte]): uint32 =
  (uint32(a[0]) shl 24) or (uint32(a[1]) shl 16) or (uint32(a[2]) shl 8) or uint32(a[3])

proc prefixMatches(candidate, rule: array[16, byte], bits: int): bool =
  ## Compare the first `bits` bits of two 16-byte addresses. Whole bytes first,
  ## then the partial byte, so a /13 is not treated as a /8.
  if bits <= 0:
    return true
  let wholeBytes = bits div 8
  for i in 0 ..< wholeBytes:
    if candidate[i] != rule[i]:
      return false
  let restBits = bits mod 8
  if restBits > 0:
    let mask = byte((0xFF'u16 shl (8 - restBits)) and 0xFF)
    if (candidate[wholeBytes] and mask) != (rule[wholeBytes] and mask):
      return false
  true

proc newTlsConfig*(
    skipVerify: bool = false, trustAnchors: TrustAnchorStore = nil
): TlsConfig =
  ## Construct a TLS config. Defaults to **verification enabled** with no
  ## explicit trust anchors (see `TlsConfig` for what `nil` means). Pass
  ## `skipVerify = true` only for dev/test where MITM exposure is acceptable.
  ##
  ## Which advertised broker addresses may be dialed is a separate, cluster-level
  ## policy: pass a `HostAllowlist` to `newCluster` (not here), so it also covers
  ## plaintext connections.
  TlsConfig(skipVerify: skipVerify, trustAnchors: trustAnchors)

proc allowHost*(address: string): HostRule {.raises: [KafkaError].} =
  ## Rule admitting exactly one address, written as a literal IPv4 or IPv6 address.
  ## A rule names an address, never a name: an advertised host name is resolved and
  ## the resolved address is what the rule is matched against. IPv4-mapped IPv6
  ## spellings are accepted and compared as the IPv4 address they encode.
  let ip = parseIpAddressOrThrow(address, "allowed host")
  if ip.family == IpAddressFamily.IPv4:
    # Stored as the v4 bytes with a full-length prefix, so it is matched by
    # the same path as any other exact rule.
    HostRule(kind: hrkExact, address: ipv4Bytes(ip.toIp4UInt()), prefixLen: 32)
  else:
    HostRule(kind: hrkExact, address: ip.toIp6Bytes(), prefixLen: 128)

proc allowCidr*(cidr: string): HostRule {.raises: [KafkaError].} =
  ## Rule admitting a CIDR block, e.g. `"10.0.0.0/8"` or `"2001:db8::/32"`. A
  ## malformed block raises `KafkaError` at construction: a silently ignored typo'd
  ## prefix would leave the operator believing a narrow allowlist was in force.
  let slash = cidr.find('/')
  if slash < 0:
    raise newException(
      KafkaError,
      "CIDR must contain a prefix length: " & cidr & " (expected e.g. 10.0.0.0/8)",
    )
  let addrStr = cidr[0 ..< slash]
  let lenStr = cidr[slash + 1 .. ^1]
  var bits: int
  try:
    bits = parseInt(lenStr)
  except ValueError:
    raise newException(KafkaError, "CIDR prefix length is not a number: " & cidr)
  let ip = parseIpAddressOrThrow(addrStr, "CIDR base")
  if ip.family == IpAddressFamily.IPv4:
    if bits < 0 or bits > 32:
      raise newException(KafkaError, "IPv4 prefix length must be 0..32, got " & $bits)
    # Store the v4 bytes, not the (all-zero) v6 view: hrkIp4Prefix reads the first
    # four bytes of `address`, so a v6 buffer would compare 0 against every candidate.
    HostRule(kind: hrkIp4Prefix, address: ipv4Bytes(ip.toIp4UInt()), prefixLen: bits)
  else:
    if bits < 0 or bits > 128:
      raise newException(KafkaError, "IPv6 prefix length must be 0..128, got " & $bits)
    HostRule(kind: hrkIp6Prefix, address: ip.toIp6Bytes(), prefixLen: bits)

proc allowPrivateHosts*(): HostRule =
  ## Rule admitting the addresses a same-datacentre or containerised broker fleet
  ## uses: RFC1918 (10/8, 172.16/12, 192.168/16) and loopback (127/8 and ::1/128).
  ## Deliberately EXCLUDES link-local (169.254/16, where cloud instance metadata
  ## lives) and IPv6 unique-local addresses (fc00::/7): the allowlist combines by
  ## OR only, so a broad ULA range would also admit infrastructure addresses
  ## inside it. Combine with allowCidr for other IPv6 ranges as needed.
  for (base, bits) in [
    (0x0A000000'u32, 8), # 10.0.0.0/8
    (0xAC100000'u32, 12), # 172.16.0.0/12
    (0xC0A80000'u32, 16), # 192.168.0.0/16
    (0x7F000000'u32, 8), # 127.0.0.0/8
  ]:
    result.networks.add((base: base, bits: bits))
  const loopback6: array[16, byte] = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]
  result.v6Networks.add((base: loopback6, bits: 128))
  result.kind = hrkPrivateNetworks

proc v4Of(ip: IpAddress): uint32 =
  ## The IPv4 address `ip` denotes, whether written as v4 or as IPv4-mapped v6.
  ## Both are the same host on the wire, and both are dialable, so both are
  ## matched by v4 rules.
  if ip.family == IpAddressFamily.IPv4:
    ip.toIp4UInt()
  else:
    ip.ip4MappedToUInt()

proc v4PrefixMatches(v, network: uint32, bits: int): bool =
  if bits <= 0:
    return true
  if bits > 32:
    # Wider than the value being compared: `32 - bits` is negative, so the shift
    # would wrap to a full mask and turn this into an exact comparison against
    # the wrong bytes. No v4 prefix is longer than 32 by construction.
    return false
  let mask = 0xFFFFFFFF'u32 shl (32 - bits)
  (v and mask) == (network and mask)

proc ruleMatches(rule: HostRule, ip: IpAddress): bool =
  ## Whether one rule admits `ip`. A v4 address — including the IPv4-mapped v6
  ## spelling — is matched by v4 rules; a v6 rule matches only genuine v6, so it
  ## cannot be satisfied by the v4 address it wraps.
  let v4shaped = ip.family == IpAddressFamily.IPv4 or ip.isIp4MappedV6()
  case rule.kind
  of hrkPrivateNetworks:
    if v4shaped:
      let v = v4Of(ip)
      for n in rule.networks:
        if v4PrefixMatches(v, n.base, n.bits):
          return true
      false
    else:
      let candidate = ip.toIp6Bytes()
      for n in rule.v6Networks:
        if prefixMatches(candidate, n.base, n.bits):
          return true
      false
  of hrkIp4Prefix:
    if not v4shaped:
      return false
    v4PrefixMatches(v4Of(ip), ipv4BytesAt(rule.address), rule.prefixLen)
  of hrkIp6Prefix:
    if not (ip.family == IpAddressFamily.IPv6 and not ip.isIp4MappedV6()):
      return false
    prefixMatches(ip.toIp6Bytes(), rule.address, rule.prefixLen)
  of hrkExact:
    # An exact rule stores its length as /32 or /128, so the length says which
    # family it names: a rule matches only an address of its own family, never
    # the leading bytes of its address read as the other one.
    if v4shaped:
      if rule.prefixLen > 32:
        return false
      v4PrefixMatches(v4Of(ip), ipv4BytesAt(rule.address), rule.prefixLen)
    else:
      if rule.prefixLen <= 32:
        return false
      prefixMatches(ip.toIp6Bytes(), rule.address, rule.prefixLen)

proc allowedIp(allowlist: HostAllowlist, ip: IpAddress): bool =
  ## Whether `ip` satisfies any rule; `allowlist` must not be nil.
  for rule in allowlist.rules:
    if ruleMatches(rule, ip):
      return true
  false

proc addressAllowed*(allowlist: HostAllowlist, address: TransportAddress): bool =
  ## Whether a broker may be dialed at `address`; the dial paths apply this after
  ## resolving an advertised host name, matching every answer, so a name cannot
  ## smuggle the client past the rules.
  ##
  ## `nil` permits everything (no allowlist configured, the default). A non-IP
  ## transport is denied: brokers are dialed over IPv4 or IPv6, so any other family
  ## is a caller error rather than a permitted target.
  if allowlist == nil:
    return true
  case address.family
  of AddressFamily.IPv4, AddressFamily.IPv6:
    allowlist.allowedIp(address.toIpAddress())
  else:
    false

proc hostAllowed*(allowlist: HostAllowlist, host: string): bool =
  ## Whether the address literal `host` satisfies the allowlist. For callers that
  ## hold an advertised string rather than a resolved address; the dial paths use
  ## `addressAllowed` after resolving.
  ##
  ## `nil` permits everything; a non-nil list always has at least one rule
  ## (`newHostAllowlist` rejects an empty one). A string that does not parse as an
  ## address is DENIED rather than permitted — a metadata response can carry any
  ## string, so this fails closed and a host name never satisfies a rule on its own.
  if allowlist == nil:
    return true
  var ip: IpAddress
  try:
    ip = parseIpAddress(host)
  except CatchableError:
    return false
  allowlist.allowedIp(ip)

proc newHostAllowlist*(rules: seq[HostRule]): HostAllowlist {.raises: [KafkaError].} =
  ## Build the dial allowlist from `allowHost` / `allowCidr` / `allowPrivateHosts`
  ## rules; pass it to `newCluster` and through it to producers / consumers. Every
  ## resolved dial address is checked before a socket is opened, TLS or plaintext.
  ## The check fails closed: a host name none of whose answers is permitted is
  ## refused, as is one that does not resolve.
  ##
  ## An empty `rules` raises `KafkaError`: it would deny every address, which is never
  ## what the caller meant. Omit the `hostAllowlist` argument to disable the check
  ## explicitly.
  if rules.len == 0:
    raise newException(
      KafkaError,
      "host allowlist must contain at least one rule (build them with " &
        "allowHost / allowCidr / allowPrivateHosts); omit hostAllowlist to " &
        "disable the check",
    )
  HostAllowlist(rules: rules)

proc newBrokerConnection(
    transport: StreamTransport,
    reader: AsyncStreamReader,
    writer: AsyncStreamWriter,
    clientId: string,
    tlsStream: TLSAsyncStream = nil,
    baseReader: AsyncStreamReader = nil,
    baseWriter: AsyncStreamWriter = nil,
    requestTimeoutMs: int32 = 30_000,
    maxInFlight: int = defaultMaxInFlight,
): BrokerConnection =
  BrokerConnection(
    transport: transport,
    tlsStream: tlsStream,
    baseReader: baseReader,
    baseWriter: baseWriter,
    reader: reader,
    writer: writer,
    clientId: clientId,
    inFlight: newInFlightWindow(maxInFlight),
    writeLock: newAsyncLock(),
    closed: false,
    requestTimeoutMs: requestTimeoutMs,
  )

proc inFlightCount*(conn: BrokerConnection): int =
  conn.inFlight.inFlightCount

proc releaseSlot*(conn: BrokerConnection, corrId: int32): bool =
  ## Idempotently release the in-flight slot held by a handle issued on this
  ## connection, returning whether it was present. `awaitResponse` uses the
  ## window-level release directly; this is the surface for a caller settling a
  ## handle it will never await (e.g. `produceAbandon`).
  conn.inFlight.releaseSlot(corrId)

proc maxInFlight*(conn: BrokerConnection): int =
  conn.inFlight.maxInFlight

proc isTls*(conn: BrokerConnection): bool =
  ## True when this connection was established over TLS; the broker layer uses it to
  ## decide whether a SASL mechanism with `requireTls = true` may send credentials.
  conn != nil and conn.tlsStream != nil

proc orphanResponseCount*(conn: BrokerConnection): int64 {.raises: [].} =
  ## Cumulative orphan responses on this connection; reset when re-established.
  if conn == nil: 0 else: conn.inFlight.orphanResponseCount

proc doubleReleaseCount*(conn: BrokerConnection): int64 {.raises: [].} =
  ## Cumulative semaphore double-release attempts on this connection; reset when
  ## re-established.
  if conn == nil: 0 else: conn.inFlight.doubleReleaseCount

proc setOnOrphan*(conn: BrokerConnection, cb: OrphanResponseCallback) {.raises: [].} =
  ## Install a per-orphan callback on this connection's in-flight window.
  if conn != nil:
    conn.inFlight.setOnOrphan(cb)

when defined(posix):
  proc enableKeepAlive(transport: StreamTransport) =
    ## Best-effort TCP keepalive so the OS detects silent brokers. On Linux the
    ## probe timers are tightened (first probe 60s idle, then every 10s, drop
    ## after 3 failures ~= 90s) since the OS default (~2h) is far too slow;
    ## elsewhere only SO_KEEPALIVE is set (per-request timeouts stay the primary
    ## liveness mechanism). Failures are non-fatal.
    let fd = SocketHandle(int(transport.fd))
    try:
      var on: cint = 1
      discard setsockopt(
        fd, cint(SOL_SOCKET), cint(SO_KEEPALIVE), addr on, SockLen(sizeof(on))
      )
    except CatchableError:
      absorbDuringTeardown("connection::enableKeepAlive", "SO_KEEPALIVE opt failed")
    when defined(linux):
      # std/posix does not export these; they are stable Linux ABI values.
      # Level is IPPROTO_TCP.
      const
        tcpKeepIdle = cint(4) # TCP_KEEPIDLE — idle seconds before first probe
        tcpKeepIntvl = cint(5) # TCP_KEEPINTVL — seconds between probes
        tcpKeepCnt = cint(6) # TCP_KEEPCNT — failed probes before drop
        keepIdleSecs = cint(60)
        keepIntvlSecs = cint(10)
        keepProbeCount = cint(3)
      proc setTcpOpt(opt, value: cint) =
        try:
          var v = value
          discard setsockopt(fd, cint(IPPROTO_TCP), opt, addr v, SockLen(sizeof(v)))
        except CatchableError:
          absorbDuringTeardown(
            "connection::enableKeepAlive", "TCP keepalive opt failed", $opt
          )

      setTcpOpt(tcpKeepIdle, keepIdleSecs)
      setTcpOpt(tcpKeepIntvl, keepIntvlSecs)
      setTcpOpt(tcpKeepCnt, keepProbeCount)

type ReadLoopExitKind = enum
  invalidFrameSize
  transportError
  streamError

proc exitReadLoop(
    conn: BrokerConnection, kind: ReadLoopExitKind, detail: string
) {.raises: [].} =
  let reason =
    case kind
    of invalidFrameSize:
      "invalid frame size: " & detail
    of transportError:
      "transport error: " & detail
    of streamError:
      "stream error: " & detail

  # State and in-flight cleanup must not depend on a logger completing. The
  # shared safe logging boundary isolates CatchableError from the handler.
  conn.inFlight.failAll(reason)
  conn.closed = true

  case kind
  of invalidFrameSize:
    warn(
      "Connection for client '", conn.clientId, "' failed: invalid frame size ", detail,
      " (rejecting)",
    )
  of transportError:
    warn(
      "Connection for client '", conn.clientId, "' readLoop died: transport error: ",
      detail,
    )
  of streamError:
    warn(
      "Connection for client '", conn.clientId, "' readLoop died: stream error: ",
      detail,
    )

when defined(kafkaConnTestHooks):
  proc armReadLoopTransportErrorForTest*(conn: BrokerConnection) =
    ## Arm a one-shot failure inside readLoop's own try boundary. Current
    ## Chronos stream adapters normalize raw transport errors, so the catch arm
    ## otherwise has no deterministic public-path trigger.
    conn.readErrorInjector = proc() =
      raise newException(TransportError, "injected transport failure")

  proc readLoopCompletedForTest*(
      conn: BrokerConnection
  ): Future[bool] {.async: (raises: [CancelledError]).} =
    ## `true` means readLoop completed successfully rather than storing a failure.
    await conn.readLoopFut.join()
    result = conn.readLoopFut.completed

proc readLoop(conn: BrokerConnection) {.async: (raises: []).} =
  ## Reads frames and dispatches them to in-flight slots; `inFlight.slots` needs no
  ## lock because chronos is single-threaded and all mutations happen between awaits.
  try:
    while not conn.closed:
      when defined(kafkaConnTestHooks):
        if not conn.readErrorInjector.isNil:
          let injectError = conn.readErrorInjector
          conn.readErrorInjector = nil
          injectError()

      var sizeBuf: array[4, byte]
      await conn.reader.readExactly(addr sizeBuf[0], 4)
      let rawFrameSize = fromBytesBE(uint32, sizeBuf)

      if rawFrameSize < 4'u32 or rawFrameSize > defaultMaxFrameSize.uint32:
        exitReadLoop(conn, invalidFrameSize, $rawFrameSize)
        break

      # Explicit int64 narrowing; the static assert above is the single source of
      # truth for the upper bound (a 32-bit host would wrap if it were raised).
      let frameSize = int(int64(rawFrameSize))
      var frame = newSeq[byte](frameSize)
      await conn.reader.readExactly(addr frame[0], frameSize)

      let corrId = cast[int32](fromBytesBE(uint32, frame.toOpenArray(0, 3)))

      # Unknown / stale / duplicate correlation ids are silently dropped by
      # `dispatchResponse`; per-response parse errors fail only the affected slot.
      conn.inFlight.dispatchResponse(corrId, frame)
  except TransportError as e:
    exitReadLoop(conn, transportError, e.msg)
  except AsyncStreamError as e:
    exitReadLoop(conn, streamError, e.msg)
  except CancelledError:
    conn.inFlight.failAll("connection cancelled")
    conn.closed = true

proc connect*(
    address: TransportAddress,
    clientId: string,
    hostname: string = "",
    tlsConfig: TlsConfig = nil,
    hostAllowlist: HostAllowlist = nil,
    connectTimeoutMs: int32 = 30_000,
    requestTimeoutMs: int32 = 30_000,
    maxInFlight: int = defaultMaxInFlight,
): Future[BrokerConnection] {.async: (raises: [KafkaConnectionError, CancelledError]).} =
  ## Open a broker connection. `connectTimeoutMs` bounds both the TCP connect and
  ## (with `tlsConfig`) the TLS handshake; default 30000. A non-positive value is
  ## clamped to `maxIndefiniteWaitMs`, so a peer that accepts TCP but never speaks
  ## cannot hang the connect forever.
  ##
  ## `hostname` — the broker's advertised address, which under TLS is also the
  ## certificate's subject name. When `hostAllowlist` is set, `address` must match
  ## one of its rules or the connection is refused before any socket is opened, for
  ## plaintext connections as well (TLS cannot bound where a metadata response
  ## redirects the client). Rules are matched against the address actually dialed,
  ## never the name; a `nil` allowlist is a no-op.
  let effectiveConnectTimeoutMs = effectiveTimeoutMs(connectTimeoutMs)

  if not addressAllowed(hostAllowlist, address):
    raise newException(
      KafkaConnectionError,
      "broker address " & $address & " is not permitted by the configured " &
        "host allowlist",
    )

  var transport: StreamTransport
  let connectFut = connect(address)
  try:
    transport = await connectFut.wait(milliseconds(effectiveConnectTimeoutMs))
  except TransportError as e:
    raise newException(
      KafkaConnectionError, "failed to connect to " & $address & ": " & e.msg
    )
  except AsyncTimeoutError:
    await connectFut.cancelAndWait()
    if connectFut.completed:
      await connectFut.value().closeWait()
    raise newException(
      KafkaConnectionError,
      "connection timed out after " & $effectiveConnectTimeoutMs & "ms to " & $address,
    )
  except CancelledError as e:
    # Mirror the AsyncTimeoutError cleanup: if the connect had already completed
    # when the caller cancelled us, close the transport so the socket does not leak.
    await connectFut.cancelAndWait()
    if connectFut.completed:
      await connectFut.value().closeWait()
    raise e

  when defined(posix):
    enableKeepAlive(transport)

  if tlsConfig != nil:
    # No hostname → no verifiable SNI. Silently defaulting to "localhost" hid
    # certificate mismatches, so require skipVerify to opt into it explicitly.
    if hostname.len == 0 and not tlsConfig.skipVerify:
      await transport.closeWait()
      raise newException(
        KafkaConnectionError,
        "TLS requires a hostname for SNI / cert verification (address " & $address &
          "); set skipVerify to bypass",
      )
    let serverName = if hostname.len > 0: hostname else: "localhost"
    var flags: set[TLSFlags] = {}
    if tlsConfig.skipVerify:
      flags.incl(TLSFlags.NoVerifyHost)
      flags.incl(TLSFlags.NoVerifyServerName)
      warn "TLS certificate verification disabled for " & $address &
        " (TlsConfig.skipVerify = true) — vulnerable to MITM; do not use in production"

    let baseReader = newAsyncStreamReader(transport)
    let baseWriter = newAsyncStreamWriter(transport)

    var tlsStream: TLSAsyncStream
    try:
      if tlsConfig.trustAnchors != nil:
        tlsStream = newTLSClientAsyncStream(
          baseReader,
          baseWriter,
          serverName,
          flags = flags,
          trustAnchors = tlsConfig.trustAnchors,
        )
      else:
        tlsStream =
          newTLSClientAsyncStream(baseReader, baseWriter, serverName, flags = flags)
    except AsyncStreamError as e:
      await allFutures(baseReader.closeWait(), baseWriter.closeWait())
      await transport.closeWait()
      raise newException(KafkaConnectionError, "TLS init error: " & e.msg)

    # A proc, not an inline template, so its raises set stays pinned to
    # CancelledError even if chronos changes closeWait signatures.
    proc cleanupTlsAsync(
        ts: TLSAsyncStream,
        br: AsyncStreamReader,
        bw: AsyncStreamWriter,
        tr: StreamTransport,
    ) {.async: (raises: [CancelledError]).} =
      await allFutures(ts.reader.closeWait(), ts.writer.closeWait())
      await allFutures(br.closeWait(), bw.closeWait())
      await tr.closeWait()

    try:
      # connectTimeoutMs must also bound the handshake: a peer that accepts TCP
      # but never speaks TLS (plaintext port over TLS, hung broker) would leave
      # this pending forever — keepalive never fires for an alive-but-silent
      # peer and the caller paths have no outer timeout.
      await tlsStream.handshake().wait(milliseconds(effectiveConnectTimeoutMs))
    except AsyncStreamError as e:
      try:
        await cleanupTlsAsync(tlsStream, baseReader, baseWriter, transport)
      except CancelledError:
        # Surface the original handshake error; a concurrent cancel during
        # cleanup must not mask the TLS failure the caller needs. The connect
        # future still observes cancellation on its next poll.
        absorbDuringTeardown(
          "connection::connect", "cancel during TLS cleanup after a handshake error"
        )
      raise newException(KafkaConnectionError, "TLS handshake error: " & e.msg)
    except AsyncTimeoutError:
      # `wait` already cancelled the handshake future and awaited its teardown,
      # so cleanup only closes the streams and transport. Swallow a cancel
      # during cleanup for the same reason as the AsyncStreamError branch.
      try:
        await cleanupTlsAsync(tlsStream, baseReader, baseWriter, transport)
      except CancelledError:
        absorbDuringTeardown(
          "connection::connect", "cancel during TLS cleanup after a handshake timeout"
        )
      raise newException(
        KafkaConnectionError,
        "TLS handshake timed out after " & $effectiveConnectTimeoutMs & "ms to " &
          $address,
      )
    except CancelledError:
      # A second cancel while cleaning up would leak whatever hadn't closed yet;
      # swallow it here so cleanup completes, then re-raise the original cancel.
      try:
        await cleanupTlsAsync(tlsStream, baseReader, baseWriter, transport)
      except CancelledError:
        absorbDuringTeardown("connection::connect", "second cancel during TLS cleanup")
      raise

    result = newBrokerConnection(
      transport, tlsStream.reader, tlsStream.writer, clientId, tlsStream, baseReader,
      baseWriter, requestTimeoutMs, maxInFlight,
    )
    result.readLoopFut = readLoop(result)
  else:
    let reader = newAsyncStreamReader(transport)
    let writer = newAsyncStreamWriter(transport)
    result = newBrokerConnection(
      transport,
      reader,
      writer,
      clientId,
      requestTimeoutMs = requestTimeoutMs,
      maxInFlight = maxInFlight,
    )
    result.readLoopFut = readLoop(result)

proc buildFrame(
    apiKey: ApiKey,
    apiVersion: int16,
    corrId: int32,
    clientId: string,
    body: seq[byte],
    flexible: bool = false,
): seq[byte] {.raises: [KafkaProtocolError].} =
  var w = initKafkaWriter()
  w.writeInt16(int16(ord(apiKey)))
  w.writeInt16(apiVersion)
  w.writeInt32(corrId)
  # ClientId is always NULLABLE_STRING (never compact), even in flexible headers
  # — the RequestHeader schema sets flexibleVersions "none" for it.
  w.writeNullableString(some(clientId))
  if flexible:
    w.buf.add(0x00'u8) # empty tagged_fields (request header v2)
  w.buf.add(body)

  if w.buf.len > defaultMaxFrameSize:
    raise newException(
      KafkaProtocolError,
      "request frame too large: " & $w.buf.len & " bytes (max " & $defaultMaxFrameSize &
        ")",
    )
  let frameSize = int32(w.buf.len)
  let sizeBytes = toBytesBE(cast[uint32](frameSize))

  result = newSeqOfCap[byte](4 + w.buf.len)
  result.add(sizeBytes)
  result.add(w.buf)

proc writeRequest*(
    conn: BrokerConnection,
    apiKey: ApiKey,
    apiVersion: int16,
    body: seq[byte],
    flexible: bool = false,
): Future[InFlightHandle] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Acquire an in-flight slot, write the request frame, and return a handle to
  ## await separately via `awaitResponse`. Splitting write from wait lets a
  ## producer hold a per-partition write-order gate across only the write while
  ## pipelining up to `maxInFlight` responses (KIP-185).
  ##
  ## On success the slot/semaphore stay held — ownership transfers to the handle,
  ## and the caller MUST eventually call `awaitResponse` (or `releaseSlot`) to
  ## release them. All failure paths clean up the slot before raising.
  if conn.closed:
    raise newException(KafkaConnectionError, "connection is closed")

  # Enforce the in-flight cap. acquireInflight releases the permit if the acquire
  # is cancelled after it was taken; from here to slot registration is synchronous,
  # so no other cancellation window exists.
  await conn.inFlight.acquireInflight()

  # If the connection died while we were queued, abort early — releasing the
  # semaphore we just acquired so other waiters don't deadlock.
  if conn.closed:
    conn.inFlight.releaseSemaphore()
    raise newException(KafkaConnectionError, "connection closed while queued")

  var corrId: int32
  var fut: Future[ResponsePayload]
  try:
    let pair = conn.inFlight.assign(flexible, apiKey)
    corrId = pair.corrId
    fut = pair.future
  except KafkaProtocolError as e:
    conn.inFlight.releaseSemaphore()
    raise e

  # `slotOwned` tracks whether the finally block must still clean up the slot;
  # the success path transfers ownership to the returned handle.
  var slotOwned = true

  template cleanupSlot() =
    if slotOwned:
      slotOwned = false
      discard conn.inFlight.releaseSlot(corrId)

  try:
    let frame =
      try:
        buildFrame(apiKey, apiVersion, corrId, conn.clientId, body, flexible)
      except KafkaProtocolError as e:
        cleanupSlot()
        raise e

    # Serialize wire writes per connection so frames do not interleave.
    await conn.acquireWriteLock()
    # Set when a write error surfaces after `fut` was already completed by
    # readLoop — surfacing the error would let a non-idempotent producer
    # retry a request the broker already accepted.
    var handOffCompletedResponse = false
    # A failed or timed-out write leaves the multiplexed stream at a
    # non-frame boundary, so every later frame is mis-framed. Retire the
    # connection so it is rebuilt, not reused. failAll wakes any awaitResponse
    # parked on a sibling slot instead of making them wait out request timeout.
    template handleWriteFailure(reason: string) =
      conn.closed = true
      if fut.finished:
        handOffCompletedResponse = true
      else:
        cleanupSlot()
        conn.inFlight.failAll(reason)
        raise newException(KafkaConnectionError, reason)

    try:
      # Bound writer.write: an unbounded stuck write holds writeLock until TCP
      # keepalive, stalling close / prune / metadata refresh, which all block on it.
      let writeTimeoutMs = effectiveTimeoutMs(conn.requestTimeoutMs)
      await conn.writer.write(unsafeAddr frame[0], frame.len).wait(
        milliseconds(writeTimeoutMs)
      )
    except AsyncTimeoutError:
      handleWriteFailure("write timed out")
    except TransportError as e:
      handleWriteFailure("write failed: " & e.msg)
    except AsyncStreamError as e:
      handleWriteFailure("write failed: " & e.msg)
    except CancelledError:
      # Cancellation contract wins over an already-delivered response.
      conn.closed = true
      cleanupSlot()
      conn.inFlight.failAll("connection cancelled during write")
      raise
    finally:
      conn.writeLock.releaseSafely("connection.handleWriteFailure")

    if handOffCompletedResponse:
      # readLoop already delivered THIS request's response before the write error
      # surfaced, so the caller gets a valid payload. The transport is unreliable
      # though — conn.closed is set and readLoop will exit, so sibling slots can
      # never be dispatched: failAll wakes their waiters instead of stranding them
      # until their own requestTimeoutMs expires.
      conn.inFlight.failAll("connection closed after response delivered")
      slotOwned = false
      return InFlightHandle(
        corrId: corrId, fut: fut, apiKey: apiKey, apiVersion: apiVersion, conn: conn
      )

    # If the readLoop already exited (server closed before/while we wrote), the
    # future never completes — fail it now rather than waiting for the timeout.
    if conn.closed:
      cleanupSlot()
      raise newException(KafkaConnectionError, "connection closed after write")

    # Success: transfer slot ownership to the handle so the finally below is a
    # no-op and the slot/semaphore survive until awaitResponse resolves them.
    slotOwned = false
    return InFlightHandle(
      corrId: corrId, fut: fut, apiKey: apiKey, apiVersion: apiVersion, conn: conn
    )
  except KafkaError:
    cleanupSlot()
    raise
  except CancelledError:
    cleanupSlot()
    raise
  except CatchableError as e:
    cleanupSlot()
    let err = newException(KafkaConnectionError, e.msg)
    err.parent = e
    raise err
  finally:
    cleanupSlot()

proc awaitResponse*(
    conn: BrokerConnection, handle: InFlightHandle, timeoutMs: int32 = -1
): Future[ResponsePayload] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Await the response for a previously-written request. `timeoutMs`: negative =
  ## connection default; 0 = wait (almost) indefinitely (clamped to
  ## `maxIndefiniteWaitMs`); positive = that many ms. On timeout/cancel/failure the
  ## slot is released via idempotent `releaseSlot`; on success readLoop already
  ## pulled it.
  ##
  ## `conn` MUST be the connection the handle was issued on: slot release and the
  ## timeout default target `handle.conn`, so even under `-d:danger` a mismatched
  ## `conn` cannot free a slot in the wrong window.
  ##
  ## Rejects the fire-and-forget sentinel (`corrId < 0`, acks=0 produce) with a
  ## catchable KafkaError instead of nil-dereferencing its nil `conn`/`fut`.
  if handle.corrId < 0:
    raise newException(
      KafkaError,
      "cannot await a fire-and-forget sentinel handle (" & $handle.apiKey &
        "); acks=0 produce has no response",
    )
  if conn != handle.conn:
    raise newException(KafkaError, "awaitResponse: conn does not match handle.conn")
  let owner = handle.conn
  let configuredTimeoutMs = if timeoutMs < 0: owner.requestTimeoutMs else: timeoutMs
  let effectiveWaitMs = effectiveTimeoutMs(configuredTimeoutMs)
  try:
    result = await handle.fut.wait(milliseconds(effectiveWaitMs))
  except AsyncTimeoutError:
    discard owner.inFlight.releaseSlot(handle.corrId)
    raise newException(
      KafkaTimeoutError,
      "request timed out after " & $effectiveWaitMs & "ms for " & $handle.apiKey,
    )
  except CancelledError:
    discard owner.inFlight.releaseSlot(handle.corrId)
    raise
  except KafkaError:
    # readLoop (failAll / dispatchResponse) already failed the future and pulled
    # the slot, so releaseSlot is a no-op here.
    discard owner.inFlight.releaseSlot(handle.corrId)
    raise
  except CatchableError as e:
    # Defensive: the future could be failed with a non-Kafka CatchableError.
    # Keep the raises contract [KafkaError, CancelledError].
    discard owner.inFlight.releaseSlot(handle.corrId)
    let err = newException(KafkaConnectionError, e.msg)
    err.parent = e
    raise err

proc sendRequest*(
    conn: BrokerConnection,
    apiKey: ApiKey,
    apiVersion: int16,
    body: seq[byte],
    flexible: bool = false,
    timeoutMs: int32 = -1,
): Future[ResponsePayload] {.async: (raises: [KafkaError, CancelledError]).} =
  ## `timeoutMs` overrides `conn.requestTimeoutMs` for this call only (negative =
  ## default; 0 = indefinite; positive = ms), for APIs whose broker-side wait
  ## exceeds the default (e.g. JoinGroup up to `rebalanceTimeoutMs`). Thin
  ## `writeRequest` + `awaitResponse` composition for non-pipelined callers.
  let handle = await conn.writeRequest(apiKey, apiVersion, body, flexible)
  result = await conn.awaitResponse(handle, timeoutMs)

proc sendRequestFireAndForget*(
    conn: BrokerConnection, apiKey: ApiKey, apiVersion: int16, body: seq[byte]
): Future[void] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Fire-and-forget: writes a request without registering a slot, for `acks=0`
  ## produce where Kafka sends no response. Respects the in-flight window for
  ## backpressure but releases the semaphore right after the write.
  if conn.closed:
    raise newException(KafkaConnectionError, "connection is closed")

  # Enforce in-flight cap for backpressure. acquireInflight releases the permit
  # if cancelled after it was taken (see writeRequest).
  await conn.inFlight.acquireInflight()

  if conn.closed:
    conn.inFlight.releaseSemaphore()
    raise newException(KafkaConnectionError, "connection closed while queued")

  # Bump correlation id without registering a slot (no response). Route through
  # the shared allocator so the cursor stays consistent with slotted requests.
  let corrId = conn.inFlight.allocateFireAndForgetCorrId()

  try:
    let frame = buildFrame(apiKey, apiVersion, corrId, conn.clientId, body)

    await conn.acquireWriteLock()
    try:
      let writeTimeoutMs = effectiveTimeoutMs(conn.requestTimeoutMs)
      await conn.writer.write(unsafeAddr frame[0], frame.len).wait(
        milliseconds(writeTimeoutMs)
      )
    except AsyncTimeoutError:
      conn.closed = true
      conn.inFlight.failAll("write timed out")
      raise newException(KafkaConnectionError, "write timed out")
    except TransportError as e:
      # A failed (possibly partial) write desyncs the multiplexed stream; retire the
      # connection so it is rebuilt, not reused, and wake sibling awaitResponse
      # waiters instead of stranding them until request timeout.
      conn.closed = true
      conn.inFlight.failAll("write failed: " & e.msg)
      raise newException(KafkaConnectionError, "write failed: " & e.msg)
    except AsyncStreamError as e:
      conn.closed = true
      conn.inFlight.failAll("write failed: " & e.msg)
      raise newException(KafkaConnectionError, "write failed: " & e.msg)
    except CancelledError:
      # Partial frame may have reached the wire; same desync hazard.
      conn.closed = true
      conn.inFlight.failAll("connection cancelled during write")
      raise
    finally:
      conn.writeLock.releaseSafely("connection.sendRequestFireAndForget")
  finally:
    conn.inFlight.releaseSemaphore()

proc close*(conn: BrokerConnection) {.async: (raises: []).} =
  # `disposed` guards physical teardown so concurrent close()s and
  # readLoop-triggered `closed=true` cannot cause double-free or leak.
  if conn.disposed:
    return

  conn.disposed = true
  conn.closed = true
  conn.inFlight.failAll("connection closed")
  if not conn.readLoopFut.isNil and not conn.readLoopFut.finished:
    await conn.readLoopFut.cancelAndWait()

  # Serialize writer teardown against any in-flight write: a concurrent write that
  # already passed its `conn.closed` check may be suspended inside
  # `conn.writer.write()`, and closing streams underneath it desyncs the stream or
  # trips a chronos assertion. Hold `writeLock` across teardown so close waits for
  # the active write (new writes are rejected by `conn.closed`).
  var writeLockHeld = false
  let lockFut = conn.writeLock.acquire()
  try:
    await lockFut
    writeLockHeld = true
  except CancelledError:
    # Same edge as acquireInflight: chronos may take the lock synchronously and then
    # raise a late CancelledError at the await, so the held lock must be released.
    if lockFut.completed():
      writeLockHeld = true
  except AsyncLockError:
    # Defensive: acquire() can only fail by cancellation, but close() proceeds
    # best-effort either way.
    discard

  # Close order: TLS streams, then base streams, then transport. Chronos' TLS close
  # does NOT cascade to base streams, so all three layers need explicit closing.
  await conn.reader.closeWait()
  await conn.writer.closeWait()
  if not conn.baseReader.isNil:
    await conn.baseReader.closeWait()
  if not conn.baseWriter.isNil:
    await conn.baseWriter.closeWait()
  await conn.transport.closeWait()

  if writeLockHeld:
    conn.writeLock.releaseSafely("connection.close")
