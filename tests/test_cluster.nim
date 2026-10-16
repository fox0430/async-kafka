import std/[unittest, tables, sets, options, strutils, importutils]
import std/logging as stdLogging
from std/times import epochTime

import pkg/stew/endians2

import async_kafka/[async_backend, broker, connection, errors, sasl, sasl_scram]
import async_kafka/cluster {.all.}
import async_kafka/protocol/[primitives, messages, api_keys]

import fake_broker, broker_helpers, cluster_helpers, async_helpers

privateAccess(Cluster)
privateAccess(Broker)

type
  ClusterMetricEvent = tuple[event: string, tags: seq[(string, string)], value: int64]
  ClusterMetricSink = ref object
    events: seq[ClusterMetricEvent]

  ClusterLogSink = ref object of stdLogging.Logger
    messages: seq[string]

method log(
    logger: ClusterLogSink, level: stdLogging.Level, args: varargs[string, `$`]
) {.gcsafe.} =
  ## Captures the diagnostics an application handler would have printed. The
  ## library installs no log handler, so a test that wants to assert on a log
  ## line has to register this and set the filter itself.
  logger.messages.add(args.join(""))

proc makeClusterMetricHook(sink: ClusterMetricSink): MetricsHook =
  result = proc(
      event: cstring, tags: openArray[(cstring, cstring)], value: int64
  ) {.gcsafe, raises: [].} =
    var copied: seq[(string, string)]
    for t in tags:
      copied.add(($t[0], $t[1]))
    sink.events.add((event: $event, tags: copied, value: value))

proc clusterMetricCount(sink: ClusterMetricSink, name: string): int =
  for ev in sink.events:
    if ev.event == name:
      inc result

proc clusterMetricReason(sink: ClusterMetricSink, name: string): string =
  for ev in sink.events:
    if ev.event == name:
      for (k, v) in ev.tags:
        if k == "reason":
          return v

proc clusterMetricTag(sink: ClusterMetricSink, name, key: string): string =
  for ev in sink.events:
    if ev.event == name:
      for (k, v) in ev.tags:
        if k == key:
          return v

suite "Cluster: metadata debounce configuration":
  test "default metadataDebounceInterval is 1 second":
    let c = newCluster(@["127.0.0.1:9092"])
    check c.metadataDebounceInterval == seconds(1)

  test "custom metadataDebounceInterval is stored":
    let c =
      newCluster(@["127.0.0.1:9092"], metadataDebounceInterval = milliseconds(100))
    check c.metadataDebounceInterval == milliseconds(100)

suite "Cluster: metadata lifetime validation":
  test "non-positive metadataMaxAge is rejected":
    ## A non-positive TTL makes the "metadata is fresh" fast path never succeed,
    ## so every lookup would pay a broker round-trip. 0 is not a caching off
    ## switch — unlike the consumer-side `metadataMaxAgeMs`, whose doc says so.
    expect KafkaError:
      discard newCluster(@["127.0.0.1:9092"], metadataMaxAge = seconds(0))
    expect KafkaError:
      discard newCluster(@["127.0.0.1:9092"], metadataMaxAge = seconds(-1))

  test "zero metadataDebounceInterval is accepted, negative is rejected":
    let c = newCluster(@["127.0.0.1:9092"], metadataDebounceInterval = seconds(0))
    check c.metadataDebounceInterval == seconds(0)
    expect KafkaError:
      discard
        newCluster(@["127.0.0.1:9092"], metadataDebounceInterval = milliseconds(-1))

suite "Cluster: backoff config validation":
  test "empty bootstrapAddrs is rejected":
    expect KafkaError:
      discard newCluster(@[])

  test "negative reconnectBackoffMs is rejected":
    expect KafkaError:
      discard newCluster(@["127.0.0.1:9092"], reconnectBackoffMs = -1'i32)

  test "reconnectBackoffMaxMs below reconnectBackoffMs is rejected":
    expect KafkaError:
      discard newCluster(
        @["127.0.0.1:9092"],
        reconnectBackoffMs = 500'i32,
        reconnectBackoffMaxMs = 100'i32,
      )

  test "valid backoff config is accepted":
    let c = newCluster(
      @["127.0.0.1:9092"], reconnectBackoffMs = 50'i32, reconnectBackoffMaxMs = 1000'i32
    )
    check not c.isNil

suite "Cluster: saslMechanisms validation":
  test "nil entries are rejected at construction":
    expect KafkaError:
      discard newCluster(@["127.0.0.1:9092"], saslMechanisms = @[SaslMechanism(nil)])

  test "a list of real candidates is accepted":
    let c = newCluster(
      @["127.0.0.1:9092"],
      saslMechanisms = @[newSaslScram("user", "password", scramSha256)],
    )
    check not c.isNil

suite "Broker: backoff config validation":
  ## newCluster already rejects inverted/negative backoff with KafkaError;
  ## newBroker repeats the predicates so a direct construction fails the same
  ## way instead of aborting the caller's process.
  test "negative reconnectBackoffMs is rejected":
    expect KafkaError:
      discard
        newBroker(0'i32, "127.0.0.1", 9092'i32, "test", reconnectBackoffMs = -1'i32)

  test "reconnectBackoffMaxMs below reconnectBackoffMs is rejected":
    expect KafkaError:
      discard newBroker(
        0'i32,
        "127.0.0.1",
        9092'i32,
        "test",
        reconnectBackoffMs = 500'i32,
        reconnectBackoffMaxMs = 100'i32,
      )

  test "valid backoff config is accepted":
    let b = newBroker(
      0'i32,
      "127.0.0.1",
      9092'i32,
      "test",
      reconnectBackoffMs = 50'i32,
      reconnectBackoffMaxMs = 1000'i32,
    )
    check not b.isNil

suite "Cluster: bootstrap address port validation":
  ## parseAddress used to accept any parseInt result, so newCluster's
  ## int32(port) wrap could dial a different port in release (e.g. 2^32+9092 →
  ## 9092). Anything outside 1..65535 is rejected before that cast. Port 1 stays
  ## valid: connection-refused tests rely on it.

  test "port 65536 is rejected":
    expect KafkaConnectionError:
      discard newCluster(@["127.0.0.1:65536"])

  test "port 0 is rejected":
    expect KafkaConnectionError:
      discard newCluster(@["127.0.0.1:0"])

  test "negative port is rejected":
    expect KafkaConnectionError:
      discard newCluster(@["127.0.0.1:-1"])

  test "IPv6 port 65536 is rejected":
    expect KafkaConnectionError:
      discard newCluster(@["[::1]:65536"])

  test "wraparound port is rejected":
    # 2^32+74 would become 74 under an int32 cast.
    expect KafkaConnectionError:
      discard newCluster(@["127.0.0.1:4294967370"])

  test "port 9092 is accepted":
    let c = newCluster(@["127.0.0.1:9092"])
    check c.bootstrapBrokers[0] == ("127.0.0.1", 9092)

  test "IPv6 port 9092 is accepted":
    let c = newCluster(@["[::1]:9092"])
    check c.bootstrapBrokers[0] == ("::1", 9092)

  test "port 1 remains a valid bootstrap address":
    let c = newCluster(@["127.0.0.1:1"])
    check c.bootstrapBrokers[0].port == 1

suite "Cluster: metadata refresh timeout configuration":
  test "default metadataRefreshTimeoutMs is 0 (derive from requestTimeoutMs)":
    let c = newCluster(@["127.0.0.1:9092"])
    check c.metadataRefreshTimeoutMs == 0'i32

  test "custom metadataRefreshTimeoutMs is stored":
    let c = newCluster(@["127.0.0.1:9092"], metadataRefreshTimeoutMs = 500'i32)
    check c.metadataRefreshTimeoutMs == 500'i32
    check c.metadataRefreshTimeoutBudgetMs() == 500'i64

  test "non-positive request timeout uses the bounded indefinite policy":
    for timeoutMs in [0'i32, -3'i32]:
      let c = newCluster(@["127.0.0.1:9092"], requestTimeoutMs = timeoutMs)
      check c.metadataRefreshTimeoutBudgetMs() == 1_800_000'i64

  test "int32.high request timeout widens the derived budget without overflow":
    let c = newCluster(@["127.0.0.1:9092"], requestTimeoutMs = int32.high)
    check c.metadataRefreshTimeoutBudgetMs() == int64(int32.high) * 3'i64
    check saturatingMultiplyNonNegative(int64.high, 3'i64) == int64.high

  test "positive metadata refresh override takes precedence":
    let c = newCluster(
      @["127.0.0.1:9092"],
      requestTimeoutMs = int32.high,
      metadataRefreshTimeoutMs = 123'i32,
    )
    check c.metadataRefreshTimeoutBudgetMs() == 123'i64

  test "refreshMetadata propagates inner failure through withTimeout":
    ## withTimeout returns bool for finished-vs-timed-out but does not propagate
    ## the wrapped future's exception; treating `completed == true` as success
    ## silently swallowed KafkaConnectionError. The inner future is now re-awaited
    ## so the caller observes the real failure.
    proc testBody() {.async: (raises: [Exception]).} =
      # A refused port fails the inner future with KafkaConnectionError well
      # before the overall timeout elapses.
      let c = newCluster(
        @["127.0.0.1:1"],
        connectTimeoutMs = 5_000,
        requestTimeoutMs = 5_000,
        metadataRefreshTimeoutMs = 2_000,
      )
      var raised = false
      var errMsg = ""
      try:
        await c.refreshMetadata()
      except KafkaError as e:
        raised = true
        errMsg = e.msg
      check raised
      # Must be the actual failure reason, not a timeout wrapper.
      check "failed to fetch metadata" in errMsg or "refused" in errMsg or
        "connection" in errMsg.toLowerAscii()
      await c.close()

    waitFor testBody()

  test "refreshMetadata emits refresh.failed when no broker answers":
    ## Regression: a whole-refresh failure used to raise with no metricsHook
    ## signal. The success `cluster.metadata.refresh` event is the counterpart;
    ## this path must emit `.failed` with reason=no_broker once and must not
    ## emit the success event.
    proc testBody() {.async: (raises: [Exception]).} =
      let sink = ClusterMetricSink(events: @[])
      let logger = ClusterLogSink()
      let previousLogFilter = stdLogging.getLogFilter()
      stdLogging.setLogFilter(stdLogging.lvlAll)
      stdLogging.addHandler(logger)
      try:
        let c = newCluster(
          @["127.0.0.1:1"],
          connectTimeoutMs = 5_000,
          requestTimeoutMs = 5_000,
          metadataRefreshTimeoutMs = 2_000,
          metricsHook = makeClusterMetricHook(sink),
        )
        var raised = false
        try:
          await c.refreshMetadata()
        except KafkaConnectionError:
          raised = true
        check raised
        check clusterMetricCount(sink, "cluster.metadata.refresh") == 0
        check clusterMetricCount(sink, "cluster.metadata.refresh.failed") == 1
        check clusterMetricReason(sink, "cluster.metadata.refresh.failed") == "no_broker"
        for ev in sink.events:
          if ev.event == "cluster.metadata.refresh.failed":
            check ev.value == 1
        # One bootstrap address, so one skip before the whole-refresh .failed.
        # host/port match reconnect tags.
        check clusterMetricCount(sink, "cluster.metadata.refresh.attempt.failed") == 1
        check clusterMetricTag(sink, "cluster.metadata.refresh.attempt.failed", "host") ==
          "127.0.0.1"
        check clusterMetricTag(sink, "cluster.metadata.refresh.attempt.failed", "port") ==
          "1"
        for ev in sink.events:
          if ev.event == "cluster.metadata.refresh.attempt.failed":
            check ev.value == 1
        await c.close()
      finally:
        stdLogging.removeHandler(logger)
        stdLogging.setLogFilter(previousLogFilter)

      # A metric alone left the operator without the reason or the candidate
      # that died: the per-broker failure is a debug line and the whole-refresh
      # failure is a warn that names what was being fetched.
      var sawCandidate, sawRefreshWarn = false
      for message in logger.messages:
        if "Metadata request to bootstrap 127.0.0.1:1 failed" in message:
          sawCandidate = true
        if "Metadata refresh failed: no broker answered" in message:
          sawRefreshWarn = true
      check sawCandidate
      check sawRefreshWarn

    waitFor testBody()

  test "refreshMetadata honors custom metadataRefreshTimeoutMs":
    ## refreshMetadata used a hard-coded `requestTimeoutMs * 3` bound callers
    ## could not override. With the new parameter, a bootstrap broker that accepts
    ## but never replies must raise within the configured budget, not the 90s fallback.
    proc testBody() {.async: (raises: [Exception]).} =
      proc slowCallback(
          server: StreamServer, transport: StreamTransport
      ) {.async: (raises: []).} =
        try:
          while not transport.closed:
            await sleepAsync(milliseconds(50))
        except CancelledError:
          discard
        await transport.closeWait()

      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, slowCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let c = newCluster(
        @[$boundAddr],
        connectTimeoutMs = 10_000,
        requestTimeoutMs = 10_000,
        metadataRefreshTimeoutMs = 200'i32,
      )
      let startMs = epochTime()
      var raised = false
      var errMsg = ""
      try:
        await c.refreshMetadata()
      except KafkaError as e:
        raised = true
        errMsg = e.msg
      let elapsedMs = (epochTime() - startMs) * 1000
      check raised
      check "timed out" in errMsg
      # Must bail out near the 200ms cap, not the 10s request/connect
      # timeout or the pre-fix requestTimeoutMs * 3 fallback (30s here).
      check elapsedMs < 2000.0
      await c.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

proc serveSaslAuthFailure(
    fb: FakeBroker, errorCode: int16, errorMessage: string
): Future[void] {.async: (raises: []).} =
  ## Serve the ApiVersions → SaslHandshake → SaslAuthenticate stack the way a
  ## broker that rejects the credentials does: accept the mechanism, then fail
  ## the authenticate with `errorCode` / `errorMessage`.
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
                apiKey: int16(ord(akSaslHandshake)), minVersion: 0, maxVersion: 1
              ),
              FakeApiVersion(
                apiKey: int16(ord(akSaslAuthenticate)), minVersion: 0, maxVersion: 1
              ),
            ],
          )
        )
      elif key == int16(ord(akSaslHandshake)):
        await req.respond(buildSaslHandshakeResponseBody(0'i16, []))
      elif key == int16(ord(akSaslAuthenticate)):
        await req.respond(
          buildSaslAuthenticateResponseBody(
            errorCode, errorMessage = some(errorMessage)
          )
        )
    except CatchableError:
      break

proc serveApiVersionsAndMetadata(
    fb: FakeBroker, port: int32
): Future[void] {.async: (raises: []).} =
  ## Serve ApiVersions + a Metadata response advertising a single broker
  ## (nodeId 0 at `port`) and no topics.
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
            ],
          )
        )
      elif key == int16(ord(akMetadata)):
        await req.respond(
          buildMetadataResponseBody(
            [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
            controllerId = 0'i32,
            topics = newSeq[FakeTopicMeta](),
          )
        )
    except CatchableError:
      break

suite "Cluster: aggregate failures name the failing candidate":
  ## A Cluster-wide failure used to reach the caller as a bare "failed to fetch
  ## metadata from any broker" / "no broker available": which broker died and
  ## why was a debug line, so a wrong SASL password was indistinguishable from
  ## an unreachable port unless the application installed a log handler.

  test "refreshMetadata carries the broker rejection, the endpoint and the mechanism":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let handlerFut = serveSaslAuthFailure(fb, 58'i16, "Authentication failed")
      let logger = ClusterLogSink()
      let previousLogFilter = stdLogging.getLogFilter()
      stdLogging.setLogFilter(stdLogging.lvlAll)
      stdLogging.addHandler(logger)
      try:
        let c = newCluster(
          @["127.0.0.1:" & $port],
          saslMechanism = newSaslScram("user", "wrong-password", scramSha256),
          metadataDebounceInterval = milliseconds(0),
        )
        var raised = false
        var errMsg = ""
        try:
          await c.refreshMetadata()
        except KafkaConnectionError as e:
          raised = true
          errMsg = e.msg
        check raised
        check "failed to fetch metadata from any broker" in errMsg
        check ("127.0.0.1:" & $port) in errMsg
        check "SASL_AUTHENTICATION_FAILED (58)" in errMsg
        check "SCRAM-SHA-256" in errMsg
        check "Authentication failed" in errMsg
        # The operator-facing warn carries the same cause as the exception.
        var sawWarn = false
        for message in logger.messages:
          if "Metadata refresh failed: no broker answered" in message and
              "SASL_AUTHENTICATION_FAILED (58)" in message:
            sawWarn = true
        check sawWarn
        await c.close()
      finally:
        stdLogging.removeHandler(logger)
        stdLogging.setLogFilter(previousLogFilter)
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "an answered rejection outranks a later transport failure":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let handlerFut = serveSaslAuthFailure(fb, 58'i16, "Authentication failed")
      let mechanism = newSaslScram("user", "wrong-password", scramSha256)
      let c = newCluster(
        @["127.0.0.1:1"], # bootstrap address nothing listens on
        saslMechanism = mechanism,
        connectTimeoutMs = 2_000'i32,
        requestTimeoutMs = 2_000'i32,
        metadataDebounceInterval = milliseconds(0),
      )
      # A known broker that answers and rejects is tried before the bootstrap
      # address; the aggregate must name the rejection, not the refused port.
      c.brokers[7'i32] =
        newBroker(7'i32, "127.0.0.1", port, "cluster-cause", saslMechanism = mechanism)

      var errMsg = ""
      try:
        await c.refreshMetadata()
      except KafkaConnectionError as e:
        errMsg = e.msg
      check "SASL_AUTHENTICATION_FAILED (58)" in errMsg
      check ("cause: 127.0.0.1:" & $port & ": ") in errMsg
      check "cause: 127.0.0.1:1: " notin errMsg

      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "coordinator lookup carries the candidate's rejection":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let handlerFut = serveSaslAuthFailure(fb, 58'i16, "Authentication failed")
      let mechanism = newSaslScram("user", "wrong-password", scramSha256)
      let c = newCluster(
        @["127.0.0.1:" & $port],
        saslMechanism = mechanism,
        metadataDebounceInterval = milliseconds(0),
      )
      # Keep the metadata refresh out of the way so the lookup's own sweep is
      # what fails; the registry holds only the rejecting broker.
      c.markMetadataFreshForTest()
      c.brokers[7'i32] =
        newBroker(7'i32, "127.0.0.1", port, "cluster-cause", saslMechanism = mechanism)

      var raised = false
      var errMsg = ""
      try:
        discard await c.coordinatorBroker("some-group")
      except KafkaError as e:
        raised = true
        errMsg = e.msg
      check raised
      check "failed to find group coordinator for: some-group" in errMsg
      check ("127.0.0.1:" & $port) in errMsg
      check "SASL_AUTHENTICATION_FAILED (58)" in errMsg

      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "anyBroker keeps the refresh cause instead of a bare no-broker error":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let handlerFut = serveSaslAuthFailure(fb, 58'i16, "Authentication failed")
      let c = newCluster(
        @["127.0.0.1:" & $port],
        saslMechanism = newSaslScram("user", "wrong-password", scramSha256),
        metadataDebounceInterval = milliseconds(0),
      )

      var errMsg = ""
      try:
        discard await c.anyBroker()
      except KafkaConnectionError as e:
        errMsg = e.msg
      check "failed to fetch metadata from any broker" in errMsg
      check "SASL_AUTHENTICATION_FAILED (58)" in errMsg

      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "a registry with no reachable broker names the last one tried":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      # The metadata response advertises an address nothing listens on, so the
      # refresh succeeds and every connect after it fails on the transport.
      let handlerFut = serveApiVersionsAndMetadata(fb, 1'i32)
      let c = newCluster(
        @["127.0.0.1:" & $port],
        connectTimeoutMs = 2_000'i32,
        requestTimeoutMs = 2_000'i32,
        metadataDebounceInterval = milliseconds(0),
      )

      var errMsg = ""
      try:
        discard await c.anyBroker()
      except KafkaConnectionError as e:
        errMsg = e.msg
      check errMsg.startsWith("no broker available (cause: 127.0.0.1:1: ")
      check "CONNECTION_ERROR" in errMsg

      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "a broker-controlled message cannot forge lines or inflate the error":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let handlerFut =
        serveSaslAuthFailure(fb, 58'i16, "bad\r\ncredentials " & repeat("x", 500))
      let c = newCluster(
        @["127.0.0.1:" & $port],
        saslMechanism = newSaslScram("user", "wrong-password", scramSha256),
        metadataDebounceInterval = milliseconds(0),
      )

      var errMsg = ""
      try:
        await c.refreshMetadata()
      except KafkaConnectionError as e:
        errMsg = e.msg
      check "bad" in errMsg
      check "credentials" in errMsg
      check "..." in errMsg
      check "\n" notin errMsg
      check "\r" notin errMsg
      # The broker's 500-byte payload is truncated, not carried wholesale.
      check repeat("x", 50) in errMsg
      check repeat("x", 200) notin errMsg

      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Cluster: metadata refresh effective timeout path":
  proc runCase(timeoutMs: int32, viaMaybe: bool) {.async: (raises: [Exception]).} =
    let fb = newFakeBroker()
    let port = int32(fb.address().port)
    let handlerFut = serveApiVersionsAndMetadata(fb, port)
    let c = newCluster(
      @["127.0.0.1:" & $port],
      connectTimeoutMs = 5_000'i32,
      requestTimeoutMs = timeoutMs,
      metadataDebounceInterval = milliseconds(0),
    )

    try:
      if viaMaybe:
        check await c.maybeRefreshMetadata()
      else:
        await c.refreshMetadata()
      check c.brokerCountForTest() == 1
    finally:
      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

  test "refreshMetadata handles non-positive and int32.high request timeouts":
    proc testBody() {.async: (raises: [Exception]).} =
      for timeoutMs in [0'i32, -3'i32, int32.high]:
        await runCase(timeoutMs, viaMaybe = false)

    waitFor testBody()

  test "maybeRefreshMetadata handles non-positive and int32.high request timeouts":
    proc testBody() {.async: (raises: [Exception]).} =
      for timeoutMs in [0'i32, -3'i32, int32.high]:
        await runCase(timeoutMs, viaMaybe = true)

    waitFor testBody()

proc serveTopicSetMetadata(
    fb: FakeBroker, port: int32, topicNames: ref seq[string]
): Future[void] {.async: (raises: []).} =
  ## Serve ApiVersions + Metadata advertising one broker and the (mutable) set
  ## of topics, each with a single partition. Lets a test shrink the cluster's
  ## topic set between full refreshes.
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
            ],
          )
        )
      elif key == int16(ord(akMetadata)):
        var topics: seq[FakeTopicMeta]
        for name in topicNames[]:
          topics.add(
            FakeTopicMeta(
              name: name,
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
          )
        await req.respond(
          buildMetadataResponseBody(
            [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
            controllerId = 0'i32,
            topics = topics,
          )
        )
    except CatchableError:
      break

suite "Cluster: full refresh evicts deleted topics":
  ## Regression: a full metadata refresh updated/added topics but never removed
  ## cached entries for topics absent from the response, so getTopicPartitionsCopy
  ## kept returning a stale partition list for a deleted topic.

  test "a topic dropped from the cluster is evicted from topicPartitions":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let topicNames = new(seq[string])
      topicNames[] = @["a", "b"]
      let handlerFut = serveTopicSetMetadata(fb, port, topicNames)

      let c = newCluster(
        @["127.0.0.1:" & $port],
        clientId = "m4-test",
        metadataDebounceInterval = milliseconds(0),
      )

      await c.refreshMetadata()
      check c.getTopicPartitionsCopy("a").isSome
      check c.getTopicPartitionsCopy("b").isSome

      # Topic "b" is deleted upstream; the next full refresh must evict it.
      topicNames[] = @["a"]
      await c.refreshMetadata(force = true)
      check c.getTopicPartitionsCopy("a").isSome
      check c.getTopicPartitionsCopy("b").isNone

      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Cluster: anyBroker fallback forces a refresh":
  ## Regression: the fallback refresh after all known brokers failed used the
  ## debounced refreshMetadata, so a refresh moments earlier turned it into a
  ## no-op and anyBroker raised "no broker available" without ever trying the
  ## bootstrap addresses. It now forces the refresh.

  test "a recent refresh does not suppress the bootstrap fallback":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let handlerFut = serveApiVersionsAndMetadata(fb, port)

      let c = newCluster(
        @["127.0.0.1:" & $port],
        clientId = "m8-test",
        metadataDebounceInterval = seconds(60),
      )
      # Seed an unreachable known broker and stamp a recent refresh so the
      # debounce would suppress a non-forced fallback refresh.
      c.brokers[99'i32] = newBroker(99'i32, "127.0.0.1", 1'i32, "m8-bogus")
      c.lastMetadataRefresh = Moment.now()

      # anyBroker must still reach bootstrap and return a usable broker.
      let b = await c.anyBroker()
      check b.isConnected

      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Broker: unsupported API carries an error code":
  ## Regression: the per-RPC "unsupported API" guard raised a bare KafkaApiError
  ## whose errorCode stayed keNone (= success), so a caller branching on
  ## errorClass(e.errorCode) would misread it. It now carries
  ## keUnsupportedVersion.

  test "an un-negotiated API raises KafkaApiError with keUnsupportedVersion":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      proc handler() {.async: (raises: []).} =
        # Advertise ONLY ApiVersions, so akMetadata negotiates to -1.
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
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
        except CatchableError:
          discard

      let handlerFut = handler()
      let b = newBroker(0'i32, "127.0.0.1", port, "m6-unsupported")
      await b.connect()

      var code = keNone
      try:
        discard await b.metadata(none(seq[string]))
      except KafkaApiError as e:
        code = e.errorCode
      check code == keUnsupportedVersion

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Broker: address change while disconnected":
  ## Regression: addressDirty was cleared only on the connected-and-dirty path in
  ## ensureConnected, so a broker that changed address while disconnected redialed
  ## correctly but left the flag set — the next ensureConnected then took the slow
  ## path and tore down the freshly-good connection. connectImpl now clears it on success.

  test "reconnect after a disconnected address change is not torn down again":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      proc handler() {.async: (raises: []).} =
        # Respond ApiVersions to every connect handshake (initial + reconnect).
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            await req.respond(
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
      let b = newBroker(0'i32, "127.0.0.1", port, "m5-test")

      await b.connect()
      check b.isReady

      # readLoop died (broker bounce): connection gone, and getOrCreateBroker
      # flagged a new address.
      await b.close()
      check not b.isConnected
      b.markAddressDirty()

      # Reconnect. connectImpl dials and, with the fix, clears addressDirty.
      await b.ensureConnectedForTest()
      check b.isReady
      check not b.isAddressDirty()
      let conn1 = b.connForTest()

      # The next ensureConnected must fast-path: the connection is healthy and no
      # address change is pending. Before the fix the lingering flag forced a
      # close()+reconnect here, swapping the connection out from under callers.
      await b.ensureConnectedForTest()
      let conn2 = b.connForTest()
      check conn2 == conn1

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

proc waitUntilDisposed(
    conn: BrokerConnection, timeout = 1.seconds
): Future[bool] {.async: (raises: [CancelledError]).} =
  ## Drain `detachConn`'s `asyncSpawn old.close()`. `disposed` is set at the
  ## top of `close()`, before failAll and stream teardown.
  let deadline = Moment.now() + timeout
  while Moment.now() < deadline:
    if conn.disposed:
      return true
    await sleepAsync(milliseconds(10))
  return false

suite "Broker: detachConn":
  ## detachConn had no direct tests. The addressDirty reconnect suite above
  ## never calls it — it flags the broker by hand after close(). These pin the
  ## teardown contract: sync return, nil `b.conn`, failAll of in-flight handles,
  ## and `asyncSpawn old.close()` actually disposing the detached connection.

  test "nil connection is a no-op":
    let b = newBroker(0'i32, "127.0.0.1", 9092'i32, "detach-nil")
    b.detachConn()
    check b.connForTest() == nil
    check not b.isConnected

  test "nil connection does not emit connection.detached":
    ## A no-op detach must stay silent. Emitting on nil would count idle
    ## getOrCreateBroker lookups as address-change teardowns.
    let sink = ClusterMetricSink(events: @[])
    let b = newBroker(
      0'i32,
      "127.0.0.1",
      9092'i32,
      "detach-nil-metric",
      metricsHook = makeClusterMetricHook(sink),
    )
    b.detachConn()
    check clusterMetricCount(sink, "broker.connection.detached") == 0

  test "a live connection is torn down and in-flight requests fail":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let parked = new(bool)

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
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
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akMetadata)):
              parked[] = true
          except CatchableError:
            discard

      let handlerFut = handler()
      let b = newBroker(0'i32, "127.0.0.1", port, "detach-inflight")
      await b.connect()
      check b.isReady
      let old = b.connForTest()
      check not old.closed
      check not old.disposed

      let metaFut = b.metadata(none(seq[string]))
      var seen = false
      for _ in 0 ..< 200:
        if parked[]:
          seen = true
          break
        await sleepAsync(milliseconds(5))
      check seen
      check not metaFut.finished

      b.detachConn()
      check b.connForTest() == nil
      check not b.isConnected
      check not b.isReady
      check old.closed

      var failed = false
      try:
        discard await metaFut.wait(2.seconds)
      except KafkaConnectionError:
        failed = true
      check failed
      check await waitUntilDisposed(old)

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "detachConn emits connection.detached once":
    ## Regression: address-change teardown used to be silent except for
    ## reconnect/orphan events. A real detach must emit
    ## broker.connection.detached once with host/port of the detached address.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
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
                    )
                  ],
                )
              )
          except CatchableError:
            discard

      let handlerFut = handler()
      let sink = ClusterMetricSink(events: @[])
      let b = newBroker(
        0'i32,
        "127.0.0.1",
        port,
        "detach-metric",
        metricsHook = makeClusterMetricHook(sink),
      )
      await b.connect()
      check b.isReady
      let old = b.connForTest()
      b.detachConn()
      check clusterMetricCount(sink, "broker.connection.detached") == 1
      for ev in sink.events:
        if ev.event == "broker.connection.detached":
          check ev.value == 1
          check ev.tags == @[("host", "127.0.0.1"), ("port", $port)]
      check await waitUntilDisposed(old)

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "an orphan response is reported with its correlation id":
    ## The read loop drops a response frame that matches no live slot. The
    ## broker installs the callback that turns it into an event; the value is
    ## the orphaned correlation id, so a client/server id mismatch can be told
    ## apart from a timeout.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      # Answer only the handshake: the frames for the request below are driven
      # from the test, which needs the captured request to itself.
      proc handshake() {.async: (raises: []).} =
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

      let handlerFut = handshake()
      let sink = ClusterMetricSink(events: @[])
      let b = newBroker(
        0'i32,
        "127.0.0.1",
        port,
        "orphan-metric",
        metricsHook = makeClusterMetricHook(sink),
      )
      await b.connect()
      check b.isReady

      let conn = b.connForTest()
      let pending = conn.sendRequest(akApiVersions, 0'i16, @[])
      let req = await fb.nextRequest()
      await req.respondWithCorrelationId(0x10001'i32, @[0xAA'u8])
      await sleepAsync(milliseconds(20))
      check clusterMetricCount(sink, "connection.orphan_response") == 1
      for ev in sink.events:
        if ev.event == "connection.orphan_response":
          check ev.value == 0x10001'i64
          check ev.tags == @[("host", "127.0.0.1"), ("port", $port)]

      # The awaited response still resolves, and it is not an orphan.
      await req.respond(@[0x42'u8])
      check (await pending).bodyBytes == @[0x42'u8]
      check clusterMetricCount(sink, "connection.orphan_response") == 1

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "getOrCreateBroker detaches on address change only":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
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
                    )
                  ],
                )
              )
          except CatchableError:
            discard

      let handlerFut = handler()
      let c = newCluster(@["127.0.0.1:" & $port], clientId = "detach-gocb")
      let b = newBroker(0'i32, "127.0.0.1", port, "detach-gocb")
      await b.connect()
      check b.isReady
      c.registerBrokerForTest(b)
      let old = b.connForTest()

      let same = c.getOrCreateBroker(b.nodeId, b.host, b.port)
      check same == b
      check b.connForTest() == old
      check not old.closed
      check not b.isAddressDirty()

      let moved = c.getOrCreateBroker(b.nodeId, "10.0.0.1", 19092'i32)
      check moved == b
      check b.host == "10.0.0.1"
      check b.port == 19092'i32
      check b.isAddressDirty()
      check b.connForTest() == nil
      check not b.isConnected
      check old.closed
      check await waitUntilDisposed(old)

      await b.close()
      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "getOrCreateBroker emits connection.detached on address change only":
    ## Same-address lookup must not emit; host/port change must emit once
    ## tagged with the *old* address (detachConn runs before getOrCreateBroker
    ## overwrites them).
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
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
                    )
                  ],
                )
              )
          except CatchableError:
            discard

      let handlerFut = handler()
      let sink = ClusterMetricSink(events: @[])
      let c = newCluster(@["127.0.0.1:" & $port], clientId = "detach-gocb-metric")
      let b = newBroker(
        0'i32,
        "127.0.0.1",
        port,
        "detach-gocb-metric",
        metricsHook = makeClusterMetricHook(sink),
      )
      await b.connect()
      check b.isReady
      c.registerBrokerForTest(b)
      let old = b.connForTest()
      let oldPort = $port

      discard c.getOrCreateBroker(b.nodeId, b.host, b.port)
      check clusterMetricCount(sink, "broker.connection.detached") == 0

      discard c.getOrCreateBroker(b.nodeId, "10.0.0.1", 19092'i32)
      check clusterMetricCount(sink, "broker.connection.detached") == 1
      for ev in sink.events:
        if ev.event == "broker.connection.detached":
          check ev.value == 1
          check ev.tags == @[("host", "127.0.0.1"), ("port", oldPort)]
      check await waitUntilDisposed(old)

      await b.close()
      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Cluster: close() serializes against in-flight refresh":
  ## Regression: close() cleared the broker table without stateLock, so a metadata
  ## refresh parked in its bootstrap-fallback await could resume after close(),
  ## adopt the bootstrap connection into c.brokers, and leak a socket + readLoop.
  ## close() now acquires stateLock so the refresh completes before the table is cleared.

  test "a refresh adopting a bootstrap broker cannot repopulate after close()":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      let parked = new(bool) # set once the refresh is awaiting the metadata reply
      let release = new(bool) # set by the test to let the handler reply
      parked[] = false
      release[] = false

      proc handler() {.async: (raises: []).} =
        try:
          # ApiVersions handshake for the bootstrap connection.
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
                ],
              )
            )
          except CatchableError:
            discard
          # Metadata request: the refresh is now parked awaiting our reply
          # (holding stateLock). Hold the reply until the test releases us, so
          # close() is invoked while the refresh is mid-flight.
          let md = await fb.nextRequest(timeout = 2.seconds)
          parked[] = true
          while not release[]:
            await sleepAsync(5.milliseconds)
          # Reply with a broker whose host/port match the bootstrap connection,
          # so the adoption path installs the tempBroker into c.brokers.
          try:
            await md.respond(
              buildMetadataResponseBody(
                [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
                controllerId = 0'i32,
                topics = newSeq[FakeTopicMeta](),
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let c = newCluster(@["127.0.0.1:" & $port], clientId = "m4-race")

      # The refresh acquires stateLock, connects to the bootstrap broker, and
      # parks awaiting the metadata reply.
      let refreshFut = c.refreshMetadata(force = true)
      while not parked[]:
        await sleepAsync(5.milliseconds)

      # Invoke close() while the refresh is parked. With the fix it blocks on
      # stateLock; without it, it clears the (currently empty) table and returns
      # immediately, letting the refresh adopt a broker afterwards.
      let closeFut = c.close()
      await sleepAsync(20.milliseconds)
      release[] = true

      await allFutures(refreshFut, closeFut)

      # The adopted broker must not survive close(): either close() waited for
      # the refresh and then cleared it, or the refresh's adoption never raced
      # ahead of the clear. Stale entries here mean a leaked connection.
      check c.brokerCountForTest() == 0

      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "findCoordinatorRunner cannot repopulate c.brokers / c.coordinators after close()":
    ## Regression: an unlocked runner used to resurrect broker + coordinator
    ## entries after close() cleared them (zombie connection unreachable from
    ## close(); readLoop + socket leak). Fix: stateLock-gated write.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      let parked = new(bool)
      let release = new(bool)
      parked[] = false
      release[] = false

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
                    apiKey: int16(ord(akFindCoordinator)), minVersion: 1, maxVersion: 1
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          # Metadata for the coordinator lookup's maybeRefreshMetadata prelude.
          try:
            let md = await fb.nextRequest(timeout = 2.seconds)
            await md.respond(
              buildMetadataResponseBody(
                [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
                controllerId = 0'i32,
                topics = newSeq[FakeTopicMeta](),
              )
            )
          except CatchableError:
            discard
          # FindCoordinator: hold the reply until the test releases us. The
          # runner is parked here while close() runs.
          let fcr = await fb.nextRequest(timeout = 2.seconds)
          parked[] = true
          while not release[]:
            await sleepAsync(5.milliseconds)
          try:
            # Reply with a NEW nodeId (9) whose adoption would leak a socket
            # if the write is not gated.
            await fcr.respond(
              buildFindCoordinatorResponseBodyV1(
                errorCode = 0'i16, nodeId = 9'i32, host = "127.0.0.1", port = port
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let c = newCluster(@["127.0.0.1:" & $port], clientId = "close-fc-race")

      # Kick off the coordinator lookup and wait until the runner is parked on
      # its FindCoordinator reply.
      let joinFut = c.findCoordinatorBroker("g", ckGroup)
      while not parked[]:
        await sleepAsync(5.milliseconds)

      let closeFut = c.close()
      # Give close() time to acquire stateLock, retire brokers, and clear the
      # tables while the runner is still parked on its reply.
      await sleepAsync(50.milliseconds)
      release[] = true

      # The runner resumes, but its stateLock-gated write must skip. joinFut
      # sees the failed slot and raises. close() completes cleanly.
      var joinRaised = false
      try:
        discard await joinFut
      except KafkaError:
        joinRaised = true
      check joinRaised
      await closeFut

      # The registry stays empty — the runner did not resurrect entries.
      check c.brokerCountForTest() == 0
      check not c.hasCachedCoordinatorForTest("g")

      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "findCoordinatorRunner falls back to bootstrap when all known brokers are unreachable":
    ## Regression: coordinator discovery only probed `snapshotBrokers()` and
    ## raised when every registry broker was unreachable, even though bootstrap
    ## addresses were still serving. A stale registry (broker restarts moved the
    ## cluster while the client kept its old broker list) made consumer group /
    ## transactional coordinator lookups fail until the next metadata refresh
    ## replaced the registry. Mirror of refreshMetadataUnlocked's bootstrap path.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

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
                    apiKey: int16(ord(akFindCoordinator)), minVersion: 1, maxVersion: 1
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          let fc = await fb.nextRequest(timeout = 2.seconds)
          try:
            await fc.respond(
              buildFindCoordinatorResponseBodyV1(
                errorCode = 0'i16, nodeId = 0'i32, host = "127.0.0.1", port = port
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let sink = ClusterMetricSink(events: @[])
      let c = newCluster(
        @["127.0.0.1:" & $port],
        clientId = "bootstrap-fallback",
        metricsHook = makeClusterMetricHook(sink),
      )

      # Registry holds only an unreachable broker (port 1 refuses connections);
      # metadata is stamped fresh so the lookup skips the refresh prelude and
      # goes straight to coordinator discovery with the stale registry.
      registerBrokerForTest(
        c, newBroker(42'i32, "127.0.0.1", 1'i32, "bootstrap-fallback")
      )
      markMetadataFreshForTest(c)

      let coord = await c.findCoordinatorBroker("g", ckGroup)
      check coord.host == "127.0.0.1"
      check coord.port == port
      check coord.nodeId == 0'i32
      check c.hasCachedCoordinatorForTest("g")
      # Known-broker continue is a different site from bootstrap. One refuse
      # then a live bootstrap must emit attempt.failed once and must not mix
      # refresh events.
      check clusterMetricCount(sink, "cluster.coordinator.lookup.attempt.failed") == 1
      check clusterMetricTag(sink, "cluster.coordinator.lookup.attempt.failed", "host") ==
        "127.0.0.1"
      check clusterMetricTag(sink, "cluster.coordinator.lookup.attempt.failed", "port") ==
        "1"
      for ev in sink.events:
        if ev.event == "cluster.coordinator.lookup.attempt.failed":
          check ev.value == 1
      check clusterMetricCount(sink, "cluster.metadata.refresh.attempt.failed") == 0
      check clusterMetricCount(sink, "cluster.metadata.refresh.failed") == 0
      # Bootstrap answered, so the whole-lookup .failed must not fire.
      check clusterMetricCount(sink, "cluster.coordinator.lookup.failed") == 0

      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "a retriable FindCoordinator answer is retried before failing":
    ## Regression: COORDINATOR_LOAD_IN_PROGRESS (and the other retriable
    ## coordinator codes) were raised straight out of the lookup, so start()
    ## failed while __consumer_offsets was still loading. The answering broker
    ## is retried with backoff.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      var lookupCalls = 0

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
                    apiKey: int16(ord(akFindCoordinator)), minVersion: 1, maxVersion: 1
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          # Two "not ready" answers, then the coordinator.
          for attempt in 0 .. 2:
            let fc = await fb.nextRequest(timeout = 2.seconds)
            inc lookupCalls
            try:
              if attempt < 2:
                await fc.respond(
                  buildFindCoordinatorResponseBodyV1(
                    errorCode = int16(ord(keCoordinatorLoadInProgress))
                  )
                )
              else:
                await fc.respond(
                  buildFindCoordinatorResponseBodyV1(
                    errorCode = 0'i16, nodeId = 0'i32, host = "127.0.0.1", port = port
                  )
                )
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let sink = ClusterMetricSink(events: @[])
      let c = newCluster(
        @["127.0.0.1:" & $port],
        clientId = "coord-retry",
        metricsHook = makeClusterMetricHook(sink),
      )
      # Registry holds only an unreachable broker (port 1 refuses connections);
      # metadata is stamped fresh so the lookup skips the refresh prelude and goes
      # straight to coordinator discovery with the stale registry, falling back to
      # the bootstrap address that answers.
      registerBrokerForTest(c, newBroker(42'i32, "127.0.0.1", 1'i32, "coord-retry"))
      markMetadataFreshForTest(c)

      let coord = await c.findCoordinatorBroker("g", ckGroup)
      check lookupCalls == 3
      check coord.host == "127.0.0.1"
      check coord.port == port
      check c.hasCachedCoordinatorForTest("g")
      check clusterMetricCount(sink, "cluster.coordinator.lookup.failed") == 2

      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "findCoordinatorBroker emits attempt.failed when no broker answers":
    ## Bootstrap continue is a different site from the known-broker loop. Fresh
    ## metadata skips the refresh prelude so only the coordinator runner's skip
    ## is visible.
    proc testBody() {.async: (raises: [Exception]).} =
      let sink = ClusterMetricSink(events: @[])
      let c = newCluster(
        @["127.0.0.1:1"],
        connectTimeoutMs = 5_000,
        requestTimeoutMs = 5_000,
        metricsHook = makeClusterMetricHook(sink),
      )
      markMetadataFreshForTest(c)
      var raised = false
      var errMsg = ""
      try:
        discard await c.findCoordinatorBroker("g", ckGroup)
      except KafkaError as e:
        raised = true
        errMsg = e.msg
      check raised
      check "failed to find" in errMsg
      check clusterMetricCount(sink, "cluster.coordinator.lookup.attempt.failed") == 1
      check clusterMetricTag(sink, "cluster.coordinator.lookup.attempt.failed", "host") ==
        "127.0.0.1"
      check clusterMetricTag(sink, "cluster.coordinator.lookup.attempt.failed", "port") ==
        "1"
      for ev in sink.events:
        if ev.event == "cluster.coordinator.lookup.attempt.failed":
          check ev.value == 1
      check clusterMetricCount(sink, "cluster.metadata.refresh.attempt.failed") == 0
      check clusterMetricCount(sink, "cluster.metadata.refresh.failed") == 0
      # One bootstrap address died, then the runner raises. Counterpart to
      # refresh.failed / reason=no_broker.
      check clusterMetricCount(sink, "cluster.coordinator.lookup.failed") == 1
      check clusterMetricReason(sink, "cluster.coordinator.lookup.failed") == "no_broker"
      for ev in sink.events:
        if ev.event == "cluster.coordinator.lookup.failed":
          check ev.value == 1
      await c.close()

    waitFor testBody()

  test "findCoordinatorBroker emits lookup.failed when FindCoordinator returns an error":
    ## A live broker can answer FindCoordinator with errorCode != keNone. That
    ## is not a candidate skip and not no_broker; refresh.failed's empty_brokers
    ## counterpart. Same event, reason=api_error. The code is non-retriable on
    ## purpose: a retriable one is retried before failing (see the retry test),
    ## which is a different path.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

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
                    apiKey: int16(ord(akFindCoordinator)), minVersion: 1, maxVersion: 1
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          let fc = await fb.nextRequest(timeout = 2.seconds)
          try:
            await fc.respond(
              buildFindCoordinatorResponseBodyV1(
                errorCode = keGroupAuthorizationFailed.toInt16,
                nodeId = 0'i32,
                host = "127.0.0.1",
                port = port,
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let sink = ClusterMetricSink(events: @[])
      let c = newCluster(
        @["127.0.0.1:" & $port],
        clientId = "coord-api-error",
        metricsHook = makeClusterMetricHook(sink),
      )
      markMetadataFreshForTest(c)

      var raised = false
      var code = keNone
      try:
        discard await c.findCoordinatorBroker("g", ckGroup)
      except KafkaApiError as e:
        raised = true
        code = e.errorCode
      check raised
      check code == keGroupAuthorizationFailed
      check clusterMetricCount(sink, "cluster.coordinator.lookup.failed") == 1
      check clusterMetricReason(sink, "cluster.coordinator.lookup.failed") == "api_error"
      for ev in sink.events:
        if ev.event == "cluster.coordinator.lookup.failed":
          check ev.value == 1
      check clusterMetricCount(sink, "cluster.coordinator.lookup.attempt.failed") == 0
      check clusterMetricCount(sink, "cluster.metadata.refresh.attempt.failed") == 0
      check clusterMetricCount(sink, "cluster.metadata.refresh.failed") == 0

      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "close() fails pendingRefreshes slots so joiners do not wait for the runner":
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])

      let slot = newFuture[void]("cluster.invalidateLeader")
      c.pendingRefreshes["t"] = slot

      await c.close()

      check slot.finished
      check slot.failed
      check slot.error of KafkaClosedError
      check c.pendingRefreshes.len == 0

    waitFor testBody()

  test "close() unblocks a joiner parked in joinSharedSlot on a pendingRefreshes slot":
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])

      let slot = newFuture[void]("cluster.invalidateLeader")
      c.pendingRefreshes["t"] = slot

      let tp = TopicPartition(topic: "t", partition: 0)
      let joinFut = c.invalidateLeader(tp, keNotLeaderOrFollower)
      await sleepAsync(20.milliseconds)

      await c.close()

      var joinRaised = false
      try:
        discard await joinFut
      except KafkaError:
        joinRaised = true
      check joinRaised
      check c.pendingRefreshes.len == 0

    waitFor testBody()

suite "Cluster: stale broker cleanup":
  test "pruneStaleBrokers removes brokers not in active set":
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])
      let b1 = newBroker(1, "host1", 9092, "test")
      let b2 = newBroker(2, "host2", 9092, "test")
      let b3 = newBroker(3, "host3", 9092, "test")
      c.brokers[1] = b1
      c.brokers[2] = b2
      c.brokers[3] = b3

      await c.pruneStaleBrokers(toHashSet([1'i32, 3'i32]))

      check c.brokers.len == 2
      check 1'i32 in c.brokers
      check 3'i32 in c.brokers
      check 2'i32 notin c.brokers

    waitFor testBody()

  test "pruneStaleBrokers cleans coordinator caches":
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])
      let b1 = newBroker(1, "host1", 9092, "test")
      let b2 = newBroker(2, "host2", 9092, "test")
      c.brokers[1] = b1
      c.brokers[2] = b2

      c.coordinators["group-a"] = 1'i32
      c.coordinators["group-b"] = 2'i32
      c.txnCoordinators["txn-x"] = 2'i32

      await c.pruneStaleBrokers(toHashSet([1'i32]))

      check c.brokers.len == 1
      check c.coordinators.len == 1
      check "group-a" in c.coordinators
      check "group-b" notin c.coordinators
      check c.txnCoordinators.len == 0

    waitFor testBody()

  test "pruneStaleBrokers keeps all brokers when all active":
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])
      let b1 = newBroker(1, "host1", 9092, "test")
      let b2 = newBroker(2, "host2", 9092, "test")
      c.brokers[1] = b1
      c.brokers[2] = b2

      await c.pruneStaleBrokers(toHashSet([1'i32, 2'i32]))

      check c.brokers.len == 2

    waitFor testBody()

  test "pruneStaleBrokers handles empty broker table":
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])

      await c.pruneStaleBrokers(toHashSet([1'i32]))

      check c.brokers.len == 0

    waitFor testBody()

  test "pruneStaleBrokers keeps a broker with in-flight requests":
    ## pruneStaleBrokers used to close every broker whose nodeId left the active
    ## set, including ones with a caller still awaiting sendTypedRequest —
    ## cancelling the in-flight request. The broker now stays in the registry until
    ## inFlightRequests drains; the next refresh prunes only the quiescent ones.
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])
      let b1 = newBroker(1, "host1", 9092, "test")
      let b2 = newBroker(2, "host2", 9092, "test")
      c.brokers[1] = b1
      c.brokers[2] = b2
      b2.setInFlightForTesting(1)

      # MetadataResponse no longer mentions either id, but the prune
      # should keep b2 alive because something is awaiting it.
      await c.pruneStaleBrokers(toHashSet(newSeq[int32]()))
      check 1'i32 notin c.brokers
      check 2'i32 in c.brokers

      # Once the caller finishes, the next prune removes it.
      b2.setInFlightForTesting(0)
      await c.pruneStaleBrokers(toHashSet(newSeq[int32]()))
      check 2'i32 notin c.brokers

    waitFor testBody()

  test "pruneStaleBrokers keeps a broker mid-handshake":
    ## ApiVersions and SASL traffic during connectImpl bypass sendTypedRequest, so
    ## they never bump inFlightCount. Without the handshaking flag a prune would
    ## treat the broker as idle and tear it down mid-handshake, nil-ing b.conn.
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])
      let b1 = newBroker(1, "host1", 9092, "test")
      c.brokers[1] = b1
      # TCP up, SASL handshake still in progress: inFlightCount stays 0 because
      # handshake traffic bypasses sendTypedRequest.
      check b1.inFlightRequests == 0
      b1.setHandshaking(true)

      await c.pruneStaleBrokers(toHashSet(newSeq[int32]()))
      check 1'i32 in c.brokers

      # When the handshake completes, the next prune cycle reclaims it.
      b1.setHandshaking(false)
      await c.pruneStaleBrokers(toHashSet(newSeq[int32]()))
      check 1'i32 notin c.brokers

    waitFor testBody()

  test "pruneStaleBrokers does not see a caller parked in the throttle wait":
    ## The third state that is invisible to the prune: the throttle wait runs
    ## before the request is written and before inFlightCount is bumped, so a
    ## broker absent from the active set looks idle and is retired at once (no
    ## grace, unlike the in-flight case above). Nothing has been written at that
    ## point, so the parked caller surfaces a connection error for its retry
    ## loop instead of losing a request that was already on the wire.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

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
                  apiKey: int16(ord(akOffsetCommit)), minVersion: 0, maxVersion: 3
                ),
              ],
            )
          )
          # The parked request must never reach the wire: it is retired first.
          discard await fb.nextRequest(timeout = 1.seconds)
        except CatchableError:
          discard

      let handlerFut = handler()
      let b = newBroker(
        0'i32, "127.0.0.1", port, "throttle-prune-test", requestTimeoutMs = 5000
      )
      await b.connect()
      # Short enough that the caller waits the throttle out rather than timing
      # out on it: the park is the window this test is about.
      b.applyThrottle(500'i32)

      let c = newCluster(@["127.0.0.1:9092"])
      c.registerBrokerForTest(b)

      let req = OffsetCommitRequest(
        groupId: "g",
        generationId: -1'i32,
        memberId: "",
        retentionTimeMs: -1'i64,
        topics: @[
          OffsetCommitTopicReq(
            topic: "t",
            partitions: @[
              OffsetCommitPartitionReq(
                partition: 0'i32, offset: 1'i64, metadata: none(string)
              )
            ],
          )
        ],
      )
      let commitFut = b.offsetCommit(req)
      await sleepAsync(50.milliseconds) # the call is parked in awaitThrottle
      check b.inFlightRequests == 0 # invisible to the prune

      # Nothing in the active set, nothing in flight: retired without the grace.
      await c.pruneStaleBrokers(toHashSet(newSeq[int32]()))
      check 0'i32 notin c.brokers
      check b.isRetired

      var msg = ""
      try:
        discard await commitFut
      except KafkaConnectionError as e:
        msg = e.msg
      check "before request could be sent" in msg

      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "pruneStaleBrokers uses the effective non-positive request timeout":
    ## A zero request timeout means the same bounded indefinite wait as the
    ## connection layer, not a zero-length grace window that force-prunes on the
    ## first cycle.
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"], requestTimeoutMs = 0'i32)
      let b1 = newBroker(1, "host1", 9092, "test")
      c.brokers[1] = b1
      b1.setInFlightForTesting(1)
      b1.setFirstSkippedPruneAt(Moment.now() - seconds(2))

      await c.pruneStaleBrokers(toHashSet(newSeq[int32]()))

      check 1'i32 in c.brokers
      await c.close()

    waitFor testBody()

  test "pruneStaleBrokers force-closes a broker whose in-flight never drains":
    ## Backstop for the keep-on-in-flight policy: a wedged request must
    ## not leak a broker indefinitely. Once the stale-since timestamp
    ## predates the grace window, the prune reclaims the slot regardless
    ## of the in-flight count.
    proc testBody() {.async: (raises: [Exception]).} =
      let sink = ClusterMetricSink(events: @[])
      let c = newCluster(@["127.0.0.1:9092"], metricsHook = makeClusterMetricHook(sink))
      let b1 = newBroker(1, "host1", 9092, "test")
      c.brokers[1] = b1
      b1.setInFlightForTesting(1)
      # Pretend this broker has been stuck stale far longer than the
      # grace window (requestTimeoutMs * 2, here 60s by default).
      b1.setFirstSkippedPruneAt(Moment.now() - milliseconds(10 * 60 * 1000))

      await c.pruneStaleBrokers(toHashSet(newSeq[int32]()))

      check 1'i32 notin c.brokers
      # A forced removal is the only prune that discards in-flight work, so it
      # is the only one that reports itself.
      check clusterMetricCount(sink, "cluster.broker.force_pruned") == 1
      check clusterMetricTag(sink, "cluster.broker.force_pruned", "nodeId") == "1"

    waitFor testBody()

  test "pruneStaleBrokers defers force-close while a widened-timeout request is legitimately in flight":
    ## JoinGroup / SyncGroup widen the response deadline to
    ## rebalanceTimeoutMs + requestTimeoutMs, which can exceed the default
    ## graceMs floor of requestTimeoutMs * 2. sendTypedRequest records that
    ## widened deadline on the broker; pruneStaleBrokers must respect it and
    ## not force-retire a normally-progressing join.
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])
      let b1 = newBroker(1, "host1", 9092, "test")
      c.brokers[1] = b1
      b1.setInFlightForTesting(1)
      # Stale-since predates the grace floor: the pre-fix code would force-retire.
      b1.setFirstSkippedPruneAt(Moment.now() - milliseconds(10 * 60 * 1000))
      # But a widened JoinGroup timeout is still legitimately outstanding.
      b1.latestInFlightDeadline = Moment.now() + milliseconds(60 * 1000)

      await c.pruneStaleBrokers(toHashSet(newSeq[int32]()))

      check 1'i32 in c.brokers

      # Once that deadline elapses, the next prune reclaims the wedged slot.
      b1.latestInFlightDeadline = Moment.now() - milliseconds(1)
      await c.pruneStaleBrokers(toHashSet(newSeq[int32]()))
      check 1'i32 notin c.brokers

    waitFor testBody()

  test "pruneStaleBrokers never force-closes a broker busy only with acks=0 writes":
    ## acks=0 fire-and-forget writes bump a dedicated counter, not the response
    ## in-flight count. The force-prune backstop reclaims a wedged response-waiter,
    ## but a fire-and-forget write has no response and no retry, so force-closing it
    ## would silently drop the record. Such a broker must stay registered until drained.
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])
      let b1 = newBroker(1, "host1", 9092, "test")
      c.brokers[1] = b1
      # An acks=0 write is on the wire; no response is awaited.
      b1.setFireAndForgetForTesting(1)
      check b1.inFlightRequests == 0
      # Pretend the broker has been stale far longer than the force-prune
      # grace window — a wedged response-waiter would be reclaimed here.
      b1.setFirstSkippedPruneAt(Moment.now() - milliseconds(10 * 60 * 1000))

      await c.pruneStaleBrokers(toHashSet(newSeq[int32]()))
      # Still kept: the write must not be torn down mid-flight.
      check 1'i32 in c.brokers

      # Once the write drains, the next prune removes it cleanly.
      b1.setFireAndForgetForTesting(0)
      await c.pruneStaleBrokers(toHashSet(newSeq[int32]()))
      check 1'i32 notin c.brokers

    waitFor testBody()

  test "pruneStaleBrokers clears coordinator cache even when broker is kept for in-flight":
    ## Coordinator cache entries pointing at a stale broker are dropped regardless
    ## of in-flight state: the broker stays registered so the in-flight Future can
    ## resolve, but the next coordinator lookup must re-issue FindCoordinator.
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])
      let b1 = newBroker(1, "host1", 9092, "test")
      c.brokers[1] = b1
      c.coordinators["group-a"] = 1'i32
      b1.setInFlightForTesting(1)

      await c.pruneStaleBrokers(toHashSet(newSeq[int32]()))

      # Broker stays in the registry to let the in-flight Future resolve.
      check 1'i32 in c.brokers
      # Coordinator cache cleared so the next lookup re-issues FindCoordinator.
      check "group-a" notin c.coordinators

    waitFor testBody()

  test "pruned broker is retired and refuses to reconnect":
    ## pruneStaleBrokers removes and closes a stale broker, but a caller that
    ## resolved it before the prune still holds a reference. ensureConnected/connect
    ## used to silently revive it, leaking a socket + readLoop the registry no longer
    ## tracks. retire() now makes connectImpl fail fast so the caller re-resolves.
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])
      let b1 = newBroker(1, "host1", 9092, "test")
      c.brokers[1] = b1
      check not b1.isRetired

      await c.pruneStaleBrokers(toHashSet(newSeq[int32]()))
      check 1'i32 notin c.brokers
      check b1.isRetired

      # The still-held reference must not reconnect: connect() fast-fails
      # through connectImpl's retired guard rather than dialing host1:9092.
      var raised = false
      try:
        await b1.connect()
      except KafkaConnectionError:
        raised = true
      check raised

    waitFor testBody()

  test "ensureConnected refuses a retired broker whose conn is still alive":
    ## retire() sets b.retired BEFORE awaiting b.conn.close(), so a concurrent
    ## ensureConnected can observe (isReady and retired) in that window. The lock-free
    ## fast path checked only isReady/addressDirty and returned "connected"; it now
    ## also requires not retired, so the caller routes through connectImpl's guard.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "retired-fastpath-test")

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
                  )
                ],
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()
      check b.isReady

      # Stand in for the mid-retire window: flag set, connection still alive.
      b.setRetiredForTesting()
      check b.isReady
      check b.isRetired

      # The fast path must not admit the retired broker; connectImpl raises.
      var raised = false
      try:
        await b.ensureConnectedForTest()
      except KafkaConnectionError:
        raised = true
      check raised

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Broker: ensureConnected vs in-flight handshake":
  test "ensureConnected does not fast-path past an in-flight handshake":
    ## connectImpl assigns b.conn BEFORE ApiVersions negotiation and SASL auth run,
    ## so ensureConnected's lock-free fast path admitted a foreign request onto a
    ## half-authenticated connection — a SASL-exchange corruption that kills it. The
    ## fast path must also require `not isHandshaking()` and park on connectLock.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "handshake-fastpath-test")

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          let avBody = buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              )
            ],
          )
          try:
            await av.respond(avBody)
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()
      check b.isConnected

      # Stand in for a connectImpl that is mid-handshake: conn is up, the
      # connect lock is held, and the handshaking flag is set.
      await b.acquireConnectLockForTest()
      b.setHandshaking(true)

      let ensFut = b.ensureConnectedForTest()
      await sleepAsync(20.milliseconds)
      # Before the fix the fast path returned immediately despite the
      # in-flight handshake; now the caller parks on connectLock.
      check not ensFut.finished

      b.setHandshaking(false)
      b.releaseConnectLockForTest()
      check awaitCompleted(ensFut, 1.seconds)

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 100.milliseconds)

    waitFor testBody()

suite "Broker: connState derived view":
  test "connState derives disconnected/handshaking/ready from the flags":
    ## The enum is a read-time projection of (conn, conn.closed, negotiated,
    ## handshaking) — it cannot drift out of sync the way a stored flag could.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "connstate-test")
      check b.connState == bcsDisconnected
      check not b.isReady

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
                  )
                ],
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()
      check b.connState == bcsReady
      check b.isReady
      check b.isConnected # conn-aliveness is unchanged by the derived view

      # Mid-handshake view, exactly as connectImpl sets it.
      b.setHandshaking(true)
      check b.connState == bcsHandshaking
      check not b.isReady
      check b.isConnected # still alive — only readiness is gated
      b.setHandshaking(false)
      check b.connState == bcsReady

      await b.close()
      check b.connState == bcsDisconnected

      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Cluster: bootstrap broker cleanup on cancel":
  test "cancelled metadata refresh closes temp broker":
    proc testBody() {.async: (raises: [Exception]).} =
      # Mock server accepts connections but never responds, so metadata() blocks
      # until cancelled.
      proc slowCallback(
          server: StreamServer, transport: StreamTransport
      ) {.async: (raises: []).} =
        try:
          while not transport.closed:
            await sleepAsync(milliseconds(50))
        except CancelledError:
          discard
        await transport.closeWait()

      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, slowCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let c =
        newCluster(@[$boundAddr], connectTimeoutMs = 5000, requestTimeoutMs = 5000)

      let refreshFut = c.refreshMetadata()

      await sleepAsync(milliseconds(100))
      await refreshFut.cancelAndWait()

      # The temp broker must be cleaned up (no leak): no brokers registered.
      check c.brokers.len == 0

      server.stop()
      await server.closeWait()

    waitFor testBody()

proc buildApiVersionsResponse(corrId: int32): seq[byte] =
  ## Build a minimal ApiVersions v0 response (errorCode=0, one Metadata apiKey v0-v1).
  var w = initKafkaWriter()
  w.writeInt32(corrId)
  w.writeInt16(0'i16) # errorCode = NONE
  # apiKeys array with just Metadata (apiKey=3, v0-v1)
  w.writeArrayLen(1'i32)
  w.writeInt16(3'i16) # apiKey = Metadata
  w.writeInt16(0'i16) # minVersion
  w.writeInt16(1'i16) # maxVersion
  result = w.buf

proc buildMetadataResponse(
    corrId: int32, brokerHost: string, brokerPort: int32
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## Build a Metadata v1 response with one broker and no topics.
  var w = initKafkaWriter()
  w.writeInt32(corrId)
  # brokers array
  w.writeArrayLen(1'i32)
  w.writeInt32(1'i32) # nodeId
  w.writeString(brokerHost)
  w.writeInt32(brokerPort)
  w.writeNullableString(none(string)) # rack
  # controllerId
  w.writeInt32(1'i32)
  # topics array (empty)
  w.writeArrayLen(0'i32)
  result = w.buf

var gMockBrokerPort {.global.}: int32 = 0
var gMockBrokerHost {.global.}: cstring = "127.0.0.1"
  ## Address the metadata response advertises. Defaults to the literal every
  ## suite binds; a suite that wants the advertised broker dialed by name sets
  ## this to a resolvable name instead. A `cstring` (not a `string`) so the
  ## async callback can read it without losing gcsafe.

proc mockBrokerCallback(
    server: StreamServer, transport: StreamTransport
) {.async: (raises: []).} =
  ## Handles ApiVersions then Metadata requests using gMockBrokerPort.
  try:
    for _ in 0 ..< 2:
      var sizeBuf: array[4, byte]
      await transport.readExactly(addr sizeBuf[0], 4)
      let reqSize = int(fromBytesBE(uint32, sizeBuf))
      var reqBuf = newSeq[byte](reqSize)
      await transport.readExactly(addr reqBuf[0], reqSize)

      let apiKey = int16(fromBytesBE(uint16, [reqBuf[0], reqBuf[1]]))
      let corrId =
        cast[int32](fromBytesBE(uint32, [reqBuf[4], reqBuf[5], reqBuf[6], reqBuf[7]]))

      var resp: seq[byte]
      if apiKey == 18: # ApiVersions
        resp = buildApiVersionsResponse(corrId)
      elif apiKey == 3: # Metadata
        resp = buildMetadataResponse(corrId, $gMockBrokerHost, gMockBrokerPort)
      else:
        continue

      var frameSize = toBytesBE(uint32(resp.len))
      discard await transport.write(addr frameSize[0], 4)
      discard await transport.write(addr resp[0], resp.len)
    # Keep the connection open so the adopted broker remains connected
    while not transport.closed:
      await sleepAsync(milliseconds(50))
  except TransportError, CancelledError, KafkaProtocolError:
    discard
  await transport.closeWait()

suite "Cluster: bootstrap broker connection reuse":
  test "bootstrap broker connection is adopted into cluster":
    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, mockBrokerCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()
      gMockBrokerPort = int32(boundAddr.port)

      let c = newCluster(
        @["127.0.0.1:" & $boundAddr.port],
        connectTimeoutMs = 5000,
        requestTimeoutMs = 5000,
      )

      await c.refreshMetadata()

      # The cluster must have adopted the bootstrap broker.
      check c.brokers.len == 1
      check 1'i32 in c.brokers
      # The adopted broker is still connected (reused, not reconnected).
      check c.brokers[1].isConnected

      await c.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

suite "Cluster: host allowlist":
  ## The allowlist is a cluster-level dial policy, so the wiring from
  ## `newCluster` down to `connection.connect` is part of the contract — a
  ## dropped parameter here silently disables the check while the unit tests on
  ## `hostAllowed` keep passing.

  test "an allowed bootstrap address connects":
    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, mockBrokerCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()
      gMockBrokerPort = int32(boundAddr.port)

      let c = newCluster(
        @["127.0.0.1:" & $boundAddr.port],
        hostAllowlist = newHostAllowlist(@[allowCidr("127.0.0.0/8")]),
        connectTimeoutMs = 5000,
        requestTimeoutMs = 5000,
      )

      await c.refreshMetadata()
      check c.brokers.len == 1
      check c.brokers[1].isConnected

      await c.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "a host-name bootstrap address is bounded by where it resolves":
    proc testBody() {.async: (raises: [Exception]).} =
      # A name is the case a TLS deployment needs (the certificate names a host,
      # not an address), and it is the case the string gate used to refuse
      # outright — so the rules could never bound it. Both the bootstrap address
      # and the address the metadata response advertises are names here, and the
      # advertised one matching the bootstrap is what proves the same connection
      # was adopted rather than a second, unchecked one opened.
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, mockBrokerCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      gMockBrokerPort = int32(boundAddr.port)
      gMockBrokerHost = "localhost"
      defer:
        gMockBrokerHost = "127.0.0.1"

      let c = newCluster(
        @["localhost:" & $boundAddr.port],
        hostAllowlist = newHostAllowlist(@[allowPrivateHosts()]),
        connectTimeoutMs = 5000,
        requestTimeoutMs = 5000,
      )

      await c.refreshMetadata()
      # `localhost` resolves to loopback, which the rules admit.
      check c.brokers.len == 1
      check c.brokers[1].isConnected
      check c.brokers[1].host == "localhost"

      await c.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "a disallowed bootstrap address is refused before the dial":
    proc testBody() {.async: (raises: [Exception]).} =
      let logger = ClusterLogSink()
      let previousLogFilter = stdLogging.getLogFilter()
      stdLogging.setLogFilter(stdLogging.lvlAll)
      stdLogging.addHandler(logger)
      try:
        let address = initTAddress("127.0.0.1:0")
        let server =
          createStreamServer(address, mockBrokerCallback, flags = {ReuseAddr})
        server.start()
        let boundAddr = server.localAddress()
        gMockBrokerPort = int32(boundAddr.port)

        # The server IS reachable; only the allowlist rules it out. Without the
        # check reaching the connection layer this refresh would succeed.
        let c = newCluster(
          @["127.0.0.1:" & $boundAddr.port],
          hostAllowlist = newHostAllowlist(@[allowCidr("10.0.0.0/8")]),
          connectTimeoutMs = 5000,
          requestTimeoutMs = 5000,
        )

        var raised = false
        try:
          await c.refreshMetadata()
        except KafkaError:
          raised = true
        check raised
        check c.brokers.len == 0
        # The refusal reason reaches the diagnostic surface, not just the
        # generic "no broker answered".
        var sawRefusal = false
        for message in logger.messages:
          if "not permitted" in message:
            sawRefusal = true
        check sawRefusal

        await c.close()
        server.stop()
        await server.closeWait()
      finally:
        stdLogging.removeHandler(logger)
        stdLogging.setLogFilter(previousLogFilter)

    waitFor testBody()

var gMetadataCount {.global.}: int = 0

proc countingBrokerCallback(
    server: StreamServer, transport: StreamTransport
) {.async: (raises: []).} =
  ## Like mockBrokerCallback but loops indefinitely and counts every Metadata
  ## request into `gMetadataCount`, so a test can assert how many times the
  ## cluster actually hit the broker across debounced / forced refreshes.
  try:
    while not transport.closed:
      var sizeBuf: array[4, byte]
      await transport.readExactly(addr sizeBuf[0], 4)
      let reqSize = int(fromBytesBE(uint32, sizeBuf))
      var reqBuf = newSeq[byte](reqSize)
      await transport.readExactly(addr reqBuf[0], reqSize)
      let apiKey = int16(fromBytesBE(uint16, [reqBuf[0], reqBuf[1]]))
      let corrId =
        cast[int32](fromBytesBE(uint32, [reqBuf[4], reqBuf[5], reqBuf[6], reqBuf[7]]))
      var resp: seq[byte]
      if apiKey == 18: # ApiVersions
        resp = buildApiVersionsResponse(corrId)
      elif apiKey == 3: # Metadata
        inc gMetadataCount
        resp = buildMetadataResponse(corrId, $gMockBrokerHost, gMockBrokerPort)
      else:
        continue
      var frameSize = toBytesBE(uint32(resp.len))
      discard await transport.write(addr frameSize[0], 4)
      discard await transport.write(addr resp[0], resp.len)
  except TransportError, CancelledError, KafkaProtocolError:
    discard
  await transport.closeWait()

suite "Cluster: refreshMetadata force bypasses debounce":
  test "force = true re-fetches inside the debounce window":
    proc testBody() {.async: (raises: [Exception]).} =
      gMetadataCount = 0
      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, countingBrokerCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()
      gMockBrokerPort = int32(boundAddr.port)

      # Large debounce window so the second (unforced) refresh is guaranteed to
      # be debounced and the third only proceeds because force bypasses it.
      let c = newCluster(
        @["127.0.0.1:" & $boundAddr.port],
        connectTimeoutMs = 5000,
        requestTimeoutMs = 5000,
        metadataDebounceInterval = seconds(60),
      )

      await c.refreshMetadata()
      check gMetadataCount == 1

      # Within the 60s debounce window: a plain refresh is a no-op.
      await c.refreshMetadata()
      check gMetadataCount == 1

      # force = true bypasses the debounce and actually re-fetches.
      await c.refreshMetadata(force = true)
      check gMetadataCount == 2

      await c.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

suite "Cluster: degenerate MetadataResponse":
  test "empty brokers list is refused so pruneStaleBrokers cannot wipe the registry":
    ## Regression: pruneStaleBrokers used to run with the empty activeIds set
    ## derived from an empty resp.brokers, retiring every known broker and
    ## orphaning topic leaders. The apply now raises before touching state.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let refreshCount = new(int)
      refreshCount[] = 0

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
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
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akMetadata)):
              refreshCount[].inc
              let brokers =
                if refreshCount[] == 1:
                  @[FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)]
                else:
                  newSeq[FakeBrokerMeta]()
              await req.respond(
                buildMetadataResponseBody(
                  brokers, controllerId = 0'i32, topics = newSeq[FakeTopicMeta]()
                )
              )
          except CatchableError:
            break

      let hf = handler()
      let c = newCluster(@["127.0.0.1:" & $port], clientId = "empty-brokers")

      await c.refreshMetadata(force = true)
      check c.brokerCountForTest() == 1

      var raised = false
      try:
        await c.refreshMetadata(force = true)
      except KafkaProtocolError:
        raised = true
      check raised
      # The registry survives: pruneStaleBrokers never ran with an empty set.
      check c.brokerCountForTest() == 1

      await c.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor testBody()

  test "empty brokers list emits refresh.failed and not the success event":
    ## A MetadataResponse with brokers=[] is refused before prune. That raise
    ## must emit `cluster.metadata.refresh.failed` with reason=empty_brokers
    ## once, and must not emit the success event.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let refreshCount = new(int)
      refreshCount[] = 0
      let sink = ClusterMetricSink(events: @[])

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
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
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akMetadata)):
              refreshCount[].inc
              let brokers =
                if refreshCount[] == 1:
                  @[FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)]
                else:
                  newSeq[FakeBrokerMeta]()
              await req.respond(
                buildMetadataResponseBody(
                  brokers, controllerId = 0'i32, topics = newSeq[FakeTopicMeta]()
                )
              )
          except CatchableError:
            break

      let hf = handler()
      let c = newCluster(
        @["127.0.0.1:" & $port],
        clientId = "empty-brokers-metrics",
        metricsHook = makeClusterMetricHook(sink),
      )

      await c.refreshMetadata(force = true)
      check c.brokerCountForTest() == 1
      check clusterMetricCount(sink, "cluster.metadata.refresh") == 1
      # Drop the warm-up success so the failing refresh is the only window.
      sink.events.setLen(0)

      var raised = false
      try:
        await c.refreshMetadata(force = true)
      except KafkaProtocolError:
        raised = true
      check raised
      check c.brokerCountForTest() == 1
      check clusterMetricCount(sink, "cluster.metadata.refresh") == 0
      check clusterMetricCount(sink, "cluster.metadata.refresh.failed") == 1
      check clusterMetricReason(sink, "cluster.metadata.refresh.failed") ==
        "empty_brokers"
      for ev in sink.events:
        if ev.event == "cluster.metadata.refresh.failed":
          check ev.value == 1
      # RPC succeeded; empty_brokers is a refused body, not a candidate skip.
      check clusterMetricCount(sink, "cluster.metadata.refresh.attempt.failed") == 0

      await c.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor testBody()

  test "refreshMetadata emits attempt.failed for a dead known broker then succeeds":
    ## The known-broker continue is a different site from bootstrap. A
    ## refuse-then-bootstrap-success refresh must emit attempt.failed once and
    ## must not emit the whole-refresh .failed.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let handlerFut = serveApiVersionsAndMetadata(fb, port)
      let sink = ClusterMetricSink(events: @[])
      let c = newCluster(
        @["127.0.0.1:" & $port],
        clientId = "candidate-failed",
        connectTimeoutMs = 5_000,
        requestTimeoutMs = 5_000,
        metricsHook = makeClusterMetricHook(sink),
      )
      c.brokers[99'i32] = newBroker(
        99'i32, "127.0.0.1", 1'i32, "candidate-failed", connectTimeoutMs = 5_000
      )
      await c.refreshMetadata(force = true)
      check clusterMetricCount(sink, "cluster.metadata.refresh") == 1
      check clusterMetricCount(sink, "cluster.metadata.refresh.failed") == 0
      check clusterMetricCount(sink, "cluster.metadata.refresh.attempt.failed") == 1
      check clusterMetricTag(sink, "cluster.metadata.refresh.attempt.failed", "host") ==
        "127.0.0.1"
      check clusterMetricTag(sink, "cluster.metadata.refresh.attempt.failed", "port") ==
        "1"
      for ev in sink.events:
        if ev.event == "cluster.metadata.refresh.attempt.failed":
          check ev.value == 1
      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Cluster: snapshot accessors":
  test "snapshotBrokers returns a stable copy":
    let c = newCluster(@["127.0.0.1:9092"])
    let b1 = newBroker(1, "host1", 9092, "test")
    let b2 = newBroker(2, "host2", 9092, "test")
    c.brokers[1] = b1
    c.brokers[2] = b2

    let snap = c.snapshotBrokers()
    check snap.len == 2

    # Mutating the live table must not affect the snapshot.
    let b3 = newBroker(3, "host3", 9092, "test")
    c.brokers[3] = b3
    check snap.len == 2
    check c.brokers.len == 3

    # Removing from the live table also leaves the snapshot intact.
    c.brokers.del(1)
    check snap.len == 2
    check c.brokers.len == 2

  test "getBrokerById returns Some/None correctly":
    let c = newCluster(@["127.0.0.1:9092"])
    let b1 = newBroker(1, "host1", 9092, "test")
    c.brokers[1] = b1

    let found = c.getBrokerById(1)
    check found.isSome
    check found.get.host == "host1"

    let missing = c.getBrokerById(99)
    check missing.isNone

  test "getPartitionLeader returns Some/None correctly":
    let c = newCluster(@["127.0.0.1:9092"])
    let tp = TopicPartition(topic: "t", partition: 0)
    let other = TopicPartition(topic: "t", partition: 7)
    # partitionLeaders is internal; this checks the snapshot accessor on an empty
    # cluster as a sanity check.
    check c.getPartitionLeader(tp).isNone
    check c.getPartitionLeader(other).isNone

  test "leaderEpoch monotonicity rejects stale MetadataResponse (KIP-320)":
    ## A MetadataResponse with a leaderEpoch strictly lower than what we
    ## already cached is dropped: the prior (nodeId, leaderEpoch) is kept
    ## and a `cluster.metadata.stale_leader_epoch` metric is emitted.
    ## Without this guard, a stale-view broker could rewind cached state
    ## and cause subsequent commits/fetches to be fenced.
    let sink = ClusterMetricSink(events: @[])
    let c = newCluster(@["127.0.0.1:9092"], metricsHook = makeClusterMetricHook(sink))
    let tp = TopicPartition(topic: "t", partition: 0)
    c.setPartitionLeaderForTest(tp, nodeId = 5, leaderEpoch = 10)

    # Stale candidate (epoch 7 < cached 10) is rejected; cached entry wins.
    let stale = PartitionLeader(nodeId: 9, leaderEpoch: 7)
    c.applyPartitionLeaderMonotonicForTest(
      tp, stale, some(PartitionLeader(nodeId: 5, leaderEpoch: 10)), "t"
    )
    let kept = c.getPartitionLeader(tp)
    check kept.isSome
    check kept.get.nodeId == 5'i32
    check kept.get.leaderEpoch == 10'i32
    check clusterMetricCount(sink, "cluster.metadata.stale_leader_epoch") == 1
    check clusterMetricTag(sink, "cluster.metadata.stale_leader_epoch", "stale") == "7"
    check clusterMetricTag(sink, "cluster.metadata.stale_leader_epoch", "kept") == "10"

    # Fresh candidate (epoch 11 > cached 10) wins.
    let fresh = PartitionLeader(nodeId: 9, leaderEpoch: 11)
    c.applyPartitionLeaderMonotonicForTest(
      tp, fresh, some(PartitionLeader(nodeId: 5, leaderEpoch: 10)), "t"
    )
    let updated = c.getPartitionLeader(tp)
    check updated.isSome
    check updated.get.nodeId == 9'i32
    check updated.get.leaderEpoch == 11'i32
    # Only the rejected candidate is an event: a forward epoch move is normal.
    check clusterMetricCount(sink, "cluster.metadata.stale_leader_epoch") == 1

    # -1 prev (pre-KIP-320 broker) is treated as unknown; any concrete
    # candidate replaces it.
    let tp2 = TopicPartition(topic: "t", partition: 1)
    c.applyPartitionLeaderMonotonicForTest(
      tp2,
      PartitionLeader(nodeId: 3, leaderEpoch: 0),
      some(PartitionLeader(nodeId: 3, leaderEpoch: -1)),
      "t",
    )
    check c.getPartitionLeader(tp2).get.leaderEpoch == 0'i32

    # No prior entry: candidate wins unconditionally.
    let tp3 = TopicPartition(topic: "t", partition: 2)
    c.applyPartitionLeaderMonotonicForTest(
      tp3, PartitionLeader(nodeId: 7, leaderEpoch: 4), none(PartitionLeader), "t"
    )
    check c.getPartitionLeader(tp3).get.leaderEpoch == 4'i32

  test "getPartitionLeader carries leaderEpoch (KIP-320)":
    let c = newCluster(@["127.0.0.1:9092"])
    let tp = TopicPartition(topic: "t", partition: 0)
    let stale = TopicPartition(topic: "t", partition: 1)
    # Seed two partitions: one with a real epoch (mimicking MetadataResponse
    # v7+) and one with -1 (mimicking a pre-v7 broker that does not surface
    # leader_epoch). Both must round-trip through getPartitionLeader.
    c.setPartitionLeaderForTest(tp, nodeId = 3, leaderEpoch = 42)
    c.setPartitionLeaderForTest(stale, nodeId = 3, leaderEpoch = -1)

    let live = c.getPartitionLeader(tp)
    check live.isSome
    check live.get.nodeId == 3'i32
    check live.get.leaderEpoch == 42'i32

    let unknown = c.getPartitionLeader(stale)
    check unknown.isSome
    check unknown.get.leaderEpoch == -1'i32

    # The id-only convenience accessor must agree with .get.nodeId.
    check c.getPartitionLeaderId(tp) == some(3'i32)
    check c.getPartitionLeaderId(TopicPartition(topic: "t", partition: 99)).isNone

  test "getCachedCoordinator returns Some/None for both group and txn":
    let c = newCluster(@["127.0.0.1:9092"])
    c.coordinators["group-a"] = 5'i32
    c.txnCoordinators["txn-a"] = 9'i32

    let group = c.getCachedCoordinator("group-a")
    check group.isSome
    check group.get == 5'i32

    let txn = c.getCachedCoordinator("txn-a", txn = true)
    check txn.isSome
    check txn.get == 9'i32

    check c.getCachedCoordinator("missing").isNone
    check c.getCachedCoordinator("missing", txn = true).isNone

  test "getTopicPartitionsCopy returns a stable copy":
    let c = newCluster(@["127.0.0.1:9092"])
    c.topicPartitions["t"] = @[
      PartitionMeta(
        partitionIndex: 0,
        leaderId: 1,
        errorCode: keNone,
        replicaNodes: @[],
        isrNodes: @[],
      )
    ]

    let snap = c.getTopicPartitionsCopy("t")
    check snap.isSome
    check snap.get.len == 1

    # Mutating the live table must not affect the snapshot.
    c.topicPartitions["t"][0] = PartitionMeta(
      partitionIndex: 99,
      leaderId: 99,
      errorCode: keNone,
      replicaNodes: @[],
      isrNodes: @[],
    )
    check snap.get[0].partitionIndex == 0

    check c.getTopicPartitionsCopy("missing").isNone

suite "Cluster: lifecycle state":
  test "fresh cluster is in lsRunning state":
    let c = newCluster(@["127.0.0.1:9092"])
    check c.state == lsRunning
    check not c.isClosed

  test "close transitions to lsClosed":
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])
      await c.close()
      check c.state == lsClosed
      check c.isClosed
      # Second close is idempotent.
      await c.close()
      check c.state == lsClosed

    waitFor testBody()

  test "anyBroker on closed cluster raises KafkaClosedError":
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])
      await c.close()
      var raised = false
      try:
        discard await c.anyBroker()
      except KafkaClosedError:
        raised = true
      check raised

    waitFor testBody()

  test "coordinator APIs on closed cluster raise KafkaClosedError":
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:9092"])
      await c.close()
      var raised = false
      try:
        discard await c.coordinatorBroker("group-a")
      except KafkaClosedError:
        raised = true
      check raised
      raised = false
      try:
        discard await c.txnCoordinatorBroker("txn-id")
      except KafkaClosedError:
        raised = true
      check raised

    waitFor testBody()

var gErrPartPort {.global.}: int32 = 0

proc buildMetadataRespErroredPartition(
    corrId: int32, brokerPort: int32
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## Metadata v1: one broker, topic "t" (errorCode NONE) carrying a single
  ## partition (index 1) that returns LEADER_NOT_AVAILABLE. v1 has no
  ## leaderEpoch/offlineReplicas/topicId fields.
  var w = initKafkaWriter()
  w.writeInt32(corrId)
  # brokers
  w.writeArrayLen(1'i32)
  w.writeInt32(1'i32) # nodeId
  w.writeString("127.0.0.1")
  w.writeInt32(brokerPort)
  w.writeNullableString(none(string)) # rack
  # controllerId
  w.writeInt32(1'i32)
  # topics
  w.writeArrayLen(1'i32)
  w.writeInt16(0'i16) # topic errorCode = NONE
  w.writeString("t")
  w.writeBool(false) # isInternal
  # partitions
  w.writeArrayLen(1'i32)
  w.writeInt16(5'i16) # partition errorCode = LEADER_NOT_AVAILABLE
  w.writeInt32(1'i32) # partitionIndex
  w.writeInt32(-1'i32) # leaderId (unknown while the leader is unavailable)
  w.writeArrayLen(0'i32) # replicaNodes
  w.writeArrayLen(0'i32) # isrNodes
  result = w.buf

proc erroredPartitionBrokerCallback(
    server: StreamServer, transport: StreamTransport
) {.async: (raises: []).} =
  ## Responds to ApiVersions then a Metadata request whose single partition is
  ## errored, so a partial refresh exercises the per-partition error branch.
  try:
    for _ in 0 ..< 2:
      var sizeBuf: array[4, byte]
      await transport.readExactly(addr sizeBuf[0], 4)
      let reqSize = int(fromBytesBE(uint32, sizeBuf))
      var reqBuf = newSeq[byte](reqSize)
      await transport.readExactly(addr reqBuf[0], reqSize)
      let apiKey = int16(fromBytesBE(uint16, [reqBuf[0], reqBuf[1]]))
      let corrId =
        cast[int32](fromBytesBE(uint32, [reqBuf[4], reqBuf[5], reqBuf[6], reqBuf[7]]))
      var resp: seq[byte]
      if apiKey == 18: # ApiVersions
        resp = buildApiVersionsResponse(corrId)
      elif apiKey == 3: # Metadata
        resp = buildMetadataRespErroredPartition(corrId, gErrPartPort)
      else:
        continue
      var frameSize = toBytesBE(uint32(resp.len))
      discard await transport.write(addr frameSize[0], 4)
      discard await transport.write(addr resp[0], resp.len)
  except TransportError, CancelledError, KafkaProtocolError:
    discard
  await transport.closeWait()

suite "Cluster: partial refresh preserves a leader on partition error":
  test "a transient partition error keeps the previously-known leader (KIP-320 baseline)":
    ## A partial refresh cleared the requested topic's leaders and then skipped any
    ## partition that came back errored, dropping its cached (nodeId, leaderEpoch)
    ## and destroying the monotonicity baseline. The leader must survive a transient
    ## per-partition error.
    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, erroredPartitionBrokerCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()
      gErrPartPort = int32(boundAddr.port)

      let c = newCluster(
        @["127.0.0.1:" & $boundAddr.port],
        connectTimeoutMs = 5000,
        requestTimeoutMs = 5000,
      )

      # Seed a known leader for the partition that the refresh will error on.
      let tp = TopicPartition(topic: "t", partition: 1)
      c.setPartitionLeaderForTest(tp, nodeId = 6, leaderEpoch = 3)
      check c.getPartitionLeader(tp).get.nodeId == 6'i32

      # Partial refresh for "t": the topic is fine but partition 1 is errored.
      await c.refreshMetadata(some(@["t"]))

      # The leader must be retained, not dropped.
      let kept = c.getPartitionLeader(tp)
      check kept.isSome
      check kept.get.nodeId == 6'i32
      check kept.get.leaderEpoch == 3'i32

      await c.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

var gErrTopicPort {.global.}: int32 = 0

proc buildMetadataRespErroredTopic(
    corrId: int32, brokerPort: int32
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## Metadata v1: one broker, topic "t" returning LEADER_NOT_AVAILABLE at the
  ## topic level (topic errorCode != NONE) with an empty partition array, as a
  ## broker does mid leader-election. A partial refresh must hit the
  ## topic-level error branch.
  var w = initKafkaWriter()
  w.writeInt32(corrId)
  # brokers
  w.writeArrayLen(1'i32)
  w.writeInt32(1'i32) # nodeId
  w.writeString("127.0.0.1")
  w.writeInt32(brokerPort)
  w.writeNullableString(none(string)) # rack
  # controllerId
  w.writeInt32(1'i32)
  # topics
  w.writeArrayLen(1'i32)
  w.writeInt16(5'i16) # topic errorCode = LEADER_NOT_AVAILABLE
  w.writeString("t")
  w.writeBool(false) # isInternal
  # partitions: none are listed when the topic itself is errored
  w.writeArrayLen(0'i32)
  result = w.buf

proc erroredTopicBrokerCallback(
    server: StreamServer, transport: StreamTransport
) {.async: (raises: []).} =
  ## Responds to ApiVersions then a Metadata request whose topic carries a
  ## topic-level error, so a partial refresh exercises the topic-level branch.
  try:
    for _ in 0 ..< 2:
      var sizeBuf: array[4, byte]
      await transport.readExactly(addr sizeBuf[0], 4)
      let reqSize = int(fromBytesBE(uint32, sizeBuf))
      var reqBuf = newSeq[byte](reqSize)
      await transport.readExactly(addr reqBuf[0], reqSize)
      let apiKey = int16(fromBytesBE(uint16, [reqBuf[0], reqBuf[1]]))
      let corrId =
        cast[int32](fromBytesBE(uint32, [reqBuf[4], reqBuf[5], reqBuf[6], reqBuf[7]]))
      var resp: seq[byte]
      if apiKey == 18: # ApiVersions
        resp = buildApiVersionsResponse(corrId)
      elif apiKey == 3: # Metadata
        resp = buildMetadataRespErroredTopic(corrId, gErrTopicPort)
      else:
        continue
      var frameSize = toBytesBE(uint32(resp.len))
      discard await transport.write(addr frameSize[0], 4)
      discard await transport.write(addr resp[0], resp.len)
  except TransportError, CancelledError, KafkaProtocolError:
    discard
  await transport.closeWait()

suite "Cluster: partial refresh preserves a leader on topic-level error":
  test "a transient topic-level error keeps the previously-known leader (KIP-320 baseline)":
    ## A partial refresh cleared the requested topic's leaders and then, on a
    ## topic-level error, dropped the topicPartitions entry and continued — skipping
    ## the prevLeaders restore. The lost (nodeId, leaderEpoch) then sends an unfenced
    ## committedLeaderEpoch=-1. The leader must survive a topic-level error too.
    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, erroredTopicBrokerCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()
      gErrTopicPort = int32(boundAddr.port)

      let c = newCluster(
        @["127.0.0.1:" & $boundAddr.port],
        connectTimeoutMs = 5000,
        requestTimeoutMs = 5000,
      )

      # Seed a known leader for a partition of the topic that will error.
      let tp = TopicPartition(topic: "t", partition: 1)
      c.setPartitionLeaderForTest(tp, nodeId = 6, leaderEpoch = 3)
      check c.getPartitionLeader(tp).get.nodeId == 6'i32

      # Partial refresh for "t": the topic comes back with a topic-level error.
      await c.refreshMetadata(some(@["t"]))

      # The leader must be retained, not dropped.
      let kept = c.getPartitionLeader(tp)
      check kept.isSome
      check kept.get.nodeId == 6'i32
      check kept.get.leaderEpoch == 3'i32

      await c.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

var gNoLeaderPort {.global.}: int32 = 0

proc buildMetadataRespNoLeader(
    corrId: int32, brokerPort: int32
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## Metadata v1: one broker, topic "t" (errorCode NONE) carrying two partitions
  ## that both report errorCode NONE yet leaderId -1 — the rare "no leader yet"
  ## gap some brokers surface mid-election without an accompanying error code.
  ## v1 has no leaderEpoch/offlineReplicas/topicId fields.
  var w = initKafkaWriter()
  w.writeInt32(corrId)
  # brokers
  w.writeArrayLen(1'i32)
  w.writeInt32(1'i32) # nodeId
  w.writeString("127.0.0.1")
  w.writeInt32(brokerPort)
  w.writeNullableString(none(string)) # rack
  # controllerId
  w.writeInt32(1'i32)
  # topics
  w.writeArrayLen(1'i32)
  w.writeInt16(0'i16) # topic errorCode = NONE
  w.writeString("t")
  w.writeBool(false) # isInternal
  # partitions
  w.writeArrayLen(2'i32)
  # partition 1: NONE error, but no leader elected
  w.writeInt16(0'i16) # partition errorCode = NONE
  w.writeInt32(1'i32) # partitionIndex
  w.writeInt32(-1'i32) # leaderId (no leader yet)
  w.writeArrayLen(0'i32) # replicaNodes
  w.writeArrayLen(0'i32) # isrNodes
  # partition 2: NONE error, but no leader elected
  w.writeInt16(0'i16) # partition errorCode = NONE
  w.writeInt32(2'i32) # partitionIndex
  w.writeInt32(-1'i32) # leaderId (no leader yet)
  w.writeArrayLen(0'i32) # replicaNodes
  w.writeArrayLen(0'i32) # isrNodes
  result = w.buf

proc noLeaderBrokerCallback(
    server: StreamServer, transport: StreamTransport
) {.async: (raises: []).} =
  ## Responds to ApiVersions then a Metadata request whose partitions report
  ## errorCode NONE with leaderId -1, so a partial refresh exercises the
  ## leaderless branch.
  try:
    for _ in 0 ..< 2:
      var sizeBuf: array[4, byte]
      await transport.readExactly(addr sizeBuf[0], 4)
      let reqSize = int(fromBytesBE(uint32, sizeBuf))
      var reqBuf = newSeq[byte](reqSize)
      await transport.readExactly(addr reqBuf[0], reqSize)
      let apiKey = int16(fromBytesBE(uint16, [reqBuf[0], reqBuf[1]]))
      let corrId =
        cast[int32](fromBytesBE(uint32, [reqBuf[4], reqBuf[5], reqBuf[6], reqBuf[7]]))
      var resp: seq[byte]
      if apiKey == 18: # ApiVersions
        resp = buildApiVersionsResponse(corrId)
      elif apiKey == 3: # Metadata
        resp = buildMetadataRespNoLeader(corrId, gNoLeaderPort)
      else:
        continue
      var frameSize = toBytesBE(uint32(resp.len))
      discard await transport.write(addr frameSize[0], 4)
      discard await transport.write(addr resp[0], resp.len)
  except TransportError, CancelledError, KafkaProtocolError:
    discard
  await transport.closeWait()

suite "Cluster: partial refresh handles a leaderless partition (errorCode NONE, leaderId -1)":
  test "a no-leader partition is not cached as broker -1 and keeps any known leader":
    ## A partition returning errorCode NONE but leaderId -1 (a momentary leader gap
    ## some brokers report without an error code) was cached verbatim as nodeId -1.
    ## getPartitionLeader then returned some(-1) and brokerForPartition raised the
    ## phantom "leader broker -1 not known". Treat it like a per-partition error:
    ## keep a previously-known leader, otherwise leave it absent.
    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, noLeaderBrokerCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()
      gNoLeaderPort = int32(boundAddr.port)

      let c = newCluster(
        @["127.0.0.1:" & $boundAddr.port],
        connectTimeoutMs = 5000,
        requestTimeoutMs = 5000,
      )

      # Partition 1 has a previously-known (pre-KIP-320) leader; partition 2 has
      # none. Both come back with errorCode NONE but leaderId -1.
      let tp1 = TopicPartition(topic: "t", partition: 1)
      let tp2 = TopicPartition(topic: "t", partition: 2)
      c.setPartitionLeaderForTest(tp1, nodeId = 6, leaderEpoch = -1)
      check c.getPartitionLeader(tp1).get.nodeId == 6'i32

      await c.refreshMetadata(some(@["t"]))

      # Partition 1: the known leader survives. The monotonic guard alone would
      # NOT have saved it here — the prior epoch is -1, so a leaderId -1 candidate
      # would otherwise have overwritten nodeId 6.
      let kept = c.getPartitionLeader(tp1)
      check kept.isSome
      check kept.get.nodeId == 6'i32

      # Partition 2: no leader was known, so leaderId -1 must leave the entry
      # absent — not cached as some(nodeId = -1).
      check c.getPartitionLeader(tp2).isNone

      await c.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

suite "Cluster: partition ordering":
  test "cached partitions are sorted by partitionIndex (key-hash determinism)":
    ## A MetadataResponse does not guarantee partition ordering, but the key-hash
    ## partitioner indexes as `parts[hash mod count].partitionIndex`. The cluster
    ## must sort by partitionIndex at refresh so a key maps to the same partition
    ## across refreshes — matching Apache Kafka's `toPositive(murmur2) % count`.
    ## Feeds shuffled wire order (2, 0, 1) and asserts the cache comes back sorted.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)

      proc serve() {.async: (raises: []).} =
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
                  apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                ),
              ],
            )
          )
          let md = await fb.nextRequest(timeout = 2.seconds)
          await md.respond(
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
                      partitionIndex: 2'i32,
                      leaderId: 0'i32,
                      replicas: @[0'i32],
                      isr: @[0'i32],
                    ),
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
        except CatchableError:
          discard

      let serveFut = serve()
      let c = newCluster(@[host & ":" & $port], clientId = "sort-test")
      await c.refreshMetadata(some(@["t"]))

      let partsOpt = c.getTopicPartitionsCopy("t")
      check partsOpt.isSome
      let parts = partsOpt.get
      check parts.len == 3
      # Ascending by partitionIndex regardless of the shuffled wire order, and
      # contiguous so parts[i].partitionIndex == i — the exact invariant the
      # key-hash partitioner relies on.
      for i in 0 ..< parts.len:
        check parts[i].partitionIndex == int32(i)

      await c.close()
      await fb.close()
      awaitDrain(serveFut, 1.seconds)

    waitFor testBody()

suite "Cluster: invalidateLeader":
  test "non-stale code is a no-op that returns false and touches no RPC":
    proc testBody() {.async: (raises: [Exception]).} =
      gMetadataCount = 0
      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, countingBrokerCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()
      gMockBrokerPort = int32(boundAddr.port)

      let c = newCluster(
        @["127.0.0.1:" & $boundAddr.port],
        connectTimeoutMs = 5000,
        requestTimeoutMs = 5000,
        metadataDebounceInterval = seconds(60),
      )
      await c.refreshMetadata()
      check gMetadataCount == 1

      let tp = TopicPartition(topic: "t", partition: 0)
      c.setPartitionLeaderForTest(tp, nodeId = 1, leaderEpoch = 3)

      let touched = await c.invalidateLeader(tp, keNone)
      check touched == false
      # Cached leader untouched, no extra RPC.
      let kept = c.getPartitionLeader(tp)
      check kept.isSome
      check kept.get.nodeId == 1'i32
      check gMetadataCount == 1

      # keOffsetOutOfRange is not a stale-leader code either.
      let touched2 = await c.invalidateLeader(tp, keOffsetOutOfRange)
      check touched2 == false
      check gMetadataCount == 1

      await c.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "stale code drops the cached leader and drives one refresh":
    proc testBody() {.async: (raises: [Exception]).} =
      gMetadataCount = 0
      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, countingBrokerCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()
      gMockBrokerPort = int32(boundAddr.port)

      let c = newCluster(
        @["127.0.0.1:" & $boundAddr.port],
        connectTimeoutMs = 5000,
        requestTimeoutMs = 5000,
        metadataDebounceInterval = seconds(60),
      )
      await c.refreshMetadata()
      check gMetadataCount == 1

      let tp = TopicPartition(topic: "t", partition: 0)
      c.setPartitionLeaderForTest(tp, nodeId = 1, leaderEpoch = 3)

      let touched = await c.invalidateLeader(tp, keNotLeaderOrFollower)
      check touched
      # Cache cleared (mock broker's response has no topics), one extra RPC.
      check c.getPartitionLeader(tp).isNone
      check gMetadataCount == 2

      await c.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "concurrent calls for the same topic collapse into one RPC":
    proc testBody() {.async: (raises: [Exception]).} =
      gMetadataCount = 0
      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, countingBrokerCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()
      gMockBrokerPort = int32(boundAddr.port)

      let c = newCluster(
        @["127.0.0.1:" & $boundAddr.port],
        connectTimeoutMs = 5000,
        requestTimeoutMs = 5000,
        metadataDebounceInterval = seconds(60),
      )
      await c.refreshMetadata()
      check gMetadataCount == 1

      # Fan out N invalidations for distinct partitions of the SAME topic.
      # Coalesce guarantees a single MetadataRequest RPC.
      var futs: seq[Future[bool]]
      for part in 0 ..< 5:
        let tp = TopicPartition(topic: "same", partition: int32(part))
        c.setPartitionLeaderForTest(tp, nodeId = 1, leaderEpoch = 3)
        futs.add(c.invalidateLeader(tp, keFencedLeaderEpoch))
      discard await allFinished(futs)
      for f in futs:
        check f.read()
      check gMetadataCount == 2

      await c.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "concurrent calls for distinct topics each drive a refresh":
    proc testBody() {.async: (raises: [Exception]).} =
      gMetadataCount = 0
      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, countingBrokerCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()
      gMockBrokerPort = int32(boundAddr.port)

      let c = newCluster(
        @["127.0.0.1:" & $boundAddr.port],
        connectTimeoutMs = 5000,
        requestTimeoutMs = 5000,
        metadataDebounceInterval = seconds(60),
      )
      await c.refreshMetadata()
      check gMetadataCount == 1

      var futs: seq[Future[bool]]
      for i in 0 ..< 3:
        let tp = TopicPartition(topic: "t" & $i, partition: 0)
        c.setPartitionLeaderForTest(tp, nodeId = 1, leaderEpoch = 3)
        futs.add(c.invalidateLeader(tp, keUnknownTopicOrPartition))
      discard await allFinished(futs)
      check gMetadataCount == 4

      await c.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "a refresh that already completed keeps the fresh leader":
    proc testBody() {.async: (raises: [Exception]).} =
      let c = newCluster(@["127.0.0.1:1"])
      let tp = TopicPartition(topic: "fresh", partition: 0)
      c.setPartitionLeaderForTest(tp, nodeId = 1, leaderEpoch = 3)
      # Simulate the window where a refresh finished but its runner has not yet
      # del'd the pendingRefreshes entry: the cached leader IS that RPC's fresh
      # result. invalidateLeader must not tear it down and must not drive a
      # duplicate refresh (pre-fix it deleted the fresh leader, then returned
      # true without an RPC, wasting the next brokerForPartition).
      let done = newFuture[void]("cluster.invalidateLeader")
      done.complete()
      c.pendingRefreshes["fresh"] = done

      let touched = await c.invalidateLeader(tp, keFencedLeaderEpoch)
      check touched
      # Fresh leader survives the invalidate.
      let kept = c.getPartitionLeader(tp)
      check kept.isSome
      check kept.get.nodeId == 1'i32
      check kept.get.leaderEpoch == 3'i32

      await c.close()

    waitFor testBody()

  test "a refresh that already failed tears down the leader and retries":
    ## Contrast with the completed-slot test above: a failed pending entry is a
    ## dead attempt, not a still-fresh RPC. invalidateLeader must fall through,
    ## drop the cached leader, and drive a new MetadataRequest. Treating failed
    ## like completed would keep a stale leader and skip the RPC.
    proc testBody() {.async: (raises: [Exception]).} =
      gMetadataCount = 0
      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, countingBrokerCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()
      gMockBrokerPort = int32(boundAddr.port)

      let c = newCluster(
        @["127.0.0.1:" & $boundAddr.port],
        connectTimeoutMs = 5000,
        requestTimeoutMs = 5000,
        metadataDebounceInterval = seconds(60),
      )
      await c.refreshMetadata()
      check gMetadataCount == 1

      let tp = TopicPartition(topic: "failed", partition: 0)
      c.setPartitionLeaderForTest(tp, nodeId = 1, leaderEpoch = 3)
      let stale = newFuture[void]("cluster.invalidateLeader")
      stale.fail(newException(KafkaError, "previous refresh failed"))
      check stale.finished
      check not stale.completed
      c.pendingRefreshes["failed"] = stale

      let touched = await c.invalidateLeader(tp, keFencedLeaderEpoch)
      check touched
      check c.getPartitionLeader(tp).isNone
      check gMetadataCount == 2

      await c.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "a refresh that was cancelled tears down the leader and retries":
    ## Same fall-through as a failed slot: cancelled is finished but not
    ## completed, so invalidateLeader must not take the success early-return.
    proc testBody() {.async: (raises: [Exception]).} =
      gMetadataCount = 0
      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, countingBrokerCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()
      gMockBrokerPort = int32(boundAddr.port)

      let c = newCluster(
        @["127.0.0.1:" & $boundAddr.port],
        connectTimeoutMs = 5000,
        requestTimeoutMs = 5000,
        metadataDebounceInterval = seconds(60),
      )
      await c.refreshMetadata()
      check gMetadataCount == 1

      let tp = TopicPartition(topic: "cancelled", partition: 0)
      c.setPartitionLeaderForTest(tp, nodeId = 1, leaderEpoch = 3)
      let stale = newFuture[void]("cluster.invalidateLeader")
      check stale.tryCancel()
      check stale.finished
      check not stale.completed
      c.pendingRefreshes["cancelled"] = stale

      let touched = await c.invalidateLeader(tp, keFencedLeaderEpoch)
      check touched
      check c.getPartitionLeader(tp).isNone
      check gMetadataCount == 2

      await c.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

suite "Cluster: coalesce joiner cancel isolation":
  ## Regression: joiners directly awaited the shared coalesce slot, so chronos'
  ## `tryCancel` walked internalChild → slot → cancelled the slot itself,
  ## failing every peer joiner with a spurious CancelledError while the detached
  ## runner's real result was thrown away by the `if not slot.finished` guard.

  test "cancelling one invalidateLeader joiner does not fail its peer":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

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
                ],
              )
            )
          except CatchableError:
            discard
          # Initial refresh: reply immediately so the cluster is warm.
          let md1 = await fb.nextRequest(timeout = 2.seconds)
          try:
            await md1.respond(
              buildMetadataResponseBody(
                [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
                controllerId = 0'i32,
                topics = newSeq[FakeTopicMeta](),
              )
            )
          except CatchableError:
            discard
          # Coalesced refresh: park the runner, cancel a joiner mid-flight,
          # then reply so the surviving joiner sees a real result.
          let md2 = await fb.nextRequest(timeout = 2.seconds)
          await sleepAsync(80.milliseconds)
          try:
            await md2.respond(
              buildMetadataResponseBody(
                [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
                controllerId = 0'i32,
                topics = newSeq[FakeTopicMeta](),
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let c = newCluster(@["127.0.0.1:" & $port], clientId = "coalesce-cancel")
      await c.refreshMetadata()

      let tp = TopicPartition(topic: "t", partition: 0)
      c.setPartitionLeaderForTest(tp, nodeId = 0'i32, leaderEpoch = 3)

      let j1 = c.invalidateLeader(tp, keFencedLeaderEpoch)
      let j2 = c.invalidateLeader(tp, keFencedLeaderEpoch)

      # Yield so both joiners register on the shared slot and the runner parks
      # awaiting the fake broker's reply.
      await sleepAsync(20.milliseconds)

      await j1.cancelAndWait()
      check j1.cancelled

      let j2Result = await j2
      check j2Result

      await c.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()
