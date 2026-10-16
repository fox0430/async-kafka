## Broker-level throttle tracking and producer retry-path integration with
## throttle_time_ms. Verifies (1) the Broker helpers `applyThrottle`,
## `throttleRemainingMs`, `awaitThrottle`, and (2) that doSendWithRetry pays the
## remaining broker throttle when retrying.

import std/[unittest, options, monotimes]
from std/times import inMilliseconds

import chronos

import async_kafka/[errors, cluster, broker, connection]
import async_kafka/internal/metrics
import async_kafka/protocol/[api_keys, messages]
import async_kafka/producer/producer

import fake_broker, async_helpers, broker_helpers

type
  ProduceBehavior = ref object
    errorCodes: seq[int16]
    throttleMs: seq[int32]
    index: int

  MetricEvent = tuple[event: string, tags: seq[(string, string)], value: int64]

  MetricSink = ref object
    events: seq[MetricEvent]

const throttleTopic = "throttle-topic"

suite "Broker: throttle helpers":
  test "applyThrottle no-op for zero or negative":
    let b = newBroker(0'i32, "127.0.0.1", 0'i32, "test")
    b.applyThrottle(0'i32)
    check b.throttleRemainingMs() == 0'i32
    b.applyThrottle(-5'i32)
    check b.throttleRemainingMs() == 0'i32

  test "applyThrottle sets a remaining deadline":
    let b = newBroker(0'i32, "127.0.0.1", 0'i32, "test")
    b.applyThrottle(200'i32)
    let remaining = b.throttleRemainingMs()
    # Should be within a few ms of 200ms (Moment.now is sampled twice).
    check remaining > 100'i32
    check remaining <= 200'i32

  test "applyThrottle keeps the longer deadline":
    let b = newBroker(0'i32, "127.0.0.1", 0'i32, "test")
    b.applyThrottle(500'i32)
    b.applyThrottle(50'i32) # shorter — should NOT shrink the deadline
    let remaining = b.throttleRemainingMs()
    check remaining > 100'i32

  test "throttleRemainingMs returns 0 after deadline elapses":
    proc body() {.async: (raises: [Exception]).} =
      let b = newBroker(0'i32, "127.0.0.1", 0'i32, "test")
      b.applyThrottle(50'i32)
      check b.throttleRemainingMs() > 0'i32
      await sleepAsync(80.milliseconds)
      check b.throttleRemainingMs() == 0'i32

    waitFor body()

  test "awaitThrottle sleeps the remaining interval":
    proc body() {.async: (raises: [Exception]).} =
      let b = newBroker(0'i32, "127.0.0.1", 0'i32, "test")
      b.applyThrottle(120'i32)
      let start = getMonoTime()
      await b.awaitThrottle()
      let elapsed = (getMonoTime() - start).inMilliseconds
      # Allow scheduler slack but verify we actually waited.
      check elapsed >= 100

    waitFor body()

  test "awaitThrottle emits waited_ms after sleeping":
    ## Regression: quota park used to be silent except for the rare clamp
    ## event. A completed wait must emit broker.throttle.waited_ms once with
    ## wall-clock milliseconds and the same host/port tags as clamped.
    proc body() {.async: (raises: [Exception]).} =
      let sink = MetricSink(events: @[])
      let hook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.closure, gcsafe, raises: [].} =
        var copied: seq[(string, string)]
        for t in tags:
          copied.add(($t[0], $t[1]))
        sink.events.add((event: $event, tags: copied, value: value))
      let b = newBroker(0'i32, "127.0.0.1", 0'i32, "test", metricsHook = hook)
      b.applyThrottle(120'i32)
      await b.awaitThrottle()
      var waited: seq[MetricEvent]
      for ev in sink.events:
        if ev.event == "broker.throttle.waited_ms":
          waited.add(ev)
      check waited.len == 1
      check waited[0].value > 0
      check waited[0].tags == @[("host", "127.0.0.1"), ("port", "0")]

    waitFor body()

  test "awaitThrottle is a no-op when no throttle is pending":
    proc body() {.async: (raises: [Exception]).} =
      let b = newBroker(0'i32, "127.0.0.1", 0'i32, "test")
      let start = getMonoTime()
      await b.awaitThrottle()
      let elapsed = (getMonoTime() - start).inMilliseconds
      check elapsed < 20

    waitFor body()

  test "awaitThrottle does not emit waited_ms when no throttle is pending":
    ## The no-op path must stay silent: emitting waited_ms=0 would drown
    ## quota-park counts in idle roundTrips.
    proc body() {.async: (raises: [Exception]).} =
      var waited = 0
      let hook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.closure, gcsafe, raises: [].} =
        if $event == "broker.throttle.waited_ms":
          inc waited
      let b = newBroker(0'i32, "127.0.0.1", 0'i32, "test", metricsHook = hook)
      await b.awaitThrottle()
      check waited == 0

    waitFor body()

  test "awaitThrottle re-evaluates a throttle extended mid-sleep":
    ## applyThrottle runs from response handlers and can push the deadline out
    ## while awaitThrottle is parked in sleepAsync. The wait must observe the
    ## extension and keep sleeping, not wake on the stale snapshot and return
    ## early — otherwise the caller writes into the freshly-extended window and
    ## re-violates the quota.
    proc body() {.async: (raises: [Exception]).} =
      let b = newBroker(0'i32, "127.0.0.1", 0'i32, "test")
      b.applyThrottle(80'i32)

      proc extendLater() {.async: (raises: [Exception]).} =
        # Fire mid-sleep (the initial snapshot is ~80ms) and push the deadline
        # out to ~t+120ms.
        await sleepAsync(40.milliseconds)
        b.applyThrottle(80'i32)

      let start = getMonoTime()
      let extender = extendLater()
      await b.awaitThrottle()
      let elapsed = (getMonoTime() - start).inMilliseconds
      await extender
      # A single-sleep implementation would return near the 80ms snapshot. The
      # extension moves the real deadline to ~120ms, so a correct wait blocks
      # until then.
      check elapsed >= 110
      check b.throttleRemainingMs() == 0'i32

    waitFor body()

  test "applyThrottle clamps malicious / out-of-range values":
    ## A broker that returns int32.high ms (~24 days) would park the
    ## client forever. applyThrottle clamps to maxThrottleMs (10 min) and
    ## emits broker.throttle.clamped so the situation is observable.
    var clampedEvents = 0
    let hook = proc(
        event: cstring, tags: openArray[(cstring, cstring)], value: int64
    ) {.closure, gcsafe, raises: [].} =
      if $event == "broker.throttle.clamped":
        clampedEvents.inc

    let b = newBroker(0'i32, "127.0.0.1", 0'i32, "test", metricsHook = hook)
    b.applyThrottle(int32.high)
    check clampedEvents == 1
    let remaining = b.throttleRemainingMs()
    check remaining <= maxThrottleMs
    # Normal-range values must NOT trigger the clamp metric.
    b.applyThrottle(500'i32)
    check clampedEvents == 1

  test "awaitThrottle raises when the throttle outlasts the budget":
    ## A throttle longer than the caller's budget must fail with
    ## KafkaTimeoutError at the budget boundary — never park the caller for
    ## the throttle's full duration.
    proc body() {.async: (raises: [Exception]).} =
      let b = newBroker(0'i32, "127.0.0.1", 0'i32, "test")
      b.applyThrottle(60_000'i32)
      let start = getMonoTime()
      expect KafkaTimeoutError:
        await b.awaitThrottle(200'i32)
      let elapsed = (getMonoTime() - start).inMilliseconds
      check elapsed >= 150
      check elapsed < 1000
      # The wait must not have consumed the (much longer) throttle.
      check b.throttleRemainingMs() > 50_000'i32

    waitFor body()

  test "awaitThrottle waits the throttle out when the budget is longer":
    proc body() {.async: (raises: [Exception]).} =
      let b = newBroker(0'i32, "127.0.0.1", 0'i32, "test")
      b.applyThrottle(100'i32)
      let start = getMonoTime()
      await b.awaitThrottle(5_000'i32)
      let elapsed = (getMonoTime() - start).inMilliseconds
      check elapsed >= 80
      check b.throttleRemainingMs() == 0'i32

    waitFor body()

  test "awaitThrottle with timeoutMs = 0 uses bounded indefinite semantics":
    ## 0 mirrors response waits: a short throttle completes, while a much longer
    ## throttle shares the same ten-minute ceiling.
    proc body() {.async: (raises: [Exception]).} =
      let b = newBroker(0'i32, "127.0.0.1", 0'i32, "test")
      b.applyThrottle(120'i32)
      let start = getMonoTime()
      await b.awaitThrottle(0'i32)
      let elapsed = (getMonoTime() - start).inMilliseconds
      check elapsed >= 100

    waitFor body()

  test "awaitThrottle default budget is the broker requestTimeoutMs":
    ## Negative timeoutMs means the broker default, exactly like
    ## awaitResponse — so the public no-arg form can't park a caller past
    ## the broker's configured request timeout either.
    proc body() {.async: (raises: [Exception]).} =
      let b = newBroker(0'i32, "127.0.0.1", 0'i32, "test", requestTimeoutMs = 200)
      b.applyThrottle(60_000'i32)
      let start = getMonoTime()
      expect KafkaTimeoutError:
        await b.awaitThrottle()
      let elapsed = (getMonoTime() - start).inMilliseconds
      check elapsed >= 150
      check elapsed < 1000

    waitFor body()

suite "Broker: throttle honored before a fresh request":
  test "a throttled broker delays the next typed request, not just retries":
    ## Regression: the happy path consulted the throttle only on the
    ## producer/fetch retry branches, so the FIRST request after a throttled
    ## response fired immediately and re-violated the quota. writeTypedRequest
    ## now awaits the throttle before issuing any typed request.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "throttle-precheck-test")

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          let avBody = buildApiVersionsResponseBody(
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
          try:
            await av.respond(avBody)
          except CatchableError:
            discard
          let oc = await fb.nextRequest(timeout = 2.seconds)
          let body = buildOffsetCommitResponseBodyV3(
            topic = "t", partition = 0'i32, errorCode = 0'i16, throttleTimeMs = 0'i32
          )
          try:
            await oc.respond(body)
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()
      # connect() negotiates ApiVersions over the handshake path (not throttled).
      # Apply a throttle AFTER connect so it gates the NEXT typed request.
      b.applyThrottle(150'i32)

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
      let start = getMonoTime()
      discard await b.offsetCommit(req)
      let elapsed = (getMonoTime() - start).inMilliseconds
      # The request must not hit the wire until the throttle elapses.
      check elapsed >= 120

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

  test "acks=0 produce honours the throttle before firing":
    ## Regression: produce()'s acks=0 fire-and-forget path skipped
    ## awaitThrottle (unlike produceWrite), so a throttled broker was hammered.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "throttle-acks0-test")

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          let avBody = buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              ),
              FakeApiVersion(
                apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
              ),
            ],
          )
          try:
            await av.respond(avBody)
          except CatchableError:
            discard
          # acks=0 has no response; just drain the produce frame off the wire.
          discard await fb.nextRequest(timeout = 2.seconds)
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()
      b.applyThrottle(150'i32)

      let req = ProduceRequest(
        transactionalId: none(string),
        acks: 0,
        timeoutMs: 30000,
        topics: @[
          ProduceTopicReq(
            topic: "t",
            partitions: @[ProducePartitionReq(partition: 0'i32, recordSet: @[0xAA'u8])],
          )
        ],
      )
      let start = getMonoTime()
      discard await b.produce(req)
      let elapsed = (getMonoTime() - start).inMilliseconds
      # The fire-and-forget write must wait out the throttle.
      check elapsed >= 120

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

  test "a throttle longer than the request timeout fails the typed request":
    ## Regression: writeTypedRequest's throttle wait was outside the caller's
    ## timeout, so a long quota throttle parked the request for its full
    ## duration (up to maxThrottleMs = 10 min) past the configured timeout.
    ## The wait now shares the request's timeout budget and raises
    ## KafkaTimeoutError at the boundary, before the request hits the wire.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(
        0'i32, "127.0.0.1", port, "throttle-timeout-test", requestTimeoutMs = 300
      )

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
          # No further request should arrive: the throttle must fail the
          # request before it is written.
          discard await fb.nextRequest(timeout = 1.seconds)
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()
      b.applyThrottle(60_000'i32)

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
      let start = getMonoTime()
      expect KafkaTimeoutError:
        discard await b.offsetCommit(req)
      let elapsed = (getMonoTime() - start).inMilliseconds
      check elapsed >= 250
      check elapsed < 1000

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

  test "acks=0 produce fails a throttle longer than the request timeout":
    ## Same regression for the fire-and-forget path: writeFireAndForget's
    ## throttle wait must respect the broker requestTimeoutMs instead of
    ## parking the write for the throttle's full duration.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(
        0'i32, "127.0.0.1", port, "throttle-acks0-timeout-test", requestTimeoutMs = 300
      )

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
                  apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                ),
              ],
            )
          )
          # No produce frame should arrive: the throttle must fail the write
          # before it is sent.
          discard await fb.nextRequest(timeout = 1.seconds)
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()
      b.applyThrottle(60_000'i32)

      let req = ProduceRequest(
        transactionalId: none(string),
        acks: 0,
        timeoutMs: 30000,
        topics: @[
          ProduceTopicReq(
            topic: "t",
            partitions: @[ProducePartitionReq(partition: 0'i32, recordSet: @[0xAA'u8])],
          )
        ],
      )
      let start = getMonoTime()
      expect KafkaTimeoutError:
        discard await b.produce(req)
      let elapsed = (getMonoTime() - start).inMilliseconds
      check elapsed >= 250
      check elapsed < 1000

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

proc nextResponse(b: ProduceBehavior): (int16, int32) =
  let i =
    if b.index >= b.errorCodes.len:
      b.errorCodes.len - 1
    else:
      b.index
  inc b.index
  let ec = b.errorCodes[i]
  let th =
    if i < b.throttleMs.len:
      b.throttleMs[i]
    else:
      0'i32
  (ec, th)

proc autoRespond(
    fb: FakeBroker, host: string, port: int32, topic: string, behavior: ProduceBehavior
) {.async: (raises: []).} =
  # Use a generous timeout: the producer's retry path may sleep for the
  # broker-requested throttle (hundreds of ms) before re-sending, and the
  # background loop would otherwise time out before the retry arrives.
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 2.seconds)
    except CatchableError:
      break
    try:
      let key = req.apiKey
      if key == int16(ord(akApiVersions)):
        let body = buildApiVersionsResponseBody(
          0'i16,
          [
            FakeApiVersion(
              apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
            ),
            FakeApiVersion(apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1),
            FakeApiVersion(apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3),
          ],
        )
        await req.respond(body)
      elif key == int16(ord(akMetadata)):
        let body = buildMetadataResponseBody(
          [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
          controllerId = 0'i32,
          topics = [
            FakeTopicMeta(
              name: topic,
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
          ],
        )
        await req.respond(body)
      elif key == int16(ord(akProduce)):
        let (errorCode, throttleMs) = behavior.nextResponse()
        let body = buildProduceResponseBody(
          topic = topic,
          partition = 0'i32,
          errorCode = errorCode,
          baseOffset = 7'i64,
          logAppendTimeMs = -1'i64,
          throttleTimeMs = throttleMs,
        )
        await req.respond(body)
    except CatchableError:
      break

proc makeSink(): MetricSink =
  MetricSink(events: @[])

proc makeHook(sink: MetricSink): MetricsHook =
  result = proc(
      event: cstring, tags: openArray[(cstring, cstring)], value: int64
  ) {.gcsafe, raises: [].} =
    var copied: seq[(string, string)]
    for t in tags:
      copied.add(($t[0], $t[1]))
    sink.events.add((event: $event, tags: copied, value: value))

proc findEvent(sink: MetricSink, name: string): Option[MetricEvent] =
  for ev in sink.events:
    if ev.event == name:
      return some(ev)
  none(MetricEvent)

proc eventNames(sink: MetricSink): seq[string] =
  for ev in sink.events:
    result.add(ev.event)

suite "Producer retry: throttle_time_ms respected":
  test "throttle on retriable response extends the retry sleep":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)

      # First Produce: retriable error (19 = NOT_ENOUGH_REPLICAS) with a
      # 400ms throttle. Second Produce: success, no throttle. The retry
      # sleep must pay the ~400ms remaining throttle even though retryBackoffMs
      # is 10ms.
      let behavior =
        ProduceBehavior(errorCodes: @[19'i16, 0'i16], throttleMs: @[400'i32, 0'i32])
      let respFut = autoRespond(fb, host, port, throttleTopic, behavior)

      let sink = makeSink()
      let c = newCluster(@[host & ":" & $port], clientId = "throttle-test")
      let p = newKafkaProducer(
        c,
        lingerMs = 0,
        retryBackoffMs = 10,
        maxRetries = 3,
        metricsHook = makeHook(sink),
      )

      let start = getMonoTime()
      let res = await p.send(throttleTopic, @[0xD1'u8])
      let elapsedMs = (getMonoTime() - start).inMilliseconds
      check res.offset == 7'i64

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

      let names = sink.eventNames()
      check "producer.send.retry" in names
      check "producer.send.ok" in names
      # throttle_ms metric is emitted only when throttle > 0 contributed.
      check "producer.send.retry.throttle_ms" in names
      let thEv = sink.findEvent("producer.send.retry.throttle_ms").get()
      check thEv.value >= 300'i64 # ~400 minus scheduler slack
      check thEv.value <= 400'i64

      # backoff_ms reports the *effective* sleep — must reflect the throttle.
      let backoffEv = sink.findEvent("producer.send.retry.backoff_ms").get()
      check backoffEv.value >= 300'i64

      # Real-time elapsed must reflect the throttle wait too.
      check elapsedMs >= 300

    waitFor body()

  test "no throttle metric when broker reports throttleTimeMs = 0":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)

      # Retriable failure with no throttle.
      let behavior =
        ProduceBehavior(errorCodes: @[19'i16, 0'i16], throttleMs: @[0'i32, 0'i32])
      let respFut = autoRespond(fb, host, port, throttleTopic, behavior)

      let sink = makeSink()
      let c = newCluster(@[host & ":" & $port], clientId = "no-throttle-test")
      let p = newKafkaProducer(
        c,
        lingerMs = 0,
        retryBackoffMs = 10,
        maxRetries = 3,
        metricsHook = makeHook(sink),
      )

      discard await p.send(throttleTopic, @[0xD2'u8])

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

      let names = sink.eventNames()
      check "producer.send.retry" in names
      check "producer.send.retry.throttle_ms" notin names

    waitFor body()

suite "OffsetCommit/OffsetFetch throttle propagation":
  ## Regression: the OffsetCommit/OffsetFetch helpers ignored
  ## throttle_time_ms, so a coordinator asking the consumer to back off was
  ## not heard. Negotiating v3 carries the field; these pin that it reaches
  ## the per-broker throttle deadline.

  proc respondApiVersions(req: CapturedRequest) {.async: (raises: []).} =
    try:
      let body = buildApiVersionsResponseBody(
        0'i16,
        [
          FakeApiVersion(
            apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
          ),
          FakeApiVersion(
            apiKey: int16(ord(akOffsetCommit)), minVersion: 0, maxVersion: 3
          ),
          FakeApiVersion(
            apiKey: int16(ord(akOffsetFetch)), minVersion: 0, maxVersion: 3
          ),
        ],
      )
      await req.respond(body)
    except CatchableError:
      discard

  test "offsetCommit v3 throttle_time_ms updates broker deadline":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "throttle-oc-test")

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          await respondApiVersions(av)
          let oc = await fb.nextRequest(timeout = 2.seconds)
          let body = buildOffsetCommitResponseBodyV3(
            topic = "t", partition = 0'i32, errorCode = 0'i16, throttleTimeMs = 250'i32
          )
          try:
            await oc.respond(body)
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()

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
      let resp = await b.offsetCommit(req)
      check resp.throttleTimeMs == 250'i32

      let remaining = b.throttleRemainingMs()
      check remaining > 100'i32 # broad lower bound to absorb scheduler jitter
      check remaining <= 250'i32

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

  test "offsetFetch v3 throttle_time_ms updates broker deadline":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "throttle-of-test")

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          await respondApiVersions(av)
          let ofReq = await fb.nextRequest(timeout = 2.seconds)
          let body = buildOffsetFetchResponseBodyV3(
            topic = "t",
            partition = 0'i32,
            errorCode = 0'i16,
            offset = 42'i64,
            throttleTimeMs = 180'i32,
          )
          try:
            await ofReq.respond(body)
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()

      let req = OffsetFetchRequest(
        groupId: "g", topics: @[OffsetFetchTopicReq(topic: "t", partitions: @[0'i32])]
      )
      let resp = await b.offsetFetch(req)
      check resp.throttleTimeMs == 180'i32
      check resp.topics.len == 1
      check resp.topics[0].partitions[0].offset == 42'i64

      let remaining = b.throttleRemainingMs()
      check remaining > 80'i32
      check remaining <= 180'i32

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

suite "Throttle parity across coordinator APIs":
  ## Regression: Heartbeat / JoinGroup / SyncGroup / LeaveGroup /
  ## ListOffsets / FindCoordinator did not parse throttle_time_ms, so a
  ## broker asking the consumer to slow down on any of these RPCs was not
  ## heard. Bumping each API's negotiated max version now propagates the
  ## value into the per-broker throttle deadline.

  proc respondApiVersionsAll(req: CapturedRequest) {.async: (raises: []).} =
    try:
      let body = buildApiVersionsResponseBody(
        0'i16,
        [
          FakeApiVersion(
            apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
          ),
          FakeApiVersion(apiKey: int16(ord(akHeartbeat)), minVersion: 0, maxVersion: 1),
          FakeApiVersion(apiKey: int16(ord(akJoinGroup)), minVersion: 0, maxVersion: 2),
          FakeApiVersion(apiKey: int16(ord(akSyncGroup)), minVersion: 0, maxVersion: 1),
          FakeApiVersion(apiKey: int16(ord(akLeaveGroup)), minVersion: 0, maxVersion: 1),
          FakeApiVersion(
            apiKey: int16(ord(akListOffsets)), minVersion: 0, maxVersion: 2
          ),
          FakeApiVersion(
            apiKey: int16(ord(akFindCoordinator)), minVersion: 0, maxVersion: 1
          ),
        ],
      )
      await req.respond(body)
    except CatchableError:
      discard

  template setupBroker(name: string): tuple[fb: FakeBroker, b: Broker] =
    let fbVar = newFakeBroker()
    let port = int32(fbVar.address().port)
    let bVar = newBroker(0'i32, "127.0.0.1", port, name)
    (fb: fbVar, b: bVar)

  test "heartbeat v1 throttle_time_ms updates broker deadline":
    proc body() {.async: (raises: [Exception]).} =
      let (fb, b) = setupBroker("throttle-hb-test")
      proc handler() {.async: (raises: []).} =
        try:
          await respondApiVersionsAll(await fb.nextRequest(timeout = 2.seconds))
          let hb = await fb.nextRequest(timeout = 2.seconds)
          try:
            await hb.respond(buildHeartbeatResponseBodyV1(throttleTimeMs = 220'i32))
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()
      let resp = await b.heartbeat(
        HeartbeatRequest(groupId: "g", generationId: 1, memberId: "m")
      )
      check resp.throttleTimeMs == 220'i32
      let remaining = b.throttleRemainingMs()
      check remaining > 100'i32
      check remaining <= 220'i32
      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

  test "syncGroup v1 throttle_time_ms updates broker deadline":
    proc body() {.async: (raises: [Exception]).} =
      let (fb, b) = setupBroker("throttle-sg-test")
      proc handler() {.async: (raises: []).} =
        try:
          await respondApiVersionsAll(await fb.nextRequest(timeout = 2.seconds))
          let sg = await fb.nextRequest(timeout = 2.seconds)
          try:
            await sg.respond(buildSyncGroupResponseBodyV1(throttleTimeMs = 180'i32))
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()
      let resp = await b.syncGroup(
        SyncGroupRequest(groupId: "g", generationId: 1, memberId: "m")
      )
      check resp.throttleTimeMs == 180'i32
      let remaining = b.throttleRemainingMs()
      check remaining > 80'i32
      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

  test "leaveGroup v1 throttle_time_ms updates broker deadline":
    proc body() {.async: (raises: [Exception]).} =
      let (fb, b) = setupBroker("throttle-lg-test")
      proc handler() {.async: (raises: []).} =
        try:
          await respondApiVersionsAll(await fb.nextRequest(timeout = 2.seconds))
          let lg = await fb.nextRequest(timeout = 2.seconds)
          try:
            await lg.respond(buildLeaveGroupResponseBodyV1(throttleTimeMs = 150'i32))
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()
      let resp = await b.leaveGroup(LeaveGroupRequest(groupId: "g", memberId: "m"))
      check resp.throttleTimeMs == 150'i32
      check b.throttleRemainingMs() > 50'i32
      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

  test "joinGroup v2 throttle_time_ms updates broker deadline":
    proc body() {.async: (raises: [Exception]).} =
      let (fb, b) = setupBroker("throttle-jg-test")
      proc handler() {.async: (raises: []).} =
        try:
          await respondApiVersionsAll(await fb.nextRequest(timeout = 2.seconds))
          let jg = await fb.nextRequest(timeout = 2.seconds)
          try:
            await jg.respond(buildJoinGroupResponseBodyV2(throttleTimeMs = 200'i32))
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()
      let resp = await b.joinGroup(
        JoinGroupRequest(
          groupId: "g",
          sessionTimeoutMs: 30000,
          rebalanceTimeoutMs: 60000,
          memberId: "",
          protocolType: "consumer",
          protocols: @[],
        )
      )
      check resp.throttleTimeMs == 200'i32
      check b.throttleRemainingMs() > 100'i32
      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

  test "listOffsets v2 throttle_time_ms updates broker deadline":
    proc body() {.async: (raises: [Exception]).} =
      let (fb, b) = setupBroker("throttle-lo-test")
      proc handler() {.async: (raises: []).} =
        try:
          await respondApiVersionsAll(await fb.nextRequest(timeout = 2.seconds))
          let lo = await fb.nextRequest(timeout = 2.seconds)
          try:
            await lo.respond(
              buildListOffsetsResponseBodyV2(
                topic = "t",
                partition = 0'i32,
                errorCode = 0'i16,
                timestamp = -1'i64,
                offset = 0'i64,
                throttleTimeMs = 130'i32,
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()
      let resp = await b.listOffsets(
        ListOffsetsRequest(
          replicaId: -1,
          isolationLevel: ilReadUncommitted,
          topics: @[
            ListOffsetsTopicReq(
              topic: "t",
              partitions: @[ListOffsetsPartitionReq(partition: 0, timestamp: -1)],
            )
          ],
        )
      )
      check resp.throttleTimeMs == 130'i32
      check b.throttleRemainingMs() > 50'i32
      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

  test "findCoordinator v1 throttle_time_ms updates broker deadline":
    proc body() {.async: (raises: [Exception]).} =
      let (fb, b) = setupBroker("throttle-fc-test")
      proc handler() {.async: (raises: []).} =
        try:
          await respondApiVersionsAll(await fb.nextRequest(timeout = 2.seconds))
          let fc = await fb.nextRequest(timeout = 2.seconds)
          try:
            await fc.respond(
              buildFindCoordinatorResponseBodyV1(throttleTimeMs = 110'i32)
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()
      let resp = await b.findCoordinator("group-a", coordinatorType = 0'i8)
      check resp.throttleTimeMs == 110'i32
      check b.throttleRemainingMs() > 40'i32
      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

suite "Broker: produceWrite handle contract":
  test "acks=0 sentinel: produceAwait resolves to none, abandon is a no-op":
    ## The acks=0 sentinel (`corrId = -1`, nil conn/fut) has no response and
    ## no in-flight slot. `produceAwait` must keep resolving it to none, and
    ## `produceAbandon` must not touch any count. The other handle consumers
    ## (awaitTypedResponse / awaitResponse) are now unreachable by type: the
    ## sentinel is wrapped in `ProduceInFlight`, not an `InFlightHandle`.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "acks0-sentinel-test")

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
                    apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          # acks=0 produce has no response; just drain the frame.
          discard await fb.nextRequest(timeout = 2.seconds)
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()

      let req = ProduceRequest(
        transactionalId: none(string),
        acks: 0,
        timeoutMs: 30000,
        topics: @[
          ProduceTopicReq(
            topic: "t",
            partitions: @[ProducePartitionReq(partition: 0'i32, recordSet: @[0xAA'u8])],
          )
        ],
      )
      let handle = await b.produceWrite(req)
      check b.inFlightRequests() == 0 # fire-and-forget never bumps the count

      # The documented consumer resolves the sentinel to none.
      let resp = await b.produceAwait(handle)
      check resp.isNone

      # Abandoning the sentinel is a no-op (no slot, no count).
      b.produceAbandon(handle)
      check b.inFlightRequests() == 0

      # Awaiting it again is a contract violation.
      expect KafkaError:
        discard await b.produceAwait(handle)

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

  test "produceAbandon releases the in-flight slot and broker count":
    ## A handle dropped without await keeps the connection's in-flight slot
    ## until teardown and the broker's inFlightCount forever. `produceAbandon`
    ## must release both so the pipeline window and pruneStaleBrokers are not
    ## held hostage by a response that never comes.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "produce-abandon-test")

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
                    apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          # Produce: drain WITHOUT responding, so the slot can only be
          # released by the abandon path (no response ever arrives).
          discard await fb.nextRequest(timeout = 2.seconds)
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()

      let req = ProduceRequest(
        transactionalId: none(string),
        acks: 1,
        timeoutMs: 30000,
        topics: @[
          ProduceTopicReq(
            topic: "t",
            partitions: @[ProducePartitionReq(partition: 0'i32, recordSet: @[0xAA'u8])],
          )
        ],
      )
      let handle = await b.produceWrite(req)
      check b.inFlightRequests() == 1
      check connForTest(b).inFlightCount == 1

      b.produceAbandon(handle)
      check b.inFlightRequests() == 0
      check connForTest(b).inFlightCount == 0

      # Abandoning again is a no-op: the counts must not go negative.
      b.produceAbandon(handle)
      check b.inFlightRequests() == 0
      check connForTest(b).inFlightCount == 0

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

  test "await then abandon is a no-op (no double decrement)":
    ## Consuming a handle via produceAwait already settles the broker count;
    ## a later abandon must not decrement it again.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "produce-await-then-abandon-test")

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
                    apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          let pr = await fb.nextRequest(timeout = 2.seconds)
          try:
            await pr.respond(buildProduceResponseBody("t", 0'i32, 0'i16, 42))
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()

      let req = ProduceRequest(
        transactionalId: none(string),
        acks: 1,
        timeoutMs: 30000,
        topics: @[
          ProduceTopicReq(
            topic: "t",
            partitions: @[ProducePartitionReq(partition: 0'i32, recordSet: @[0xAA'u8])],
          )
        ],
      )
      let handle = await b.produceWrite(req)
      let resp = await b.produceAwait(handle)
      check resp.isSome
      check resp.get.topics[0].partitions[0].baseOffset == 42
      check b.inFlightRequests() == 0

      b.produceAbandon(handle)
      check b.inFlightRequests() == 0
      check connForTest(b).inFlightCount == 0

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()

  test "abandon then await raises":
    ## `produceAbandon` consumes the handle; awaiting it afterwards is a
    ## contract violation that must raise instead of silently double-settling
    ## the in-flight accounting.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let b = newBroker(0'i32, "127.0.0.1", port, "produce-abandon-then-await-test")

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
                    apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          # Respond to Produce; the abandoned handle must not consume it.
          let pr = await fb.nextRequest(timeout = 2.seconds)
          try:
            await pr.respond(buildProduceResponseBody("t", 0'i32, 0'i16, 42))
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      await b.connect()

      let req = ProduceRequest(
        transactionalId: none(string),
        acks: 1,
        timeoutMs: 30000,
        topics: @[
          ProduceTopicReq(
            topic: "t",
            partitions: @[ProducePartitionReq(partition: 0'i32, recordSet: @[0xAA'u8])],
          )
        ],
      )
      let handle = await b.produceWrite(req)
      b.produceAbandon(handle)
      check b.inFlightRequests() == 0

      expect KafkaError:
        discard await b.produceAwait(handle)

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor body()
