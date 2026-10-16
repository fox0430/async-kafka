## End-to-end metric event tests: the positive paths (producer.batch.flush,
## producer.send.ok/retry/error) must fire when the producer actually sends
## to a broker. Drives a real TCP handshake against FakeBroker and asserts
## the captured metric events. (Empty-batch flush guard: an empty extracted
## batch returns before producer.batch.flush is emitted, so no event fires.)

import std/[unittest, options]

import chronos

import async_kafka/[errors, cluster, broker]
import async_kafka/internal/metrics
import async_kafka/protocol/api_keys
import async_kafka/protocol/messages
import async_kafka/producer/producer
import async_kafka/producer/types

import fake_broker, producer_helpers, async_helpers

type
  ProduceBehavior = ref object
    responses: seq[int16]
      ## Per-Produce error codes consumed in order. After the seq is
      ## exhausted the last value is repeated, so a single trailing 0
      ## yields "fail once, then succeed forever".
    index: int

  MetricEvent = tuple[event: string, tags: seq[(string, string)], value: int64]

  MetricSink = ref object
    events: seq[MetricEvent]

const testTopic = "metrics-topic"

proc nextErrorCode(b: ProduceBehavior): int16 =
  if b.index >= b.responses.len:
    return b.responses[^1]
  result = b.responses[b.index]
  inc b.index

proc autoRespond(
    fb: FakeBroker, host: string, port: int32, topic: string, behavior: ProduceBehavior
) {.async: (raises: []).} =
  ## Background coroutine: dispatches FakeBroker requests by API key until
  ## nextRequest fails (timeout, close, transport error).
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 250.milliseconds)
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
        let errorCode = behavior.nextErrorCode()
        let body = buildProduceResponseBody(
          topic = topic,
          partition = 0'i32,
          errorCode = errorCode,
          baseOffset = 100'i64,
          logAppendTimeMs = -1'i64,
          throttleTimeMs = 0'i32,
        )
        await req.respond(body)
      # Unknown apiKey: drop silently — the producer's request will time out
      # and the test will fail visibly with a meaningful error.
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

proc eventNames(sink: MetricSink): seq[string] =
  for ev in sink.events:
    result.add(ev.event)

proc findEvent(sink: MetricSink, name: string): Option[MetricEvent] =
  for ev in sink.events:
    if ev.event == name:
      return some(ev)
  none(MetricEvent)

proc tagValue(ev: MetricEvent, key: string): Option[string] =
  for (k, v) in ev.tags:
    if k == key:
      return some(v)
  none(string)

var emitMetricHookEvals = 0

proc countingHook(): MetricsHook =
  ## Each call increments a global counter, so a test can detect whether the
  ## emitMetric template evaluates its `hook` argument more than once.
  inc emitMetricHookEvals
  result = proc(
      event: cstring, tags: openArray[(cstring, cstring)], value: int64
  ) {.gcsafe, raises: [].} =
    discard

suite "Metrics: emitMetric evaluates its hook argument once":
  ## Regression: the template referenced `hook` twice (the nil-check and the
  ## call), so a side-effecting hook expression was evaluated twice. It now
  ## binds the argument to a local first.

  test "cstring overload evaluates the hook expression exactly once":
    emitMetricHookEvals = 0
    emitMetric(countingHook(), meProducerSendOk, [(cstring"k", cstring"v")], 1'i64)
    check emitMetricHookEvals == 1

  test "string overload evaluates the hook expression exactly once":
    emitMetricHookEvals = 0
    emitMetric(countingHook(), meProducerSendOk, [("k", "v")], 1'i64)
    check emitMetricHookEvals == 1

suite "Producer: end-to-end metric events":
  test "happy path emits batch.flush and send.ok":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)

      let behavior = ProduceBehavior(responses: @[0'i16])
      let respFut = autoRespond(fb, host, port, testTopic, behavior)

      let sink = makeSink()
      let c = newCluster(@[host & ":" & $port], clientId = "metrics-test")
      # lingerMs > 0 with the default maxBatchRecords exercises the
      # linger-timer-driven flushBatch path. This is also a regression
      # test for lingerTimeout self-cancellation in flushBatch.
      let p = newKafkaProducer(c, lingerMs = 20, metricsHook = makeHook(sink))

      let res = await p.send(testTopic, @[0xA1'u8, 0xA2'u8])
      check res.partition == 0'i32
      check res.offset == 100'i64

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

      let names = sink.eventNames()
      check "producer.batch.flush" in names
      check "producer.send.ok" in names
      check "producer.send.retry" notin names
      check "producer.send.error" notin names

      let flushEv = sink.findEvent("producer.batch.flush").get()
      check flushEv.tagValue("topic").get() == testTopic
      check flushEv.tagValue("partition").get() == "0"
      check flushEv.value == 1'i64

      let okEv = sink.findEvent("producer.send.ok").get()
      check okEv.tagValue("topic").get() == testTopic
      check okEv.tagValue("partition").get() == "0"
      check okEv.value == 1'i64

    waitFor body()

  test "retriable error emits batch.flush, send.retry, then send.ok":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)

      # error 19 = NOT_ENOUGH_REPLICAS (retriable). One failure then success.
      let behavior = ProduceBehavior(responses: @[19'i16, 0'i16])
      let respFut = autoRespond(fb, host, port, testTopic, behavior)

      let sink = makeSink()
      let c = newCluster(@[host & ":" & $port], clientId = "metrics-test")
      let p = newKafkaProducer(
        c,
        lingerMs = 20,
        retryBackoffMs = 10,
        maxRetries = 3,
        metricsHook = makeHook(sink),
      )

      let res = await p.send(testTopic, @[0xB1'u8])
      check res.offset == 100'i64

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

      let names = sink.eventNames()
      check "producer.batch.flush" in names
      check "producer.send.retry" in names
      check "producer.send.ok" in names
      check "producer.send.error" notin names

      let retryEv = sink.findEvent("producer.send.retry").get()
      check retryEv.tagValue("topic").get() == testTopic
      check retryEv.tagValue("partition").get() == "0"
      # One shape for every retry (error_code + reason): error_code is the
      # SCREAMING_SNAKE failure kind a dashboard GROUPs BY, reason the
      # lowercase context of this attempt.
      check retryEv.tagValue("error_code").get() == "NOT_ENOUGH_REPLICAS"
      check retryEv.tagValue("reason").get() == "not_enough_replicas"
      check retryEv.value == 1'i64 # attempt = 1 on the first retry
      # backoff_ms is a measurement, not a tag — it must arrive as the value
      # of its own event so high-cardinality numeric values don't pollute the
      # tag dimension.
      check "producer.send.retry.backoff_ms" in names
      let backoffEv = sink.findEvent("producer.send.retry.backoff_ms").get()
      check backoffEv.tagValue("topic").get() == testTopic
      check backoffEv.tagValue("partition").get() == "0"
      check backoffEv.tagValue("reason").isSome
      check backoffEv.tagValue("backoff_ms").isNone
      check backoffEv.value > 0'i64

    waitFor body()

  test "producer-id reset retry path backs off (no RPC storm)":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)

      # Always reject Produce with OUT_OF_ORDER_SEQUENCE_NUMBER so the idempotent
      # producer keeps driving the producer-id reset path. Without a backoff
      # between resets this hammers InitProducerId+Produce at RTT cadence; the
      # fix emits producer.send.retry.backoff_ms and sleeps before each retry.
      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 1.seconds)
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
                    FakeApiVersion(
                      apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                    ),
                  ],
                )
              )
            elif key == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                  controllerId = 0'i32,
                  topics = [
                    FakeTopicMeta(
                      name: testTopic,
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
              )
            elif key == int16(ord(akInitProducerId)):
              await req.respond(
                buildInitProducerIdResponseBody(
                  errorCode = 0'i16, producerId = 1000'i64, producerEpoch = 0'i16
                )
              )
            elif key == int16(ord(akProduce)):
              await req.respond(
                buildProduceResponseBody(
                  topic = testTopic,
                  partition = 0'i32,
                  errorCode = int16(ord(keOutOfOrderSequenceNumber)),
                  baseOffset = -1'i64,
                )
              )
          except CatchableError:
            break

      let respFut = handler()

      let sink = makeSink()
      let c = newCluster(@[host & ":" & $port], clientId = "metrics-test")
      # Idempotent producers clamp maxRetries to int32.high, so only the
      # delivery-timeout bounds the loop. Keep it short so the send terminates
      # after a handful of (backed-off) reset cycles instead of running for the
      # default 120s.
      let p = newKafkaProducer(
        c,
        enableIdempotence = true,
        lingerMs = 0,
        retryBackoffMs = 5,
        deliveryTimeoutMs = 500,
        metricsHook = makeHook(sink),
      )

      var raised = false
      try:
        discard await p.send(testTopic, @[0xC1'u8]).wait(5.seconds)
      except KafkaError:
        raised = true
      check raised

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

      # The reset retry path must back off (emit a backoff_ms measurement), not
      # spin InitProducerId+Produce with zero delay.
      check "producer.send.retry.backoff_ms" in sink.eventNames()
      var sawResetBackoff = false
      var sawResetRetry = false
      for ev in sink.events:
        if ev.event == "producer.send.retry.backoff_ms" and
            ev.tagValue("reason") == some("producer_id_reset"):
          check ev.value > 0'i64
          sawResetBackoff = true
        if ev.event == "producer.send.retry" and
            ev.tagValue("reason") == some("producer_id_reset"):
          check ev.tagValue("error_code").get() == "OUT_OF_ORDER_SEQUENCE_NUMBER"
          sawResetRetry = true
      check sawResetBackoff
      check sawResetRetry
      # The 500ms delivery deadline elapses while the reset path retries, so the
      # send ends on this event instead of looping until the RPC budget runs out.
      check "producer.send.delivery_timeout" in sink.eventNames()
      # Each reset re-encodes the batches still in flight; without this the
      # sequence rewrite is invisible until a broker rejects one.
      check findEvent(sink, "producer.inflight.resequenced").isSome

    waitFor body()

  test "re-init failure on the reset retry path surfaces the original error":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)

      # Produce always returns OUT_OF_ORDER_SEQUENCE_NUMBER (drives a producer-id
      # reset). The first InitProducerId (lazy init) succeeds; the re-init during
      # the reset fails. The send must surface the ORIGINAL OUT_OF_ORDER error,
      # not the re-init error that masked it before the fix.
      var initCount = 0
      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 1.seconds)
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
                    FakeApiVersion(
                      apiKey: int16(ord(akInitProducerId)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akProduce)), minVersion: 3, maxVersion: 3
                    ),
                  ],
                )
              )
            elif key == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: host, port: port)],
                  controllerId = 0'i32,
                  topics = [
                    FakeTopicMeta(
                      name: testTopic,
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
              )
            elif key == int16(ord(akInitProducerId)):
              inc initCount
              # First init (lazy) succeeds; the re-init during reset fails.
              let ec =
                if initCount == 1: 0'i16 else: keCoordinatorLoadInProgress.toInt16
              await req.respond(
                buildInitProducerIdResponseBody(
                  errorCode = ec, producerId = 1000'i64, producerEpoch = 0'i16
                )
              )
            elif key == int16(ord(akProduce)):
              await req.respond(
                buildProduceResponseBody(
                  topic = testTopic,
                  partition = 0'i32,
                  errorCode = int16(ord(keOutOfOrderSequenceNumber)),
                  baseOffset = -1'i64,
                )
              )
          except CatchableError:
            break

      let respFut = handler()
      let c = newCluster(@[host & ":" & $port], clientId = "metrics-test")
      let p = newKafkaProducer(
        c, enableIdempotence = true, lingerMs = 0, retryBackoffMs = 5, timeoutMs = 2000
      )

      var raisedCode = keNone
      try:
        discard await p.send(testTopic, @[0xD1'u8]).wait(5.seconds)
      except KafkaApiError as e:
        raisedCode = e.errorCode
      except KafkaError:
        discard
      # The original OUT_OF_ORDER must reach the caller, not the re-init's
      # COORDINATOR_LOAD_IN_PROGRESS.
      check raisedCode == keOutOfOrderSequenceNumber

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor body()

  test "non-retriable error emits batch.flush and send.error":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)

      # error 29 = TOPIC_AUTHORIZATION_FAILED (fatal, non-retriable).
      let behavior = ProduceBehavior(responses: @[29'i16])
      let respFut = autoRespond(fb, host, port, testTopic, behavior)

      let sink = makeSink()
      let c = newCluster(@[host & ":" & $port], clientId = "metrics-test")
      let p = newKafkaProducer(
        c,
        lingerMs = 20,
        retryBackoffMs = 10,
        maxRetries = 3,
        metricsHook = makeHook(sink),
      )

      # KafkaApiError must survive the batched-future hop in send() — this
      # also regression-tests that the dynamic subtype is preserved.
      var raised = false
      var apiErrCode: KafkaErrorCode
      try:
        discard await p.send(testTopic, @[0xC1'u8])
      except KafkaApiError as e:
        raised = true
        apiErrCode = e.errorCode
      check raised
      check apiErrCode == keTopicAuthorizationFailed

      await p.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

      let names = sink.eventNames()
      check "producer.batch.flush" in names
      check "producer.send.error" in names
      check "producer.send.ok" notin names
      check "producer.send.retry" notin names

      let errEv = sink.findEvent("producer.send.error").get()
      check errEv.tagValue("topic").get() == testTopic
      check errEv.tagValue("partition").get() == "0"
      check errEv.tagValue("error_code").isSome
      # TOPIC_AUTHORIZATION_FAILED (29) is ecFatal: dashboards group on
      # `error_class` to separate hopeless failures from transient ones
      # without re-classifying each error_code value on the consumer side.
      check errEv.tagValue("error_class").get() == "fatal"

    waitFor body()

suite "Producer: DLQ and txn lifecycle metrics":
  ## Regression coverage for producer.txn.* events only: dashboards track
  ## transaction health independently of producer.send.error. producer.dropped
  ## and producer.dlq.* are covered elsewhere.

  test "producer.txn.commit.error fires on EndTxn transport failure":
    proc body() {.async: (raises: [Exception]).} =
      let sink = makeSink()
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 200,
        metricsHook = makeHook(sink),
      )
      # No real coordinator; EndTxn will fail at the connect step, which
      # commitTransaction maps to its KafkaError catch block. The metric
      # must fire regardless of which branch ends the transaction.
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      p.setTxnState(tsInTransaction)
      # Register a partition so the transaction is started broker-side and
      # commitTransaction actually sends EndTxn (which fails at connect) — an
      # empty transaction would skip EndTxn entirely and report success.
      p.addTxnPartition(TopicPartition(topic: "t", partition: 0))
      var raised = false
      try:
        await p.commitTransaction()
      except KafkaError:
        raised = true
      check raised
      check "producer.txn.commit.error" in sink.eventNames()
      check "producer.txn.commit.ok" notin sink.eventNames()
      await dummy.close()
      await p.close()
      await cluster.close()

    waitFor body()

  test "producer.txn.abort.error fires on EndTxn transport failure":
    proc body() {.async: (raises: [Exception]).} =
      let sink = makeSink()
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        timeoutMs = 200,
        metricsHook = makeHook(sink),
      )
      let dummy = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 100,
        connectTimeoutMs = 100,
      )
      p.setTxnCoordinator(dummy)
      p.setTxnState(tsInTransaction)
      # Register a partition so the transaction is started broker-side and
      # abortTransaction actually sends EndTxn(abort) (which fails at connect) —
      # an empty transaction would skip EndTxn entirely and report success.
      p.addTxnPartition(TopicPartition(topic: "t", partition: 0))
      var raised = false
      try:
        await p.abortTransaction()
      except KafkaError:
        raised = true
      check raised
      check "producer.txn.abort.error" in sink.eventNames()
      check "producer.txn.abort.ok" notin sink.eventNames()
      await dummy.close()
      await p.close()
      await cluster.close()

    waitFor body()
