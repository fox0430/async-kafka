import std/[unittest, options]

import chronos

import async_kafka/[connection, errors]
import async_kafka/protocol/[api_keys, messages, primitives]

import ./fake_broker

suite "fake_broker: basics":
  test "echo round-trip via BrokerConnection":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn = await connection.connect(fb.address(), "test-client")

      let handler = proc(): Future[void] {.async: (raises: [Exception]).} =
        let req = await fb.nextRequest()
        check req.apiKey == int16(ord(akApiVersions))
        check req.apiVersion == 0'i16
        check req.correlationId == 0'i32
        check req.clientId == "test-client"
        await req.respond(@[0xDE'u8, 0xAD'u8, 0xBE'u8, 0xEF'u8])

      let handlerFut = handler()
      let resp = await conn.sendRequest(akApiVersions, 0'i16, @[])
      await handlerFut

      check resp.bodyBytes == @[0xDE'u8, 0xAD'u8, 0xBE'u8, 0xEF'u8]

      await conn.close()
      await fb.close()

    waitFor testBody()

  test "captures clientId and request body":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn = await connection.connect(fb.address(), "alice")

      let handler = proc(): Future[void] {.async: (raises: [Exception]).} =
        let req = await fb.nextRequest()
        check req.clientId == "alice"
        # Body for non-flexible request is exactly the bytes the test passed.
        check req.body == @[0x11'u8, 0x22'u8, 0x33'u8]
        await req.respond(@[])

      let handlerFut = handler()
      discard await conn.sendRequest(akProduce, 3'i16, @[0x11'u8, 0x22'u8, 0x33'u8])
      await handlerFut

      await conn.close()
      await fb.close()

    waitFor testBody()

  test "flexible response round-trip":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn = await connection.connect(fb.address(), "test-client")

      let handler = proc(): Future[void] {.async: (raises: [Exception]).} =
        let req = await fb.nextRequest()
        # The flexible request body starts with the request-header tagged_fields byte.
        check req.body.len >= 1
        check req.body[0] == 0x00'u8
        await req.respond(@[0xCA'u8, 0xFE'u8], flexible = true)

      let handlerFut = handler()
      let resp = await conn.sendRequest(akApiVersions, 3'i16, @[], flexible = true)
      await handlerFut

      # Connection layer strips the flexible response-header tagged_fields byte.
      check resp.bodyBytes == @[0xCA'u8, 0xFE'u8]

      await conn.close()
      await fb.close()

    waitFor testBody()

suite "fake_broker: ordering and timing":
  test "responds to multiple requests in reverse order":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn = await connection.connect(fb.address(), "test-client")

      let handler = proc(): Future[void] {.async: (raises: [Exception]).} =
        var captured: seq[CapturedRequest] = @[]
        for _ in 0 ..< 3:
          captured.add(await fb.nextRequest())
        # Respond in reverse order — each future should still resolve to its
        # own request body, proving correlation-id dispatch works.
        for i in countdown(captured.high, 0):
          let payload = @[byte(captured[i].correlationId and 0xFF)]
          await captured[i].respond(payload)

      let handlerFut = handler()
      let f0 = conn.sendRequest(akApiVersions, 0'i16, @[])
      let f1 = conn.sendRequest(akApiVersions, 0'i16, @[])
      let f2 = conn.sendRequest(akApiVersions, 0'i16, @[])
      let r0 = await f0
      let r1 = await f1
      let r2 = await f2
      await handlerFut

      check r0.bodyBytes == @[0x00'u8]
      check r1.bodyBytes == @[0x01'u8]
      check r2.bodyBytes == @[0x02'u8]

      await conn.close()
      await fb.close()

    waitFor testBody()

  test "delayed response is delivered":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn =
        await connection.connect(fb.address(), "test-client", requestTimeoutMs = 10_000)

      let handler = proc(): Future[void] {.async: (raises: [Exception]).} =
        let req = await fb.nextRequest()
        await sleepAsync(milliseconds(80))
        await req.respond(@[0x77'u8])

      let handlerFut = handler()
      let resp = await conn.sendRequest(akApiVersions, 0'i16, @[])
      await handlerFut

      check resp.bodyBytes == @[0x77'u8]

      await conn.close()
      await fb.close()

    waitFor testBody()

  test "split response (chunked write) round-trip":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn =
        await connection.connect(fb.address(), "test-client", requestTimeoutMs = 10_000)

      let handler = proc(): Future[void] {.async: (raises: [Exception]).} =
        let req = await fb.nextRequest()
        # The full response frame is [size:4] [corrId:4] [body...]; split it
        # after the 4-byte size prefix so the client must reassemble across
        # two reads.
        await req.respondInChunks(
          @[0xAA'u8, 0xBB'u8, 0xCC'u8, 0xDD'u8],
          firstChunkBytes = 4,
          delayBetween = milliseconds(50),
        )

      let handlerFut = handler()
      let resp = await conn.sendRequest(akApiVersions, 0'i16, @[])
      await handlerFut

      check resp.bodyBytes == @[0xAA'u8, 0xBB'u8, 0xCC'u8, 0xDD'u8]

      await conn.close()
      await fb.close()

    waitFor testBody()

suite "fake_broker: failure modes":
  test "transport closed mid-request fails sendRequest":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn =
        await connection.connect(fb.address(), "test-client", requestTimeoutMs = 30_000)

      let handler = proc(): Future[void] {.async: (raises: [Exception]).} =
        let req = await fb.nextRequest()
        await req.closeTransport()

      let handlerFut = handler()
      var raised = false
      try:
        discard await conn.sendRequest(akApiVersions, 0'i16, @[])
      except KafkaConnectionError:
        raised = true
      await handlerFut

      check raised
      check conn.closed

      await fb.close()

    waitFor testBody()

  test "waitAccepted unblocks after a connection arrives":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let waitFut = fb.waitAccepted(timeout = 5.seconds)
      let conn = await connection.connect(fb.address(), "test-client")
      await waitFut
      check fb.transports.len >= 1

      await conn.close()
      await fb.close()

    waitFor testBody()

  test "tryNextRequest is non-blocking and returns None when empty":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      check fb.tryNextRequest().isNone
      check fb.pendingRequests() == 0

      let conn = await connection.connect(fb.address(), "test-client")
      # sendRequestFireAndForget doesn't wait for a response, so we don't
      # block the test on never-arriving bytes.
      await conn.sendRequestFireAndForget(akProduce, 3'i16, @[0x42'u8])

      # Wait briefly for the fake broker to enqueue the request.
      var seen = false
      for _ in 0 ..< 50:
        if fb.pendingRequests() > 0:
          seen = true
          break
        await sleepAsync(milliseconds(10))
      check seen

      let opt = fb.tryNextRequest()
      check opt.isSome
      # Fire-and-forget uses the disjoint negative id range; the first FAF id is -1.
      check opt.get().correlationId == -1'i32

      await conn.close()
      await fb.close()

    waitFor testBody()

suite "fake_broker: response builders":
  test "ApiVersions v0 round-trip":
    let body = buildApiVersionsResponseBody(
      0'i16,
      [
        FakeApiVersion(apiKey: 0'i16, minVersion: 3, maxVersion: 3),
        FakeApiVersion(apiKey: 3'i16, minVersion: 1, maxVersion: 1),
        FakeApiVersion(apiKey: 18'i16, minVersion: 0, maxVersion: 0),
      ],
    )
    var r = initKafkaReader(body)
    let resp = r.decode(ApiVersionsResponse)
    check resp.errorCode == KafkaErrorCode(0)
    check resp.apiKeys.len == 3
    check resp.apiKeys[0].apiKey == 0'i16
    check resp.apiKeys[0].maxVersion == 3'i16
    check resp.apiKeys[2].apiKey == 18'i16
    check r.remaining == 0

  test "Metadata v1 round-trip":
    let body = buildMetadataResponseBody(
      [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: 9092'i32)],
      controllerId = 0'i32,
      topics = [
        FakeTopicMeta(
          name: "demo",
          errorCode: 0'i16,
          isInternal: false,
          partitions: @[
            FakePartitionMeta(
              partitionIndex: 0'i32, leaderId: 0'i32, replicas: @[0'i32], isr: @[0'i32]
            )
          ],
        )
      ],
    )
    var r = initKafkaReader(body)
    let resp = r.decode(MetadataResponse)
    check resp.brokers.len == 1
    check resp.brokers[0].host == "127.0.0.1"
    check resp.brokers[0].port == 9092'i32
    check resp.brokers[0].rack.isNone
    check resp.controllerId == 0'i32
    check resp.topics.len == 1
    check resp.topics[0].name == "demo"
    check resp.topics[0].partitions.len == 1
    check resp.topics[0].partitions[0].leaderId == 0'i32
    check resp.topics[0].partitions[0].replicaNodes == @[0'i32]
    check r.remaining == 0

  test "Produce v3 round-trip":
    let body = buildProduceResponseBody(
      topic = "demo",
      partition = 0'i32,
      errorCode = 0'i16,
      baseOffset = 42'i64,
      logAppendTimeMs = -1'i64,
      throttleTimeMs = 7'i32,
    )
    var r = initKafkaReader(body)
    let resp = r.decode(ProduceResponse)
    check resp.topics.len == 1
    check resp.topics[0].topic == "demo"
    check resp.topics[0].partitions.len == 1
    check resp.topics[0].partitions[0].partition == 0'i32
    check resp.topics[0].partitions[0].errorCode == KafkaErrorCode(0)
    check resp.topics[0].partitions[0].baseOffset == 42'i64
    check resp.topics[0].partitions[0].timestampMs == -1'i64
    check resp.throttleTimeMs == 7'i32
    check r.remaining == 0
