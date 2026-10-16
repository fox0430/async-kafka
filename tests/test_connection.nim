import std/[unittest, options, strutils, logging]

import chronos
import pkg/stew/endians2

import async_kafka/[connection, tls_anchors, broker, errors]
import async_kafka/protocol/api_keys

import fake_broker, broker_helpers, async_helpers

proc dummyCallback(
    server: StreamServer, transport: StreamTransport
) {.async: (raises: []).} =
  ## Callback that just keeps the connection open until the transport is closed.
  try:
    while not transport.closed:
      await sleepAsync(milliseconds(50))
  except CancelledError:
    discard
  await transport.closeWait()

proc closeImmediatelyCallback(
    server: StreamServer, transport: StreamTransport
) {.async: (raises: []).} =
  ## Callback that immediately closes the accepted connection.
  await transport.closeWait()

type ThrowingLogger = ref object of Logger
  calls: int

method log*(
    logger: ThrowingLogger, level: Level, args: varargs[string, `$`]
) {.gcsafe.} =
  inc logger.calls
  raise newException(IOError, "intentional logger failure")

template withThrowingLogger(body: untyped): untyped =
  block:
    let logger = ThrowingLogger()
    let previousLogFilter = getLogFilter()
    setLogFilter(lvlAll)
    addHandler(logger)
    try:
      body
    finally:
      removeHandler(logger)
      setLogFilter(previousLogFilter)
    logger.calls

suite "Connection: effective timeout policy":
  test "non-positive timeouts use the bounded indefinite wait":
    check effectiveTimeoutMs(0'i32) == 600_000'i32
    check effectiveTimeoutMs(-1'i32) == 600_000'i32
    check effectiveTimeoutMs(int32.low) == 600_000'i32

  test "positive timeouts are preserved":
    check effectiveTimeoutMs(1'i32) == 1'i32
    check effectiveTimeoutMs(30_000'i32) == 30_000'i32
    check effectiveTimeoutMs(int32.high) == int32.high

suite "Connection: TLS handshake failure cleanup":
  test "TLS handshake to plain TCP port raises KafkaConnectionError":
    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, closeImmediatelyCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let tlsConfig = newTlsConfig(skipVerify = true)
      var raised = false
      try:
        let conn =
          await connection.connect(boundAddr, "test-client", "localhost", tlsConfig)
        await conn.close()
      except KafkaConnectionError:
        raised = true

      check raised == true
      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "TLS handshake failure does not leak resources":
    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, closeImmediatelyCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      for i in 0 ..< 10:
        try:
          let conn = await connection.connect(
            boundAddr, "test-client", "localhost", newTlsConfig(skipVerify = true)
          )
          await conn.close()
        except KafkaConnectionError:
          discard

      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "TLS handshake to a silent peer times out via connectTimeoutMs":
    ## A peer that accepts TCP but never speaks TLS (a plaintext port reached over
    ## TLS, a hung broker) must not leave the handshake pending forever.
    ## connectTimeoutMs has to bound the handshake itself, not just the TCP connect —
    ## TCP keepalive never fires for an alive-but-silent peer.
    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      # dummyCallback accepts and holds the connection open but never sends a
      # byte, so the TLS ServerHello never arrives and the handshake stalls.
      let server = createStreamServer(address, dummyCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let tlsConfig = newTlsConfig(skipVerify = true)
      var raised = false
      var msg = ""
      let started = Moment.now()
      try:
        let conn = await connection.connect(
          boundAddr, "test-client", "localhost", tlsConfig, connectTimeoutMs = 200'i32
        )
        await conn.close()
      except KafkaConnectionError as e:
        raised = true
        msg = e.msg
      let elapsedMs = (Moment.now() - started).milliseconds

      check raised == true
      # The handshake-timeout branch fired specifically, not an unrelated error.
      check "timed out" in msg
      # And it fired near the configured budget rather than hanging on the 30s
      # default — a generous upper bound keeps CI non-flaky.
      check elapsedMs < 5_000

      server.stop()
      await server.closeWait()

    waitFor testBody()

# --- PEM trust anchors ------------------------------------------------------
#
# Throwaway certificates generated for this suite. The RSA one and the EC one
# only exist to exercise the two public-key branches of the decoder; the
# private key stands in for "PEM data with no certificate in it".

const
  RsaCaPem = """
-----BEGIN CERTIFICATE-----
MIIDJTCCAg2gAwIBAgIUfNaiXujOWGFXShTww/phwtWopNowDQYJKoZIhvcNAQEL
BQAwIjEgMB4GA1UEAwwXYXN5bmMta2Fma2EgdGVzdCBSU0EgQ0EwHhcNMjYxMDEz
MTc1NjUwWhcNMzYxMDEwMTc1NjUwWjAiMSAwHgYDVQQDDBdhc3luYy1rYWZrYSB0
ZXN0IFJTQSBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAI9n5Czt
3RwE66DQSzZqLeSEf+eQZS64/8a2Uc4hFYImL7LgkmPNc76vMgYm//1dWax/byxa
F+22KHSqCAa9ZqY+KABRjbLLPp9nda1Q1NddDzhXI/SLzO/5ORwViUQ4D4WJViii
hVhGKumdcPQWhbNn9we+PYrzohpYO5tEkVJ4+d73dplj97RyAivO8iLeMpISYwD6
PpMF/BpMGh2til7b05wfmgzxhBFz0UqoJ4oYTZPMf6mmwmO1D9Z8361ZjQ3Bkvd9
nHltUvV5X491cQ7nvQKvoOnj0/G4hf5VgOVRWPKW3D1YtOyZrjCCH92SIjXo63BH
rARQ0EuMmZmEzxMCAwEAAaNTMFEwHQYDVR0OBBYEFM7XQRa/MZKt5bPGQxlE3D6j
K6cuMB8GA1UdIwQYMBaAFM7XQRa/MZKt5bPGQxlE3D6jK6cuMA8GA1UdEwEB/wQF
MAMBAf8wDQYJKoZIhvcNAQELBQADggEBAIjzgS7lmscu4j0M0esOOpikpbbzSlhk
0cX9CvDIfy4haXqK1yadCJn377bekdl9V3kEygMnUQql4bGz1CxW3akKLKIVjRmF
OOywrUBJjv+qOZeyEx8DQlhWqj/TUfCmeh3pNy3tgahZzrhkZdPJkhFs3qLYv8Vn
ClHzX9hwXQeXtCDgqp/kn0NpxVJeEL7HNPuqbH1ffCT23Hiv+tuclfuoKFAK0Y6c
WsUxOUu6+dVZ3iaJvQ1DvsJTn86YUMaZlRtFe+vR2tbkFWEHUM2MoNwfo3nF77UN
DMYCPFkJ05cO58mr8lZktfXfh6AmhkffMm5iBkZdCs/hLCvxbHaWpzI=
-----END CERTIFICATE-----
"""

  EcCaPem = """
-----BEGIN CERTIFICATE-----
MIIBlzCCAT2gAwIBAgIUU6k5mIZ06bPr9qkKeaYPsGekOkwwCgYIKoZIzj0EAwIw
ITEfMB0GA1UEAwwWYXN5bmMta2Fma2EgdGVzdCBFQyBDQTAeFw0yNjEwMTMxNzU2
NTBaFw0zNjEwMTAxNzU2NTBaMCExHzAdBgNVBAMMFmFzeW5jLWthZmthIHRlc3Qg
RUMgQ0EwWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAS0OxjKOUNFmelHQdzQsHUn
vkAcnq4VLlJhbiA+IF5Hy6PsM6XpXKP7oloDBp1/3kiwG1YVJq7QxqbjR9c+UZAm
o1MwUTAdBgNVHQ4EFgQUEG2koNGFKyv3fo3YbvJFeScOWKQwHwYDVR0jBBgwFoAU
EG2koNGFKyv3fo3YbvJFeScOWKQwDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQD
AgNIADBFAiAyJdpKSr+etfaHWJxRaXnAyZDaHbfp2VaSTHfgCelj6wIhAJ8Ysji4
WIZDEIxSIE/4FgRx3huusXDX+KLuLnqYbGfZ
-----END CERTIFICATE-----
"""

  PrivateKeyOnlyPem = """
-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg2vTv3wFj/QYfSMYg
cvnrAO2TWOT7n1gg4/itPKPupS2hRANCAASUtQORsPyLufm91IKBVSZ5WiO50N7F
V1CKdecE4Hr6mlwLgWxP6ts4LXaLb+uNnxq1jxQFW1gQAgFDR8gyyz3M
-----END PRIVATE KEY-----
"""

suite "Connection: PEM trust anchors":
  test "an RSA and an EC certificate are both accepted":
    check newTrustAnchorStoreFromPem(RsaCaPem) != nil
    check newTrustAnchorStoreFromPem(EcCaPem) != nil
    check newTrustAnchorStoreFromPem(RsaCaPem & EcCaPem) != nil

  test "PEM data without a certificate block raises":
    expect KafkaError:
      discard newTrustAnchorStoreFromPem(PrivateKeyOnlyPem)

  test "an unterminated certificate block raises":
    expect KafkaError:
      discard newTrustAnchorStoreFromPem("-----BEGIN CERTIFICATE-----\nAAAA\n")

  test "invalid base64 raises":
    expect KafkaError:
      discard newTrustAnchorStoreFromPem(
        "-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n"
      )

  test "a block that is not a certificate raises":
    expect KafkaError:
      discard newTrustAnchorStoreFromPem(
        "-----BEGIN CERTIFICATE-----\nbm90IGEgY2VydGlmaWNhdGU=\n" &
          "-----END CERTIFICATE-----\n"
      )

suite "Connection: non-TLS connect and close":
  test "Connect and close to plain TCP server":
    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, dummyCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let conn = await connection.connect(boundAddr, "test-client")
      check conn.closed == false

      await conn.close()
      check conn.closed == true

      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "Close is idempotent":
    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, dummyCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let conn = await connection.connect(boundAddr, "test-client")

      await conn.close()
      await conn.close()
      check conn.closed == true

      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "Non-positive connectTimeoutMs means indefinite, not immediate timeout":
    ## connectTimeoutMs <= 0 used to be passed straight to wait() and fire an
    ## immediate timeout, contradicting awaitResponse which clamps a non-positive
    ## timeout to "wait (almost) indefinitely". connect() now mirrors that, so 0 and
    ## negatives succeed against a live server instead of raising.
    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, dummyCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      for timeout in [0'i32, -5'i32]:
        let conn =
          await connection.connect(boundAddr, "test-client", connectTimeoutMs = timeout)
        check conn.closed == false
        await conn.close()

      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "close() waits for an in-flight write before tearing down the writer":
    ## close() must serialize writer teardown behind writeLock, so a concurrent
    ## write (which holds writeLock around writer.write()) is never closed underneath.
    ## We stand in by holding the lock, then assert close() blocks until it is released.
    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, dummyCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let conn = await connection.connect(boundAddr, "test-client")

      # Stand in for an in-flight write that owns writeLock.
      await conn.acquireWriteLockForTest()

      let closeFut = conn.close()
      await sleepAsync(milliseconds(50))
      # close() has done failAll + readLoop cancel but must now be blocked
      # acquiring writeLock — it cannot have torn down the writer yet.
      check not closeFut.finished

      # Release the lock; close() acquires it and finishes the teardown.
      conn.releaseWriteLockForTest()
      # close() raises nothing, so wait() on it directly would trigger
      # chronos's "failed always returns false" warning; join() avoids it
      # (and a timeout won't cancel the close itself).
      await closeFut.join().wait(2.seconds)
      check conn.closed == true

      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "Connect to refused port raises KafkaConnectionError":
    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:1")
      var raised = false
      try:
        let conn = await connection.connect(address, "test-client")
        await conn.close()
      except KafkaConnectionError:
        raised = true
      check raised == true

    waitFor testBody()

suite "Connection: malformed flexible response":
  test "Malformed varint in flexible response fails only that slot":
    ## Per-response parse errors fail just the affected slot; the connection
    ## itself stays open so concurrent in-flight requests can still complete.
    proc testBody() {.async: (raises: [Exception]).} =
      ## Send a response frame where the tagged_fields varint has all continuation
      ## bits set (no terminator), so the frame ends mid-varint: the decoder
      ## allows varints up to 10 bytes.
      proc malformedVarintCallback(
          server: StreamServer, transport: StreamTransport
      ) {.async: (raises: []).} =
        try:
          var sizeBuf: array[4, byte]
          await transport.readExactly(addr sizeBuf[0], 4)
          let reqSize = int(fromBytesBE(uint32, sizeBuf))
          var reqBuf = newSeq[byte](reqSize)
          await transport.readExactly(addr reqBuf[0], reqSize)

          # Correlation ID is at bytes 4..7 (after apiKey + apiVersion).
          let corrId = fromBytesBE(uint32, [reqBuf[4], reqBuf[5], reqBuf[6], reqBuf[7]])

          # Build a malformed response: correlation ID + 6 bytes of 0xFF (all continuation bits)
          var resp: seq[byte]
          let corrBytes = toBytesBE(corrId)
          resp.add(corrBytes)
          # 6 bytes of 0xFF — no terminator, so the frame ends mid-varint
          for i in 0 ..< 6:
            resp.add(0xFF'u8)

          var frameSize = toBytesBE(uint32(resp.len))
          discard await transport.write(addr frameSize[0], 4)
          discard await transport.write(addr resp[0], resp.len)
        except TransportError, CancelledError:
          discard
        await transport.closeWait()

      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, malformedVarintCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let conn = await connection.connect(boundAddr, "test-client")

      var raised = false
      try:
        discard await conn.sendRequest(akApiVersions, 0'i16, @[], flexible = true)
      except KafkaProtocolError:
        raised = true

      check raised == true
      # Connection is not torn down by the parse error itself; it may still
      # close later because the test server hangs up its socket, but that's
      # separate from the per-slot failure path.

      server.stop()
      await server.closeWait()

    waitFor testBody()

suite "Connection: oversized frame size guard":
  test "rawFrameSize beyond defaultMaxFrameSize closes connection":
    proc testBody() {.async: (raises: [Exception]).} =
      ## Send a response whose 4-byte length prefix encodes a frame larger
      ## than the 100MB internal cap. The client must reject up front,
      ## bail out of readLoop, and fail any in-flight request — never
      ## attempt to allocate the claimed buffer.
      proc oversizedFrameCallback(
          server: StreamServer, transport: StreamTransport
      ) {.async: (raises: []).} =
        try:
          var sizeBuf: array[4, byte]
          await transport.readExactly(addr sizeBuf[0], 4)
          let reqSize = int(fromBytesBE(uint32, sizeBuf))
          var reqBuf = newSeq[byte](reqSize)
          await transport.readExactly(addr reqBuf[0], reqSize)
          # Respond with a frame size one byte past the documented cap.
          # The client must not allocate the body; it should treat this as
          # a protocol error and tear down the connection.
          let bogusSize: uint32 = 104_857_601'u32
          var prefix = toBytesBE(bogusSize)
          discard await transport.write(addr prefix[0], 4)
        except TransportError, CancelledError:
          discard
        await transport.closeWait()

      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, oversizedFrameCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let conn = await connection.connect(boundAddr, "test-client")
      var raised = false
      try:
        discard await conn.sendRequest(akApiVersions, 0'i16, @[], flexible = true)
      except KafkaConnectionError:
        raised = true

      check raised == true
      check conn.closed == true

      server.stop()
      await server.closeWait()

    waitFor testBody()

suite "Connection: readLoop safe error cleanup":
  test "invalid frame fails in-flight requests before a throwing logger runs":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      try:
        let conn = await connection.connect(
          fb.address(), "safe-log-client", requestTimeoutMs = 500
        )
        try:
          let requestFut = conn.sendRequest(akApiVersions, 0'i16, @[])
          let request = await fb.nextRequest()

          let loggerCalls = withThrowingLogger:
            # A zero length is below the four-byte correlation-id minimum.
            await request.respondRaw(@[0x00'u8, 0x00'u8, 0x00'u8, 0x00'u8])

            var failedWithConnectionError = false
            try:
              discard await requestFut
            except KafkaConnectionError:
              failedWithConnectionError = true

            check failedWithConnectionError
            check conn.closed
            check conn.inFlightCount == 0
            # A logger exception must not become the readLoop future's exception.
            check await conn.readLoopCompletedForTest()
          check loggerCalls == 1
        finally:
          await conn.close()
      finally:
        await fb.close()

    waitFor testBody()

  test "transport exit fails in-flight requests before a throwing logger runs":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      try:
        let conn = await connection.connect(
          fb.address(), "safe-log-client", requestTimeoutMs = 500, maxInFlight = 2
        )
        try:
          let first = conn.sendRequest(akApiVersions, 0'i16, @[])
          let second = conn.sendRequest(akApiVersions, 0'i16, @[])
          let firstRequest = await fb.nextRequest()
          discard await fb.nextRequest()

          conn.armReadLoopTransportErrorForTest()
          let loggerCalls = withThrowingLogger:
            # Complete the read that was already pending. The next iteration
            # raises the injected TransportError inside readLoop's own try.
            await firstRequest.respond(@[0x42'u8])
            check (await first).bodyBytes == @[0x42'u8]

            var failedWithConnectionError = false
            try:
              discard await second
            except KafkaConnectionError:
              failedWithConnectionError = true

            check failedWithConnectionError
            check conn.closed
            check conn.inFlightCount == 0
            check await conn.readLoopCompletedForTest()
          check loggerCalls == 1
        finally:
          await conn.close()
      finally:
        await fb.close()

    waitFor testBody()

  test "stream exit fails siblings and completes cleanly when the logger throws":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      try:
        let conn = await connection.connect(
          fb.address(), "safe-log-client", requestTimeoutMs = 500, maxInFlight = 2
        )
        try:
          let first = conn.sendRequest(akApiVersions, 0'i16, @[])
          let second = conn.sendRequest(akApiVersions, 0'i16, @[])
          discard await fb.nextRequest()
          discard await fb.nextRequest()

          let loggerCalls = withThrowingLogger:
            await fb.closeAllTransports()

            var failed = 0
            for requestFut in [first, second]:
              try:
                discard await requestFut
              except KafkaConnectionError:
                inc failed

            check failed == 2
            check conn.closed
            check conn.inFlightCount == 0
            check await conn.readLoopCompletedForTest()
          check loggerCalls == 1
        finally:
          await conn.close()
      finally:
        await fb.close()

    waitFor testBody()

suite "Connection: sendRequestFireAndForget frame format":
  test "sendRequestFireAndForget sends correctly framed request":
    proc testBody() {.async: (raises: [Exception]).} =
      var receivedFrame: seq[byte]
      proc captureCallback(
          server: StreamServer, transport: StreamTransport
      ) {.async: (raises: []).} =
        try:
          var sizeBuf: array[4, byte]
          await transport.readExactly(addr sizeBuf[0], 4)
          let reqSize = int(fromBytesBE(uint32, sizeBuf))
          receivedFrame = newSeq[byte](reqSize)
          await transport.readExactly(addr receivedFrame[0], reqSize)
        except TransportError, CancelledError:
          discard
        await transport.closeWait()

      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, captureCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let conn = await connection.connect(boundAddr, "test-client")
      let body = @[0xAA'u8, 0xBB'u8]
      await conn.sendRequestFireAndForget(akProduce, 3'i16, body)

      await sleepAsync(milliseconds(50))

      # Parse header: apiKey(2) + apiVersion(2) + corrId(4) + clientId(2+N) + body
      check receivedFrame.len > 0
      let apiKey = int16(fromBytesBE(uint16, [receivedFrame[0], receivedFrame[1]]))
      let apiVer = int16(fromBytesBE(uint16, [receivedFrame[2], receivedFrame[3]]))
      let corrId = cast[int32](fromBytesBE(
        uint32, [receivedFrame[4], receivedFrame[5], receivedFrame[6], receivedFrame[7]]
      ))
      check apiKey == 0'i16 # akProduce
      check apiVer == 3'i16
      # Fire-and-forget allocates from the disjoint negative id range so it can
      # never alias a slotted request; the first FAF id is -1.
      check corrId == -1'i32
      check receivedFrame[^2] == 0xAA'u8
      check receivedFrame[^1] == 0xBB'u8

      await conn.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "sendRequestFireAndForget respects in-flight window":
    proc testBody() {.async: (raises: [Exception]).} =
      var requestCount = 0
      proc serverCallback(
          server: StreamServer, transport: StreamTransport
      ) {.async: (raises: []).} =
        try:
          while true:
            var sizeBuf: array[4, byte]
            await transport.readExactly(addr sizeBuf[0], 4)
            let reqSize = int(fromBytesBE(uint32, sizeBuf))
            var frame = newSeq[byte](reqSize)
            await transport.readExactly(addr frame[0], reqSize)
            requestCount.inc
        except TransportError, CancelledError:
          discard
        await transport.closeWait()

      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, serverCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      # maxInFlight=1: only one concurrent request allowed
      let conn = await connection.connect(boundAddr, "test-client", maxInFlight = 1)

      # First fire-and-forget: acquires and releases semaphore
      await conn.sendRequestFireAndForget(akProduce, 3'i16, @[0x01'u8])
      # Second fire-and-forget: should succeed because semaphore was released
      await conn.sendRequestFireAndForget(akProduce, 3'i16, @[0x02'u8])

      await sleepAsync(milliseconds(50))
      check requestCount == 2

      await conn.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

suite "Connection: flexible frame format":
  test "sendRequest with flexible=true uses compact string encoding for clientId":
    proc testBody() {.async: (raises: [Exception]).} =
      var receivedFrame: seq[byte]
      proc captureAndReplyCallback(
          server: StreamServer, transport: StreamTransport
      ) {.async: (raises: []).} =
        try:
          var sizeBuf: array[4, byte]
          await transport.readExactly(addr sizeBuf[0], 4)
          let reqSize = int(fromBytesBE(uint32, sizeBuf))
          receivedFrame = newSeq[byte](reqSize)
          await transport.readExactly(addr receivedFrame[0], reqSize)

          let corrId = fromBytesBE(
            uint32,
            [receivedFrame[4], receivedFrame[5], receivedFrame[6], receivedFrame[7]],
          )

          # Build a valid flexible response: corrId + tagged_fields(0x00)
          var resp: seq[byte]
          let corrBytes = toBytesBE(corrId)
          resp.add(corrBytes)
          resp.add(0x00'u8) # empty tagged_fields

          var frameSize = toBytesBE(uint32(resp.len))
          discard await transport.write(addr frameSize[0], 4)
          discard await transport.write(addr resp[0], resp.len)
        except TransportError, CancelledError:
          discard
        await transport.closeWait()

      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, captureAndReplyCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let clientId = "test-client" # 11 bytes
      let conn = await connection.connect(boundAddr, clientId)
      let body = @[0xAA'u8, 0xBB'u8]
      discard await conn.sendRequest(akApiVersions, 3'i16, body, flexible = true)

      # Parse header: apiKey(2) + apiVersion(2) + corrId(4) = offset 8
      # ClientId is always NULLABLE_STRING (int16 length prefix), even in flexible
      # headers — the Kafka RequestHeader schema specifies flexibleVersions: "none"
      # for ClientId.
      check receivedFrame.len > 8
      # Standard encoding: int16(11) = 0x00 0x0B (2 bytes)
      check receivedFrame[8] == 0x00'u8
      check receivedFrame[9] == 0x0B'u8
      # Followed by "test-client" (11 bytes)
      let encodedClientId = receivedFrame[10 ..< 10 + 11]
      for i, c in clientId:
        check encodedClientId[i] == byte(c)
      # After clientId: tagged_fields byte 0x00
      check receivedFrame[10 + 11] == 0x00'u8
      # Then the body
      check receivedFrame[^2] == 0xAA'u8
      check receivedFrame[^1] == 0xBB'u8

      await conn.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "sendRequest with flexible=false uses standard string encoding for clientId":
    proc testBody() {.async: (raises: [Exception]).} =
      var receivedFrame: seq[byte]
      proc captureAndReplyCallback(
          server: StreamServer, transport: StreamTransport
      ) {.async: (raises: []).} =
        try:
          var sizeBuf: array[4, byte]
          await transport.readExactly(addr sizeBuf[0], 4)
          let reqSize = int(fromBytesBE(uint32, sizeBuf))
          receivedFrame = newSeq[byte](reqSize)
          await transport.readExactly(addr receivedFrame[0], reqSize)

          let corrId = fromBytesBE(
            uint32,
            [receivedFrame[4], receivedFrame[5], receivedFrame[6], receivedFrame[7]],
          )

          # Non-flexible response: just corrId
          var resp: seq[byte]
          let corrBytes = toBytesBE(corrId)
          resp.add(corrBytes)

          var frameSize = toBytesBE(uint32(resp.len))
          discard await transport.write(addr frameSize[0], 4)
          discard await transport.write(addr resp[0], resp.len)
        except TransportError, CancelledError:
          discard
        await transport.closeWait()

      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, captureAndReplyCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let clientId = "test-client" # 11 bytes
      let conn = await connection.connect(boundAddr, clientId)
      let body = @[0xAA'u8, 0xBB'u8]
      discard await conn.sendRequest(akApiVersions, 3'i16, body, flexible = false)

      # Non-flexible: clientId is NULLABLE_STRING with int16 length prefix
      # At offset 8: 0x00 0x0B (int16 = 11), then 11 bytes
      check receivedFrame.len > 8
      check receivedFrame[8] == 0x00'u8
      check receivedFrame[9] == 0x0B'u8

      await conn.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

suite "Connection: InFlightHandle pins its connection":
  test "writeRequest binds handle.conn and awaitResponse resolves on it":
    ## The pipelined write/await split must carry the originating connection in the
    ## handle so the await never re-reads a broker field that a concurrent reconnect
    ## or force-prune could swap or nil.
    proc testBody() {.async: (raises: [Exception]).} =
      proc echoCorrId(
          server: StreamServer, transport: StreamTransport
      ) {.async: (raises: []).} =
        try:
          var sizeBuf: array[4, byte]
          await transport.readExactly(addr sizeBuf[0], 4)
          let reqSize = int(fromBytesBE(uint32, sizeBuf))
          var frame = newSeq[byte](reqSize)
          await transport.readExactly(addr frame[0], reqSize)
          let corrId = fromBytesBE(uint32, [frame[4], frame[5], frame[6], frame[7]])
          # Non-flexible response: corrId only.
          var resp: seq[byte]
          resp.add(toBytesBE(corrId))
          var frameSize = toBytesBE(uint32(resp.len))
          discard await transport.write(addr frameSize[0], 4)
          discard await transport.write(addr resp[0], resp.len)
        except TransportError, CancelledError:
          discard
        await transport.closeWait()

      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, echoCorrId, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let conn = await connection.connect(boundAddr, "test-client")
      let handle =
        await conn.writeRequest(akApiVersions, 3'i16, @[0xAA'u8], flexible = false)
      # The handle pins the exact connection it was written on.
      check handle.conn == conn
      # And the matching await resolves against that pinned connection.
      discard await conn.awaitResponse(handle)

      await conn.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

suite "Connection: sendRequest on closed connection":
  test "sendRequest on closed connection raises KafkaConnectionError":
    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, dummyCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let conn = await connection.connect(boundAddr, "test-client")
      await conn.close()

      var raised = false
      try:
        discard await conn.sendRequest(akApiVersions, 0'i16, @[])
      except KafkaConnectionError:
        raised = true
      check raised == true

      server.stop()
      await server.closeWait()

    waitFor testBody()

suite "Connection: readLoop exit handling":
  test "sendRequest fails promptly when server closes connection after write":
    proc testBody() {.async: (raises: [Exception]).} =
      ## When the server closes its end after receiving a request, the readLoop
      ## should set closed=true and the pending future should fail with a
      ## connection error rather than hanging until timeout.
      proc closeAfterReadCallback(
          server: StreamServer, transport: StreamTransport
      ) {.async: (raises: []).} =
        try:
          var sizeBuf: array[4, byte]
          await transport.readExactly(addr sizeBuf[0], 4)
          let reqSize = int(fromBytesBE(uint32, sizeBuf))
          var reqBuf = newSeq[byte](reqSize)
          await transport.readExactly(addr reqBuf[0], reqSize)
          # Don't send a response — just close.
        except TransportError, CancelledError:
          discard
        await transport.closeWait()

      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, closeAfterReadCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      # Use a long timeout so the test doesn't pass due to timeout
      let conn =
        await connection.connect(boundAddr, "test-client", requestTimeoutMs = 30000)

      var raised = false
      try:
        discard await conn.sendRequest(akApiVersions, 0'i16, @[])
      except KafkaConnectionError:
        raised = true

      check raised == true
      check conn.closed == true

      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "close() after readLoop sets closed still disposes resources":
    ## A previous implementation early-returned from close() when `closed==true`, so
    ## if readLoop flipped the flag on a socket error before close() ran,
    ## reader/writer/transport leaked. The `disposed` flag distinguishes "logically
    ## unusable" from "physically torn down".
    proc closeAfterReadCallback(
        server: StreamServer, transport: StreamTransport
    ) {.async: (raises: []).} =
      try:
        var sizeBuf: array[4, byte]
        await transport.readExactly(addr sizeBuf[0], 4)
        let reqSize = int(fromBytesBE(uint32, sizeBuf))
        var reqBuf = newSeq[byte](reqSize)
        await transport.readExactly(addr reqBuf[0], reqSize)
      except TransportError, CancelledError:
        discard
      await transport.closeWait()

    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server =
        createStreamServer(address, closeAfterReadCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let conn =
        await connection.connect(boundAddr, "test-client", requestTimeoutMs = 30000)
      check conn.disposed == false

      # Drive readLoop into an error path so it sets closed=true.
      try:
        discard await conn.sendRequest(akApiVersions, 0'i16, @[])
      except KafkaConnectionError:
        discard
      check conn.closed == true
      check conn.disposed == false

      # Explicit close must still tear down resources even though
      # `closed` was already set by readLoop.
      await conn.close()
      check conn.disposed == true

      # A second close must be a no-op (disposed guards re-entry).
      await conn.close()
      check conn.disposed == true

      server.stop()
      await server.closeWait()

    waitFor testBody()

proc serveApiVersions(fb: FakeBroker): Future[void] {.async: (raises: []).} =
  ## Answers the ApiVersions probe `broker.connect` sends. Without it the fake
  ## stays silent and the connect times out instead of reporting a connected
  ## broker.
  try:
    let req = await fb.nextRequest(timeout = 5.seconds)
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

suite "Broker: advertised host names are resolved":
  test "a host name connects once it resolves":
    proc testBody() {.async: (raises: [Exception]).} =
      # Bind where `localhost` resolves to, so the test does not depend on
      # whether getaddrinfo prefers the IPv4 or the IPv6 answer.
      let fb = newFakeBroker(resolveTAddress("localhost", Port(0))[0])
      defer:
        await fb.close()

      let port = int32(fb.address().port)
      let serving = fb.serveApiVersions()
      let broker = newBroker(-1, "localhost", port, "test-client")
      await broker.connect()
      check broker.isConnected
      await broker.close()
      await serving

    waitFor testBody()

  test "an unresolvable host name raises KafkaConnectionError":
    proc testBody() {.async: (raises: [Exception]).} =
      # The resolver is stubbed: the real one would send a DNS query (a
      # blocking one) and the wording of its failure differs per platform.
      let broker = newBroker(-1, "async-kafka.invalid", 9092, "test-client")
      armDnsResolverForTest(
        broker,
        proc(
            host: string, port: Port
        ): seq[TransportAddress] {.gcsafe, raises: [TransportAddressError].} =
          raise newException(TransportAddressError, "Name or service not known"),
      )
      defer:
        clearDnsResolverForTest(broker)

      var caught = false
      var caughtMsg = ""
      try:
        await broker.connect()
      except KafkaConnectionError as e:
        caught = true
        caughtMsg = e.msg
      check caught
      # A resolution failure is not a malformed address, and it must name the
      # host it could not resolve.
      check "cannot resolve" in caughtMsg
      check "async-kafka.invalid:9092" in caughtMsg
      check "Name or service not known" in caughtMsg
      check (not broker.isConnected)
      # Recorded so the reconnect backoff grows: retrying a dead resolver at
      # full speed would re-enter the blocking lookup immediately.
      check broker.failuresForTest == 1

    waitFor testBody()

  test "an empty resolver answer raises and is recorded as a failure":
    proc testBody() {.async: (raises: [Exception]).} =
      let broker = newBroker(-1, "empty-answer.invalid", 9092, "test-client")
      armDnsResolverForTest(
        broker,
        proc(
            host: string, port: Port
        ): seq[TransportAddress] {.gcsafe, raises: [TransportAddressError].} =
          @[],
      )
      defer:
        clearDnsResolverForTest(broker)

      var caught = false
      var caughtMsg = ""
      try:
        await broker.connect()
      except KafkaConnectionError as e:
        caught = true
        caughtMsg = e.msg
      check caught
      check "cannot resolve" in caughtMsg
      check broker.failuresForTest == 1

    waitFor testBody()

  test "the other answers are tried when the first is unreachable":
    proc testBody() {.async: (raises: [Exception]).} =
      # Bind then release a port so the first answer is a refused connection.
      let probe = createStreamServer(
        initTAddress("127.0.0.1:0"), dummyCallback, flags = {ReuseAddr}
      )
      probe.start()
      let dead = probe.localAddress()
      probe.stop()
      await probe.closeWait()

      let fb = newFakeBroker()
      defer:
        await fb.close()
      let live = initTAddress("127.0.0.1", int(fb.address().port))
      let serving = fb.serveApiVersions()

      let broker = newBroker(
        -1, "multi-answer.invalid", 9092, "test-client", connectTimeoutMs = 3000
      )
      armDnsResolverForTest(
        broker,
        proc(
            host: string, port: Port
        ): seq[TransportAddress] {.gcsafe, raises: [TransportAddressError].} =
          @[dead, live],
      )
      defer:
        clearDnsResolverForTest(broker)

      await broker.connect()
      check broker.isConnected
      # The live answer succeeding means no failure was recorded for the dead
      # one: a partially reachable name is not a reconnect failure.
      check broker.failuresForTest == 0
      await broker.close()
      await serving

    waitFor testBody()

  test "an out-of-range port is refused before it reaches the resolver":
    proc testBody() {.async: (raises: [Exception]).} =
      # `Port(70000'i32)` truncates to 4464 instead of raising, so a broker
      # advertising this port would be dialed at a different one.
      for port in [70000'i32, -1'i32, 0'i32]:
        let broker = newBroker(-1, "broken-port.invalid", port, "test-client")
        # A resolver that is reached would surface as "cannot resolve"
        # instead, so the message alone pins the ordering.
        armDnsResolverForTest(
          broker,
          proc(
              host: string, port: Port
          ): seq[TransportAddress] {.gcsafe, raises: [TransportAddressError].} =
            raise newException(TransportAddressError, "resolver was reached"),
        )
        defer:
          clearDnsResolverForTest(broker)

        var caught = false
        var caughtMsg = ""
        try:
          await broker.connect()
        except KafkaConnectionError as e:
          caught = true
          caughtMsg = e.msg
        check caught
        check "port out of range" in caughtMsg
        check not broker.isConnected
        check broker.failuresForTest == 0

    waitFor testBody()

var gAllowlistProbeAccepts {.global.}: int

proc countAcceptCallback(
    server: StreamServer, transport: StreamTransport
) {.async: (raises: []).} =
  ## Counts a connection the allowlist should never have opened.
  inc gAllowlistProbeAccepts
  try:
    while not transport.closed:
      await sleepAsync(milliseconds(50))
  except CancelledError:
    discard
  await transport.closeWait()

suite "Broker: allowlist bounds a resolved name":
  ## The allowlist used to be matched against the advertised string, which no
  ## host name can satisfy — so enabling it made every name-advertising broker
  ## unreachable, including the TLS case where a name is mandatory. The rules
  ## are now matched against the answers, before any socket opens.

  test "a name that resolves into the rules connects":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker(resolveTAddress("localhost", Port(0))[0])
      defer:
        await fb.close()

      let port = int32(fb.address().port)
      let serving = fb.serveApiVersions()
      let broker = newBroker(
        -1,
        "localhost",
        port,
        "test-client",
        hostAllowlist = newHostAllowlist(@[allowPrivateHosts()]),
      )
      await broker.connect()
      check broker.isConnected
      await broker.close()
      await serving

    waitFor testBody()

  test "a name whose answers are all denied is refused before any dial":
    proc testBody() {.async: (raises: [Exception]).} =
      gAllowlistProbeAccepts = 0
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, countAcceptCallback, flags = {ReuseAddr})
      server.start()
      defer:
        server.stop()
        await server.closeWait()
      let listening = initTAddress("127.0.0.1", int(server.localAddress().port))

      let broker = newBroker(
        -1,
        "denied.invalid",
        9092,
        "test-client",
        hostAllowlist = newHostAllowlist(@[allowCidr("10.0.0.0/8")]),
      )
      armDnsResolverForTest(
        broker,
        proc(
            host: string, port: Port
        ): seq[TransportAddress] {.gcsafe, raises: [TransportAddressError].} =
          @[listening],
      )
      defer:
        clearDnsResolverForTest(broker)

      var caught = false
      var caughtMsg = ""
      try:
        await broker.connect()
      except KafkaConnectionError as e:
        caught = true
        caughtMsg = e.msg
      check caught
      check "not permitted" in caughtMsg
      check not broker.isConnected
      # The listener is live, so a skipped gate would have shown up as an
      # accepted connection instead of this error.
      check gAllowlistProbeAccepts == 0
      # A policy refusal is not a transport failure: recording one would both
      # mislabel the metric and grow the backoff for a config problem.
      check broker.failuresForTest == 0

    waitFor testBody()

  test "a denied answer is skipped in favour of a permitted one":
    proc testBody() {.async: (raises: [Exception]).} =
      # 127.0.0.2 is loopback too, so the denied answer is dialable: the test
      # proves it was filtered, not that it happened to be unreachable.
      gAllowlistProbeAccepts = 0
      let deniedServer = createStreamServer(
        initTAddress("127.0.0.2:0"), countAcceptCallback, flags = {ReuseAddr}
      )
      deniedServer.start()
      defer:
        deniedServer.stop()
        await deniedServer.closeWait()
      let denied = initTAddress("127.0.0.2", int(deniedServer.localAddress().port))

      let fb = newFakeBroker()
      defer:
        await fb.close()
      let permitted = initTAddress("127.0.0.1", int(fb.address().port))
      let serving = fb.serveApiVersions()

      let broker = newBroker(
        -1,
        "mixed.invalid",
        9092,
        "test-client",
        hostAllowlist = newHostAllowlist(@[allowHost("127.0.0.1")]),
      )
      armDnsResolverForTest(
        broker,
        proc(
            host: string, port: Port
        ): seq[TransportAddress] {.gcsafe, raises: [TransportAddressError].} =
          @[denied, permitted],
      )
      defer:
        clearDnsResolverForTest(broker)

      await broker.connect()
      check broker.isConnected
      check gAllowlistProbeAccepts == 0
      # Skipping a denied answer is not a connection failure either.
      check broker.failuresForTest == 0
      await broker.close()
      await serving

    waitFor testBody()

suite "Broker: request after close raises KafkaConnectionError":
  test "metadata after close raises KafkaConnectionError":
    proc testBody() {.async: (raises: [Exception]).} =
      ## After closing a broker, any API call should raise KafkaConnectionError
      ## rather than crashing with a nil dereference.
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, dummyCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let b = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = int32(boundAddr.port),
        clientId = "test-client",
        requestTimeoutMs = 5000,
      )

      # Close without ever connecting — conn is nil
      await b.close()

      var raised = false
      try:
        discard await b.metadata(some(@["test-topic"]))
      except KafkaConnectionError:
        raised = true
      except KafkaError:
        # ensureConnected may raise a different KafkaError if connect fails;
        # either way, no nil dereference is the goal.
        raised = true
      check raised == true

      server.stop()
      await server.closeWait()

    waitFor testBody()

suite "Broker: CancelledError during connect cleans up connection":
  test "cancelling connect during handshake does not leak connection":
    proc testBody() {.async: (raises: [Exception]).} =
      ## A mock server accepts the TCP connection but never sends a response,
      ## so the broker gets stuck during API version negotiation.
      ## Cancelling the connect future should close the underlying connection.
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, dummyCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()

      let b = newBroker(
        nodeId = -1,
        host = "127.0.0.1",
        port = int32(boundAddr.port),
        clientId = "test-client",
        requestTimeoutMs = 30000,
      )

      let connectFut = b.connect()
      # Give enough time for TCP connect to succeed and negotiation to start
      await sleepAsync(milliseconds(200))

      check not connectFut.finished
      await connectFut.cancelAndWait()

      # After cancellation, the broker's connection must be cleaned up
      check not b.isConnected

      server.stop()
      await server.closeWait()

    waitFor testBody()

suite "Connection: InFlightWindow in-flight cap":
  test "third request blocks until a slot is released":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn = await connection.connect(
        fb.address(), "test-client", requestTimeoutMs = 10_000, maxInFlight = 2
      )
      check conn.maxInFlight == 2

      # Fire two requests but do not await them yet — they should fill the
      # in-flight window. The fake broker will sit on the captured requests.
      let f1 = conn.sendRequest(akApiVersions, 0'i16, @[])
      let f2 = conn.sendRequest(akApiVersions, 0'i16, @[])

      let r1 = await fb.nextRequest()
      let r2 = await fb.nextRequest()
      check conn.inFlightCount == 2

      # The third sendRequest should block on the semaphore. Give it time to
      # fail spuriously if the cap isn't being enforced.
      let f3 = conn.sendRequest(akApiVersions, 0'i16, @[])
      await sleepAsync(milliseconds(50))
      check not f3.finished
      # And the broker has not yet seen any third request.
      check fb.pendingRequests() == 0

      # Release the first slot — that should let the third request through.
      await r1.respond(@[0xAA'u8])
      let body1 = await f1
      check body1.bodyBytes == @[0xAA'u8]

      let r3 = await fb.nextRequest()
      check conn.inFlightCount == 2 # f2 still in flight, f3 just took the slot

      await r2.respond(@[0xBB'u8])
      await r3.respond(@[0xCC'u8])
      let body2 = await f2
      let body3 = await f3
      check body2.bodyBytes == @[0xBB'u8]
      check body3.bodyBytes == @[0xCC'u8]
      check conn.inFlightCount == 0

      await conn.close()
      await fb.close()

    waitFor testBody()

suite "Connection: InFlightWindow per-request timeout":
  test "one request times out while a sibling succeeds":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn = await connection.connect(
        fb.address(), "test-client", requestTimeoutMs = 200, maxInFlight = 4
      )

      let fSlow = conn.sendRequest(akApiVersions, 0'i16, @[])
      let fFast = conn.sendRequest(akApiVersions, 0'i16, @[])

      let rSlow = await fb.nextRequest()
      let rFast = await fb.nextRequest()
      await rFast.respond(@[0x99'u8])
      let bodyFast = await fFast
      check bodyFast.bodyBytes == @[0x99'u8]

      # Slow request never gets a response → must hit the per-request timeout.
      var raised = false
      try:
        discard await fSlow
      except KafkaTimeoutError:
        raised = true
      check raised

      # The slot must have been released after the timeout.
      check conn.inFlightCount == 0

      # And the connection must still be usable for fresh requests.
      let fProbe = conn.sendRequest(akApiVersions, 0'i16, @[])
      let rProbe = await fb.nextRequest()
      await rProbe.respond(@[0x55'u8])
      check (await fProbe).bodyBytes == @[0x55'u8]

      # Quiet the slow handler that's still alive on the fake broker side.
      discard rSlow

      await conn.close()
      await fb.close()

    waitFor testBody()

suite "Connection: InFlightWindow disconnect handling":
  test "all in-flight requests fail when transport closes":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn = await connection.connect(
        fb.address(), "test-client", requestTimeoutMs = 30_000, maxInFlight = 4
      )

      let f1 = conn.sendRequest(akApiVersions, 0'i16, @[])
      let f2 = conn.sendRequest(akApiVersions, 0'i16, @[])
      let f3 = conn.sendRequest(akApiVersions, 0'i16, @[])

      discard await fb.nextRequest()
      discard await fb.nextRequest()
      discard await fb.nextRequest()
      check conn.inFlightCount == 3

      await fb.closeAllTransports()

      var failed = 0
      for fut in [f1, f2, f3]:
        try:
          discard await fut
        except KafkaConnectionError:
          inc failed
      check failed == 3
      check conn.closed
      check conn.inFlightCount == 0

      var subsequentRaised = false
      try:
        discard await conn.sendRequest(akApiVersions, 0'i16, @[])
      except KafkaConnectionError:
        subsequentRaised = true
      check subsequentRaised

      await conn.close()
      await fb.close()

    waitFor testBody()

suite "Connection: InFlightWindow correlation id dispatch":
  test "a non-positive window size is rejected":
    expect KafkaConnectionError:
      discard newInFlightWindow(0)

  test "duplicate response for the same correlation id is dropped":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn =
        await connection.connect(fb.address(), "test-client", requestTimeoutMs = 5_000)

      let f = conn.sendRequest(akApiVersions, 0'i16, @[])
      let req = await fb.nextRequest()

      await req.respond(@[0x01'u8])
      let body = await f
      check body.bodyBytes == @[0x01'u8]

      # A second response with the same correlation id must be silently dropped —
      # the slot is already gone.
      await req.respondWithCorrelationId(req.correlationId, @[0x02'u8])
      await sleepAsync(milliseconds(50))
      check not conn.closed

      let f2 = conn.sendRequest(akApiVersions, 0'i16, @[])
      let r2 = await fb.nextRequest()
      await r2.respond(@[0x03'u8])
      check (await f2).bodyBytes == @[0x03'u8]

      await conn.close()
      await fb.close()

    waitFor testBody()

  test "malformed flexible response on one slot does not fail siblings":
    ## A single response with a malformed flexible header used to call failAll() on
    ## the whole connection, taking down every concurrent in-flight request. The new
    ## contract isolates the parse error to the affected slot only.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn =
        await connection.connect(fb.address(), "test-client", requestTimeoutMs = 5_000)

      # Slot Alpha is flexible and will receive a malformed tagged_fields header.
      let futAlpha = conn.sendRequest(akApiVersions, 0'i16, @[], flexible = true)
      let reqAlpha = await fb.nextRequest()
      # Slot Beta is non-flexible and must remain unaffected.
      let futBeta = conn.sendRequest(akApiVersions, 0'i16, @[], flexible = false)
      let reqBeta = await fb.nextRequest()

      # Build a malformed flexible response body: corrId is added by
      # respondWithCorrelationId; we only supply the post-corrId bytes here.
      # Six 0xFF bytes with no terminator: the frame ends mid-varint (the
      # decoder allows up to 10 bytes).
      var malformed: seq[byte]
      for _ in 0 ..< 6:
        malformed.add(0xFF'u8)
      await reqAlpha.respondWithCorrelationId(reqAlpha.correlationId, malformed)

      var alphaRaised = false
      var alphaMsg = ""
      try:
        discard await futAlpha
      except KafkaProtocolError as e:
        alphaRaised = true
        alphaMsg = e.msg
      check alphaRaised
      # The isolated failure carries correlation context for operators.
      check alphaMsg.contains("corrId=" & $reqAlpha.correlationId)
      check alphaMsg.contains($akApiVersions)

      # The connection must still be healthy; slot Beta should now resolve cleanly.
      check not conn.closed
      check not conn.disposed
      await reqBeta.respond(@[0x42'u8])
      check (await futBeta).bodyBytes == @[0x42'u8]

      await conn.close()
      await fb.close()

    waitFor testBody()

  test "response with unknown correlation id is dropped, real response still resolves":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn =
        await connection.connect(fb.address(), "test-client", requestTimeoutMs = 5_000)

      let f = conn.sendRequest(akApiVersions, 0'i16, @[])
      let req = await fb.nextRequest()

      # Send a stray frame for a correlation id we never asked about.
      await req.respondWithCorrelationId(0x7FFFFFFF'i32, @[0xFF'u8])
      await sleepAsync(milliseconds(20))
      check not f.finished
      check not conn.closed

      await req.respond(@[0x42'u8])
      check (await f).bodyBytes == @[0x42'u8]

      await conn.close()
      await fb.close()

    waitFor testBody()

  test "orphan responses increment the counter and fire onOrphan":
    ## A stray response for a cancelled / timed-out / unknown correlation id used to
    ## be silently dropped. The connection now bumps a public counter and fires
    ## onOrphan so the broker layer can emit a metric and decide whether to reconnect.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn =
        await connection.connect(fb.address(), "test-client", requestTimeoutMs = 5_000)

      var orphanIds: seq[int32]
      conn.setOnOrphan(
        proc(corrId: int32) {.closure, gcsafe, raises: [].} =
          orphanIds.add(corrId)
      )
      check conn.orphanResponseCount == 0

      let f = conn.sendRequest(akApiVersions, 0'i16, @[])
      let req = await fb.nextRequest()

      # Two stray frames; both must be counted.
      await req.respondWithCorrelationId(0x10001'i32, @[0xAA'u8])
      await req.respondWithCorrelationId(0x10002'i32, @[0xBB'u8])
      await sleepAsync(milliseconds(20))
      check conn.orphanResponseCount == 2
      check orphanIds == @[0x10001'i32, 0x10002'i32]

      # The real response still arrives and the future resolves.
      await req.respond(@[0x42'u8])
      check (await f).bodyBytes == @[0x42'u8]
      # Real response is NOT an orphan; counter is unchanged.
      check conn.orphanResponseCount == 2

      await conn.close()
      await fb.close()

    waitFor testBody()

  test "semaphore double-release is counted and swallowed":
    let w = newInFlightWindow(1)
    check w.doubleReleaseCount == 0

    # assign without acquiring → semaphore is still at max; failAll will
    # attempt a release that exceeds the semaphore capacity.
    discard w.assign(false, akApiVersions)
    w.failAll("test teardown")

    check w.doubleReleaseCount == 1
    check w.inFlightCount == 0

suite "Connection: InFlightWindow cancellation safety":
  test "cancelled sendRequest releases its in-flight slot":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn = await connection.connect(
        fb.address(), "test-client", requestTimeoutMs = 30_000, maxInFlight = 1
      )

      let fStuck = conn.sendRequest(akApiVersions, 0'i16, @[])
      let rStuck = await fb.nextRequest()
      check conn.inFlightCount == 1

      # Cancel the in-flight request without ever responding to it.
      await fStuck.cancelAndWait()
      check conn.inFlightCount == 0

      # The slot must be available again, even though we never replied to the
      # original request — the next sendRequest should be admitted immediately.
      let fNext = conn.sendRequest(akApiVersions, 0'i16, @[])
      let rNext = await fb.nextRequest()
      await rNext.respond(@[0x77'u8])
      check (await fNext).bodyBytes == @[0x77'u8]

      discard rStuck

      await conn.close()
      await fb.close()

    waitFor testBody()

  test "cancelled queued acquire does not over-release the in-flight slot":
    ## A request cancelled while still waiting for an in-flight permit must not
    ## release a permit it never held: acquireInflight only releases when its acquire
    ## future actually completed, and a cancelled waiter is `cancelled()`, not
    ## `completed()`. A naive unconditional release would let more than maxInFlight run.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn = await connection.connect(
        fb.address(), "test-client", requestTimeoutMs = 30_000, maxInFlight = 1
      )

      # f1 takes the only slot and stays in flight (never answered).
      let f1 = conn.sendRequest(akApiVersions, 0'i16, @[])
      let r1 = await fb.nextRequest()
      check conn.inFlightCount == 1

      # f2 queues on the exhausted semaphore, then is cancelled while queued.
      let f2 = conn.sendRequest(akApiVersions, 0'i16, @[])
      await sleepAsync(50.milliseconds)
      check conn.inFlightCount == 1 # f2 is a waiter, not an assigned slot
      await f2.cancelAndWait()
      check conn.inFlightCount == 1

      # If the cancelled waiter had wrongly released a permit, f3 would be
      # admitted immediately (inFlightCount == 2). It must instead queue.
      let f3 = conn.sendRequest(akApiVersions, 0'i16, @[])
      await sleepAsync(50.milliseconds)
      check conn.inFlightCount == 1

      # Free f1's slot; the permit must pass to f3 (skipping the cancelled f2),
      # and f3 must complete normally.
      await r1.respond(@[0x01'u8])
      check (await f1).bodyBytes == @[0x01'u8]
      let r3 = await fb.nextRequest()
      await r3.respond(@[0x03'u8])
      check (await f3).bodyBytes == @[0x03'u8]

      await conn.close()
      await fb.close()

    waitFor testBody()

  test "cancelled sync inflight acquire does not leak the permit":
    ## Same cancel-after-take edge as writeLock, on the in-flight semaphore.
    ## A leaked permit with maxInFlight=1 deadlocks every later sendRequest.
    ## The queued-waiter test above cancels a `cancelled()` future and does
    ## not execute the `fut.completed()` self-release.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn = await connection.connect(
        fb.address(), "test-client", requestTimeoutMs = 30_000, maxInFlight = 1
      )
      armConnAcquirePark(conn)
      try:
        check conn.inflightAvailableForTest() == 1
        let f = conn.acquireInflightForTest()
        check conn.inflightAvailableForTest() == 0
        check not f.finished
        await f.cancelAndWait()
        check conn.inflightAvailableForTest() == 1

        let fNext = conn.sendRequest(akApiVersions, 0'i16, @[])
        let rNext = await fb.nextRequest().wait(2.seconds)
        await rNext.respond(@[0x77'u8])
        check (await fNext.wait(2.seconds)).bodyBytes == @[0x77'u8]
      finally:
        clearConnAcquirePark(conn)
        await conn.close()
        await fb.close()

    waitFor testBody()

suite "Connection: write lock cancellation safety":
  test "cancelled queued write does not wedge the write lock":
    ## A writeRequest cancelled while queued on writeLock (a concurrent write
    ## holds it) must not leak the lock: after the cancel the next write is
    ## admitted, and close() — which also blocks on writeLock — completes.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn =
        await connection.connect(fb.address(), "test-client", requestTimeoutMs = 30_000)

      # Stand in for an in-flight write that owns writeLock.
      await conn.acquireWriteLockForTest()

      # A second write queues on the lock, then is cancelled while queued.
      let fW = conn.sendRequest(akApiVersions, 0'i16, @[])
      await sleepAsync(50.milliseconds)
      check not fW.finished
      await fW.cancelAndWait()

      # Release the lock: the cancelled waiter must not have leaked it.
      conn.releaseWriteLockForTest()

      # A fresh write must be admitted and complete normally.
      let fNext = conn.sendRequest(akApiVersions, 0'i16, @[])
      let rNext = await fb.nextRequest()
      await rNext.respond(@[0x77'u8])
      check (await fNext).bodyBytes == @[0x77'u8]

      await conn.close()
      await fb.close()

    waitFor testBody()

  test "cancelled sync writeLock acquire does not leak the lock":
    ## chronos can take writeLock synchronously and return a completed future
    ## with no cancel callback. Cancelling at that await must self-release, or
    ## every later write / close hangs. The park hook yields after the take so
    ## the cancel is deterministic; the queued-waiter test above does not hit
    ## this branch.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn =
        await connection.connect(fb.address(), "test-client", requestTimeoutMs = 30_000)
      armConnAcquirePark(conn)
      try:
        check not conn.writeLockHeldForTest()
        let f = conn.acquireWriteLockGuardedForTest()
        check conn.writeLockHeldForTest()
        check not f.finished
        await f.cancelAndWait()
        check not conn.writeLockHeldForTest()

        let fNext = conn.sendRequest(akApiVersions, 0'i16, @[])
        let rNext = await fb.nextRequest().wait(2.seconds)
        await rNext.respond(@[0x77'u8])
        check (await fNext.wait(2.seconds)).bodyBytes == @[0x77'u8]
      finally:
        clearConnAcquirePark(conn)
        await conn.close()
        await fb.close()

    waitFor testBody()

suite "Connection: InFlightWindow correlation id allocation":
  test "fire-and-forget and slotted requests never collide":
    ## Fire-and-forget corrIds walk a disjoint negative range so a spurious
    ## broker frame for a past FAF id cannot alias into a slot a later slotted
    ## request now owns. All interleaved corrIds must still be distinct on the wire.
    proc testBody() {.async: (raises: [Exception]).} =
      var received: seq[int32] = @[]
      proc captureCallback(
          server: StreamServer, transport: StreamTransport
      ) {.async: (raises: []).} =
        try:
          while not transport.closed:
            var sizeBuf: array[4, byte]
            await transport.readExactly(addr sizeBuf[0], 4)
            let reqSize = int(fromBytesBE(uint32, sizeBuf))
            var frame = newSeq[byte](reqSize)
            await transport.readExactly(addr frame[0], reqSize)
            if frame.len >= 8:
              let corrId = cast[int32](fromBytesBE(
                uint32, [frame[4], frame[5], frame[6], frame[7]]
              ))
              received.add(corrId)
        except TransportError, CancelledError:
          discard
        await transport.closeWait()

      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, captureCallback, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()
      let conn = await connection.connect(boundAddr, "test-client", maxInFlight = 4)

      # Interleave fire-and-forget and slotted paths. Slotted requests will
      # never get a response from this bare capture server; that is fine —
      # we only care about the request-side corrIds that hit the wire.
      await conn.sendRequestFireAndForget(akProduce, 3'i16, @[0xAA'u8])
      let f1 = conn.sendRequest(akApiVersions, 0'i16, @[])
      await conn.sendRequestFireAndForget(akProduce, 3'i16, @[0xBB'u8])
      let f2 = conn.sendRequest(akApiVersions, 0'i16, @[])
      await conn.sendRequestFireAndForget(akProduce, 3'i16, @[0xCC'u8])

      await sleepAsync(milliseconds(100))
      check received.len == 5
      # Distinct corrIds across the whole interleave.
      for i in 0 ..< received.len:
        for j in i + 1 ..< received.len:
          check received[i] != received[j]

      # Cancel the two slotted futures so conn.close() doesn't wait on them.
      await f1.cancelAndWait()
      await f2.cancelAndWait()
      await conn.close()
      server.stop()
      await server.closeWait()

    waitFor testBody()

  test "consecutive requests do not collide and counter advances":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let conn = await connection.connect(
        fb.address(), "test-client", requestTimeoutMs = 5_000, maxInFlight = 4
      )

      var seen: seq[int32] = @[]
      for i in 0 ..< 8:
        let f = conn.sendRequest(akApiVersions, 0'i16, @[])
        let req = await fb.nextRequest()
        seen.add(req.correlationId)
        await req.respond(@[byte(i)])
        check (await f).bodyBytes == @[byte(i)]

      # Each captured correlation id must be unique within the run.
      for i in 0 ..< seen.len:
        for j in i + 1 ..< seen.len:
          check seen[i] != seen[j]

      await conn.close()
      await fb.close()

    waitFor testBody()

suite "Broker: reconnect backoff is consumed outside connectLock":
  ## Regression: connectImpl used to sleep the reconnect backoff while holding
  ## `connectLock`, so a second coroutine queued behind the first one's full backoff
  ## before attempting its own connect. Backoff is now consumed in the public entry
  ## points (`connect` / `ensureConnected`) before the lock, so parallel connects share it.

  test "two parallel connects share the same backoff window":
    proc body() {.async: (raises: [Exception]).} =
      # Port 1 is reliably refused — TCP connect fails fast.
      let b = newBroker(
        nodeId = 0'i32,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "backoff-parity-test",
        reconnectBackoffMs = 300'i32,
        connectTimeoutMs = 200'i32,
        requestTimeoutMs = 200'i32,
      )
      # Prime the broker: first connect raises and populates
      # consecutiveFailures + currentBackoff.
      try:
        await b.connect()
      except KafkaError:
        discard

      # Now two parallel connects. Each must wait at most one backoff
      # round + the failing TCP connect. If the lock were held across
      # the sleep, this would take ~2× the backoff. With the fix, both
      # run concurrently and total time is ~1× backoff + a small slop.
      let started = Moment.now()
      let a = b.connect()
      let c = b.connect()
      try:
        await a
      except KafkaError:
        discard
      try:
        await c
      except KafkaError:
        discard
      let elapsedMs = (Moment.now() - started).milliseconds
      # A refused dial skips connectTimeout, and the retry backoff is jittered:
      # 300ms doubled to 600ms ±25% = 450-750ms, so the 900ms bound holds.
      # Old behaviour: ~900ms or more (two backoffs back-to-back).
      check elapsedMs < 900

      await b.close()

    waitFor body()

suite "Broker: reconnect disposes the dropped connection":
  ## Regression: when a connection's readLoop dies it only flags `closed` + `failAll`;
  ## it never disposes the transport / streams. The next connect must tear down that
  ## stale closed connection before `connectTcp` overwrites `b.conn`, otherwise the
  ## old socket fd and chronos streams leak on every reconnect.

  test "a reconnect after a dropped connection disposes the stale conn":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      # Background responder: answer every ApiVersions handshake (initial
      # connect AND the reconnect below) so connect() can complete.
      proc serveApiVersions() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          if req.apiKey == int16(ord(akApiVersions)):
            let body = buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                )
              ],
            )
            try:
              await req.respond(body)
            except CatchableError:
              discard

      let serverFut = serveApiVersions()
      let b = newBroker(0'i32, "127.0.0.1", port, "reconnect-leak-test")

      await b.connect()
      check b.isConnected
      let oldConn = b.connForTest()
      check not oldConn.disposed

      # Broker slams the door: the client's readLoop dies and flags the
      # connection closed, but does NOT dispose it.
      await fb.closeAllTransports()
      var dropped = false
      for _ in 0 ..< 100:
        if oldConn.closed:
          dropped = true
          break
        await sleepAsync(milliseconds(10))
      check dropped
      check not b.isConnected
      # Leak precondition: readLoop flagged closed but left the transport live.
      check not oldConn.disposed

      # Reconnect. connectImpl must dispose the stale conn before opening a
      # fresh one.
      await b.connect()
      check b.isConnected
      check oldConn.disposed # the fix: stale conn torn down, not leaked
      check b.connForTest() != oldConn # a genuinely new connection

      await b.close()
      await fb.close()
      awaitDrain(serverFut, 1.seconds)

    waitFor body()

suite "Broker: close keeps a connection installed during its await":
  ## Regression: close() awaited `conn.close()` and only then executed
  ## `b.conn = nil`. A concurrent connect that established a new connection
  ## during that await had its reference wiped by the resuming close, leaving
  ## the new socket fd and readLoop fiber with no owner to close them.

  test "a connection installed while close() is suspended survives":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      proc serveApiVersions() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          if req.apiKey == int16(ord(akApiVersions)):
            let body = buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                )
              ],
            )
            try:
              await req.respond(body)
            except CatchableError:
              discard

      let serverFut = serveApiVersions()
      let b = newBroker(0'i32, "127.0.0.1", port, "close-ownership-test")

      await b.connect()
      check b.isConnected
      let oldConn = b.connForTest()

      # Park the teardown inside connection.close() by holding the write lock
      # it takes to serialize writer teardown.
      await oldConn.acquireWriteLockForTest()
      let closeFut = b.close()
      check oldConn.disposed

      # A concurrent caller installs a fresh connection while close() is still
      # suspended.
      await b.connect()
      check b.isConnected
      let newConn = b.connForTest()
      check newConn != oldConn
      check not newConn.disposed

      # Let the parked close resume. It owns only the connection it claimed;
      # the freshly installed one must remain the broker's connection.
      oldConn.releaseWriteLockForTest()
      await closeFut
      check b.connForTest() == newConn
      check b.isConnected
      check oldConn.disposed

      await b.close()
      await fb.close()
      awaitDrain(serverFut, 1.seconds)

    waitFor body()

suite "Broker host allowlist":
  ## A broker's advertised host becomes both the dial target and the name the
  ## certificate is verified against, so a metadata response can redirect the
  ## client anywhere the peer can reach and have that peer present a matching
  ## certificate. The allowlist bounds where a broker is allowed to send us.
  ##
  ## It is a cluster-level network policy, not a TLS setting: the gate runs for
  ## plaintext connections too, since a metadata redirect is exactly what TLS
  ## cannot bound on its own.
  ##
  ## Matching is on the parsed address, never the string: "::ffff:127.0.0.1" and
  ## "127.0.0.1" are the same host, and a string comparison lets the first
  ## spell a literal-IP allowlist that the second walks straight through.

  test "no allowlist permits everything; an explicitly empty one is rejected":
    # The default must not break existing deployments: no allowlist means "no
    # restriction", which is what every caller before this change got implicitly.
    check hostAllowed(nil, "10.0.0.5")
    check hostAllowed(nil, "203.0.113.7")
    check hostAllowed(nil, "::1")
    # An empty list matches no address, so it would deny every one: it cannot be
    # constructed at all. Omitting the allowlist is the way to say "no check".
    expect KafkaError:
      discard newHostAllowlist(@[])

  test "an exact rule admits only that address":
    let allow = newHostAllowlist(@[allowHost("10.0.0.5")])
    check allow.hostAllowed("10.0.0.5")
    check not allow.hostAllowed("10.0.0.6")
    check not allow.hostAllowed("203.0.113.7")

  test "an exact rule never admits an address of the other family":
    # An exact rule stores its family as the prefix length (32 for v4, 128 for
    # v6). Reading a candidate as the other family compares the rule's leading
    # bytes against an address they do not describe, which let `allowHost("::1")`
    # admit 0.0.0.0 — on Linux a loopback dial the operator never allowed, and
    # reachable from a hostile metadata response.
    let loopback6 = newHostAllowlist(@[allowHost("::1")])
    check loopback6.hostAllowed("::1")
    check not loopback6.hostAllowed("0.0.0.0")
    check not loopback6.hostAllowed("127.0.0.1")
    check not loopback6.addressAllowed(initTAddress("0.0.0.0:9092"))

    let broker6 = newHostAllowlist(@[allowHost("2001:db8::1")])
    check broker6.hostAllowed("2001:db8::1")
    check broker6.addressAllowed(initTAddress("[2001:db8::1]:9092"))
    # 32.1.13.184 is the first four bytes of 2001:db8::1 read as an IPv4 address.
    check not broker6.hostAllowed("32.1.13.184")
    check not broker6.hostAllowed("2001:db8::2")

    # The same read in the other direction: a genuine v6 address sharing the
    # rule's leading 32 bits is not the v4 host the rule names.
    let broker4 = newHostAllowlist(@[allowHost("10.0.0.5")])
    check broker4.hostAllowed("10.0.0.5")
    check not broker4.hostAllowed("0a00:5::")

  test "a CIDR rule admits its range and excludes the rest":
    let allow = newHostAllowlist(@[allowCidr("10.0.0.0/8")])
    check allow.hostAllowed("10.0.0.5")
    check allow.hostAllowed("10.255.255.254")
    check not allow.hostAllowed("11.0.0.5")
    check not allow.hostAllowed("9.255.255.255")

  test "a /0 prefix admits everything, and a partial byte is not rounded":
    # A /0 is the explicit form of "no restriction" if you want it recorded in
    # config rather than defaulted.
    let all = newHostAllowlist(@[allowCidr("0.0.0.0/0")])
    check all.hostAllowed("203.0.113.7")
    check all.hostAllowed("10.0.0.1")
    # /13 is one whole byte plus the top 5 bits of the second. Comparing whole
    # bytes only would admit 10.0.0.0/8 and let 10.16.0.0 through.
    let thirteen = newHostAllowlist(@[allowCidr("10.8.0.0/13")])
    check thirteen.hostAllowed("10.8.0.1")
    check thirteen.hostAllowed("10.15.255.255")
    check not thirteen.hostAllowed("10.16.0.0")
    check not thirteen.hostAllowed("10.7.255.255")

  test "IPv4-mapped IPv6 spellings do not walk through an IPv4 rule":
    # The reason matching parses: every one of these is 10.0.0.5 on the wire, so
    # only a parsed comparison admits them; a string comparison would reject them.
    let allow = newHostAllowlist(@[allowCidr("10.0.0.0/8")])
    for spelling in ["::ffff:10.0.0.5", "0:0:0:0:0:ffff:10.0.0.5", "::FFFF:10.0.0.5"]:
      check allow.hostAllowed(spelling)
    # And the reverse: an IPv6 deny must not be evaded by the IPv4 spelling.
    let v6only = newHostAllowlist(@[allowCidr("2001:db8::/32")])
    check not v6only.hostAllowed("::ffff:10.0.0.5")
    check not v6only.hostAllowed("10.0.0.5")

  test "a private-range shorthand covers exactly RFC1918 and loopback":
    let allow = newHostAllowlist(@[allowPrivateHosts()])
    check allow.hostAllowed("10.0.0.5")
    check allow.hostAllowed("172.16.0.1")
    check allow.hostAllowed("172.31.255.255")
    check allow.hostAllowed("192.168.1.1")
    check allow.hostAllowed("127.0.0.1")
    # The boundaries are the point: 172.15/172.32 are outside RFC1918.
    check not allow.hostAllowed("172.15.255.255")
    check not allow.hostAllowed("172.32.0.1")
    check not allow.hostAllowed("203.0.113.7")
    check not allow.hostAllowed("169.254.169.254") # link-local / cloud metadata
    check not allow.hostAllowed("8.8.8.8")

  test "rules combine as OR, and an unparseable host is denied":
    let allow = newHostAllowlist(@[allowHost("10.0.0.5"), allowCidr("192.168.0.0/16")])
    check allow.hostAllowed("10.0.0.5")
    check allow.hostAllowed("192.168.9.9")
    check not allow.hostAllowed("172.16.0.1")
    # A metadata response can carry any string at all. "not an address" must be
    # a denial, never a permissive default.
    check not allow.hostAllowed("")
    check not allow.hostAllowed("broker.internal")
    check not allow.hostAllowed("10.0.0.5.evil.com")

  test "the dial gate matches the address, not the advertised name":
    # `hostAllowed` is the string form: a name is not an address, so it never
    # satisfies a rule. The dial paths resolve first and use `addressAllowed`,
    # which is what makes a name-advertising broker reachable under a list.
    let allow = newHostAllowlist(@[allowCidr("10.0.0.0/8")])
    check hostAllowed(nil, "broker.internal")
    check addressAllowed(nil, initTAddress("10.0.0.5:9092"))
    check not allow.hostAllowed("broker.internal")
    check allow.addressAllowed(initTAddress("10.0.0.5:9092"))
    check not allow.addressAllowed(initTAddress("203.0.113.7:9092"))
    # An IPv4-mapped v6 answer is the same host as the v4 rule it encodes.
    check allow.addressAllowed(initTAddress("[::ffff:10.0.0.5]:9092"))
    let v6Only = newHostAllowlist(@[allowCidr("2001:db8::/32")])
    check v6Only.addressAllowed(initTAddress("[2001:db8::1]:9092"))
    check not v6Only.addressAllowed(initTAddress("[::ffff:10.0.0.5]:9092"))
    # Only IP transports can be a broker dial target; anything else is denied
    # rather than falling through to a permissive default.
    check not allow.addressAllowed(
      TransportAddress(family: AddressFamily.None, port: Port(9092))
    )

  test "a malformed rule is rejected at construction":
    # Silently ignoring a typo'd prefix would leave the operator believing a
    # narrow allowlist is in force when nothing is being checked.
    expect KafkaError:
      discard allowCidr("10.0.0.0/33")
    expect KafkaError:
      discard allowCidr("10.0.0.0")
    expect KafkaError:
      discard allowCidr("not-a-cidr")
    expect KafkaError:
      discard allowCidr("10.0.0.0/abc")
    expect KafkaError:
      discard allowCidr("10.0.0.0/-1")
    expect KafkaError:
      discard allowCidr("2001:db8::/129")
    expect KafkaError:
      discard allowHost("nonsense")
    # A well-formed rule at either edge is fine.
    discard allowCidr("0.0.0.0/0")
    discard allowCidr("10.0.0.0/32")
    discard allowCidr("::/0")
    discard newHostAllowlist(@[allowCidr("10.0.0.0/8")])

  test "a matching allowlist gets past the gate":
    # The other half of the refusal test, and the reason the gate is placed
    # before the socket work: with a matching rule the connect proceeds and
    # fails on its own merits (the dummy server speaks raw TCP, so the TLS
    # handshake times out), which is a DIFFERENT failure from the allowlist's.
    # Asserting the error is not the allowlist error is what pins "the gate let
    # it through" without standing up a TLS server the suite has no helper for.
    proc body() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, dummyCallback, flags = {ReuseAddr})
      server.start()
      defer:
        server.stop()
        await server.closeWait()
      let bound = server.localAddress()
      let allow = newHostAllowlist(@[allowCidr("127.0.0.0/8")])
      try:
        let conn = await connection.connect(
          initTAddress("127.0.0.1", int(bound.port)),
          "test-client",
          "127.0.0.1",
          newTlsConfig(),
          allow,
        )
        check false # dummy server holds the socket, so this is unreachable
        await conn.close()
      except KafkaConnectionError as e:
        # Reaching here at all means the gate let the dial through; these two
        # pin that the failure was not the gate after all.
        check "not permitted" notin e.msg
        check "allowlist" notin e.msg

    waitFor body()

  test "a matching allowlist admits a plaintext connection":
    ## The point of the promotion: the gate is not part of the TLS branch, so a
    ## plaintext cluster with an allowlist is both checked and still usable.
    proc body() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, dummyCallback, flags = {ReuseAddr})
      server.start()
      defer:
        server.stop()
        await server.closeWait()
      let bound = server.localAddress()
      let allow = newHostAllowlist(@[allowCidr("127.0.0.0/8")])
      let conn = await connection.connect(
        initTAddress("127.0.0.1", int(bound.port)),
        "test-client",
        "127.0.0.1",
        hostAllowlist = allow,
      )
      check conn.closed == false
      await conn.close()

    waitFor body()

  test "a host outside the allowlist is refused before any socket is opened":
    ## A metadata response naming an address the operator did not sanction must
    ## not become a dial — with or without TLS. The server here IS listening, so
    ## a skipped gate would show up as a successful connect instead of the
    ## allowlist error.
    proc body() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, dummyCallback, flags = {ReuseAddr})
      server.start()
      defer:
        server.stop()
        await server.closeWait()
      let bound = server.localAddress()
      let allow = newHostAllowlist(@[allowCidr("10.0.0.0/8")])
      for tlsConfig in [TlsConfig(nil), newTlsConfig()]:
        try:
          let conn = await connection.connect(
            initTAddress("127.0.0.1", int(bound.port)),
            "test-client",
            "127.0.0.1",
            tlsConfig,
            allow,
          )
          check false # a dial that should not have been attempted
          await conn.close()
        except KafkaConnectionError as e:
          check "not permitted" in e.msg
          check "127.0.0.1" in e.msg

    waitFor body()
