import std/[unittest, os, strutils]
import std/logging as stdLogging

import chronos

import async_kafka/errors
import async_kafka/async_backend
import
  async_kafka/internal/[backoff, cancel, metrics, coordinator_rpc, retry, safe_logging]

const projectRoot = currentSourcePath.parentDir.parentDir

type ThrowingLogger = ref object of stdLogging.Logger
  calls: int

method log(
    logger: ThrowingLogger, level: stdLogging.Level, args: varargs[string, `$`]
) {.gcsafe.} =
  inc logger.calls
  raise newException(IOError, "intentional logger failure")

suite "safe logging":
  test "catchable handler failures do not escape diagnostic boundaries":
    let logger = ThrowingLogger()
    let previousLogFilter = stdLogging.getLogFilter()
    stdLogging.setLogFilter(stdLogging.lvlAll)
    stdLogging.addHandler(logger)
    try:
      safe_logging.debug "debug"
      safe_logging.info "info"
      safe_logging.warn "warn"
      safe_logging.error "error"
      check logger.calls == 4
    finally:
      stdLogging.removeHandler(logger)
      stdLogging.setLogFilter(previousLogFilter)

  test "production sources do not suppress raises checks":
    let forbidden = "cast(raises:"
    var sourcePaths: seq[string]
    for pattern in ["*.nim", "*/*.nim"]:
      for sourcePath in walkFiles(projectRoot / "async_kafka" / pattern):
        sourcePaths.add(sourcePath)
    check sourcePaths.len >= 30
    for sourcePath in sourcePaths:
      let source = readFile(sourcePath)
      var normalized = source
      for whitespace in [' ', '\t', '\r', '\n']:
        normalized = normalized.replace($whitespace, "")
      check forbidden notin normalized

suite "jitteredBackoff":
  test "is at least 1ms even for very small base":
    for attempt in 1 .. 5:
      check jitteredBackoff(1'i32, attempt) >= 1'i32

  test "exponential growth caps at 2^maxShift":
    # base 100, maxShift 5 → max slot is 100 * 32 = 3200; ±25% jitter has
    # amplitude exp/4 = ±800, so the maximum returned value is 3200 + 800 = 4000.
    var observedMax = 0'i32
    for _ in 1 .. 200:
      let v = jitteredBackoff(100'i32, attempt = 100, maxShift = 5)
      if v > observedMax:
        observedMax = v
    check observedMax <= 4800'i32
    check observedMax >= 1600'i32

  test "jitter produces variation across calls":
    var first = jitteredBackoff(100'i32, 3)
    var sawDifferent = false
    for _ in 1 .. 20:
      if jitteredBackoff(100'i32, 3) != first:
        sawDifferent = true
        break
    check sawDifferent

  test "attempt=0 maps to slot index 0 (no shift)":
    # Slot 0: base ± 25% → between 75 and 125 (jitter amplitude base/4 = 25),
    # never below the 1ms floor.
    for _ in 1 .. 50:
      let v = jitteredBackoff(100'i32, 0)
      check v >= 1'i32
      check v <= 200'i32 # 100 + 50 jitter range / 2 = 125 + safety

  test "a large base does not overflow int32 (clamped, no RangeDefect)":
    # base * 2^shift can exceed int32.high; the result must be clamped into
    # [1, int32.high] rather than raising RangeDefect or wrapping negative.
    for attempt in 0 .. 8:
      let v = jitteredBackoff(int32.high, attempt)
      check v >= 1'i32
      check v <= int32.high
    # Even a mid-range base whose 2^maxShift product overflows is bounded.
    let big = jitteredBackoff(200_000_000'i32, attempt = 100, maxShift = 5)
    check big >= 1'i32
    check big <= int32.high

suite "MetricsHook":
  test "emitMetric is no-op when hook is nil":
    var hook: MetricsHook = nil
    emitMetric(hook, meProducerSendOk, [("k", "v")], 1)

  test "emitMetric forwards to hook":
    # Use a ref-typed closure environment so gcsafe is preserved when the
    # hook mutates state. `test` is a block, not a proc, so a naked `let`
    # would be a global; the nested runTest() proc keeps `cap` local instead.
    type Capture = ref object
      events: seq[(string, seq[(string, string)], int64)]

    proc runTest() =
      let cap = Capture()
      let hook: MetricsHook = proc(
          event: cstring, tags: openArray[(cstring, cstring)], value: int64
      ) {.gcsafe, raises: [].} =
        var tagsCopy: seq[(string, string)]
        for (k, v) in tags:
          tagsCopy.add(($k, $v))
        cap.events.add(($event, tagsCopy, value))

      emitMetric(hook, meProducerSendOk, [("topic", "orders")], 5)
      emitMetric(
        hook, meProducerSendError, [("topic", "orders"), ("error", "timeout")], 1
      )

      check cap.events.len == 2
      check cap.events[0][0] == "producer.send.ok"
      check cap.events[0][1].len == 1
      check cap.events[0][1][0] == ("topic", "orders")
      check cap.events[0][2] == 5
      check cap.events[1][0] == "producer.send.error"
      check cap.events[1][1].len == 2

    runTest()

type
  FakeCoordResp = object
    errorCode: KafkaErrorCode
    value: int

  FakeRaise = enum
    ## What `nextCoordResponse` raises for a given attempt index, to exercise
    ## the driver's exception path per transport-error kind.
    frNone
    frConnection
    frTimeout
    frProtocol
    frPlainError ## a non-transport KafkaError — must propagate, never retried

  CoordHarness = ref object
    script: seq[KafkaErrorCode]
    raiseKind: seq[FakeRaise]
    callCount: int
    rediscoverCount: int

proc fakeErrorCodeOf(r: FakeCoordResp): KafkaErrorCode =
  r.errorCode

proc nextCoordResponse(
    h: CoordHarness
): Future[FakeCoordResp] {.async: (raises: [KafkaError]).} =
  let idx = h.callCount
  inc h.callCount
  if idx < h.raiseKind.len:
    case h.raiseKind[idx]
    of frConnection:
      raise newException(KafkaConnectionError, "transport dropped")
    of frTimeout:
      raise newException(KafkaTimeoutError, "request timed out")
    of frProtocol:
      raise newException(KafkaProtocolError, "malformed frame")
    of frPlainError:
      raise newException(KafkaError, "non-transport failure")
    of frNone:
      discard
  let code =
    if idx < h.script.len:
      h.script[idx]
    else:
      h.script[^1]
  FakeCoordResp(errorCode: code, value: idx)

suite "runCoordinatorRpc":
  test "success short-circuits on the first attempt":
    proc body() {.async: (raises: [Exception]).} =
      let ctx = RetryContext(isCoordinatorRpc: true, isTransactional: true)
      let h = CoordHarness(script: @[keNone], raiseKind: @[frNone])
      let resp = runCoordinatorRpc(
        ctx,
        5,
        1'i32,
        inc h.rediscoverCount,
        fakeErrorCodeOf,
        await nextCoordResponse(h),
      )
      check resp.errorCode == keNone
      check h.callCount == 1
      check h.rediscoverCount == 0

    waitFor body()

  test "NOT_COORDINATOR triggers rediscover then succeeds":
    proc body() {.async: (raises: [Exception]).} =
      let ctx = RetryContext(isCoordinatorRpc: true, isTransactional: true)
      let h =
        CoordHarness(script: @[keNotCoordinator, keNone], raiseKind: @[frNone, frNone])
      let resp = runCoordinatorRpc(
        ctx,
        5,
        1'i32,
        inc h.rediscoverCount,
        fakeErrorCodeOf,
        await nextCoordResponse(h),
      )
      check resp.errorCode == keNone
      check h.callCount == 2
      check h.rediscoverCount == 1

    waitFor body()

  test "COORDINATOR_LOAD_IN_PROGRESS retries the same coordinator (no rediscover)":
    proc body() {.async: (raises: [Exception]).} =
      let ctx = RetryContext(isCoordinatorRpc: true, isTransactional: true)
      let h = CoordHarness(
        script: @[keCoordinatorLoadInProgress, keNone], raiseKind: @[frNone, frNone]
      )
      let resp = runCoordinatorRpc(
        ctx,
        5,
        1'i32,
        inc h.rediscoverCount,
        fakeErrorCodeOf,
        await nextCoordResponse(h),
      )
      check resp.errorCode == keNone
      check h.callCount == 2
      # raRetrySame backs off and resends but must NOT re-resolve the coordinator.
      check h.rediscoverCount == 0

    waitFor body()

  test "terminal code surfaces the last response unchanged":
    proc body() {.async: (raises: [Exception]).} =
      let ctx = RetryContext(isCoordinatorRpc: true, isTransactional: true)
      let h = CoordHarness(script: @[keProducerFenced], raiseKind: @[frNone])
      let resp = runCoordinatorRpc(
        ctx,
        5,
        1'i32,
        inc h.rediscoverCount,
        fakeErrorCodeOf,
        await nextCoordResponse(h),
      )
      # The driver never raises an API error — it yields the terminal response so
      # the caller's own errorCode handling fires.
      check resp.errorCode == keProducerFenced
      check h.callCount == 1
      check h.rediscoverCount == 0

    waitFor body()

  test "exhausted budget yields the last retriable response":
    proc body() {.async: (raises: [Exception]).} =
      let ctx = RetryContext(isCoordinatorRpc: true, isTransactional: true)
      let h = CoordHarness(
        script: @[
          keCoordinatorLoadInProgress, keCoordinatorLoadInProgress,
          keCoordinatorLoadInProgress,
        ],
        raiseKind: @[frNone, frNone, frNone],
      )
      let resp = runCoordinatorRpc(
        ctx,
        3,
        1'i32,
        inc h.rediscoverCount,
        fakeErrorCodeOf,
        await nextCoordResponse(h),
      )
      check resp.errorCode == keCoordinatorLoadInProgress
      check h.callCount == 3
      check h.rediscoverCount == 0

    waitFor body()

  test "connection drop is treated as a move and rediscovered":
    proc body() {.async: (raises: [Exception]).} =
      let ctx = RetryContext(isCoordinatorRpc: true, isTransactional: true)
      let h =
        CoordHarness(script: @[keNone, keNone], raiseKind: @[frConnection, frNone])
      let resp = runCoordinatorRpc(
        ctx,
        5,
        1'i32,
        inc h.rediscoverCount,
        fakeErrorCodeOf,
        await nextCoordResponse(h),
      )
      check resp.errorCode == keNone
      check h.callCount == 2
      check h.rediscoverCount == 1

    waitFor body()

  test "connection drop past the budget is re-raised":
    proc body() {.async: (raises: [Exception]).} =
      let ctx = RetryContext(isCoordinatorRpc: true, isTransactional: true)
      let h = CoordHarness(
        script: @[keNone, keNone, keNone],
        raiseKind: @[frConnection, frConnection, frConnection],
      )
      var raised = false
      try:
        discard runCoordinatorRpc(
          ctx,
          2,
          1'i32,
          inc h.rediscoverCount,
          fakeErrorCodeOf,
          await nextCoordResponse(h),
        )
      except KafkaConnectionError:
        raised = true
      check raised
      # attempt 1 drops → rediscover; attempt 2 drops with the budget spent → raise.
      check h.callCount == 2
      check h.rediscoverCount == 1

    waitFor body()

  test "request timeout is treated as a move and rediscovered":
    proc body() {.async: (raises: [Exception]).} =
      let ctx = RetryContext(isCoordinatorRpc: true, isTransactional: true)
      let h = CoordHarness(script: @[keNone, keNone], raiseKind: @[frTimeout, frNone])
      let resp = runCoordinatorRpc(
        ctx,
        5,
        1'i32,
        inc h.rediscoverCount,
        fakeErrorCodeOf,
        await nextCoordResponse(h),
      )
      check resp.errorCode == keNone
      check h.callCount == 2
      check h.rediscoverCount == 1

    waitFor body()

  test "protocol error is treated as a move and rediscovered":
    proc body() {.async: (raises: [Exception]).} =
      let ctx = RetryContext(isCoordinatorRpc: true, isTransactional: true)
      let h = CoordHarness(script: @[keNone, keNone], raiseKind: @[frProtocol, frNone])
      let resp = runCoordinatorRpc(
        ctx,
        5,
        1'i32,
        inc h.rediscoverCount,
        fakeErrorCodeOf,
        await nextCoordResponse(h),
      )
      check resp.errorCode == keNone
      check h.callCount == 2
      check h.rediscoverCount == 1

    waitFor body()

  test "timeout past the budget is re-raised":
    proc body() {.async: (raises: [Exception]).} =
      let ctx = RetryContext(isCoordinatorRpc: true, isTransactional: true)
      let h = CoordHarness(
        script: @[keNone, keNone, keNone], raiseKind: @[frTimeout, frTimeout]
      )
      var raised = false
      try:
        discard runCoordinatorRpc(
          ctx,
          2,
          1'i32,
          inc h.rediscoverCount,
          fakeErrorCodeOf,
          await nextCoordResponse(h),
        )
      except KafkaTimeoutError:
        raised = true
      check raised
      check h.callCount == 2
      check h.rediscoverCount == 1

    waitFor body()

  test "non-transport KafkaError propagates without retry or rediscover":
    proc body() {.async: (raises: [Exception]).} =
      let ctx = RetryContext(isCoordinatorRpc: true, isTransactional: true)
      let h =
        CoordHarness(script: @[keNone, keNone], raiseKind: @[frPlainError, frNone])
      var raised = false
      try:
        discard runCoordinatorRpc(
          ctx,
          5,
          1'i32,
          inc h.rediscoverCount,
          fakeErrorCodeOf,
          await nextCoordResponse(h),
        )
      except KafkaError:
        raised = true
      check raised
      # No retry: a non-transport failure (e.g. KafkaClosedError) is not a
      # coordinator move and must surface immediately.
      check h.callCount == 1
      check h.rediscoverCount == 0

    waitFor body()

  test "unsupported RetryAction is treated as terminal, not backoff-retried":
    proc body() {.async: (raises: [Exception]).} =
      # A RetryContext that enables a recovery this driver does not own
      # (isFetchPartition → raResetOffset). A misconfigured caller must not get
      # a silent retry loop on an action the driver cannot service: yield the
      # last response so the caller's errorCode handling fires.
      let ctx = RetryContext(isCoordinatorRpc: true, isFetchPartition: true)
      let h = CoordHarness(script: @[keOffsetOutOfRange], raiseKind: @[frNone])
      let resp = runCoordinatorRpc(
        ctx,
        5,
        1'i32,
        inc h.rediscoverCount,
        fakeErrorCodeOf,
        await nextCoordResponse(h),
      )
      check resp.errorCode == keOffsetOutOfRange
      check h.callCount == 1
      check h.rediscoverCount == 0

    waitFor body()

type RecordingLogger = ref object of stdLogging.Logger
  messages: seq[string]

method log(
    logger: RecordingLogger, level: stdLogging.Level, args: varargs[string, `$`]
) {.gcsafe.} =
  ## Captures what a handler would print, so a test can assert that a diagnostic
  ## actually reaches the application's handler with the identifier in it.
  logger.messages.add(args.join(""))

suite "absorbDuringTeardown":
  test "an absorbed failure reaches the handler with its site and reason":
    ## The cancel-policy gate proves every swallowed teardown failure routes
    ## through this entry point, but a template that dropped its body would
    ## keep that gate green. Assert the emitted line carries both constants.
    let logger = RecordingLogger()
    let previousLogFilter = stdLogging.getLogFilter()
    stdLogging.setLogFilter(stdLogging.lvlAll)
    stdLogging.addHandler(logger)
    try:
      absorbDuringTeardown("tests.internal.cancel", "intentional teardown failure")
      check logger.messages.len == 1
      check "tests.internal.cancel" in logger.messages[0]
      check "intentional teardown failure" in logger.messages[0]

      absorbDuringTeardown(
        "tests.internal.cancel.detail", "intentional teardown failure", "detail-42"
      )
      check logger.messages.len == 2
      check "tests.internal.cancel.detail" in logger.messages[1]
      check "detail-42" in logger.messages[1]
    finally:
      stdLogging.removeHandler(logger)
      stdLogging.setLogFilter(previousLogFilter)

suite "releaseSafely":
  test "AsyncLock release without a matching acquire is swallowed":
    let lock = newAsyncLock()
    # A bare release() raises AsyncLockError; releaseSafely must swallow it so it
    # is safe from defer/finally cleanup that must not raise.
    var bareRaised = false
    try:
      lock.release()
    except AsyncLockError:
      bareRaised = true
    check bareRaised
    releaseSafely(lock, "tests.internal.unpaired-lock")

  test "AsyncLock release after acquire frees the lock":
    proc body() {.async: (raises: [Exception]).} =
      let lock = newAsyncLock()
      await lock.acquire()
      releaseSafely(lock, "tests.internal.paired-lock")
      # The release landed: a fresh acquire completes instead of blocking.
      await lock.acquire()
      lock.release()

    waitFor body()

  test "AsyncSemaphore release without a matching acquire is swallowed":
    let sem = newAsyncSemaphore(1)
    var bareRaised = false
    try:
      sem.release()
    except AsyncSemaphoreError:
      bareRaised = true
    check bareRaised
    releaseSafely(sem, "tests.internal.unpaired-sem")

  test "an unmatched release is reported with its call site":
    ## The warn used to be anonymous, so an operator could not tell which of the
    ## ~30 release sites broke its acquire/release pairing.
    let logger = RecordingLogger()
    let previousLogFilter = stdLogging.getLogFilter()
    stdLogging.setLogFilter(stdLogging.lvlAll)
    stdLogging.addHandler(logger)
    try:
      releaseSafely(newAsyncLock(), "tests.internal.unmatched")
      check logger.messages.len == 1
      check "without matching acquire" in logger.messages[0]
      check "tests.internal.unmatched" in logger.messages[0]

      releaseSafely(newAsyncSemaphore(1), "tests.internal.unmatched-sem")
      check logger.messages.len == 2
      check "tests.internal.unmatched-sem" in logger.messages[1]
    finally:
      stdLogging.removeHandler(logger)
      stdLogging.setLogFilter(previousLogFilter)

  test "AsyncSemaphore release after acquire returns the permit":
    proc body() {.async: (raises: [Exception]).} =
      let sem = newAsyncSemaphore(1)
      await sem.acquire()
      releaseSafely(sem, "tests.internal.paired-sem")
      # The permit was returned: the next acquire completes instead of blocking.
      await sem.acquire()
      sem.release()

    waitFor body()
