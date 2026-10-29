## Producer module — lifecycle: the two `newKafkaProducer` constructors, the
## `isClosed` / `state` / `ownsCluster` accessors, and the staged `close`.

import std/[tables, options, sets, importutils]

import ../[async_backend, errors, cluster, connection, sasl]
import ../protocol/[messages, compression]
import ../internal/[cancel, metrics, safe_logging]

import types, transaction, batch

privateAccess(KafkaProducer)

proc validateProducerConfig*(
    lingerMs: int32,
    acks: int16,
    timeoutMs: int32,
    transactionTimeoutMs: int32,
    maxRetries: int32,
    maxBatchRecords: int32,
    maxBatchBytes: int32,
    retryBackoffMs: int32,
    maxPendingFireAndForget: int32,
    maxPendingRecords: int32,
    maxInFlightRequests: int32,
    transactionalId: Option[string],
) {.raises: [KafkaError].} =
  ## Raise on any config the constructors reject. Shared so both `newKafkaProducer`
  ## overloads validate IDENTICALLY, and so the bootstrap-broker overload can
  ## validate BEFORE creating its own Cluster — a rejection must not leak an
  ## already-constructed Cluster.
  if lingerMs < 0:
    raise newException(KafkaError, "lingerMs must be >= 0, got " & $lingerMs)
  if acks notin {-1'i16, 0'i16, 1'i16}:
    raise newException(
      KafkaError,
      "acks must be one of {-1, 0, 1} (0=none, 1=leader, -1=all), got " & $acks,
    )
  if transactionTimeoutMs <= 0:
    raise newException(
      KafkaError, "transactionTimeoutMs must be > 0, got " & $transactionTimeoutMs
    )
  if maxRetries < 0:
    raise newException(KafkaError, "maxRetries must be >= 0, got " & $maxRetries)
  if maxBatchRecords <= 0:
    raise
      newException(KafkaError, "maxBatchRecords must be > 0, got " & $maxBatchRecords)
  if maxBatchBytes <= 0:
    raise newException(KafkaError, "maxBatchBytes must be > 0, got " & $maxBatchBytes)
  if retryBackoffMs < 0:
    raise
      newException(KafkaError, "retryBackoffMs must be >= 0, got " & $retryBackoffMs)
  if timeoutMs <= 0:
    raise newException(KafkaError, "timeoutMs must be > 0, got " & $timeoutMs)
  if maxPendingFireAndForget <= 0:
    raise newException(
      KafkaError, "maxPendingFireAndForget must be > 0, got " & $maxPendingFireAndForget
    )
  if maxPendingRecords <= 0:
    raise newException(
      KafkaError, "maxPendingRecords must be > 0, got " & $maxPendingRecords
    )
  if maxInFlightRequests <= 0:
    # chronos rejects a zero-permit semaphore (AssertionDefect), and negatives are
    # nonsensical.
    raise newException(
      KafkaError, "maxInFlightRequests must be > 0, got " & $maxInFlightRequests
    )
  # Kafka accepts 1..255 chars of [a-zA-Z0-9._-]; the broker rejects an invalid id
  # only at the first InitProducerId round-trip, long after the misconfiguration.
  if transactionalId.isSome:
    let id = transactionalId.get()
    if id.len == 0 or id.len > 255:
      raise newException(
        KafkaError, "transactionalId must be 1..255 characters, got " & $id.len
      )
    for c in id:
      if c notin {'a' .. 'z', 'A' .. 'Z', '0' .. '9', '.', '_', '-'}:
        raise newException(
          KafkaError,
          "transactionalId may only contain [a-zA-Z0-9._-], got '" & $c & "'",
        )

proc newKafkaProducer*(
    cluster: Cluster,
    acks: int16 = 1,
    timeoutMs: int32 = 30000,
    compression: CompressionCodec = ccNone,
    lingerMs: int32 = 0,
    maxBatchRecords: int32 = 1000,
    maxBatchBytes: int32 = 1_048_576, # 1 MiB, matches Kafka Java batch.size
    maxRetries: int32 = 3,
    deliveryTimeoutMs: int32 = 120_000,
    retryBackoffMs: int32 = 100,
    maxInFlightRequests: int32 = 5,
    enableIdempotence: bool = false,
    transactionalId: Option[string] = none(string),
    transactionTimeoutMs: int32 = 60000,
    errorHandler: ErrorHandler = nil,
    maxPendingFireAndForget: int32 = 1000,
    partitionStrategy: PartitionStrategy = psRoundRobin,
    customPartitioner: CustomPartitioner = nil,
    maxPendingRecords: int32 = 10_000,
    metricsHook: MetricsHook = nil,
): KafkaProducer {.raises: [KafkaError].} =
  ## Build a producer over `cluster`. `validateProducerConfig` checks the numeric and
  ## enum knobs eagerly (acks, timeouts, batch caps, retries, in-flight limit,
  ## transactional id); transactional mode then forces idempotence, `acks = -1`, and
  ## unbounded retries. The caller owns the producer via `close`.
  ##
  ## Does NOT own `cluster`: `close()` leaves it open (see `ownsCluster`).
  ##
  ## The knobs, with the ones whose sentinel is not obvious called out:
  ##
  ##   - `acks`: `-1` every in-sync replica, `1` the leader, `0` none (the record is
  ##     written without a response). Idempotent and transactional producers are
  ##     forced to `-1`.
  ##   - `timeoutMs`: broker-side wait for those acks; it is also the per-stage
  ##     budget `close` gives the transaction abort, the flush, and the drains.
  ##   - `compression`: `ccNone` (default), `ccGzip`, `ccSnappy`, `ccLz4`, `ccZstd`.
  ##   - `lingerMs`: how long a partition may wait for batch company; `0` sends
  ##     immediately. The batched path is the one `maxPendingRecords` backpressures.
  ##   - `maxBatchRecords` / `maxBatchBytes`: per-partition caps that seal and flush
  ##     the batch once reached; the byte cap is Kafka Java's `batch.size`.
  ##   - `maxRetries`: per-send retry cap, raised to `int32.high` by idempotence.
  ##   - `deliveryTimeoutMs`: wall-clock deadline for one send across its retries,
  ##     `<= 0` meaning unbounded (Kafka Java's `delivery.timeout.ms`).
  ##   - `retryBackoffMs`: base of the exponential, jittered retry backoff. The delay
  ##     is never below 1ms, so `0` is not an off switch.
  ##   - `maxInFlightRequests`: outstanding produce requests per partition, clamped to
  ##     5 while idempotence is on because the broker's dedup window is that wide.
  ##     Each broker connection also has a fixed window of 5 requests shared by every
  ##     partition it leads (and non-produce requests), so a broker leading many
  ##     partitions sees less parallelism than this per-partition limit suggests.
  ##   - `enableIdempotence` / `transactionalId`: idempotent delivery, and the
  ##     transactional API (`1..255` characters of `[a-zA-Z0-9._-]`). Either forces
  ##     `acks = -1`, unbounded retries, and the in-flight clamp. The per-partition
  ##     sequence number is a signed int32 that wraps from `int32.high` back to 0; the
  ##     client-side wrap is unit-tested, but a wire-level wrap against a real broker
  ##     (roughly `2^31` records on one producer-id) is not exercised in CI, so a
  ##     deployment at that scale should validate the broker's wrap acceptance itself.
  ##   - `transactionTimeoutMs`: broker-side window for a transaction (Kafka Java's
  ##     `transaction.timeout.ms`).
  ##   - `errorHandler`: failed-record routing. `nil` fails the record back to the
  ##     caller, and transactional sends bypass the handler entirely. A handler that
  ##     returns `eaDeadLetter` needs `setDeadLetterQueue` to have somewhere to
  ##     divert the record; without it the send resolves as `pstDeadLetterFailed`.
  ##   - `maxPendingFireAndForget`: outstanding `sendFireAndForget` tasks.
  ##   - `partitionStrategy` / `customPartitioner`: how `send` picks a partition when
  ##     it is not given one; a custom partitioner wins over the strategy.
  ##   - `maxPendingRecords`: cross-partition cap on records queued by the batched
  ##     path, which is what parks a fast producer.
  ##   - `metricsHook`: inline callback for producer events; `nil` makes each emit a
  ##     no-op. This overload passes it to the producer only, not to `cluster`.
  validateProducerConfig(
    lingerMs, acks, timeoutMs, transactionTimeoutMs, maxRetries, maxBatchRecords,
    maxBatchBytes, retryBackoffMs, maxPendingFireAndForget, maxPendingRecords,
    maxInFlightRequests, transactionalId,
  )

  var actualAcks = acks
  # deliveryTimeoutMs <= 0 means "unbounded" (count-based cap only): normalize
  # negatives to 0. There is no sentinel distinct from negatives.
  let actualDeliveryTimeoutMs = if deliveryTimeoutMs < 0: 0'i32 else: deliveryTimeoutMs
  var actualMaxRetries = maxRetries
  var actualEnableIdempotence = enableIdempotence
  var actualMaxInFlight = maxInFlightRequests
  if transactionalId.isSome:
    actualEnableIdempotence = true
    actualAcks = -1'i16
    actualMaxRetries = int32.high
  elif actualEnableIdempotence:
    actualAcks = -1'i16
    actualMaxRetries = int32.high

  # KIP-185: the broker keeps only a 5-batch dedup window per
  # producer-id/partition and the idempotent retry path relies on it — beyond that,
  # a retried lower-sequence batch is fenced as OUT_OF_ORDER_SEQUENCE_NUMBER (reset
  # loops) or silently reordered. Java rejects max.in.flight > 5 here; we clamp.
  const maxInFlightWithIdempotence = 5'i32
  if actualEnableIdempotence and actualMaxInFlight > maxInFlightWithIdempotence:
    warn "Producer maxInFlightRequests=",
      maxInFlightRequests,
      " capped to ",
      maxInFlightWithIdempotence,
      " due to ",
      (if transactionalId.isSome: "transactional mode" else: "enableIdempotence")
    actualMaxInFlight = maxInFlightWithIdempotence

  # Non-idempotent producers can reorder within a partition: a retried
  # lower-sequence batch (e.g. after NOT_LEADER_FOR_PARTITION backoff) can land
  # after a later batch that already ack'd. Java/librdkafka document the trade-off
  # but don't warn; surface it so the caller opts in explicitly. Idempotence
  # re-sequences retries, so it is exempt.
  elif not actualEnableIdempotence and actualMaxInFlight > 1 and actualMaxRetries > 0:
    warn "Non-idempotent producer with maxInFlightRequests=",
      maxInFlightRequests, " and maxRetries=", maxRetries,
      " may reorder records on retry; ",
      "set maxInFlightRequests=1 or enableIdempotence=true ",
      "to preserve per-partition order"

  # Surface silent overrides: a caller who requested e.g. acks=0 with
  # enableIdempotence learns that idempotence forced acks=all.
  if actualAcks != acks:
    warn "Producer acks=",
      acks,
      " overridden to ",
      actualAcks,
      " due to ",
      (if transactionalId.isSome: "transactional mode" else: "enableIdempotence")

  KafkaProducer(
    cluster: cluster,
    acks: actualAcks,
    timeoutMs: timeoutMs,
    compression: compression,
    lingerMs: lingerMs,
    maxBatchRecords: maxBatchRecords,
    maxBatchBytes: maxBatchBytes,
    maxRetries: actualMaxRetries,
    deliveryTimeoutMs: actualDeliveryTimeoutMs,
    retryBackoffMs: retryBackoffMs,
    maxInFlightRequests: actualMaxInFlight,
    enableIdempotence: actualEnableIdempotence,
    producerId: -1'i64,
    producerEpoch: -1'i16,
    sequenceNumbers: initTable[TopicPartition, int32](),
    idempotenceInitialized: false,
    initLock: newAsyncLock(),
    partitionCounters: initTable[string, int32](),
    batches: initTable[TopicPartition, seq[PendingMessage]](),
    pendingBatchBytes: initTable[TopicPartition, int64](),
    lingerTimers: initTable[TopicPartition, Future[void]](),
    lingerTimerEpoch: initTable[TopicPartition, uint64](),
    batchLock: newAsyncLock(),
    inflightSemaphores: initTable[TopicPartition, AsyncSemaphore](),
    writeChainTail: initTable[TopicPartition, Future[void]](),
    completionChainTail: initTable[TopicPartition, Future[void]](),
    state: lsRunning,
    clusterOwned: false,
    transactionalId: transactionalId,
    transactionTimeoutMs: transactionTimeoutMs,
    txnState: tsUninitialized,
    txnScope: TxnScope(),
    txnLock: newAsyncLock(),
    errorHandler: errorHandler,
    partitionStrategy: partitionStrategy,
    customPartitioner: customPartitioner,
    fireAndForgetSem: newAsyncSemaphore(maxPendingFireAndForget),
    pendingRecordsSem: newAsyncSemaphore(maxPendingRecords),
    maxPendingRecords: maxPendingRecords,
    metricsHook: guardMetricsHook(metricsHook),
  )

proc newKafkaProducer*(
    bootstrapBrokers: seq[string],
    clientId: string = "async-kafka-producer",
    acks: int16 = 1,
    timeoutMs: int32 = 30000,
    compression: CompressionCodec = ccNone,
    lingerMs: int32 = 0,
    maxBatchRecords: int32 = 1000,
    maxBatchBytes: int32 = 1_048_576,
    maxRetries: int32 = 3,
    deliveryTimeoutMs: int32 = 120_000,
    retryBackoffMs: int32 = 100,
    maxInFlightRequests: int32 = 5,
    enableIdempotence: bool = false,
    transactionalId: Option[string] = none(string),
    transactionTimeoutMs: int32 = 60000,
    errorHandler: ErrorHandler = nil,
    maxPendingFireAndForget: int32 = 1000,
    partitionStrategy: PartitionStrategy = psRoundRobin,
    customPartitioner: CustomPartitioner = nil,
    tlsConfig: TlsConfig = nil,
    hostAllowlist: HostAllowlist = nil,
    saslMechanism: SaslMechanism = nil,
    maxPendingRecords: int32 = 10_000,
    metricsHook: MetricsHook = nil,
): KafkaProducer {.raises: [KafkaError].} =
  ## Create a producer that owns its own Cluster; `close()` closes it too. The knobs are
  ## the ones `newKafkaProducer(cluster, ...)` documents, plus the transport settings for
  ## the Cluster it builds: `clientId` (the name the broker sees in request headers and
  ## logs), `tlsConfig` (`nil` = plaintext), `hostAllowlist` (`nil` disables the dial
  ## check; see `HostAllowlist`), and `saslMechanism` (`nil` = no SASL handshake), which
  ## also requires TLS unless it was built with `allowPlaintext = true`. `metricsHook`
  ## reaches both this Cluster and the producer.
  # Validate the producer config BEFORE creating the Cluster: a rejection must not
  # leave an already-constructed one behind (broker connections and a metadata loop
  # leaking until GC or process exit). The delegated constructor re-validates the
  # same arguments, so this cannot disagree with it.
  validateProducerConfig(
    lingerMs, acks, timeoutMs, transactionTimeoutMs, maxRetries, maxBatchRecords,
    maxBatchBytes, retryBackoffMs, maxPendingFireAndForget, maxPendingRecords,
    maxInFlightRequests, transactionalId,
  )
  let cluster = newCluster(
    bootstrapBrokers,
    clientId,
    tlsConfig = tlsConfig,
    hostAllowlist = hostAllowlist,
    saslMechanism = saslMechanism,
    metricsHook = metricsHook,
  )
  result = newKafkaProducer(
    cluster,
    acks = acks,
    timeoutMs = timeoutMs,
    compression = compression,
    lingerMs = lingerMs,
    maxBatchRecords = maxBatchRecords,
    maxBatchBytes = maxBatchBytes,
    maxRetries = maxRetries,
    deliveryTimeoutMs = deliveryTimeoutMs,
    retryBackoffMs = retryBackoffMs,
    maxInFlightRequests = maxInFlightRequests,
    enableIdempotence = enableIdempotence,
    transactionalId = transactionalId,
    transactionTimeoutMs = transactionTimeoutMs,
    errorHandler = errorHandler,
    maxPendingFireAndForget = maxPendingFireAndForget,
    partitionStrategy = partitionStrategy,
    customPartitioner = customPartitioner,
    maxPendingRecords = maxPendingRecords,
    metricsHook = metricsHook,
  )
  result.clusterOwned = true

proc isClosed*(p: KafkaProducer): bool =
  ## True once close has started or finished.
  p.state.isClosingOrClosed

proc state*(p: KafkaProducer): LifecycleState =
  ## Current lifecycle state: `lsRunning` from construction (a producer needs no explicit
  ## start), `lsClosing` while `close` runs, `lsClosed` after it.
  p.state

proc ownsCluster*(p: KafkaProducer): bool {.raises: [].} =
  ## True when this producer created its own Cluster (`newKafkaProducer` with
  ## bootstrap addresses) and therefore closes it in `close()`; false when the
  ## Cluster was injected and the caller owns it.
  p.clusterOwned

proc close*(p: KafkaProducer) {.async: (raises: []).}
  ## Single-flight close: a concurrent second close() parks on the in-progress
  ## teardown instead of draining / clearing the same tables again (forward-declared
  ## for closeImpl's DLQ cascade; see the definition below).

proc closeImpl(p: KafkaProducer) {.async: (raises: []).} =
  ## Four-stage close contract:
  ##   1. lsClosing — new send()/beginTransaction()/etc. raise KafkaClosedError.
  ##   2. abort any active transaction so the broker doesn't keep waiting.
  ##   3. drain in-flight batches and fire-and-forget tasks.
  ##   4. tear down per-partition timers, release the owned Cluster, → lsClosed.
  if p.state == lsClosed:
    return
  p.state = lsClosing
  let closeStarted = Moment.now()

  # Abort an active transaction with a valid coordinator. Skip when nothing was
  # started broker-side (a prior commit/abort cleared the sets) — reissuing EndTxn
  # would only confuse the coordinator. `txnStarted` is checked with the partition
  # sets so an offsets-only transaction (nothing in `txnPartitions`) is still
  # aborted rather than left to time out.
  if p.transactionalId.isSome and p.txnState in txnInProgressStates and
      p.txnCoordinator != nil and
      (p.txnPartitions.len > 0 or p.txnPendingPartitions.len > 0 or p.txnStarted):
    # Bound the abort by timeoutMs so a held txnLock or unresponsive coordinator
    # cannot deadlock close(); on timeout the future is cancelled and we fall
    # through to cleanup.
    try:
      let completed = await withTimeout(p.abortTransaction(), milliseconds(p.timeoutMs))
      if not completed:
        warn "Transaction abort timed out during close after ", p.timeoutMs, "ms"
        emitMetric(
          p.metricsHook,
          meProducerCloseAbortFailed,
          [(cstring"reason", cstring"timeout")],
          1,
        )
        if not p.tryTransition(tsError):
          warnTransitionFailed(p, tsError, "close-timeout abort")
    except KafkaError as e:
      warn "Failed to abort transaction during close: ", e.msg
    except CancelledError:
      debug "Transaction abort cancelled during close"

  # Bound by timeoutMs so a blocked flush cannot hang close(). withTimeout cannot
  # see this timeout: flush() swallows CancelledError and completes, so race a
  # timer and treat CancelledError as the caller's close, not a flush outcome.
  # After a timeout we still wait for flush to settle so failRemaining does not
  # contend with it for batchLock.
  block flushWithTimeout:
    let flushFut = p.flush()
    let timer = sleepAsync(milliseconds(p.timeoutMs))
    let raced = newFuture[void]("producer.close.flush.race")
    proc finishFlushRace(udata: pointer) {.gcsafe, raises: [].} =
      if not raced.finished:
        raced.complete()

    flushFut.addCallback(finishFlushRace)
    timer.addCallback(finishFlushRace)
    try:
      await raced
      if not flushFut.finished:
        emitMetric(
          p.metricsHook,
          meProducerCloseFlushFailed,
          [(cstring"reason", cstring"timeout")],
          1,
        )
        flushFut.cancelSoon()
      else:
        cancelTimer(timer)
      await flushFut
    except CancelledError:
      break flushWithTimeout
    except CatchableError:
      absorbDuringTeardown("producer::close", "flush failed during close")

  # Fail any batch futures that flush could not send (timeout, cancellation, or
  # lock contention can cause flush to return without draining all batches).
  block failRemaining:
    var batchLockHeld = false
    let lockFut = p.batchLock.acquire()
    try:
      await lockFut
      batchLockHeld = true
    except CancelledError:
      # A cancellation can land after a wake-up already handed us the lock, so the
      # finally still must release; a cancelled queued waiter never took it, and
      # releasing would free a lock another coroutine holds (AsyncLock has no
      # per-coroutine ownership).
      batchLockHeld = lockFut.completed()
      absorbDuringTeardown(
        "producer::close", "cancel acquiring batchLock to fail remaining batches"
      )
    try:
      var remainingTps: seq[TopicPartition]
      for tp in p.batches.keys:
        remainingTps.add(tp)
      if remainingTps.len > 0:
        warn "Failing ", remainingTps.len, " unflushed partition batch(es) during close"
        emitMetric(
          p.metricsHook,
          meProducerCloseUnflushedBatches,
          noTags,
          int64(remainingTps.len),
        )
        let err = newException(KafkaError, "producer closed before batch could be sent")
        for tp in remainingTps:
          p.failBatchFutures(tp, err)
    finally:
      if batchLockHeld:
        p.batchLock.releaseSafely("producer.finishFlushRace")

  # Drain pending fire-and-forget tasks. Loops because a straggler that passed its
  # state check before the transition may add() after our setLen(0); those are
  # finite (new calls are rejected at their acquire-time state guard). Bounded by a
  # single timeoutMs budget — a per-iteration cap let stragglers strand forever.
  let drainDeadline = Moment.now() + milliseconds(p.timeoutMs)
  while p.pendingFireAndForget.len > 0:
    var pending: seq[Future[void]]
    for fut in p.pendingFireAndForget:
      if not fut.finished:
        pending.add(fut)
    # Snapshot-and-clear: an in-flight straggler's add() lands in the now-empty
    # list and is picked up next iteration.
    p.pendingFireAndForget.setLen(0)

    if pending.len == 0:
      break

    let now = Moment.now()
    if now >= drainDeadline:
      warn "Fire-and-forget drain budget elapsed with ",
        pending.len, " task(s) still running; cancelling"
      var cancelled = 0'i64
      for fut in pending:
        if not fut.finished:
          fut.cancelSoon()
          inc cancelled
      if cancelled > 0:
        emitMetric(p.metricsHook, meProducerCloseDrainCancelled, noTags, cancelled)
      break

    let remaining = drainDeadline - now
    try:
      await allFutures(pending).wait(remaining)
    except AsyncTimeoutError:
      # Slice budget ran out: re-park stragglers so the next iteration's deadline
      # check retries or cancels them; never silently strand a running task.
      for fut in pending:
        if not fut.finished:
          p.pendingFireAndForget.add(fut)
    except CancelledError:
      absorbDuringTeardown(
        "producer::close", "cancelled while draining the send pipeline"
      )
      break

  # Snapshot-and-clear BEFORE cancelling: `cancelAndWait` yields, so a linger timer
  # can `del` its own entry mid-iteration; clearing first makes that a no-op.
  var lingerFuts: seq[Future[void]]
  for fut in p.lingerTimers.values:
    lingerFuts.add(fut)
  p.lingerTimers.clear()
  p.lingerTimerEpoch.clear()
  var lingerCancelled = 0'i64
  # The reaping stage absorbs a cancellation instead of unwinding: `close()` never
  # re-runs `closeImpl`, so a cancel delivered here would otherwise leave the
  # producer in `lsClosing` with the owned Cluster open, and a timer that was never
  # reaped would later fire `flushBatch` against a Cluster being torn down. The
  # stage is restartable — reaping a finished timer is a no-op — so a cancel at any
  # point in it resumes where it stopped, including the test park below.
  var reaped = 0
  while reaped < lingerFuts.len:
    try:
      when defined(kafkaCloseTestHooks):
        if p.closeLingerPark != nil:
          let park = p.closeLingerPark
          p.closeLingerPark = nil
          p.closeLingerParked = true
          await park
      let fut = lingerFuts[reaped]
      if not fut.finished:
        await fut.cancelAndWait()
        inc lingerCancelled
      inc reaped
    except CancelledError:
      absorbDuringTeardown("producer::close", "cancel reaping a linger timer")
  when defined(kafkaCloseTestHooks):
    p.closeLingerParked = false
  if lingerCancelled > 0:
    emitMetric(p.metricsHook, meProducerCloseLingerCancelled, noTags, lingerCancelled)

  # Drop per-partition bookkeeping; `maybeEvictIdlePartitionState`
  # reclaims idle partitions already, so this releases what was in flight at close.
  # `sequenceNumbers` goes too: idle eviction preserves it for the producer-id's
  # lifetime (broker-tracked per (producerId, partition)) and it is discardable
  # only now, while shutting down.
  p.inflightSemaphores.clear()
  p.writeChainTail.clear()
  p.completionChainTail.clear()
  p.pendingBatchBytes.clear()
  p.stickyPartitions.clear()
  p.sequenceNumbers.clear()
  p.txnPartitions.clear()
  p.txnPendingPartitions.clear()
  p.inflightBatches.clear()
  p.partitionCounters.clear()
  if p.clusterOwned:
    try:
      await p.cluster.close()
    except CancelledError:
      # A cancel delivered at this boundary must not skip the state transition
      # below. Cluster.close() absorbs its own cancellation and still reaches
      # lsClosed, so the Cluster is not left half-open.
      absorbDuringTeardown("producer::close", "cancel closing the owned Cluster")

  # Capture the owned link in this synchronous turn so the cascade uses the
  # same snapshot the guard checked — never re-deref after an intervening await.
  let ownedDlq = p.dlqProducer
  if ownedDlq != nil and p.ownsDlqProducer:
    # Cascade close into an owned DLQ producer after the local cluster close, so it
    # drains against its own cluster without contention; errors are swallowed (close
    # is best-effort). Bound by timeoutMs: withTimeout cannot see this timeout
    # (close swallows CancelledError), so race a timer; CancelledError here is the
    # caller's close.
    let dlqCloseFut = close(ownedDlq)
    let timer = sleepAsync(milliseconds(p.timeoutMs))
    let raced = newFuture[void]("producer.close.dlq.race")
    proc finishRace(udata: pointer) {.gcsafe, raises: [].} =
      if not raced.finished:
        raced.complete()

    dlqCloseFut.addCallback(finishRace)
    timer.addCallback(finishRace)
    try:
      await raced
      if not dlqCloseFut.finished:
        warn "Owned DLQ producer close timed out during close after ", p.timeoutMs, "ms"
        emitMetric(
          p.metricsHook,
          meProducerCloseDlqFailed,
          [(cstring"reason", cstring"timeout")],
          1,
        )
        dlqCloseFut.cancelSoon()
      else:
        cancelTimer(timer)
    except CancelledError:
      absorbDuringTeardown("producer::close", "cancel closing the DLQ producer")
    except CatchableError:
      absorbDuringTeardown("producer::close", "DLQ producer close failed")
  emitMetric(
    p.metricsHook,
    meProducerCloseDurationMs,
    noTags,
    int64((Moment.now() - closeStarted).milliseconds),
  )
  p.state = lsClosed

proc close*(p: KafkaProducer) {.async: (raises: []).} =
  ## Single-flight close: a concurrent second close() parks on the in-progress
  ## teardown instead of re-draining the same tables. Chronos is single-threaded,
  ## so the check-and-claim below (state test + `closeFuture`) is atomic.
  if p.state.isClosingOrClosed:
    if p.closeFuture != nil and not p.closeFuture.finished:
      try:
        await p.closeFuture
      except CatchableError:
        absorbDuringTeardown("producer::close", "a concurrent close() failed")
    return
  p.closeFuture = closeImpl(p)
  try:
    await p.closeFuture
  except CatchableError:
    absorbDuringTeardown("producer::close", "closeImpl failed")

proc setDeadLetterQueue*(
    p: KafkaProducer, topic: string, producer: KafkaProducer, ownsProducer: bool = false
): Future[void] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Failed-send routing: `ownsProducer = false` (default) means the caller closes
  ## `producer`; `true` makes `p.close()` cascade into it after `p`'s own teardown.
  ##
  ## This is what gives an `eaDeadLetter` handler somewhere to send: without it, a
  ## failed send resolves as `pstDeadLetterFailed` (plus a `warn` and a
  ## `producer.dlq.error` metric tagged `status=unconfigured`) instead of diverting.
  ## `eaDrop`, and a handler that raises (which falls back to `eaFail`), need none.
  ##
  ## Calling this again replaces the configuration: a replaced *owned* producer is
  ## closed before this returns unless it is the one being installed, since
  ## `p.close()` only cascades into the configuration current at close time.
  ##
  ## The three fields below are swapped in one synchronous unit (no await), so every
  ## reader sees an atomic old-or-new configuration without a lock.
  requireRunning(p)
  if producer == nil:
    raise newException(KafkaError, "dead letter queue producer must not be nil")
  if producer == p:
    raise newException(
      KafkaError,
      "dead letter queue producer must not be the same instance as the source producer",
    )
  if topic.len == 0:
    raise newException(KafkaError, "dead letter queue topic must not be empty")
  # Reject a DLQ cycle (A→B→A, or longer): if `p` is reachable via `producer`'s
  # own DLQ chain, a record failing every hop could bounce between producers.
  # Bounded walk so a pre-existing cycle among unrelated producers can't hang.
  var hop = producer.dlqProducer
  var guard = 0
  while hop != nil and guard < 64:
    if hop == p:
      raise newException(
        KafkaError,
        "dead letter queue configuration would form a cycle back to the source producer",
      )
    hop = hop.dlqProducer
    inc guard

  # Swap in one synchronous turn, before the await below: the new configuration
  # is installed even if closing the replaced producer is cancelled.
  let replaced = p.dlqProducer
  let replacedOwned = p.ownsDlqProducer
  p.dlqTopic = topic
  p.dlqProducer = producer
  p.ownsDlqProducer = ownsProducer

  if replacedOwned and replaced != nil and replaced != producer:
    # close() is `raises: []` and absorbs a cancel into its own teardown, so a
    # cancellation here stops THIS await without stranding the replaced producer.
    await replaced.close()
