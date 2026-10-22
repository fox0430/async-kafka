## Producer module — transactional API and the transaction state machine. Holds
## the legal-transition table (`canTransition`) and the single mutator
## (`tryTransition`), plus the broker RPCs that don't depend on `flush`.
## `commitTransaction` / `abortTransaction` live with the batch pipeline because
## they call `flush` and would otherwise create a circular import.

import std/[tables, options, sets, importutils]

import ../[async_backend, errors, broker, cluster]
import ../protocol/messages
import ../internal/[backoff, cancel, metrics, retry, coordinator_rpc, safe_logging]

import types

privateAccess(KafkaProducer)

const
  txnCoordMaxAttempts = 5
  txnCoordBackoffBaseMs = 100'i32

proc canTransition*(src, dst: TransactionState): bool =
  ## True when the state machine permits the src → dst edge.
  if src == dst:
    # Self-loops let cleanup paths re-assert an already-applied state.
    return true
  case src
  of tsUninitialized:
    dst in {tsReady, tsFatal}
  of tsReady:
    dst in {tsInTransaction, tsFatal}
  of tsInTransaction:
    dst in {tsCommitting, tsAborting, tsError, tsFatal}
  of tsCommitting:
    # A failed commit can leave the producer in tsCommitting with the user then
    # calling abortTransaction, so commit → abort must stay legal.
    dst in {tsReady, tsAborting, tsError, tsFatal}
  of tsAborting:
    dst in {tsReady, tsError, tsFatal}
  of tsError:
    dst in {tsAborting, tsReady, tsFatal}
  of tsFatal:
    false

proc tryTransition*(p: KafkaProducer, dst: TransactionState): bool {.discardable.} =
  ## The single point of mutation for `txnState`; false means the edge was
  ## refused. `tsFatal` is terminal and the producer must then be closed.
  if canTransition(p.txnState, dst):
    p.txnState = dst
    true
  else:
    false

template warnTransitionFailed*(
    p: KafkaProducer, dst: TransactionState, context: string
) =
  ## Log a refused `txnState` transition: `canTransition` gates every transition, so
  ## a refusal means a concurrent path already moved the producer unexpectedly.
  warn context & ": cannot transition to " & $dst & " from " & $p.txnState

proc transitionToError*(p: KafkaProducer, code: KafkaErrorCode) =
  ## Pick tsFatal vs tsError by error class, context-aware so PRODUCER_FENCED and
  ## friends poison the producer while INVALID_RECORD (87),
  ## OUT_OF_ORDER_SEQUENCE_NUMBER (45) and UNKNOWN_PRODUCER_ID (59) — non-retriable
  ## context-free but abortable inside a txn — fail only the current txn (tsError).
  ## Keeps the transition in step with the context-aware `error_class` metric label.
  ##
  ## A real change emits `producer.txn.fatal` / `producer.txn.error` with
  ## `error_code`; self-loops do not (batch catch re-invokes this after EndTxn).
  if errorClassForContext(code, isTransactional = p.transactionalId.isSome) == ecFatal:
    let prev = p.txnState
    if not p.tryTransition(tsFatal):
      warnTransitionFailed(
        p, tsFatal, "transitionToError(" & kafkaErrorName(code) & ")"
      )
    elif prev != tsFatal:
      emitMetric(
        p.metricsHook, meProducerTxnFatal, [("error_code", kafkaErrorName(code))], 1
      )
  else:
    let prev = p.txnState
    if not p.tryTransition(tsError):
      warnTransitionFailed(
        p, tsError, "transitionToError(" & kafkaErrorName(code) & ")"
      )
    elif prev != tsError:
      emitMetric(
        p.metricsHook, meProducerTxnError, [("error_code", kafkaErrorName(code))], 1
      )

proc refreshTxnCoordinator*(
    p: KafkaProducer
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Drop the cached coordinator mapping and re-resolve `p.txnCoordinator`. Without
  ## this a coordinator move left every commit/abort failing against the stale
  ## broker handle until the user re-ran initTransactions.
  if p.transactionalId.isNone:
    raise newException(KafkaError, "producer is not transactional")
  let txnId = p.transactionalId.get
  p.cluster.invalidateTxnCoordinator(txnId)
  p.txnCoordinator = await p.cluster.txnCoordinatorBroker(txnId)

proc firstTxnErrorCode*(
    r: InitProducerIdResponse
): KafkaErrorCode {.inline, raises: [].} =
  r.errorCode

proc firstTxnErrorCode*(
    r: AddOffsetsToTxnResponse
): KafkaErrorCode {.inline, raises: [].} =
  r.errorCode

proc firstTxnErrorCode*(r: EndTxnResponse): KafkaErrorCode {.inline, raises: [].} =
  r.errorCode

proc firstTxnErrorCode*(r: AddPartitionsToTxnResponse): KafkaErrorCode {.raises: [].} =
  ## The first non-keNone per-partition code (no top-level code). Retrying re-sends
  ## the full partition set, which the broker treats idempotently.
  for t in r.errors:
    for pr in t.partitions:
      if pr.errorCode != keNone:
        return pr.errorCode
  keNone

proc firstTxnErrorCode*(r: TxnOffsetCommitResponse): KafkaErrorCode {.raises: [].} =
  ## The first non-keNone per-partition code (no top-level code). Retrying re-sends
  ## the full offset set, which the broker treats idempotently within the transaction.
  for t in r.topics:
    for pr in t.partitions:
      if pr.errorCode != keNone:
        return pr.errorCode
  keNone

template withTxnCoordinatorRetry*(p: KafkaProducer, call: untyped): untyped =
  ## Evaluate `call` — an `await`ed txn-coordinator RPC that re-reads
  ## `p.txnCoordinator` each evaluation — through the shared `runCoordinatorRpc`
  ## driver, so the moved/retry/surface decision comes from `retry.classifyCode`:
  ## NOT_COORDINATOR / COORDINATOR_NOT_AVAILABLE or a transport failure re-resolves
  ## via `refreshTxnCoordinator`; other `ecRetriable` codes retry the same
  ## coordinator; anything else (incl. keNone) or an exhausted budget yields the
  ## response as-is for terminal handling at the call site.
  runCoordinatorRpc(
    rpcCtx =
      RetryContext(isTransactional: true, isIdempotent: false, isCoordinatorRpc: true),
    maxAttempts = txnCoordMaxAttempts,
    backoffBaseMs = txnCoordBackoffBaseMs,
    rediscover = await p.refreshTxnCoordinator(),
    errorCodeOf = firstTxnErrorCode,
    rpcCall = call,
  )

proc initTransactions*(
    p: KafkaProducer
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Allocates a producer identity (tsUninitialized → tsReady); a real init emits
  ## `producer.txn.init`. Refused inits raise and do not emit.
  if p.transactionalId.isNone:
    raise newException(KafkaError, "producer is not transactional")
  # Reject a fatal producer before InitProducerId: tsFatal is terminal, so the
  # trailing tryTransition(tsReady) would fail — but only after the RPC allocated a
  # fresh PID (zombie transactional.id) while reporting init success.
  if p.txnState == tsFatal:
    raise newException(KafkaError, "producer is in a fatal state and must be closed")
  # A second initTransactions would allocate a new PID under the same
  # transactional.id, then silently drop the tsReady transition below (illegal edge),
  # orphaning the coordinator's ongoing txn.
  if p.txnState != tsUninitialized:
    raise
      newException(KafkaError, "initTransactions already called; state=" & $p.txnState)

  await p.initLock.acquire()
  try:
    # Recheck under the lock — a concurrent init or a fatal transition may have
    # landed while we were queued.
    if p.txnState == tsFatal:
      raise newException(KafkaError, "producer is in a fatal state and must be closed")
    if p.txnState != tsUninitialized:
      raise newException(
        KafkaError, "initTransactions already called; state=" & $p.txnState
      )

    p.txnCoordinator = await p.cluster.txnCoordinatorBroker(p.transactionalId.get)
    let req = InitProducerIdRequest(
      transactionalId: p.transactionalId,
      transactionTimeoutMs: p.transactionTimeoutMs,
      producerId: -1,
      producerEpoch: -1,
    )
    let resp = withTxnCoordinatorRetry(p, await p.txnCoordinator.initProducerId(req))
    if resp.errorCode != keNone:
      raise newKafkaApiError(
        resp.errorCode, "InitProducerId (txn) failed: " & kafkaErrorName(resp.errorCode)
      )
    p.producerId = resp.producerId
    p.producerEpoch = resp.producerEpoch
    p.sequenceNumbers.clear()
    p.idempotenceInitialized = true
    # A concurrent transitionToError could have flipped to tsFatal during the RPC;
    # surface the rejected transition instead of leaving a fresh PID under a fatal
    # producer.
    if not p.tryTransition(tsReady):
      raise newException(
        KafkaError,
        "InitProducerId succeeded but transition to tsReady rejected from " & $p.txnState,
      )
    emitMetric(p.metricsHook, meProducerTxnInit, noTags, 1)
  finally:
    p.initLock.releaseSafely("producer.initTransactions")

proc beginTransaction*(p: KafkaProducer) {.raises: [KafkaError].} =
  ## Starts a client-side transaction (tsReady → tsInTransaction); a real begin
  ## emits `producer.txn.begin`. Refused begins raise and do not emit.
  if p.transactionalId.isNone:
    raise newException(KafkaError, "producer is not transactional")
  # Reject after lsClosing: a close() awaiting abortTransaction's pre-lock
  # ensureEpochBumped / txnLock.acquire would otherwise yield to a synchronous
  # beginTransaction that clears txnStarted, and the resumed abort would skip
  # EndTxn and orphan the coordinator's Ongoing txn.
  requireRunning(p)
  # Safe without acquire: no await points, so nothing can interleave here.
  if p.txnLock.locked:
    raise newException(KafkaError, "another transaction operation is in progress")
  if p.txnState == tsFatal:
    raise newException(KafkaError, "producer is in a fatal state and must be closed")
  # Only tsReady may begin: canTransition's src == dst self-loop would accept
  # tsInTransaction → tsInTransaction, and the resets below would then wipe a live
  # txn's tracking — a later commit/abort/close would skip EndTxn.
  if p.txnState != tsReady:
    raise newException(KafkaError, "cannot begin transaction in state: " & $p.txnState)
  if not p.tryTransition(tsInTransaction):
    # Unreachable: the guard above admits only tsReady, and tsReady →
    # tsInTransaction is legal. Kept for symmetry with the state machine.
    warnTransitionFailed(p, tsInTransaction, "beginTransaction")
  else:
    emitMetric(p.metricsHook, meProducerTxnBegin, noTags, 1)
  inc p.txnSeq
  # Sends still counted belong to the txn that just ended and never decrement this one.
  p.txnUnjoinedSends = 0
  p.finalizeTxnTracking(owesEpochBump = false)
  # Client-only change: the coordinator has no txn yet, so a commit/abort before the
  # first send must skip EndTxn; finalizeTxnTracking clears the broker-side marker.

proc markPartitionForTxn*(p: KafkaProducer, tp: TopicPartition) =
  if p.transactionalId.isNone or tp in p.txnPartitions:
    return
  p.txnPendingPartitions.incl(tp)

proc registerPendingTxnPartitions*(
    p: KafkaProducer
) {.async: (raises: [KafkaError, CancelledError]).} =
  if p.txnPendingPartitions.len == 0:
    return

  var topicParts: Table[string, seq[int32]]
  for tp in p.txnPendingPartitions:
    topicParts.mgetOrPut(tp.topic, @[]).add(tp.partition)

  var topics: seq[AddPartitionsToTxnTopicReq]
  for topic, parts in topicParts:
    topics.add(AddPartitionsToTxnTopicReq(topic: topic, partitions: parts))

  let addReq = AddPartitionsToTxnRequest(
    transactionalId: p.transactionalId.get,
    producerId: p.producerId,
    producerEpoch: p.producerEpoch,
    topics: topics,
  )
  let addResp =
    withTxnCoordinatorRetry(p, await p.txnCoordinator.addPartitionsToTxn(addReq))

  # Scan the per-partition results once (AddPartitionsToTxn has no top-level code):
  # capture the first error and whether any partition was accepted, since a broker
  # that adds one partition but rejects a sibling has still moved this txn to
  # `Ongoing`.
  var firstErr = keNone
  var anyAdded = false
  for t in addResp.errors:
    for pr in t.partitions:
      if pr.errorCode == keNone:
        anyAdded = true
      elif firstErr == keNone:
        firstErr = pr.errorCode

  if firstErr != keNone:
    # Mark started BEFORE raising when any partition was accepted: the coordinator
    # moved to `Ongoing`, so the abort/close that follows must still issue EndTxn
    # instead of orphaning it until transaction.timeout.ms. With nothing accepted the
    # txn stayed `Empty`, where EndTxn would be INVALID_TXN_STATE.
    if anyAdded:
      p.txnStarted = true
    raise newKafkaApiError(
      firstErr, "AddPartitionsToTxn failed: " & kafkaErrorName(firstErr)
    )

  for tp in p.txnPendingPartitions:
    p.txnPartitions.incl(tp)
  p.txnPendingPartitions.clear()
  # Success moved the txn to `Ongoing`: commit/abort must now send a real EndTxn.
  p.txnStarted = true

type
  EpochBumpOutcome = enum
    eboApplied ## New identity acquired; `pid`/`epoch` hold it.
    eboDeferred ## Budget exhausted or non-retriable: pendingEpochBump stays set.
    eboRetriable ## Transport/retriable failure: the caller should try again.

  EpochBumpAttempt = object
    outcome: EpochBumpOutcome
    pid: int64
    epoch: int16

proc bumpEpochOnce(
    p: KafkaProducer, basePid: int64, baseEpoch: int16, isLast: bool
): Future[EpochBumpAttempt] {.async: (raises: [KafkaError, CancelledError]).} =
  ## A single InitProducerId attempt against the txn coordinator with no producer
  ## lock held; state is untouched (the caller applies a success via
  ## `applyEpochBump`). Retry/backoff policy and deferral live in `bumpEpoch`.
  let bumpReq = InitProducerIdRequest(
    transactionalId: p.transactionalId,
    transactionTimeoutMs: p.transactionTimeoutMs,
    producerId: basePid,
    producerEpoch: baseEpoch,
  )

  var bumpResp: InitProducerIdResponse
  try:
    bumpResp = await p.txnCoordinator.initProducerId(bumpReq)
  except KafkaConnectionError, KafkaTimeoutError, KafkaProtocolError:
    # Transport failure post-EndTxn: raising would report a committed txn as failed.
    # Only KafkaConnectionError hints at a coordinator move worth re-resolving.
    let curExc = getCurrentException()
    if isLast:
      p.pendingEpochBump = true
      emitMetric(
        p.metricsHook,
        meProducerTxnBumpEpochDeferred,
        [
          # The arm catches only the three transport KafkaError subtypes, so the
          # downcast is exact (as with `(ref KafkaApiError)(offsetsErr)` below).
          ("error_code", kafkaErrorTag((ref KafkaError)(curExc))),
          ("retriable", "true"),
        ],
        1,
      )
      return EpochBumpAttempt(outcome: eboDeferred)
    if curExc of KafkaConnectionError:
      try:
        await p.refreshTxnCoordinator()
      except KafkaError:
        discard
    return EpochBumpAttempt(outcome: eboRetriable)

  if bumpResp.errorCode == keNone:
    return EpochBumpAttempt(
      outcome: eboApplied, pid: bumpResp.producerId, epoch: bumpResp.producerEpoch
    )

  # Classify with transactional context so INVALID_PRODUCER_ID_MAPPING (49) stays
  # abortable inside a txn (matching Java's TransactionManager) instead of looping as
  # it would for idempotent-only. OUT_OF_ORDER_SEQUENCE_NUMBER (45) is expected
  # transiently after EndTxn; CONCURRENT_TRANSACTIONS (51) means the txn-state
  # machine has not quiesced.
  let retriable =
    errorClassForContext(bumpResp.errorCode, isTransactional = true) == ecRetriable or
    bumpResp.errorCode == keOutOfOrderSequenceNumber
  if not retriable:
    # EndTxn already succeeded broker-side; raising would report a completed txn as
    # failed, so poison the producer and defer.
    p.transitionToError(bumpResp.errorCode)
    p.pendingEpochBump = true
    emitMetric(
      p.metricsHook,
      meProducerTxnBumpEpochDeferred,
      [("error_code", kafkaErrorName(bumpResp.errorCode)), ("retriable", "false")],
      1,
    )
    return EpochBumpAttempt(outcome: eboDeferred)

  if isCoordinatorMoved(bumpResp.errorCode):
    # Coordinator moved: retrying the same handle cannot succeed; re-resolve first.
    try:
      await p.refreshTxnCoordinator()
    except KafkaError:
      discard # next iteration retries against the old handle

  if isLast:
    # Budget exhausted but retriable: defer rather than fail the transaction call.
    p.pendingEpochBump = true
    emitMetric(
      p.metricsHook,
      meProducerTxnBumpEpochDeferred,
      [("error_code", kafkaErrorName(bumpResp.errorCode)), ("retriable", "true")],
      1,
    )
    return EpochBumpAttempt(outcome: eboDeferred)

  EpochBumpAttempt(outcome: eboRetriable)

proc applyEpochBump(
    p: KafkaProducer, basePid: int64, baseEpoch: int16, newPid: int64, newEpoch: int16
): bool =
  ## Apply a successful bump response in one synchronous block, so it cannot
  ## interleave with `allocateInflight`'s encode or `resetProducerId` (chronos is
  ## single-threaded; this read-modify-write spans no await). A stale response —
  ## `resetProducerId` changed the identity while the RPC was in flight — is
  ## discarded and left pending for the next caller, and false is returned.
  if p.producerId != basePid or p.producerEpoch != baseEpoch:
    return false
  p.producerId = newPid
  p.producerEpoch = newEpoch
  p.sequenceNumbers.clear()
  p.pendingEpochBump = false
  true

proc bumpEpoch*(p: KafkaProducer) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Re-initialize producer ID to get a bumped epoch after EndTxn. A real bump
  ## emits `producer.txn.bump_epoch`; deferred / abandoned bumps do not. Retries
  ## with jittered backoff while the broker may still be finalizing. A non-retriable
  ## error poisons the producer and defers the bump without raising; retriable failures
  ## that exhaust the budget **return without raising** but set `pendingEpochBump`
  ## ("outcome final, bump still owed"), retried by the next
  ## send/commit/abort/sendOffsetsToTransaction via `ensureEpochBumped`
  ## (beginTransaction cannot await, so the following send/commit/abort does).
  ##
  ## No producer lock is held across the network I/O: each attempt runs lock-free
  ## (`bumpEpochOnce`) and the apply is a synchronous block (`applyEpochBump`), so
  ## `resetProducerId` / `ensureIdempotenceInitialized` / `reassignSequencesAfterReset`
  ## / `initTransactions` are not blocked by a slow bump. Duplicate InitProducerId
  ## RPCs are prevented by `ensureEpochBumped`'s single-flight gate on the send path
  ## and by `txnLock` on the commit/abort path.
  # tsFatal is terminal and a deferred bump can never be consumed, but the entry
  # points run ensureEpochBumped BEFORE their tsFatal check: without this guard a
  # poisoned producer with pendingEpochBump set would fire a fresh InitProducerId
  # (orphaned PID) on every call, only for the state check to raise right after.
  # Return without raising — the bump is irrelevant once fatal.
  if p.txnState == tsFatal:
    return

  const bumpEpochAttempts = 7
  for i in 0 ..< bumpEpochAttempts:
    if i > 0:
      await sleepAsync(milliseconds(jitteredBackoff(50'i32, i)))
    # If close() flipped to lsClosing/lsClosed, abandon the bump: another
    # initProducerId would log a fresh PID for a discarded producer (zombie txn-id).
    # Return *without raising* — the preceding EndTxn already succeeded broker-side.
    if p.state != lsRunning:
      return

    let basePid = p.producerId
    let baseEpoch = p.producerEpoch
    let attempt =
      await p.bumpEpochOnce(basePid, baseEpoch, isLast = i == bumpEpochAttempts - 1)
    case attempt.outcome
    of eboApplied:
      if not p.applyEpochBump(basePid, baseEpoch, attempt.pid, attempt.epoch):
        # Identity changed while the RPC was in flight (resetProducerId): leave
        # pendingEpochBump set and emit no success — the next caller retries.
        return
      # Emit only once the identity is actually installed.
      emitMetric(p.metricsHook, meProducerTxnBumpEpoch, noTags, 1)
      return
    of eboDeferred:
      return
    of eboRetriable:
      discard

proc ensureEpochBumped*(
    p: KafkaProducer
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Retry a deferred bump (`pendingEpochBump` is set whenever a bump is deferred,
  ## on a non-retriable error or an exhausted budget); no-op when the flag is false.
  ## Called by the
  ## transactional send path before the first record of a new transaction.
  ##
  ## send reaches this holding no producer lock, so without a gate concurrent sends
  ## could all race into `bumpEpoch` and issue redundant InitProducerId RPCs (burning
  ## sequence numbers). The single-flight gate lets the first arrival drive the bump
  ## while followers await `epochBumpDone`. commit/abort call `bumpEpoch` directly
  ## under `txnLock`, which already serializes them against each other and against
  ## send's later txnLock acquire.
  if not p.pendingEpochBump:
    return

  # A non-retriable broker error leaves the producer poisoned in tsError with the
  # flag still set (bumpEpoch's defer path), and re-attempting from tsError can only
  # reproduce the same error — one wasted InitProducerId per entry point until a
  # successful EndTxn changes the epoch broker-side, which commit/abort do by calling
  # bumpEpoch directly. Mirrors bumpEpoch's tsFatal short-circuit.
  if p.txnState == tsError:
    return

  if p.epochBumpInflight:
    let done = p.epochBumpDone
    if done != nil:
      # The gate future is only ever completed, never failed — a waiting caller
      # surfaces a cancel and ignores the rest (see the write-chain gate).
      try:
        await done
      except CancelledError as e:
        raise e
      except CatchableError:
        absorbDuringTeardown("producer::bumpEpoch", "epoch-bump gate failed")
    return

  p.epochBumpInflight = true
  let done = newFuture[void]("epochBump")
  p.epochBumpDone = done
  defer:
    p.epochBumpInflight = false
    p.epochBumpDone = nil
    if not done.finished:
      done.complete()

  await p.bumpEpoch()

proc resolveAndTxnOffsetCommit(
    p: KafkaProducer, groupId: string, req: TxnOffsetCommitRequest
): Future[TxnOffsetCommitResponse] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Re-resolve the GROUP coordinator (TxnOffsetCommit targets it, not the txn
  ## coordinator) and send. A separate proc so `runCoordinatorRpc` re-evaluates a
  ## single `await` per attempt — a nested double-`await` trips Nim's lowering.
  let groupCoord = await p.cluster.coordinatorBroker(groupId)
  await groupCoord.txnOffsetCommit(req)

proc sendOffsetsToTransaction*(
    p: KafkaProducer,
    offsets: Table[TopicPartition, int64],
    groupMetadata: ConsumerGroupMetadata,
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Commit consumer offsets inside the current transaction (consume-process-
  ## produce / EOS). Each value in `offsets` is the next offset to read — the last
  ## processed record's offset + 1 — as in the consumer's own commits; committing
  ## the record's own offset re-delivers it after a restart or rebalance.
  ##
  ## Success emits `producer.txn.send_offsets`; transport / cancel emit
  ## `producer.txn.send_offsets.error` (`error_code` / `error_class`), while refused
  ## sends and API
  ## failures raise and do not emit those (API failures already emit
  ## `producer.txn.error` / `fatal`). A real generationId/memberId in `groupMetadata`
  ## (from `consumer.groupMetadata()`) makes the broker fence the commit against a
  ## stale consumer generation (KIP-447); `initUnfencedConsumerGroupMetadata` is the
  ## deliberate pre-447 form. Re-validated here, before any wire bytes, so a half-set
  ## or later-mutated pair is refused instead of silently unfenced.
  # A commit and a synchronous beginTransaction can open the next txn while this
  # awaits, and the state alone reads tsInTransaction again.
  let txnSeq = p.txnSeq
  validateConsumerGroupMetadata(groupMetadata)
  # Before the awaits too, so a call outside a txn fails as such, not as a replaced txn.
  if p.txnState != tsInTransaction:
    raise newException(KafkaError, "must be in transaction to send offsets")

  var locked = false
  try:
    # Resolve a deferred epoch bump before txnLock (the single-flight gate must not
    # run inside it) so AddOffsetsToTxn / TxnOffsetCommit carry the current epoch.
    await p.ensureEpochBumped()
    await p.txnLock.acquire()
    locked = true
    if p.txnState != tsInTransaction:
      raise newException(KafkaError, "must be in transaction to send offsets")
    if p.txnSeq != txnSeq:
      raise newException(
        KafkaError,
        "transaction changed during sendOffsetsToTransaction (a later transaction is open)",
      )

    let addReq = AddOffsetsToTxnRequest(
      transactionalId: p.transactionalId.get,
      producerId: p.producerId,
      producerEpoch: p.producerEpoch,
      groupId: groupMetadata.groupId,
    )
    let addResp =
      withTxnCoordinatorRetry(p, await p.txnCoordinator.addOffsetsToTxn(addReq))
    if addResp.errorCode != keNone:
      raise newKafkaApiError(
        addResp.errorCode,
        "AddOffsetsToTxn failed: " & kafkaErrorName(addResp.errorCode),
      )
    # AddOffsetsToTxn moved the txn to `Ongoing`: a later commit/abort must send a
    # real EndTxn even if no records were produced (offsets-only EOS).
    p.txnStarted = true

    # committedLeaderEpoch from cached metadata (v2+; -1 when unknown) so a commit
    # against a stale leader view fails FENCED_LEADER_EPOCH, not unfenced.
    var topicReqs = initTable[string, seq[TxnOffsetCommitPartitionReq]]()
    for tp, offset in offsets:
      let leaderInfo = p.cluster.getPartitionLeader(tp)
      let leaderEpoch = if leaderInfo.isSome: leaderInfo.get.leaderEpoch else: -1'i32
      topicReqs.mgetOrPut(tp.topic, @[]).add(
        TxnOffsetCommitPartitionReq(
          partition: tp.partition,
          offset: offset,
          committedLeaderEpoch: leaderEpoch,
          metadata: none(string),
        )
      )

    var commitTopics: seq[TxnOffsetCommitTopicReq]
    for topic, parts in topicReqs:
      commitTopics.add(TxnOffsetCommitTopicReq(topic: topic, partitions: parts))

    let commitReq = TxnOffsetCommitRequest(
      transactionalId: p.transactionalId.get,
      groupId: groupMetadata.groupId,
      producerId: p.producerId,
      producerEpoch: p.producerEpoch,
      generationId: groupMetadata.generationId,
      memberId: groupMetadata.memberId,
      groupInstanceId: groupMetadata.groupInstanceId,
      topics: commitTopics,
    )

    # TxnOffsetCommit targets the GROUP coordinator, so `rediscover` invalidates the
    # GROUP mapping (re-resolved by the call's `coordinatorBroker` next attempt)
    # rather than `refreshTxnCoordinator`; a group-coordinator failover mid-EOS need
    # not force the caller to abort.
    let commitResp = runCoordinatorRpc(
      rpcCtx =
        RetryContext(isTransactional: true, isIdempotent: false, isCoordinatorRpc: true),
      maxAttempts = txnCoordMaxAttempts,
      backoffBaseMs = txnCoordBackoffBaseMs,
      rediscover = p.cluster.invalidateCoordinator(groupMetadata.groupId),
      errorCodeOf = firstTxnErrorCode,
      rpcCall = await p.resolveAndTxnOffsetCommit(groupMetadata.groupId, commitReq),
    )
    let commitErr = firstTxnErrorCode(commitResp)
    if commitErr != keNone:
      raise newKafkaApiError(
        commitErr, "TxnOffsetCommit failed: " & kafkaErrorName(commitErr)
      )
    # Emit only after both RPCs succeed: refused sends and API failures raise above,
    # and API errors already emit producer.txn.error / fatal.
    emitMetric(p.metricsHook, meProducerTxnSendOffsets, noTags, 1)
  except KafkaError as offsetsErr:
    # Poison before re-raising so a later commitTransaction is refused by the
    # post-flush tsError/tsFatal guard, instead of committing offsets that never
    # reached the coordinator (EOS violation). An API error picks tsFatal vs tsError
    # by class; a transport error is abortable. The guard keeps a wrong-state
    # rejection from poisoning an unrelated committing/aborting txn, or a later one.
    if p.txnSeq == txnSeq and p.txnState == tsInTransaction:
      if offsetsErr of KafkaApiError:
        p.transitionToError((ref KafkaApiError)(offsetsErr).errorCode)
      else:
        if not p.tryTransition(tsError):
          warnTransitionFailed(p, tsError, "sendOffsetsToTransaction transport error")
        # tryTransition emits no producer.txn.error; this is the commit.error
        # analogue for a silent abortable poison.
        emitMetric(
          p.metricsHook,
          meProducerTxnSendOffsetsError,
          txnErrorTags(kafkaErrorTag(offsetsErr), $errorClassToCstring(ecUnknown)),
          1,
        )
    raise
  except CancelledError:
    # A cancel before the offsets committed, while waiting for the bump or txnLock
    # included, would let a later commit EndTxn(commit) a txn missing them; abortable.
    if p.txnSeq == txnSeq and p.txnState == tsInTransaction:
      if not p.tryTransition(tsError):
        warnTransitionFailed(p, tsError, "sendOffsetsToTransaction cancellation")
      emitMetric(
        p.metricsHook,
        meProducerTxnSendOffsetsError,
        txnErrorTags("CANCELLED", $errorClassToCstring(ecUnknown)),
        1,
      )
    raise
  finally:
    if locked:
      p.txnLock.releaseSafely("producer.sendOffsetsToTransaction")

proc sendOffsetsToTransaction*(
    p: KafkaProducer, offsets: Table[TopicPartition, int64], groupId: string
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Legacy overload: commit offsets without consumer-group fencing, sending the
  ## KIP-447 sentinels (generationId = -1, memberId = "") so the broker behaves as
  ## for a pre-447 client. Prefer the `ConsumerGroupMetadata` overload with
  ## `consumer.groupMetadata()` for exactly-once fencing. `offsets` holds the next
  ## offset to read (last processed + 1), as in that overload.
  await p.sendOffsetsToTransaction(offsets, initUnfencedConsumerGroupMetadata(groupId))
