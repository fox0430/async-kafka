import std/unittest

import async_kafka/errors
import async_kafka/internal/[retry, backoff]

proc ctx(idem, txn: bool, coord = false, fetch = false): RetryContext =
  RetryContext(
    isTransactional: txn,
    isIdempotent: idem,
    isCoordinatorRpc: coord,
    isFetchPartition: fetch,
  )

proc apiFacts(code: KafkaErrorCode, c: RetryContext): ErrorFacts =
  ## Build ErrorFacts the way the loops will — via inspectError on a real
  ## KafkaApiError so the context-aware `cls` is exercised end to end.
  inspectError(newKafkaApiError(code, "test"), c)

suite "retry.inspectError":
  test "API error exposes code, context-aware class, no transport flags":
    let f = apiFacts(keNotLeaderOrFollower, ctx(idem = true, txn = false))
    check f.isApi
    check f.apiCode == keNotLeaderOrFollower
    check f.cls == ecRetriable
    check not f.isConnLevel
    check not f.isTimeout
    check not f.isProtocol
    check not f.isTransport

  test "class is resolved with transactional context":
    # INVALID_PRODUCER_EPOCH (47): abortable in a txn, producer-id-reset for an
    # idempotent-only producer.
    check apiFacts(keInvalidProducerEpoch, ctx(true, txn = true)).cls == ecAbortable
    check apiFacts(keInvalidProducerEpoch, ctx(true, txn = false)).cls ==
      ecNeedsProducerIdReset
    # INVALID_RECORD (87): abortable in a txn, fatal otherwise.
    check apiFacts(keInvalidRecord, ctx(true, txn = true)).cls == ecAbortable
    check apiFacts(keInvalidRecord, ctx(true, txn = false)).cls == ecFatal
    # OUT_OF_ORDER_SEQUENCE_NUMBER (45) / UNKNOWN_PRODUCER_ID (59):
    # producer-id-reset for an idempotent-only producer, abortable in a txn
    # (Java TransactionManager parity — never resettable mid-transaction).
    check apiFacts(keOutOfOrderSequenceNumber, ctx(true, txn = true)).cls == ecAbortable
    check apiFacts(keOutOfOrderSequenceNumber, ctx(true, txn = false)).cls ==
      ecNeedsProducerIdReset
    check apiFacts(keUnknownProducerId, ctx(true, txn = true)).cls == ecAbortable
    check apiFacts(keUnknownProducerId, ctx(true, txn = false)).cls ==
      ecNeedsProducerIdReset

  test "connection error is transport, not API":
    let f = inspectError(
      (ref KafkaConnectionError)(newException(KafkaConnectionError, "x")),
      ctx(true, false),
    )
    check not f.isApi
    check f.apiCode == keNone
    check f.cls == ecUnknown
    check f.isConnLevel
    check f.isTransport

  test "timeout and protocol errors are transport":
    check inspectError(
      (ref KafkaTimeoutError)(newException(KafkaTimeoutError, "x")), ctx(true, false)
    ).isTimeout
    check inspectError(
      (ref KafkaProtocolError)(newException(KafkaProtocolError, "x")), ctx(true, false)
    ).isProtocol

  test "a plain KafkaError is neither API nor transport":
    let f =
      inspectError((ref KafkaError)(newException(KafkaError, "x")), ctx(true, false))
    check not f.isApi
    check not f.isTransport

suite "retry.classify — idempotent, non-transactional producer":
  let c = ctx(idem = true, txn = false)

  test "DUPLICATE_SEQUENCE_NUMBER -> raDuplicate":
    check classify(apiFacts(keDuplicateSequenceNumber, c), c) == raDuplicate

  test "OUT_OF_ORDER / UNKNOWN_PRODUCER_ID -> raResetProducerId":
    check classify(apiFacts(keOutOfOrderSequenceNumber, c), c) == raResetProducerId
    check classify(apiFacts(keUnknownProducerId, c), c) == raResetProducerId

  test "INVALID_PRODUCER_EPOCH -> raResetProducerId (fenced epoch, no txn to abort)":
    check classify(apiFacts(keInvalidProducerEpoch, c), c) == raResetProducerId

  test "INVALID_PRODUCER_ID_MAPPING -> raRetrySame (retriable for idempotent-only)":
    check classify(apiFacts(keInvalidProducerIdMapping, c), c) == raRetrySame

  test "retriable code -> raRetrySame":
    check classify(apiFacts(keNotLeaderOrFollower, c), c) == raRetrySame

  test "fatal / non-retriable codes -> raTerminal":
    check classify(apiFacts(keMessageTooLarge, c), c) == raTerminal
    check classify(apiFacts(keInvalidRecord, c), c) == raTerminal

suite "retry.classify — transactional producer":
  let c = ctx(idem = true, txn = true)

  test "abortable codes -> raTerminal (caller poisons the txn)":
    check classify(apiFacts(keInvalidProducerEpoch, c), c) == raTerminal
    check classify(apiFacts(keInvalidProducerIdMapping, c), c) == raTerminal
    check classify(apiFacts(keInvalidRecord, c), c) == raTerminal

  test "OUT_OF_ORDER -> raTerminal (cannot reset producer-id mid-txn)":
    check classify(apiFacts(keOutOfOrderSequenceNumber, c), c) == raTerminal

  test "CONCURRENT_TRANSACTIONS -> raRetrySame":
    check classify(apiFacts(keConcurrentTransactions, c), c) == raRetrySame

  test "DUPLICATE_SEQUENCE_NUMBER -> raDuplicate (dedup still applies in a txn)":
    check classify(apiFacts(keDuplicateSequenceNumber, c), c) == raDuplicate

suite "retry.classify — non-idempotent producer":
  let c = ctx(idem = false, txn = false)

  test "retriable code -> raRetrySame":
    check classify(apiFacts(keNotLeaderOrFollower, c), c) == raRetrySame

  test "idempotent-only recovery codes are terminal without idempotence":
    check classify(apiFacts(keDuplicateSequenceNumber, c), c) == raTerminal
    check classify(apiFacts(keOutOfOrderSequenceNumber, c), c) == raTerminal

suite "retry.classify — transport errors":
  test "connection / timeout / protocol errors are always raRetrySame":
    # isCoordinatorRpc is off for these callers; on a coordinator RPC the same
    # transport failures map to raRediscover instead.
    let c = ctx(true, false)
    let conn = inspectError(
      (ref KafkaConnectionError)(newException(KafkaConnectionError, "x")), c
    )
    let tmo =
      inspectError((ref KafkaTimeoutError)(newException(KafkaTimeoutError, "x")), c)
    let proto =
      inspectError((ref KafkaProtocolError)(newException(KafkaProtocolError, "x")), c)
    check classify(conn, c) == raRetrySame
    check classify(tmo, c) == raRetrySame
    check classify(proto, c) == raRetrySame

  test "a plain non-transport KafkaError is raTerminal":
    let c = ctx(true, false)
    let f = inspectError((ref KafkaError)(newException(KafkaError, "x")), c)
    check classify(f, c) == raTerminal

suite "retry.isCoordinatorMoved":
  test "NOT_COORDINATOR and GROUP_COORDINATOR_NOT_AVAILABLE are moved":
    check isCoordinatorMoved(keNotCoordinator)
    check isCoordinatorMoved(keGroupCoordinatorNotAvailable)

  test "COORDINATOR_LOAD_IN_PROGRESS is NOT moved (retry same coordinator)":
    check not isCoordinatorMoved(keCoordinatorLoadInProgress)

  test "success and unrelated codes are not moved":
    check not isCoordinatorMoved(keNone)
    check not isCoordinatorMoved(keNotLeaderOrFollower)

suite "retry.classify — coordinator RPC":
  let c = ctx(idem = false, txn = false, coord = true)

  test "coordinator moved -> raRediscover":
    check classify(apiFacts(keNotCoordinator, c), c) == raRediscover
    check classify(apiFacts(keGroupCoordinatorNotAvailable, c), c) == raRediscover

  test "COORDINATOR_LOAD_IN_PROGRESS -> raRetrySame (same coordinator)":
    check classify(apiFacts(keCoordinatorLoadInProgress, c), c) == raRetrySame

  test "other retriable codes -> raRetrySame":
    check classify(apiFacts(keNotLeaderOrFollower, c), c) == raRetrySame

  test "fatal codes -> raTerminal":
    check classify(apiFacts(keMessageTooLarge, c), c) == raTerminal

  test "a dropped transport is treated as a coordinator move":
    let conn = inspectError(
      (ref KafkaConnectionError)(newException(KafkaConnectionError, "x")), c
    )
    let tmo =
      inspectError((ref KafkaTimeoutError)(newException(KafkaTimeoutError, "x")), c)
    let proto =
      inspectError((ref KafkaProtocolError)(newException(KafkaProtocolError, "x")), c)
    check classify(conn, c) == raRediscover
    check classify(tmo, c) == raRediscover
    check classify(proto, c) == raRediscover

  test "transactional coordinator context keeps the moved decision":
    let tc = ctx(idem = true, txn = true, coord = true)
    check classify(apiFacts(keNotCoordinator, tc), tc) == raRediscover
    check classify(apiFacts(keCoordinatorLoadInProgress, tc), tc) == raRetrySame

suite "retry.classify — coordinator codes are inert without isCoordinatorRpc":
  # The producer send loop passes isCoordinatorRpc = false; a coordinator-moved
  # code must keep its plain ecRetriable behaviour there, never raRediscover.
  let c = ctx(idem = true, txn = false, coord = false)

  test "NOT_COORDINATOR -> raRetrySame for a non-coordinator caller":
    check classify(apiFacts(keNotCoordinator, c), c) == raRetrySame
    check classify(apiFacts(keGroupCoordinatorNotAvailable, c), c) == raRetrySame

suite "retry.classify — consumer fetch partition":
  let fc = ctx(idem = false, txn = false, fetch = true)

  test "OFFSET_OUT_OF_RANGE -> raResetOffset":
    check classify(apiFacts(keOffsetOutOfRange, fc), fc) == raResetOffset
    check classifyCode(keOffsetOutOfRange, fc) == raResetOffset

  test "retriable code -> raRetrySame (skip this round)":
    check classify(apiFacts(keNotLeaderOrFollower, fc), fc) == raRetrySame
    check classify(apiFacts(keFencedLeaderEpoch, fc), fc) == raRetrySame

  test "rebalance-required -> raRetrySame (heartbeat drives rejoin)":
    check classify(apiFacts(keIllegalGeneration, fc), fc) == raRetrySame
    check classify(apiFacts(keUnknownMemberId, fc), fc) == raRetrySame
    check classify(apiFacts(keRebalanceInProgress, fc), fc) == raRetrySame

  test "fatal codes -> raTerminal":
    check classify(apiFacts(keTopicAuthorizationFailed, fc), fc) == raTerminal
    check classify(apiFacts(keCorruptMessage, fc), fc) == raRetrySame
      # CORRUPT_MESSAGE is retriable per Java parity.

  test "OFFSET_OUT_OF_RANGE without isFetchPartition stays raTerminal":
    # Other callers (coordinator RPCs, producer sends) never see this code, but
    # if one ever does the classifier must not silently apply reset semantics.
    let nfc = ctx(idem = false, txn = false, fetch = false)
    check classify(apiFacts(keOffsetOutOfRange, nfc), nfc) == raTerminal
    let cc = ctx(idem = false, txn = false, coord = true, fetch = false)
    check classify(apiFacts(keOffsetOutOfRange, cc), cc) == raTerminal

  test "rebalance-required outside fetch context stays raTerminal":
    let cc = ctx(idem = false, txn = false, coord = true, fetch = false)
    check classify(apiFacts(keIllegalGeneration, cc), cc) == raTerminal

suite "retry.classifyCode — response-code path":
  let c = ctx(idem = false, txn = false, coord = true)

  test "matches the exception path for coordinator codes":
    check classifyCode(keNotCoordinator, c) == raRediscover
    check classifyCode(keGroupCoordinatorNotAvailable, c) == raRediscover
    check classifyCode(keCoordinatorLoadInProgress, c) == raRetrySame
    check classifyCode(keNotLeaderOrFollower, c) == raRetrySame
    check classifyCode(keMessageTooLarge, c) == raTerminal

  test "context is honoured (transactional reclassification)":
    let tc = ctx(idem = true, txn = true, coord = true)
    # INVALID_RECORD (87): abortable in a txn -> not retriable -> terminal.
    check classifyCode(keInvalidRecord, tc) == raTerminal

suite "retry.siblingResetLikely":
  test "encoded epoch older than current -> likely (sibling bumped resetEpoch)":
    check siblingResetLikely(3, 4)
    check siblingResetLikely(0, 1)

  test "encoded == current -> not likely (no bump observed)":
    check not siblingResetLikely(5, 5)
    check not siblingResetLikely(0, 0)

  test "encoded > current is not likely (should never happen; be defensive)":
    check not siblingResetLikely(2, 1)

suite "retry.retryBudgetExhausted":
  test "next attempt within cap and no deadline is fine":
    check not retryBudgetExhausted(1, 3, pastDeadline = false)
    check not retryBudgetExhausted(3, 3, pastDeadline = false)

  test "next attempt exceeding cap exhausts the budget":
    check retryBudgetExhausted(4, 3, pastDeadline = false)

  test "past deadline exhausts the budget even if the attempt cap is huge":
    check retryBudgetExhausted(1, int32.high, pastDeadline = true)

  test "int32.high cap with pastDeadline false never trips on the count alone":
    check not retryBudgetExhausted(int32.high, int32.high, pastDeadline = false)

suite "retry.retryDelay":
  test "no throttle equals a plain jittered backoff (>= 1ms)":
    for attempt in 0 .. 6:
      check retryDelay(100'i32, attempt) >= 1'i32

  test "a larger throttle dominates the backoff":
    # jitteredBackoff(10, 1) is at most ~15ms; a 100000ms throttle must win.
    check retryDelay(10'i32, 1, 100000'i32) == 100000'i32

  test "a zero throttle never lowers the backoff below 1ms":
    check retryDelay(1'i32, 1, 0'i32) >= 1'i32

  test "result is always at least the throttle":
    for attempt in 1 .. 5:
      check retryDelay(50'i32, attempt, 5000'i32) >= 5000'i32
