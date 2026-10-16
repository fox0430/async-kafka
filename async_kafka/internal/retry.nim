import ../errors

## Single source of truth for "given this error, what should the retry loop do".
##
## Pure and centralized so call sites cannot drift, but control flow stays with each
## loop: it owns the side effects (sequence allocation, producer-id reset, coordinator
## re-resolution, txn poisoning) and the retry budget. `classify` ignores attempts
## left — the loop downgrades a would-be retry to `raTerminal` when the budget is spent.

type
  RetryAction* = enum
    ## The action a retry loop should take for a single failed attempt; a loop
    ## only ever observes the subset it can produce.
    raProceed ## driver sentinel for "no error seen"; classify never returns it
    raDuplicate ## idempotent producer: broker already has the record
    raRetrySame ## transient — back off and resend to the same target
    raRediscover ## coordinator moved — re-resolve, retry
    raResetProducerId ## idempotent producer needs a KIP-360 reset
    raResetOffset ## consumer fetch: OFFSET_OUT_OF_RANGE — apply reset policy
    raTerminal ## stop retrying; the caller raises on the yielded failure

  RetryContext* = object
    ## Caller-side context that makes an error code's action unambiguous.
    isTransactional*: bool
    isIdempotent*: bool
    isCoordinatorRpc*: bool
    isFetchPartition*: bool
      ## Per-partition code from a FetchResponse: enables
      ## `keOffsetOutOfRange -> raResetOffset` and `ecRebalanceRequired -> raRetrySame`
      ## (the skipped round rejoins via heartbeat).

  ErrorFacts* = object
    ## Side-effect-free facts about a failure, built from a thrown `ref KafkaError`
    ## (`inspectError`) or from a response code (`factsForCode`), so call sites can
    ## build `error_code`/`error_class` labels without re-inspecting the exception.
    isApi*: bool ## the error is a `KafkaApiError` and carries a broker `apiCode`
    apiCode*: KafkaErrorCode ## the broker error code (keNone when `not isApi`)
    cls*: ErrorClass
      ## `errorClassForContext(apiCode, ctx.isTransactional)`; `ecUnknown` when `not isApi`
    isConnLevel*: bool ## a `KafkaConnectionError` (transport dropped)
    isTimeout*: bool ## a `KafkaTimeoutError` (request timed out)
    isProtocol*: bool ## a `KafkaProtocolError` (malformed/unexpected frame)

proc inspectError*(e: ref KafkaError, ctx: RetryContext): ErrorFacts {.raises: [].} =
  ## Extract the classification-relevant facts. Classification is context-aware
  ## (`ctx.isTransactional`), so INVALID_PRODUCER_EPOCH (47) /
  ## INVALID_PRODUCER_ID_MAPPING (49) / INVALID_RECORD (87) get the right treatment.
  result.isApi = e of KafkaApiError
  if result.isApi:
    result.apiCode = (ref KafkaApiError)(e).errorCode
    result.cls = errorClassForContext(result.apiCode, ctx.isTransactional)
  else:
    result.apiCode = keNone
    result.cls = ecUnknown
  result.isConnLevel = e of KafkaConnectionError
  result.isTimeout = e of KafkaTimeoutError
  result.isProtocol = e of KafkaProtocolError

func isTransport*(facts: ErrorFacts): bool {.inline.} =
  ## A non-API transport failure (connection dropped / timed out / bad frame),
  ## always retriable regardless of error code.
  facts.isConnLevel or facts.isTimeout or facts.isProtocol

func isCoordinatorMoved*(code: KafkaErrorCode): bool {.inline, raises: [].} =
  ## True when this broker is no longer (or not yet) the coordinator for this key,
  ## so the cached mapping must be re-resolved via FindCoordinator rather than
  ## retried against the same broker. NOT_COORDINATOR (16) and
  ## GROUP_COORDINATOR_NOT_AVAILABLE (15) mean moved. COORDINATOR_LOAD_IN_PROGRESS (14)
  ## is deliberately NOT here: that broker IS the coordinator, still loading
  ## its state, so the retry stays on the same target (`ecRetriable` -> resend).
  code == keNotCoordinator or code == keGroupCoordinatorNotAvailable

proc classify*(facts: ErrorFacts, ctx: RetryContext): RetryAction {.raises: [].} =
  ## Pure decision from `(facts, ctx)`, ignoring retry budget.
  if facts.isApi:
    if ctx.isFetchPartition and facts.apiCode == keOffsetOutOfRange:
      return raResetOffset
    if ctx.isIdempotent and facts.cls == ecDuplicate:
      return raDuplicate
    if ctx.isIdempotent and not ctx.isTransactional and
        facts.cls == ecNeedsProducerIdReset:
      return raResetProducerId
    if ctx.isCoordinatorRpc and isCoordinatorMoved(facts.apiCode):
      return raRediscover
    if facts.cls == ecRetriable:
      return raRetrySame
    if ctx.isFetchPartition and facts.cls == ecRebalanceRequired:
      return raRetrySame
    return raTerminal
  if facts.isTransport:
    # A dropped coordinator connection counts as a move: the old handle may no
    # longer be the coordinator, so re-resolve before retrying.
    if ctx.isCoordinatorRpc:
      return raRediscover
    return raRetrySame
  raTerminal

proc factsForCode*(code: KafkaErrorCode, ctx: RetryContext): ErrorFacts {.raises: [].} =
  ## `ErrorFacts` for a non-`keNone` broker error code carried in a RESPONSE body:
  ## the coordinator RPCs must reach the same `classify` decision as the exception
  ## path. Callers handle `keNone` (success) themselves before calling this.
  result.isApi = true
  result.apiCode = code
  result.cls = errorClassForContext(code, ctx.isTransactional)

proc classifyCode*(
    code: KafkaErrorCode, ctx: RetryContext
): RetryAction {.raises: [].} =
  ## Response-code path: classify a non-`keNone` error code read out of a response
  ## body (coordinator RPCs and fetch partition errors). Equivalent to
  ## `classify(factsForCode(code, ctx), ctx)`.
  classify(factsForCode(code, ctx), ctx)

func siblingResetLikely*(encoded, current: int64): bool {.inline, raises: [].} =
  ## True when this batch was encoded under an older PID-reset epoch than the
  ## producer currently holds — a sibling coroutine bumped `resetEpoch` while we
  ## were parked. Callers MUST re-check after `refreshInflightFromQueue`: only when
  ## the refresh confirms `encoded == current` are the frozen bytes safe to retry
  ## (a bumped counter with an un-republished queue means the sibling died midway),
  ## which is why "likely": the boolean alone does not license the retry.
  encoded < current

func retryBudgetExhausted*(
    nextAttempt, maxRetries: int32, pastDeadline: bool
): bool {.inline, raises: [].} =
  ## Would advancing to `nextAttempt` (== current attempt + 1) violate the retry
  ## budget? Folds the `maxRetries` cap and `delivery.timeout.ms` into the one
  ## place that downgrades a would-be retriable failure to terminal.
  pastDeadline or nextAttempt > maxRetries
