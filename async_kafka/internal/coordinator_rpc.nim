import ../async_backend
import ../errors
import retry, backoff

## The shared retry driver for the coordinator-bound RPCs that use it, whose
## response carries a broker error code. Owns the "send → read code → (moved?
## re-resolve : retriable? wait : surface) → back off → retry" loop; the *decision*
## is delegated to `retry.classifyCode` so every coordinator RPC shares one
## classification.
##
## The call site still owns the response type, the error-code extractor, the
## rediscover side effect (txn vs group coordinator differ), the budget and backoff
## base, and all terminal handling. The driver never raises an API error itself: on a
## terminal code or an exhausted budget it yields the LAST response unchanged so the
## caller's `if resp.errorCode != keNone` handling fires (raise / poison the txn). A
## transport failure past the budget is re-raised (nothing to yield); a non-transport
## exception (e.g. KafkaClosedError) propagates unchanged, never retried.

template runCoordinatorRpc*(
    rpcCtx: RetryContext,
    maxAttempts: int,
    backoffBaseMs: int32,
    rediscover: untyped,
    errorCodeOf: untyped,
    rpcCall: untyped,
): untyped =
  ## Evaluate `rpcCall` — an awaited coordinator RPC expression, re-evaluated every
  ## attempt so a `rediscover` that re-points the cached handle takes effect on the
  ## next send — with bounded retries:
  ##  * `errorCodeOf(resp) == keNone`: success, produce the response.
  ##  * `raRediscover` (NOT_COORDINATOR / GROUP_COORDINATOR_NOT_AVAILABLE) or a
  ##    transport failure (dropped / timed out / malformed frame, `retry.isTransport`):
  ##    run `rediscover` (invalidate + re-resolve), back off, retry.
  ##  * `raRetrySame` (COORDINATOR_LOAD_IN_PROGRESS, CONCURRENT_TRANSACTIONS, …):
  ##    back off and retry the same coordinator.
  ##  * anything terminal, or an exhausted budget: produce the last response so the
  ##    caller raises / poisons the txn; a transport drop past the budget re-raises.
  ##
  ## `raDuplicate` / `raResetProducerId` / `raResetOffset` require recovery side
  ## effects this driver does not own (idempotence dedupe, KIP-360 PID reset, offset
  ## reset), so they are treated as terminal, not silently retried.
  var crResp: typeof(rpcCall)
  var crAttempt = 0
  while true:
    inc crAttempt
    var crAction = raProceed
    try:
      crResp = rpcCall
      let crCode = errorCodeOf(crResp)
      if crCode != keNone:
        crAction = classifyCode(crCode, rpcCtx)
    except KafkaError as e:
      # A transport-level failure (connection dropped / timed out / malformed frame)
      # is treated as a move and re-resolved, unless the budget is spent (nothing
      # left to yield → raise). Anything non-transport is re-raised unchanged: API
      # errors normally travel in the response body, and e.g. KafkaClosedError must
      # surface instead of burning retries against a closing producer.
      if not isTransport(inspectError(e, rpcCtx)):
        raise
      if crAttempt >= maxAttempts:
        raise
      crAction = raRediscover
    if crAction == raProceed or crAction == raTerminal or crAttempt >= maxAttempts:
      break
    case crAction
    of raRediscover:
      rediscover
    of raRetrySame:
      discard
    of raProceed, raTerminal:
      # Handled by the break above; here only to keep the case exhaustive.
      break
    of raDuplicate, raResetProducerId, raResetOffset:
      # Unsupported by this driver (see the contract note above): yield the last
      # response as terminal so the caller's errorCode handling fires, rather than
      # backoff-retrying an action whose recovery this loop cannot perform.
      break
    await sleepAsync(milliseconds(int(jitteredBackoff(backoffBaseMs, crAttempt))))
  crResp
