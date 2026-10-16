## Async backend configuration. Exports the chronos async framework, selected at
## compile time with `-d:asyncBackend=chronos`.
##
## Re-raising under `{.async.}`: a bare `raise` in an `except` handler re-raises
## the in-flight exception, and an `await` in that handler can outlive it. Once a
## handler nested in the same body has run, a later `await` at the handler's own
## level leaves the bare `raise` with nothing to re-raise, which surfaces as a
## `ReraiseDefect` at run time. Handlers that await capture the exception
## (`except KafkaError as e:`) and `raise e`.

const asyncBackend {.strdefine.} = "chronos"

const hasChronos* = asyncBackend == "chronos"
  ## `true` when the chronos backend is selected.

when hasChronos:
  import internal/safe_logging
  import chronos
  import chronos/streams/[asyncstream, tlsstream]
  export chronos, asyncstream, tlsstream

  proc sleepMsAsync*(ms: int): Future[void] =
    ## Sleep for `ms` milliseconds.
    sleepAsync(milliseconds(ms))

  proc cancelTimer*(fut: Future[void]) =
    ## Cancel a pending timer future to prevent future tracking warnings.
    if not fut.finished():
      fut.cancelSoon()

  proc releaseSafely*(lock: AsyncLock, site: string) {.raises: [].} =
    ## Release `lock`, swallowing the "released without a matching acquire" error so
    ## it is safe from `defer`/`finally` and detached cleanup that must not raise. It
    ## only catches a violated acquire/release pairing, which is a caller bug logged
    ## at warn level; `site` is a required, stable `module.operation` label with no
    ## default, so a warn identifies which release site broke its acquire/release
    ## pairing.
    ##
    ## The caller MUST hold the lock. Chronos' `AsyncLock` tracks `locked`/`acquired`
    ## globally, not per coroutine: releasing a lock another coroutine holds succeeds
    ## silently and does not reach the catch branch. When the acquire can fail
    ## (cancellation), guard this call with the acquire outcome.
    try:
      lock.release()
    except AsyncLockError:
      warn "AsyncLock.release called without matching acquire at ", site

  proc releaseSafely*(sem: AsyncSemaphore, site: string) {.raises: [].} =
    ## `AsyncSemaphore` counterpart to the `AsyncLock` overload above; `site` is
    ## the same `module.operation` caller label.
    try:
      sem.release()
    except AsyncSemaphoreError:
      warn "AsyncSemaphore.release called without matching acquire at ", site

else:
  {.fatal: "Unknown asyncBackend. Use -d:asyncBackend=chronos".}
