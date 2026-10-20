## Containment for user callbacks typed `raises: []` that raise anyway (by casting
## away the raises list): always a callback bug, never a signal. Call sites run the
## callback through `guardCallback` and fall back to what they do without one.
## Reports are rate-limited since some callbacks run per record or metric event.

import std/[monotimes, options, times]

import safe_logging

const callbackFailureReportInterval* = initDuration(minutes = 1)

type CallbackFailureLog* = object
  ## Report state for one callback: its first failure is logged at `warn`, later ones
  ## at most once per `callbackFailureReportInterval` with the count dropped since.
  lastReport: Option[MonoTime]
  suppressed: int

proc report(log: var CallbackFailureLog, now: MonoTime, msg: string) {.raises: [].} =
  # Stamped before logging: a log handler that runs the callback again must find the
  # window closed instead of reporting, and recursing, once more.
  log.lastReport = some(now)
  log.suppressed = 0
  warn msg

proc callbackRaised*(
    log: var CallbackFailureLog, callback: string, e: ref CatchableError, detail: string
) {.raises: [].} =
  ## Report that `callback` raised `e`. Prefer `guardCallback` where its per-call
  ## check is affordable: it also reports the dropped count once the callback recovers.
  let now = getMonoTime()
  if log.lastReport.isSome and now - log.lastReport.get < callbackFailureReportInterval:
    inc log.suppressed
    return
  var msg = callback & " raised " & $e.name & ": " & e.msg
  if detail.len > 0:
    msg.add " (" & detail & ")"
  if log.suppressed > 0:
    msg.add " (" & $log.suppressed & " more failures since the last report)"
  log.report(now, msg)

proc reportSuppressed(log: var CallbackFailureLog, callback: string) {.raises: [].} =
  let now = getMonoTime()
  if now - log.lastReport.get >= callbackFailureReportInterval:
    log.report(
      now, callback & " raised " & $log.suppressed & " more times since the last report"
    )

proc callbackReturned(
    log: var CallbackFailureLog, callback: string
) {.inline, raises: [].} =
  # Reports the dropped count once the interval passes, so a callback that stopped
  # failing does not strand it. Inline: runs on every successful call.
  if log.suppressed > 0:
    log.reportSuppressed(callback)

template guardCallback*(
    log: untyped, callback, detail: string, call: untyped
): ref CatchableError =
  ## Run `call` (a user callback invocation) and return the CatchableError it raised,
  ## or nil. A raise is reported through `log` (a `CallbackFailureLog`) with `detail`,
  ## evaluated only then. A Defect stays fatal.
  var raised: ref CatchableError = nil
  try:
    call
    callbackReturned(log, callback)
  except CatchableError as e:
    callbackRaised(log, callback, e, detail)
    raised = e
  raised
