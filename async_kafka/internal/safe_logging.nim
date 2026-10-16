## Catchable-error-safe wrappers for client diagnostics: `std/logging` dispatches
## synchronously to user handlers that can raise (e.g. a file handler's `IOError`),
## which must not break an async proc's `raises: []` contract. The wrappers keep the
## same filtering and formatting while isolating handler failures; defects stay fatal.

import std/logging as stdLogging

proc safeLog(
    level: stdLogging.Level, args: varargs[string, `$`]
) {.gcsafe, raises: [].} =
  try:
    stdLogging.log(level, args)
  except:
    discard

proc debug*(args: varargs[string, `$`]) {.gcsafe, raises: [].} =
  safeLog(stdLogging.lvlDebug, args)

proc info*(args: varargs[string, `$`]) {.gcsafe, raises: [].} =
  safeLog(stdLogging.lvlInfo, args)

proc warn*(args: varargs[string, `$`]) {.gcsafe, raises: [].} =
  safeLog(stdLogging.lvlWarn, args)

proc error*(args: varargs[string, `$`]) {.gcsafe, raises: [].} =
  safeLog(stdLogging.lvlError, args)
