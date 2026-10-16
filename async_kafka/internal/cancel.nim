## The one place where a teardown failure is allowed to disappear.
##
## `CancelledError` reaching a shutdown step is expected: `close()` must release what
## it can even when the caller cancels it. An absorbed failure is still why a resource
## was not released on the normal path, so it must leave a trace — otherwise "close()
## ran to completion" and "close() gave up halfway" are indistinguishable from outside
## the library.
##
## Absorbed teardown steps route through `absorbDuringTeardown` with compile-time site
## and reason: the site keeps the step greppable and the reason cannot rot into an
## unread runtime string. The site is a location (`producer::close`), not a metric
## event name.
##
## Not a place to report failures: anything unexpected belongs at the call site as
## `warn` + a metric; this covers an ordinary give-up (a timed-out drain, closed peer).

import safe_logging

template absorbDuringTeardown*(where: static[string], why: static[string]) =
  ## Record a failure or cancellation that teardown deliberately absorbs.
  when where == "" or why == "":
    {.error: "absorbDuringTeardown requires a non-empty site and reason".}
  else:
    safe_logging.debug("absorbed during teardown at " & where & ": " & why)

template absorbDuringTeardown*(
    where: static[string], why: static[string], detail: string
) =
  ## Overload for a site whose identifier is only known at runtime (a socket
  ## option number, a connection id). Site and reason stay compile-time constants,
  ## so the entry point still fails when either is missing.
  when where == "" or why == "":
    {.error: "absorbDuringTeardown requires a non-empty site and reason".}
  else:
    safe_logging.debug(
      "absorbed during teardown at " & where & ": " & why & " (" & detail & ")"
    )
