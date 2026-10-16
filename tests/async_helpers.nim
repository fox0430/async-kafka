## Shared async test utilities.

import std/options

import chronos

import async_kafka/internal/metrics

export chronos

type
  MetricEvent* = tuple[event: string, tags: seq[(string, string)], value: int64]
    ## One captured `MetricsHook` call. Tags are copied out of the cstring
    ## views the hook borrows, which the `MetricsHook` contract ends at the
    ## call's return.
  MetricSink* = ref object
    events*: seq[MetricEvent]
      ## Append-only capture, safe to read after the code under test settles.

proc makeMetricHook*(sink: MetricSink): MetricsHook =
  ## A `MetricsHook` that records into `sink` instead of exporting. Pass it to
  ## any `metricsHook` parameter (cluster, fetcher, consumer config) to assert
  ## which events a path emits and with which tags.
  result = proc(
      event: cstring, tags: openArray[(cstring, cstring)], value: int64
  ) {.gcsafe, raises: [].} =
    var copied: seq[(string, string)]
    for t in tags:
      copied.add(($t[0], $t[1]))
    sink.events.add((event: $event, tags: copied, value: value))

proc findMetricEvent*(sink: MetricSink, name: string): Option[MetricEvent] =
  ## First captured event with this name, or none. Event names are stable dotted
  ## literals, so a test asserts on the exact name.
  for ev in sink.events:
    if ev.event == name:
      return some(ev)
  none(MetricEvent)

proc findMetricEvents*(sink: MetricSink, name: string): seq[MetricEvent] =
  ## Every captured event with this name, for paths that emit more than once.
  for ev in sink.events:
    if ev.event == name:
      result.add(ev)

proc metricTagValue*(ev: MetricEvent, key: string): Option[string] =
  ## A tag's value by key, or none when the event did not carry that dimension.
  for (k, v) in ev.tags:
    if k == key:
      return some(v)
  none(string)

template awaitDrain*(fut: untyped, timeout: untyped) =
  ## Wait for a background `{.async: (raises: []).}` handler to finish, bounded
  ## by `timeout`, swallowing a timeout or cancellation. Such futures can never
  ## fail, so chronos' `wait` trips a spurious "no exceptions possible" [User]
  ## warning on its statically-dead `.failed`/`.error` branch; suppress it here.
  try:
    {.push warning[User]: off.}
    await fut.wait(timeout)
    {.pop.}
  except CatchableError:
    discard

template awaitCompleted*(fut: untyped, timeout: untyped): bool =
  ## Wait for a `{.async: (raises: []).}` future, returning true if it finished
  ## within `timeout` and false if it timed out. Like `awaitDrain`, it suppresses
  ## chronos' spurious "no exceptions possible" [User] warning on the dead
  ## `.failed`/`.error` branch, while still surfacing the timeout to the caller.
  var completed = false
  try:
    {.push warning[User]: off.}
    await fut.wait(timeout)
    {.pop.}
    completed = true
  except AsyncTimeoutError:
    completed = false
  completed
