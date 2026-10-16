## Logging and metrics.
##
## Diagnostics go through `std/logging`, and the client never installs a
## handler: `std/logging` starts with none, so out of the box every line is
## discarded and enabling logs is the application's job. `lvlInfo` carries the
## connection and membership lifecycle and `lvlDebug` adds the per-attempt detail
## — each metadata candidate that failed and why, stale leader epochs, coordinator
## cache invalidations and lookups. `lvlWarn` also carries configuration
## advisories and per-record recovery (a reset offset, a corrupt batch), while
## `lvlError` carries the background loops that gave up.
##
## `MetricsHook` receives every internal event (`producer.send.retry`, ...) with
## its tags and value; a nil hook makes each emit a no-op. Pass one to
## `newCluster` and `newKafkaProducer`, and set `ConsumerConfig.metricsHook` for a
## consumer; a hook passed to the Cluster is not inherited by the handles built
## from it.
##
## Prerequisites: a broker at `KAFKA_BROKERS` (default `localhost:9092`) and the
## topic named by `KAFKA_TOPIC` (default `orders`).
##
## Run from the repository root:
##
##   nim c -r examples/observability.nim

import std/[logging, os, sequtils, strutils]

import pkg/chronos
import pkg/async_kafka

# Logs on stderr, so they do not interleave with the metric lines below.
addHandler(newConsoleLogger(fmtStr = "$levelname $datetime $msg", useStderr = true))
setLogFilter(lvlInfo)

proc onMetric(
    event: cstring, tags: openArray[(cstring, cstring)], value: int64
) {.gcsafe, raises: [].} =
  # Runs inline on the client's path, so a real hook forwards to the metrics
  # client instead of printing. Tags are cstring views: copy (`$`) anything kept
  # past the call.
  var line = $event & " " & $value
  for (k, v) in tags:
    line.add " " & $k & "=" & $v
  echo line

proc main() {.async.} =
  let brokers = getEnv("KAFKA_BROKERS", "localhost:9092")
    .split(',')
    .mapIt(it.strip())
    .filterIt(it.len > 0)
  let topic = getEnv("KAFKA_TOPIC", "orders")

  let cluster = newCluster(brokers, metricsHook = onMetric)
  let producer = newKafkaProducer(cluster, metricsHook = onMetric)
  let produced = await producer.send(topic = topic, value = toBytes("hello kafka"))
  echo "produced to ", produced.partition, " at offset ", produced.offset

  await producer.close()
  await cluster.close()

try:
  waitFor main()
except KafkaError as e:
  echo "kafka error: ", e.msg
  quit(QuitFailure)
