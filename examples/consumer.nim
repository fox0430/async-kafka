## Consume a topic and commit offsets by hand.
##
## `autoCommit = false` takes the periodic commit away from the background loop,
## leaving offset management to the application: `commitSync` commits the
## position the caller reached and raises the non-fatal `KafkaCommitSkippedError`
## when a rebalance has invalidated the generation, which is caught and carried
## on from below. `commitAsync` is the fire-and-forget form and reports the same
## outcome to its callback.
##
## `pause` stops the fetch rounds for a partition while the poll loop and the
## group heartbeat keep running, `resume` undoes it, and `seek` moves a
## partition's next fetch position, dropping that partition's buffered records;
## until the next poll, a commit writes the seek target.
##
## Prerequisites: a broker at `KAFKA_BROKERS` (default `localhost:9092`) and the
## topic named by `KAFKA_TOPIC` (default `orders`).
##
## Run from the repository root:
##
##   nim c -r examples/consumer.nim

import std/[options, os, sequtils, strutils]

import pkg/chronos
import pkg/async_kafka

proc onCommit(err: ref KafkaError) {.gcsafe, raises: [].} =
  if err == nil:
    echo "async commit done"
  else:
    echo "async commit failed: ", err.msg

proc main() {.async.} =
  let brokers = getEnv("KAFKA_BROKERS", "localhost:9092")
    .split(',')
    .mapIt(it.strip())
    .filterIt(it.len > 0)
  let topic = getEnv("KAFKA_TOPIC", "orders")

  var config = initConsumerConfig(
    bootstrapBrokers = brokers, groupId = "manual-commit", topics = @[topic]
  )
  config.autoCommit = false
  # Read the topic from the beginning when the group has no committed offset;
  # the default would start at the end and this program would see nothing.
  config.offsetResetPolicy = orpEarliest
  let consumer = newKafkaConsumer(config)

  # `finally` so a failure on the processing path still releases the group; note
  # that close() commits the position reached, as this program does explicitly.
  # `start()` is inside so a join that registered a member before failing still
  # reaches `close()`, which leaves the group and releases the owned Cluster.
  try:
    await consumer.start()

    let records = await consumer.pollBatch(maxRecords = 10, timeout = 2.seconds)
    for rec in records:
      # A record without a value is a tombstone, so its absence is reported too.
      let payload =
        if rec.value.isSome:
          $rec.value.get.len & " byte(s)"
        else:
          "no value"
      echo "got ",
        rec.topic, "[", rec.partition, "] at offset ", rec.offset, ": ", payload

    try:
      await consumer.commitSync()
      echo "committed the position after ", records.len, " record(s)"
    except KafkaCommitSkippedError as e:
      # A rebalance outdates the commit: nothing was committed, and the next poll
      # rejoins the group. Non-fatal, so keep consuming.
      echo "commit skipped: ", e.msg

    if records.len > 0:
      let tp = TopicPartition(topic: records[0].topic, partition: records[0].partition)

      consumer.pause([tp])
      echo tp.topic, "[", tp.partition, "] paused: ", consumer.isPaused(tp)

      consumer.resume([tp])
      echo tp.topic, "[", tp.partition, "] resumed: ", consumer.isPaused(tp)

      # Re-read the batch's first record: a seek drops what is buffered for `tp`
      # and moves its next fetch position back.
      consumer.seek(tp, records[0].offset)
      let again = await consumer.pollBatch(maxRecords = 10, timeout = 2.seconds)
      echo "after seeking back to ", records[0].offset, ": ", again.len, " record(s)"

    # Fire-and-forget: the outcome arrives on the callback, and close() drains a
    # commit still in flight before it releases the group.
    consumer.commitAsync(onCommit)
  finally:
    await consumer.close()

try:
  waitFor main()
except KafkaError as e:
  echo "kafka error: ", e.msg
  quit(QuitFailure)
