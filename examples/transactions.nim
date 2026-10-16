## Exactly-once consume-process-produce: the offsets a batch was read at are
## committed inside the same transaction as the records produced from it, so a
## crash leaves either both or neither visible downstream.
##
## `isolationLevel = ilReadCommitted` keeps the consumer from reading records of
## aborted or still-open transactions.
##
## `autoCommit = false` stops only the periodic commit: a rebalance and `close()`
## still commit the consumed positions outside the transaction, so a batch whose
## records were never produced must not be left to a `close()`. The abort path
## below settles the transaction and leaves the consumer open on purpose.
##
## Prerequisites: a broker at `KAFKA_BROKERS` (default `localhost:9092`) whose
## transaction state topic is ready, and the input and output topics (default
## `orders-in` / `orders-out`) already created.
##
## Run from the repository root:
##
##   nim c -r examples/transactions.nim

import std/[options, os, sequtils, strutils, tables]

import pkg/chronos
import pkg/async_kafka

proc main() {.async.} =
  let brokers = getEnv("KAFKA_BROKERS", "localhost:9092")
    .split(',')
    .mapIt(it.strip())
    .filterIt(it.len > 0)
  let inTopic = getEnv("KAFKA_IN_TOPIC", "orders-in")
  let outTopic = getEnv("KAFKA_OUT_TOPIC", "orders-out")

  var config = initConsumerConfig(
    bootstrapBrokers = brokers, groupId = "orders-processor", topics = @[inTopic]
  )
  config.autoCommit = false # the transaction commits the offsets instead
  config.isolationLevel = ilReadCommitted
  # A group with no committed offset starts at the end of the topic by default;
  # read it from the beginning so a first run sees the records already there.
  config.offsetResetPolicy = orpEarliest
  let consumer = newKafkaConsumer(config)
  await consumer.start()

  let producer = newKafkaProducer(brokers, transactionalId = some("orders-tx"))
  await producer.initTransactions()

  let records = await consumer.pollBatch(maxRecords = 100, timeout = 1.seconds)
  if records.len == 0:
    echo "no records in ", inTopic, " to process"
  else:
    producer.beginTransaction() # synchronous: only local transaction state opens
    try:
      var offsets = initTable[TopicPartition, int64]()
      for rec in records:
        # The key and the headers decide how the output partitions and compacts,
        # so they are forwarded; `send` takes a value rather than an option, so a
        # null (tombstone) value arrives as empty bytes.
        discard await producer.send(
          topic = outTopic,
          value = rec.value.get(@[]),
          key = rec.key,
          headers = rec.headers,
        )
        # The committed offset is the next one to read: last processed + 1.
        offsets[TopicPartition(topic: rec.topic, partition: rec.partition)] =
          rec.offset + 1
      # Fence the offsets against this consumer's live generation (KIP-447) so a
      # rebalanced-away instance cannot commit stale work.
      await producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata())
      await producer.commitTransaction()
    except KafkaError as e:
      # Neither the records produced from this batch nor its offsets may reach
      # the brokers, and the original failure is what surfaces. A fatal producer
      # refuses abort, and close() does not abort one either, so that transaction
      # is left to the coordinator's timeout; the consumer is left open on
      # purpose, since close() would commit the position of a batch that was
      # never produced.
      try:
        await producer.abortTransaction()
      except KafkaError as abortError:
        echo "abort refused (", abortError.msg, "); the transaction will time out"
      await producer.close()
      # Re-raise the captured error: a bare raise after an await loses it.
      raise e
    echo "committed ", records.len, " record(s) to ", outTopic

  await producer.close()
  await consumer.close()

try:
  waitFor main()
except KafkaError as e:
  echo "kafka error: ", e.msg
  quit(QuitFailure)
