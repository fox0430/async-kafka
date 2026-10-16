## Choose how the producer batches, compresses, and partitions records.
##
## Batching trades latency for throughput: `lingerMs` holds a partition open so
## later records can share its batch, which is sealed early by
## `maxBatchRecords` / `maxBatchBytes`. `compression` shrinks what goes on the
## wire, and `enableIdempotence` makes delivery retry without reordering a
## partition (it forces `acks = -1` and unbounded retries).
##
## `partitionStrategy` picks the partition for a record sent without one:
## `psRoundRobin` (the default) cycles, `psKeyHash` hashes the key so a key's
## records stay together, and `psSticky` keeps filling one partition while
## `lingerMs` batches. A `customPartitioner` replaces the strategy entirely.
##
## `sendFireAndForget` is the non-awaiting form: the result goes to a callback,
## and `close` drains what is still in flight.
##
## Prerequisites: a broker at `KAFKA_BROKERS` (default `localhost:9092`) and the
## topic named by `KAFKA_TOPIC` (default `orders`), which the partitioning
## sections show best with more than one partition.
##
## Run from the repository root:
##
##   nim c -r examples/producer_settings.nim

import std/[options, os, sequtils, strutils]

import pkg/chronos
import pkg/async_kafka

proc onAck(result: ProduceResult, error: ref KafkaError) {.gcsafe, raises: [].} =
  if error == nil:
    echo "async record acked at offset ", result.offset
  else:
    echo "async record failed: ", error.msg

proc onFirstKeyByte(
    topic: string, key: Option[seq[byte]], partitionCount: int32
): int32 {.gcsafe, raises: [].} =
  ## Custom partitioner: spread on the key's first byte instead of Kafka's
  ## murmur2 hash. It runs on the send path, so it stays pure and cheap, and an
  ## index outside `0 ..< partitionCount` raises before anything is buffered.
  if key.isNone or key.get.len == 0 or partitionCount <= 0:
    return 0
  int32(key.get[0]) mod partitionCount

proc main() {.async.} =
  let brokers = getEnv("KAFKA_BROKERS", "localhost:9092")
    .split(',')
    .mapIt(it.strip())
    .filterIt(it.len > 0)
  let topic = getEnv("KAFKA_TOPIC", "orders")
  let cluster = newCluster(brokers)

  let batched = newKafkaProducer(
    cluster,
    acks = -1,
    enableIdempotence = true,
    lingerMs = 20,
    maxBatchRecords = 100,
    compression = ccGzip, # ccSnappy and ccZstd are linked in; ccLz4 needs liblz4
  )
  let keyed = newKafkaProducer(cluster, partitionStrategy = psKeyHash)
  let custom = newKafkaProducer(cluster, customPartitioner = onFirstKeyByte)
  let fireAndForget = newKafkaProducer(cluster, maxPendingFireAndForget = 16)
  # `finally` so a send failure still releases every handle; the explicit
  # closes below order the shutdown, the `finally` only covers a failure
  # before them (`close()` is single-flight, so a second call is a no-op).
  try:
    for i in 0 ..< 5:
      let produced = await batched.send(topic = topic, value = toBytes("batched-" & $i))
      echo "batched-", i, " -> partition ", produced.partition
    await batched.close()

    # One key, one partition: records sharing a key keep their relative order, and
    # a keyless record falls back to round-robin.
    for i in 0 ..< 3:
      let produced = await keyed.send(
        topic = topic, value = toBytes("keyed-" & $i), key = some(toBytes("order-42"))
      )
      echo "keyed-", i, " -> partition ", produced.partition
    let keyless = await keyed.send(topic = topic, value = toBytes("keyless"))
    echo "keyless -> partition ", keyless.partition
    await keyed.close()

    for i in 0 ..< 3:
      let produced = await custom.send(
        topic = topic, value = toBytes("custom-" & $i), key = some(@[byte(i)])
      )
      echo "custom-", i, " -> partition ", produced.partition
    await custom.close()

    for i in 0 ..< 3:
      await fireAndForget.sendFireAndForget(
        topic = topic, value = toBytes("async-" & $i), callback = onAck
      )
    await fireAndForget.close()

    await cluster.close()
  finally:
    await fireAndForget.close()
    await custom.close()
    await keyed.close()
    await batched.close()
    await cluster.close()

try:
  waitFor main()
except KafkaError as e:
  echo "kafka error: ", e.msg
  quit(QuitFailure)
