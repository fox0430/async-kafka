## Who owns the Cluster, and in which order the handles are closed.
##
## `newCluster(...)` creates a `Cluster`; `newKafkaProducer(cluster, ...)` and
## `newKafkaConsumer(config, cluster)` borrow it and never close it. The
## convenience constructors that take bootstrap addresses create a Cluster and
## own it, so their `close()` releases it. `ownsCluster` reports which case a
## handle is in, so the shutdown order is checkable at run time instead of
## inferred from the constructor that was called.
##
## Run from the repository root (a broker is not contacted: nothing here needs
## a connection to be built or closed):
##
##   nim c -r examples/ownership.nim

import std/[os, sequtils, strutils]

import pkg/chronos
import pkg/async_kafka

proc main() {.async.} =
  let brokers = getEnv("KAFKA_BROKERS", "localhost:9092")
    .split(',')
    .mapIt(it.strip())
    .filterIt(it.len > 0)
  let topic = getEnv("KAFKA_TOPIC", "orders")

  # Borrowed: the handle releases its own state, the caller closes what it
  # created, and last.
  let cluster = newCluster(brokers)
  let borrowed = newKafkaProducer(cluster)
  doAssert not borrowed.ownsCluster
  echo "producer on an injected cluster owns it: ", borrowed.ownsCluster
  await borrowed.close()
  await cluster.close()

  # Owned: the convenience constructor built a Cluster, so close() releases it.
  let owned = newKafkaProducer(brokers)
  doAssert owned.ownsCluster
  echo "producer on a convenience constructor owns it: ", owned.ownsCluster
  await owned.close()

  # The consumer follows the same rule, from either constructor. Neither one
  # dials before `start()`.
  var config = initConsumerConfig(
    bootstrapBrokers = brokers, groupId = "ownership-demo", topics = @[topic]
  )
  let shared = newCluster(brokers)
  let onShared = newKafkaConsumer(config, shared)
  doAssert not onShared.ownsCluster
  await onShared.close()
  await shared.close()

  let onOwn = newKafkaConsumer(config)
  doAssert onOwn.ownsCluster
  await onOwn.close()

try:
  waitFor main()
except KafkaError as e:
  echo "kafka error: ", e.msg
  quit(QuitFailure)
