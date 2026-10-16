## Join a consumer group, see how its partitions are split, and react to a
## rebalance.
##
## `assignmentStrategy` decides how the coordinator deals the topic's partitions
## out: `asRange` (the default) gives every member a contiguous range, while
## `asRoundRobin` deals them one by one and evens out a topic count that does not
## divide the member count.
##
## `onPartitionsAssigned` runs once the new assignment is in effect and before
## that poll returns records; `onPartitionsRevoked` runs with the old assignment
## while the membership is still valid, after the pre-rebalance commit, and
## `onPartitionsLost` runs instead when the membership was invalidated. All three
## run inline on the poll loop and may raise — the driving poll re-raises a
## callback failure as `KafkaRebalanceCallbackError` — so keep them short and
## free of calls back into the client. None of them runs on `close`, which leaves
## the group rather than rebalancing it.
##
## Prerequisites: a broker at `KAFKA_BROKERS` (default `localhost:9092`) and the
## topic named by `KAFKA_TOPIC` (default `orders`) with more than one partition,
## so the split between the two members below is visible.
##
## Run from the repository root:
##
##   nim c -r examples/consumer_group.nim

import std/[os, sequtils, strutils]

import pkg/chronos
import pkg/async_kafka

# Whether each member's `onPartitionsAssigned` has fired since the last reset.
# Written from the callbacks, read from the poll loop; Chronos is
# single-threaded, so no synchronisation is needed.
var assignedFired = [false, false]

proc logger(label: string, slot: int): RebalanceCallback =
  ## One callback per member, so the output says which one changed. It also
  ## records that the assignment arrived, so the poll loop below waits until
  ## the rebalance settles instead of a fixed round count.
  result = proc(partitions: seq[TopicPartition]) {.gcsafe, raises: [].} =
    assignedFired[slot] = true
    var line = label & ":"
    for tp in partitions:
      line.add " " & tp.topic & "[" & $tp.partition & "]"
    if partitions.len == 0:
      line.add " (none)"
    echo line

proc newMember(
    brokers: seq[string], topic, clientId: string, slot: int
): KafkaConsumer =
  var config = initConsumerConfig(
    bootstrapBrokers = brokers,
    groupId = "group-demo",
    topics = @[topic],
    clientId = clientId,
  )
  config.assignmentStrategy = asRoundRobin
  config.offsetResetPolicy = orpEarliest
  config.onPartitionsAssigned = logger(clientId & " assigned", slot)
  config.onPartitionsRevoked = logger(clientId & " revoked", slot)
  config.onPartitionsLost = logger(clientId & " lost", slot)
  newKafkaConsumer(config)

proc main() {.async.} =
  let brokers = getEnv("KAFKA_BROKERS", "localhost:9092")
    .split(',')
    .mapIt(it.strip())
    .filterIt(it.len > 0)
  let topic = getEnv("KAFKA_TOPIC", "orders")

  # The first member joins alone, so it is assigned every partition.
  let first = newMember(brokers, topic, "member-a", 0)
  let second = newMember(brokers, topic, "member-b", 1)
  # `finally` so a join that registered a member before failing still reaches
  # `close()`, which leaves the group and releases the owned Cluster. The
  # explicit closes below order the leave; the `finally` only covers a failure
  # before them (`close()` is single-flight, so a second call is a no-op).
  const settleRounds = 10
  try:
    echo "member-a joining"
    await first.start()

    # A second member joins the same group, which rebalances the assignment: the
    # callbacks fire again for both, and the partitions end up split between them.
    # A member is told about the rebalance on its next poll, where the rejoin is
    # served, so both are polled until each reports its new assignment.
    assignedFired = [false, false]
    echo "member-b joining, which forces a rebalance"
    await second.start()
    for _ in 0 ..< settleRounds:
      discard await first.pollBatch(maxRecords = 1, timeout = 2.seconds)
      discard await second.pollBatch(maxRecords = 1, timeout = 2.seconds)
      if assignedFired[0] and assignedFired[1]:
        break
    if not (assignedFired[0] and assignedFired[1]):
      echo "warning: the rebalance did not settle within the poll budget"

    # Closing one member rebalances the rest; close() itself does not run the
    # callbacks, so the surviving member keeps its remaining partitions.
    echo "member-b leaving"
    assignedFired[0] = false
    await second.close()
    for _ in 0 ..< settleRounds:
      discard await first.pollBatch(maxRecords = 1, timeout = 2.seconds)
      if assignedFired[0]:
        break
    if not assignedFired[0]:
      echo "warning: the surviving member saw no reassignment within the poll budget"
    await first.close()
  finally:
    await second.close()
    await first.close()

try:
  waitFor main()
except KafkaError as e:
  echo "kafka error: ", e.msg
  quit(QuitFailure)
