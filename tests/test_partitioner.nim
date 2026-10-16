import std/[unittest, tables, options, importutils]

import chronos

import async_kafka/errors
import async_kafka/protocol/[murmur2, messages]
import async_kafka/cluster {.all.}
import async_kafka/producer/producer
import async_kafka/producer/partitioner

import producer_helpers

privateAccess(Cluster)
privateAccess(KafkaProducer)

proc toBytes(s: string): seq[byte] =
  @(s.toOpenArrayByte(0, s.high))

proc assignPartition(key: openArray[byte], partCount: int32): int32 =
  (murmur2(key) and 0x7fffffff'i32) mod partCount

suite "murmur2":
  ## Reference values verified against Kafka-compatible murmur2 (seed 0x9747b28c)

  test "empty input":
    check murmur2([]) == 275646681'i32

  test "known strings":
    check murmur2(toBytes("a")) == -1563381124'i32
    check murmur2(toBytes("ab")) == 316155434'i32
    check murmur2(toBytes("kafka")) == -798503068'i32
    check murmur2(toBytes("hello")) == 2132663229'i32

  test "single byte values":
    check murmur2([0x00'u8]) == 375494588'i32
    check murmur2([0xff'u8]) == -311467685'i32

  test "deterministic":
    let data = toBytes("test-key")
    let h1 = murmur2(data)
    let h2 = murmur2(data)
    check h1 == h2

suite "partition assignment":
  test "known key-partition mappings":
    # 6 partitions
    check assignPartition(toBytes("order-123"), 6) == 1
    check assignPartition(toBytes("user-456"), 6) == 2
    check assignPartition(toBytes("kafka"), 6) == 4
    check assignPartition(toBytes("hello"), 6) == 3

    # 3 partitions
    check assignPartition(toBytes("order-123"), 3) == 1
    check assignPartition(toBytes("kafka"), 3) == 1
    check assignPartition(toBytes("hello"), 3) == 0

  test "single partition":
    check assignPartition(toBytes("any-key"), 1) == 0
    check assignPartition(toBytes("another-key"), 1) == 0
    check assignPartition([], 1) == 0

  test "partition always in valid range":
    for partCount in [2'i32, 3, 5, 7, 12, 32, 64, 128, 256]:
      for i in 0 ..< 100:
        let key = toBytes("key-" & $i)
        let partition = assignPartition(key, partCount)
        check partition >= 0
        check partition < partCount

  test "same key same partition across calls":
    let keys = ["user-1", "order-99", "event-abc", "session-xyz"]
    for k in keys:
      let p1 = assignPartition(toBytes(k), 10)
      let p2 = assignPartition(toBytes(k), 10)
      check p1 == p2

  test "different keys distribute across partitions":
    var seen: set[int8]
    for i in 0 ..< 50:
      let p = assignPartition(toBytes("key-" & $i), 4)
      seen.incl(int8(p))
    # With 50 keys and 4 partitions, all partitions should be hit
    check seen.card == 4

suite "round-robin counter":
  test "cycles through partitions and restarts at 0 after a reset":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster)
    # Round-robin walks 0,1,2,0,... for a 3-partition topic.
    check p.roundRobinIndex("t", 3) == 0
    check p.roundRobinIndex("t", 3) == 1
    check p.roundRobinIndex("t", 3) == 2
    check p.roundRobinIndex("t", 3) == 0
    # resolvePartition keeps the counter across metadata refreshes; it clears it
    # only when the topic has no partitions, and the modulo self-rebalances
    # against a changed partition count. Clearing it restarts the walk at 0.
    p.partitionCounters.del("t")
    check p.roundRobinIndex("t", 5) == 0
    check p.roundRobinIndex("t", 5) == 1

  test "counter wraps at int32.high without OverflowDefect":
    # Regression: `(counter + 1) and int32.high` performed the addition in
    # int32, so the increment at counter == int32.high raised OverflowDefect
    # and crashed the process after ~2^31 unkeyed sends to one topic.
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(cluster)
    p.partitionCounters["t"] = int32.high
    discard p.roundRobinIndex("t", 3)
    check p.partitionCounters["t"] == 0'i32

proc seedPartitions(cluster: Cluster, topic: string, partCount: int32) =
  var parts = newSeq[PartitionMeta](partCount)
  for i in 0'i32 ..< partCount:
    parts[i] = PartitionMeta(partitionIndex: i, leaderId: 0, errorCode: keNone)
  cluster.topicPartitions[topic] = parts
  cluster.lastMetadataRefresh = Moment.now()

suite "resolvePartition: empty-key semantics (Java compatibility)":
  # Regression: some(@[]) is a keyed record with a zero-length key. Java's
  # DefaultPartitioner hashes it with murmur2; only null keyBytes fall through
  # to sticky/round-robin. Previously the Nim producer lumped some(@[]) with
  # none() and routed empty-key records via RR, breaking co-partitioning with
  # Java clients on the same topic.

  test "psKeyHash routes some(@[]) deterministically via murmur2":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      seedPartitions(cluster, "t", 6)
      let p = newKafkaProducer(cluster, partitionStrategy = psKeyHash)
      let expected = (murmur2([]) and 0x7fffffff'i32) mod 6'i32
      for _ in 0 ..< 4:
        let got = await p.resolvePartition("t", some(newSeq[byte]()))
        check got == expected

    waitFor body()

  test "psKeyHash routes none() via round-robin":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      seedPartitions(cluster, "t", 3)
      let p = newKafkaProducer(cluster, partitionStrategy = psKeyHash)
      check (await p.resolvePartition("t", none(seq[byte]))) == 0'i32
      check (await p.resolvePartition("t", none(seq[byte]))) == 1'i32
      check (await p.resolvePartition("t", none(seq[byte]))) == 2'i32
      check (await p.resolvePartition("t", none(seq[byte]))) == 0'i32

    waitFor body()

  test "psSticky routes some(@[]) deterministically via murmur2":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      seedPartitions(cluster, "t", 6)
      let p = newKafkaProducer(cluster, partitionStrategy = psSticky)
      let expected = (murmur2([]) and 0x7fffffff'i32) mod 6'i32
      # An empty-key send must NOT populate the sticky slot: sticky is the
      # no-key fallback, and future no-key sends have to be independent of
      # whatever partition an unrelated empty-key send happened to hash to.
      let got = await p.resolvePartition("t", some(newSeq[byte]()))
      check got == expected
      check p.getStickyPartition("t").isNone

    waitFor body()

  test "psSticky routes none() by pinning a partition":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      seedPartitions(cluster, "t", 4)
      let p = newKafkaProducer(cluster, partitionStrategy = psSticky)
      let first = await p.resolvePartition("t", none(seq[byte]))
      for _ in 0 ..< 5:
        check (await p.resolvePartition("t", none(seq[byte]))) == first
      check p.getStickyPartition("t") == some(first)

    waitFor body()
