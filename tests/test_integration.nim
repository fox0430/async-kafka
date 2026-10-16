import std/[unittest, options, strutils, logging, tables, importutils, os]
from std/times import epochTime

import chronos

import async_kafka/broker
import async_kafka/cluster {.all.}
import async_kafka/sasl
import async_kafka, producer_helpers

privateAccess(Cluster)
privateAccess(Broker)
privateAccess(SaslMechanism)

const
  BootstrapBroker = "127.0.0.1:9092"
  TestTopic = "test-integration"

proc testGroupId(suffix: string): string =
  ## Unique group ID per test run to avoid stale consumer group offsets.
  {.cast(gcsafe).}:
    var runId {.global.}: string
    if runId.len == 0:
      runId = $int64(epochTime() * 1000)
    result = "test-group-integration-" & runId & "-" & suffix

addHandler(newConsoleLogger(lvlInfo))

proc createTopic(topic: string, partitions: int = 3) {.async: (raises: [Exception]).} =
  ## Create a topic via the Metadata API: the refresh triggers broker-side
  ## auto-creation, so no produce happens.
  let cluster =
    newCluster(@[BootstrapBroker], "test-setup", allowAutoTopicCreation = true)
  await cluster.refreshMetadata(some(@[topic]))
  await sleepAsync(seconds(2))
  await cluster.close()

proc toBytes(s: string): seq[byte] =
  cast[seq[byte]](s)

proc toString(b: seq[byte]): string =
  cast[string](b)

suite "Integration: Connection and Metadata":
  test "Connect to broker and fetch API versions":
    proc testBody() {.async: (raises: [Exception]).} =
      let broker = newBroker(-1, "127.0.0.1", 9092, "test-client")
      await broker.connect()
      check broker.isConnected
      check broker.apiVersions.len > 0
      await broker.close()

    waitFor testBody()

  test "Fetch cluster metadata":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@[BootstrapBroker], "test-client")
      await cluster.refreshMetadata()
      check cluster.brokers.len > 0
      await cluster.close()

    waitFor testBody()

  test "Fetch metadata for specific topic":
    proc testBody() {.async: (raises: [Exception]).} =
      await createTopic(TestTopic)
      let cluster = newCluster(@[BootstrapBroker], "test-client")
      await cluster.refreshMetadata(some(@[TestTopic]))
      check cluster.topicPartitions.hasKey(TestTopic)
      check cluster.topicPartitions[TestTopic].len > 0
      await cluster.close()

    waitFor testBody()

  test "Metadata passes an allowlist that covers the advertised address":
    ## The real broker advertises 127.0.0.1:9092, so a plaintext cluster with an
    ## allowlist covering loopback must keep working — the policy bounds where
    ## metadata may redirect the client, it does not replace normal operation.
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(
        @[BootstrapBroker],
        "test-allowlist",
        hostAllowlist = newHostAllowlist(@[allowCidr("127.0.0.0/8")]),
      )
      await cluster.refreshMetadata()
      check cluster.brokers.len > 0

      # And the same cluster refuses an advertised address outside the list.
      let denied = newCluster(
        @[BootstrapBroker],
        "test-allowlist-denied",
        hostAllowlist = newHostAllowlist(@[allowCidr("10.0.0.0/8")]),
      )
      var raised = false
      try:
        await denied.refreshMetadata()
      except KafkaError:
        raised = true
      check raised
      check denied.brokers.len == 0

      await cluster.close()
      await denied.close()

    waitFor testBody()

suite "Integration: Producer":
  test "Produce a single message":
    proc testBody() {.async: (raises: [Exception]).} =
      await createTopic(TestTopic)
      let cluster = newCluster(@[BootstrapBroker], "test-producer")
      let producer = newKafkaProducer(cluster, acks = 1)

      let r = await producer.send(TestTopic, "hello kafka".toBytes)
      check r.topic == TestTopic
      check r.partition >= 0
      check r.offset >= 0

      await producer.close()

    waitFor testBody()

  test "Produce multiple messages":
    proc testBody() {.async: (raises: [Exception]).} =
      await createTopic(TestTopic)
      let cluster = newCluster(@[BootstrapBroker], "test-producer")
      let producer = newKafkaProducer(cluster, acks = 1)

      var offsets: seq[int64]
      for i in 0 ..< 10:
        let r = await producer.send(TestTopic, ("message-" & $i).toBytes)
        check r.topic == TestTopic
        offsets.add(r.offset)

      for o in offsets:
        check o >= 0

      await producer.close()

    waitFor testBody()

  test "Produce with key":
    proc testBody() {.async: (raises: [Exception]).} =
      await createTopic(TestTopic)
      let cluster = newCluster(@[BootstrapBroker], "test-producer")
      let producer = newKafkaProducer(cluster, acks = 1)

      let r = await producer.send(
        TestTopic, "value-with-key".toBytes, key = some("my-key".toBytes)
      )
      check r.topic == TestTopic
      check r.offset >= 0

      await producer.close()

    waitFor testBody()

  test "Produce with headers":
    proc testBody() {.async: (raises: [Exception]).} =
      await createTopic(TestTopic)
      let cluster = newCluster(@[BootstrapBroker], "test-producer")
      let producer = newKafkaProducer(cluster, acks = 1)

      let headers = @[
        RecordHeader(key: "trace-id", value: some("abc123".toBytes)),
        RecordHeader(key: "source", value: some("test".toBytes)),
      ]
      let r =
        await producer.send(TestTopic, "value-with-headers".toBytes, headers = headers)
      check r.topic == TestTopic
      check r.offset >= 0

      await producer.close()

    waitFor testBody()

  test "Produce to specific partition":
    proc testBody() {.async: (raises: [Exception]).} =
      await createTopic(TestTopic)
      let cluster = newCluster(@[BootstrapBroker], "test-producer")
      let producer = newKafkaProducer(cluster, acks = 1)

      let r = await producer.send(TestTopic, "partition-0-msg".toBytes, partition = 0)
      check r.topic == TestTopic
      check r.partition == 0
      check r.offset >= 0

      await producer.close()

    waitFor testBody()

  test "Produce with acks=all":
    proc testBody() {.async: (raises: [Exception]).} =
      await createTopic(TestTopic)
      let cluster = newCluster(@[BootstrapBroker], "test-producer-acks-all")
      let producer = newKafkaProducer(cluster, acks = -1)

      let r = await producer.send(TestTopic, "acks-all-message".toBytes)
      check r.topic == TestTopic
      check r.offset >= 0

      await producer.close()

    waitFor testBody()

suite "Integration: Producer and Consumer":
  test "Produce then consume messages":
    proc testBody() {.async: (raises: [Exception]).} =
      let topic = "test-produce-consume"
      await createTopic(topic)

      let prodCluster = newCluster(@[BootstrapBroker], "test-producer")
      let producer = newKafkaProducer(prodCluster, acks = 1)

      let messageCount = 5
      for i in 0 ..< messageCount:
        discard
          await producer.send(topic, ("consume-test-" & $i).toBytes, partition = 0)
      await producer.close()

      var config = initConsumerConfig(
        bootstrapBrokers = @[BootstrapBroker],
        groupId = testGroupId("produce-consume"),
        topics = @[topic],
        clientId = "test-consumer",
      )
      config.offsetResetPolicy = orpEarliest
      config.autoCommit = false
      config.sessionTimeoutMs = 30000
      config.heartbeatIntervalMs = 3000

      let consumer = newKafkaConsumer(config)
      await consumer.start()

      let records = await pollBatch(consumer, messageCount, seconds(30))

      check records.len >= messageCount

      var found = 0
      for rec in records:
        if rec.value.isSome:
          let val = rec.value.get.toString
          if val.startsWith("consume-test-"):
            found.inc

      check found >= messageCount

      await consumer.close()

    waitFor testBody()

  test "Produce then consume with manual commit":
    proc testBody() {.async: (raises: [Exception]).} =
      let topic = "test-manual-commit"
      await createTopic(topic)

      let prodCluster = newCluster(@[BootstrapBroker], "test-producer")
      let producer = newKafkaProducer(prodCluster, acks = 1)

      for i in 0 ..< 3:
        discard await producer.send(topic, ("commit-test-" & $i).toBytes, partition = 0)
      await producer.close()

      var config = initConsumerConfig(
        bootstrapBrokers = @[BootstrapBroker],
        groupId = testGroupId("manual-commit"),
        topics = @[topic],
        clientId = "test-consumer-commit",
      )
      config.offsetResetPolicy = orpEarliest
      config.autoCommit = false

      let consumer = newKafkaConsumer(config)
      await consumer.start()

      let records = await pollBatch(consumer, 3, seconds(30))
      check records.len >= 3

      await consumer.commitSync()

      await consumer.close()

    waitFor testBody()

  test "Produce with key and consume preserving key":
    proc testBody() {.async: (raises: [Exception]).} =
      let topic = "test-key-roundtrip"
      await createTopic(topic)

      let prodCluster = newCluster(@[BootstrapBroker], "test-producer")
      let producer = newKafkaProducer(prodCluster, acks = 1)

      discard await producer.send(
        topic, "value-data".toBytes, key = some("key-data".toBytes), partition = 0
      )
      await producer.close()

      var config = initConsumerConfig(
        bootstrapBrokers = @[BootstrapBroker],
        groupId = testGroupId("key-roundtrip"),
        topics = @[topic],
        clientId = "test-consumer-key",
      )
      config.offsetResetPolicy = orpEarliest
      config.autoCommit = false

      let consumer = newKafkaConsumer(config)
      await consumer.start()

      let records = await pollBatch(consumer, 1, seconds(30))
      check records.len >= 1

      let rec = records[0]
      check rec.key.isSome
      check rec.key.get.toString == "key-data"
      check rec.value.isSome
      check rec.value.get.toString == "value-data"

      await consumer.close()

    waitFor testBody()

  test "Produce with headers and consume preserving headers":
    proc testBody() {.async: (raises: [Exception]).} =
      let topic = "test-headers-roundtrip"
      await createTopic(topic)

      let prodCluster = newCluster(@[BootstrapBroker], "test-producer")
      let producer = newKafkaProducer(prodCluster, acks = 1)

      let headers = @[
        RecordHeader(key: "h1", value: some("v1".toBytes)),
        RecordHeader(key: "h2", value: some("v2".toBytes)),
      ]
      discard await producer.send(
        topic, "header-value".toBytes, headers = headers, partition = 0
      )
      await producer.close()

      var config = initConsumerConfig(
        bootstrapBrokers = @[BootstrapBroker],
        groupId = testGroupId("headers-roundtrip"),
        topics = @[topic],
        clientId = "test-consumer-headers",
      )
      config.offsetResetPolicy = orpEarliest
      config.autoCommit = false

      let consumer = newKafkaConsumer(config)
      await consumer.start()

      let records = await pollBatch(consumer, 1, seconds(30))
      check records.len >= 1

      let rec = records[0]
      check rec.headers.len == 2
      check rec.headers[0].key == "h1"
      check rec.headers[0].value.get.toString == "v1"
      check rec.headers[1].key == "h2"
      check rec.headers[1].value.get.toString == "v2"

      await consumer.close()

    waitFor testBody()

suite "Integration: Compressed Producer and Consumer":
  proc codecRoundTrip(
      codec: CompressionCodec, label: string
  ) {.async: (raises: [Exception]).} =
    ## Produce a batch under `codec` and read it back through the real broker. The
    ## broker decompresses each batch to verify its CRC, so a codec whose frames
    ## are wrong on the wire fails here instead of only in a client-side round trip.
    let topic = "test-" & label & "-roundtrip"
    await createTopic(topic)

    let prodCluster = newCluster(@[BootstrapBroker], "test-producer-" & label)
    let producer = newKafkaProducer(prodCluster, acks = 1, compression = codec)

    for i in 0 ..< 5:
      discard await producer.send(topic, (label & "-msg-" & $i).toBytes, partition = 0)
    await producer.close()
    await prodCluster.close()

    var config = initConsumerConfig(
      bootstrapBrokers = @[BootstrapBroker],
      groupId = testGroupId(label & "-roundtrip"),
      topics = @[topic],
      clientId = "test-consumer-" & label,
    )
    config.offsetResetPolicy = orpEarliest
    config.autoCommit = false

    let consumer = newKafkaConsumer(config)
    await consumer.start()

    let records = await pollBatch(consumer, 5, seconds(30))
    check records.len >= 5

    var found = 0
    for rec in records:
      if rec.value.isSome:
        if rec.value.get.toString.startsWith(label & "-msg-"):
          found.inc
    check found >= 5

    await consumer.close()

  test "Produce and consume with gzip compression":
    waitFor codecRoundTrip(ccGzip, "gzip")

  test "Produce and consume with snappy compression":
    waitFor codecRoundTrip(ccSnappy, "snappy")

  test "Produce and consume with lz4 compression":
    ## liblz4 is resolved with dlopen rather than linked, so this is the only path
    ## that proves a real broker accepts the frames this build emits.
    waitFor codecRoundTrip(ccLz4, "lz4")

  test "Produce and consume with zstd compression":
    ## Zstd comes from the pinned fork; the broker's own decompressor is the
    ## independent check that the frames it emits are standard Zstandard.
    waitFor codecRoundTrip(ccZstd, "zstd")

  test "Snappy compression with key and headers":
    proc testBody() {.async: (raises: [Exception]).} =
      let topic = "test-snappy-full"
      await createTopic(topic)

      let prodCluster = newCluster(@[BootstrapBroker], "test-producer-snappy-full")
      let producer = newKafkaProducer(prodCluster, acks = 1, compression = ccSnappy)

      let headers = @[
        RecordHeader(key: "trace-id", value: some("snap-123".toBytes)),
        RecordHeader(key: "codec", value: some("snappy".toBytes)),
      ]
      discard await producer.send(
        topic,
        "snappy-full-value".toBytes,
        key = some("snappy-key".toBytes),
        headers = headers,
        partition = 0,
      )
      await producer.close()

      var config = initConsumerConfig(
        bootstrapBrokers = @[BootstrapBroker],
        groupId = testGroupId("snappy-full"),
        topics = @[topic],
        clientId = "test-consumer-snappy-full",
      )
      config.offsetResetPolicy = orpEarliest
      config.autoCommit = false

      let consumer = newKafkaConsumer(config)
      await consumer.start()

      let records = await pollBatch(consumer, 1, seconds(30))
      check records.len >= 1

      let rec = records[0]
      check rec.key.isSome
      check rec.key.get.toString == "snappy-key"
      check rec.value.isSome
      check rec.value.get.toString == "snappy-full-value"
      check rec.headers.len == 2
      check rec.headers[0].key == "trace-id"
      check rec.headers[0].value.get.toString == "snap-123"
      check rec.headers[1].key == "codec"
      check rec.headers[1].value.get.toString == "snappy"

      await consumer.close()

    waitFor testBody()

suite "Integration: Idempotent Producer":
  ## Smoke coverage for the non-transactional idempotent path (KIP-185):
  ## `enableIdempotence = true` without a transactional id, otherwise untested
  ## end-to-end. Pins the basic round-trip and the wrap-safe sequence allocation
  ## allocateInflight performs near int32.high.

  test "Idempotent produce round-trips and preserves order":
    proc testBody() {.async: (raises: [Exception]).} =
      let topic = "test-idempotent"
      await createTopic(topic)

      let prodCluster = newCluster(@[BootstrapBroker], "test-idempotent-producer")
      let producer = newKafkaProducer(prodCluster, enableIdempotence = true, acks = -1)

      var produceOffsets: seq[int64]
      for i in 0 ..< 10:
        let r =
          await producer.send(topic, ("idem-msg-" & $i).toBytes, partition = 0'i32)
        check r.status == pstSuccess
        check r.offset >= 0
        produceOffsets.add(r.offset)
      await producer.close()
      await prodCluster.close()

      # Offsets must be contiguous — idempotent producer must not duplicate
      # or reorder records over a single connection.
      for i in 1 ..< produceOffsets.len:
        check produceOffsets[i] == produceOffsets[i - 1] + 1

      var config = initConsumerConfig(
        bootstrapBrokers = @[BootstrapBroker],
        groupId = testGroupId("idempotent-roundtrip"),
        topics = @[topic],
        clientId = "test-consumer-idempotent",
      )
      config.offsetResetPolicy = orpEarliest
      config.autoCommit = false

      let consumer = newKafkaConsumer(config)
      await consumer.start()

      var seen = 0
      let deadline = Moment.now() + seconds(30)
      while seen < 10 and Moment.now() < deadline:
        let batch = await pollBatch(consumer, 20, seconds(5))
        for rec in batch:
          # The topic persists across runs, so earlier runs' records replay
          # from earliest. Skip anything before this run's first append; the
          # records at produceOffsets[0..] are exactly the 10 we just sent.
          if rec.offset < produceOffsets[0]:
            continue
          if rec.value.isSome:
            let val = rec.value.get.toString
            if val.startsWith("idem-msg-"):
              # The N-th idem-msg must land at produceOffsets[N] — strictly
              # monotonic and contiguous if the producer didn't duplicate.
              if seen < produceOffsets.len:
                check rec.offset == produceOffsets[seen]
              seen.inc
      check seen >= 10

      await consumer.close()

    waitFor testBody()

  test "Idempotent producer survives near-wrap sequence allocation":
    ## The producer's sequence counter wraps at int32.high. Pushing the counter
    ## close to the wrap before a real send drives encode through the same path
    ## int32.high-adjacent values take in production. Two broker outcomes are both
    ## acceptable: it tolerates the high starting sequence as the new baseline, or
    ## rejects with OUT_OF_ORDER_SEQUENCE_NUMBER and the client resets producer-id
    ## and re-sends. The test only asserts the producer stays healthy: it does
    ## NOT crash or hang, and at least one send succeeds.
    proc testBody() {.async: (raises: [Exception]).} =
      let topic = "test-idempotent-wrap"
      await createTopic(topic)

      let prodCluster = newCluster(@[BootstrapBroker], "test-idempotent-wrap")
      let producer = newKafkaProducer(prodCluster, enableIdempotence = true, acks = -1)
      # One ordinary send first so the broker materializes producer-id /
      # epoch state and the test exercises the wrap path on an already
      # initialized producer.
      let initial = await producer.send(topic, "init".toBytes, partition = 0'i32)
      check initial.status == pstSuccess

      let tp = TopicPartition(topic: topic, partition: 0'i32)
      # Push the local counter into the last few sequence slots before
      # wrap. Next send produces a baseSequence near int32.high; the one
      # after it must wrap to 0 client-side regardless of broker
      # outcome.
      producer.setSequenceNumber(tp, int32.high - 2'i32)

      var produced = 0
      for i in 0 ..< 5:
        try:
          let r =
            await producer.send(topic, ("wrap-msg-" & $i).toBytes, partition = 0'i32)
          if r.status == pstSuccess:
            produced.inc
        except KafkaError:
          # On the OUT_OF_ORDER path, the producer reset producer-id and
          # the SAME send is retried; a subsequent send re-enters from
          # sequence 0. Either way the producer stays healthy.
          discard
      check produced >= 1

      await producer.close()
      await prodCluster.close()

    waitFor testBody()

suite "Integration: Consumer Rebalance":
  test "Consumer survives rebalance when second consumer joins and leaves":
    proc testBody() {.async: (raises: [Exception]).} =
      let topic = "test-rebalance"
      await createTopic(topic)

      let prodCluster = newCluster(@[BootstrapBroker], "test-producer-rebal")
      let producer = newKafkaProducer(prodCluster, acks = 1)
      for i in 0 ..< 10:
        discard await producer.send(topic, ("rebal-msg-" & $i).toBytes, partition = 0)
      await producer.close()

      let groupId = testGroupId("rebalance")

      var config1 = initConsumerConfig(
        bootstrapBrokers = @[BootstrapBroker],
        groupId = groupId,
        topics = @[topic],
        clientId = "test-consumer-rebal-1",
      )
      config1.offsetResetPolicy = orpEarliest
      config1.autoCommit = false
      config1.sessionTimeoutMs = 10000
      config1.heartbeatIntervalMs = 1000

      let consumer1 = newKafkaConsumer(config1)
      await consumer1.start()

      let initialRecords = await pollBatch(consumer1, 3, seconds(15))
      check initialRecords.len >= 1

      # Start consumer 2 in same group — triggers rebalance on consumer 1
      var config2 = initConsumerConfig(
        bootstrapBrokers = @[BootstrapBroker],
        groupId = groupId,
        topics = @[topic],
        clientId = "test-consumer-rebal-2",
      )
      config2.offsetResetPolicy = orpEarliest
      config2.autoCommit = false
      config2.sessionTimeoutMs = 10000
      config2.heartbeatIntervalMs = 1000

      let consumer2 = newKafkaConsumer(config2)
      await consumer2.start()

      # Give time for rebalance to propagate via heartbeat
      await sleepAsync(seconds(5))

      # Close consumer 2 — triggers another rebalance on consumer 1
      await consumer2.close()

      # Give time for rebalance to propagate
      await sleepAsync(seconds(5))

      let prodCluster2 = newCluster(@[BootstrapBroker], "test-producer-rebal-2")
      let producer2 = newKafkaProducer(prodCluster2, acks = 1)
      for i in 0 ..< 5:
        discard await producer2.send(topic, ("post-rebal-" & $i).toBytes, partition = 0)
      await producer2.close()

      # Consumer 1 should recover from rebalance and continue polling
      var postRebalRecords: seq[FetchedRecord]
      let deadline = Moment.now() + seconds(30)
      while postRebalRecords.len < 1 and Moment.now() < deadline:
        let batch = await pollBatch(consumer1, 10, seconds(5))
        for rec in batch:
          if rec.value.isSome:
            let val = rec.value.get.toString
            if val.startsWith("post-rebal-"):
              postRebalRecords.add(rec)

      check postRebalRecords.len >= 1
      check consumer1.state == lsRunning

      await consumer1.close()

    waitFor testBody()

suite "Integration: FindCoordinator":
  test "Find coordinator for consumer group":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@[BootstrapBroker], "test-client")
      let coordinator = await cluster.coordinatorBroker("test-find-coord-group")
      check coordinator != nil
      check coordinator.isConnected
      await cluster.close()

    waitFor testBody()

  test "Find transaction coordinator":
    proc testBody() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@[BootstrapBroker], "test-client")
      let coordinator = await cluster.txnCoordinatorBroker("test-txn-id")
      check coordinator != nil
      check coordinator.isConnected
      await cluster.close()

    waitFor testBody()

suite "Integration: Transactions":
  test "Transaction commit - messages visible":
    proc testBody() {.async: (raises: [Exception]).} =
      let topic = "test-txn-commit"
      await createTopic(topic)

      let prodCluster = newCluster(@[BootstrapBroker], "test-txn-producer")
      let producer =
        newKafkaProducer(prodCluster, transactionalId = some("txn-commit-test"))
      await producer.initTransactions()

      producer.beginTransaction()
      for i in 0 ..< 3:
        discard await producer.send(topic, ("txn-msg-" & $i).toBytes, partition = 0)
      await producer.commitTransaction()
      await producer.close()
      await prodCluster.close()

      var config = initConsumerConfig(
        bootstrapBrokers = @[BootstrapBroker],
        groupId = testGroupId("txn-commit"),
        topics = @[topic],
        clientId = "test-consumer-txn-commit",
      )
      config.offsetResetPolicy = orpEarliest
      config.autoCommit = false
      config.isolationLevel = ilReadCommitted

      let consumer = newKafkaConsumer(config)
      await consumer.start()

      # In read_committed mode, LSO propagation may require multiple polls.
      # The topic may contain leftover messages from prior runs, so poll
      # until we find all 3 txn-msg-* records or time out.
      var found = 0
      let deadline = Moment.now() + seconds(30)
      while found < 3 and Moment.now() < deadline:
        let batch = await pollBatch(consumer, 10, seconds(5))
        for rec in batch:
          if rec.value.isSome:
            let val = rec.value.get.toString
            if val.startsWith("txn-msg-"):
              found.inc
      check found >= 3

      await consumer.close()

    waitFor testBody()

  test "sendOffsetsToTransaction with consumer group metadata (KIP-447 v3)":
    proc testBody() {.async: (raises: [Exception]).} =
      # Use the shared TestTopic, already created and fully propagated (including
      # to the transaction coordinator). A brand-new topic here would race
      # AddPartitionsToTxn with UNKNOWN_TOPIC_OR_PARTITION — a cold-topic
      # provisioning artifact, not a wire issue.
      let topic = TestTopic

      # A consumer joined to a group supplies a real (generationId, memberId),
      # so TxnOffsetCommit goes out at v3 with the fencing fields populated.
      var config = initConsumerConfig(
        bootstrapBrokers = @[BootstrapBroker],
        groupId = testGroupId("txn-sendoffsets"),
        topics = @[topic],
        clientId = "test-consumer-sendoffsets",
      )
      config.offsetResetPolicy = orpEarliest
      config.autoCommit = false
      let consumer = newKafkaConsumer(config)
      await consumer.start()
      let gm = consumer.groupMetadata()
      check gm.generationId >= 1 # joined → real generation
      check gm.memberId.len > 0

      let prodCluster = newCluster(@[BootstrapBroker], "test-txn-sendoffsets-producer")
      let producer =
        newKafkaProducer(prodCluster, transactionalId = some("txn-sendoffsets-test"))
      await producer.initTransactions()
      producer.beginTransaction()
      discard await producer.send(topic, "eos-msg".toBytes, partition = 0)

      var offsets = initTable[TopicPartition, int64]()
      offsets[TopicPartition(topic: topic, partition: 0)] = 1'i64
      # Exercises AddOffsetsToTxn v3, TxnOffsetCommit v3 (KIP-447 fencing
      # fields), and EndTxn v3 against the live broker.
      await producer.sendOffsetsToTransaction(offsets, gm)
      await producer.commitTransaction()

      await producer.close()
      await prodCluster.close()
      await consumer.close()

    waitFor testBody()

  test "Transaction abort - messages not visible with read_committed":
    proc testBody() {.async: (raises: [Exception]).} =
      let topic = "test-txn-abort"
      await createTopic(topic)

      # Produce a committed marker first so consumer has something to read up to
      let setupCluster = newCluster(@[BootstrapBroker], "test-setup-producer")
      let setupProducer =
        newKafkaProducer(setupCluster, transactionalId = some("txn-abort-setup"))
      await setupProducer.initTransactions()
      setupProducer.beginTransaction()
      discard await setupProducer.send(topic, "visible-msg".toBytes, partition = 0)
      await setupProducer.commitTransaction()
      await setupProducer.close()

      let prodCluster = newCluster(@[BootstrapBroker], "test-txn-abort-producer")
      let producer =
        newKafkaProducer(prodCluster, transactionalId = some("txn-abort-test"))
      await producer.initTransactions()

      producer.beginTransaction()
      for i in 0 ..< 3:
        discard await producer.send(topic, ("aborted-msg-" & $i).toBytes, partition = 0)
      await producer.abortTransaction()
      await producer.close()

      # Another committed marker after abort
      let endCluster = newCluster(@[BootstrapBroker], "test-end-producer")
      let endProducer =
        newKafkaProducer(endCluster, transactionalId = some("txn-abort-end"))
      await endProducer.initTransactions()
      endProducer.beginTransaction()
      discard await endProducer.send(topic, "end-marker".toBytes, partition = 0)
      await endProducer.commitTransaction()
      await endProducer.close()

      # Consume with read_committed — aborted messages should be filtered
      var config = initConsumerConfig(
        bootstrapBrokers = @[BootstrapBroker],
        groupId = testGroupId("txn-abort"),
        topics = @[topic],
        clientId = "test-consumer-txn-abort",
      )
      config.offsetResetPolicy = orpEarliest
      config.autoCommit = false
      config.isolationLevel = ilReadCommitted

      let consumer = newKafkaConsumer(config)
      await consumer.start()

      # Positive control: the committed markers must be visible. Without this,
      # a read_committed filter that drops EVERYTHING still passes with an
      # empty batch (vacuous pass).
      var sawVisible = false
      var sawEnd = false
      var sawAborted = false
      let deadline = Moment.now() + seconds(30)
      while (not (sawVisible and sawEnd)) and Moment.now() < deadline:
        let records = await pollBatch(consumer, 10, seconds(5))
        for rec in records:
          if rec.value.isSome:
            let val = rec.value.get.toString
            if val == "visible-msg":
              sawVisible = true
            elif val == "end-marker":
              sawEnd = true
            elif val.startsWith("aborted-msg-"):
              sawAborted = true
      check sawVisible
      check sawEnd
      check not sawAborted

      await consumer.close()

      # Verify the aborted records actually exist: reading the same topic with
      # read_uncommitted must surface them, proving the read_committed filter
      # above hid (not mysteriously lost) them.
      var rawConfig = initConsumerConfig(
        bootstrapBrokers = @[BootstrapBroker],
        groupId = testGroupId("txn-abort-raw"),
        topics = @[topic],
        clientId = "test-consumer-txn-abort-raw",
      )
      rawConfig.offsetResetPolicy = orpEarliest
      rawConfig.autoCommit = false
      rawConfig.isolationLevel = ilReadUncommitted
      let rawConsumer = newKafkaConsumer(rawConfig)
      await rawConsumer.start()
      let rawDeadline = Moment.now() + seconds(30)
      var rawAborted = 0
      while rawAborted < 3 and Moment.now() < rawDeadline:
        let batch = await pollBatch(rawConsumer, 10, seconds(5))
        for rec in batch:
          if rec.value.isSome and rec.value.get.toString.startsWith("aborted-msg-"):
            rawAborted.inc
      check rawAborted >= 3
      await rawConsumer.close()

    waitFor testBody()

  test "Multiple transactions on same producer":
    proc testBody() {.async: (raises: [Exception]).} =
      let topic = "test-txn-multi"
      await createTopic(topic)

      let prodCluster = newCluster(@[BootstrapBroker], "test-txn-multi-producer")
      let producer =
        newKafkaProducer(prodCluster, transactionalId = some("txn-multi-test"))
      await producer.initTransactions()

      producer.beginTransaction()
      discard await producer.send(topic, "txn1-msg".toBytes, partition = 0)
      await producer.commitTransaction()

      producer.beginTransaction()
      discard await producer.send(topic, "txn2-msg".toBytes, partition = 0)
      await producer.commitTransaction()

      await producer.close()

      var config = initConsumerConfig(
        bootstrapBrokers = @[BootstrapBroker],
        groupId = testGroupId("txn-multi"),
        topics = @[topic],
        clientId = "test-consumer-txn-multi",
      )
      config.offsetResetPolicy = orpEarliest
      config.autoCommit = false
      config.isolationLevel = ilReadCommitted

      let consumer = newKafkaConsumer(config)
      await consumer.start()

      let records = await pollBatch(consumer, 2, seconds(30))
      check records.len >= 2

      await consumer.close()

    waitFor testBody()

# --- SASL against the broker's SASL_PLAINTEXT listener ----------------------
#
# Every other suite talks to the PLAINTEXT listener on 9092. These drive the
# mechanisms against a broker that refuses anonymous traffic, which is the only
# place the authenticated paths can be observed end to end: the unit suites
# pair the client with a fake broker written from the same reading of the
# protocol, so a shared misunderstanding of the handshake would pass there and
# fail here. The local development deployment owns the listener and the
# credentials; the SCRAM users are created by its warm-up step.

const
  SaslHost = "127.0.0.1"
  SaslPort = 9094
  SaslBroker = SaslHost & ":" & $SaslPort
  SaslScramUser = "scram-user"
  SaslScramPassword = "scram-secret"
  SaslPlainUser = "client"
  SaslPlainPassword = "client-secret"

proc scramMechanism(hash: ScramHash): SaslMechanism =
  newSaslScram(SaslScramUser, SaslScramPassword, hash)

proc unsupportedMechanism(): SaslMechanism =
  ## Names a mechanism the test broker does not enable, so the handshake is
  ## answered with UNSUPPORTED_SASL_MECHANISM — the reply the candidate list
  ## exists to recover from. Built by hand because every constructor in the
  ## library names a mechanism this broker does enable.
  SaslMechanism(
    mechanismName: "GSSAPI",
    tlsRequired: false,
    newSession: proc(): SaslSession {.raises: [].} =
      # Called before the handshake; the rejected mechanism leaves
      # initialResponse and challenge unused.
      SaslSession(
        initialResponse: proc(): seq[byte] {.raises: [KafkaError].} =
          @[],
        challenge: proc(
            data: seq[byte]
        ): Future[(SaslState, seq[byte])] {.
            gcsafe, async: (raises: [KafkaError, CancelledError])
        .} =
          return (ssComplete, @[]),
      ),
  )

suite "Integration: SASL":
  test "SCRAM-SHA-256 authenticates and the session stays usable":
    proc testBody() {.async: (raises: [Exception]).} =
      let broker = newBroker(
        -1, SaslHost, SaslPort, "test-sasl", saslMechanism = scramMechanism(scramSha256)
      )
      await broker.connect()
      check broker.isConnected
      check broker.apiVersions.len > 0

      # A request after the handshake is answered, so the authenticated session
      # is usable and not merely a socket that happened to open.
      let meta = await broker.metadata(none(seq[string]), false)
      check meta.brokers.len == 1
      # The broker advertises the SASL listener, so the connections a client
      # opens from this metadata authenticate as well.
      check meta.brokers[0].port == SaslPort

      await broker.close()

    waitFor testBody()

  test "SCRAM-SHA-512 authenticates":
    proc testBody() {.async: (raises: [Exception]).} =
      let broker = newBroker(
        -1, SaslHost, SaslPort, "test-sasl", saslMechanism = scramMechanism(scramSha512)
      )
      await broker.connect()
      check broker.isConnected

      let meta = await broker.metadata(none(seq[string]), false)
      check meta.brokers.len == 1

      await broker.close()

    waitFor testBody()

  test "PLAIN authenticates once the plaintext refusal is opted out of":
    proc testBody() {.async: (raises: [Exception]).} =
      let broker = newBroker(
        -1,
        SaslHost,
        SaslPort,
        "test-sasl",
        saslMechanism =
          newSaslPlain(SaslPlainUser, SaslPlainPassword, allowPlaintext = true),
      )
      await broker.connect()
      check broker.isConnected

      let meta = await broker.metadata(none(seq[string]), false)
      check meta.brokers.len == 1

      await broker.close()

    waitFor testBody()

  test "PLAIN keeps its password off a plaintext connection":
    proc testBody() {.async: (raises: [Exception]).} =
      let mech = newSaslPlain(SaslPlainUser, SaslPlainPassword)
      let broker = newBroker(-1, SaslHost, SaslPort, "test-sasl", saslMechanism = mech)
      var caught = false
      var caughtMsg = ""
      try:
        await broker.connect()
      except KafkaConnectionError as e:
        caught = true
        caughtMsg = e.msg
      check caught
      check "requires a TLS connection" in caughtMsg
      check (not broker.isConnected)
      await broker.close()

    waitFor testBody()

  test "a wrong password surfaces the broker's authentication failure":
    proc testBody() {.async: (raises: [Exception]).} =
      let mech = newSaslScram(SaslScramUser, "not-the-password", scramSha256)
      let broker = newBroker(-1, SaslHost, SaslPort, "test-sasl", saslMechanism = mech)
      var caughtApi = false
      var caughtCode = keNone
      try:
        await broker.connect()
      except KafkaApiError as e:
        caughtApi = true
        caughtCode = e.errorCode
      # The broker's own code (58) reaches the caller: the failure is not
      # flattened into a generic connection error.
      check caughtApi
      check caughtCode == keSaslAuthenticationFailed
      check (not broker.isConnected)
      await broker.close()

    waitFor testBody()

  test "an unsupported mechanism falls through to the next candidate":
    proc testBody() {.async: (raises: [Exception]).} =
      let broker = newBroker(
        -1,
        SaslHost,
        SaslPort,
        "test-sasl",
        saslMechanisms = @[unsupportedMechanism(), scramMechanism(scramSha256)],
      )
      await broker.connect()
      check broker.isConnected

      # Only an authenticated session is answered, so this fails if the rejected
      # candidate was skipped instead of being replaced by the next one.
      let meta = await broker.metadata(none(seq[string]), false)
      check meta.brokers.len == 1

      await broker.close()

    waitFor testBody()

  test "the listener refuses a request that never authenticated":
    proc testBody() {.async: (raises: [Exception]).} =
      let broker = newBroker(-1, SaslHost, SaslPort, "test-sasl")
      # ApiVersions is allowed before authentication, so the connect itself
      # succeeds and reports a connected broker.
      await broker.connect()
      check broker.isConnected
      check broker.apiVersions.len > 0

      var answered = false
      try:
        discard await broker.metadata(none(seq[string]), false)
        answered = true
      except KafkaError:
        discard
      check (not answered)
      await broker.close()

    waitFor testBody()

  test "SCRAM-SHA-512 producer and consumer round-trip":
    proc testBody() {.async: (raises: [Exception]).} =
      let topic = "test-sasl-roundtrip"

      block createTopic:
        let cluster = newCluster(
          @[SaslBroker],
          "test-sasl-setup",
          saslMechanism = scramMechanism(scramSha512),
          allowAutoTopicCreation = true,
        )
        await cluster.refreshMetadata(some(@[topic]))
        await sleepAsync(seconds(2))
        await cluster.close()

      let producerCluster = newCluster(
        @[SaslBroker], "test-sasl-producer", saslMechanism = scramMechanism(scramSha512)
      )
      let producer = newKafkaProducer(producerCluster, acks = 1)
      discard await producer.send(topic, "sasl-roundtrip".toBytes, partition = 0)
      await producer.close()

      # The consumer reaches the group coordinator and the partition leader over
      # connections of its own, so this covers the fan-out, not just bootstrap.
      var config = initConsumerConfig(
        bootstrapBrokers = @[SaslBroker],
        groupId = testGroupId("sasl"),
        topics = @[topic],
        clientId = "test-consumer-sasl",
      )
      config.saslMechanism = scramMechanism(scramSha512)
      config.offsetResetPolicy = orpEarliest
      config.autoCommit = false

      let consumer = newKafkaConsumer(config)
      await consumer.start()

      let records = await pollBatch(consumer, 1, seconds(30))
      var found = false
      for rec in records:
        if rec.value.isSome and toString(rec.value.get) == "sasl-roundtrip":
          found = true
      check found

      await consumer.close()

    waitFor testBody()

# --- SASL over TLS against the broker's SASL_SSL listener -------------------
#
# The listener offers PLAIN over TLS (port 9095). This is the only path that
# exercises certificate verification against a real broker: the CA is written
# by the local development deployment's certificate service and git-ignored,
# so a missing file means the stack was never started. PLAIN's TLS requirement
# is what makes the verification load-bearing — `allowPlaintext` is deliberately
# *not* passed here.

const
  # `localhost`, matching the listener's advertised address: BearSSL verifies
  # against the certificate's dNSName SANs, so a client dialling the IP literal
  # fails verification even though the certificate also lists `127.0.0.1`.
  SaslSslHost = "localhost"
  SaslSslPort = 9095
  SaslSslBroker = SaslSslHost & ":" & $SaslSslPort
  # Resolved from this file, not from the working directory: the suite is also
  # run as a compiled binary, from wherever the caller happens to stand.
  CaCertPath = parentDir(parentDir(currentSourcePath)) / "docker" / "certs" / "ca.crt"

proc saslSslTlsConfig(): TlsConfig =
  ## Verification on, anchored at the integration broker's own CA.
  if not fileExists(CaCertPath):
    raise newException(
      KafkaError,
      "missing " & CaCertPath & "; run `docker compose up` so the certgen " &
        "service writes the integration broker's certificate",
    )
  newTlsConfig(trustAnchors = newTrustAnchorStoreFromPem(readFile(CaCertPath)))

suite "Integration: SASL over TLS":
  test "PLAIN authenticates over a verified TLS connection":
    proc testBody() {.async: (raises: [Exception]).} =
      let broker = newBroker(
        -1,
        SaslSslHost,
        SaslSslPort,
        "test-sasl-ssl",
        tlsConfig = saslSslTlsConfig(),
        saslMechanism = newSaslPlain(SaslPlainUser, SaslPlainPassword),
      )
      await broker.connect()
      check broker.isConnected
      check broker.apiVersions.len > 0

      let meta = await broker.metadata(none(seq[string]), false)
      check meta.brokers.len == 1
      # The broker advertises the SASL_SSL listener, so the connections a client
      # opens from this metadata authenticate over TLS as well.
      check meta.brokers[0].port == SaslSslPort

      await broker.close()

    waitFor testBody()

  test "PLAIN refuses skipVerify even over TLS":
    proc testBody() {.async: (raises: [Exception]).} =
      let broker = newBroker(
        -1,
        SaslSslHost,
        SaslSslPort,
        "test-sasl-ssl",
        tlsConfig = newTlsConfig(skipVerify = true),
        saslMechanism = newSaslPlain(SaslPlainUser, SaslPlainPassword),
      )
      var caught = false
      var caughtMsg = ""
      try:
        await broker.connect()
      except KafkaConnectionError as e:
        caught = true
        caughtMsg = e.msg
      check caught
      check "skipVerify" in caughtMsg
      check (not broker.isConnected)
      await broker.close()

    waitFor testBody()

  test "an unanchored certificate fails the handshake":
    proc testBody() {.async: (raises: [Exception]).} =
      let broker = newBroker(
        -1,
        SaslSslHost,
        SaslSslPort,
        "test-sasl-ssl",
        tlsConfig = newTlsConfig(),
        saslMechanism = newSaslPlain(SaslPlainUser, SaslPlainPassword),
      )
      var caught = false
      var caughtMsg = ""
      try:
        await broker.connect()
      except KafkaConnectionError as e:
        caught = true
        caughtMsg = e.msg
      check caught
      check "TLS handshake error" in caughtMsg
      check (not broker.isConnected)
      await broker.close()

    waitFor testBody()

  test "PLAIN over TLS carries a produce and consume round-trip":
    proc testBody() {.async: (raises: [Exception]).} =
      let topic = "test-sasl-ssl-roundtrip"
      let tlsConfig = saslSslTlsConfig()

      block createTopic:
        let cluster = newCluster(
          @[SaslSslBroker],
          "test-sasl-ssl-setup",
          tlsConfig = tlsConfig,
          saslMechanism = newSaslPlain(SaslPlainUser, SaslPlainPassword),
          allowAutoTopicCreation = true,
        )
        await cluster.refreshMetadata(some(@[topic]))
        await sleepAsync(seconds(2))
        await cluster.close()

      let producerCluster = newCluster(
        @[SaslSslBroker],
        "test-sasl-ssl-producer",
        tlsConfig = tlsConfig,
        saslMechanism = newSaslPlain(SaslPlainUser, SaslPlainPassword),
      )
      let producer = newKafkaProducer(producerCluster, acks = 1)
      discard await producer.send(topic, "sasl-ssl-roundtrip".toBytes, partition = 0)
      await producer.close()

      # The consumer opens connections of its own to the coordinator and the
      # partition leader, and each of those authenticates over TLS again.
      var config = initConsumerConfig(
        bootstrapBrokers = @[SaslSslBroker],
        groupId = testGroupId("sasl-ssl"),
        topics = @[topic],
        clientId = "test-consumer-sasl-ssl",
      )
      config.tlsConfig = tlsConfig
      config.saslMechanism = newSaslPlain(SaslPlainUser, SaslPlainPassword)
      config.offsetResetPolicy = orpEarliest
      config.autoCommit = false

      let consumer = newKafkaConsumer(config)
      await consumer.start()

      let records = await pollBatch(consumer, 1, seconds(30))
      var found = false
      for rec in records:
        if rec.value.isSome and toString(rec.value.get) == "sasl-ssl-roundtrip":
          found = true
      check found

      await consumer.close()

    waitFor testBody()
