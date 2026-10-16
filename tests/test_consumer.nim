import std/[unittest, options, tables, importutils]
from std/strutils import contains
from std/times import epochTime

from async_kafka/connection import newTlsConfig, newHostAllowlist, allowHost
from async_kafka/sasl_plain import newSaslPlain

import chronos

import async_kafka/[errors, cluster, broker]
import async_kafka/protocol/[messages, api_keys, primitives]
import async_kafka/consumer/[consumer, fetcher, group, assignment]
import async_kafka/producer/producer as kafkaProducer
import
  fake_broker, consumer_helpers, cluster_helpers, async_helpers, fetcher_helpers,
  producer_helpers

# Internal tests intentionally construct and inspect the delivery context that
# remains private in the application facade.
privateAccess(FetchedRecord)

type
  RebalanceProbe = ref object
    ## Counts the requests a coordinator fake-broker sees so a test can assert
    ## that a transient OffsetFetch failure retried only the offset fetch and
    ## never re-issued JoinGroup.
    joinCount: int
    offsetFetchCount: int

  RediscoverProbe = ref object
    ## Counts what the coordinator fake-broker sees so a test can assert that an
    ## OffsetFetch NOT_COORDINATOR retry re-resolved the coordinator (FindCoordinator
    ## issued) instead of merely dropping the cache, and never re-issued JoinGroup.
    ## The commit-side retry (OffsetCommit) reports through offsetCommitCount.
    joinCount: int
    offsetFetchCount: int
    offsetCommitCount: int
    findCoordinatorCount: int

suite "Consumer: lifecycle state":
  test "fresh consumer is in lsUninitialized state":
    let config = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:9092"],
      groupId = "test-group",
      topics = @["test-topic"],
    )
    let cluster = newCluster(@["127.0.0.1:9092"])
    let c = newKafkaConsumer(config, cluster)
    check c.state == lsUninitialized
    check not c.isClosed

  test "poll on never-started consumer raises KafkaClosedError":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:9092"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster = newCluster(@["127.0.0.1:9092"])
      let c = newKafkaConsumer(config, cluster)
      var raised = false
      try:
        discard await c.poll(milliseconds(10))
      except KafkaClosedError:
        raised = true
      check raised
      await cluster.close()

    waitFor testBody()

  test "commitSync on never-started consumer raises KafkaClosedError":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:9092"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster = newCluster(@["127.0.0.1:9092"])
      let c = newKafkaConsumer(config, cluster)
      var raised = false
      try:
        await c.commitSync()
      except KafkaClosedError:
        raised = true
      check raised
      await cluster.close()

    waitFor testBody()

  test "pollBatch on never-started consumer raises KafkaClosedError":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:9092"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster = newCluster(@["127.0.0.1:9092"])
      let c = newKafkaConsumer(config, cluster)
      var raised = false
      try:
        discard await c.pollBatch(maxRecords = 10, timeout = milliseconds(10))
      except KafkaClosedError:
        raised = true
      check raised
      await cluster.close()

    waitFor testBody()

suite "Consumer: config normalization":
  test "direct ConsumerConfig zero fields land on library defaults":
    # Regression: previously only `recordQueueSize` had a 0→default fallback,
    # so a directly built ConsumerConfig sent sessionTimeoutMs=0 to JoinGroup
    # and, with autoCommit=true, hot-looped on sleepAsync(0).
    let rawConfig = ConsumerConfig(
      bootstrapBrokers: @["127.0.0.1:9092"],
      groupId: "test-group",
      topics: @["test-topic"],
      clientId: "test",
      autoCommit: true,
    )
    let cluster = newCluster(@["127.0.0.1:9092"])
    let c = newKafkaConsumer(rawConfig, cluster)
    let normalized = c.configForTest()
    check normalized.sessionTimeoutMs == 30000
    check normalized.heartbeatIntervalMs == 3000
    check normalized.rebalanceTimeoutMs == 60000
    check normalized.maxWaitMs == 500
    check normalized.minFetchBytes == 1
    check normalized.maxFetchBytes == 52428800
    check normalized.maxPartitionFetchBytes == 1048576
    check normalized.maxTotalDecompressedBytes == defaultMaxTotalDecompressedBytes
    check c.fetcherForTest().maxTotalDecompressedBytes ==
      defaultMaxTotalDecompressedBytes
    check normalized.autoCommitIntervalMs == 5000
    check normalized.recordQueueSize == 1000
    # "0 disables" fields must survive untouched.
    check normalized.maxPollIntervalMs == 0
    check normalized.metadataMaxAgeMs == 0
    check normalized.maxFetcherRestartAttempts == 0
    waitFor cluster.close()

  test "explicit non-zero fields are preserved":
    let rawConfig = ConsumerConfig(
      bootstrapBrokers: @["127.0.0.1:9092"],
      groupId: "test-group",
      topics: @["test-topic"],
      clientId: "test",
      sessionTimeoutMs: 12345'i32,
      heartbeatIntervalMs: 777'i32,
      autoCommitIntervalMs: 111'i32,
      recordQueueSize: 42,
      maxTotalDecompressedBytes: 1_048_576,
    )
    let cluster = newCluster(@["127.0.0.1:9092"])
    let c = newKafkaConsumer(rawConfig, cluster)
    let normalized = c.configForTest()
    check normalized.sessionTimeoutMs == 12345'i32
    check normalized.heartbeatIntervalMs == 777'i32
    check normalized.autoCommitIntervalMs == 111'i32
    check normalized.recordQueueSize == 42
    check normalized.maxTotalDecompressedBytes == 1_048_576
    check c.fetcherForTest().maxTotalDecompressedBytes == 1_048_576
    check normalized.rebalanceTimeoutMs == 60000
    waitFor cluster.close()

  test "initConsumerConfig keeps the zero decompression-budget sentinel":
    let cfg = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:9092"], groupId = "g", topics = @["t"]
    )
    check cfg.maxTotalDecompressedBytes == 0

  test "negative maxTotalDecompressedBytes is rejected":
    let rawConfig = ConsumerConfig(
      bootstrapBrokers: @["127.0.0.1:9092"],
      groupId: "test-group",
      topics: @["test-topic"],
      clientId: "test",
      maxTotalDecompressedBytes: -1,
    )
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect(KafkaError):
      discard newKafkaConsumer(rawConfig, cluster)
    waitFor cluster.close()

  test "an injected Cluster rejects the transport settings it cannot apply":
    # Ignoring them let a config that asks for TLS dial plaintext and look safe.
    let base = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:9092"],
      groupId = "test-group",
      topics = @["test-topic"],
    )
    let cluster = newCluster(@["127.0.0.1:9092"])

    var withTls = base
    withTls.tlsConfig = newTlsConfig()
    expect(KafkaError):
      discard newKafkaConsumer(withTls, cluster)

    var withSasl = base
    withSasl.saslMechanism = newSaslPlain("user", "password", allowPlaintext = true)
    expect(KafkaError):
      discard newKafkaConsumer(withSasl, cluster)

    var withAllowlist = base
    withAllowlist.hostAllowlist = newHostAllowlist(@[allowHost("127.0.0.1")])
    expect(KafkaError):
      discard newKafkaConsumer(withAllowlist, cluster)

    # The overload that owns its Cluster reads all three, so it stays accepted.
    var owned = base
    owned.tlsConfig = newTlsConfig()
    owned.saslMechanism = newSaslPlain("user", "password", allowPlaintext = true)
    owned.hostAllowlist = newHostAllowlist(@[allowHost("127.0.0.1")])
    let consumer = newKafkaConsumer(owned)
    check consumer.ownsCluster
    waitFor consumer.close()
    waitFor cluster.close()

suite "Consumer: close idempotence":
  test "ownsCluster distinguishes an injected Cluster from an owned one":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:9092"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster = newCluster(@["127.0.0.1:9092"])
      let injected = newKafkaConsumer(config, cluster)
      check not injected.ownsCluster
      # The caller owns an injected Cluster and closes it after the consumer.
      let owned = newKafkaConsumer(config)
      check owned.ownsCluster
      await owned.close()
      await injected.close()
      await cluster.close()

    waitFor testBody()

  test "close is idempotent on never-started consumer":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:9092"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster = newCluster(@["127.0.0.1:9092"])
      let c = newKafkaConsumer(config, cluster)
      await c.close()
      check c.isClosed
      check c.state == lsClosed
      # Second close must not crash and must keep state at lsClosed.
      await c.close()
      check c.state == lsClosed
      await cluster.close()

    waitFor testBody()

  test "a cancel while the fetcher is stopped still closes the consumer":
    ## Regression: fetcher.stop() awaited `cancelAndWait` unguarded, so a close()
    ## cancelled at that await unwound with the consumer left in lsClosing (close()
    ## never re-runs closeImpl) and its owned Cluster still open.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:9092"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let c = newKafkaConsumer(config)
      check c.ownsCluster
      let cluster = c.clusterForTest()
      c.markRunning()
      # An armed loop with no assignment parks in fetchOnce's empty-assignment
      # sleep (queueFull stays false), so the fetcher's stop stage still has a
      # loop to reap.
      c.fetcherForTest().start()

      # Park the fetcher-stop stage, then cancel the close while it is inside it.
      discard c.armCloseFetcherParkForTest()
      let closeFut = c.close()
      while not c.closeFetcherParkedForTest():
        await sleepAsync(milliseconds(1))
      closeFut.cancelSoon()
      check awaitCompleted(closeFut, milliseconds(5000))

      check c.state == lsClosed
      check cluster.isClosed
      # The stage kept going after the cancel: the fetch loop was reaped.
      check not c.fetcherForTest().isLoopAlive()

    waitFor testBody()

  test "API calls after close raise KafkaClosedError":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:9092"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster = newCluster(@["127.0.0.1:9092"])
      let c = newKafkaConsumer(config, cluster)
      await c.close()

      var pollRaised = false
      try:
        discard await c.poll(milliseconds(10))
      except KafkaClosedError:
        pollRaised = true
      check pollRaised

      var commitRaised = false
      try:
        await c.commitSync()
      except KafkaClosedError:
        commitRaised = true
      check commitRaised

      await cluster.close()

    waitFor testBody()

  test "start after close raises KafkaClosedError":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:9092"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster = newCluster(@["127.0.0.1:9092"])
      let c = newKafkaConsumer(config, cluster)
      await c.close()

      var raised = false
      try:
        await c.start()
      except KafkaClosedError:
        raised = true
      check raised

      await cluster.close()

    waitFor testBody()

suite "Consumer: tryPoll":
  test "returns some(record) when the fetcher has one":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.fetcherForTest().enqueueRecordForTest(
        FetchedRecord(topic: "test-topic", partition: 0'i32, offset: 7'i64)
      )

      let got = await c.tryPoll(2.seconds)
      check got.isSome
      check got.get().offset == 7'i64
      check got.get().topic == "test-topic"

      await c.close()
      await cluster.close()

    waitFor testBody()

  test "returns none on a deadline instead of raising KafkaTimeoutError":
    ## The whole point of tryPoll: `poll`'s KafkaTimeoutError becomes none so a
    ## caller can loop without exception handling.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      # No fetcher records arrive, so the deadline elapses.
      let got = await c.tryPoll(milliseconds(100))
      check got.isNone

      await cluster.close()

    waitFor testBody()

suite "Consumer: pollBatch timeout semantics":
  test "pollBatch returns empty seq when zero records collected":
    ## pollBatch is the lenient variant: a deadline elapsing without any
    ## records returns an empty seq so callers can poll in a loop without
    ## exception handling. Use pollBatchStrict to distinguish "no records
    ## right now" from "0 records is the answer".
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      # Fetcher is not started here, and no records will arrive — the
      # deadline must elapse and produce an empty seq (not an exception).

      var recordCount = -1
      var raised = false
      try:
        let records = await c.pollBatch(maxRecords = 5, timeout = milliseconds(150))
        recordCount = records.len
      except KafkaError:
        raised = true
      check (not raised)
      check recordCount == 0
      await cluster.close()

    waitFor testBody()

  test "pollBatchStrict raises KafkaTimeoutError when zero records collected":
    ## pollBatchStrict mirrors poll()'s convention — a deadline elapsing
    ## without any records becomes KafkaTimeoutError so the caller can tell
    ## "no records right now" apart from "0 records is the answer".
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      var timedOut = false
      try:
        discard await c.pollBatchStrict(maxRecords = 5, timeout = milliseconds(150))
      except KafkaTimeoutError:
        timedOut = true
      check timedOut
      await cluster.close()

    waitFor testBody()

  test "pollBatch with maxRecords = 0 returns empty immediately":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      let start = Moment.now()
      let records = await c.pollBatch(maxRecords = 0, timeout = seconds(10))
      let elapsed = Moment.now() - start
      check records.len == 0
      check elapsed < seconds(1)
      await cluster.close()

    waitFor testBody()

  test "pollBatchStrict with maxRecords = 0 returns empty immediately (not timeout)":
    ## A 0-record request is vacuously satisfied — it must not raise
    ## KafkaTimeoutError.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      let start = Moment.now()
      let records = await c.pollBatchStrict(maxRecords = 0, timeout = seconds(10))
      let elapsed = Moment.now() - start
      check records.len == 0
      check elapsed < seconds(1)
      await cluster.close()

    waitFor testBody()

suite "Consumer: poll deadline during rebalance":
  ## An in-flight rebalance handshake (revoke → JoinGroup → SyncGroup →
  ## re-fetch initial offsets) shares the caller's `timeout` budget: a
  ## failed/slow rebalance must still return by the deadline, never block
  ## past it.

  test "poll honors timeout when rebalance retries fail":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.requestRebalance()

      let timeoutMs = 300
      let startMs = epochTime()
      var raised = false
      try:
        discard await c.poll(milliseconds(timeoutMs))
      except KafkaError:
        raised = true
      let elapsedMs = (epochTime() - startMs) * 1000
      # The call must return within a small multiple of the requested
      # timeout — the rebalance budget MUST NOT overshoot the user's
      # deadline.
      check elapsedMs < 1200.0
      discard raised
      await cluster.close()

    waitFor testBody()

  test "pollBatchStrict honors timeout when rebalance retries fail":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.requestRebalance()

      let timeoutMs = 300
      let startMs = epochTime()
      var raised = false
      try:
        discard
          await c.pollBatchStrict(maxRecords = 10, timeout = milliseconds(timeoutMs))
      except KafkaError:
        raised = true
      let elapsedMs = (epochTime() - startMs) * 1000
      check elapsedMs < 1200.0
      discard raised
      await cluster.close()

    waitFor testBody()

  test "a slow rejoin is parked across the poll deadline, not cancelled":
    ## A JoinGroup handshake slower than the poll timeout must return within
    ## budget AND leave the rejoin parked on c.rejoinFut for the next poll to
    ## resume — cancelling it mid-handshake would advance the broker generation
    ## and churn the group into another rebalance.
    proc noReply(
        server: StreamServer, transport: StreamTransport
    ) {.async: (raises: []).} =
      # Accept the TCP connection but never answer, so the coordinator
      # handshake the rejoin needs hangs far longer than the poll timeout.
      try:
        while not transport.closed:
          await sleepAsync(milliseconds(20))
      except CancelledError:
        discard
      await transport.closeWait()

    proc testBody() {.async: (raises: [Exception]).} =
      let address = initTAddress("127.0.0.1:0")
      let server = createStreamServer(address, noReply, flags = {ReuseAddr})
      server.start()
      let boundAddr = server.localAddress()
      let bootstrap = "127.0.0.1:" & $boundAddr.port
      let config = initConsumerConfig(
        bootstrapBrokers = @[bootstrap], groupId = "g", topics = @["t"]
      )
      # Long timeouts so the handshake does not fail fast — it must still be in
      # flight when the short poll deadline elapses.
      let cluster =
        newCluster(@[bootstrap], connectTimeoutMs = 30000, requestTimeoutMs = 30000)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.requestRebalance()

      let startMs = epochTime()
      discard await c.pollBatch(maxRecords = 10, timeout = milliseconds(200))
      let elapsedMs = (epochTime() - startMs) * 1000
      check elapsedMs < 1500.0
      # The rejoin was parked, not cancelled — still in flight for the next poll.
      check c.rejoinInFlightForTest()

      # close() tears the parked rejoin down so it cannot outlive the consumer.
      await c.close()
      check not c.rejoinInFlightForTest()

      server.stop()
      await server.closeWait()
      await cluster.close()

    waitFor testBody()

  test "a close that clears a finished parked rejoin does not crash the poll":
    ## Regression: the parked poll re-read `c.rejoinFut` after the race await.
    ## close() clears that field for a *finished* rejoin (there is nothing left to
    ## cancel), so a close that ran in the window between the join completing and
    ## the parked poll resuming turned that read into a nil deref. The poll now
    ## holds its own handle across the await.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.requestRebalance()

      # A rejoin the test finishes by hand, standing in for the JoinGroup +
      # SyncGroup round-trip.
      let rejoinFut = Future[seq[TopicPartition]]
        .Raising([KafkaError, CancelledError])
        .init("test-parked-rejoin")
      c.injectRejoinFutForTest(rejoinFut)

      let pollFut = c.poll(milliseconds(5000))
      await sleepAsync(milliseconds(20)) # let the poll park on the race
      check not pollFut.finished
      check c.rejoinInFlightForTest()

      # Finish the join, then start close() in the same scheduler turn: the
      # parked poll's resumption is queued behind this call, and close reaches
      # the rejoin teardown before it yields, so the field is already cleared by
      # the time the poll continues.
      rejoinFut.complete(@[])
      let closeFut = c.close()

      var outcome = "returned"
      try:
        discard await pollFut
      except KafkaClosedError:
        outcome = "closed"
      except CancelledError:
        outcome = "cancelled"
      # The assignment was joined, but the consumer is closing: the documented
      # outcome is the closed rejection, not a crash and not a stale success.
      check outcome == "closed"

      await closeFut
      check not c.rejoinInFlightForTest()
      await cluster.close()

    waitFor testBody()

suite "Consumer: close during the pre-rebalance commit":
  test "a close that lands while onJoinPrepare commits does not start a join":
    ## maybeRejoin awaits onJoinPrepare (the pre-rebalance commit) before it
    ## starts JoinGroup. close() can run to completion inside that await: it
    ## tears down the rejoin it finds — there is none yet, the join has not
    ## started — and LeaveGroups the membership. A join started after the await
    ## would re-register the membership the close just dropped, so the state
    ## re-check must return instead. Probed by parking the commit and counting
    ## the JoinGroup the broker sees.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let probe = RebalanceProbe()
      let commitSeen = newFuture[void]("pre-rebalance-commit-seen")
      let release = newFuture[void]("release-pre-rebalance-commit")

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            let key = req.apiKey
            if key == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akFindCoordinator)),
                      minVersion: 0,
                      maxVersion: 1,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akOffsetCommit)), minVersion: 0, maxVersion: 3
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akJoinGroup)), minVersion: 0, maxVersion: 2
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akSyncGroup)), minVersion: 0, maxVersion: 1
                    ),
                  ],
                )
              )
            elif key == int16(ord(akMetadata)):
              await req.respond(
                buildMetadataResponseBody(
                  [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
                  controllerId = 0'i32,
                  topics = [
                    FakeTopicMeta(
                      name: "t",
                      errorCode: 0'i16,
                      isInternal: false,
                      partitions: @[
                        FakePartitionMeta(
                          partitionIndex: 0'i32,
                          leaderId: 0'i32,
                          replicas: @[0'i32],
                          isr: @[0'i32],
                        )
                      ],
                    )
                  ],
                )
              )
            elif key == int16(ord(akFindCoordinator)):
              # Served so a mutated run (guard removed) reaches JoinGroup and
              # the count below, instead of stalling on coordinator discovery.
              await req.respond(
                buildFindCoordinatorResponseBodyV1(
                  nodeId = 0'i32, host = "127.0.0.1", port = port
                )
              )
            elif key == int16(ord(akOffsetCommit)):
              if not commitSeen.finished:
                commitSeen.complete()
              # Park the pre-rebalance commit until the test has closed the
              # consumer. Bounded so a mutation cannot wedge the handler.
              try:
                await release.wait(2.seconds)
              except AsyncTimeoutError:
                discard
              await req.respond(
                buildOffsetCommitResponseBodyV3(
                  topic = "t", partition = 0'i32, errorCode = keNone.toInt16
                )
              )
            elif key == int16(ord(akJoinGroup)):
              # The regression under test: a JoinGroup for a consumer whose
              # close() already ran. Dropped, so the buggy path times out.
              inc probe.joinCount
            # Anything else: drop silently.
          except CatchableError:
            break

      let handlerFut = handler()

      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:" & $port], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:" & $port], requestTimeoutMs = 1000)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.requestRebalance()
      c.setGroupCoordinatorForTest(newBroker(0'i32, "127.0.0.1", port, "coord"))
      c.setGroupGenerationForTest(7'i32)

      let tp = TopicPartition(topic: "t", partition: 0)
      let f = c.fetcherForTest()
      f.registerPartitionForTest(tp, 0)
      f.bumpGeneration(7'i32)
      f.advanceConsumedOffset(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 4'i64)
      )

      let pollFut = c.poll(milliseconds(5000))
      # The poll is parked inside onJoinPrepare's commit; only this test can
      # let it finish.
      await commitSeen.wait(2.seconds)
      check not pollFut.finished

      # close() runs synchronously up to lsClosing, then parks on the commit
      # lock this poll holds. Releasing afterwards lets maybeRejoin resume
      # against a consumer that is already shutting down.
      let closeFut = c.close()
      check c.state == lsClosing
      release.complete()

      var outcome = "returned"
      try:
        discard await pollFut.wait(2.seconds)
      except AsyncTimeoutError:
        outcome = "hung"
      except KafkaClosedError:
        outcome = "closed"
      except KafkaError:
        outcome = "error"
      except CancelledError:
        outcome = "cancelled"
      check outcome == "closed"
      # The guard: no JoinGroup may be issued once close() took the consumer
      # out of lsRunning.
      check probe.joinCount == 0

      await closeFut
      check c.state == lsClosed
      if not pollFut.finished:
        # Mutation-only path (the guard removed): reap the stray join.
        try:
          await pollFut.cancelAndWait()
        except CancelledError:
          discard
      await cluster.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Consumer: pollBatch deadline during rebalance":
  test "pollBatch honors timeout when rebalance retries fail":
    ## Regression: handleRebalanceIfNeeded slept an uncapped 500ms * attempt
    ## between retries, so a pollBatch(timeout=300ms) caller could block 1.5s+
    ## on failed rebalances. The deadline now caps the inter-attempt backoff at
    ## the remaining time and the retry loop bails early.
    proc testBody() {.async: (raises: [Exception]).} =
      # Point at a refused port so coordinator lookup / joinAndSync fails
      # fast on every retry — what we want to measure is the *sleep* bound.
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.requestRebalance()

      let timeoutMs = 300
      let startMs = epochTime()
      var raised = false
      try:
        discard await c.pollBatch(maxRecords = 10, timeout = milliseconds(timeoutMs))
      except KafkaError:
        # handleRebalanceIfNeeded may raise the last retry error; that is
        # part of the contract — the caller is free to retry. What matters
        # for this regression is that we returned within the deadline.
        raised = true
      let elapsedMs = (epochTime() - startMs) * 1000
      # Pre-fix floor was ~1500ms (500+1000 sleeps). After the fix the
      # whole call must stay within a small multiple of the requested
      # timeout.
      check elapsedMs < 1200.0
      discard raised # either raised or returned empty is acceptable
      await cluster.close()

    waitFor testBody()

suite "Consumer: close mid-flight":
  test "poll mid-flight surfaces KafkaClosedError after close":
    # Regression: close() flips state mid-flight; the in-flight poll() must not
    # hang past its own timeout. After the wait deadline elapses the post-wait
    # requireRunning() raises KafkaClosedError, and the call returns within a
    # bounded window (no future leaked on the empty record queue).
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      let pollFut = c.poll(milliseconds(200))
      await sleepAsync(milliseconds(50))
      await c.close()

      let startMs = epochTime()
      var raised = false
      var timedOut = false
      try:
        discard await pollFut
      except KafkaClosedError:
        raised = true
      except KafkaTimeoutError:
        timedOut = true
      except KafkaError:
        discard
      except CancelledError:
        raised = true
      let elapsedMs = (epochTime() - startMs) * 1000
      # Either outcome is acceptable as long as the call returned within a
      # small window after the poll timeout (200ms total).
      check raised or timedOut
      check elapsedMs < 600.0
      check c.state == lsClosed
      await cluster.close()

    waitFor testBody()

  test "pollBatch mid-flight returns within the deadline after close":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      let batchFut = c.pollBatch(maxRecords = 10, timeout = milliseconds(200))
      await sleepAsync(milliseconds(50))
      await c.close()

      let startMs = epochTime()
      try:
        discard await batchFut
      except KafkaError:
        discard
      except CancelledError:
        discard
      let elapsedMs = (epochTime() - startMs) * 1000
      check elapsedMs < 600.0
      check c.state == lsClosed
      await cluster.close()

    waitFor testBody()

suite "Consumer: commitOffsets is pure (no rebalance side effect)":
  ## Regression: commitOffsets() used to arm rebalanceRequested inline on an
  ## ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID partition error, hiding the side
  ## effect. It is now pure — the error rides in KafkaCommitError.partitionErrors
  ## and callers arm the flag via armRebalanceFromCommitError.

  test "armRebalanceFromCommitError sets the flag on ILLEGAL_GENERATION":
    let config = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    let cluster = newCluster(@["127.0.0.1:1"])
    let c = newKafkaConsumer(config, cluster)
    check c.rebalanceRequestedForTest() == false
    let err = newKafkaCommitError(
      keIllegalGeneration,
      "OffsetCommit error",
      @[PartitionCommitError(topic: "t", partition: 0, errorCode: keIllegalGeneration)],
    )
    c.armRebalanceFromCommitError(err)
    check c.rebalanceRequestedForTest() == true
    waitFor cluster.close()

  test "armRebalanceFromCommitError ignores non-rebalance codes":
    let config = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    let cluster = newCluster(@["127.0.0.1:1"])
    let c = newKafkaConsumer(config, cluster)
    let err = newKafkaCommitError(
      keOffsetMetadataTooLarge,
      "OffsetCommit error",
      @[
        PartitionCommitError(
          topic: "t", partition: 0, errorCode: keOffsetMetadataTooLarge
        )
      ],
    )
    c.armRebalanceFromCommitError(err)
    check c.rebalanceRequestedForTest() == false
    waitFor cluster.close()

  test "armRebalanceFromCommitError sets flag on UNKNOWN_MEMBER_ID":
    let config = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    let cluster = newCluster(@["127.0.0.1:1"])
    let c = newKafkaConsumer(config, cluster)
    let err = newKafkaCommitError(
      keUnknownMemberId,
      "OffsetCommit error",
      @[PartitionCommitError(topic: "t", partition: 0, errorCode: keUnknownMemberId)],
    )
    c.armRebalanceFromCommitError(err)
    check c.rebalanceRequestedForTest() == true
    waitFor cluster.close()

suite "Consumer: commit gate keys on generation, not gsStable":
  test "commit is attempted while gsRebalancing with a valid generation":
    ## Regression: the pre-rebalance commit was gated on gsStable, but the
    ## heartbeat loop flips to gsRebalancing on REBALANCE_IN_PROGRESS before it
    ## runs, silently no-op'ing the commit every rebalance. The gate now keys on
    ## a valid generation; probe: a gate-passing commit raises against the dead
    ## coordinator (a gsStable regression would no-op, raised == false).
    proc testBody() {.async: (raises: [Exception]).} =
      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      config.sessionTimeoutMs = 1000'i32
      config.heartbeatIntervalMs = 300'i32
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      # Unreachable coordinator + valid generation + non-stable state.
      let coord = newBroker(
        nodeId = 1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 50,
        connectTimeoutMs = 50,
      )
      c.setGroupCoordinatorForTest(coord)
      c.setGroupGenerationForTest(7'i32)
      c.setGroupStateForTest(gsRebalancing)

      # Inject a consumed offset so commitOffsets has something to commit
      # (otherwise it returns early on an empty offset set, before the gate
      # matters).
      let tp = TopicPartition(topic: "t", partition: 0)
      let f = c.fetcherForTest()
      f.registerPartitionForTest(tp, 0)
      f.advanceConsumedOffset(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 4'i64)
      )
      check f.getConsumedOffsets().len == 1
      # The pre-rebalance commit runs while the fetcher's assignment is still on
      # the CURRENT generation (joinAndSync has not yet advanced it), so the
      # fetcher generation matches the group's — otherwise the generation-window
      # gate (see the "generation lags" test below) would skip the commit for a
      # different reason and this gsStable-vs-generation probe would be moot.
      f.bumpGeneration(7'i32)

      var raised = false
      try:
        await c.commitSync()
      except KafkaError:
        raised = true
      check raised # gate passed -> RPC attempted -> failed against dead coordinator

      await c.close()
      await coord.close()
      await cluster.close()

    waitFor testBody()

  test "commit is skipped while the fetcher's assignment generation lags the group":
    ## Fetcher's assignmentGeneration lags group.generationId in the rebalance
    ## window; commit under the new gen would rewind the new owner. Skip must
    ## surface as KafkaCommitSkippedError (previously silent).
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      # Unreachable coordinator: any commit RPC that passes the gate raises a
      # transport error (same probe the gsStable-gate test above uses).
      let coord = newBroker(
        nodeId = 1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 50,
        connectTimeoutMs = 50,
      )
      c.setGroupCoordinatorForTest(coord)

      # The previous assignment's consumed offset is still parked in the fetcher
      # (fetcher.stop() does not clear f.partitions across a rebalance).
      let tp = TopicPartition(topic: "t", partition: 0)
      let f = c.fetcherForTest()
      f.registerPartitionForTest(tp, 0)
      f.advanceConsumedOffset(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 4'i64)
      )
      check f.getConsumedOffsets().len == 1

      # Post-rebalance window: the group is on generation 8, but the fetcher's
      # assignment is still tagged with generation 7 because the next poll has
      # not yet run setPartitions + bumpGeneration.
      c.setGroupGenerationForTest(8'i32)
      f.bumpGeneration(7'i32)

      var skipped = false
      try:
        await c.commitSync()
      except KafkaCommitSkippedError:
        skipped = true
      check skipped

      # Positive control: after fetcher catches up the same commit hits the
      # dead coordinator and raises a non-skip error.
      f.bumpGeneration(8'i32)
      var raisedNonSkip = false
      try:
        await c.commitSync()
      except KafkaCommitSkippedError:
        discard
      except KafkaError:
        raisedNonSkip = true
      check raisedNonSkip

      await c.close()
      await coord.close()
      await cluster.close()

    waitFor testBody()

suite "Consumer: commitSync defers the position advance until the broker acks":
  test "a raised commit leaves pendingAdvance and the consumed offset untouched":
    ## Regression: commitSync flushed pendingAdvance BEFORE awaiting
    ## commitOffsets with no rollback, so a raised commit left un-acked offsets
    ## exposed for a later auto-commit to commit behind the caller's back. The
    ## advance now happens only after the broker acks, so a failed commitSync
    ## leaves the position where the caller last observed it.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      # Unreachable coordinator: a commit that passes the gate raises a transport
      # error (same probe the generation-gate tests above use).
      let coord = newBroker(
        nodeId = 1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 50,
        connectTimeoutMs = 50,
      )
      c.setGroupCoordinatorForTest(coord)
      c.setGroupGenerationForTest(7'i32)

      # A record was handed out by a prior poll, but its advance is still deferred
      # in pendingAdvance: consumedOffset has not moved (everConsumed is still
      # false, so getConsumedOffsets is empty).
      let tp = TopicPartition(topic: "t", partition: 0)
      let f = c.fetcherForTest()
      f.registerPartitionForTest(tp, 0)
      f.bumpGeneration(7'i32)
      c.injectPendingAdvanceForTest(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 4'i64)
      )
      check c.pendingAdvanceLenForTest() == 1
      check f.getConsumedOffsets().len == 0

      var raised = false
      try:
        await c.commitSync()
      except KafkaError:
        raised = true
      check raised # commit RPC attempted against the dead coordinator

      # The failed commit must NOT have advanced the position: the deferred
      # advance is still pending and the committable offset table is still empty,
      # so a subsequent auto-commit has nothing to commit.
      check c.pendingAdvanceLenForTest() == 1
      check f.getConsumedOffsets().len == 0

      await c.close()
      await coord.close()
      await cluster.close()

    waitFor testBody()

  test "a successful commit advances the position and drains pendingAdvance":
    ## Positive control for the regression above: once the broker accepts the
    ## commit, the deferred advance IS applied — pendingAdvance drains and the
    ## committable offset moves to offset + 1.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akOffsetCommit)), minVersion: 0, maxVersion: 3
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          let oc = await fb.nextRequest(timeout = 2.seconds)
          try:
            await oc.respond(
              buildOffsetCommitResponseBodyV3(
                topic = "t", partition = 0'i32, errorCode = 0'i16
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      let coord = newBroker(0'i32, "127.0.0.1", port, "coord")
      c.setGroupCoordinatorForTest(coord)
      c.setGroupGenerationForTest(7'i32)

      let tp = TopicPartition(topic: "t", partition: 0)
      let f = c.fetcherForTest()
      f.registerPartitionForTest(tp, 0)
      f.bumpGeneration(7'i32)
      c.injectPendingAdvanceForTest(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 4'i64)
      )

      await c.commitSync()

      # Broker acked -> the deferred advance is applied: pendingAdvance is empty
      # and the committable offset is now offset + 1.
      check c.pendingAdvanceLenForTest() == 0
      let consumed = f.getConsumedOffsets()
      check consumed.len == 1
      check consumed[tp] == 5'i64

      # Drop the coordinator so close()'s final commit short-circuits instead of
      # re-hitting the now-idle fake broker.
      c.setGroupCoordinatorForTest(nil)
      await c.close()
      await coord.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "a fenced commitSync arms rebalanceRequested before raising":
    ## Regression: commitSync was the only commit path that did not arm
    ## rebalanceRequested on an ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID partition
    ## error — recovery then depended on the heartbeat interval, extending the
    ## stale delivery window. It now mirrors autoCommitLoop / onJoinPrepare /
    ## commitAsync: arm the flag, then raise.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akOffsetCommit)), minVersion: 0, maxVersion: 3
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          let oc = await fb.nextRequest(timeout = 2.seconds)
          try:
            await oc.respond(
              buildOffsetCommitResponseBodyV3(
                topic = "t",
                partition = 0'i32,
                errorCode = int16(ord(keIllegalGeneration)),
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      let coord = newBroker(0'i32, "127.0.0.1", port, "coord")
      c.setGroupCoordinatorForTest(coord)
      c.setGroupGenerationForTest(7'i32)

      let tp = TopicPartition(topic: "t", partition: 0)
      let f = c.fetcherForTest()
      f.registerPartitionForTest(tp, 0)
      f.bumpGeneration(7'i32)
      c.injectPendingAdvanceForTest(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 4'i64)
      )

      check c.rebalanceRequestedForTest() == false
      var raised = false
      try:
        await c.commitSync()
      except KafkaCommitError:
        raised = true
      check raised

      # The fenced commit armed the rebalance for the next poll AND left the
      # deferred advance unflushed (no rollback-free commit happened).
      check c.rebalanceRequestedForTest() == true
      check c.pendingAdvanceLenForTest() == 1

      # Drop the coordinator so close()'s final commit short-circuits instead of
      # re-hitting the now-idle fake broker.
      c.setGroupCoordinatorForTest(nil)
      await c.close()
      await coord.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Consumer: fetcher restart escalation":
  test "tryRestartFetcher escalates after maxFetcherRestartAttempts":
    ## Regression: a permanent fetch error made tryRestartFetcher loop forever on
    ## its 5s cooldown, never surfacing to poll(). poll now raises after
    ## maxFetcherRestartAttempts consecutive failed restarts.
    proc testBody() {.async: (raises: [Exception]).} =
      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      config.maxFetcherRestartAttempts = 3
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.fetcherForTest().setLastError(newException(KafkaError, "AUTHORIZATION_FAILED"))

      # Three restarts succeed (just call start, no actual fetch happens).
      # We must fast-forward past the 5-second cooldown between attempts.
      for i in 1 .. 3:
        c.fetcherForTest().lastRestartAttempt = Moment.now() - seconds(10)
        # Counter still below cap → restart is attempted, no raise.
        c.tryRestartFetcherForTest()
        # Re-arm the failure so the next attempt also sees a stopped fetcher.
        await c.fetcherForTest().stop()
        c.fetcherForTest().setLastError(
          newException(KafkaError, "AUTHORIZATION_FAILED")
        )

      check c.fetcherForTest().consecutiveRestartAttempts == 3

      # Fourth attempt should escalate: counter == cap.
      c.fetcherForTest().lastRestartAttempt = Moment.now() - seconds(10)
      var raised = false
      var msg = ""
      try:
        c.tryRestartFetcherForTest()
      except KafkaError as e:
        raised = true
        msg = e.msg
      check raised
      check "unrecoverable" in msg
      check "AUTHORIZATION_FAILED" in msg
      await c.close()
      await cluster.close()

    waitFor testBody()

  test "consecutiveRestartAttempts resets to 0 on successful fetch":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      # Pretend a few restarts happened.
      c.fetcherForTest().consecutiveRestartAttempts = 5
      # Simulate the fetchFromBroker "anySucceeded" reset.
      c.fetcherForTest().lastError = nil
      c.fetcherForTest().consecutiveRestartAttempts = 0
      check c.fetcherForTest().consecutiveRestartAttempts == 0
      await c.close()
      await cluster.close()

    waitFor testBody()

  test "permanent KafkaApiError escalates without consuming attempts":
    ## tryRestartFetcher must skip the 5s × N cooldown when the last fetch error
    ## is an instance-fatal ApiError (isInstanceFatal): AUTHORIZATION_FAILED,
    ## UNSUPPORTED_VERSION and friends raise on the first call rather than
    ## burning the full maxFetcherRestartAttempts budget on a hopeless retry. A
    ## record-level ecFatal code keeps the cooldown instead.
    proc testBody() {.async: (raises: [Exception]).} =
      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      config.maxFetcherRestartAttempts = 10
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.fetcherForTest().setLastError(
        newKafkaApiError(
          keTopicAuthorizationFailed, "Topic authorization failed for [t]"
        )
      )

      var raised = false
      var msg = ""
      try:
        c.tryRestartFetcherForTest()
      except KafkaApiError as e:
        raised = true
        msg = e.msg
      check raised
      check kafkaErrorName(keTopicAuthorizationFailed) in msg or "authorization" in msg
      # Attempt counter must not have been consumed by the fast-path
      # escalation — restarting from a hopeless permanent error should
      # not eat retries reserved for transient failures.
      check c.fetcherForTest().consecutiveRestartAttempts == 0

      await c.close()
      await cluster.close()

    waitFor testBody()

  test "tryRestartFetcher cooldown error reports restart progress":
    ## Within the 5s restart cooldown, the error surfaced to poll() must report
    ## restart progress (attempt/cap, cooling down) so a caller can tell a
    ## transient retry from an imminent give-up, not just "fetcher stopped".
    proc testBody() {.async: (raises: [Exception]).} =
      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      config.maxFetcherRestartAttempts = 5
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      # Non-API KafkaError (not isPermanent) so the fast-path escalation is
      # skipped and we land on the cooldown branch.
      c.fetcherForTest().setLastError(
        newException(KafkaError, "transient fetch glitch")
      )
      # A recent restart attempt puts us inside the 5s cooldown window.
      c.fetcherForTest().lastRestartAttempt = Moment.now()

      var raised = false
      var msg = ""
      try:
        c.tryRestartFetcherForTest()
      except KafkaError as e:
        raised = true
        msg = e.msg
      check raised
      check "cooling down" in msg
      check "/5" in msg # cap progress
      check "transient fetch glitch" in msg

      await c.close()
      await cluster.close()

    waitFor testBody()

  test "alive fetch loop with lastError surfaces without incrementing counter":
    # Regression: start() no-ops on running=true, so repeated tryRestartFetcher
    # calls used to inflate consecutiveRestartAttempts without any real restart.
    proc testBody() {.async: (raises: [Exception]).} =
      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      config.maxFetcherRestartAttempts = 3
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      let liveFut = newFuture[void]("fake-fetch-loop")
      c.fetcherForTest().injectFetchLoopFutForTest(liveFut)
      c.fetcherForTest().setRunningForTest(true)
      c.fetcherForTest().setLastError(
        newException(KafkaError, "persistent fetch failure after 5 consecutive errors")
      )

      var raiseCount = 0
      for _ in 1 .. 10:
        try:
          c.tryRestartFetcherForTest()
        except KafkaError as e:
          raiseCount.inc
          check "fetcher error" in e.msg
          check "persistent fetch failure" in e.msg

      check raiseCount == 10
      check c.fetcherForTest().consecutiveRestartAttempts == 0

      liveFut.complete()
      c.fetcherForTest().setRunningForTest(false)
      c.fetcherForTest().injectFetchLoopFutForTest(nil)
      await c.close()
      await cluster.close()

    waitFor testBody()

  test "dead fetch loop with running=true normalizes state before restart":
    # Regression: fetchLoop returned but running was never cleared, so start()
    # short-circuited on running=true. tryRestartFetcher must normalize first.
    proc testBody() {.async: (raises: [Exception]).} =
      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      config.maxFetcherRestartAttempts = 3
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      let deadFut = newFuture[void]("fake-dead-loop")
      deadFut.complete()
      c.fetcherForTest().injectFetchLoopFutForTest(deadFut)
      c.fetcherForTest().setRunningForTest(true)
      c.fetcherForTest().setLastError(
        newException(KafkaError, "transient fetch glitch")
      )
      c.fetcherForTest().lastRestartAttempt = Moment.now() - seconds(10)

      c.tryRestartFetcherForTest()

      check c.fetcherForTest().consecutiveRestartAttempts == 1
      check c.fetcherForTest().isRunningForTest()
      # start() clears lastError; if it silently no-oped the error would stick.
      check c.fetcherForTest().lastError == nil

      await c.fetcherForTest().stop()
      await c.close()
      await cluster.close()

    waitFor testBody()

suite "Consumer: close drains autoCommit instead of cancelling":
  test "close awaits a slow commitLoopFut up to drainBudgetMs":
    ## Regression (OffsetCommit Schrödinger bug): close() called
    ## commitLoopFut.cancelAndWait() unconditionally, interrupting a mid-flight
    ## commit — the broker applied it but the client saw CancelledError. close now
    ## waits for the future to finish, cancelling only after the drain budget.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      # A "slow commit" that finishes after 200 ms. close() must wait for it (the
      # drain budget is max(sessionTimeoutMs - 1000, 2000) = 29000ms by default)
      # and not cancel it.
      var commitFinished = false
      proc slowCommit() {.async: (raises: []).} =
        try:
          await sleepAsync(milliseconds(200))
          commitFinished = true
        except CancelledError:
          discard

      let slow = slowCommit()
      c.injectCommitLoopFut(slow)

      let startMs = epochTime()
      await c.close()
      let elapsedMs = (epochTime() - startMs) * 1000

      check commitFinished
      check slow.finished
      check not slow.cancelled
      # Must have waited at least the slow commit's duration but not the
      # whole drain budget (29s).
      check elapsedMs >= 180.0
      check elapsedMs < 5000.0
      check c.state == lsClosed
      await cluster.close()

    waitFor testBody()

  test "close cancels commitLoopFut as fallback after drainBudgetMs":
    ## When a commit is wedged longer than the drain budget, close() must
    ## fall back to cancelAndWait so the consumer never blocks forever.
    proc testBody() {.async: (raises: [Exception]).} =
      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      config.sessionTimeoutMs = 2000'i32 # drain budget = max(2000 - 1000, 2000) = 2000ms
      config.heartbeatIntervalMs = 500'i32 # must be < sessionTimeoutMs
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      proc wedged() {.async: (raises: []).} =
        try:
          await sleepAsync(seconds(30))
        except CancelledError:
          discard

      let wedgedFut = wedged()
      c.injectCommitLoopFut(wedgedFut)

      let startMs = epochTime()
      await c.close()
      let elapsedMs = (epochTime() - startMs) * 1000

      check wedgedFut.finished
      # Close returned around the drain budget (2s) — well under the
      # wedged future's 30 s sleep.
      check elapsedMs >= 900.0
      check elapsedMs < 4000.0
      check c.state == lsClosed
      await cluster.close()

    waitFor testBody()

  test "close shares one budget across drain + final commit (no overshoot)":
    ## Regression: the drain phases and final commit each got their own
    ## sessionTimeoutMs/2 slice, so a wedged drain then a wedged commit could burn
    ## 1.5 * sessionTimeoutMs — past the broker's session expiry. The shared budget
    ## caps cumulative wall clock at sessionTimeoutMs minus a LeaveGroup reserve.
    proc testBody() {.async: (raises: [Exception]).} =
      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      config.sessionTimeoutMs = 3000'i32
      config.heartbeatIntervalMs = 500'i32
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      proc wedged() {.async: (raises: []).} =
        try:
          await sleepAsync(seconds(30))
        except CancelledError:
          discard

      c.injectCommitLoopFut(wedged())
      c.appendPendingCommitForTest(wedged())

      let startMs = epochTime()
      await c.close()
      let elapsedMs = (epochTime() - startMs) * 1000

      # Total budget = sessionTimeoutMs - 1000ms reserve = 2000ms.
      # Each phase has at least a 500ms floor for the commit, so worst
      # case is roughly 2000ms + final-commit floor + cluster.close.
      # Pre-fix behaviour would be ~4500ms (1.5x sessionTimeoutMs).
      check c.state == lsClosed
      check elapsedMs < 4000.0
      await cluster.close()

    waitFor testBody()

  test "close returns 0ms timeouts once the shared budget is exhausted":
    proc testBody() {.async: (raises: [Exception]).} =
      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      config.sessionTimeoutMs = 3000'i32
      config.heartbeatIntervalMs = 500'i32
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      proc wedged() {.async: (raises: []).} =
        try:
          await sleepAsync(seconds(30))
        except CancelledError:
          discard

      c.injectCommitLoopFut(wedged())
      c.appendPendingCommitForTest(wedged())

      let startMs = epochTime()
      await c.close()
      let elapsedMs = (epochTime() - startMs) * 1000

      # totalBudgetMs = max(3000 - 1000, 2000) = 2000ms.
      # The first drain (commitLoop) consumes the entire 2000ms budget.
      # Once exhausted, remaining phases must get 0ms — not their floor
      # (100ms + 500ms) — so total stays near 2000ms, not 2600ms+.
      check c.state == lsClosed
      check elapsedMs < 2500.0
      await cluster.close()

    waitFor testBody()

  test "close drains pendingCommits cooperatively":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      var slowFinished = false
      proc slow() {.async: (raises: []).} =
        try:
          await sleepAsync(milliseconds(150))
          slowFinished = true
        except CancelledError:
          discard

      let slowFut = slow()
      c.appendPendingCommitForTest(slowFut)

      let startMs = epochTime()
      await c.close()
      let elapsedMs = (epochTime() - startMs) * 1000

      check slowFinished
      check slowFut.finished
      check not slowFut.cancelled
      check elapsedMs >= 130.0
      await cluster.close()

    waitFor testBody()

  test "close reaps a timed-out final commit instead of orphaning it":
    ## Regression: when the final commit's wait timed out at zero remaining
    ## budget, close() left the commitOffsets coroutine running past close() —
    ## holding commitLock and issuing an OffsetCommit against a cluster that
    ## was being torn down (chronos wait() with a zero timeout fails the wait
    ## without cancelling the inner future). close() now cancels and awaits
    ## that future, so no coroutine outlives the consumer (the invariant the
    ## drain phases hold).
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akOffsetCommit)), minVersion: 0, maxVersion: 3
                  ),
                ],
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()

      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      config.sessionTimeoutMs = 3000'i32
      config.heartbeatIntervalMs = 500'i32
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      let coord = newBroker(0'i32, "127.0.0.1", port, "coord")
      c.setGroupCoordinatorForTest(coord)
      c.setGroupGenerationForTest(7'i32)

      let tp = TopicPartition(topic: "t", partition: 0)
      let f = c.fetcherForTest()
      f.registerPartitionForTest(tp, 0)
      f.bumpGeneration(7'i32)
      c.injectPendingAdvanceForTest(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 4'i64)
      )

      # Exhaust the close budget in the first drain phase (sessionTimeoutMs -
      # 1000ms reserve = 2000ms): the final commit then gets a 0ms wait, which
      # chronos's wait() resolves by failing WITHOUT cancelling the inner
      # future — the deterministic orphan the fix targets.
      proc wedged() {.async: (raises: []).} =
        try:
          await sleepAsync(seconds(30))
        except CancelledError:
          discard

      c.injectCommitLoopFut(wedged())

      await c.close()

      # The final commit timed out at zero budget; it must have been cancelled
      # and reaped (never ran: commitLock free, no OffsetCommit ever sent)
      # rather than left running past close().
      check c.state == lsClosed
      check not c.commitLockHeldForTest()
      check fb.pendingRequests() == 0

      await coord.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Consumer: heartbeat continues across rebalance":
  test "state machine permits the gsStable→gsRebalancing→gsJoining→gsSyncing→gsStable chain":
    # Regression: the heartbeat loop used to exit as soon as state left gsStable.
    # It now runs while `g.state != gsUnjoined`, so it must legally span every
    # state the rebalance flow walks. Verified against tryTransition, the single
    # source of truth for legal edges.
    let cluster = newCluster(@["127.0.0.1:1"])
    let g = newConsumerGroup(cluster, "grp", @["t"], 30000, 3000)
    discard g.tryTransition(gsJoining)
    check g.state == gsJoining
    discard g.tryTransition(gsSyncing)
    check g.state == gsSyncing
    discard g.tryTransition(gsStable)
    check g.state == gsStable
    discard g.tryTransition(gsRebalancing)
    check g.state == gsRebalancing
    discard g.tryTransition(gsJoining)
    check g.state == gsJoining
    discard g.tryTransition(gsSyncing)
    check g.state == gsSyncing
    discard g.tryTransition(gsStable)
    check g.state == gsStable

  test "rebalance signal is gated on gsStable":
    # The new heartbeat loop only flips state into gsRebalancing when the
    # snapshot says it is currently stable. From any other state the rejoin
    # path already owns the transition, so a duplicate signal would corrupt
    # the rejoin in flight. We mirror that gate inline and verify it.
    let cluster = newCluster(@["127.0.0.1:1"])
    let g = newConsumerGroup(cluster, "grp", @["t"], 30000, 3000)

    # gsStable: signal flips state and sets the request flag.
    discard g.tryTransition(gsJoining)
    discard g.tryTransition(gsSyncing)
    discard g.tryTransition(gsStable)
    if g.state == gsStable:
      discard g.tryTransition(gsRebalancing)
      g.rebalanceRequested = true
    check g.state == gsRebalancing
    check g.rebalanceRequested

    # Reset rebalanceRequested and walk to gsJoining.
    g.rebalanceRequested = false
    discard g.tryTransition(gsJoining)

    # gsJoining: signal must be a no-op (state stays gsJoining, flag stays false).
    if g.state == gsStable:
      discard g.tryTransition(gsRebalancing)
      g.rebalanceRequested = true
    check g.state == gsJoining
    check not g.rebalanceRequested

proc serveOneOffsetFetchSuccess(fb: FakeBroker): Future[void] {.async: (raises: []).} =
  ## Serve ApiVersions, then one successful OffsetFetch v3 for t-0, so a pinned
  ## rejoin can carry the rebalance bookkeeping through to completion.
  try:
    let av = await fb.nextRequest(timeout = 2.seconds)
    try:
      await av.respond(
        buildApiVersionsResponseBody(
          0'i16,
          [
            FakeApiVersion(
              apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
            ),
            FakeApiVersion(
              apiKey: int16(ord(akOffsetFetch)), minVersion: 0, maxVersion: 3
            ),
          ],
        )
      )
    except CatchableError:
      discard
    let ofr = await fb.nextRequest(timeout = 2.seconds)
    try:
      await ofr.respond(
        buildOffsetFetchResponseBodyV3(
          topic = "t",
          partition = 0'i32,
          errorCode = 0'i16,
          offset = 42'i64,
          topLevelErrorCode = 0'i16,
        )
      )
    except CatchableError:
      discard
  except CatchableError:
    discard

suite "Consumer: rebalance listeners":
  test "onPartitionsRevoked fires with prior assignment when rebalance starts":
    # Without a real broker the rejoin will fail on every retry; what we want
    # to assert is that the revoked callback was invoked at teardown with the
    # partitions that were previously assigned — and that the lost callback did
    # NOT also fire for the same assignment (they are mutually exclusive).
    proc testBody() {.async: (raises: [Exception]).} =
      var revokedSeen: seq[TopicPartition]
      var revokedCount = 0
      var lostCount = 0

      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      config.onPartitionsRevoked = proc(parts: seq[TopicPartition]) {.raises: [].} =
        revokedSeen = parts
        revokedCount.inc
      config.onPartitionsLost = proc(parts: seq[TopicPartition]) {.raises: [].} =
        lostCount.inc

      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      # A valid generation marks the membership as still live, so the teardown
      # takes the graceful (revoked) path.
      c.setGroupGenerationForTest(5)
      c.setAssignedForTest(
        @[
          TopicPartition(topic: "test-topic", partition: 0),
          TopicPartition(topic: "test-topic", partition: 1),
        ]
      )
      c.requestRebalance()

      try:
        discard await c.pollBatch(maxRecords = 1, timeout = milliseconds(200))
      except KafkaError:
        discard

      check revokedCount == 1
      check revokedSeen.len == 2
      check revokedSeen[0].topic == "test-topic"
      check revokedSeen[0].partition == 0
      check revokedSeen[1].partition == 1
      check lostCount == 0
      await cluster.close()

    waitFor testBody()

  test "onPartitionsLost fires instead of onPartitionsRevoked when membership is invalid":
    # generationId is -1 (as after heartbeat fencing or a max.poll self-leave),
    # so the teardown must treat the assignment as LOST: exactly one lost
    # notification, no revoked notification, even though the rejoin then fails
    # through every retry.
    proc testBody() {.async: (raises: [Exception]).} =
      var lostSeen: seq[TopicPartition]
      var lostCount = 0
      var revokedCount = 0

      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      config.onPartitionsLost = proc(parts: seq[TopicPartition]) {.raises: [].} =
        lostSeen = parts
        lostCount.inc
      config.onPartitionsRevoked = proc(parts: seq[TopicPartition]) {.raises: [].} =
        revokedCount.inc

      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setAssignedForTest(@[TopicPartition(topic: "test-topic", partition: 0)])
      c.requestRebalance()

      try:
        discard await c.pollBatch(maxRecords = 1, timeout = milliseconds(300))
      except KafkaError:
        discard

      check lostCount == 1
      check lostSeen.len == 1
      check lostSeen[0].partition == 0
      check revokedCount == 0
      await cluster.close()

    waitFor testBody()

  test "teardown listener fires exactly once across retry exhaustion":
    # After the 3-retry rebalance loop exhausts, `rebalanceRequested` stays armed
    # so the next poll retries. The teardown block (revoked/lost callback +
    # pre-rebalance commit) must NOT re-fire on that retry — Java's
    # `needsJoinPrepare` parity: exactly one revoked/lost per rebalance episode.
    proc testBody() {.async: (raises: [Exception]).} =
      var revokedCount = 0
      var lostCount = 0

      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      config.onPartitionsRevoked = proc(parts: seq[TopicPartition]) {.raises: [].} =
        revokedCount.inc
      config.onPartitionsLost = proc(parts: seq[TopicPartition]) {.raises: [].} =
        lostCount.inc

      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupGenerationForTest(5)
      c.setAssignedForTest(
        @[
          TopicPartition(topic: "test-topic", partition: 0),
          TopicPartition(topic: "test-topic", partition: 1),
        ]
      )
      c.requestRebalance()

      # Poll 1: teardown fires, every rejoin attempt fails (unreachable peer).
      try:
        discard await c.pollBatch(maxRecords = 1, timeout = milliseconds(200))
      except KafkaError:
        discard
      check revokedCount == 1
      check lostCount == 0

      # Poll 2: rebalanceRequested is still armed, but rejoinPrepareDone must
      # gate the teardown so the listener does not fire a second time.
      try:
        discard await c.pollBatch(maxRecords = 1, timeout = milliseconds(200))
      except KafkaError:
        discard
      check revokedCount == 1
      check lostCount == 0

      await cluster.close()

    waitFor testBody()

  test "revoked listener exception is superseded by the rebalance failure":
    # The listener raises at teardown, then the rejoin fails on every retry.
    # The rejoin error must win (Java parity): poll raises the rebalance error,
    # not KafkaRebalanceCallbackError, and the listener still ran exactly once.
    proc testBody() {.async: (raises: [Exception]).} =
      var revokedCount = 0

      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      config.onPartitionsRevoked = proc(
          parts: seq[TopicPartition]
      ) {.raises: [CatchableError].} =
        revokedCount.inc
        raise newException(ValueError, "listener boom")

      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupGenerationForTest(5)
      c.setAssignedForTest(@[TopicPartition(topic: "test-topic", partition: 0)])
      c.requestRebalance()

      var sawCallbackError = false
      var sawKafkaError = false
      try:
        await c.handleRebalanceForTest()
      except KafkaRebalanceCallbackError:
        sawCallbackError = true
      except KafkaError:
        sawKafkaError = true

      check revokedCount == 1
      check not sawCallbackError
      check sawKafkaError
      await cluster.close()

    waitFor testBody()

  test "revoked switches to lost when heartbeat resets membership mid-commit":
    # TOCTOU: teardown decides the graceful (revoked) path at gen >= 0, then
    # awaits the pre-rebalance commit. If the heartbeat resets membership
    # (fencing / max.poll self-leave) during that await, the exclusive-callback
    # contract must fire onPartitionsLost instead of onPartitionsRevoked (Java
    # ConsumerCoordinator switches on the same signal).
    proc testBody() {.async: (raises: [Exception]).} =
      var revokedCount = 0
      var lostCount = 0

      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      config.onPartitionsRevoked = proc(parts: seq[TopicPartition]) {.raises: [].} =
        revokedCount.inc
      config.onPartitionsLost = proc(parts: seq[TopicPartition]) {.raises: [].} =
        lostCount.inc

      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      # Branch decision at teardown reads gen=5 → revoked path enters.
      c.setGroupGenerationForTest(5)
      c.setAssignedForTest(@[TopicPartition(topic: "test-topic", partition: 0)])
      c.requestRebalance()

      # Hold the commit lock so teardown's `await c.commitOffsets()` blocks;
      # then simulate a heartbeat resetMembership() mid-commit and release.
      await c.acquireCommitLockForTest()

      proc simulateHeartbeatReset() {.async: (raises: []).} =
        try:
          await sleepAsync(50.milliseconds)
        except CancelledError:
          return
        c.setGroupGenerationForTest(-1)
        c.releaseCommitLockForTest()

      let armFut = simulateHeartbeatReset()

      try:
        await c.handleRebalanceForTest()
      except KafkaError:
        discard

      await armFut

      check revokedCount == 0
      check lostCount == 1
      await cluster.close()

    waitFor testBody()

  test "assigned listener exception surfaces after the rebalance completes":
    # The listener raises on the new assignment. The rebalance bookkeeping must
    # still complete (rebalanceRequested cleared, fetcher restarted) and only
    # then does poll surface the exception, wrapped in
    # KafkaRebalanceCallbackError with the original as parent.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coordinator = newBroker(0'i32, "127.0.0.1", port, "coord")
      let handlerFut = fb.serveOneOffsetFetchSuccess()

      var assignedCount = 0
      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      config.onPartitionsAssigned = proc(
          parts: seq[TopicPartition]
      ) {.raises: [CatchableError].} =
        assignedCount.inc
        raise newException(ValueError, "assigned boom")

      let cluster = newCluster(@["127.0.0.1:1"])
      # Seed metadata so setPartitions resolves t-0 to the fake broker instead
      # of dialing the dead bootstrap address.
      cluster.registerBrokerForTest(coordinator)
      cluster.setPartitionLeaderForTest(TopicPartition(topic: "t", partition: 0), 0)
      cluster.markMetadataFreshForTest()
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupCoordinatorForTest(coordinator)
      c.requestRebalance()

      proc okRejoin(): Future[seq[TopicPartition]] {.
          async: (raises: [KafkaError, CancelledError])
      .} =
        return @[TopicPartition(topic: "t", partition: 0)]

      c.injectRejoinFutForTest(okRejoin())

      var callbackErrMsg = ""
      var parentMsg = ""
      try:
        await c.handleRebalanceForTest().wait(5.seconds)
      except KafkaRebalanceCallbackError as e:
        callbackErrMsg = e.msg
        if e.parent != nil:
          parentMsg = e.parent.msg

      check assignedCount == 1
      check callbackErrMsg.contains("onPartitionsAssigned")
      check parentMsg == "assigned boom"
      # Bookkeeping completed before the raise.
      check not c.rebalanceRequestedForTest()

      await c.close()
      await coordinator.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "rebalance retry exhaustion arms fetcher restart for next poll":
    ## Regression: handleRebalanceIfNeeded used to stop the fetcher and raise
    ## without setting fetcher.lastError, so the next poll's tryRestartFetcher
    ## (which gates on lastError != nil) never fired and the consumer stayed
    ## silently stopped.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setAssignedForTest(@[TopicPartition(topic: "test-topic", partition: 0)])
      c.requestRebalance()

      try:
        discard await c.pollBatch(maxRecords = 1, timeout = milliseconds(300))
      except KafkaError:
        discard

      # After retry exhaustion, the fetcher must be marked as failed so the
      # next poll's auto-restart logic fires instead of leaving the consumer
      # silently stopped.
      check c.fetcherLastError() != nil
      await cluster.close()

    waitFor testBody()

  test "permanent rebalance error arms fetcher restart (setLastError symmetry)":
    ## Regression: the permanent-error exit of handleRebalanceIfNeeded raised
    ## without calling fetcher.setLastError, unlike the retry-exhausted exit.
    ## Inject a rejoin failing with a permanent (ecFatal) KafkaApiError: the error
    ## must surface AND the fetcher be marked failed, so both exits are symmetric.
    proc failingRejoin(): Future[seq[TopicPartition]] {.
        async: (raises: [KafkaError, CancelledError])
    .} =
      raise newKafkaApiError(keGroupAuthorizationFailed, "permanent rejoin failure")

    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setAssignedForTest(@[TopicPartition(topic: "test-topic", partition: 0)])
      c.requestRebalance()
      c.injectRejoinFutForTest(failingRejoin())

      var caughtApi = false
      var caughtCode = keNone
      try:
        discard await c.pollBatch(maxRecords = 1, timeout = milliseconds(300))
      except KafkaApiError as e:
        caughtApi = true
        caughtCode = e.errorCode
      except KafkaError:
        discard

      check caughtApi
      check caughtCode == keGroupAuthorizationFailed
      check c.fetcherLastError() != nil
      await cluster.close()

    waitFor testBody()

  test "rebalance callbacks are optional (nil is no-op)":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setAssignedForTest(@[TopicPartition(topic: "test-topic", partition: 0)])
      c.requestRebalance()

      # No callbacks set — must not crash.
      try:
        discard await c.pollBatch(maxRecords = 1, timeout = milliseconds(200))
      except KafkaError:
        discard
      await cluster.close()

    waitFor testBody()

suite "Producer: KafkaClosedError type":
  test "send after close raises KafkaClosedError specifically":
    proc testBody() {.async: (raises: [Exception]).} =
      # The send-after-close rejection uses the dedicated KafkaClosedError type,
      # not the generic KafkaError.
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = kafkaProducer.newKafkaProducer(cluster, lingerMs = 100)
      p.markClosed()
      var raised = false
      try:
        discard await p.send("test-topic", @[0x01'u8])
      except KafkaClosedError:
        raised = true
      except KafkaError:
        check false # should be the more specific KafkaClosedError, not generic
      check raised
      await cluster.close()

    waitFor testBody()

suite "Consumer: OffsetFetch error classification":
  # The classifier maps a partition-level OffsetFetch errorCode to the action
  # fetchInitialOffsets takes (use offset, reset policy, invalidate coordinator,
  # rebalance, retriable, fatal). Earlier any non-keNone code was treated as
  # ofaNoCommitted, masking transient coordinator errors and causing mass
  # replay / skip.

  test "keNone with offset >= 0 yields ofaUseOffset":
    check classifyOffsetFetchError(keNone, 42'i64) == ofaUseOffset
    check classifyOffsetFetchError(keNone, 0'i64) == ofaUseOffset

  test "keNone with offset == -1 yields ofaNoCommitted":
    check classifyOffsetFetchError(keNone, -1'i64) == ofaNoCommitted

  test "coordinator-moved codes yield ofaInvalidateCoordinator":
    # Both NOT_COORDINATOR and GROUP_COORDINATOR_NOT_AVAILABLE must drop the
    # cached coordinator. The latter previously fell through to ofaRetriable
    # (only NOT_COORDINATOR was special-cased), leaving the stale mapping cached.
    check classifyOffsetFetchError(keNotCoordinator, -1'i64) == ofaInvalidateCoordinator
    check classifyOffsetFetchError(keGroupCoordinatorNotAvailable, -1'i64) ==
      ofaInvalidateCoordinator

  test "keCoordinatorLoadInProgress yields ofaRetriable":
    check classifyOffsetFetchError(keCoordinatorLoadInProgress, -1'i64) == ofaRetriable

  test "ecRebalanceRequired (UNKNOWN_MEMBER_ID, ILLEGAL_GENERATION) yields ofaRebalance":
    check classifyOffsetFetchError(keUnknownMemberId, -1'i64) == ofaRebalance
    check classifyOffsetFetchError(keIllegalGeneration, -1'i64) == ofaRebalance
    check classifyOffsetFetchError(keRebalanceInProgress, -1'i64) == ofaRebalance

  test "fatal codes (OFFSET_METADATA_TOO_LARGE, GROUP_AUTH_FAILED) yield ofaFatal":
    check classifyOffsetFetchError(keOffsetMetadataTooLarge, -1'i64) == ofaFatal
    check classifyOffsetFetchError(keGroupAuthorizationFailed, -1'i64) == ofaFatal

suite "Consumer: OffsetFetch top-level error":
  ## Regression: the top-level OffsetFetchResponse errorCode (v2+) was unchecked.
  ## A group-level failure (e.g. COORDINATOR_LOAD_IN_PROGRESS on failover) has an
  ## EMPTY topics array, so per-partition classification never ran, every
  ## partition read as "no committed offset", and the reset policy silently jumped
  ## to latest/earliest — data loss (orpLatest) or mass replay (orpEarliest).

  proc setupOffsetFetchHandler(
      fb: FakeBroker, topLevelErrorCode: int16
  ): Future[void] {.async: (raises: []).} =
    ## Serve ApiVersions (OffsetFetch up to v3), then one OffsetFetch v3
    ## response whose only error is the top-level code.
    try:
      let av = await fb.nextRequest(timeout = 2.seconds)
      try:
        await av.respond(
          buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              ),
              FakeApiVersion(
                apiKey: int16(ord(akOffsetFetch)), minVersion: 0, maxVersion: 3
              ),
            ],
          )
        )
      except CatchableError:
        discard
      let ofr = await fb.nextRequest(timeout = 2.seconds)
      try:
        await ofr.respond(
          buildOffsetFetchResponseBodyV3(
            topic = "t",
            partition = 0'i32,
            errorCode = 0'i16,
            offset = -1'i64,
            topLevelErrorCode = topLevelErrorCode,
          )
        )
      except CatchableError:
        discard
    except CatchableError:
      discard

  test "top-level NOT_COORDINATOR raises and invalidates the cached coordinator":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coordinator = newBroker(0'i32, "127.0.0.1", port, "coord")
      let handlerFut = fb.setupOffsetFetchHandler(keNotCoordinator.toInt16)

      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupCoordinatorForTest(coordinator)
      cluster.seedCoordinatorForTest("g", coordinator)
      check cluster.hasCachedCoordinatorForTest("g")

      var raised = keNone
      try:
        await c.fetchInitialOffsetsForTest(@[TopicPartition(topic: "t", partition: 0)])
      except KafkaApiError as e:
        raised = e.errorCode
      check raised == keNotCoordinator
      # The stale coordinator mapping must be dropped so the rebalance retry
      # re-issues FindCoordinator instead of re-hitting the wrong broker.
      check not cluster.hasCachedCoordinatorForTest("g")

      await c.close()
      await coordinator.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "top-level REBALANCE_IN_PROGRESS raises and arms rebalanceRequested":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coordinator = newBroker(0'i32, "127.0.0.1", port, "coord")
      let handlerFut = fb.setupOffsetFetchHandler(keRebalanceInProgress.toInt16)

      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupCoordinatorForTest(coordinator)
      check not c.rebalanceRequestedForTest()

      var raised = keNone
      try:
        await c.fetchInitialOffsetsForTest(@[TopicPartition(topic: "t", partition: 0)])
      except KafkaApiError as e:
        raised = e.errorCode
      check raised == keRebalanceInProgress
      check c.rebalanceRequestedForTest()

      await c.close()
      await coordinator.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "top-level COORDINATOR_LOAD_IN_PROGRESS raises a retriable KafkaApiError":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coordinator = newBroker(0'i32, "127.0.0.1", port, "coord")
      let handlerFut = fb.setupOffsetFetchHandler(keCoordinatorLoadInProgress.toInt16)

      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupCoordinatorForTest(coordinator)

      var raised = keNone
      try:
        await c.fetchInitialOffsetsForTest(@[TopicPartition(topic: "t", partition: 0)])
      except KafkaApiError as e:
        raised = e.errorCode
      check raised == keCoordinatorLoadInProgress
      # Retriable: handleRebalanceIfNeeded's retry loop absorbs it.
      check isRetriable(raised)

      await c.close()
      await coordinator.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "top-level errorCode == 0 is not treated as an error":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coordinator = newBroker(0'i32, "127.0.0.1", port, "coord")
      let handlerFut = fb.setupOffsetFetchHandler(0'i16)

      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupCoordinatorForTest(coordinator)

      # Empty partition list so setPartitions needs no broker resolution; the
      # point is that the new top-level guard must NOT fire on keNone.
      await c.fetchInitialOffsetsForTest(newSeq[TopicPartition]())

      await c.close()
      await coordinator.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Consumer: OffsetFetch mixed-batch recovery":
  ## Regression: a mixed batch of per-partition OffsetFetch errors was flattened
  ## to the FIRST non-keNone code, so a NOT_COORDINATOR + UNKNOWN_MEMBER_ID batch
  ## only ran the winner's recovery. KafkaOffsetFetchError now carries every code
  ## in partitionErrors and both actions apply.

  proc setupMixedOffsetFetchHandler(
      fb: FakeBroker, parts: seq[FakeOffsetFetchPartition]
  ): Future[void] {.async: (raises: []).} =
    try:
      let av = await fb.nextRequest(timeout = 2.seconds)
      try:
        await av.respond(
          buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              ),
              FakeApiVersion(
                apiKey: int16(ord(akOffsetFetch)), minVersion: 0, maxVersion: 3
              ),
            ],
          )
        )
      except CatchableError:
        discard
      let ofr = await fb.nextRequest(timeout = 2.seconds)
      try:
        await ofr.respond(
          buildOffsetFetchResponseBodyV3Multi(topic = "t", partitions = parts)
        )
      except CatchableError:
        discard
    except CatchableError:
      discard

  test "mixed NOT_COORDINATOR + UNKNOWN_MEMBER_ID batch surfaces both codes":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coordinator = newBroker(0'i32, "127.0.0.1", port, "coord")
      let parts = @[
        FakeOffsetFetchPartition(
          partition: 0'i32, errorCode: keNotCoordinator.toInt16, offset: -1'i64
        ),
        FakeOffsetFetchPartition(
          partition: 1'i32, errorCode: keUnknownMemberId.toInt16, offset: -1'i64
        ),
      ]
      let handlerFut = fb.setupMixedOffsetFetchHandler(parts)

      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupCoordinatorForTest(coordinator)
      cluster.seedCoordinatorForTest("g", coordinator)
      check cluster.hasCachedCoordinatorForTest("g")

      var raisedCodes: seq[KafkaErrorCode]
      var primary = keNone
      try:
        await c.fetchInitialOffsetsForTest(
          @[
            TopicPartition(topic: "t", partition: 0'i32),
            TopicPartition(topic: "t", partition: 1'i32),
          ]
        )
      except KafkaOffsetFetchError as e:
        primary = e.errorCode
        for pe in e.partitionErrors:
          raisedCodes.add(pe.errorCode)

      # Both codes must reach the caller via partitionErrors, not just the
      # first-error winner.
      check raisedCodes.len == 2
      check keNotCoordinator in raisedCodes
      check keUnknownMemberId in raisedCodes
      check primary != keNone

      # Pre-apply side effects fire on the coordinator-moved and rebalance
      # entries even when the "first" errorCode alone would only cover one.
      check not cluster.hasCachedCoordinatorForTest("g")
      check c.rebalanceRequestedForTest()

      await c.close()
      await coordinator.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

proc serveRejoinSplitCoordinator(
    fb: FakeBroker, probe: RebalanceProbe
): Future[void] {.async: (raises: []).} =
  ## Serve ApiVersions, then fail every OffsetFetch with
  ## COORDINATOR_LOAD_IN_PROGRESS (a retriable coordinator-failover error).
  ## JoinGroup is counted (and dropped): a correct retry never re-sends it, so
  ## the rebalance loop exhausts its retries on the offset fetch alone.
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 2.seconds)
    except CatchableError:
      break
    try:
      let key = req.apiKey
      if key == int16(ord(akApiVersions)):
        await req.respond(
          buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              ),
              FakeApiVersion(
                apiKey: int16(ord(akOffsetFetch)), minVersion: 0, maxVersion: 3
              ),
              FakeApiVersion(
                apiKey: int16(ord(akJoinGroup)), minVersion: 0, maxVersion: 2
              ),
              FakeApiVersion(
                apiKey: int16(ord(akSyncGroup)), minVersion: 0, maxVersion: 1
              ),
            ],
          )
        )
      elif key == int16(ord(akOffsetFetch)):
        inc probe.offsetFetchCount
        await req.respond(
          buildOffsetFetchResponseBodyV3(
            topic = "t",
            partition = 0'i32,
            errorCode = 0'i16,
            offset = -1'i64,
            topLevelErrorCode = keCoordinatorLoadInProgress.toInt16,
          )
        )
      elif key == int16(ord(akJoinGroup)):
        # A rejoin here is the regression under test. Count it and drop so the
        # buggy path stalls (the test bounds the call with a timeout).
        inc probe.joinCount
      # Anything else: drop silently.
    except CatchableError:
      break

suite "Consumer: transient OffsetFetch retries without rejoin":
  ## Regression: handleRebalanceIfNeeded retried JoinGroup+SyncGroup AND
  ## OffsetFetch as one unit, so a transient OffsetFetch failure re-issued
  ## JoinGroup — advancing the generation and triggering a group-wide rebalance
  ## storm. The fix holds a successful join and retries only the offset fetch.

  test "COORDINATOR_LOAD_IN_PROGRESS on OffsetFetch retries the fetch, never re-joins":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coordinator = newBroker(0'i32, "127.0.0.1", port, "coord")
      let probe = RebalanceProbe()
      let handlerFut = fb.serveRejoinSplitCoordinator(probe)

      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupCoordinatorForTest(coordinator)
      c.requestRebalance()

      # Pin a JoinGroup+SyncGroup that already succeeded with one partition. The
      # rebalance loop must consume this once; if it (incorrectly) re-joins on
      # each OffsetFetch failure it calls the real joinAndSync and the probe's
      # joinCount rises.
      proc okRejoin(): Future[seq[TopicPartition]] {.
          async: (raises: [KafkaError, CancelledError])
      .} =
        return @[TopicPartition(topic: "t", partition: 0)]

      c.injectRejoinFutForTest(okRejoin())

      var raisedCode = keNone
      try:
        await c.handleRebalanceForTest().wait(5.seconds)
      except KafkaApiError as e:
        raisedCode = e.errorCode
      except KafkaError:
        discard

      # The offset fetch was retried maxRebalanceRetries (3) times and the
      # transient error finally surfaced — with ZERO re-joins. A rejoin storm
      # would have shown joinCount > 0 and fewer raw offset fetches.
      check raisedCode == keCoordinatorLoadInProgress
      check probe.joinCount == 0
      check probe.offsetFetchCount == 3

      await c.close()
      await coordinator.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

proc serveNotCoordinatorOffsetFetch(
    fb: FakeBroker, port: int32, probe: RediscoverProbe
): Future[void] {.async: (raises: []).} =
  ## Serve ApiVersions + Metadata + FindCoordinator (pointing back at this
  ## broker), then fail every OffsetFetch with NOT_COORDINATOR. Metadata lets the
  ## cluster re-resolve the coordinator (coordinatorBroker refreshes it before
  ## FindCoordinator). JoinGroup is counted and dropped — the rejoin is pinned,
  ## so a JoinGroup here would be the rejoin-storm regression.
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 2.seconds)
    except CatchableError:
      break
    try:
      let key = req.apiKey
      if key == int16(ord(akApiVersions)):
        await req.respond(
          buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              ),
              FakeApiVersion(
                apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
              ),
              FakeApiVersion(
                apiKey: int16(ord(akFindCoordinator)), minVersion: 0, maxVersion: 1
              ),
              FakeApiVersion(
                apiKey: int16(ord(akOffsetFetch)), minVersion: 0, maxVersion: 3
              ),
              FakeApiVersion(
                apiKey: int16(ord(akJoinGroup)), minVersion: 0, maxVersion: 2
              ),
              FakeApiVersion(
                apiKey: int16(ord(akSyncGroup)), minVersion: 0, maxVersion: 1
              ),
            ],
          )
        )
      elif key == int16(ord(akMetadata)):
        await req.respond(
          buildMetadataResponseBody(
            [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
            controllerId = 0'i32,
            topics = [
              FakeTopicMeta(
                name: "t",
                errorCode: 0'i16,
                isInternal: false,
                partitions: @[
                  FakePartitionMeta(
                    partitionIndex: 0'i32,
                    leaderId: 0'i32,
                    replicas: @[0'i32],
                    isr: @[0'i32],
                  )
                ],
              )
            ],
          )
        )
      elif key == int16(ord(akFindCoordinator)):
        inc probe.findCoordinatorCount
        await req.respond(
          buildFindCoordinatorResponseBodyV1(
            nodeId = 0'i32, host = "127.0.0.1", port = port
          )
        )
      elif key == int16(ord(akOffsetFetch)):
        inc probe.offsetFetchCount
        await req.respond(
          buildOffsetFetchResponseBodyV3(
            topic = "t",
            partition = 0'i32,
            errorCode = 0'i16,
            offset = -1'i64,
            topLevelErrorCode = keNotCoordinator.toInt16,
          )
        )
      elif key == int16(ord(akJoinGroup)):
        # A rejoin here is the rebalance-storm regression. Count and drop.
        inc probe.joinCount
      # Anything else: drop silently.
    except CatchableError:
      break

suite "Consumer: OffsetFetch NOT_COORDINATOR re-resolves and recovers":
  ## Regression: on NOT_COORDINATOR the OffsetFetch phase only invalidated the
  ## cached coordinator without re-pointing group.coordinator, so every retry hit
  ## the SAME pre-failover broker and exhausted — and exhaustion held the
  ## assignment with rebalanceRequested cleared, so the next poll restarted the
  ## fetcher on the OLD generation and dropped every record (no-delivery livelock).
  ## The fix re-resolves via rediscoverCoordinator and re-arms rebalanceRequested.

  test "NOT_COORDINATOR re-issues FindCoordinator, never re-joins, and re-arms rebalance":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coordinator = newBroker(0'i32, "127.0.0.1", port, "coord")
      let probe = RediscoverProbe()
      let handlerFut = fb.serveNotCoordinatorOffsetFetch(port, probe)

      # Bootstrap points at the fake broker so the rediscovery FindCoordinator is
      # actually reachable. The broken path issued none at all.
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:" & $port], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:" & $port])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupCoordinatorForTest(coordinator)
      c.requestRebalance()

      # Pin a JoinGroup+SyncGroup that already succeeded (joined.isSome), so the
      # rebalance loop enters the OffsetFetch phase directly. A buggy re-join on
      # each OffsetFetch failure would raise probe.joinCount.
      proc okRejoin(): Future[seq[TopicPartition]] {.
          async: (raises: [KafkaError, CancelledError])
      .} =
        return @[TopicPartition(topic: "t", partition: 0)]

      c.injectRejoinFutForTest(okRejoin())

      var raisedCode = keNone
      try:
        await c.handleRebalanceForTest().wait(10.seconds)
      except KafkaApiError as e:
        raisedCode = e.errorCode
      except KafkaError:
        discard

      # The fetch was retried maxRebalanceRetries (3) times and finally surfaced
      # NOT_COORDINATOR, with ZERO re-joins (no rebalance storm).
      check raisedCode == keNotCoordinator
      check probe.offsetFetchCount == 3
      check probe.joinCount == 0
      # Fix 1: each retry re-resolved the coordinator. The broken path only
      # invalidated the cache and issued no FindCoordinator (count would be 0).
      check probe.findCoordinatorCount >= 1
      # Fix 2: exhaustion with the assignment held must re-arm rebalanceRequested
      # so the next poll rejoins instead of livelocking on a stale generation.
      check c.rebalanceRequestedForTest()

      await c.close()
      await coordinator.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

proc serveCommitRediscovery(
    fb: FakeBroker,
    port: int32,
    probe: RediscoverProbe,
    commitCodes: seq[int16],
    findFails: bool,
    holdFind: Future[void] = nil,
): Future[void] {.async: (raises: []).} =
  ## Serve ApiVersions + Metadata + FindCoordinator, then answer OffsetCommit
  ## with `commitCodes` in order (keNone once the script is exhausted). Lets a
  ## commit-side NOT_COORDINATOR drive the rediscovery retry through a real
  ## Broker, the way a coordinator failover does.
  var commitIdx = 0
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 2.seconds)
    except CatchableError:
      break
    try:
      let key = req.apiKey
      if key == int16(ord(akApiVersions)):
        await req.respond(
          buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              ),
              FakeApiVersion(
                apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
              ),
              FakeApiVersion(
                apiKey: int16(ord(akFindCoordinator)), minVersion: 0, maxVersion: 1
              ),
              FakeApiVersion(
                apiKey: int16(ord(akOffsetCommit)), minVersion: 0, maxVersion: 3
              ),
            ],
          )
        )
      elif key == int16(ord(akMetadata)):
        await req.respond(
          buildMetadataResponseBody(
            [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
            controllerId = 0'i32,
            topics = [
              FakeTopicMeta(
                name: "t",
                errorCode: 0'i16,
                isInternal: false,
                partitions: @[
                  FakePartitionMeta(
                    partitionIndex: 0'i32,
                    leaderId: 0'i32,
                    replicas: @[0'i32],
                    isr: @[0'i32],
                  )
                ],
              )
            ],
          )
        )
      elif key == int16(ord(akFindCoordinator)):
        inc probe.findCoordinatorCount
        if holdFind != nil:
          # Park the lookup so a test can cancel the commit inside the
          # rediscovery, exercising the rdCancelled arm. Bounded so a mutation
          # cannot wedge the handler.
          try:
            await holdFind.wait(2.seconds)
          except AsyncTimeoutError:
            discard
        if findFails:
          # A non-retriable code on purpose: findCoordinatorBroker retries a
          # retriable answer itself (3 attempts), which would multiply the RPC
          # count this probe asserts on and hide the rediscovery-budget behavior
          # under test here.
          await req.respond(
            buildFindCoordinatorResponseBodyV1(
              errorCode = int16(ord(keGroupAuthorizationFailed)),
              errorMessage = some("coordinator lookup not authorized"),
            )
          )
        else:
          await req.respond(
            buildFindCoordinatorResponseBodyV1(
              nodeId = 0'i32, host = "127.0.0.1", port = port
            )
          )
      elif key == int16(ord(akOffsetCommit)):
        inc probe.offsetCommitCount
        let code =
          if commitIdx < commitCodes.len:
            commitCodes[commitIdx]
          else:
            keNone.toInt16
        inc commitIdx
        await req.respond(
          buildOffsetCommitResponseBodyV3(
            topic = "t", partition = 0'i32, errorCode = code
          )
        )
      # Anything else: drop silently.
    except CatchableError:
      break

proc seedCommitter(port: int32, rediscoverAttempts: int = 3): (KafkaConsumer, Cluster) =
  ## Consumer with the minimum state a commit needs: a coordinator, a live
  ## generation, and one consumed record whose deferred advance makes the
  ## committed offset observable (5).
  var config = initConsumerConfig(
    bootstrapBrokers = @["127.0.0.1:" & $port], groupId = "g", topics = @["t"]
  )
  config.maxCoordinatorRediscoverAttempts = rediscoverAttempts
  let cluster = newCluster(@["127.0.0.1:" & $port])
  let c = newKafkaConsumer(config, cluster)
  c.markRunning()
  c.setGroupCoordinatorForTest(newBroker(0'i32, "127.0.0.1", port, "coord"))
  c.setGroupGenerationForTest(7'i32)

  let tp = TopicPartition(topic: "t", partition: 0)
  let f = c.fetcherForTest()
  f.registerPartitionForTest(tp, 0)
  f.bumpGeneration(7'i32)
  c.injectPendingAdvanceForTest(
    FetchedRecord(topic: "t", partition: 0'i32, offset: 4'i64)
  )
  (c, cluster)

suite "Consumer: commit-time coordinator rediscovery":
  ## commitOffsetsLocked retries an OffsetCommit whose partition error says the
  ## coordinator moved or is still loading (Java's ConsumerCoordinator): drop
  ## the cached coordinator, re-resolve it, reissue up to
  ## maxCoordinatorRediscoverAttempts. The OffsetFetch side of that dance has a
  ## regression test above; the commit side had none, so the retry, the
  ## attempt-exhausted break, and the rdFailed fall-through were unverified.

  test "NOT_COORDINATOR re-resolves the coordinator and retries the commit":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let probe = RediscoverProbe()
      let handlerFut = serveCommitRediscovery(
        fb, port, probe, @[keNotCoordinator.toInt16, keNone.toInt16], false
      )
      let (c, cluster) = seedCommitter(port)

      await c.commitSync()

      # Two OffsetCommits: the first rejected, the second accepted after
      # FindCoordinator re-pointed the group at the new coordinator.
      check probe.offsetCommitCount == 2
      check probe.findCoordinatorCount == 1
      # A retry that gave up would have raised with the broker's error; the
      # accepted retry instead commits the caller's position and applies the
      # deferred advance.
      check c.pendingAdvanceLenForTest() == 0
      let tp = TopicPartition(topic: "t", partition: 0)
      check c.fetcherForTest().getConsumedOffsets()[tp] == 5'i64

      # Drop the coordinator so close()'s final commit short-circuits.
      c.setGroupCoordinatorForTest(nil)
      await c.close()
      await cluster.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "COORDINATOR_LOAD_IN_PROGRESS is retried like a moved coordinator":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let probe = RediscoverProbe()
      let handlerFut = serveCommitRediscovery(
        fb, port, probe, @[keCoordinatorLoadInProgress.toInt16, keNone.toInt16], false
      )
      let (c, cluster) = seedCommitter(port)

      await c.commitSync()

      # The loading coordinator is in the same retryable class as a moved one,
      # not a hard commit failure.
      check probe.offsetCommitCount == 2
      check probe.findCoordinatorCount == 1
      check c.pendingAdvanceLenForTest() == 0

      c.setGroupCoordinatorForTest(nil)
      await c.close()
      await cluster.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "a failed rediscovery surfaces the broker's own commit error":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let probe = RediscoverProbe()
      let handlerFut =
        serveCommitRediscovery(fb, port, probe, @[keNotCoordinator.toInt16], true)
      # Two attempts: the retry is issued once, and its rediscovery burns both
      # FindCoordinator attempts before reporting rdFailed.
      let (c, cluster) = seedCommitter(port, rediscoverAttempts = 2)

      var code = keNone
      var partCode = keNone
      var partErrors = 0
      try:
        await c.commitSync()
      except KafkaCommitError as e:
        code = e.errorCode
        partErrors = e.partitionErrors.len
        if partErrors > 0:
          partCode = e.partitionErrors[0].errorCode

      # rdFailed falls through with the last broker response: the caller sees
      # NOT_COORDINATOR, not a synthetic rediscovery error.
      check code == keNotCoordinator
      check partErrors == 1
      check partCode == keNotCoordinator
      check probe.offsetCommitCount == 1
      check probe.findCoordinatorCount == 2
      # A rejected commit must not advance the position.
      check c.pendingAdvanceLenForTest() == 1

      c.setGroupCoordinatorForTest(nil)
      await c.close()
      await cluster.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "a single-attempt budget rejects without a rediscovery round":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let probe = RediscoverProbe()
      let handlerFut =
        serveCommitRediscovery(fb, port, probe, @[keNotCoordinator.toInt16], false)
      let (c, cluster) = seedCommitter(port, rediscoverAttempts = 1)

      var code = keNone
      try:
        await c.commitSync()
      except KafkaCommitError as e:
        code = e.errorCode

      # maxCoordinatorRediscoverAttempts == 1 means one OffsetCommit and no
      # rediscovery: the bound is honored rather than silently replaced by 3.
      check code == keNotCoordinator
      check probe.offsetCommitCount == 1
      check probe.findCoordinatorCount == 0

      c.setGroupCoordinatorForTest(nil)
      await c.close()
      await cluster.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "a commit cancelled during rediscovery still releases the commit lock":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let probe = RediscoverProbe()
      let holdFind = newFuture[void]("hold-find-coordinator")
      let handlerFut = serveCommitRediscovery(
        fb, port, probe, @[keNotCoordinator.toInt16], false, holdFind
      )
      let (c, cluster) = seedCommitter(port)

      let commitFut = c.commitSync()
      # Wait until the retry has dropped the cache and parked in
      # FindCoordinator, so the cancel lands on the rdCancelled arm.
      var waitedMs = 0
      while probe.findCoordinatorCount == 0 and waitedMs < 2000:
        await sleepAsync(milliseconds(5))
        waitedMs += 5
      check probe.findCoordinatorCount == 1

      try:
        await commitFut.cancelAndWait()
      except CancelledError:
        discard

      # rdCancelled must propagate as a cancellation, and the commit must
      # release commitLock on its way out — otherwise the next commit, or
      # close()'s final commit, would block on a lock nobody holds.
      check commitFut.cancelled()
      check not c.commitLockHeldForTest()
      # A cancelled commit must not advance the position either.
      check c.pendingAdvanceLenForTest() == 1

      c.setGroupCoordinatorForTest(nil)
      await c.close()
      await cluster.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

proc serveGrowableMetadata(
    fb: FakeBroker, port: int32, partCount: ref int
): Future[void] {.async: (raises: []).} =
  ## Serve ApiVersions and Metadata for topic "t" with a partition count the
  ## test can change between refreshes, so the growth check sees an expansion.
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 2.seconds)
    except CatchableError:
      break
    try:
      let key = req.apiKey
      if key == int16(ord(akApiVersions)):
        await req.respond(
          buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              ),
              FakeApiVersion(
                apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
              ),
            ],
          )
        )
      elif key == int16(ord(akMetadata)):
        var parts: seq[FakePartitionMeta]
        for i in 0 ..< partCount[]:
          parts.add(
            FakePartitionMeta(
              partitionIndex: int32(i),
              leaderId: 0'i32,
              replicas: @[0'i32],
              isr: @[0'i32],
            )
          )
        await req.respond(
          buildMetadataResponseBody(
            [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
            controllerId = 0'i32,
            topics = [
              FakeTopicMeta(
                name: "t", errorCode: 0'i16, isInternal: false, partitions: parts
              )
            ],
          )
        )
      # Anything else: drop.
    except CatchableError:
      break

suite "Consumer: subscribed-topic partition growth arms a rebalance":
  ## Regression: nothing watched a subscribed topic's partition count, so
  ## partitions added after the group stabilised were never assigned and never
  ## consumed (silent data stall). checkPartitionGrowth refreshes metadata on
  ## every call and arms a rebalance on an increase; the heartbeat loop dispatches
  ## it on the metadata.max.age cadence.

  test "an increase in partition count requests a rebalance":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let partCount = new(int)
      partCount[] = 2
      let handlerFut = serveGrowableMetadata(fb, port, partCount)

      let cluster = newCluster(@["127.0.0.1:" & $port], clientId = "m6-test")
      let g = newConsumerGroup(cluster, "g", @["t"])
      g.state = gsStable

      # First check establishes the baseline (2 partitions); no rebalance.
      await g.checkPartitionGrowth()
      check not g.rebalanceRequested

      # The topic gains partitions; the next check must arm a rebalance.
      partCount[] = 4
      await g.checkPartitionGrowth()
      check g.rebalanceRequested

      await cluster.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "a steady partition count does not request a rebalance":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let partCount = new(int)
      partCount[] = 3
      let handlerFut = serveGrowableMetadata(fb, port, partCount)

      let cluster = newCluster(@["127.0.0.1:" & $port], clientId = "m6-test")
      let g = newConsumerGroup(cluster, "g", @["t"])
      g.state = gsStable

      await g.checkPartitionGrowth()
      check not g.rebalanceRequested
      # No change between checks.
      await g.checkPartitionGrowth()
      check not g.rebalanceRequested

      await cluster.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Consumer: buffered records delivered before a permanent fetch error":
  ## Regression: when the fetcher stopped on a permanent error after records were
  ## already enqueued, poll/pollBatch called tryRestartFetcher (which re-raises)
  ## BEFORE draining the queue, stranding those records. The poll path now delivers
  ## buffered records first and surfaces the error only once the queue is empty.

  proc seedBufferedConsumer(): (KafkaConsumer, Cluster) =
    let config = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    let cluster = newCluster(@["127.0.0.1:1"])
    let c = newKafkaConsumer(config, cluster)
    c.markRunning()
    c.setGroupGenerationForTest(5'i32)
    let fetcher = c.fetcherForTest()
    fetcher.bumpGeneration(5'i32)
    for off in 0'i64 ..< 2'i64:
      fetcher.enqueueRecordForTest(
        FetchedRecord(
          topic: "t", partition: 0'i32, offset: off, value: some(@[byte(off)])
        )
      )
    # A healthy broker buffered the records above; the fetcher now hits a
    # permanent (ecFatal) error so tryRestartFetcher re-raises instead of
    # restarting.
    fetcher.setLastError(newKafkaApiError(keGroupAuthorizationFailed, "perm"))
    (c, cluster)

  test "pollBatch returns queued records, then surfaces the error":
    proc testBody() {.async: (raises: [Exception]).} =
      let (c, cluster) = seedBufferedConsumer()

      let batch = await c.pollBatch(maxRecords = 10, timeout = milliseconds(200))
      check batch.len == 2
      check batch[0].offset == 0
      check batch[1].offset == 1

      # Queue drained: the permanent error now surfaces.
      var raisedCode = keNone
      try:
        discard await c.pollBatch(maxRecords = 10, timeout = milliseconds(200))
      except KafkaApiError as e:
        raisedCode = e.errorCode
      except KafkaError:
        discard
      check raisedCode == keGroupAuthorizationFailed

      await c.close()
      await cluster.close()

    waitFor testBody()

  test "poll returns a queued record, then surfaces the error":
    proc testBody() {.async: (raises: [Exception]).} =
      let (c, cluster) = seedBufferedConsumer()

      let r0 = await c.poll(timeout = milliseconds(200))
      check r0.offset == 0
      let r1 = await c.poll(timeout = milliseconds(200))
      check r1.offset == 1

      var raisedCode = keNone
      try:
        discard await c.poll(timeout = milliseconds(200))
      except KafkaApiError as e:
        raisedCode = e.errorCode
      except KafkaError:
        discard
      check raisedCode == keGroupAuthorizationFailed

      await c.close()
      await cluster.close()

    waitFor testBody()

suite "Consumer: pollBatch keeps collected records when a later iteration fails":
  ## Regression: the batch-poll loop re-checks rebalance/running/restart each
  ## iteration. A KafkaError raised there after records were popped discarded the
  ## local batch — but the fetchOffset advanced at enqueue time, so they were
  ## never re-fetched: a permanent skip breaking at-least-once on a transient
  ## fetch failure. The loop must return the partial batch and let the error
  ## surface on the next poll.

  test "pollBatch returns collected records when the fetcher dies mid-collection; next poll raises":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      let f = c.fetcherForTest()
      f.enqueueRecordForTest(FetchedRecord(topic: "t", partition: 0'i32, offset: 0'i64))

      proc failLater() {.async: (raises: []).} =
        ## Let the batch loop pop record 0 and park on the empty queue, then
        ## put the fetcher into a "stopped, cooling down" state and feed one
        ## more record so the loop wakes, collects it, and trips over the
        ## error at the top of the NEXT iteration.
        try:
          await sleepAsync(50.milliseconds)
        except CancelledError:
          return
        f.setLastError(newException(KafkaError, "boom"))
        f.lastRestartAttempt = Moment.now() # within the 5s restart cooldown
        f.enqueueRecordForTest(
          FetchedRecord(topic: "t", partition: 0'i32, offset: 1'i64)
        )

      let failFut = failLater()
      let recs = await c.pollBatch(5, 2.seconds)
      # Both popped records are delivered instead of being discarded by the
      # cooldown error raised on the third iteration.
      check recs.len == 2
      check recs[0].offset == 0'i64
      check recs[1].offset == 1'i64

      # The persistent error surfaces on the next call (no records collected).
      expect KafkaError:
        discard await c.pollBatch(5, 50.milliseconds)

      awaitDrain(failFut, 1.seconds)
      await c.close()
      await cluster.close()

    waitFor testBody()

  test "pollBatchStrict returns the partial batch instead of raising":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      let f = c.fetcherForTest()
      f.enqueueRecordForTest(FetchedRecord(topic: "t", partition: 0'i32, offset: 0'i64))

      proc failLater() {.async: (raises: []).} =
        try:
          await sleepAsync(50.milliseconds)
        except CancelledError:
          return
        f.setLastError(newException(KafkaError, "boom"))
        f.lastRestartAttempt = Moment.now()
        f.enqueueRecordForTest(
          FetchedRecord(topic: "t", partition: 0'i32, offset: 1'i64)
        )

      let failFut = failLater()
      let recs = await c.pollBatchStrict(5, 2.seconds)
      check recs.len == 2

      expect KafkaError:
        discard await c.pollBatchStrict(5, 50.milliseconds)

      awaitDrain(failFut, 1.seconds)
      await c.close()
      await cluster.close()

    waitFor testBody()

suite "Consumer: pollBatch surfaces one-shot listener error over partial batch":
  ## Regression: handleRebalanceIfNeeded clears rejoinListenerError before
  ## raising KafkaRebalanceCallbackError, so it is a one-shot delivery. The
  ## pollBatch catch clause used to break on records.len > 0 assuming every
  ## error there re-raises on the next poll — but this one does not, so the
  ## listener exception was silently lost when prior iterations had collected
  ## records. Now the callback error always surfaces.

  test "pollBatch raises the listener error even when earlier iterations collected records":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coordinator = newBroker(0'i32, "127.0.0.1", port, "coord")
      let handlerFut = fb.serveOneOffsetFetchSuccess()

      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      config.onPartitionsAssigned = proc(
          parts: seq[TopicPartition]
      ) {.raises: [CatchableError].} =
        raise newException(ValueError, "assigned boom")

      let cluster = newCluster(@["127.0.0.1:1"])
      cluster.registerBrokerForTest(coordinator)
      cluster.setPartitionLeaderForTest(TopicPartition(topic: "t", partition: 0), 0)
      cluster.markMetadataFreshForTest()
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupCoordinatorForTest(coordinator)
      c.setGroupGenerationForTest(1'i32)

      let f = c.fetcherForTest()
      f.bumpGeneration(1'i32)
      # iter 1 pops this record before any rebalance is armed.
      f.enqueueRecordForTest(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 0'i64, generation: 1'i32)
      )

      proc okRejoin(): Future[seq[TopicPartition]] {.
          async: (raises: [KafkaError, CancelledError])
      .} =
        return @[TopicPartition(topic: "t", partition: 0)]

      proc armRebalanceLater() {.async: (raises: []).} =
        # Wait until iter 1 has settled on records.get(), then arm the rebalance
        # and enqueue a second record so iter 2's get() wakes and iter 3 enters
        # handleRebalanceIfNeeded with a non-empty local batch.
        try:
          await sleepAsync(50.milliseconds)
        except CancelledError:
          return
        c.injectRejoinFutForTest(okRejoin())
        c.requestRebalance()
        f.enqueueRecordForTest(
          FetchedRecord(topic: "t", partition: 0'i32, offset: 1'i64, generation: 1'i32)
        )

      let armFut = armRebalanceLater()

      var raisedCallback = false
      var callbackErrMsg = ""
      try:
        discard await c.pollBatch(maxRecords = 5, timeout = 3.seconds)
      except KafkaRebalanceCallbackError as e:
        raisedCallback = true
        callbackErrMsg = e.msg

      check raisedCallback
      check callbackErrMsg.contains("onPartitionsAssigned")

      awaitDrain(armFut, 1.seconds)
      await c.close()
      await coordinator.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Consumer: recordQueueSize config":
  ## Regression: fetcher.records used a hard-coded queueSize=1000 because
  ## ConsumerConfig didn't expose the setting. Now the field is plumbed
  ## from ConsumerConfig through newKafkaConsumer to newFetcher.

  test "default initConsumerConfig sets recordQueueSize = 1000":
    let cfg = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    check cfg.recordQueueSize == 1000

  test "custom recordQueueSize survives consumer construction":
    var cfg = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    cfg.recordQueueSize = 50
    let cluster =
      newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
    let c = newKafkaConsumer(cfg, cluster)
    # recordQueueCapacity() should match config: it reports the size cached at
    # construction because chronos's AsyncQueue keeps its maxsize private.
    check c.fetcherForTest().recordQueueCapacity() == 50
    waitFor c.close()
    waitFor cluster.close()

  test "zero recordQueueSize falls back to 1000":
    let cfg = ConsumerConfig(
      bootstrapBrokers: @["127.0.0.1:1"],
      groupId: "g",
      topics: @["t"],
      clientId: "c",
      sessionTimeoutMs: 30000,
      heartbeatIntervalMs: 3000,
      rebalanceTimeoutMs: 60000,
      maxWaitMs: 500,
      minFetchBytes: 1,
      maxFetchBytes: 52428800,
      maxPartitionFetchBytes: 1048576,
      autoCommit: false,
      autoCommitIntervalMs: 5000,
      offsetResetPolicy: orpLatest,
      maxFetcherRestartAttempts: 10,
      recordQueueSize: 0, # explicit zero — the fallback should kick in
    )
    let cluster =
      newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
    let c = newKafkaConsumer(cfg, cluster)
    check c.fetcherForTest().recordQueueCapacity() == 1000
    waitFor c.close()
    waitFor cluster.close()

suite "Consumer: maxValidationAttempts config":
  ## The KIP-320 validation-stall escalation budget is plumbed from
  ## ConsumerConfig through newKafkaConsumer to newFetcher.

  test "default initConsumerConfig sets maxValidationAttempts = 10":
    let cfg = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    check cfg.maxValidationAttempts == 10

  test "custom maxValidationAttempts survives consumer construction":
    var cfg = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    cfg.maxValidationAttempts = 3
    let cluster =
      newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
    let c = newKafkaConsumer(cfg, cluster)
    check c.fetcherForTest().maxValidationAttempts() == 3
    waitFor c.close()
    waitFor cluster.close()

  test "zero maxValidationAttempts falls back to 10":
    let cfg = ConsumerConfig(
      bootstrapBrokers: @["127.0.0.1:1"],
      groupId: "g",
      topics: @["t"],
      clientId: "c",
      sessionTimeoutMs: 30000,
      heartbeatIntervalMs: 3000,
      rebalanceTimeoutMs: 60000,
      maxWaitMs: 500,
      minFetchBytes: 1,
      maxFetchBytes: 52428800,
      maxPartitionFetchBytes: 1048576,
      autoCommit: false,
      autoCommitIntervalMs: 5000,
      offsetResetPolicy: orpLatest,
      maxFetcherRestartAttempts: 10,
      recordQueueSize: 1000,
      maxValidationAttempts: 0, # explicit zero — the fallback should kick in
    )
    let cluster =
      newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
    let c = newKafkaConsumer(cfg, cluster)
    check c.fetcherForTest().maxValidationAttempts() == 10
    waitFor c.close()
    waitFor cluster.close()

  test "negative maxValidationAttempts is rejected":
    var cfg = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    cfg.maxValidationAttempts = -1
    let cluster =
      newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
    expect(KafkaError):
      discard newKafkaConsumer(cfg, cluster)
    waitFor cluster.close()

  test "negative recordQueueSize is rejected, 0 falls back to the default":
    ## 0 is the documented "unset" sentinel and withDefaults resolves it; a
    ## negative capacity would reach the fetcher and disable backpressure.
    var cfg = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    cfg.recordQueueSize = -1
    let cluster =
      newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
    expect(KafkaError):
      discard newKafkaConsumer(cfg, cluster)

    cfg.recordQueueSize = 0
    let c = newKafkaConsumer(cfg, cluster)
    check c.fetcherForTest().recordQueueCapacity == 1000
    waitFor c.close()
    waitFor cluster.close()

  test "negative maxPollIntervalMs and metadataMaxAgeMs are rejected":
    ## Both fields are left at 0 by withDefaults because 0 is their documented
    ## off switch (see ConsumerConfig); a negative value has no such meaning.
    var cfg = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    let cluster =
      newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
    cfg.maxPollIntervalMs = -1
    expect(KafkaError):
      discard newKafkaConsumer(cfg, cluster)

    cfg.maxPollIntervalMs = 300_000
    cfg.metadataMaxAgeMs = -1
    expect(KafkaError):
      discard newKafkaConsumer(cfg, cluster)
    waitFor cluster.close()

  test "consumer config validation raises KafkaError, not ValueError":
    ## `newCluster` documents "Configuration is validated eagerly and raises
    ## KafkaError on misuse", but the consumer path used ValueError, so an
    ## `except KafkaError` around construction missed every consumer misconfig.
    var cfg = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    # 0 is mapped to the default by withDefaults before validation runs, so a
    # negative value is what reaches the validator.
    cfg.sessionTimeoutMs = -1
    let cluster =
      newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
    var valueError = false
    var kafkaError = false
    try:
      discard newKafkaConsumer(cfg, cluster)
    except ValueError:
      valueError = true
    except KafkaError:
      kafkaError = true
    check kafkaError
    check not valueError
    waitFor cluster.close()

suite "Consumer: heartbeatTimeoutMs / maxCoordinatorRediscoverAttempts config":
  ## The per-heartbeat-request timeout and the coordinator rediscovery budget
  ## are plumbed from ConsumerConfig through newKafkaConsumer to
  ## newConsumerGroup.

  test "default initConsumerConfig leaves heartbeatTimeoutMs auto and sets rediscover = 3":
    let cfg = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    check cfg.heartbeatTimeoutMs == 0
    check cfg.maxCoordinatorRediscoverAttempts == 3

  test "custom values survive consumer construction":
    var cfg = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    cfg.heartbeatTimeoutMs = 42
    cfg.maxCoordinatorRediscoverAttempts = 7
    let cluster =
      newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
    let c = newKafkaConsumer(cfg, cluster)
    check c.groupHeartbeatTimeoutForTest() == 42
    check c.groupMaxRediscoverAttemptsForTest() == 7
    waitFor c.close()
    waitFor cluster.close()

  test "zero heartbeatTimeoutMs auto-defaults to sessionTimeoutMs div 3":
    let cfg = ConsumerConfig(
      bootstrapBrokers: @["127.0.0.1:1"],
      groupId: "g",
      topics: @["t"],
      clientId: "c",
      sessionTimeoutMs: 30000,
      maxWaitMs: 500,
      minFetchBytes: 1,
      maxFetchBytes: 52428800,
      maxPartitionFetchBytes: 1048576,
      autoCommit: false,
      autoCommitIntervalMs: 5000,
      offsetResetPolicy: orpLatest,
      maxFetcherRestartAttempts: 10,
      recordQueueSize: 1000,
      maxValidationAttempts: 10,
    )
    let cluster =
      newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
    let c = newKafkaConsumer(cfg, cluster)
    check c.groupHeartbeatTimeoutForTest() == 30000 div 3
    waitFor c.close()
    waitFor cluster.close()

  test "negative heartbeatTimeoutMs is rejected":
    var cfg = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    cfg.heartbeatTimeoutMs = -1
    let cluster =
      newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
    expect(KafkaError):
      discard newKafkaConsumer(cfg, cluster)
    waitFor cluster.close()

  test "heartbeatTimeoutMs above sessionTimeoutMs is rejected":
    var cfg = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    cfg.heartbeatTimeoutMs = cfg.sessionTimeoutMs + 1
    let cluster =
      newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
    expect(KafkaError):
      discard newKafkaConsumer(cfg, cluster)
    waitFor cluster.close()

  test "zero maxCoordinatorRediscoverAttempts falls back to 3":
    let cfg = ConsumerConfig(
      bootstrapBrokers: @["127.0.0.1:1"],
      groupId: "g",
      topics: @["t"],
      clientId: "c",
      sessionTimeoutMs: 30000,
      heartbeatIntervalMs: 3000,
      rebalanceTimeoutMs: 60000,
      maxWaitMs: 500,
      minFetchBytes: 1,
      maxFetchBytes: 52428800,
      maxPartitionFetchBytes: 1048576,
      autoCommit: false,
      autoCommitIntervalMs: 5000,
      offsetResetPolicy: orpLatest,
      maxFetcherRestartAttempts: 10,
      recordQueueSize: 1000,
      maxValidationAttempts: 10,
      maxCoordinatorRediscoverAttempts: 0,
        # explicit zero — the fallback should kick in
    )
    let cluster =
      newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
    let c = newKafkaConsumer(cfg, cluster)
    check c.groupMaxRediscoverAttemptsForTest() == 3
    waitFor c.close()
    waitFor cluster.close()

suite "Consumer: pollBatch deferred position update (Java parity)":
  ## Regression: pollBatch* advanced consumedOffset inline per record, so a caller
  ## that cancelled mid-iteration or never observed the batch still had those
  ## records counted as consumed (and committed). Mirroring Java, records
  ## collected in poll* now queue in pendingAdvance and flush on the next
  ## poll*/commit* call.

  test "flushPendingAdvance is a no-op on a fresh consumer":
    let config = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:9092"], groupId = "g", topics = @["t"]
    )
    let cluster = newCluster(@["127.0.0.1:9092"])
    let c = newKafkaConsumer(config, cluster)
    check c.pendingAdvanceLenForTest() == 0
    c.flushPendingAdvance() # idempotent on empty queue
    check c.pendingAdvanceLenForTest() == 0

  test "commitSync surfaces a skip when the state guard blocks the RPC":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:9092"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:9092"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.fetcherForTest().setLastError(nil)

      # Prime a pending record so the commit has something non-empty to commit;
      # an empty commit is a Java-parity no-op (see the next test) and would
      # bypass the generation gate entirely.
      let tp = TopicPartition(topic: "t", partition: 0'i32)
      c.fetcherForTest().registerPartitionForTest(tp, 0)
      c.injectPendingAdvanceForTest(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 4'i64)
      )
      check c.pendingAdvanceLenForTest() == 1

      # No coordinator / generationId < 0: commitSync raises so the caller
      # learns the commit did not land, and pendingAdvance is not flushed.
      var skipped = false
      try:
        await c.commitSync()
      except KafkaCommitSkippedError:
        skipped = true
      check skipped
      check c.pendingAdvanceLenForTest() == 1

      await c.close()
      await cluster.close()

    waitFor testBody()

  test "commitSync is a no-op (Java parity) when there is nothing to commit":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:9092"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:9092"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.fetcherForTest().setLastError(nil)
      check c.pendingAdvanceLenForTest() == 0
      # No coordinator, no consumed offsets → no RPC, no raise. Mirrors
      # Java's Consumer.commitSync(emptyMap): a commit that would write
      # nothing cannot rewind anyone, so short-circuit before the gates.
      await c.commitSync()
      check c.pendingAdvanceLenForTest() == 0

      await c.close()
      await cluster.close()

    waitFor testBody()

  test "seek drops a prior-poll record so it cannot clobber the seeked offset":
    ## Regression: a record from a previous poll sits in pendingAdvance until the
    ## next flush. seek() updated the fetcher position but left that stale record
    ## queued, so the next flushPendingAdvance advanced past the seeked offset and
    ## silently undid the seek. seek must drop pendingAdvance entries for the tp.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      let tp = TopicPartition(topic: "t", partition: 0'i32)
      let f = c.fetcherForTest()
      f.registerPartitionForTest(tp, 0)
      # A record returned by a previous poll, not yet flushed (offset 9 would
      # advance consumedOffset to 10).
      c.injectPendingAdvanceForTest(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 9'i64)
      )
      check c.pendingAdvanceLenForTest() == 1
      # Seek backwards. The stale pending record for `tp` must be dropped.
      c.seek(tp, 5'i64)
      check c.pendingAdvanceLenForTest() == 0
      # Flushing now is a no-op, so the seeked offset survives.
      c.flushPendingAdvance()
      check f.getConsumedOffsets()[tp] == 5'i64
      await c.close()
      await cluster.close()

    waitFor testBody()

  test "invalid seek preserves pendingAdvance so the caller can still commit":
    ## Regression: seek() used to filter pendingAdvance BEFORE validating via
    ## fetcher.seek. On an invalid seek (unassigned tp, negative offset), the
    ## records already handed to the caller were dropped from pendingAdvance
    ## and could no longer be committed, forcing duplicate consumption after
    ## restart.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      let assigned = TopicPartition(topic: "t", partition: 0'i32)
      let unassigned = TopicPartition(topic: "t", partition: 99'i32)
      let f = c.fetcherForTest()
      f.registerPartitionForTest(assigned, 0)
      c.injectPendingAdvanceForTest(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 9'i64)
      )
      check c.pendingAdvanceLenForTest() == 1

      # Invalid seek: unassigned tp raises, pendingAdvance must survive.
      var raisedUnassigned = false
      try:
        c.seek(unassigned, 5'i64)
      except KafkaError:
        raisedUnassigned = true
      check raisedUnassigned
      check c.pendingAdvanceLenForTest() == 1

      # Invalid seek: negative offset raises, pendingAdvance must survive.
      var raisedNegative = false
      try:
        c.seek(assigned, -1'i64)
      except KafkaError:
        raisedNegative = true
      check raisedNegative
      check c.pendingAdvanceLenForTest() == 1

      # Flushing now applies the deferred advance from the original poll.
      c.flushPendingAdvance()
      check f.getConsumedOffsets()[assigned] == 10'i64
      await c.close()
      await cluster.close()

    waitFor testBody()

suite "Consumer: commitAsync offset snapshot (Java parity)":
  ## Regression: commitAsync documents a SYNCHRONOUS position snapshot, but used
  ## to read the offsets inside the spawned task AFTER commitLock.acquire(). A
  ## caller that polled/seeked in that window advanced the live position, so the
  ## task committed past unprocessed records — silent at-most-once loss. The
  ## offsets and generation are now captured at the call instant, passed by value.

  test "commits the call-time offsets even after a later poll advances the position":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      # Generous request timeout: the commit RPC must complete a real round-trip
      # to the fake coordinator (the dead-coordinator probe tests use 50ms to
      # fail fast — here we need it to succeed).
      let coordinator = newBroker(
        nodeId = 0'i32,
        host = "127.0.0.1",
        port = port,
        clientId = "coord",
        requestTimeoutMs = 2000,
        connectTimeoutMs = 2000,
      )

      # Coordinator handler: negotiate OffsetCommit v2, then decode the single
      # commit and record the offset it carried.
      var committedOffset = -1'i64
      proc handler() {.async: (raises: []).} =
        try:
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akOffsetCommit)), minVersion: 2, maxVersion: 2
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          let oc = await fb.nextRequest(timeout = 2.seconds)
          try:
            var r = initKafkaReader(oc.body)
            let req = r.decode(OffsetCommitRequest, 2'i16)
            if req.topics.len > 0 and req.topics[0].partitions.len > 0:
              committedOffset = req.topics[0].partitions[0].offset
            await oc.respond(buildOffsetCommitResponseBodyV3("t", 0'i32, 0'i16))
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupCoordinatorForTest(coordinator)
      c.setGroupGenerationForTest(7'i32)

      let tp = TopicPartition(topic: "t", partition: 0)
      let f = c.fetcherForTest()
      f.registerPartitionForTest(tp, 0)
      # Batch A (..4) handed to the caller: consumedOffset becomes 5.
      f.advanceConsumedOffset(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 4'i64)
      )
      f.bumpGeneration(7'i32)
      check f.getConsumedOffsets()[tp] == 5'i64

      # Park a commit task on the lock so it cannot read the position yet — this
      # is the window the bug exploited.
      await c.acquireCommitLockForTest()
      c.commitAsync() # snapshots {tp: 5} synchronously, then spawns the task

      # The caller polls again while the commit is parked: batch B (5..9) advances
      # the live position to 10. The parked commit must NOT pick this up.
      f.advanceConsumedOffset(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 9'i64)
      )
      check f.getConsumedOffsets()[tp] == 10'i64

      # Release the lock; the task proceeds and commits the snapshot, not 10.
      c.releaseCommitLockForTest()
      for fut in c.pendingCommitsForTest():
        awaitDrain(fut, 2.seconds)
      awaitDrain(handlerFut, 2.seconds)

      check committedOffset == 5'i64

      await c.close()
      await coordinator.close()
      await fb.close()
      await cluster.close()

    waitFor testBody()

  test "skips a snapshot whose generation no longer matches the live one":
    ## Committing a stale snapshot under a newer generation would rewind the
    ## new owner. Skip surfaces as KafkaCommitSkippedError.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      let coord = newBroker(
        nodeId = 1,
        host = "127.0.0.1",
        port = 1'i32,
        clientId = "test-client",
        requestTimeoutMs = 50,
        connectTimeoutMs = 50,
      )
      c.setGroupCoordinatorForTest(coord)

      let tp = TopicPartition(topic: "t", partition: 0)
      let f = c.fetcherForTest()
      f.registerPartitionForTest(tp, 0)
      f.advanceConsumedOffset(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 4'i64)
      )

      # The group has rebalanced to generation 8 and the next poll has already
      # caught the fetcher up to it (assignmentGeneration gate would pass), but
      # the snapshot below was taken back on generation 7.
      c.setGroupGenerationForTest(8'i32)
      f.bumpGeneration(8'i32)
      var snap = initTable[TopicPartition, int64]()
      snap[tp] = 5'i64

      var skipped = false
      try:
        await c.commitSnapshotForTest(snap, 7'i32)
      except KafkaCommitSkippedError:
        skipped = true
      check skipped

      # Positive control: the same offsets under the live gen hit the dead
      # coordinator and raise a non-skip error.
      var raisedNonSkip = false
      try:
        await c.commitSnapshotForTest(snap, 8'i32)
      except KafkaCommitSkippedError:
        discard
      except KafkaError:
        raisedNonSkip = true
      check raisedNonSkip

      await c.close()
      await coord.close()
      await cluster.close()

    waitFor testBody()

  test "routes the closed-consumer error through the callback instead of raising":
    ## Regression: commitAsync is fire-and-forget with errors via the callback,
    ## but a closing/closed consumer's leading requireRunning raised
    ## KafkaClosedError synchronously, bypassing the callback. The closed-state
    ## error must reach the callback, and no task may spawn during shutdown.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markClosed()

      var called = false
      var reported: ref KafkaError = nil
      proc cb(err: ref KafkaError) {.gcsafe, raises: [].} =
        called = true
        reported = err

      # Must NOT raise synchronously despite the consumer being closed.
      c.commitAsync(cb)

      check called
      check reported != nil
      check reported of KafkaClosedError
      check c.pendingCommitsForTest().len == 0

      await cluster.close()

    waitFor testBody()

  test "does not raise synchronously when closed and no callback is given":
    ## The no-callback closed path mirrors the in-task no-callback error
    ## handling: log and return, never raise on the caller's coroutine.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markClosed()

      c.commitAsync()
      check c.pendingCommitsForTest().len == 0

      await cluster.close()

    waitFor testBody()

suite "Consumer: rebalance generation filter (poll-side)":
  ## poll/pollBatch/pollBatchStrict must silently drop records tagged with a
  ## previous group generation. Without the filter, records enqueued before a
  ## rebalance would be returned as if they belonged to the new assignment,
  ## producing duplicates or stale data.

  template makeRecord(gen: int32, off: int64): FetchedRecord =
    FetchedRecord(
      topic: "t",
      partition: 0,
      offset: off,
      timestamp: 0,
      key: none(seq[byte]),
      value: none(seq[byte]),
      headers: @[],
      generation: gen,
    )

  test "poll drops a record from a previous generation and surfaces the live one":
    proc testBody() {.async: (raises: [Exception]).} =
      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let sink = MetricSink()
      config.metricsHook = makeMetricHook(sink)
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupGenerationForTest(5'i32)

      let f = c.fetcherForTest()
      # Push a stale-generation record (gen 3) followed by a live one (gen 5).
      f.bumpGeneration(3'i32)
      f.enqueueRecordForTest(makeRecord(0'i32, 100))
      f.bumpGeneration(5'i32)
      f.enqueueRecordForTest(makeRecord(0'i32, 101))

      let got = await c.poll(milliseconds(500))
      check got.generation == 5'i32
      check got.offset == 101
      # One record was withheld; the event names both generations so a gap in the
      # offsets is traceable to the rebalance that caused it.
      let dropped = findMetricEvents(sink, "consumer.rebalance.stale_records_dropped")
      check dropped.len == 1
      check metricTagValue(dropped[0], "record_generation") == some("3")
      check metricTagValue(dropped[0], "current_generation") == some("5")

      await cluster.close()

    waitFor testBody()

  test "pollBatch silently filters out stale-generation records":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupGenerationForTest(7'i32)

      let f = c.fetcherForTest()
      f.bumpGeneration(3'i32)
      f.enqueueRecordForTest(makeRecord(0'i32, 200))
      f.enqueueRecordForTest(makeRecord(0'i32, 201))
      f.bumpGeneration(7'i32)
      f.enqueueRecordForTest(makeRecord(0'i32, 202))
      f.enqueueRecordForTest(makeRecord(0'i32, 203))

      let batch = await c.pollBatch(maxRecords = 10, timeout = milliseconds(500))
      check batch.len == 2
      check batch[0].offset == 202
      check batch[1].offset == 203
      for r in batch:
        check r.generation == 7'i32

      await cluster.close()

    waitFor testBody()

  test "pollBatchStrict times out when only stale-generation records are present":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupGenerationForTest(9'i32)

      let f = c.fetcherForTest()
      f.bumpGeneration(2'i32)
      f.enqueueRecordForTest(makeRecord(0'i32, 300))

      var timedOut = false
      try:
        discard await c.pollBatchStrict(maxRecords = 5, timeout = milliseconds(200))
      except KafkaTimeoutError:
        timedOut = true
      check timedOut
      await cluster.close()

    waitFor testBody()

  test "pollBatch drops records superseded by a generation bump mid-collection":
    ## Records collected under the live generation pass the collect-time stale
    ## check, but a rebalance can advance the generation before the batch returns.
    ## The post-collection filter must drop them so a stale offset+1 never enters
    ## pendingAdvance and clobbers the position the new generation just reset.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupGenerationForTest(5'i32)

      let f = c.fetcherForTest()
      # Records tagged with the CURRENT generation: they pass the collect-time
      # dropIfStaleGeneration check and are gathered into the batch.
      f.bumpGeneration(5'i32)
      f.enqueueRecordForTest(makeRecord(5'i32, 400))
      f.enqueueRecordForTest(makeRecord(5'i32, 401))

      # Simulate a rebalance completing after collection but before the batch
      # is returned: bump the live generation while pollBatch is still waiting
      # out its timeout for more records.
      proc bumpLater() {.async: (raises: [Exception]).} =
        await sleepAsync(milliseconds(100))
        c.setGroupGenerationForTest(6'i32)

      let bumpFut = bumpLater()
      let batch = await c.pollBatch(maxRecords = 10, timeout = milliseconds(500))
      await bumpFut

      # Both gen-5 records are now stale; none returned, none queued to advance.
      check batch.len == 0
      check c.pendingAdvanceLenForTest() == 0

      await cluster.close()

    waitFor testBody()

  test "pollBatchStrict returns empty (not timeout) when a rebalance empties the batch":
    ## Strict counterpart to the pollBatch generation-bump test: when a rebalance
    ## drops all collected records post-collection, data WAS collected, so this is
    ## a rebalance not a timeout — pollBatchStrict must return an empty seq, not
    ## raise a misleading KafkaTimeoutError.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupGenerationForTest(5'i32)

      let f = c.fetcherForTest()
      f.bumpGeneration(5'i32)
      f.enqueueRecordForTest(makeRecord(5'i32, 400))
      f.enqueueRecordForTest(makeRecord(5'i32, 401))

      proc bumpLater() {.async: (raises: [Exception]).} =
        await sleepAsync(milliseconds(100))
        c.setGroupGenerationForTest(6'i32)

      let bumpFut = bumpLater()
      var raised = false
      var batch: seq[FetchedRecord]
      try:
        batch = await c.pollBatchStrict(maxRecords = 10, timeout = milliseconds(500))
      except KafkaTimeoutError:
        raised = true
      await bumpFut

      # Collected-then-dropped is a rebalance, not a timeout: empty, no raise.
      check not raised
      check batch.len == 0
      check c.pendingAdvanceLenForTest() == 0

      await cluster.close()

    waitFor testBody()

suite "Consumer: reposition filter (poll-side)":
  ## poll / pollBatch / pollBatchStrict must silently drop records whose
  ## posEpoch snapshot no longer matches the partition's live posEpoch — a
  ## seek() / OOR reset that raced ahead of enqueuePartition's post-put
  ## re-check would otherwise leak one pre-seek record.

  template makeRecord(tp: TopicPartition, off: int64, epoch: int64): FetchedRecord =
    FetchedRecord(
      topic: tp.topic,
      partition: tp.partition,
      offset: off,
      timestamp: 0,
      key: none(seq[byte]),
      value: none(seq[byte]),
      headers: @[],
      generation: 1'i32,
      posEpoch: epoch,
    )

  test "poll drops a pre-reposition record and surfaces the live-epoch one":
    proc testBody() {.async: (raises: [Exception]).} =
      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let sink = MetricSink()
      config.metricsHook = makeMetricHook(sink)
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupGenerationForTest(1'i32)

      let f = c.fetcherForTest()
      f.bumpGeneration(1'i32)
      let tp = TopicPartition(topic: "t", partition: 0'i32)
      f.registerPartitionForTest(tp, 0'i64)
      f.applyOffsetResetForTest(tp, 100'i64) # bumps posEpoch to 1

      # Stale (posEpoch 0) then live (posEpoch 1).
      f.enqueueRecordRawForTest(makeRecord(tp, 50, 0))
      f.enqueueRecordRawForTest(makeRecord(tp, 100, f.posEpochForTest(tp)))

      let got = await c.poll(milliseconds(500))
      check got.offset == 100
      check got.posEpoch == f.posEpochForTest(tp)
      # The dropped record's position is reported against the live epoch: a seek
      # raced ahead of the enqueue, and that is what the event has to say.
      let dropped = findMetricEvents(sink, "consumer.reposition.stale_records_dropped")
      check dropped.len == 1
      check metricTagValue(dropped[0], "record_pos_epoch") == some("0")
      check metricTagValue(dropped[0], "current_pos_epoch") == some("1")

      await cluster.close()

    waitFor testBody()

  test "pollBatch silently filters out pre-reposition records":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster =
        newCluster(@["127.0.0.1:1"], connectTimeoutMs = 50, requestTimeoutMs = 50)
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()
      c.setGroupGenerationForTest(1'i32)

      let f = c.fetcherForTest()
      f.bumpGeneration(1'i32)
      let tp = TopicPartition(topic: "t", partition: 0'i32)
      f.registerPartitionForTest(tp, 0'i64)
      f.applyOffsetResetForTest(tp, 200'i64)
      let live = f.posEpochForTest(tp)

      f.enqueueRecordRawForTest(makeRecord(tp, 50, 0))
      f.enqueueRecordRawForTest(makeRecord(tp, 51, 0))
      f.enqueueRecordRawForTest(makeRecord(tp, 200, live))
      f.enqueueRecordRawForTest(makeRecord(tp, 201, live))

      let batch = await c.pollBatch(maxRecords = 10, timeout = milliseconds(500))
      check batch.len == 2
      check batch[0].offset == 200
      check batch[1].offset == 201

      await cluster.close()

    waitFor testBody()

suite "Consumer: zombie-member cleanup on close":
  test "close() sends LeaveGroup when a partial join left a memberId":
    ## Regression: a start() that joined the group (memberId installed) then
    ## failed/cancelled before lsRunning left a heartbeat-less zombie member.
    ## close() used to short-circuit on lsUninitialized and never send
    ## LeaveGroup, stalling the group until the session timed out.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coordinator = newBroker(0'i32, "127.0.0.1", port, "coord")

      var sawLeaveGroup = false
      proc handler() {.async: (raises: []).} =
        try:
          # ensureConnected negotiates API versions on first use.
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akLeaveGroup)), minVersion: 0, maxVersion: 1
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          let lg = await fb.nextRequest(timeout = 2.seconds)
          if lg.apiKey == int16(ord(akLeaveGroup)):
            sawLeaveGroup = true
          try:
            await lg.respond(buildLeaveGroupResponseBodyV1())
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      # Simulate a partial join: coordinator + memberId set, never reached
      # lsRunning. This is exactly the post-joinAndSync failure window.
      c.setPartialMembershipForTest(coordinator, "member-1")
      check c.state == lsUninitialized

      await c.close()
      check c.state == lsClosed
      check sawLeaveGroup

      await coordinator.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "concurrent close() calls run a single teardown":
    ## Regression: close() only bailed on lsClosed, so a close() arriving while
    ## another was parked in LeaveGroup ran the teardown again — a second drain,
    ## a second LeaveGroup, and a second final commit. close() is now
    ## single-flight: the second caller parks on the in-progress one.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coordinator = newBroker(0'i32, "127.0.0.1", port, "coord")

      var leaveGroups = 0
      let leaveArrived = newFuture[void]("leave-arrived")
      let releaseLeave = newFuture[void]("leave-release")
      proc handler() {.async: (raises: []).} =
        try:
          # ensureConnected negotiates API versions on first use.
          let av = await fb.nextRequest(timeout = 2.seconds)
          try:
            await av.respond(
              buildApiVersionsResponseBody(
                0'i16,
                [
                  FakeApiVersion(
                    apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                  ),
                  FakeApiVersion(
                    apiKey: int16(ord(akLeaveGroup)), minVersion: 0, maxVersion: 1
                  ),
                ],
              )
            )
          except CatchableError:
            discard
          let lg = await fb.nextRequest(timeout = 2.seconds)
          if lg.apiKey == int16(ord(akLeaveGroup)):
            inc leaveGroups
          leaveArrived.complete()
          # Hold the response so the close that sent it is still mid-teardown
          # when the second close arrives.
          await releaseLeave
          try:
            await lg.respond(buildLeaveGroupResponseBodyV1())
          except CatchableError:
            discard
          # A second teardown would send its own LeaveGroup on the same
          # connection, and it is sent before the release above.
          try:
            let again = await fb.nextRequest(timeout = 300.milliseconds)
            if again.apiKey == int16(ord(akLeaveGroup)):
              inc leaveGroups
            try:
              await again.respond(buildLeaveGroupResponseBodyV1())
            except CatchableError:
              discard
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()

      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"],
        groupId = "test-group",
        topics = @["test-topic"],
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.setPartialMembershipForTest(coordinator, "member-1")

      let f1 = c.close()
      await leaveArrived
      check not f1.finished # parked on the held LeaveGroup response
      let f2 = c.close()
      check not f2.finished # parked on f1, not running its own teardown
      releaseLeave.complete()
      await f1
      await f2
      check c.state == lsClosed
      check leaveGroups == 1

      await coordinator.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Consumer: auto-commit position advance is coupled to poll":
  ## Regression (Phase 3): the background auto-commit loop calls bare
  ## commitOffsets, which must NOT flush pendingAdvance — else the timer would
  ## commit a batch the app is still processing, widening the at-most-once loss
  ## window. Explicit commits (commitSync/commitAsync/close) and poll DO flush.

  test "background commitOffsets does NOT advance pendingAdvance":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      let tp = TopicPartition(topic: "t", partition: 0)
      let f = c.fetcherForTest()
      f.registerPartitionForTest(tp, 0)
      # Simulate "poll returned this record but the app has not finished it".
      c.injectPendingAdvanceForTest(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 4'i64)
      )
      check c.pendingAdvanceLenForTest() == 1
      check f.getConsumedOffsets().len == 0

      # The bare commitOffsets used by the auto-commit timer must not touch
      # pendingAdvance. With no consumed offsets it no-ops before the coordinator
      # gate, so nothing raises (autoCommitLoop swallows skips in production).
      try:
        await c.commitOffsetsForTest()
      except KafkaCommitSkippedError:
        discard

      check c.pendingAdvanceLenForTest() == 1
      check f.getConsumedOffsets().len == 0

      await c.close()
      await cluster.close()

    waitFor testBody()

  test "commitSync does NOT advance pendingAdvance when the RPC is skipped":
    ## commitSync's contract: advance the live position only after the broker
    ## accepts. On skip the advance must stay parked and the skip surface via
    ## KafkaCommitSkippedError so the caller can retry after the next poll.
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      let tp = TopicPartition(topic: "t", partition: 0)
      let f = c.fetcherForTest()
      f.registerPartitionForTest(tp, 0)
      c.injectPendingAdvanceForTest(
        FetchedRecord(topic: "t", partition: 0'i32, offset: 4'i64)
      )
      check c.pendingAdvanceLenForTest() == 1

      var skipped = false
      try:
        await c.commitSync()
      except KafkaCommitSkippedError:
        skipped = true
      check skipped

      check c.pendingAdvanceLenForTest() == 1
      check f.getConsumedOffsets().len == 0

      await c.close()
      await cluster.close()

    waitFor testBody()

suite "Consumer: max.poll.interval.ms enforcement":
  ## Phase 4: when the application stops polling past maxPollIntervalMs the
  ## heartbeat loop self-leaves the group (so partitions reassign) and arms
  ## pollIntervalExceeded. The next poll surfaces that to the caller exactly
  ## once and clears the flag; a subsequent poll rejoins.

  test "default config sets maxPollIntervalMs to 300000":
    let config = initConsumerConfig(
      bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
    )
    check config.maxPollIntervalMs == 300000'i32

  test "poll raises once on pollIntervalExceeded then clears the flag":
    proc testBody() {.async: (raises: [Exception]).} =
      let config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:1"], groupId = "g", topics = @["t"]
      )
      let cluster = newCluster(@["127.0.0.1:1"])
      let c = newKafkaConsumer(config, cluster)
      c.markRunning()

      # Simulate the heartbeat loop having self-left due to a poll-interval
      # timeout. Avoids standing up a live heartbeat + coordinator to drive
      # the timeout.
      c.setPollIntervalExceededForTest(true)

      var raised = false
      var msg = ""
      try:
        discard await c.poll(milliseconds(10))
      except KafkaError as e:
        raised = true
        msg = e.msg
      check raised
      check "max.poll.interval.ms" in msg
      # Surfaced exactly once: the flag is cleared so a later poll can rejoin
      # rather than raising forever.
      check c.pollIntervalExceededForTest() == false

      await c.close()
      await cluster.close()

    waitFor testBody()

suite "Consumer: failed (re)join keeps the heartbeat alive":
  ## Regression: a failed JoinGroup/SyncGroup must leave the group in
  ## gsRebalancing, NOT gsUnjoined — the member has not left, so the heartbeat
  ## loop (`while g.state != gsUnjoined`) keeps running and avoids a session
  ## eviction. We seed a connected fake-broker coordinator to reach the handler.

  proc respondApiVersions(req: CapturedRequest) {.async: (raises: []).} =
    try:
      await req.respond(
        buildApiVersionsResponseBody(
          0'i16,
          [
            FakeApiVersion(
              apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
            ),
            FakeApiVersion(
              apiKey: int16(ord(akJoinGroup)), minVersion: 0, maxVersion: 2
            ),
            FakeApiVersion(
              apiKey: int16(ord(akSyncGroup)), minVersion: 0, maxVersion: 1
            ),
          ],
        )
      )
    except CatchableError:
      discard

  proc respondApiVersionsWithHeartbeat(req: CapturedRequest) {.async: (raises: []).} =
    try:
      await req.respond(
        buildApiVersionsResponseBody(
          0'i16,
          [
            FakeApiVersion(
              apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
            ),
            FakeApiVersion(
              apiKey: int16(ord(akHeartbeat)), minVersion: 0, maxVersion: 1
            ),
          ],
        )
      )
    except CatchableError:
      discard

  test "JoinGroup error transitions the group to gsRebalancing (not gsUnjoined)":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coord = newBroker(7'i32, "127.0.0.1", port, "coord")

      proc handler() {.async: (raises: []).} =
        try:
          await respondApiVersions(await fb.nextRequest(timeout = 2.seconds))
          let jg = await fb.nextRequest(timeout = 2.seconds)
          try:
            # REBALANCE_IN_PROGRESS (27): a retriable rejoin failure.
            await jg.respond(
              buildJoinGroupResponseBodyV2(
                errorCode = int16(ord(keRebalanceInProgress))
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let cluster = newCluster(@["127.0.0.1:" & $port])
      await coord.connect()
      cluster.seedCoordinatorForTest("test-group", coord)
      let g = newConsumerGroup(cluster, "test-group", @["topic-a"])

      var raised = false
      try:
        discard await g.joinAndSync()
      except KafkaError:
        raised = true
      check raised
      # The fix: a failed rejoin parks in gsRebalancing so the heartbeat loop
      # survives; before the fix this was gsUnjoined and the loop returned.
      check g.state == gsRebalancing
      check g.state != gsUnjoined

      await coord.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "SyncGroup error transitions the group to gsRebalancing (not gsUnjoined)":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coord = newBroker(7'i32, "127.0.0.1", port, "coord")

      proc handler() {.async: (raises: []).} =
        try:
          await respondApiVersions(await fb.nextRequest(timeout = 2.seconds))
          let jg = await fb.nextRequest(timeout = 2.seconds)
          try:
            # JoinGroup succeeds; leader != our memberId so we skip assignment.
            await jg.respond(
              buildJoinGroupResponseBodyV2(
                errorCode = 0'i16,
                generationId = 1'i32,
                protocolName = "range",
                leader = "other-leader",
                memberId = "member-self",
              )
            )
          except CatchableError:
            discard
          let sg = await fb.nextRequest(timeout = 2.seconds)
          try:
            await sg.respond(
              buildSyncGroupResponseBodyV1(
                errorCode = int16(ord(keRebalanceInProgress))
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let cluster = newCluster(@["127.0.0.1:" & $port])
      await coord.connect()
      cluster.seedCoordinatorForTest("test-group", coord)
      let g = newConsumerGroup(cluster, "test-group", @["topic-a"])

      var raised = false
      try:
        discard await g.joinAndSync()
      except KafkaError:
        raised = true
      check raised
      check g.state == gsRebalancing
      check g.state != gsUnjoined

      await coord.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "JoinGroup ILLEGAL_GENERATION resets membership for the next rejoin":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coord = newBroker(7'i32, "127.0.0.1", port, "coord")

      proc handler() {.async: (raises: []).} =
        try:
          await respondApiVersions(await fb.nextRequest(timeout = 2.seconds))
          let jg = await fb.nextRequest(timeout = 2.seconds)
          try:
            await jg.respond(
              buildJoinGroupResponseBodyV2(errorCode = int16(ord(keIllegalGeneration)))
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let cluster = newCluster(@["127.0.0.1:" & $port])
      await coord.connect()
      cluster.seedCoordinatorForTest("test-group", coord)
      let g = newConsumerGroup(cluster, "test-group", @["topic-a"])
      # Simulate a prior membership the broker has since invalidated.
      g.memberId = "stale-member"
      g.generationId = 3'i32

      var raised = false
      try:
        discard await g.joinAndSync()
      except KafkaError:
        raised = true
      check raised
      # The public API must reset the stale membership itself so a second
      # caller re-joins fresh instead of looping on the rejected memberId.
      check g.memberId == ""
      check g.generationId == -1'i32
      check g.state == gsRebalancing

      await coord.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "SyncGroup UNKNOWN_MEMBER_ID resets membership for the next rejoin":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coord = newBroker(7'i32, "127.0.0.1", port, "coord")

      proc handler() {.async: (raises: []).} =
        try:
          await respondApiVersions(await fb.nextRequest(timeout = 2.seconds))
          let jg = await fb.nextRequest(timeout = 2.seconds)
          try:
            # JoinGroup succeeds with a fresh generation...
            await jg.respond(
              buildJoinGroupResponseBodyV2(
                errorCode = 0'i16,
                generationId = 1'i32,
                protocolName = "range",
                leader = "other-leader",
                memberId = "member-self",
              )
            )
          except CatchableError:
            discard
          let sg = await fb.nextRequest(timeout = 2.seconds)
          try:
            # ...but SyncGroup rejects the membership.
            await sg.respond(
              buildSyncGroupResponseBodyV1(errorCode = int16(ord(keUnknownMemberId)))
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let cluster = newCluster(@["127.0.0.1:" & $port])
      await coord.connect()
      cluster.seedCoordinatorForTest("test-group", coord)
      let g = newConsumerGroup(cluster, "test-group", @["topic-a"])

      var raised = false
      try:
        discard await g.joinAndSync()
      except KafkaError:
        raised = true
      check raised
      # doSyncGroup had installed member-self/1 before SyncGroup failed; the
      # reset must clear it.
      check g.memberId == ""
      check g.generationId == -1'i32
      check g.state == gsRebalancing

      await coord.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "teardown during JoinGroup leaves memberId/generationId untouched":
    ## Regression: doSyncGroup stamped the JoinGroup response's memberId and
    ## generationId BEFORE the gsSyncing transition check. A teardown that won
    ## during the JoinGroup await (gsUnjoined) then saw the fresh membership
    ## installed on top of its reset — a zombie membership the coordinator
    ## heartbeats for but the local group no longer does.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coord = newBroker(7'i32, "127.0.0.1", port, "coord")
      let joinCaptured = newAsyncEvent()
      let teardownDone = newAsyncEvent()

      proc handler() {.async: (raises: []).} =
        try:
          await respondApiVersions(await fb.nextRequest(timeout = 2.seconds))
          let jg = await fb.nextRequest(timeout = 2.seconds)
          joinCaptured.fire()
          await teardownDone.wait()
          try:
            await jg.respond(
              buildJoinGroupResponseBodyV2(
                errorCode = 0'i16,
                generationId = 5'i32,
                protocolName = "range",
                leader = "other-leader",
                memberId = "member-fresh",
              )
            )
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let cluster = newCluster(@["127.0.0.1:" & $port])
      await coord.connect()
      cluster.seedCoordinatorForTest("test-group", coord)
      let g = newConsumerGroup(cluster, "test-group", @["topic-a"])

      let joinFut = g.joinAndSync()
      await joinCaptured.wait()
      # Mirror what teardown (leave()/close()) does while the join is in flight.
      discard g.tryTransition(gsUnjoined)
      g.resetMembership()
      teardownDone.fire()

      var raised = false
      try:
        discard await joinFut
      except KafkaError:
        raised = true
      check raised
      # The failed transition must not have stamped the response's membership
      # onto the dead group: it stays exactly as teardown left it.
      check g.memberId == ""
      check g.generationId == -1'i32
      check g.state == gsUnjoined

      await coord.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "permanent heartbeat error lands in gsUnjoined and stops the loop":
    ## Regression: the GroupState doc reserves gsUnjoined for the real "stop
    ## heartbeating" paths — including a permanent heartbeat error — but the
    ## loop returned with the state left at gsStable/gsRebalancing, so
    ## observers read the wrong state after the fatal exit.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coord = newBroker(7'i32, "127.0.0.1", port, "coord")

      proc handler() {.async: (raises: []).} =
        try:
          await respondApiVersionsWithHeartbeat(
            await fb.nextRequest(timeout = 2.seconds)
          )
          while true:
            let hb = await fb.nextRequest(timeout = 2.seconds)
            try:
              await hb.respond(
                buildHeartbeatResponseBodyV1(
                  errorCode = int16(ord(keGroupAuthorizationFailed))
                )
              )
            except CatchableError:
              discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let cluster = newCluster(@["127.0.0.1:" & $port])
      await coord.connect()
      cluster.seedCoordinatorForTest("test-group", coord)
      let g = newConsumerGroup(cluster, "test-group", @["topic-a"], 1000'i32, 100'i32)
      # A joined member whose session dies on the first heartbeat.
      g.coordinator = coord
      g.memberId = "member-1"
      g.generationId = 1'i32
      g.state = gsStable

      var exitFired = 0
      g.onHeartbeatExit = proc() {.gcsafe, raises: [].} =
        exitFired.inc

      g.startHeartbeat()
      let deadline = Moment.now() + seconds(5)
      while g.state != gsUnjoined and Moment.now() < deadline:
        await sleepAsync(milliseconds(10))

      check g.state == gsUnjoined
      check g.rebalanceRequested
      check exitFired == 1
      await g.stopHeartbeat()

      await coord.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

proc serveParkedHeartbeatAndFindCoordinator(
    fb: FakeBroker,
    port: int32,
    parked: AsyncEvent,
    probe: RediscoverProbe,
    findCoordinatorError: int16,
    answerLaterHeartbeats: bool,
): Future[void] {.async: (raises: []).} =
  ## Answer ApiVersions so the coordinator can connect, park the first Heartbeat
  ## (so `heartbeatTimeoutMs` can fire), then handle FindCoordinator. Later
  ## Heartbeats are either answered (`rdSuccess` continues the loop) or left
  ## parked (`rdFailed` rebalances and the loop retries).
  var firstHeartbeat = true
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 2.seconds)
    except CatchableError:
      break
    try:
      let key = req.apiKey
      if key == int16(ord(akApiVersions)):
        await req.respond(
          buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              ),
              FakeApiVersion(
                apiKey: int16(ord(akHeartbeat)), minVersion: 0, maxVersion: 1
              ),
              FakeApiVersion(
                apiKey: int16(ord(akFindCoordinator)), minVersion: 0, maxVersion: 1
              ),
            ],
          )
        )
      elif key == int16(ord(akHeartbeat)):
        if firstHeartbeat:
          firstHeartbeat = false
          parked.fire()
        elif answerLaterHeartbeats:
          await req.respond(buildHeartbeatResponseBodyV1())
      elif key == int16(ord(akFindCoordinator)):
        inc probe.findCoordinatorCount
        await req.respond(
          buildFindCoordinatorResponseBodyV1(
            errorCode = findCoordinatorError,
            nodeId = 7'i32,
            host = "127.0.0.1",
            port = port,
          )
        )
    except CatchableError:
      break

suite "Consumer: heartbeatTimeoutMs actual timeout":
  ## Config plumbing (heartbeatTimeoutMs / rediscover attempts) was covered, but
  ## the live path — Heartbeat `wait(heartbeatTimeout)` firing AsyncTimeoutError
  ## → rediscoverCoordinator → rdFailed signalRebalance / rdSuccess continue —
  ## was not. Timeout does not LeaveGroup or call onHeartbeatExit; those belong
  ## to the permanent-error exit.

  proc makeTimedHeartbeatGroup(
      cluster: Cluster, coord: Broker
  ): ConsumerGroup {.raises: [KafkaError].} =
    let g = newConsumerGroup(
      cluster,
      "test-group",
      @["topic-a"],
      sessionTimeoutMs = 30_000'i32,
      heartbeatIntervalMs = 50'i32,
      heartbeatTimeoutMs = 80'i32,
      maxCoordinatorRediscoverAttempts = 1,
      maxPollIntervalMs = 0'i32,
      metadataMaxAgeMs = 0'i32,
    )
    g.coordinator = coord
    g.memberId = "member-1"
    g.generationId = 1'i32
    g.state = gsStable
    g

  test "heartbeat timeout rebalances when coordinator rediscovery is exhausted":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let parked = newAsyncEvent()
      let probe = RediscoverProbe()
      let hf = serveParkedHeartbeatAndFindCoordinator(
        fb,
        port,
        parked,
        probe,
        findCoordinatorError = keGroupCoordinatorNotAvailable.toInt16,
        answerLaterHeartbeats = false,
      )

      let coord = newBroker(7'i32, "127.0.0.1", port, "coord")
      let cluster = newCluster(@["127.0.0.1:" & $port])
      await coord.connect()
      cluster.seedCoordinatorForTest("test-group", coord)
      cluster.markMetadataFreshForTest()
      let g = makeTimedHeartbeatGroup(cluster, coord)

      var exitFired = 0
      g.onHeartbeatExit = proc() {.gcsafe, raises: [].} =
        exitFired.inc

      g.startHeartbeat()
      await parked.wait().wait(5.seconds)

      let deadline = Moment.now() + seconds(5)
      while g.state == gsStable and Moment.now() < deadline:
        await sleepAsync(milliseconds(10))

      check g.state == gsRebalancing
      check g.rebalanceRequested
      check probe.findCoordinatorCount >= 1
      check not cluster.hasCachedCoordinatorForTest("test-group")
      check exitFired == 0

      await g.stopHeartbeat()
      await coord.close()
      await fb.close()
      await cluster.close()
      awaitDrain(hf, 1.seconds)

    waitFor testBody()

  test "heartbeat timeout does not rebalance when rediscovery succeeds":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let parked = newAsyncEvent()
      let probe = RediscoverProbe()
      let hf = serveParkedHeartbeatAndFindCoordinator(
        fb,
        port,
        parked,
        probe,
        findCoordinatorError = 0'i16,
        answerLaterHeartbeats = true,
      )

      let coord = newBroker(7'i32, "127.0.0.1", port, "coord")
      let cluster = newCluster(@["127.0.0.1:" & $port])
      await coord.connect()
      cluster.seedCoordinatorForTest("test-group", coord)
      cluster.markMetadataFreshForTest()
      let g = makeTimedHeartbeatGroup(cluster, coord)

      var exitFired = 0
      g.onHeartbeatExit = proc() {.gcsafe, raises: [].} =
        exitFired.inc

      g.startHeartbeat()
      await parked.wait().wait(5.seconds)

      let deadline = Moment.now() + seconds(5)
      while probe.findCoordinatorCount == 0 and Moment.now() < deadline:
        await sleepAsync(milliseconds(10))

      check probe.findCoordinatorCount >= 1
      await sleepAsync(milliseconds(100))
      check g.state == gsStable
      check not g.rebalanceRequested
      check cluster.hasCachedCoordinatorForTest("test-group")
      check exitFired == 0

      await g.stopHeartbeat()
      await coord.close()
      await fb.close()
      await cluster.close()
      awaitDrain(hf, 1.seconds)

    waitFor testBody()

suite "Consumer: leader isolates corrupt member subscriptions":
  ## Regression: performAssignment decoded every member's subscription without
  ## per-member isolation. One corrupt metadata blob raised, so the leader
  ## re-failed every JoinGroup — a rebalance storm (with generation bumps) that
  ## persisted until the corrupt member left. The fix skips the member (warn)
  ## and completes the assignment.

  test "undecodable member metadata is skipped and the rebalance completes":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let coord = newBroker(7'i32, "127.0.0.1", port, "coord")
      var syncAssignmentMemberCount = -1
      var syncAssignmentFirstMember = ""

      proc respondApiVersions(req: CapturedRequest) {.async: (raises: []).} =
        try:
          await req.respond(
            buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akJoinGroup)), minVersion: 0, maxVersion: 2
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akSyncGroup)), minVersion: 0, maxVersion: 1
                ),
              ],
            )
          )
        except CatchableError:
          discard

      proc handler() {.async: (raises: []).} =
        try:
          await respondApiVersions(await fb.nextRequest(timeout = 2.seconds))
          let jg = await fb.nextRequest(timeout = 2.seconds)
          try:
            let validSub = encodeSubscription(@["topic-a"])
            await jg.respond(
              buildJoinGroupResponseBodyV2(
                errorCode = 0'i16,
                generationId = 1'i32,
                protocolName = "range",
                leader = "member-self",
                memberId = "member-self",
                members = @[
                  (memberId: "member-self", metadata: validSub),
                  # version = -1 (0xFF, 0xFF) → decodeSubscription raises
                  # KafkaProtocolError; before the fix this failed the leader's
                  # whole assignment and rebalance-stormed the group.
                  (memberId: "member-corrupt", metadata: @[0xff'u8, 0xff'u8]),
                ],
              )
            )
          except CatchableError:
            discard
          let md = await fb.nextRequest(timeout = 2.seconds)
          try:
            await md.respond(
              buildMetadataResponseBody(
                [FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)],
                controllerId = 0'i32,
                topics = [
                  FakeTopicMeta(
                    name: "topic-a",
                    errorCode: 0'i16,
                    isInternal: false,
                    partitions: @[
                      FakePartitionMeta(
                        partitionIndex: 0'i32,
                        leaderId: 0'i32,
                        replicas: @[0'i32],
                        isr: @[0'i32],
                      )
                    ],
                  )
                ],
              )
            )
          except CatchableError:
            discard
          let sg = await fb.nextRequest(timeout = 2.seconds)
          try:
            # The leader must still send SyncGroup with exactly OUR assignment:
            # the corrupt member is skipped, not encoded.
            var r = initKafkaReader(sg.body)
            let req = r.decode(SyncGroupRequest, 1'i16)
            syncAssignmentMemberCount = req.assignments.len
            syncAssignmentFirstMember = req.assignments[0].memberId
            await sg.respond(buildSyncGroupResponseBodyV1(errorCode = 0'i16))
          except CatchableError:
            discard
        except CatchableError:
          discard

      let handlerFut = handler()
      let cluster = newCluster(@["127.0.0.1:" & $port])
      await coord.connect()
      cluster.seedCoordinatorForTest("test-group", coord)
      let g = newConsumerGroup(cluster, "test-group", @["topic-a"])

      var raised = false
      try:
        discard await g.joinAndSync()
      except CatchableError:
        raised = true
      # The corrupt member must not stall the rebalance: join+sync completes
      # and the group reaches gsStable.
      check not raised
      check g.state == gsStable
      check syncAssignmentMemberCount == 1
      check syncAssignmentFirstMember == "member-self"

      await coord.close()
      await fb.close()
      await cluster.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Consumer normal path end to end":
  ## The success path through the public API: start() joins the group, fetches
  ## initial offsets, fires onPartitionsAssigned, spawns the fetcher and
  ## heartbeat loops, and poll() then returns a record the broker actually sent.
  ## The rest of this file forces `markRunning` and calls the entry point under
  ## test directly, so this sequence was never executed in-process — the
  ## integration tests against a real broker cover it.

  proc smokeRecord(baseOffset: int64, value: byte): seq[byte] =
    ## One single-record RecordBatch v2, enough for the fetcher to deliver a
    ## record without a compression or multi-batch path in the way.
    let batch = RecordBatch(
      baseOffset: baseOffset,
      partitionLeaderEpoch: 0'i32,
      attributes: 0'i16,
      firstTimestamp: 0'i64,
      producerId: -1'i64,
      producerEpoch: -1'i16,
      baseSequence: -1'i32,
      records: @[
        Record(
          offset: 0'i64,
          timestamp: 0'i64,
          key: none(seq[byte]),
          value: some(@[value]),
          headers: @[],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(batch)
    w.buf

  test "start, poll and close drive a full group session":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "smoke", partition: 0)

      let versions = @[
        FakeApiVersion(apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0),
        FakeApiVersion(apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1),
        FakeApiVersion(
          apiKey: int16(ord(akFindCoordinator)), minVersion: 0, maxVersion: 1
        ),
        FakeApiVersion(apiKey: int16(ord(akJoinGroup)), minVersion: 0, maxVersion: 2),
        FakeApiVersion(apiKey: int16(ord(akSyncGroup)), minVersion: 0, maxVersion: 1),
        FakeApiVersion(apiKey: int16(ord(akHeartbeat)), minVersion: 0, maxVersion: 1),
        FakeApiVersion(apiKey: int16(ord(akLeaveGroup)), minVersion: 0, maxVersion: 1),
        FakeApiVersion(apiKey: int16(ord(akOffsetFetch)), minVersion: 1, maxVersion: 3),
        FakeApiVersion(apiKey: int16(ord(akOffsetCommit)), minVersion: 1, maxVersion: 3),
        FakeApiVersion(apiKey: int16(ord(akFetch)), minVersion: 4, maxVersion: 7),
      ]

      var assignedInListener: seq[TopicPartition] = @[]
      var apiKeysSeen = initTable[int16, int]()

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 5.seconds)
          except CatchableError:
            break
          apiKeysSeen.mgetOrPut(req.apiKey, 0).inc
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(buildApiVersionsResponseBody(0'i16, versions))
            elif req.apiKey == int16(ord(akMetadata)):
              let brokers: seq[FakeBrokerMeta] =
                @[FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: port)]
              let topics: seq[FakeTopicMeta] = @[
                FakeTopicMeta(
                  name: "smoke",
                  errorCode: 0'i16,
                  isInternal: false,
                  partitions: @[
                    FakePartitionMeta(
                      partitionIndex: 0'i32,
                      leaderId: 0'i32,
                      replicas: @[0'i32],
                      isr: @[0'i32],
                    )
                  ],
                )
              ]
              await req.respond(
                buildMetadataResponseBody(
                  brokers, controllerId = 0'i32, topics = topics
                )
              )
            elif req.apiKey == int16(ord(akFindCoordinator)):
              await req.respond(
                buildFindCoordinatorResponseBodyV1(
                  errorCode = 0'i16, nodeId = 0'i32, host = "127.0.0.1", port = port
                )
              )
            elif req.apiKey == int16(ord(akJoinGroup)):
              let members: seq[tuple[memberId: string, metadata: seq[byte]]] =
                @[(memberId: "member-1", metadata: encodeSubscription(@["smoke"]))]
              await req.respond(
                buildJoinGroupResponseBodyV2(
                  errorCode = 0'i16,
                  generationId = 1'i32,
                  protocolName = "range",
                  # We are the sole member and so the leader: the assignment is
                  # computed on our side from the members we report here.
                  leader = "member-1",
                  memberId = "member-1",
                  members = members,
                )
              )
            elif req.apiKey == int16(ord(akSyncGroup)):
              await req.respond(
                buildSyncGroupResponseBodyV1(
                  errorCode = 0'i16, assignment = encodeAssignment(@[tp])
                )
              )
            elif req.apiKey == int16(ord(akOffsetFetch)):
              await req.respond(
                buildOffsetFetchResponseBodyV3("smoke", 0'i32, 0'i16, 0'i64)
              )
            elif req.apiKey == int16(ord(akHeartbeat)):
              await req.respond(buildHeartbeatResponseBodyV1())
            elif req.apiKey == int16(ord(akLeaveGroup)):
              await req.respond(buildLeaveGroupResponseBodyV1())
            elif req.apiKey == int16(ord(akFetch)):
              let fetchTopics: seq[FakeFetchTopic] = @[
                FakeFetchTopic(
                  topic: "smoke",
                  partitions: @[
                    FakeFetchPartition(
                      partition: 0'i32,
                      errorCode: 0'i16,
                      highWatermark: 1'i64,
                      lastStableOffset: 1'i64,
                      logStartOffset: 0'i64,
                      recordsRaw: some(smokeRecord(0'i64, byte('x'))),
                    )
                  ],
                )
              ]
              await req.respond(
                buildFetchResponseBody(
                  throttleTimeMs = 0'i32,
                  errorCode = 0'i16,
                  sessionId = 0'i32,
                  topics = fetchTopics,
                )
              )
            else:
              await req.closeTransport()
          except CatchableError:
            discard

      let h = handler()
      let sink = MetricSink(events: @[])
      var config = initConsumerConfig(
        bootstrapBrokers = @["127.0.0.1:" & $port],
        groupId = "smoke-group",
        topics = @["smoke"],
      )
      config.autoCommit = false # no commit in this test; keep the wire minimal
      config.metricsHook = makeMetricHook(sink)
      config.onPartitionsAssigned = proc(
          partitions: seq[TopicPartition]
      ) {.gcsafe, raises: [].} =
        assignedInListener = partitions

      let cluster = newCluster(@["127.0.0.1:" & $port])
      let c = newKafkaConsumer(config, cluster)

      # The whole point: reach lsRunning through start(), not markRunning().
      await c.start()

      let records = await c.pollBatch(maxRecords = 10, timeout = milliseconds(5000))
      check records.len == 1
      check records[0].topic == "smoke"
      check records[0].partition == 0'i32
      check records[0].offset == 0'i64
      check records[0].value == some(@[byte('x')])

      # start() fires the listener before the fetch loop spawns, per the Java
      # contract that lets user code seek() inside it.
      check assignedInListener == @[tp]

      # Every stage of the session actually went over the wire.
      for key in [akFindCoordinator, akJoinGroup, akSyncGroup, akOffsetFetch, akFetch]:
        check apiKeysSeen.getOrDefault(int16(ord(key)), 0) > 0

      # The fetch round reported the record at enqueue, before poll() consumed it.
      let ev = findMetricEvent(sink, "consumer.fetch.records")
      check ev.isSome
      check ev.get.value == 1'i64
      check metricTagValue(ev.get, "topic") == some("smoke")
      check metricTagValue(ev.get, "partition") == some("0")

      # Every completed fetch round reports its duration.
      let latency = findMetricEvent(sink, "consumer.fetch.latency_ms")
      check latency.isSome
      check metricTagValue(latency.get, "broker") == some("0")

      await c.close()
      await cluster.close()
      await fb.close()
      awaitDrain(h, 1.seconds)

    waitFor body()
