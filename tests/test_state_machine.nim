## In-memory unit tests for the producer transaction and consumer group
## state machines introduced in Phase 5. These exercise tryTransition
## directly without spinning up a real broker.

import std/[unittest, options, strutils, importutils]

import chronos

import async_kafka/[errors, cluster]
import async_kafka/consumer/group
import async_kafka/producer/producer
import async_kafka/producer/types
import async_kafka/producer/transaction
import async_kafka/protocol/messages

import producer_helpers

privateAccess(KafkaProducer)

type
  TxnMetricEvent = tuple[event: string, tags: seq[(string, string)], value: int64]
  TxnMetricSink = ref object
    events: seq[TxnMetricEvent]

proc makeTxnMetricHook(sink: TxnMetricSink): MetricsHook =
  result = proc(
      event: cstring, tags: openArray[(cstring, cstring)], value: int64
  ) {.gcsafe, raises: [].} =
    var copied: seq[(string, string)]
    for t in tags:
      copied.add(($t[0], $t[1]))
    sink.events.add((event: $event, tags: copied, value: value))

proc txnMetricsNamed(sink: TxnMetricSink, name: string): seq[TxnMetricEvent] =
  for ev in sink.events:
    if ev.event == name:
      result.add(ev)

suite "Producer: TransactionState transitions":
  test "fresh producer is in tsUninitialized":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster, transactionalId = some("test-txn"), enableIdempotence = true
    )
    check p.txnState == tsUninitialized

  test "tryTransition validates allowed edges":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster, transactionalId = some("test-txn"), enableIdempotence = true
    )
    # Uninitialized → Ready (after init) is allowed.
    p.setTxnState(tsUninitialized)
    check p.tryTransitionForTest(tsReady)
    check p.txnState == tsReady

    # Ready → InTransaction
    check p.tryTransitionForTest(tsInTransaction)
    check p.txnState == tsInTransaction

    # InTransaction → Committing
    check p.tryTransitionForTest(tsCommitting)
    check p.txnState == tsCommitting

    # Committing → Ready
    check p.tryTransitionForTest(tsReady)
    check p.txnState == tsReady

  test "tryTransition rejects illegal edges":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster, transactionalId = some("test-txn"), enableIdempotence = true
    )
    # Uninitialized cannot jump straight to InTransaction.
    p.setTxnState(tsUninitialized)
    check not p.tryTransitionForTest(tsInTransaction)
    check p.txnState == tsUninitialized

    # Ready cannot go straight to Committing.
    p.setTxnState(tsReady)
    check not p.tryTransitionForTest(tsCommitting)
    check p.txnState == tsReady

    # InTransaction cannot bypass commit/abort and become Ready directly.
    p.setTxnState(tsInTransaction)
    check not p.tryTransitionForTest(tsReady)
    check p.txnState == tsInTransaction

  test "tsFatal is absorbing":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster, transactionalId = some("test-txn"), enableIdempotence = true
    )
    p.setTxnState(tsInTransaction)
    check p.tryTransitionForTest(tsFatal)
    check p.txnState == tsFatal
    # Once fatal, no further transitions are allowed.
    check not p.tryTransitionForTest(tsReady)
    check not p.tryTransitionForTest(tsInTransaction)
    check not p.tryTransitionForTest(tsAborting)
    check p.txnState == tsFatal

  test "self-transition is idempotent":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster, transactionalId = some("test-txn"), enableIdempotence = true
    )
    p.setTxnState(tsInTransaction)
    check p.tryTransitionForTest(tsInTransaction)
    check p.txnState == tsInTransaction

  test "tsError can recover via abort":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster, transactionalId = some("test-txn"), enableIdempotence = true
    )
    p.setTxnState(tsError)
    check p.tryTransitionForTest(tsAborting)
    check p.txnState == tsAborting

  test "transitionToError from tsCommitting picks tsError for retriable code":
    ## Regression: commitTransaction's post-EndTxn bump can yield a retriable
    ## code (OUT_OF_ORDER_SEQUENCE_NUMBER); the transaction module and the batch
    ## pipeline's catch both call transitionToError. Both must land on tsError
    ## (not tsCommitting, not tsFatal) so the user can recover via abort.
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster, transactionalId = some("test-txn"), enableIdempotence = true
    )
    p.setTxnState(tsCommitting)
    p.transitionToError(keOutOfOrderSequenceNumber)
    check p.txnState == tsError
    # Second call (the batch pipeline's catch re-invokes transitionToError)
    # must be idempotent — tsError self-loop is allowed.
    p.transitionToError(keOutOfOrderSequenceNumber)
    check p.txnState == tsError

  test "transitionToError emits producer.txn.error once":
    ## transaction.tsFatal: a real tsError transition used to be silent except
    ## for the refused-edge warn. metricsHook must see producer.txn.error once
    ## with error_code; a self-loop (batch catch re-invoke) must not increment.
    let sink = TxnMetricSink(events: @[])
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster,
      transactionalId = some("test-txn"),
      enableIdempotence = true,
      metricsHook = makeTxnMetricHook(sink),
    )
    p.setTxnState(tsCommitting)
    p.transitionToError(keOutOfOrderSequenceNumber)
    check p.txnState == tsError
    let first = txnMetricsNamed(sink, "producer.txn.error")
    check first.len == 1
    check first[0].value == 1
    check first[0].tags == @[("error_code", "OUT_OF_ORDER_SEQUENCE_NUMBER")]
    p.transitionToError(keOutOfOrderSequenceNumber)
    check p.txnState == tsError
    check txnMetricsNamed(sink, "producer.txn.error").len == 1

  test "transitionToError from tsCommitting picks tsFatal for fatal code":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster, transactionalId = some("test-txn"), enableIdempotence = true
    )
    p.setTxnState(tsCommitting)
    p.transitionToError(keProducerFenced)
    check p.txnState == tsFatal
    # Fatal is absorbing — re-invocation does not regress to tsError.
    p.transitionToError(keOutOfOrderSequenceNumber)
    check p.txnState == tsFatal

  test "transitionToError emits producer.txn.fatal once":
    ## transaction.tsFatal: PRODUCER_FENCED (close required) used to leave no
    ## metricsHook signal. A real tsFatal change emits producer.txn.fatal once;
    ## a retriable code against the absorbing state must not increment.
    let sink = TxnMetricSink(events: @[])
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster,
      transactionalId = some("test-txn"),
      enableIdempotence = true,
      metricsHook = makeTxnMetricHook(sink),
    )
    p.setTxnState(tsCommitting)
    p.transitionToError(keProducerFenced)
    check p.txnState == tsFatal
    let first = txnMetricsNamed(sink, "producer.txn.fatal")
    check first.len == 1
    check first[0].value == 1
    check first[0].tags == @[("error_code", "PRODUCER_FENCED")]
    p.transitionToError(keOutOfOrderSequenceNumber)
    check p.txnState == tsFatal
    check txnMetricsNamed(sink, "producer.txn.fatal").len == 1
    check txnMetricsNamed(sink, "producer.txn.error").len == 0

  test "transitionToError from tsAborting picks tsError for retriable code":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster, transactionalId = some("test-txn"), enableIdempotence = true
    )
    p.setTxnState(tsAborting)
    p.transitionToError(keOutOfOrderSequenceNumber)
    check p.txnState == tsError

  test "transactional INVALID_RECORD picks tsError, not tsFatal":
    ## INVALID_RECORD (87) is fatal in the context-free matrix but abortable
    ## inside a transaction (Java TransactionManager parity). A Produce
    ## response carrying 87 must fail only the current txn — leaving it
    ## recoverable via abort — instead of poisoning the producer for good.
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster, transactionalId = some("test-txn"), enableIdempotence = true
    )
    p.setTxnState(tsInTransaction)
    p.transitionToError(keInvalidRecord)
    check p.txnState == tsError
    check p.tryTransitionForTest(tsAborting)

  test "beginTransaction in tsFatal raises":
    proc testBody() {.async: (raises: [Exception]).} =
      let sink = TxnMetricSink(events: @[])
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster,
        transactionalId = some("test-txn"),
        enableIdempotence = true,
        metricsHook = makeTxnMetricHook(sink),
      )
      p.setTxnState(tsFatal)
      var raised = false
      try:
        p.beginTransaction()
      except KafkaError as e:
        raised = true
        check "fatal" in e.msg
      check raised
      check txnMetricsNamed(sink, "producer.txn.begin").len == 0

    waitFor testBody()

  test "beginTransaction emits producer.txn.begin":
    ## transaction.begin: tsReady → tsInTransaction used to leave no
    ## metricsHook signal. A real begin emits producer.txn.begin once;
    ## a refused re-entry must not increment.
    let sink = TxnMetricSink(events: @[])
    let cluster = newCluster(@["127.0.0.1:9092"])
    let p = newKafkaProducer(
      cluster,
      transactionalId = some("test-txn"),
      enableIdempotence = true,
      metricsHook = makeTxnMetricHook(sink),
    )
    p.setTxnState(tsReady)
    p.beginTransaction()
    check p.txnState == tsInTransaction
    let first = txnMetricsNamed(sink, "producer.txn.begin")
    check first.len == 1
    check first[0].value == 1
    check first[0].tags.len == 0
    var raised = false
    try:
      p.beginTransaction()
    except KafkaError as e:
      raised = true
      check "tsInTransaction" in e.msg
    check raised
    check p.txnState == tsInTransaction
    check txnMetricsNamed(sink, "producer.txn.begin").len == 1

suite "Consumer: GroupState transitions":
  test "fresh group is in gsUnjoined":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let g = newConsumerGroup(cluster, "group-a", @["topic-a"])
    check g.state == gsUnjoined

  test "tryTransition allows the canonical join → sync → stable path":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let g = newConsumerGroup(cluster, "group-a", @["topic-a"])
    check g.tryTransition(gsJoining)
    check g.state == gsJoining
    check g.tryTransition(gsSyncing)
    check g.state == gsSyncing
    check g.tryTransition(gsStable)
    check g.state == gsStable

  test "tryTransition allows stable → rebalancing → joining":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let g = newConsumerGroup(cluster, "group-a", @["topic-a"])
    g.state = gsStable
    check g.tryTransition(gsRebalancing)
    check g.state == gsRebalancing
    check g.tryTransition(gsJoining)
    check g.state == gsJoining

  test "tryTransition rejects illegal edges":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let g = newConsumerGroup(cluster, "group-a", @["topic-a"])
    # Unjoined cannot jump to Stable.
    check not g.tryTransition(gsStable)
    # Unjoined cannot jump to Syncing.
    check not g.tryTransition(gsSyncing)
    check g.state == gsUnjoined

  test "self-transition is idempotent":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let g = newConsumerGroup(cluster, "group-a", @["topic-a"])
    g.state = gsStable
    check g.tryTransition(gsStable)
    check g.state == gsStable

  test "tryTransition from gsSyncing to gsRebalancing is allowed":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let g = newConsumerGroup(cluster, "group-a", @["topic-a"])
    discard g.tryTransition(gsJoining)
    discard g.tryTransition(gsSyncing)
    check g.tryTransition(gsRebalancing)
    check g.state == gsRebalancing

  test "tryTransition rejects gsUnjoined to gsRebalancing":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let g = newConsumerGroup(cluster, "group-a", @["topic-a"])
    check g.state == gsUnjoined
    check not g.tryTransition(gsRebalancing)
    check g.state == gsUnjoined

  test "tryTransition allows stable → joining (direct re-join)":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let g = newConsumerGroup(cluster, "group-a", @["topic-a"])
    g.state = gsStable
    check g.tryTransition(gsJoining)
    check g.state == gsJoining

  test "newConsumerGroup rejects heartbeatIntervalMs <= 0":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect(KafkaError):
      discard newConsumerGroup(cluster, "g", @["t"], heartbeatIntervalMs = 0)
    expect(KafkaError):
      discard newConsumerGroup(cluster, "g", @["t"], heartbeatIntervalMs = -1)

  test "newConsumerGroup rejects sessionTimeoutMs <= 0":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect(KafkaError):
      discard newConsumerGroup(cluster, "g", @["t"], sessionTimeoutMs = 0)

  test "newConsumerGroup rejects heartbeatIntervalMs >= sessionTimeoutMs":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect(KafkaError):
      discard newConsumerGroup(
        cluster, "g", @["t"], sessionTimeoutMs = 3000, heartbeatIntervalMs = 3000
      )
    expect(KafkaError):
      discard newConsumerGroup(
        cluster, "g", @["t"], sessionTimeoutMs = 3000, heartbeatIntervalMs = 5000
      )

  test "newConsumerGroup rejects negative heartbeatTimeoutMs":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect(KafkaError):
      discard newConsumerGroup(cluster, "g", @["t"], heartbeatTimeoutMs = -1)

  test "newConsumerGroup rejects heartbeatTimeoutMs above sessionTimeoutMs":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect(KafkaError):
      discard newConsumerGroup(
        cluster,
        "g",
        @["t"],
        sessionTimeoutMs = 30000,
        heartbeatIntervalMs = 3000,
        heartbeatTimeoutMs = 30001,
      )

  test "newConsumerGroup accepts auto and session-bound heartbeat timeouts":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let auto = newConsumerGroup(
      cluster, "g", @["t"], sessionTimeoutMs = 2, heartbeatIntervalMs = 1
    )
    check auto.heartbeatTimeoutMs() == 1
    let equal = newConsumerGroup(
      cluster,
      "g",
      @["t"],
      sessionTimeoutMs = 30000,
      heartbeatIntervalMs = 3000,
      heartbeatTimeoutMs = 30000,
    )
    check equal.heartbeatTimeoutMs() == 30000

  test "newConsumerGroup rejects rebalanceTimeoutMs <= 0":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect(KafkaError):
      discard newConsumerGroup(cluster, "g", @["t"], rebalanceTimeoutMs = 0)
    expect(KafkaError):
      discard newConsumerGroup(cluster, "g", @["t"], rebalanceTimeoutMs = -1)

  test "newConsumerGroup rejects negative poll interval and metadata max age":
    ## 0 is the documented off switch for both checks (ConsumerConfig says so);
    ## a negative value would disable them with nothing saying why.
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect(KafkaError):
      discard newConsumerGroup(cluster, "g", @["t"], maxPollIntervalMs = -1)
    expect(KafkaError):
      discard newConsumerGroup(cluster, "g", @["t"], metadataMaxAgeMs = -1)

  test "newConsumerGroup accepts 0 for both documented off switches":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let g = newConsumerGroup(
      cluster, "g", @["t"], maxPollIntervalMs = 0, metadataMaxAgeMs = 0
    )
    check g.maxPollIntervalMs == 0
    check g.metadataMaxAgeMs == 0

  test "newConsumerGroup rejects rebalanceTimeoutMs < sessionTimeoutMs":
    let cluster = newCluster(@["127.0.0.1:9092"])
    expect(KafkaError):
      discard newConsumerGroup(
        cluster, "g", @["t"], sessionTimeoutMs = 30000, rebalanceTimeoutMs = 29999
      )

  test "newConsumerGroup accepts rebalanceTimeoutMs >= sessionTimeoutMs":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let equal = newConsumerGroup(
      cluster, "g", @["t"], sessionTimeoutMs = 30000, rebalanceTimeoutMs = 30000
    )
    check equal.rebalanceTimeoutMs == 30000'i32
    let defaults = newConsumerGroup(cluster, "g", @["t"])
    check defaults.sessionTimeoutMs == 30000'i32
    check defaults.rebalanceTimeoutMs == 60000'i32

  test "resetMembership clears memberId and generationId":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let g = newConsumerGroup(cluster, "group-a", @["topic-a"])
    g.memberId = "member-42"
    g.generationId = 7
    g.resetMembership()
    check g.memberId == ""
    check g.generationId == -1'i32

  test "resetMembership with custom values":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let g = newConsumerGroup(cluster, "group-a", @["topic-a"])
    g.resetMembership("new-member", 3)
    check g.memberId == "new-member"
    check g.generationId == 3'i32

  test "snapshotMembership returns a consistent triple":
    let cluster = newCluster(@["127.0.0.1:9092"])
    let g = newConsumerGroup(cluster, "group-a", @["topic-a"])
    g.memberId = "member-42"
    g.generationId = 7
    g.state = gsStable
    let snap = g.snapshotMembership()
    check snap.memberId == "member-42"
    check snap.generationId == 7'i32
    check snap.state == gsStable

  test "heartbeatResponseIsCurrent gates stale rebalance heartbeats":
    # Current: stable group, generation unchanged since the snapshot → act.
    check heartbeatResponseIsCurrent(gsStable, 7'i32, 7'i32)
    # Stale: a concurrent rejoin advanced the generation during the await →
    # ignore the ILLEGAL_GENERATION so it cannot clobber the new membership.
    check not heartbeatResponseIsCurrent(gsStable, 7'i32, 8'i32)
    # Already rebalancing when snapshotted → the rejoin path owns it.
    check not heartbeatResponseIsCurrent(gsRebalancing, 7'i32, 7'i32)
    check not heartbeatResponseIsCurrent(gsJoining, 7'i32, 7'i32)

  test "pollIntervalExpired fires only when enabled and elapsed":
    let base = Moment.init(1_000_000'i64, nanoseconds(0))
    # Disabled (<= 0) never fires, even after a long gap.
    check not pollIntervalExpired(0'i32, base, base + seconds(3600))
    check not pollIntervalExpired(-1'i32, base, base + seconds(3600))
    # Enabled but not yet elapsed.
    check not pollIntervalExpired(1000'i32, base, base + milliseconds(999))
    # Exactly at the boundary is not "more than" — strict greater-than.
    check not pollIntervalExpired(1000'i32, base, base + milliseconds(1000))
    # Past the window → fire.
    check pollIntervalExpired(1000'i32, base, base + milliseconds(1001))
    check pollIntervalExpired(300000'i32, base, base + seconds(301))

  test "sessionTimerExpired fires only when enabled and elapsed":
    let base = Moment.init(1_000_000'i64, nanoseconds(0))
    # Disabled (<= 0) never fires.
    check not sessionTimerExpired(0'i32, base, base + seconds(3600))
    check not sessionTimerExpired(-1'i32, base, base + seconds(3600))
    # Enabled but within the session window.
    check not sessionTimerExpired(30000'i32, base, base + milliseconds(29999))
    # Exactly at the boundary is not "more than" — strict greater-than.
    check not sessionTimerExpired(30000'i32, base, base + milliseconds(30000))
    # Past the session window → fire.
    check sessionTimerExpired(30000'i32, base, base + milliseconds(30001))
    check sessionTimerExpired(30000'i32, base, base + seconds(60))

suite "Producer KIP-185: epoch-bump sequence restart":
  ## After a commit/abort the broker bumps the producer epoch. KIP-185 requires
  ## clearing the per-TP sequence table so the next batch restarts at 0 under the
  ## new (id, epoch). Pins that against re-using the pre-bump sequence (broker
  ## fences) or leaving the in-flight queue stale.

  proc makeRecord(): Record =
    Record(
      offset: 0,
      timestamp: 0,
      key: none(seq[byte]),
      value: some(@[0x01'u8]),
      headers: @[],
    )

  test "in-flight queue is cleared on producer-id bump":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster, transactionalId = some("test-txn"), enableIdempotence = true
      )
      p.setProducerId(1000'i64, 0'i16)
      let tp = TopicPartition(topic: "t", partition: 0)

      discard await p.allocateInflightForTest(@[makeRecord()], tp, 0)
      check p.inflightBatchCountForTest(tp) == 1

      # Simulate a successful transaction commit followed by bumpEpoch:
      # producerId stays the same, epoch increments. KIP-185 requires
      # the sequence table to reset so the next send starts at seq=0.
      p.setProducerId(1000'i64, 1'i16)
      p.clearInflightForTest()
      p.setSequenceNumber(tp, 0'i32)

      check p.inflightBatchCountForTest(tp) == 0
      check p.getSequenceNumber(tp) == 0'i32

      # The next allocation on the same TP must start at seq=0 under
      # the new epoch.
      discard await p.allocateInflightForTest(@[makeRecord()], tp, 0)
      check p.getInflightBaseSequenceForTest(tp, 0) == 0'i32
      check p.getInflightProducerEpochForTest(tp, 0) == 1'i16

    waitFor body()

  test "bumpEpoch abandons silently when producer is closing":
    ## Regression: close() flips state out of lsRunning before abortTransaction,
    ## whose post-EndTxn bumpEpoch must NOT raise — the EndTxn already succeeded,
    ## so raising would report a good abort as failed and emit a spurious
    ## txn.*.error. bumpEpoch returns immediately once state != lsRunning.
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:9092"])
      let p = newKafkaProducer(
        cluster, transactionalId = some("test-txn"), enableIdempotence = true
      )
      p.setProducerId(1000'i64, 5'i16)
      p.markClosed() # state -> lsClosed (!= lsRunning)
      await p.bumpEpoch() # must not raise, must not dereference nil coordinator
      check p.getProducerEpoch() == 5'i16

    waitFor body()

suite "firstTxnErrorCode response extraction":
  ## The coordinator RPC driver reads a single error code out of each txn
  ## response via the firstTxnErrorCode overloads. The top-level-code responses
  ## pass it through; the per-partition responses (AddPartitionsToTxn /
  ## TxnOffsetCommit carry no top-level code) scan for the first non-keNone.

  test "InitProducerIdResponse passes through the top-level code":
    check firstTxnErrorCode(InitProducerIdResponse(errorCode: keNone)) == keNone
    check firstTxnErrorCode(InitProducerIdResponse(errorCode: keConcurrentTransactions)) ==
      keConcurrentTransactions

  test "AddOffsetsToTxnResponse passes through the top-level code":
    check firstTxnErrorCode(AddOffsetsToTxnResponse(errorCode: keNone)) == keNone
    check firstTxnErrorCode(AddOffsetsToTxnResponse(errorCode: keNotCoordinator)) ==
      keNotCoordinator

  test "EndTxnResponse passes through the top-level code":
    check firstTxnErrorCode(EndTxnResponse(errorCode: keNone)) == keNone
    check firstTxnErrorCode(EndTxnResponse(errorCode: keProducerFenced)) ==
      keProducerFenced

  test "AddPartitionsToTxnResponse returns the first non-keNone across topics":
    let resp = AddPartitionsToTxnResponse(
      errors: @[
        AddPartitionsToTxnTopicResp(
          topic: "a",
          partitions: @[
            AddPartitionsToTxnPartitionResp(partition: 0'i32, errorCode: keNone),
            AddPartitionsToTxnPartitionResp(partition: 1'i32, errorCode: keNone),
          ],
        ),
        AddPartitionsToTxnTopicResp(
          topic: "b",
          partitions: @[
            AddPartitionsToTxnPartitionResp(
              partition: 0'i32, errorCode: keOperationNotAttempted
            )
          ],
        ),
      ]
    )
    check firstTxnErrorCode(resp) == keOperationNotAttempted

  test "TxnOffsetCommitResponse returns the first non-keNone across topics":
    let resp = TxnOffsetCommitResponse(
      topics: @[
        TxnOffsetCommitTopicResp(
          topic: "a",
          partitions:
            @[TxnOffsetCommitPartitionResp(partition: 0'i32, errorCode: keNone)],
        ),
        TxnOffsetCommitTopicResp(
          topic: "b",
          partitions: @[
            TxnOffsetCommitPartitionResp(
              partition: 2'i32, errorCode: keCoordinatorLoadInProgress
            )
          ],
        ),
      ]
    )
    check firstTxnErrorCode(resp) == keCoordinatorLoadInProgress

  test "TxnOffsetCommitResponse with all keNone yields keNone":
    let resp = TxnOffsetCommitResponse(
      topics: @[
        TxnOffsetCommitTopicResp(
          topic: "a",
          partitions:
            @[TxnOffsetCommitPartitionResp(partition: 0'i32, errorCode: keNone)],
        )
      ]
    )
    check firstTxnErrorCode(resp) == keNone
    check firstTxnErrorCode(TxnOffsetCommitResponse()) == keNone
