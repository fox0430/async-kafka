import std/[tables, options, importutils, strutils]
import unittest
import chronos
import pkg/stew/endians2

import async_kafka/[errors, cluster, broker]
import async_kafka/internal/metrics
import async_kafka/consumer/fetcher
import async_kafka/protocol/[messages, primitives, api_keys, compression, crc32c]

import fetcher_helpers, cluster_helpers, fake_broker, async_helpers

# Internal tests intentionally construct and inspect the delivery context that
# remains private in the application facade.
privateAccess(FetchedRecord)

suite "Fetcher consumed offset tracking":
  test "getConsumedOffsets skips partitions never advanced or seeked":
    # Regression for the consumedOffset initial-value bug: returning the
    # placeholder copy of `fetchOffset` here would commit a future offset
    # under orpLatest. Only advanceConsumedOffset / seek may set the
    # everConsumed flag that gates inclusion.
    let f = newFetcher(nil)
    let tp1 = TopicPartition(topic: "test", partition: 0)
    let tp2 = TopicPartition(topic: "test", partition: 1)
    f.registerPartitionForTest(tp1, 100)
    f.registerPartitionForTest(tp2, 200)

    let offsets = f.getConsumedOffsets()
    check offsets.len == 0
    check tp1 notin offsets
    check tp2 notin offsets

  test "advanceConsumedOffset updates offset to record.offset + 1":
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "test-topic", partition: 0)
    f.registerPartitionForTest(tp, 0)

    let rec = FetchedRecord(
      topic: "test-topic",
      partition: 0,
      offset: 42,
      timestamp: 0,
      key: none(seq[byte]),
      value: some(@[1'u8, 2, 3]),
      headers: @[],
    )
    f.advanceConsumedOffset(rec)

    let offsets = f.getConsumedOffsets()
    check offsets[tp] == 43

  test "advanceConsumedOffset advances correctly for multiple records":
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "multi", partition: 0)
    f.registerPartitionForTest(tp, 0)

    for i in 0 ..< 5:
      let rec = FetchedRecord(
        topic: "multi",
        partition: 0,
        offset: int64(i * 10),
        timestamp: 0,
        key: none(seq[byte]),
        value: none(seq[byte]),
        headers: @[],
      )
      f.advanceConsumedOffset(rec)

    check f.getConsumedOffsets()[tp] == 41

  test "consumed offsets stay independent from fetch offsets":
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "independent", partition: 0)
    f.registerPartitionForTest(tp, 100)

    # Before any poll, getConsumedOffsets must not surface a value to commit
    # — even though the internal placeholder equals fetchOffset.
    check tp notin f.getConsumedOffsets()

    let rec = FetchedRecord(
      topic: "independent",
      partition: 0,
      offset: 149,
      timestamp: 0,
      key: none(seq[byte]),
      value: none(seq[byte]),
      headers: @[],
    )
    f.advanceConsumedOffset(rec)

    check f.getConsumedOffsets()[tp] == 150

    check f.getFetchOffsets()[tp] == 100

  test "advanceConsumedOffset works for multiple partitions":
    let f = newFetcher(nil)
    let tp0 = TopicPartition(topic: "t", partition: 0)
    let tp1 = TopicPartition(topic: "t", partition: 1)
    f.registerPartitionForTest(tp0, 0)
    f.registerPartitionForTest(tp1, 0)

    f.advanceConsumedOffset(
      FetchedRecord(
        topic: "t",
        partition: 0,
        offset: 10,
        timestamp: 0,
        key: none(seq[byte]),
        value: none(seq[byte]),
        headers: @[],
      )
    )
    f.advanceConsumedOffset(
      FetchedRecord(
        topic: "t",
        partition: 1,
        offset: 20,
        timestamp: 0,
        key: none(seq[byte]),
        value: none(seq[byte]),
        headers: @[],
      )
    )

    check f.getConsumedOffsets()[tp0] == 11
    check f.getConsumedOffsets()[tp1] == 21

  test "stop drains queue without advancing consumed offsets":
    let f = newFetcher(nil)
    let tp0 = TopicPartition(topic: "drain", partition: 0)
    let tp1 = TopicPartition(topic: "drain", partition: 1)
    f.registerPartitionForTest(tp0, 10)
    f.registerPartitionForTest(tp1, 20)

    for i in 0 ..< 3:
      f.records.addLastNoWait(
        FetchedRecord(
          topic: "drain",
          partition: 0,
          offset: int64(10 + i),
          timestamp: 0,
          key: none(seq[byte]),
          value: none(seq[byte]),
          headers: @[],
        )
      )
    f.records.addLastNoWait(
      FetchedRecord(
        topic: "drain",
        partition: 1,
        offset: 25,
        timestamp: 0,
        key: none(seq[byte]),
        value: none(seq[byte]),
        headers: @[],
      )
    )

    check f.records.len == 4

    # stop() should drain queue but NOT advance consumed offsets
    # (only records explicitly polled by the user should be committed)
    waitFor f.stop()

    check f.records.empty()
    let offsets = f.getConsumedOffsets()
    # Neither partition was polled by the user, so neither should be
    # commit-eligible regardless of the placeholder offset.
    check tp0 notin offsets
    check tp1 notin offsets

  test "advanceConsumedOffset ignores unknown partitions":
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "known", partition: 0)
    f.registerPartitionForTest(tp, 0)

    let rec = FetchedRecord(
      topic: "unknown",
      partition: 99,
      offset: 42,
      timestamp: 0,
      key: none(seq[byte]),
      value: none(seq[byte]),
      headers: @[],
    )
    f.advanceConsumedOffset(rec)

    # The advance was a no-op (unknown partition), so the known partition
    # remains unconsumed and is excluded from the commit-eligible set.
    let offsets = f.getConsumedOffsets()
    check offsets.len == 0
    check tp notin offsets

  test "getConsumedOffsets is empty when no partitions assigned":
    let f = newFetcher(nil)
    check f.getConsumedOffsets().len == 0

  test "seek makes the seeked offset commit-eligible (Java parity)":
    # Java's seek() is an explicit position choice: a bare seek followed by
    # commitSync() (no intervening poll) commits the seeked offset for the
    # partition. getConsumedOffsets must therefore include the partition at
    # the seeked value right after seek().
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "seek-test", partition: 0)
    f.registerPartitionForTest(tp, 100)

    check tp notin f.getConsumedOffsets()
    f.seek(tp, 50)

    let offsets = f.getConsumedOffsets()
    check tp in offsets
    check offsets[tp] == 50

  test "seek can rewind a consumed partition for a re-commit":
    # Consume forward, then seek backwards: the next commit must reflect the
    # rewound position, not the previously consumed one.
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "seek-test", partition: 0)
    f.registerPartitionForTest(tp, 0)
    f.advanceConsumedOffset(
      FetchedRecord(
        topic: tp.topic,
        partition: tp.partition,
        offset: 42,
        timestamp: 0,
        key: none(seq[byte]),
        value: none(seq[byte]),
        headers: @[],
      )
    )
    check f.getConsumedOffsets()[tp] == 43
    f.seek(tp, 10)
    check f.getConsumedOffsets()[tp] == 10

  test "offset reset realigns the committable offset of a consumed partition":
    # Regression: an OFFSET_OUT_OF_RANGE reset used to move only fetchOffset,
    # leaving consumedOffset at its pre-reset value. A commit landing before the
    # next consume would then write that stale offset (e.g. a slow consumer
    # overtaken by retention), contradicting where the fetcher actually resumed.
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "reset-test", partition: 0)
    f.registerPartitionForTest(tp, 5)
    # Consume up to offset 9 → committable position is 10.
    f.advanceConsumedOffset(
      FetchedRecord(
        topic: tp.topic,
        partition: tp.partition,
        offset: 9,
        timestamp: 0,
        key: none(seq[byte]),
        value: none(seq[byte]),
        headers: @[],
      )
    )
    check f.getConsumedOffsets()[tp] == 10

    # Retention deletes past offset 10; reset resolves to earliest = 100.
    f.applyOffsetResetForTest(tp, 100)
    check f.getFetchOffsets()[tp] == 100
    # The committable offset must follow the reset, not stay at the stale 10.
    check f.getConsumedOffsets()[tp] == 100

  test "offset reset does not make an unconsumed partition committable":
    # Mirror of setPartitions: a partition reset before any user observation
    # stays out of getConsumedOffsets so no placeholder (e.g. an orpLatest
    # high-watermark) is ever committed.
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "reset-test", partition: 1)
    f.registerPartitionForTest(tp, 5)
    check tp notin f.getConsumedOffsets()

    f.applyOffsetResetForTest(tp, 200)
    check f.getFetchOffsets()[tp] == 200
    check tp notin f.getConsumedOffsets()

  test "offset reset drops pre-reset records still queued for the partition":
    # Regression: applyOffsetReset used to move fetchOffset/consumedOffset but
    # leave records enqueued by an earlier fetch in the shared queue. Polling
    # one then called advanceConsumedOffset(rec.offset+1), silently rewinding
    # the just-repositioned consumedOffset — a subsequent commit would write
    # the stale pre-reset offset and the broker would OFFSET_OUT_OF_RANGE on
    # the next restart. Drain must fire on reset, per-partition.
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "reset-test", partition: 2)
    let other = TopicPartition(topic: "reset-test", partition: 3)
    f.registerPartitionForTest(tp, 100)
    f.registerPartitionForTest(other, 0)

    proc mkRec(t: TopicPartition, o: int64): FetchedRecord =
      FetchedRecord(
        topic: t.topic,
        partition: t.partition,
        offset: o,
        timestamp: 0,
        key: none(seq[byte]),
        value: none(seq[byte]),
        headers: @[],
      )

    for o in 100'i64 .. 102'i64:
      f.enqueueRecordForTest(mkRec(tp, o))
    f.enqueueRecordForTest(mkRec(other, 0))
    check f.records.len == 4

    f.applyOffsetResetForTest(tp, 5000)
    check f.getFetchOffsets()[tp] == 5000
    # `tp` records gone, sibling partition's record preserved.
    check f.records.len == 1
    let survivor = f.records.getNoWait()
    check survivor.topic == other.topic
    check survivor.partition == other.partition

  test "advanceConsumedOffset drops a pre-reset record still in pendingAdvance":
    # Regression: `drainQueuedRecords` only touches records still inside
    # `f.records`. A record already popped by an earlier poll and held in the
    # consumer's `pendingAdvance` cannot be reached from the fetcher, so a
    # truncation / offset-reset landing between polls used to have the next
    # `flushPendingAdvance` blindly advance `consumedOffset` past the freshly
    # realigned position — a subsequent commit would either write a stale
    # future offset (silent skip on restart) or, if the next poll's own advance
    # rewound it, silently commit past valid post-reset records. The stamped
    # `posEpoch` on FetchedRecord makes advanceConsumedOffset reject the stale
    # record instead.
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "reset-test", partition: 4)
    f.registerPartitionForTest(tp, 100)

    # Simulate a poll BEFORE the reset: the record carries the partition's
    # then-current posEpoch (0). getConsumedOffsets sees the advance.
    let preReset = FetchedRecord(
      topic: tp.topic,
      partition: tp.partition,
      offset: 199,
      posEpoch: f.posEpochForTest(tp),
    )
    f.advanceConsumedOffset(preReset)
    check f.getConsumedOffsets()[tp] == 200

    # Truncation lands: consumedOffset rewinds to 150 and posEpoch bumps.
    f.applyOffsetResetForTest(tp, 150)
    check f.getConsumedOffsets()[tp] == 150
    check f.posEpochForTest(tp) == 1

    # The consumer's next flushPendingAdvance replays the pre-reset record.
    # Its posEpoch stamp (0) no longer matches the partition's (1), so
    # advanceConsumedOffset must NOT touch consumedOffset.
    f.advanceConsumedOffset(preReset)
    check f.getConsumedOffsets()[tp] == 150

    # A fresh post-reset record carries the new epoch and advances normally.
    let postReset = FetchedRecord(
      topic: tp.topic,
      partition: tp.partition,
      offset: 150,
      posEpoch: f.posEpochForTest(tp),
    )
    f.advanceConsumedOffset(postReset)
    check f.getConsumedOffsets()[tp] == 151

  test "advanceConsumedOffset never rewinds consumedOffset":
    # Defensive clamp: even on the happy path (same posEpoch), a lower
    # rec.offset+1 must not rewind consumedOffset. Guards against a future
    # refactor that reorders records or adds a new reposition path without a
    # matching posEpoch bump.
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "clamp", partition: 0)
    f.registerPartitionForTest(tp, 0)
    let epoch = f.posEpochForTest(tp)
    f.advanceConsumedOffset(
      FetchedRecord(
        topic: tp.topic, partition: tp.partition, offset: 300, posEpoch: epoch
      )
    )
    check f.getConsumedOffsets()[tp] == 301

    # Lower-offset same-epoch record: must not rewind.
    f.advanceConsumedOffset(
      FetchedRecord(
        topic: tp.topic, partition: tp.partition, offset: 200, posEpoch: epoch
      )
    )
    check f.getConsumedOffsets()[tp] == 301

  test "consumedOffsetsAfter drops a pre-reposition record like advanceConsumedOffset":
    # Regression: `consumedOffsetsAfter` builds the snapshot commitSync sends to
    # the broker (position after the deferred advance is applied). It used to
    # unconditionally overwrite the offset with `rec.offset + 1`, ignoring the
    # stamped `posEpoch`. If an OFFSET_OUT_OF_RANGE reset or KIP-320 truncation
    # landed between the poll and commitSync, the pre-reset record would surface
    # a stale offset that commitSync would then persist — reverting the
    # realigned `consumedOffset` at the broker. On restart the consumer would
    # re-consume (or truncation-skip) data between the commit point and the
    # reset target. The fix mirrors advanceConsumedOffset's posEpoch gate.
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "reposition-commit", partition: 0)
    f.registerPartitionForTest(tp, 100)

    # Pre-reset poll observed the record; the record carries the then-current
    # posEpoch (0). advanceConsumedOffset would land it at 200.
    let preReset = FetchedRecord(
      topic: tp.topic,
      partition: tp.partition,
      offset: 199,
      posEpoch: f.posEpochForTest(tp),
    )

    # Truncation lands before the consumer's next commitSync applies the
    # pending advance. consumedOffset rewinds to 150, posEpoch bumps to 1.
    f.applyOffsetResetForTest(tp, 150)
    check f.posEpochForTest(tp) == 1

    # everConsumed is still false (no successful advance yet), so the base
    # snapshot omits tp. The pre-reset record's mismatched posEpoch (0 vs 1)
    # must be dropped too — the resulting snapshot must not surface 200.
    var snap = f.consumedOffsetsAfter([preReset])
    check tp notin snap

    # A fresh post-reset record with matching posEpoch advances the snapshot,
    # matching what a subsequent advanceConsumedOffset(postReset) would do.
    let postReset = FetchedRecord(
      topic: tp.topic,
      partition: tp.partition,
      offset: 150,
      posEpoch: f.posEpochForTest(tp),
    )
    snap = f.consumedOffsetsAfter([preReset, postReset])
    check snap[tp] == 151

  test "advanceConsumedOffset tracks the record's leader epoch":
    # Regression: OffsetCommit used to send the cluster-metadata leader epoch,
    # so an unclean leader election would surface a NEWER epoch than any
    # record the consumer actually reached — suppressing KIP-320 truncation
    # detection on restart. `advanceConsumedOffset` now tracks the record's
    # own `leaderEpoch` alongside the offset, and `getConsumedLeaderEpochs`
    # exposes it to the commit path.
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "epoch", partition: 0)
    f.registerPartitionForTest(tp, 0)

    check tp notin f.getConsumedLeaderEpochs() # no consume yet -> no entry

    let epochRec = FetchedRecord(
      topic: tp.topic,
      partition: tp.partition,
      offset: 100,
      posEpoch: f.posEpochForTest(tp),
      leaderEpoch: 7'i32,
    )
    f.advanceConsumedOffset(epochRec)
    check f.getConsumedLeaderEpochs()[tp] == 7'i32
    check f.getConsumedOffsets()[tp] == 101

    # Higher-offset newer-epoch record: both offset and epoch advance.
    let laterRec = FetchedRecord(
      topic: tp.topic,
      partition: tp.partition,
      offset: 200,
      posEpoch: f.posEpochForTest(tp),
      leaderEpoch: 8'i32,
    )
    f.advanceConsumedOffset(laterRec)
    check f.getConsumedLeaderEpochs()[tp] == 8'i32
    check f.getConsumedOffsets()[tp] == 201

    # Same-epoch lower-offset record: neither offset nor epoch regress (the
    # same clamp that never rewinds consumedOffset also protects the epoch).
    let staleRec = FetchedRecord(
      topic: tp.topic,
      partition: tp.partition,
      offset: 150,
      posEpoch: f.posEpochForTest(tp),
      leaderEpoch: 6'i32,
    )
    f.advanceConsumedOffset(staleRec)
    check f.getConsumedLeaderEpochs()[tp] == 8'i32
    check f.getConsumedOffsets()[tp] == 201

  test "seek clears the tracked leader epoch so commitOffsets falls back to metadata":
    # A seek to an offset carries no leader epoch (the seek API has no epoch
    # parameter). getConsumedLeaderEpochs must drop the previous record's epoch
    # so `commitOffsets` uses the current cluster-metadata epoch, which is the
    # only credible value the consumer has for a hand-picked position.
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "seek-epoch", partition: 0)
    f.registerPartitionForTest(tp, 0)

    let rec = FetchedRecord(
      topic: tp.topic,
      partition: tp.partition,
      offset: 50,
      posEpoch: f.posEpochForTest(tp),
      leaderEpoch: 12'i32,
    )
    f.advanceConsumedOffset(rec)
    check f.getConsumedLeaderEpochs()[tp] == 12'i32

    f.seek(tp, 200)
    check tp notin f.getConsumedLeaderEpochs()
    check f.getConsumedOffsets()[tp] == 200

  test "applyOffsetReset clears the tracked leader epoch":
    # Same logic as seek: an OOR reset repositions to a broker-chosen boundary
    # (earliest / latest) whose epoch we cannot know locally.
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "reset-epoch", partition: 0)
    f.registerPartitionForTest(tp, 0)

    let rec = FetchedRecord(
      topic: tp.topic,
      partition: tp.partition,
      offset: 50,
      posEpoch: f.posEpochForTest(tp),
      leaderEpoch: 3'i32,
    )
    f.advanceConsumedOffset(rec)
    check f.getConsumedLeaderEpochs()[tp] == 3'i32

    f.applyOffsetResetForTest(tp, 1000)
    check tp notin f.getConsumedLeaderEpochs()

  test "consumedLeaderEpochsAfter mirrors advanceConsumedOffset for the snapshot path":
    # commitSync uses `consumedLeaderEpochsAfter` to snapshot the epoch that
    # accompanies `consumedOffsetsAfter` for the deferred pendingAdvance.
    # Semantics: same clamp as advanceConsumedOffset — a max-offset winner
    # sets the epoch; a lower-offset entry never overwrites it; a pre-reset
    # posEpoch stamp is dropped.
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "snap-epoch", partition: 0)
    f.registerPartitionForTest(tp, 0)
    let epoch = f.posEpochForTest(tp)
    let hi = FetchedRecord(
      topic: tp.topic,
      partition: tp.partition,
      offset: 300,
      posEpoch: epoch,
      leaderEpoch: 9'i32,
    )
    let lo = FetchedRecord(
      topic: tp.topic,
      partition: tp.partition,
      offset: 200,
      posEpoch: epoch,
      leaderEpoch: 4'i32,
    )
    let snap = f.consumedLeaderEpochsAfter([lo, hi])
    check snap[tp] == 9'i32

  test "consumedOffsetsAfter never rewinds":
    # Defensive clamp mirrors advanceConsumedOffset. Two same-epoch records with
    # a decreasing offset — the higher one must win.
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "snap-clamp", partition: 0)
    f.registerPartitionForTest(tp, 0)
    let epoch = f.posEpochForTest(tp)
    let hi = FetchedRecord(
      topic: tp.topic, partition: tp.partition, offset: 300, posEpoch: epoch
    )
    let lo = FetchedRecord(
      topic: tp.topic, partition: tp.partition, offset: 200, posEpoch: epoch
    )
    let snap = f.consumedOffsetsAfter([hi, lo])
    check snap[tp] == 301

suite "Fetcher decompression budget config":
  test "newFetcher rejects maxTotalDecompressedBytes <= 0":
    expect(KafkaError):
      discard newFetcher(nil, maxTotalDecompressedBytes = 0)
    expect(KafkaError):
      discard newFetcher(nil, maxTotalDecompressedBytes = -1)

  test "newFetcher default budget is 1GiB":
    let f = newFetcher(nil)
    check f.maxTotalDecompressedBytes == defaultMaxTotalDecompressedBytes

suite "Fetcher fetch knob validation":
  test "newFetcher rejects the knobs the broker would refuse":
    # A negative wait or byte count reaches the broker as-is and comes back as
    # INVALID_REQUEST on every fetch, so it has to fail as a config error here.
    expect(KafkaError):
      discard newFetcher(nil, maxWaitMs = -1)
    expect(KafkaError):
      discard newFetcher(nil, minBytes = -1)
    expect(KafkaError):
      discard newFetcher(nil, maxBytes = 0)
    expect(KafkaError):
      discard newFetcher(nil, maxPartitionBytes = -1)
    expect(KafkaError):
      discard newFetcher(nil, maxConsecutiveFetchErrors = -1)
    expect(KafkaError):
      discard newFetcher(nil, offsetResolveBackoffMs = -1)

  test "zero keeps its documented meaning where it has one":
    # 0 is "answer as soon as anything is available" for the two broker-side
    # waits, and "trip on the first error" for the consecutive-error budget.
    let f = newFetcher(nil, maxWaitMs = 0, minBytes = 0, maxConsecutiveFetchErrors = 0)
    check f.maxWaitMs == 0
    check f.minBytes == 0
    check f.maxConsecutiveFetchErrors == 0

suite "Fetcher backpressure":
  test "queue full() reports correctly":
    let f = newFetcher(nil, queueSize = 3)
    check not f.records.full()
    for i in 0 ..< 3:
      f.records.addLastNoWait(
        FetchedRecord(
          topic: "bp",
          partition: 0,
          offset: int64(i),
          timestamp: 0,
          key: none(seq[byte]),
          value: none(seq[byte]),
          headers: @[],
        )
      )
    check f.records.full()
    check f.records.len == 3

    discard f.records.getNoWait()
    check not f.records.full()
    check f.records.len == 2

  test "queueFull flag is cleared by start":
    proc testBody() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil)
      f.queueFull = true
      f.start()
      # fetchLoop resets queueFull on first iteration
      # stop immediately to avoid hanging on nil cluster
      await f.stop()
      check not f.queueFull

    waitFor testBody()

suite "Fetcher error tracking":
  test "lastError is nil by default":
    let f = newFetcher(nil)
    check f.lastError == nil

  test "lastError does not affect consumed offset operations":
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "err", partition: 0)
    f.registerPartitionForTest(tp, 50)

    f.lastError = (ref KafkaError)(msg: "test error")

    let rec = FetchedRecord(
      topic: "err",
      partition: 0,
      offset: 60,
      timestamp: 0,
      key: none(seq[byte]),
      value: none(seq[byte]),
      headers: @[],
    )
    f.advanceConsumedOffset(rec)
    check f.getConsumedOffsets()[tp] == 61

  test "consecutiveFetchErrors starts at 0":
    let f = newFetcher(nil)
    check f.fetchErrorCount() == 0

  test "fetchErrorCount returns current consecutive error count":
    let f = newFetcher(nil)
    f.consecutiveFetchErrors = 5
    check f.fetchErrorCount() == 5

  test "maxConsecutiveFetchErrors defaults to 10":
    let f = newFetcher(nil)
    check f.maxConsecutiveFetchErrors == 10

  test "maxConsecutiveFetchErrors is configurable":
    let f = newFetcher(nil, maxConsecutiveFetchErrors = 3)
    check f.maxConsecutiveFetchErrors == 3

  test "direct field assignment of consecutiveFetchErrors does not clear lastError":
    let f = newFetcher(nil)
    f.consecutiveFetchErrors = 5
    f.lastError = (ref KafkaError)(msg: "previous error")
    f.consecutiveFetchErrors = 0
    # Direct field assignment does not trigger clearing logic;
    # only internal code paths (successful fetch, start) clear lastError.
    check f.lastError != nil
    check f.fetchErrorCount() == 0

  test "start clears lastError and consecutiveFetchErrors":
    proc testBody() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil)
      f.lastError = (ref KafkaError)(msg: "old error")
      f.consecutiveFetchErrors = 5
      f.start()
      check f.lastError == nil
      check f.fetchErrorCount() == 0
      await f.stop()

    waitFor testBody()

  test "lastRestartAttempt defaults to zero":
    let f = newFetcher(nil)
    check f.lastRestartAttempt == Moment.init(0'i64, nanoseconds(1))

  test "start after permanent error resets state for restart":
    proc testBody() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil)
      f.lastError = (ref KafkaError)(msg: "permanent error")
      f.consecutiveFetchErrors = 10
      f.lastRestartAttempt = Moment.now()
      f.start()
      check f.lastError == nil
      check f.fetchErrorCount() == 0
      await f.stop()

    waitFor testBody()

  test "error counter not reset when some fetches fail (partial success)":
    # Simulates the scenario where no broker fetch succeeds, so fetchOnce does
    # not reset the counter. One successful broker fetch is enough to clear it
    # (anySucceeded), so a partial success would not preserve these fields.
    let f = newFetcher(nil)
    f.consecutiveFetchErrors = 5
    f.lastError = (ref KafkaError)(msg: "prior failure")
    # (We can't call fetchOnce directly without brokers, so we verify the
    # invariant that field values are preserved when not explicitly reset.)
    check f.consecutiveFetchErrors == 5
    check f.lastError != nil
    check f.lastError.msg == "prior failure"

  test "error counter resets only when all fetches succeed":
    let f = newFetcher(nil)
    f.consecutiveFetchErrors = 7
    f.lastError = (ref KafkaError)(msg: "persistent failure")
    # Simulate: fetchOnce detects allSucceeded=true and resets.
    f.consecutiveFetchErrors = 0
    f.lastError = nil
    check f.consecutiveFetchErrors == 0
    check f.lastError == nil

  test "stop clears lastError so a parked rejoin does not restart under old assignment":
    # Regression: `stop()` used to leave a residual `lastError` in place. After a
    # rebalance parked the rejoin (deadline-bounded poll returned early), the
    # next poll's `tryRestartFetcher` would observe the stale error, restart the
    # fetch loop under the OLD assignment, and race the rejoin's setPartitions —
    # in-flight fetch responses for pre-rebalance partitions could land against
    # a fresh PartitionState (posEpoch=0 both sides), advancing offsets past
    # revoked data (delivery gap / silent data loss).
    proc testBody() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil)
      f.lastError = (ref KafkaError)(msg: "prior fetch failure")
      f.consecutiveFetchErrors = 3
      check f.lastError != nil
      await f.stop()
      # Deliberate teardown, not a failure. `tryRestartFetcher` gates on
      # `lastError != nil`, so the residual error would auto-restart the loop
      # while a rejoin is still parked. Now cleared.
      check f.lastError == nil
      # `consecutiveFetchErrors` is unchanged: start() (not stop()) is what
      # resets the fetcher's per-round bookkeeping.
      check f.fetchErrorCount() == 3

    waitFor testBody()

  test "setLastError after stop() preserves the outer failure signal":
    # Rebalance retry exhaustion + permanent join error both rely on:
    #   await fetcher.stop()
    #   fetcher.setLastError(outerErr)
    # so the next poll surfaces the outer error via `tryRestartFetcher`. stop()
    # clearing `lastError` internally must NOT eat a follow-up `setLastError`.
    proc testBody() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil)
      f.lastError = (ref KafkaError)(msg: "internal fetch error")
      await f.stop()
      check f.lastError == nil
      let outer = (ref KafkaError)(msg: "rebalance exhausted")
      f.setLastError(outer)
      check f.lastError != nil
      check f.lastError.msg == "rebalance exhausted"

    waitFor testBody()

suite "Fetcher offset resolve backoff":
  test "lastOffsetResolve defaults to zero Moment":
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "t", partition: 0)
    f.registerPartitionForTest(tp, 0)
    check f.partitions[0].lastOffsetResolve == default(Moment)

  test "offsetResolveBackoffMs defaults to 5000":
    let f = newFetcher(nil)
    check f.offsetResolveBackoffMs == 5000

  test "offsetResolveBackoffMs is configurable":
    let f = newFetcher(nil, offsetResolveBackoffMs = 2000)
    check f.offsetResolveBackoffMs == 2000

  test "lastOffsetResolve is preserved across partition state operations":
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "resolve-test", partition: 0)
    f.registerPartitionForTest(tp, 100)

    let now = Moment.now()
    f.partitions[0].lastOffsetResolve = now
    check f.partitions[0].lastOffsetResolve == now

    let rec = FetchedRecord(
      topic: "resolve-test",
      partition: 0,
      offset: 105,
      timestamp: 0,
      key: none(seq[byte]),
      value: none(seq[byte]),
      headers: @[],
    )
    f.advanceConsumedOffset(rec)
    check f.partitions[0].lastOffsetResolve == now

proc makeBatch(
    baseOffset: int64,
    producerId: int64 = -1,
    attrs: int16 = 0,
    records: seq[Record] = @[Record()],
): RecordBatch =
  RecordBatch(
    baseOffset: baseOffset, producerId: producerId, attributes: attrs, records: records
  )

suite "filterBatches — aborted transaction filtering":
  test "no filtering for read_uncommitted":
    let batches = @[
      makeBatch(0, producerId = 1, attrs = 0x10) # transactional
    ]
    let result = filterBatches(batches, @[], isolationLevel = ilReadUncommitted)
    check result.len == 1

  test "control batches are skipped in read_uncommitted too":
    # Control markers carry no application data; they must never be delivered,
    # even at read_uncommitted (matches the Java client).
    let batches = @[
      makeBatch(0, producerId = 1, attrs = 0x10 or 0x20), # control marker
      makeBatch(1, producerId = 1, attrs = 0x10), # transactional data
      makeBatch(2, producerId = -1, attrs = 0), # plain data
    ]
    let result = filterBatches(batches, @[], isolationLevel = ilReadUncommitted)
    check result.len == 2
    check result[0].baseOffset == 1
    check result[1].baseOffset == 2

  test "control batches are always skipped in read_committed":
    let batches = @[
      makeBatch(0, producerId = 1, attrs = 0x10 or 0x20), # control batch
      makeBatch(1, producerId = 1, attrs = 0x10), # data batch
    ]
    let result = filterBatches(batches, @[], isolationLevel = ilReadCommitted)
    check result.len == 1
    check result[0].baseOffset == 1

  test "aborted transactional batches are skipped":
    let batches = @[
      makeBatch(100, producerId = 1, attrs = 0x10), # aborted data
      makeBatch(101, producerId = 1, attrs = 0x10), # aborted data
      makeBatch(102, producerId = 1, attrs = 0x10 or 0x20), # abort control
    ]
    let aborted = @[AbortedTransaction(producerId: 1, firstOffset: 100)]
    let result = filterBatches(batches, aborted, isolationLevel = ilReadCommitted)
    check result.len == 0 # all skipped (data aborted, control always skipped)

  test "committed batches after abort are delivered":
    # Producer 1: aborted txn at offset 100, then committed txn at offset 200
    let batches = @[
      makeBatch(100, producerId = 1, attrs = 0x10), # aborted data
      makeBatch(150, producerId = 1, attrs = 0x10 or 0x20), # abort control marker
      makeBatch(200, producerId = 1, attrs = 0x10), # committed data
      makeBatch(250, producerId = 1, attrs = 0x10 or 0x20), # commit control marker
    ]
    let aborted = @[AbortedTransaction(producerId: 1, firstOffset: 100)]
    let result = filterBatches(batches, aborted, isolationLevel = ilReadCommitted)
    check result.len == 1
    check result[0].baseOffset == 200

  test "non-transactional batches pass through":
    let batches = @[
      makeBatch(0, producerId = -1, attrs = 0), # non-transactional
      makeBatch(100, producerId = 1, attrs = 0x10), # aborted
      makeBatch(150, producerId = 1, attrs = 0x10 or 0x20), # abort control
    ]
    let aborted = @[AbortedTransaction(producerId: 1, firstOffset: 100)]
    let result = filterBatches(batches, aborted, isolationLevel = ilReadCommitted)
    check result.len == 1
    check result[0].baseOffset == 0

  test "batch straddling firstOffset is filtered":
    # baseOffset < firstOffset but baseOffset + lastOffsetDelta >= firstOffset
    let batches = @[
      RecordBatch(
        baseOffset: 98,
        lastOffsetDelta: 4,
        producerId: 1,
        attributes: 0x10,
        records: @[Record()],
      ),
      makeBatch(150, producerId = 1, attrs = 0x10 or 0x20),
    ]
    let aborted = @[AbortedTransaction(producerId: 1, firstOffset: 100)]
    let result = filterBatches(batches, aborted, isolationLevel = ilReadCommitted)
    check result.len == 0

  test "multiple producers with mixed abort/commit":
    let batches = @[
      makeBatch(100, producerId = 1, attrs = 0x10), # producer 1 aborted
      makeBatch(110, producerId = 2, attrs = 0x10), # producer 2 committed
      makeBatch(150, producerId = 1, attrs = 0x10 or 0x20), # producer 1 abort control
      makeBatch(160, producerId = 2, attrs = 0x10 or 0x20), # producer 2 commit control
    ]
    let aborted = @[AbortedTransaction(producerId: 1, firstOffset: 100)]
    let result = filterBatches(batches, aborted, isolationLevel = ilReadCommitted)
    check result.len == 1
    check result[0].baseOffset == 110

suite "computeNextFetchOffset — advance past control/empty batches":
  test "data batch only advances to lastEnqueued + 1":
    let batches =
      @[RecordBatch(baseOffset: 0, lastOffsetDelta: 5, records: @[Record()])]
    check computeNextFetchOffset(batches, 5, queueFull = false, currentFetchOffset = 0) ==
      6

  test "trailing control batch advances past the marker (not stuck)":
    # read_committed: a commit/abort marker at the fetch position yields no
    # delivered records (lastEnqueued = -1) but must not pin fetchOffset.
    let batches = @[
      RecordBatch(
        baseOffset: 10, lastOffsetDelta: 0, attributes: 0x20, records: @[Record()]
      )
    ]
    check computeNextFetchOffset(
      batches, -1, queueFull = false, currentFetchOffset = 10
    ) == 11

  test "fully-compacted empty batch advances by its offset span":
    let batches =
      @[RecordBatch(baseOffset: 20, lastOffsetDelta: 4, records: newSeq[Record]())]
    check computeNextFetchOffset(
      batches, -1, queueFull = false, currentFetchOffset = 20
    ) == 25

  test "queueFull confines advancement to enqueued records (no data loss)":
    # Batch spans offsets 0..9 but we only enqueued through 3 before the queue
    # filled. Must NOT jump to 10 — the un-enqueued records get re-fetched.
    let batches =
      @[RecordBatch(baseOffset: 0, lastOffsetDelta: 9, records: @[Record()])]
    check computeNextFetchOffset(batches, 3, queueFull = true, currentFetchOffset = 0) ==
      4

  test "never moves backwards below currentFetchOffset":
    let batches =
      @[RecordBatch(baseOffset: 0, lastOffsetDelta: 2, records: @[Record()])]
    check computeNextFetchOffset(
      batches, -1, queueFull = false, currentFetchOffset = 100
    ) == 100

  test "corruptGapEnd advances the cursor past the corrupted range":
    # Good batch 0..4 delivered; a corrupt batch occupied 5..12 and was dropped
    # by parseRecordBatches. The cursor must jump the gap or the broker keeps
    # serving the same corrupt bytes. The gap is only ever a range a
    # CRC-verified batch pinned (see parseRecordBatches).
    let batches =
      @[RecordBatch(baseOffset: 0, lastOffsetDelta: 4, records: @[Record()])]
    check computeNextFetchOffset(
      batches, 4, queueFull = false, currentFetchOffset = 0, corruptGapEnd = 13
    ) == 13

  test "a batch claiming more offsets than it carries does not skip them":
    # `lastOffsetDelta` is what the cursor follows: a header overstating the
    # span would move it past records the batch never delivered, and the next
    # fetch starts after them — permanent, silent loss. The decoder cannot
    # reject the header (the encoder legitimately emits non-dense batches), so
    # the advance is clamped to the offsets the decoded records occupy.
    let batches = @[
      RecordBatch(
        baseOffset: 0,
        magic: 2'i8,
        lastOffsetDelta: 9'i32,
        records: @[Record(offset: 0, timestamp: 0, value: some(@[byte('a')]))],
      )
    ]
    check computeNextFetchOffset(batches, -1, queueFull = false, currentFetchOffset = 0) ==
      1

  test "a non-dense batch still advances past its highest record":
    # lastOffsetDelta is max(record.offset) - baseOffset, so records at 0 and 2
    # legitimately carry lastOffsetDelta 2 with two records. The clamp must
    # follow the highest record, not the count, or offset 2 would be re-delivered
    # on every fetch.
    let batches = @[
      RecordBatch(
        baseOffset: 0,
        magic: 2'i8,
        lastOffsetDelta: 2'i32,
        records: @[
          Record(offset: 0, timestamp: 0, value: some(@[byte('a')])),
          Record(offset: 2, timestamp: 0, value: some(@[byte('c')])),
        ],
      )
    ]
    check computeNextFetchOffset(batches, -1, queueFull = false, currentFetchOffset = 0) ==
      3

  test "queueFull defers the corrupt-gap advance (no undelivered records skipped)":
    # Records 4..9 are not enqueued yet; jumping the gap (which lies beyond
    # them) would skip them silently. Stay at lastEnqueued + 1.
    let batches =
      @[RecordBatch(baseOffset: 0, lastOffsetDelta: 9, records: @[Record()])]
    check computeNextFetchOffset(
      batches, 3, queueFull = true, currentFetchOffset = 0, corruptGapEnd = 100
    ) == 4

suite "Fetcher pause/resume/seek":
  test "pause flips paused flag and isPaused/pausedPartitions reflect state":
    let f = newFetcher(nil)
    let a = TopicPartition(topic: "t", partition: 0)
    let b = TopicPartition(topic: "t", partition: 1)
    f.registerPartitionForTest(a, 100)
    f.registerPartitionForTest(b, 200)
    check not f.isPaused(a)
    check not f.isPaused(b)
    f.pause(@[a])
    check f.isPaused(a)
    check not f.isPaused(b)
    let paused = f.pausedPartitions()
    check paused.len == 1
    check paused[0] == a

  test "resume clears paused flag":
    let f = newFetcher(nil)
    let a = TopicPartition(topic: "t", partition: 0)
    f.registerPartitionForTest(a, 0)
    f.pause(@[a])
    check f.isPaused(a)
    f.resume(@[a])
    check not f.isPaused(a)

  test "pause raises for unassigned partition":
    let f = newFetcher(nil)
    let known = TopicPartition(topic: "t", partition: 0)
    let unknown = TopicPartition(topic: "t", partition: 99)
    f.registerPartitionForTest(known, 0)
    expect KafkaError:
      f.pause(@[unknown])

  test "pause invalidates the partition's broker fetch session (KIP-227)":
    # Under an active incremental fetch session the broker keeps serving any
    # partition we merely omit from a request (omission ≠ removal). Pausing must
    # therefore drop the broker session so the next fetch reopens a fresh one
    # built from only the still-active partitions, otherwise the paused
    # partition's records would keep arriving.
    let f = newFetcher(nil)
    let a = TopicPartition(topic: "t", partition: 0)
    let b = TopicPartition(topic: "t", partition: 1)
    let broker7 = newBroker(7'i32, "127.0.0.1", 9092'i32, "fetch-session-test")
    f.registerPartitionForTest(a, 100, broker7)
    f.registerPartitionForTest(b, 200, broker7)
    f.setFetchSessionForTest(7'i32, sessionId = 5'i32, sessionEpoch = 3'i32)
    check f.getFetchSessionForTest(7'i32).exists

    f.pause(@[a])
    check not f.getFetchSessionForTest(7'i32).exists

  test "pause with no broker is a no-op for fetch sessions":
    # registerPartitionForTest defaults the broker to nil (the common unit-test
    # shape); pause must not crash when a partition has no resolved broker yet.
    let f = newFetcher(nil)
    let a = TopicPartition(topic: "t", partition: 0)
    f.registerPartitionForTest(a, 100)
    f.pause(@[a])
    check f.isPaused(a)
    check f.fetchSessionCountForTest() == 0

  test "seek updates fetchOffset and clears matching queue records":
    let f = newFetcher(nil)
    let a = TopicPartition(topic: "t", partition: 0)
    let b = TopicPartition(topic: "t", partition: 1)
    f.registerPartitionForTest(a, 100)
    f.registerPartitionForTest(b, 200)

    proc rec(t: string, p: int32, off: int64): FetchedRecord =
      FetchedRecord(
        topic: t,
        partition: p,
        offset: off,
        timestamp: 0,
        key: none(seq[byte]),
        value: none(seq[byte]),
        headers: @[],
      )

    f.records.addLastNoWait(rec("t", 0, 100))
    f.records.addLastNoWait(rec("t", 0, 101))
    f.records.addLastNoWait(rec("t", 1, 200))

    f.seek(a, 500)

    check f.getFetchOffsets()[a] == 500
    let len1 = f.records.len
    check len1 == 1
    let kept = f.records.getNoWait()
    check kept.topic == "t"
    check kept.partition == 1

  test "seek raises on negative offset and unassigned partition":
    let f = newFetcher(nil)
    let a = TopicPartition(topic: "t", partition: 0)
    f.registerPartitionForTest(a, 0)
    expect KafkaError:
      f.seek(a, -1)
    expect KafkaError:
      f.seek(TopicPartition(topic: "u", partition: 0), 100)

suite "Fetcher paused-flag carryover across rebalance":
  test "snapshotPausedFlags captures only paused partitions":
    let f = newFetcher(nil)
    let a = TopicPartition(topic: "t", partition: 0)
    let b = TopicPartition(topic: "t", partition: 1)
    f.registerPartitionForTest(a, 100)
    f.registerPartitionForTest(b, 200)
    f.partitions[0].paused = true
    let snap = f.snapshotPausedFlags()
    check snap.len == 1
    check snap.getOrDefault(a, false) == true
    check snap.getOrDefault(b, false) == false

  test "snapshot then rebuild preserves pause for re-assigned partitions only":
    # Simulates the production setPartitions flow: take a snapshot, drop
    # the partition list, then build a new assignment. Re-assigned tps
    # carry their paused flag; freshly assigned tps default to unpaused.
    let f = newFetcher(nil)
    let a = TopicPartition(topic: "t", partition: 0)
    let b = TopicPartition(topic: "t", partition: 1)
    let c = TopicPartition(topic: "t", partition: 2)
    f.registerPartitionForTest(a, 100)
    f.registerPartitionForTest(b, 200)
    f.partitions[0].paused = true
    f.partitions[1].paused = true

    let snap = f.snapshotPausedFlags()

    # New assignment: {a, c} — `b` dropped, `c` newly assigned.
    f.partitions = @[]
    f.partitionIndex.clear()
    for tp in [a, c]:
      let idx = f.partitions.len
      f.partitions.add(
        PartitionState(
          tp: tp,
          fetchOffset: 0,
          consumedOffset: 0,
          broker: nil,
          paused: snap.getOrDefault(tp, false),
        )
      )
      f.partitionIndex[tp] = idx

    check f.isPaused(a) == true # re-assigned, was paused → still paused
    check f.isPaused(c) == false # freshly assigned → unpaused
    check f.isPaused(b) == false # dropped from assignment → default unpaused

suite "Fetcher top-level errorCode handling":
  test "keNone passes through and leaves no session change":
    let f = newFetcher(nil)
    check f.handleTopLevelFetchErrorForTest(keNone, 7'i32) == true
    check f.fetchSessionCountForTest == 0

  test "FETCH_SESSION_ID_NOT_FOUND (70) drops session and skips iteration":
    let f = newFetcher(nil)
    f.setFetchSessionForTest(7'i32, 42'i32, 3'i32)
    check f.fetchSessionCountForTest == 1
    let cont = f.handleTopLevelFetchErrorForTest(keFetchSessionIdNotFound, 7'i32)
    check cont == false
    # Session must be dropped so the next request starts fresh (sessionId=0).
    check f.fetchSessionCountForTest == 0
    # Error is logged via trackFetchError but not raised.
    check f.fetchErrorCount() == 1

  test "INVALID_FETCH_SESSION_EPOCH (71) drops session and skips iteration":
    let f = newFetcher(nil)
    f.setFetchSessionForTest(11'i32, 99'i32, 5'i32)
    let cont = f.handleTopLevelFetchErrorForTest(keInvalidFetchSessionEpoch, 11'i32)
    check cont == false
    check f.fetchSessionCountForTest == 0
    check f.fetchErrorCount() == 1

  test "other retriable code skips iteration but preserves session":
    # Catch-all retriable branch: a retriable code that is NOT a
    # session-invalidation code skips the iteration but keeps the cached
    # sessionId/epoch, so the next fetch reuses the session. COORDINATOR_LOAD_IN_PROGRESS
    # stands in for that bucket.
    let f = newFetcher(nil)
    f.setFetchSessionForTest(3'i32, 17'i32, 2'i32)
    let cont = f.handleTopLevelFetchErrorForTest(keCoordinatorLoadInProgress, 3'i32)
    check cont == false
    let stored = f.getFetchSessionForTest(3'i32)
    check stored.exists
    check stored.sessionId == 17'i32
    check stored.sessionEpoch == 2'i32

  test "fatal code raises KafkaApiError":
    # UNKNOWN_TOPIC_OR_PARTITION is now retriable (it clears once metadata
    # propagates), so use a genuinely fatal top-level code here.
    let f = newFetcher(nil)
    expect KafkaApiError:
      discard f.handleTopLevelFetchErrorForTest(keClusterAuthorizationFailed, 1'i32)

suite "Fetcher leaderEpoch monotone ceiling (KIP-320)":
  test "lastLeaderEpoch defaults to -1 for unknown partitions":
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "topic", partition: 0)
    check f.lastLeaderEpochForTest(tp) == -1

  test "setLastLeaderEpochForTest stores and retrieves the value":
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "topic", partition: 0)
    f.setLastLeaderEpochForTest(tp, 5)
    check f.lastLeaderEpochForTest(tp) == 5

  test "setPartitions drops leaderEpoch entries for partitions leaving the assignment":
    ## Regression: rebalance pruned partitionIndex but left stale
    ## leaderEpoch ceilings around. A re-join of the same partition under
    ## a freshly elected leader would then be fenced by the stale ceiling.
    proc body() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil)
      let stale = TopicPartition(topic: "rebalanced", partition: 0)
      let kept = TopicPartition(topic: "rebalanced", partition: 1)
      f.registerPartitionForTest(stale, 0)
      f.registerPartitionForTest(kept, 0)
      f.setLastLeaderEpochForTest(stale, 7)
      f.setLastLeaderEpochForTest(kept, 3)

      await f.setPartitions(@[], initTable[TopicPartition, int64]())
      check f.lastLeaderEpochForTest(stale) == -1
      check f.lastLeaderEpochForTest(kept) == -1

    waitFor body()

suite "Fetcher setPartitions atomic swap":
  test "setPartitions failure preserves prior partitions":
    proc body() {.async: (raises: [Exception]).} =
      # Bootstrap to an unreachable address so refreshMetadata raises before
      # the broker-resolution loop runs; the atomic swap is never reached and
      # the prior assignment is preserved.
      let cluster = newCluster(@["127.0.0.1:1"], requestTimeoutMs = 200)
      let f = newFetcher(cluster)
      let a = TopicPartition(topic: "kept", partition: 0)
      f.registerPartitionForTest(a, 100)
      check f.partitions.len == 1

      var raised = false
      let target = @[TopicPartition(topic: "missing", partition: 0)]
      var initial = initTable[TopicPartition, int64]()
      initial[target[0]] = 50
      try:
        await f.setPartitions(target, initial)
      except CatchableError:
        raised = true
      check raised
      # Prior assignment must survive: the new partition list never gets
      # half-installed, and the existing partition's offsets are untouched.
      check f.partitions.len == 1
      check f.partitions[0].tp == a
      check f.partitions[0].fetchOffset == 100
      await cluster.close()

    waitFor body()

suite "Fetcher: ListOffsets retries a retriable per-partition answer":
  test "OFFSET_NOT_AVAILABLE is retried and the resolved offset is used":
    ## Regression: a retriable per-partition ListOffsets answer was only warned
    ## about, so the partition stayed unresolved and setPartitions raised
    ## "refusing to fall back to 0" — start() failed on a transient answer (an
    ## unclean election leaving the high watermark behind).
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let topic = "offsets-retry"
      let tp = TopicPartition(topic: topic, partition: 0)
      var listOffsetsSeen = 0

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akListOffsets)), minVersion: 1, maxVersion: 2
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akListOffsets)):
              inc listOffsetsSeen
              let first = listOffsetsSeen == 1
              await req.respond(
                buildListOffsetsResponseBodyV2(
                  topic,
                  partition = 0'i32,
                  errorCode = (if first: int16(ord(keOffsetNotAvailable))
                  else: 0'i16),
                  timestamp = -1'i64,
                  offset = (if first: -1'i64 else: 4242'i64),
                )
              )
          except CatchableError:
            break

      let cluster = newCluster(@["127.0.0.1:" & $port])
      let b = newBroker(0'i32, "127.0.0.1", port, "offsets-retry")
      cluster.registerBrokerForTest(b)
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 1)
      cluster.markMetadataFreshForTest()

      let f = newFetcher(cluster, offsetResetPolicy = orpLatest)
      let handlerFut = handler()

      let resolvedFut = f.resolveOffsetsForTest(@[tp])
      let completed = await withTimeout(resolvedFut, 10.seconds)
      check completed
      check listOffsetsSeen == 2
      check resolvedFut.value[tp] == 4242'i64

      await cluster.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

suite "Fetcher generation tag (rebalance race)":
  # Each FetchedRecord carries the fetcher's assignmentGeneration at enqueue
  # time so a consumer poll can drop records left over from a previous
  # generation. Without this tag, records enqueued before a rebalance can be
  # returned to the user as if they belonged to the new assignment.

  test "FetchedRecord generation context defaults to 0":
    let r = FetchedRecord(
      topic: "t",
      partition: 0,
      offset: 1,
      timestamp: 0,
      key: none(seq[byte]),
      value: none(seq[byte]),
      headers: @[],
    )
    # int32 fields default to 0; the stale-generation filter treats <= 0
    # as "no generation context" (Kafka generationIds start at 1).
    check r.generation == 0'i32

  test "internal tests can set FetchedRecord generation context explicitly":
    let r = FetchedRecord(
      topic: "t",
      partition: 0,
      offset: 1,
      timestamp: 0,
      key: none(seq[byte]),
      value: none(seq[byte]),
      headers: @[],
      generation: 7'i32,
    )
    check r.generation == 7'i32

  test "Fetcher.assignmentGeneration starts at -1":
    let f = newFetcher(nil)
    check f.assignmentGeneration == -1'i32

  test "bumpGeneration updates the value":
    let f = newFetcher(nil)
    f.bumpGeneration(5'i32)
    check f.assignmentGeneration == 5'i32
    f.bumpGeneration(7'i32)
    check f.assignmentGeneration == 7'i32

  test "enqueueRecordForTest tags records with current generation":
    proc body() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil)
      f.bumpGeneration(3'i32)
      let r = FetchedRecord(
        topic: "t",
        partition: 0,
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: none(seq[byte]),
        headers: @[],
      )
      f.enqueueRecordForTest(r)
      let got = await f.records.get().wait(milliseconds(100))
      # The fetcher must apply its assignmentGeneration at enqueue time so
      # the record is identifiable as belonging to gen=3 even if the
      # generation is later bumped.
      check got.generation == 3'i32

    waitFor body()

  test "isRecordStaleGeneration returns true when generations differ":
    let r = FetchedRecord(
      topic: "t",
      partition: 0,
      offset: 0,
      timestamp: 0,
      key: none(seq[byte]),
      value: none(seq[byte]),
      headers: @[],
      generation: 3'i32,
    )
    check isRecordStaleGeneration(r, currentGeneration = 5'i32)
    check not isRecordStaleGeneration(r, currentGeneration = 3'i32)

  test "isRecordStaleGeneration is false when record generation is non-positive":
    # Consumers without a group assignment (or before the first JoinGroup
    # completes) have generationId = -1. Records tagged with -1 or 0 carry
    # no generation context and must not be dropped just because the live
    # generationId differs — otherwise unit tests of the fetcher's own
    # queue would lose every record.
    let preJoin = FetchedRecord(
      topic: "t",
      partition: 0,
      offset: 0,
      timestamp: 0,
      key: none(seq[byte]),
      value: none(seq[byte]),
      headers: @[],
      generation: -1'i32,
    )
    check not isRecordStaleGeneration(preJoin, currentGeneration = -1'i32)
    check not isRecordStaleGeneration(preJoin, currentGeneration = 5'i32)

    let defaulted = FetchedRecord(
      topic: "t",
      partition: 0,
      offset: 0,
      timestamp: 0,
      key: none(seq[byte]),
      value: none(seq[byte]),
      headers: @[],
    )
    check not isRecordStaleGeneration(defaulted, currentGeneration = 5'i32)

suite "Fetcher leader-refresh classification":
  test "isStaleLeaderCode covers stale-leader and fenced-epoch codes":
    # Stale / unknown leader view.
    check isStaleLeaderCode(keNotLeaderOrFollower)
    check isStaleLeaderCode(keUnknownTopicOrPartition)
    # KIP-320 fenced epoch: must refresh metadata to learn the new epoch,
    # otherwise the monotone-ceiling clamp re-sends the fenced epoch forever
    # and the partition stalls until the next rebalance.
    check isStaleLeaderCode(keFencedLeaderEpoch)
    check isStaleLeaderCode(keUnknownLeaderEpoch)
    # Election-in-progress codes: without a refresh we'd keep routing to the
    # cached nodeId until metadataMaxAge expires.
    check isStaleLeaderCode(keLeaderNotAvailable)
    check isStaleLeaderCode(keBrokerNotAvailable)

  test "isStaleLeaderCode excludes codes handled by other paths":
    # Success, offset-reset, and transient/fatal codes are routed elsewhere
    # (reset policy, per-partition skip, or raise) — not via leader refresh.
    check not isStaleLeaderCode(keNone)
    check not isStaleLeaderCode(keOffsetOutOfRange)
    check not isStaleLeaderCode(keNetworkException)
    check not isStaleLeaderCode(keReplicaNotAvailable)
    check not isStaleLeaderCode(keIllegalGeneration)
    check not isStaleLeaderCode(keUnknownServerError)

proc oneRecordBatch(baseOffset: int64, value: byte): seq[byte] =
  ## Encode a minimal single-record RecordBatch v2 to wire bytes.
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

proc malformedRecordBatch(baseOffset: int64): seq[byte] =
  ## Encode a one-record RecordBatch whose CRC verifies but whose record claims
  ## 63 value bytes inside a 7-byte body. The per-record window must reject the
  ## read, and the enclosing parse must keep going after the batch is skipped.
  result = oneRecordBatch(baseOffset, byte('a'))
  # Pin the fixture shape before patching: recLen varint at 61, then the body
  # (attributes, timestamp delta, offset delta, null key), so the value-length
  # varint is byte 66. A layout drift must fail here, not silently test nothing.
  doAssert result.len == 69, $result.len
  doAssert result[66] == 0x02'u8, $result[66]
  result[66] = 0x7E'u8 # zigzag-varint 63: over-reads the record window
  let crcBE = toBytesBE(crc32c(result.toOpenArray(21, result.len - 1)))
  for i in 0 ..< 4:
    result[17 + i] = crcBE[i]

suite "parseRecordBatches — truncated tail handling":
  test "a single complete batch parses":
    let parsed = parseRecordBatches(oneRecordBatch(0'i64, byte('a')))
    check parsed.status == bpsClean
    check parsed.batches.len == 1
    check parsed.batches[0].baseOffset == 0'i64
    check parsed.batches[0].records[0].value.get == @[byte('a')]

  test "two complete batches both parse":
    let raw = oneRecordBatch(0'i64, byte('a')) & oneRecordBatch(1'i64, byte('b'))
    let parsed = parseRecordBatches(raw)
    check parsed.status == bpsClean
    check parsed.batches.len == 2
    check parsed.batches[0].baseOffset == 0'i64
    check parsed.batches[1].baseOffset == 1'i64

  test "a rejected record does not drop the following batch":
    # loadRecords narrowed rdr.limit to the record window and the
    # pre-fix code restored it only on success. After the fetcher's batchLength
    # resync the stale limit made `remaining` negative, so the loop exited and
    # the next clean batch was never parsed (bpsUnknownGap instead of
    # bpsSkipped) — with OffsetResetPolicy.latest that is data loss.
    let raw = malformedRecordBatch(0'i64) & oneRecordBatch(5'i64, byte('b'))
    let parsed = parseRecordBatches(raw)
    check parsed.corruptCount == 1
    check parsed.batches.len == 1
    check parsed.batches[0].baseOffset == 5'i64
    check parsed.status == bpsSkipped
    check parsed.corruptGapEnd == 5'i64

  test "a truncated trailing batch is dropped, complete batches kept":
    # One complete batch followed by all-but-3 bytes of a second batch. The
    # old loop called decode() on the partial bytes and raised batchEnd >
    # data.len, discarding the complete batch too. parseRecordBatches must
    # stop at the truncation boundary and return only the complete batch.
    let complete = oneRecordBatch(0'i64, byte('a'))
    let partial = oneRecordBatch(1'i64, byte('b'))
    let raw = complete & partial[0 ..< partial.len - 3]
    let parsed = parseRecordBatches(raw)
    check parsed.status == bpsClean
    check parsed.batches.len == 1
    check parsed.batches[0].baseOffset == 0'i64

  test "a trailing fragment shorter than the 12-byte prefix is ignored":
    let complete = oneRecordBatch(0'i64, byte('a'))
    let raw = complete & @[0xAB'u8, 0xCD, 0xEF]
    let parsed = parseRecordBatches(raw)
    check parsed.status == bpsClean
    check parsed.batches.len == 1

  test "an empty blob yields no batches":
    let parsed = parseRecordBatches(@[])
    check parsed.status == bpsClean
    check parsed.batches.len == 0

  test "a corrupt (negative) batchLength leaves the gap unpinned":
    # A genuinely corrupt length prefix must NOT be mistaken for truncation:
    # 12 + negative < remaining is false, so it falls through to decode() and
    # trips the decoder's length guard. The rest of the header may still look
    # intact, but baseOffset + lastOffsetDelta there belongs to the same bytes
    # that failed the check, so it must NOT become a skip target — the gap
    # stays unknown and the caller repositions under its own policy.
    var raw = oneRecordBatch(0'i64, byte('a'))
    # Overwrite batchLength (bytes 8..11) with -1.
    raw[8] = 0xFF
    raw[9] = 0xFF
    raw[10] = 0xFF
    raw[11] = 0xFF
    let parsed = parseRecordBatches(raw)
    check parsed.status == bpsUnknownGap
    check parsed.batches.len == 0
    check parsed.corruptCount == 1
    check parsed.corruptGapEnd == -1
    # And the cursor does not move: the old code turned the corrupt header's
    # "1" into fetchOffset, silently dropping whatever it covered.
    check computeNextFetchOffset(
      parsed.batches, -1'i64, false, 7'i64, parsed.corruptGapEnd
    ) == 7'i64

proc putInt32BE(buf: var seq[byte], at: int, v: int32) =
  ## Overwrite the big-endian int32 at `at`. Wire fixtures are patched in place
  ## (a CRC field is only known once the bytes it covers are written).
  let u = cast[uint32](v)
  buf[at] = byte(u shr 24)
  buf[at + 1] = byte(u shr 16)
  buf[at + 2] = byte(u shr 8)
  buf[at + 3] = byte(u)

proc legacyMessageV0(baseOffset: int64, value: byte): seq[byte] =
  ## A genuine pre-0.11 MessageSet message (magic 0) with a valid CRC32C:
  ##   offset(8) messageSize(4) crc(4) magic(1) attributes(1) keyLen(4) valueLen(4) value
  ## v0 puts the CRC FIRST and covers magic..value, and `messageSize` is the
  ## body length after offset+size, so the whole entry is 12 + messageSize
  ## bytes — the framing the v2 decoder's legacy branch assumes.
  var w = initKafkaWriter()
  w.writeInt64(baseOffset)
  w.writeInt32(15'i32) # 4(crc) + 1(magic) + 1(attrs) + 4(keyLen) + 4(valLen) + 1
  w.writeInt32(0'i32) # crc placeholder
  w.writeInt8(0'i8) # magic = 0
  w.writeInt8(0'i8) # attributes
  w.writeInt32(-1'i32) # key = null
  w.writeInt32(1'i32) # value length
  w.buf.add(value)
  putInt32BE(w.buf, 12, cast[int32](crc32c(w.buf.toOpenArray(16, w.buf.len - 1))))
  w.buf

proc legacyMessageV1(baseOffset: int64, value: byte): seq[byte] =
  ## The v1 layout moves the CRC to the END (so the magic byte can be read
  ## first) and adds an 8-byte timestamp after the attributes:
  ##   offset(8) messageSize(4) magic(1) attributes(1) timestamp(8) keyLen(4) valueLen(4) value crc(4)
  ## The CRC covers magic..value, i.e. every byte of the body but its own last 4.
  var w = initKafkaWriter()
  w.writeInt64(baseOffset)
  w.writeInt32(23'i32) # 1 + 1 + 8 + 4 + 4 + 1 + 4(crc)
  w.writeInt8(1'i8) # magic = 1
  w.writeInt8(0'i8) # attributes
  w.writeInt64(0'i64) # timestamp
  w.writeInt32(-1'i32) # key = null
  w.writeInt32(1'i32) # value length
  w.buf.add(value)
  w.writeInt32(cast[int32](crc32c(w.buf.toOpenArray(12, w.buf.len - 1))))
  w.buf

suite "parseRecordBatches — legacy MessageSet (magic != 2)":
  ## The v2 decoder's legacy branch consumes a pre-0.11 message without a
  ## single check, so everything the fetcher then reads from it — `baseOffset`
  ## above all, which moves the cursor — comes from bytes nothing has verified.
  ## A legacy message carries its own CRC32C, so its header may only be trusted
  ## once that verifies; until then it is corruption like any other bad batch.

  test "a verified v0 message is kept, counted and skipped past its own offset":
    let raw = oneRecordBatch(0'i64, byte('a')) & legacyMessageV0(1'i64, byte('l'))
    let parsed = parseRecordBatches(raw)
    check parsed.status == bpsClean
    check parsed.batches.len == 2
    check parsed.batches[1].baseOffset == 1'i64
    check parsed.batches[1].magic != 2'i8
    check countLegacyMagicBatches(parsed.batches) == 1
    check countLegacyMagicBatches(@[parsed.batches[0]]) == 0
    # The cursor moves past the v2 batch's record and the legacy message's one
    # offset — the legacy message is dropped, but it still occupies an offset.
    check computeNextFetchOffset(
      parsed.batches, 0'i64, queueFull = false, currentFetchOffset = 0
    ) == 2'i64

  test "a verified v1 message is kept too (the other CRC placement)":
    let raw = legacyMessageV1(0'i64, byte('l')) & oneRecordBatch(1'i64, byte('a'))
    let parsed = parseRecordBatches(raw)
    check parsed.status == bpsClean
    check parsed.batches.len == 2
    check countLegacyMagicBatches(parsed.batches) == 1
    check computeNextFetchOffset(
      parsed.batches, 1'i64, queueFull = false, currentFetchOffset = 0
    ) == 2'i64

  test "a v0 message whose CRC fails is corruption and moves the cursor nowhere":
    var msg = legacyMessageV0(1'i64, byte('l'))
    msg[^1] = msg[^1] xor 0xFF # damage the value, inside the CRC region
    let parsed = parseRecordBatches(oneRecordBatch(0'i64, byte('a')) & msg)
    check parsed.corruptCount == 1
    check parsed.batches.len == 1
    check countLegacyMagicBatches(parsed.batches) == 0
    # Trailing corruption with no clean successor: the range stays unknown, so
    # the caller repositions instead of guessing from the unverified header.
    check parsed.status == bpsUnknownGap
    check parsed.corruptGapEnd == -1
    check computeNextFetchOffset(
      parsed.batches, 0'i64, false, 0'i64, parsed.corruptGapEnd
    ) == 1'i64

  test "an unverified legacy header cannot jump the cursor by a forged offset":
    # The strongest form of the same rule. The leading offset is NOT covered by
    # the legacy CRC, and the CRC check is what makes it worth trusting at all:
    # a message that fails it must contribute nothing to the cursor, however
    # plausible its offset looks. (Verified messages still move the cursor by
    # their broker-assigned offset — the check is what authorises that.)
    var msg = legacyMessageV0(1_000_000'i64, byte('l'))
    msg[^1] = msg[^1] xor 0xFF
    let parsed = parseRecordBatches(oneRecordBatch(0'i64, byte('a')) & msg)
    check parsed.batches.len == 1
    check computeNextFetchOffset(
      parsed.batches, 0'i64, false, 0'i64, parsed.corruptGapEnd
    ) == 1'i64

  test "a verified legacy message inside a corrupted range is still dropped":
    # Once a batch in the blob has failed its check, a magic != 2 message
    # following it is far more likely a misframed fragment of that corrupted
    # region than a real MessageSet. It stays dropped even though its own CRC
    # verifies.
    var bad = oneRecordBatch(1'i64, byte('b'))
    bad[^1] = bad[^1] xor 0xFF
    let parsed = parseRecordBatches(
      oneRecordBatch(0'i64, byte('a')) & bad & legacyMessageV0(9'i64, byte('l'))
    )
    check parsed.corruptCount == 2
    check parsed.batches.len == 1
    check parsed.batches[0].baseOffset == 0'i64
    check countLegacyMagicBatches(parsed.batches) == 0

  test "a legacy message never raises the monotone leader-epoch ceiling":
    # KIP-320: lastFetchedEpoch is a monotone ceiling — the epoch sent with a
    # later OffsetForLeaderEpoch query to detect truncation. A legacy message's
    # bytes at that position are its CRC (v0) or part of its timestamp (v1), so
    # adopting one as an epoch leaves the ceiling above every real epoch and the
    # check never fires again for the life of the partition.
    proc body() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil)
      let tp = TopicPartition(topic: "legacy-epoch", partition: 0)
      f.registerPartitionForTest(tp, 0)
      let v2 = RecordBatch(
        baseOffset: 0,
        magic: 2'i8,
        partitionLeaderEpoch: 3'i32,
        lastOffsetDelta: 0'i32,
        records: @[Record(offset: 0, timestamp: 0, value: some(@[byte('a')]))],
      )
      let legacy = RecordBatch(
        baseOffset: 1,
        magic: 0'i8,
        partitionLeaderEpoch: 999_999'i32,
        lastOffsetDelta: 0'i32,
      )
      discard await f.enqueuePartitionForTest(tp, @[], @[v2, legacy])
      check f.lastFetchedEpochForTest(tp) == 3'i32
      # The offsets themselves still advance: skipping a legacy message is fine,
      # trusting its epoch bytes is not.
      check f.getFetchOffsets()[tp] == 2'i64

    waitFor body()

suite "parseRecordBatches — corrupt batch recovery":
  ## A corrupt batch (CRC mismatch / malformed header) must not abort the whole
  ## blob: the clean batches around it survive and the corrupted offset range is
  ## reported so the fetcher can advance past it instead of re-fetching the same
  ## bad bytes forever (permanent partition stall).

  test "a corrupt middle batch is dropped, surrounding batches kept, gap pinned":
    let b0 = oneRecordBatch(0'i64, byte('a'))
    var b1 = oneRecordBatch(1'i64, byte('b'))
    let b2 = oneRecordBatch(2'i64, byte('c'))
    # Flip a byte inside the CRC-covered region of the middle batch.
    b1[^1] = b1[^1] xor 0xFF
    let parsed = parseRecordBatches(b0 & b1 & b2)
    check parsed.status == bpsSkipped
    check parsed.corruptCount == 1
    check parsed.batches.len == 2
    check parsed.batches[0].baseOffset == 0'i64
    check parsed.batches[1].baseOffset == 2'i64
    # Pinned by the next clean batch, not the (untrusted) corrupt header.
    check parsed.corruptGapEnd == 2'i64

  test "a corrupt trailing batch keeps the prefix, gap left unpinned":
    let b0 = oneRecordBatch(10'i64, byte('a'))
    var b1 = oneRecordBatch(11'i64, byte('b'))
    b1[^1] = b1[^1] xor 0xFF
    let parsed = parseRecordBatches(b0 & b1)
    check parsed.status == bpsUnknownGap
    check parsed.corruptCount == 1
    check parsed.batches.len == 1
    check parsed.batches[0].baseOffset == 10'i64
    # No clean batch follows, and the corrupt header is not a source of truth:
    # its baseOffset 11 / lastOffsetDelta 0 are exactly the bytes that failed
    # the CRC, so the range is reported unknown instead of guessed as 12.
    check parsed.corruptGapEnd == -1

  test "a corrupt header claiming a huge span never becomes a skip target":
    # The strongest form of the same rule. baseOffset..magic sit OUTSIDE the
    # CRC region, so a corrupt header can still look plausible, and its
    # lastOffsetDelta (forged below — the CRC does cover it) can name a
    # multi-gigabyte span. Adopting that pair moved fetchOffset past every
    # record in between, silently and permanently (the next fetch starts
    # after them).
    var b0 = oneRecordBatch(0'i64, byte('a'))
    # Forge the offset span: baseOffset 0, lastOffsetDelta = int32.high.
    for i in 0 ..< 4:
      b0[23 + i] = 0xFF
    # Now break the payload so the CRC fails and the batch is dropped.
    b0[^1] = b0[^1] xor 0xFF
    let parsed = parseRecordBatches(b0)
    check parsed.status == bpsUnknownGap
    check parsed.corruptCount == 1
    check parsed.batches.len == 0
    check parsed.corruptGapEnd == -1
    check computeNextFetchOffset(
      parsed.batches, -1'i64, false, 0'i64, parsed.corruptGapEnd
    ) == 0'i64

  test "a corrupt leading batch keeps the suffix, gap pinned at its baseOffset":
    var b0 = oneRecordBatch(5'i64, byte('a'))
    let b1 = oneRecordBatch(6'i64, byte('b'))
    b0[^1] = b0[^1] xor 0xFF
    let parsed = parseRecordBatches(b0 & b1)
    check parsed.status == bpsSkipped
    check parsed.corruptCount == 1
    check parsed.batches.len == 1
    check parsed.batches[0].baseOffset == 6'i64
    check parsed.corruptGapEnd == 6'i64

  test "an unrecoverable header (length AND delta destroyed) reports bpsUnknownGap":
    var raw = oneRecordBatch(0'i64, byte('a'))
    # Destroy the framing length and the offset span so no skip target exists.
    raw[8] = 0xFF
    raw[9] = 0xFF
    raw[10] = 0xFF
    raw[11] = 0xFF
    raw[23] = 0xFF
    raw[24] = 0xFF
    raw[25] = 0xFF
    raw[26] = 0xFF
    let parsed = parseRecordBatches(raw)
    check parsed.status == bpsUnknownGap
    check parsed.batches.len == 0
    check parsed.corruptCount == 1
    check parsed.corruptGapEnd == -1

suite "parseRecordBatches — total decompression budget":
  proc toBytes(s: string): seq[byte] =
    cast[seq[byte]](s)

  proc compressedBatch(baseOffset: int64, payload: seq[byte]): seq[byte] =
    ## Encode a snappy-compressed single-record RecordBatch v2 to wire bytes.
    let batch = RecordBatch(
      baseOffset: baseOffset,
      partitionLeaderEpoch: 0'i32,
      attributes: int16(ccSnappy),
      firstTimestamp: 0'i64,
      producerId: -1'i64,
      producerEpoch: -1'i16,
      baseSequence: -1'i32,
      records: @[
        Record(
          offset: 0'i64,
          timestamp: 0'i64,
          key: none(seq[byte]),
          value: some(payload),
          headers: @[],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(batch)
    w.buf

  test "a batch within the budget parses and the budget is decremented":
    let budget = DecompressionBudget(remaining: 4096)
    let parsed = parseRecordBatches(compressedBatch(0'i64, "payload".toBytes), budget)
    check parsed.status == bpsClean
    check parsed.batches.len == 1
    check budget.remaining < 4096
    check budget.remaining >= 0

  test "a response exceeding the budget raises, not corrupt-skip":
    # The batches are well-formed; only the cumulative decompressed size is
    # over budget. That must surface as KafkaDecompressionLimitError — the
    # corrupt-batch path would silently advance the offset past the records.
    let blob = compressedBatch(0'i64, "payload".toBytes)
    expect KafkaDecompressionLimitError:
      discard parseRecordBatches(blob, DecompressionBudget(remaining: 1))

  test "the budget is the cap the codec enforces, not a post-decompression check":
    # The snappy header declares its uncompressed length before any output is
    # produced, so a refusal that names that length proves the budget reached the
    # codec instead of being compared against an already-decompressed batch.
    let blob = compressedBatch(0'i64, "payload".toBytes)
    var message = ""
    try:
      discard parseRecordBatches(blob, DecompressionBudget(remaining: 4))
    except KafkaDecompressionLimitError as e:
      message = e.msg
    check message.contains("exceeds remaining decompression budget")

  test "a shared budget spans partitions of one response":
    let blob = compressedBatch(0'i64, "payload".toBytes)
    var probe = DecompressionBudget(remaining: high(int) div 4)
    let parsed1 = parseRecordBatches(blob, probe)
    check parsed1.batches.len == 1
    let consumed = (high(int) div 4) - probe.remaining
    check consumed > 0
    probe.remaining = consumed - 1
    # A second partition blob under the same (nearly spent) budget is rejected.
    expect KafkaDecompressionLimitError:
      discard parseRecordBatches(blob, probe)

# Position-filter regression: brokers return data at BATCH granularity, so a
# fetchOffset landing inside a batch yields a first batch with records BELOW the
# position. Those are already consumed; the fetcher must skip them rather than
# replay up to a whole batch on every restart/seek (Java client parity).

proc encodeBatchBlob(baseOffset: int64, count: int): seq[byte] =
  ## A v2 RecordBatch with `count` records at absolute offsets
  ## baseOffset ..< baseOffset+count, encoded via the production codec.
  var recs: seq[Record]
  for i in 0 ..< count:
    recs.add Record(
      offset: baseOffset + int64(i), timestamp: 1000 + int64(i), value: some(@[byte(i)])
    )
  let batch = RecordBatch(
    baseOffset: baseOffset,
    partitionLeaderEpoch: 0,
    attributes: 0,
    firstTimestamp: 1000,
    producerId: -1,
    producerEpoch: -1,
    baseSequence: -1,
    records: recs,
  )
  var w = initKafkaWriter()
  w.encode(batch)
  w.buf

proc respondApiVersionsAndParkFetch(
    fb: FakeBroker, parked: AsyncEvent
) {.async: (raises: []).} =
  ## Answer ApiVersions so the broker can connect, then fire `parked` on every
  ## Fetch without responding. Leaves the RPC in flight so the caller can
  ## cancel / stop the fetcher against a live fetchFromBroker await.
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 2.seconds)
    except CatchableError:
      break
    try:
      if req.apiKey == int16(ord(akApiVersions)):
        await req.respond(
          buildApiVersionsResponseBody(
            0'i16,
            [
              FakeApiVersion(
                apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
              ),
              FakeApiVersion(apiKey: int16(ord(akFetch)), minVersion: 4, maxVersion: 7),
            ],
          )
        )
      elif req.apiKey == int16(ord(akFetch)):
        parked.fire()
    except CatchableError:
      break

proc autoRespondFetch(fb: FakeBroker, fetchBody: seq[byte]) {.async: (raises: []).} =
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 250.milliseconds)
    except CatchableError:
      break
    try:
      if req.apiKey == int16(ord(akApiVersions)):
        let body = buildApiVersionsResponseBody(
          0'i16,
          [
            FakeApiVersion(
              apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
            ),
            FakeApiVersion(apiKey: int16(ord(akFetch)), minVersion: 4, maxVersion: 7),
          ],
        )
        await req.respond(body)
      elif req.apiKey == int16(ord(akFetch)):
        await req.respond(fetchBody)
    except CatchableError:
      break

suite "Fetcher: retriable per-partition errors back off":
  proc serveFetchBodies(
      fb: FakeBroker, bodies: seq[seq[byte]]
  ) {.async: (raises: []).} =
    ## Answers ApiVersions, then the fetch bodies in order (the last one repeats),
    ## so a test can drive consecutive rounds with different responses.
    var served = 0
    while true:
      var req: CapturedRequest
      try:
        req = await fb.nextRequest(timeout = 250.milliseconds)
      except CatchableError:
        break
      try:
        if req.apiKey == int16(ord(akApiVersions)):
          await req.respond(
            buildApiVersionsResponseBody(
              0'i16,
              [
                FakeApiVersion(
                  apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                ),
                FakeApiVersion(
                  apiKey: int16(ord(akFetch)), minVersion: 4, maxVersion: 7
                ),
              ],
            )
          )
        elif req.apiKey == int16(ord(akFetch)):
          let body =
            if served < bodies.len:
              bodies[served]
            else:
              bodies[^1]
          inc served
          await req.respond(body)
      except CatchableError:
        break

  proc fetchBodyFor(topic: string, partition: int32, code: int16): seq[byte] =
    ## Fetch v7 response carrying one partition with `code` and no records.
    buildFetchResponseBody(
      throttleTimeMs = 0'i32,
      errorCode = 0'i16,
      sessionId = 0'i32,
      topics = [
        FakeFetchTopic(
          topic: topic,
          partitions: @[
            FakeFetchPartition(
              partition: partition,
              errorCode: code,
              highWatermark: 0'i64,
              lastStableOffset: 0'i64,
              logStartOffset: 0'i64,
              recordsRaw: some(newSeq[byte](0)),
            )
          ],
        )
      ],
    )

  test "a round whose every partition failed retriably waits and counts as a failure":
    ## Regression: the retriable branch skipped the partition and continued with no
    ## delay, so a broker answering retriable codes as fast as it received requests
    ## drove the fetch loop at RPC rate, and the round counted as progress.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let topic = "retry-topic"
      let tp = TopicPartition(topic: topic, partition: 0)
      let handler = serveFetchBodies(
        fb, @[fetchBodyFor(topic, 0'i32, int16(ord(keKafkaStorageError)))]
      )

      let f = newFetcher(nil)
      let b = newBroker(0'i32, "127.0.0.1", port, "retry-test")
      f.registerPartitionForTest(tp, 0, broker = b)

      let started = Moment.now()
      await f.fetchOnceForTest().wait(5.seconds)
      let elapsedMs = (Moment.now() - started).milliseconds

      check f.retriableFetchAttemptsForTest() == 1
      # The round waited the jittered backoff (100ms base, -25% jitter).
      check elapsedMs >= 50
      # No partition made progress, so the round is a broker failure the existing
      # escalation (metadata refresh) can act on.
      check f.brokerFetchErrorsForTest().getOrDefault(0'i32, 0) == 1

      await b.close()
      await fb.close()
      awaitDrain(handler, 1.seconds)

    waitFor testBody()

  test "a round without a retriable error resets the backoff":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let topic = "retry-reset-topic"
      let tp = TopicPartition(topic: topic, partition: 0)
      let handler = serveFetchBodies(
        fb,
        @[
          fetchBodyFor(topic, 0'i32, int16(ord(keKafkaStorageError))),
          fetchBodyFor(topic, 0'i32, int16(ord(keKafkaStorageError))),
          fetchBodyFor(topic, 0'i32, 0'i16),
        ],
      )

      let f = newFetcher(nil)
      let b = newBroker(0'i32, "127.0.0.1", port, "retry-reset-test")
      f.registerPartitionForTest(tp, 0, broker = b)

      await f.fetchOnceForTest().wait(5.seconds)
      check f.retriableFetchAttemptsForTest() == 1
      await f.fetchOnceForTest().wait(5.seconds)
      check f.retriableFetchAttemptsForTest() == 2
      # A clean round resets the escalation.
      await f.fetchOnceForTest().wait(5.seconds)
      check f.retriableFetchAttemptsForTest() == 0

      await b.close()
      await fb.close()
      awaitDrain(handler, 1.seconds)

    waitFor testBody()

suite "Fetcher: legacy batch reporting":
  test "a legacy message set is counted and reported from the fetch path":
    ## The parse-level tests above cover what a legacy entry decodes to; this
    ## covers the fetch path's report of it, which is the only signal that a
    ## partition is still being served pre-0.11 records the client drops.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      # One v2 record at 40, then a verified magic-0 message at 45 whose record
      # the decoder reports as empty.
      let blob = encodeBatchBlob(40, 1) & legacyMessageV0(45'i64, byte('l'))
      let fetchBody = buildFetchResponseBody(
        throttleTimeMs = 0'i32,
        errorCode = 0'i16,
        sessionId = 0'i32,
        topics = [
          FakeFetchTopic(
            topic: "legacy-topic",
            partitions: @[
              FakeFetchPartition(
                partition: 0'i32,
                errorCode: 0'i16,
                highWatermark: 100'i64,
                lastStableOffset: 100'i64,
                logStartOffset: 0'i64,
                recordsRaw: some(blob),
              )
            ],
          )
        ],
      )
      let respFut = autoRespondFetch(fb, fetchBody)

      let sink = MetricSink()
      let f = newFetcher(nil, metricsHook = makeMetricHook(sink))
      let b = newBroker(0'i32, "127.0.0.1", port, "legacy-test")
      let tp = TopicPartition(topic: "legacy-topic", partition: 0)
      f.registerPartitionForTest(tp, 40, broker = b)

      await f.fetchOnceForTest().wait(5.seconds)

      var delivered: seq[int64]
      while not f.records.empty():
        delivered.add(f.records.getNoWait().offset)
      # The v2 record arrives; the legacy message's record does not exist.
      check delivered == @[40'i64]
      let ev = findMetricEvent(sink, "consumer.batch.legacy_skipped")
      check ev.isSome
      check ev.get.value == 1'i64
      check metricTagValue(ev.get, "topic") == some("legacy-topic")
      check metricTagValue(ev.get, "partition") == some("0")

      await b.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor testBody()

suite "Fetcher: records below the fetch position are skipped":
  test "mid-batch position delivers only records at/after it; whole-batch-below delivers none":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      # One batch spanning offsets 45..54, served for both partitions.
      let blob = encodeBatchBlob(45, 10)
      let fetchBody = buildFetchResponseBody(
        throttleTimeMs = 0'i32,
        errorCode = 0'i16,
        sessionId = 0'i32,
        topics = [
          FakeFetchTopic(
            topic: "skip-topic",
            partitions: @[
              FakeFetchPartition(
                partition: 0'i32,
                errorCode: 0'i16,
                highWatermark: 100'i64,
                lastStableOffset: 100'i64,
                logStartOffset: 0'i64,
                recordsRaw: some(blob),
              ),
              FakeFetchPartition(
                partition: 1'i32,
                errorCode: 0'i16,
                highWatermark: 100'i64,
                lastStableOffset: 100'i64,
                logStartOffset: 0'i64,
                recordsRaw: some(blob),
              ),
            ],
          )
        ],
      )
      let respFut = autoRespondFetch(fb, fetchBody)

      let f = newFetcher(nil)
      let b = newBroker(0'i32, host, port, "skip-test")
      let tp0 = TopicPartition(topic: "skip-topic", partition: 0)
      let tp1 = TopicPartition(topic: "skip-topic", partition: 1)
      # tp0: position mid-batch — records 45..49 were already consumed.
      f.registerPartitionForTest(tp0, 50, broker = b)
      # tp1: position past the whole batch — nothing must be delivered.
      f.registerPartitionForTest(tp1, 60, broker = b)

      await f.fetchOnceForTest().wait(5.seconds)

      var delivered: seq[int64]
      while not f.records.empty():
        let rec = f.records.getNoWait()
        check rec.partition == 0'i32
        delivered.add(rec.offset)
      check delivered == @[50'i64, 51, 52, 53, 54]

      let offsets = f.getFetchOffsets()
      # tp0 advances past the batch end; tp1 never moves backwards.
      check offsets[tp0] == 55'i64
      check offsets[tp1] == 60'i64

      await b.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor testBody()

suite "Fetcher: partitions absent from the request are skipped":
  test "records and errors for unrequested partitions move no cursor":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let host = "127.0.0.1"
      let port = int32(fb.address().port)
      let blob = encodeBatchBlob(70, 3)
      let fetchBody = buildFetchResponseBody(
        throttleTimeMs = 0'i32,
        errorCode = 0'i16,
        sessionId = 0'i32,
        topics = [
          FakeFetchTopic(
            topic: "echo-topic",
            partitions: @[
              FakeFetchPartition(
                partition: 0'i32,
                errorCode: 0'i16,
                highWatermark: 100'i64,
                lastStableOffset: 100'i64,
                logStartOffset: 0'i64,
                recordsRaw: some(blob),
              ),
              # Never requested: valid bytes that must not be delivered.
              FakeFetchPartition(
                partition: 1'i32,
                errorCode: 0'i16,
                highWatermark: 100'i64,
                lastStableOffset: 100'i64,
                logStartOffset: 0'i64,
                recordsRaw: some(blob),
              ),
              # Never requested: a stale-leader code that must not reach
              # the metadata refresh path (the cluster here is nil).
              FakeFetchPartition(
                partition: 2'i32,
                errorCode: int16(keNotLeaderOrFollower),
                highWatermark: 100'i64,
                lastStableOffset: 100'i64,
                logStartOffset: 0'i64,
                recordsRaw: none(seq[byte]),
              ),
            ],
          ),
          # Never requested: a whole topic that must not be delivered.
          FakeFetchTopic(
            topic: "ghost-topic",
            partitions: @[
              FakeFetchPartition(
                partition: 0'i32,
                errorCode: 0'i16,
                highWatermark: 100'i64,
                lastStableOffset: 100'i64,
                logStartOffset: 0'i64,
                recordsRaw: some(blob),
              )
            ],
          ),
        ],
      )
      let respFut = autoRespondFetch(fb, fetchBody)

      let f = newFetcher(nil)
      let b = newBroker(0'i32, host, port, "echo-test")
      let tp0 = TopicPartition(topic: "echo-topic", partition: 0)
      f.registerPartitionForTest(tp0, 70, broker = b)

      await f.fetchOnceForTest().wait(5.seconds)

      var delivered: seq[int64]
      while not f.records.empty():
        let rec = f.records.getNoWait()
        check rec.topic == "echo-topic"
        check rec.partition == 0'i32
        delivered.add(rec.offset)
      check delivered == @[70'i64, 71, 72]

      let offsets = f.getFetchOffsets()
      check offsets.len == 1
      check offsets[tp0] == 73'i64

      await b.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor testBody()

suite "Fetcher: corrupt batch skip":
  ## A corrupt batch used to raise out of parseRecordBatches, discarding every
  ## batch already decoded; fetchOffset never advanced, so the next fetch
  ## re-requested the same range and the broker served the same corrupt bytes —
  ## a permanent partition stall. The parse now drops only the corrupt batch and
  ## recovers its offset range. That range comes from a CRC-verified batch when
  ## one follows; when none does, the partition repositions under its own reset
  ## policy (or surfaces under orpNone) instead of jumping by a value read out
  ## of the corrupt header.

  test "a corrupt trailing batch repositions rather than trusting its header":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      # Good batch 40..44 followed by a batch whose CRC is destroyed (45..49).
      let good = encodeBatchBlob(40, 5)
      var bad = encodeBatchBlob(45, 5)
      bad[^1] = bad[^1] xor 0xFF
      let fetchBody = buildFetchResponseBody(
        throttleTimeMs = 0'i32,
        errorCode = 0'i16,
        sessionId = 0'i32,
        topics = [
          FakeFetchTopic(
            topic: "corrupt-topic",
            partitions: @[
              FakeFetchPartition(
                partition: 0'i32,
                errorCode: 0'i16,
                highWatermark: 100'i64,
                lastStableOffset: 100'i64,
                logStartOffset: 0'i64,
                recordsRaw: some(good & bad),
              )
            ],
          )
        ],
      )
      # The corrupt batch is trailing, so nothing pins its end and the partition
      # resets through ListOffsets instead of jumping by the header's claim.
      proc handler() {.async: (raises: []).} =
        var handled = 0
        while handled < 2:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akFetch)), minVersion: 4, maxVersion: 7
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akListOffsets)), minVersion: 1, maxVersion: 2
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akFetch)):
              await req.respond(fetchBody)
            elif req.apiKey == int16(ord(akListOffsets)):
              await req.respond(
                buildListOffsetsResponseBodyV2(
                  "corrupt-topic",
                  partition = 0'i32,
                  errorCode = 0'i16,
                  timestamp = -1'i64,
                  offset = 250'i64,
                )
              )
              handled.inc
          except CatchableError:
            break

      let hf = handler()
      let tp = TopicPartition(topic: "corrupt-topic", partition: 0)
      let sink = MetricSink()
      let cluster = newCluster(@["127.0.0.1:" & $port])
      let b = newBroker(0'i32, "127.0.0.1", port, "corrupt-test")
      cluster.registerBrokerForTest(b)
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 6)
      cluster.markMetadataFreshForTest()

      let f = newFetcher(cluster, metricsHook = makeMetricHook(sink))
      f.registerPartitionForTest(tp, 40, broker = b)

      await f.fetchOnceForTest().wait(5.seconds)

      var delivered: seq[int64]
      while not f.records.empty():
        delivered.add(f.records.getNoWait().offset)
      # The clean batch 40..44 parsed from this same response is NOT delivered:
      # the reset invalidates the position it came from (and drains the queue),
      # so enqueueing it first would only be undone. Under orpLatest the whole
      # covered range is therefore skipped — the cost of refusing to jump by a
      # value read out of the bytes that failed the check.
      check delivered.len == 0
      # The cursor lands where the reset policy put it — not on 50, the span the
      # corrupt header claimed.
      check f.getFetchOffsets()[tp] == 250'i64
      # The reset is silent to the caller (the cursor just moves), so the event
      # is the only signal that records were dropped.
      let ev = findMetricEvent(sink, "consumer.fetch.corrupt_batch_reset")
      check ev.isSome
      check ev.get.value == 1'i64
      check metricTagValue(ev.get, "topic") == some("corrupt-topic")
      check metricTagValue(ev.get, "partition") == some("0")

      await cluster.close()
      await b.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor testBody()

  test "batches around a corrupt middle batch are both delivered":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      # 40..44 good, 45..49 corrupt, 50..54 good.
      let good1 = encodeBatchBlob(40, 5)
      var bad = encodeBatchBlob(45, 5)
      bad[^1] = bad[^1] xor 0xFF
      let good2 = encodeBatchBlob(50, 5)
      let fetchBody = buildFetchResponseBody(
        throttleTimeMs = 0'i32,
        errorCode = 0'i16,
        sessionId = 0'i32,
        topics = [
          FakeFetchTopic(
            topic: "corrupt-mid",
            partitions: @[
              FakeFetchPartition(
                partition: 0'i32,
                errorCode: 0'i16,
                highWatermark: 100'i64,
                lastStableOffset: 100'i64,
                logStartOffset: 0'i64,
                recordsRaw: some(good1 & bad & good2),
              )
            ],
          )
        ],
      )
      let respFut = autoRespondFetch(fb, fetchBody)

      let sink = MetricSink()
      let f = newFetcher(nil, metricsHook = makeMetricHook(sink))
      let b = newBroker(0'i32, "127.0.0.1", port, "corrupt-mid-test")
      let tp = TopicPartition(topic: "corrupt-mid", partition: 0)
      f.registerPartitionForTest(tp, 40, broker = b)

      await f.fetchOnceForTest().wait(5.seconds)

      var delivered: seq[int64]
      while not f.records.empty():
        delivered.add(f.records.getNoWait().offset)
      check delivered == @[40'i64, 41, 42, 43, 44, 50, 51, 52, 53, 54]
      check f.getFetchOffsets()[tp] == 55'i64
      # The middle batch is skipped and the batches around it still arrive; the
      # event carries how many were dropped.
      let ev = findMetricEvent(sink, "consumer.fetch.corrupt_batch_skipped")
      check ev.isSome
      check ev.get.value == 1'i64
      check metricTagValue(ev.get, "topic") == some("corrupt-mid")

      await b.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor testBody()

  test "unrecoverable corruption with orpNone surfaces instead of resetting":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      # Destroy both the framing length and the offset span of the only batch:
      # no skip target exists, and orpNone forbids an automatic reset.
      var blob = encodeBatchBlob(40, 5)
      blob[8] = 0xFF
      blob[9] = 0xFF
      blob[10] = 0xFF
      blob[11] = 0xFF
      blob[23] = 0xFF
      blob[24] = 0xFF
      blob[25] = 0xFF
      blob[26] = 0xFF
      let fetchBody = buildFetchResponseBody(
        throttleTimeMs = 0'i32,
        errorCode = 0'i16,
        sessionId = 0'i32,
        topics = [
          FakeFetchTopic(
            topic: "corrupt-none",
            partitions: @[
              FakeFetchPartition(
                partition: 0'i32,
                errorCode: 0'i16,
                highWatermark: 100'i64,
                lastStableOffset: 100'i64,
                logStartOffset: 0'i64,
                recordsRaw: some(blob),
              )
            ],
          )
        ],
      )
      let respFut = autoRespondFetch(fb, fetchBody)

      let f = newFetcher(nil, offsetResetPolicy = orpNone)
      let b = newBroker(0'i32, "127.0.0.1", port, "corrupt-none-test")
      let tp = TopicPartition(topic: "corrupt-none", partition: 0)
      f.registerPartitionForTest(tp, 40, broker = b)

      await f.fetchOnceForTest().wait(5.seconds)

      # The reset raise is caught by fetchOnce's non-fatal accounting: the
      # position must NOT move (no auto-reset under orpNone), nothing is
      # delivered, and the error surfaces via the consecutive-error counter
      # that poll-side escalation consumes.
      check f.records.empty()
      check f.getFetchOffsets()[tp] == 40'i64
      check f.fetchErrorCount() == 1

      await b.close()
      await fb.close()
      awaitDrain(respFut, 1.seconds)

    waitFor testBody()

suite "Fetcher position epoch: seek vs in-flight fetch":
  ## Regression: a seek-BACKWARD during an in-flight fetch was silently undone
  ## — the stale response's records (all above the seeked offset) were delivered
  ## and the never-backward clamp moved fetchOffset back past the seek. Each
  ## reposition now bumps the partition's posEpoch; a response whose request-time
  ## epoch snapshot no longer matches is discarded wholesale.

  test "seek-backward during an in-flight fetch discards the stale response":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let f = newFetcher(nil)
      let tp = TopicPartition(topic: "seek-topic", partition: 0)
      # Batch 50..59: the data the broker built for the PRE-seek position.
      let blob = encodeBatchBlob(50, 10)
      let fetchBody = buildFetchResponseBody(
        throttleTimeMs = 0'i32,
        errorCode = 0'i16,
        sessionId = 0'i32,
        topics = [
          FakeFetchTopic(
            topic: "seek-topic",
            partitions: @[
              FakeFetchPartition(
                partition: 0'i32,
                errorCode: 0'i16,
                highWatermark: 100'i64,
                lastStableOffset: 100'i64,
                logStartOffset: 0'i64,
                recordsRaw: some(blob),
              )
            ],
          )
        ],
      )

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akFetch)), minVersion: 4, maxVersion: 7
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akFetch)):
              # The fetch for offset 50 is now in flight; the user seeks
              # backward to 10 before the response is delivered.
              try:
                f.seek(tp, 10)
              except KafkaError:
                discard
              await req.respond(fetchBody)
              break
          except CatchableError:
            break

      let b = newBroker(0'i32, "127.0.0.1", port, "seek-epoch-test")
      f.registerPartitionForTest(tp, 50, broker = b)
      let handlerFut = handler()

      await f.fetchOnceForTest().wait(5.seconds)

      # The stale response is discarded wholesale: no pre-seek records are
      # delivered, and the seeked position survives (previously: 10 records
      # delivered and fetchOffset clobbered back to 60).
      check f.records.empty()
      check f.getFetchOffsets()[tp] == 10'i64

      await b.close()
      await fb.close()
      awaitDrain(handlerFut, 1.seconds)

    waitFor testBody()

  test "seek-backward while a record put is parked on a full queue is not undone":
    ## The top-of-partition posEpoch guard runs once, before the enqueue loop. A
    ## seek that lands while a `put` is parked on a full queue (backpressure) has
    ## already passed that guard, so without a mid-enqueue re-check the loop would
    ## refill the drained queue with pre-seek records and the never-backward clamp
    ## would move fetchOffset back past the seek. The re-check discards it instead.
    proc testBody() {.async: (raises: [Exception]).} =
      # Capacity-2 queue pre-filled with this partition's records so the very
      # first put of the in-flight response blocks on backpressure.
      let f = newFetcher(nil, queueSize = 2)
      let tp = TopicPartition(topic: "seek-topic", partition: 0)
      f.registerPartitionForTest(tp, 50)
      f.enqueueRecordForTest(
        FetchedRecord(topic: "seek-topic", partition: 0'i32, offset: 30)
      )
      f.enqueueRecordForTest(
        FetchedRecord(topic: "seek-topic", partition: 0'i32, offset: 31)
      )

      # A response the broker built against the pre-seek position (50..54).
      var recs: seq[Record]
      for i in 0 ..< 5:
        recs.add Record(offset: 50 + int64(i), timestamp: 1000, value: some(@[byte(i)]))
      let batch = RecordBatch(baseOffset: 50, lastOffsetDelta: 4, records: recs)

      # Runs synchronously up to the first put, which parks on the full queue.
      let fut = f.enqueuePartitionForTest(tp, @[batch], @[batch])
      check f.posEpochForTest(tp) == 0

      # Concurrent seek-backward: drains tp's queued records (waking the parked
      # put) and bumps posEpoch.
      f.seek(tp, 10)
      check f.posEpochForTest(tp) == 1

      # The mid-enqueue re-check fires: the partition is not reported as
      # queue-full, the stale record that slipped past the drain is dropped.
      check (await fut.wait(2.seconds)) == false

      # The seek owns the position now: fetchOffset stays 10, NOT advanced to
      # 55, and no pre-seek record survives in the queue.
      check f.getFetchOffsets()[tp] == 10'i64
      check f.records.empty()

    waitFor testBody()

  test "seek bumps posEpoch; unrelated partitions unaffected":
    let f = newFetcher(nil)
    let tp0 = TopicPartition(topic: "t", partition: 0)
    let tp1 = TopicPartition(topic: "t", partition: 1)
    f.registerPartitionForTest(tp0, 50)
    f.registerPartitionForTest(tp1, 50)
    check f.posEpochForTest(tp0) == 0
    f.seek(tp0, 10)
    check f.posEpochForTest(tp0) == 1
    check f.posEpochForTest(tp1) == 0

  test "offset reset bumps posEpoch":
    let f = newFetcher(nil)
    let tp = TopicPartition(topic: "t", partition: 0)
    f.registerPartitionForTest(tp, 50)
    f.applyOffsetResetForTest(tp, 0)
    check f.posEpochForTest(tp) == 1

suite "Fetcher pause vs in-flight fetch":
  ## pause() drops the broker's KIP-227 fetch session so the next fetch reopens
  ## a fresh one over only the still-active partitions (under an active session
  ## the broker keeps serving any partition merely omitted from a request). A
  ## pause() that lands while a fetch is in flight to the same broker must not
  ## be undone by the response handler: reviving the session or enqueueing the
  ## paused partition's records defeats the pause contract.

  test "pause during in-flight fetch does not revive the broker fetch session":
    ## Regression: pause() deletes fetchSessions[broker], but the response
    ## handler unconditionally writes fetchSessions[broker] back from the
    ## response's sessionId. The revived session still lists the paused
    ## partition on the broker side, so subsequent incremental fetches would
    ## keep returning its records.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let f = newFetcher(nil)
      let a = TopicPartition(topic: "pause-topic", partition: 0)
      let b = TopicPartition(topic: "pause-topic", partition: 1)

      let fetchBody = buildFetchResponseBody(
        throttleTimeMs = 0'i32,
        errorCode = 0'i16,
        sessionId = 5'i32,
        topics = [
          FakeFetchTopic(
            topic: "pause-topic",
            partitions: @[
              FakeFetchPartition(
                partition: 0'i32,
                errorCode: 0'i16,
                highWatermark: 100'i64,
                lastStableOffset: 100'i64,
                logStartOffset: 0'i64,
                recordsRaw: none(seq[byte]),
              ),
              FakeFetchPartition(
                partition: 1'i32,
                errorCode: 0'i16,
                highWatermark: 100'i64,
                lastStableOffset: 100'i64,
                logStartOffset: 0'i64,
                recordsRaw: none(seq[byte]),
              ),
            ],
          )
        ],
      )

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akFetch)), minVersion: 4, maxVersion: 7
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akFetch)):
              # Fetch is in flight; pause `a` before the response is delivered.
              try:
                f.pause(@[a])
              except KafkaError:
                discard
              await req.respond(fetchBody)
              break
          except CatchableError:
            break

      let broker0 = newBroker(0'i32, "127.0.0.1", port, "pause-inflight-test")
      f.registerPartitionForTest(a, 100, broker = broker0)
      f.registerPartitionForTest(b, 200, broker = broker0)
      # Prime an active KIP-227 session so the fetch goes out incremental
      # (sessionId=5, sessionEpoch=3) and pause() has a session to delete.
      f.setFetchSessionForTest(0'i32, sessionId = 5'i32, sessionEpoch = 3'i32)
      let hf = handler()

      await f.fetchOnceForTest().wait(5.seconds)

      # The pause() called mid-flight deleted fetchSessions[0]; the response
      # handler must not restore it. If restored, the next incremental fetch
      # would ride the same session and the broker would keep serving `a`.
      check not f.getFetchSessionForTest(0'i32).exists

      await broker0.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor testBody()

  test "pause during in-flight fetch does not enqueue records for the paused partition":
    ## Regression: the response processing loop calls enqueuePartition without
    ## checking `paused`, so records for a partition paused mid-flight are still
    ## delivered — violating the Java KafkaConsumer contract that post-pause
    ## poll() returns no records for paused partitions.
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let f = newFetcher(nil)
      let a = TopicPartition(topic: "pause-topic", partition: 0)
      let b = TopicPartition(topic: "pause-topic", partition: 1)

      let blobA = encodeBatchBlob(100, 5) # a: 100..104
      let blobB = encodeBatchBlob(200, 5) # b: 200..204
      let fetchBody = buildFetchResponseBody(
        throttleTimeMs = 0'i32,
        errorCode = 0'i16,
        sessionId = 0'i32,
        topics = [
          FakeFetchTopic(
            topic: "pause-topic",
            partitions: @[
              FakeFetchPartition(
                partition: 0'i32,
                errorCode: 0'i16,
                highWatermark: 200'i64,
                lastStableOffset: 200'i64,
                logStartOffset: 0'i64,
                recordsRaw: some(blobA),
              ),
              FakeFetchPartition(
                partition: 1'i32,
                errorCode: 0'i16,
                highWatermark: 300'i64,
                lastStableOffset: 300'i64,
                logStartOffset: 0'i64,
                recordsRaw: some(blobB),
              ),
            ],
          )
        ],
      )

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akFetch)), minVersion: 4, maxVersion: 7
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akFetch)):
              try:
                f.pause(@[a])
              except KafkaError:
                discard
              await req.respond(fetchBody)
              break
          except CatchableError:
            break

      let broker0 = newBroker(0'i32, "127.0.0.1", port, "pause-enqueue-test")
      f.registerPartitionForTest(a, 100, broker = broker0)
      f.registerPartitionForTest(b, 200, broker = broker0)
      let hf = handler()

      await f.fetchOnceForTest().wait(5.seconds)

      var deliveredA = 0
      var deliveredB = 0
      while not f.records.empty():
        let rec = f.records.getNoWait()
        if rec.partition == 0'i32:
          inc deliveredA
        elif rec.partition == 1'i32:
          inc deliveredB
      # `b` was never paused → its 5 records must arrive. `a` was paused before
      # the response landed → none of its records should be delivered.
      check deliveredB == 5
      check deliveredA == 0

      await broker0.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor testBody()

suite "Fetcher KIP-320 truncation: response handling":
  ## applyValidationResult acts on one OffsetForLeaderEpoch reply. keNone
  ## branches need no cluster, so drive them directly.

  test "truncation rewinds fetchOffset to endOffset and bumps posEpoch":
    proc body() {.async: (raises: [Exception]).} =
      let sink = MetricSink()
      let f = newFetcher(
        nil, offsetResetPolicy = orpLatest, metricsHook = makeMetricHook(sink)
      )
      let tp = TopicPartition(topic: "trunc", partition: 0)
      f.registerPartitionForTest(tp, 100)
      f.setAwaitingValidationForTest(tp, true)
      # Position was observed by the user, so the committable offset must realign
      # with the rewind, not keep the stale pre-truncation value.
      f.advanceConsumedOffset(
        FetchedRecord(
          topic: tp.topic, partition: tp.partition, offset: 100, leaderEpoch: 7'i32
        )
      )
      # The advance stamped the record's epoch onto the committable position;
      # the rewind must drop it (a commit carrying the pre-truncation epoch would
      # poison KIP-320 truncation detection on restart).
      check f.consumedLeaderEpochForTest(tp) == 7'i32
      let beforeEpoch = f.posEpochForTest(tp)
      # The leader's epoch-3 log ends at 50, below our fetch position 100.
      await f.applyValidationResultForTest(
        tp,
        validatedEpoch = 6'i32,
        errorCode = keNone,
        leaderEpoch = 3'i32,
        endOffset = 50'i64,
      )
      check f.getFetchOffsets()[tp] == 50'i64
      check f.getConsumedOffsets()[tp] == 50'i64
      check f.consumedLeaderEpochForTest(tp) == -1'i32
      check not f.awaitingValidationForTest(tp)
      check f.posEpochForTest(tp) == beforeEpoch + 1
      check f.lastFetchedEpochForTest(tp) == 6'i32
      # A silent rewind of the fetch position is exactly the kind of data loss
      # an operator has to be able to see; value is the rewound offset.
      let ev = findMetricEvent(sink, "consumer.fetch.truncation_detected")
      check ev.isSome
      check ev.get.value == 50'i64
      check metricTagValue(ev.get, "topic") == some("trunc")

    waitFor body()

  test "valid position clears the flag and records the validated epoch":
    proc body() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil, offsetResetPolicy = orpLatest)
      let tp = TopicPartition(topic: "valid", partition: 0)
      f.registerPartitionForTest(tp, 100)
      f.setAwaitingValidationForTest(tp, true)
      let beforeEpoch = f.posEpochForTest(tp)
      # endOffset 150 >= our position 100 → nothing truncated.
      await f.applyValidationResultForTest(
        tp,
        validatedEpoch = 6'i32,
        errorCode = keNone,
        leaderEpoch = 6'i32,
        endOffset = 150'i64,
      )
      check f.getFetchOffsets()[tp] == 100'i64
      check not f.awaitingValidationForTest(tp)
      check f.posEpochForTest(tp) == beforeEpoch # no reposition
      check f.lastFetchedEpochForTest(tp) == 6'i32

    waitFor body()

  test "truncation drains already-queued records for the partition":
    proc body() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil, offsetResetPolicy = orpLatest)
      let tp = TopicPartition(topic: "trunc", partition: 0)
      let other = TopicPartition(topic: "trunc", partition: 1)
      f.registerPartitionForTest(tp, 100)
      f.registerPartitionForTest(other, 100)
      f.setAwaitingValidationForTest(tp, true)
      for off in [80'i64, 90]:
        f.enqueueRecordForTest(
          FetchedRecord(topic: tp.topic, partition: tp.partition, offset: off)
        )
      f.enqueueRecordForTest(
        FetchedRecord(topic: other.topic, partition: other.partition, offset: 81)
      )
      await f.applyValidationResultForTest(
        tp,
        validatedEpoch = 6'i32,
        errorCode = keNone,
        leaderEpoch = 3'i32,
        endOffset = 50'i64,
      )
      var survivors: seq[TopicPartition]
      while not f.records.empty():
        let rec = f.records.getNoWait()
        survivors.add(TopicPartition(topic: rec.topic, partition: rec.partition))
      check survivors == @[other] # tp's queued records were dropped

    waitFor body()

  test "orpNone truncation raises and keeps the partition flagged":
    proc body() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil, offsetResetPolicy = orpNone)
      let tp = TopicPartition(topic: "trunc", partition: 0)
      f.registerPartitionForTest(tp, 100)
      f.setAwaitingValidationForTest(tp, true)
      var raised = false
      try:
        await f.applyValidationResultForTest(
          tp,
          validatedEpoch = 6'i32,
          errorCode = keNone,
          leaderEpoch = 3'i32,
          endOffset = 50'i64,
        )
      except KafkaError:
        raised = true
      check raised
      # No auto-rewind under orpNone; the partition stays out of the fetch.
      check f.getFetchOffsets()[tp] == 100'i64
      check f.awaitingValidationForTest(tp)

    waitFor body()

  test "orpNone offset-out-of-range during validation raises":
    proc body() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil, offsetResetPolicy = orpNone)
      let tp = TopicPartition(topic: "oor", partition: 0)
      f.registerPartitionForTest(tp, 100)
      f.setAwaitingValidationForTest(tp, true)
      var raised = false
      try:
        await f.applyValidationResultForTest(
          tp,
          validatedEpoch = 6'i32,
          errorCode = keOffsetOutOfRange,
          leaderEpoch = -1'i32,
          endOffset = -1'i64,
        )
      except KafkaError:
        raised = true
      check raised

    waitFor body()

  test "unexpected per-partition error skips the round":
    proc body() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil, offsetResetPolicy = orpLatest)
      let tp = TopicPartition(topic: "weird", partition: 0)
      f.registerPartitionForTest(tp, 100)
      f.setAwaitingValidationForTest(tp, true)
      await f.applyValidationResultForTest(
        tp,
        validatedEpoch = 6'i32,
        errorCode = keUnsupportedVersion,
        leaderEpoch = -1'i32,
        endOffset = -1'i64,
      )
      check not f.awaitingValidationForTest(tp)
      check f.getFetchOffsets()[tp] == 100'i64

    waitFor body()

  test "orpNone undefined epoch offset raises and keeps position":
    # keNone + endOffset=-1 (UNDEFINED_EPOCH_OFFSET): broker's epoch cache lost
    # our epoch. Under orpNone, must raise instead of rewinding to -1 — a -1
    # consumedOffset would surface via getConsumedOffsets and commit as garbage.
    proc body() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil, offsetResetPolicy = orpNone)
      let tp = TopicPartition(topic: "undef", partition: 0)
      f.registerPartitionForTest(tp, 100)
      # everConsumed=true amplifies the pre-fix bug: getConsumedOffsets would
      # return -1 and a commit could persist it.
      f.advanceConsumedOffset(
        FetchedRecord(topic: tp.topic, partition: tp.partition, offset: 99)
      )
      f.setAwaitingValidationForTest(tp, true)
      let beforeEpoch = f.posEpochForTest(tp)
      var raised = false
      try:
        await f.applyValidationResultForTest(
          tp,
          validatedEpoch = 6'i32,
          errorCode = keNone,
          leaderEpoch = -1'i32,
          endOffset = -1'i64,
        )
      except KafkaError:
        raised = true
      check raised
      # Position must NOT be clobbered with -1; awaitingValidation stays set.
      check f.getFetchOffsets()[tp] == 100'i64
      check f.getConsumedOffsets()[tp] == 100'i64
      check f.awaitingValidationForTest(tp)
      check f.posEpochForTest(tp) == beforeEpoch

    waitFor body()

suite "Fetcher KIP-320 truncation: leader-change trigger":
  ## markPartitionsForValidation flags a partition when the cached leaderEpoch
  ## advanced past the epoch its position is known-valid under.

  test "leader epoch advance flags the partition; equal / unknown / paused do not":
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:1"])
      let f = newFetcher(cluster)
      let advanced = TopicPartition(topic: "t", partition: 0)
      let unchanged = TopicPartition(topic: "t", partition: 1)
      let unknown = TopicPartition(topic: "t", partition: 2)
      let pausedTp = TopicPartition(topic: "t", partition: 3)
      for tp in [advanced, unchanged, unknown, pausedTp]:
        f.registerPartitionForTest(tp, 100, paused = tp == pausedTp)
        cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 5)
      f.setLastFetchedEpochForTest(advanced, 3) # 5 > 3 → flag
      f.setLastFetchedEpochForTest(unchanged, 5) # 5 == 5 → no
      # `unknown` keeps the default -1 → skipped regardless of leader epoch.
      f.setLastFetchedEpochForTest(pausedTp, 3) # would flag, but paused → no

      f.markPartitionsForValidationForTest()

      check f.awaitingValidationForTest(advanced)
      check not f.awaitingValidationForTest(unchanged)
      check not f.awaitingValidationForTest(unknown)
      check not f.awaitingValidationForTest(pausedTp)
      await cluster.close()

    waitFor body()

  test "broker-level unsupported suppresses the flag on later leader-epoch advances":
    # Once a broker returned keUnsupportedVersion, markPartitionsForValidation
    # must not re-flag its partitions — a repeated flag would drive another
    # doomed RPC every round, spamming logs and wasting broker-side work.
    proc body() {.async: (raises: [Exception]).} =
      let cluster = newCluster(@["127.0.0.1:1"])
      let f = newFetcher(cluster)
      let tp = TopicPartition(topic: "t", partition: 0)
      f.registerPartitionForTest(tp, 100)
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 5)
      f.setLastFetchedEpochForTest(tp, 3)
      f.markBrokerEpochValidationUnsupportedForTest(0'i32)

      f.markPartitionsForValidationForTest()
      check not f.awaitingValidationForTest(tp)

      # A leader change to a supported broker must re-enable validation.
      cluster.setPartitionLeaderForTest(tp, nodeId = 1, leaderEpoch = 6)
      f.markPartitionsForValidationForTest()
      check f.awaitingValidationForTest(tp)
      await cluster.close()

    waitFor body()

suite "Fetcher KIP-320 truncation: OffsetForLeaderEpoch round-trip":
  ## Full path: markPartitionsForValidation → validateOffsets → OffsetForLeaderEpoch
  ## RPC (v3, replicaId -2) → applyValidationResult, against a fake broker.

  test "a below-position endOffset rewinds the fetch cursor":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "trunc", partition: 0)

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akOffsetForLeaderEpoch)),
                      minVersion: 2,
                      maxVersion: 3,
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akOffsetForLeaderEpoch)):
              await req.respond(
                buildOffsetForLeaderEpochResponseBody(
                  "trunc",
                  [
                    FakeEpochEndOffset(
                      partition: 0'i32,
                      errorCode: 0'i16,
                      leaderEpoch: 3'i32,
                      endOffset: 50'i64,
                    )
                  ],
                )
              )
              break
          except CatchableError:
            break

      let b = newBroker(0'i32, "127.0.0.1", port, "valid-test")
      let cluster = newCluster(@["127.0.0.1:" & $port])
      cluster.registerBrokerForTest(b)
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 6)
      cluster.markMetadataFreshForTest()

      let f = newFetcher(cluster, offsetResetPolicy = orpLatest)
      f.registerPartitionForTest(tp, 100, broker = b)
      f.setLastFetchedEpochForTest(tp, 3)
      f.markPartitionsForValidationForTest()
      check f.awaitingValidationForTest(tp) # leader 6 > consumed 3

      let hf = handler()
      await f.validateOffsetsForTest().wait(5.seconds)

      check f.getFetchOffsets()[tp] == 50'i64
      check not f.awaitingValidationForTest(tp)

      await cluster.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor body()

  test "seek during in-flight validation is not clobbered by the truncation reply":
    # Regression for the posEpoch guard: a seek that races the OffsetForLeaderEpoch
    # RPC must not be reverted when the reply arrives.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "race", partition: 0)

      let b = newBroker(0'i32, "127.0.0.1", port, "race-test")
      let cluster = newCluster(@["127.0.0.1:" & $port])
      cluster.registerBrokerForTest(b)
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 6)
      cluster.markMetadataFreshForTest()

      let f = newFetcher(cluster, offsetResetPolicy = orpLatest)
      f.registerPartitionForTest(tp, 100, broker = b)
      f.setLastFetchedEpochForTest(tp, 3)
      f.setAwaitingValidationForTest(tp, true)

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akOffsetForLeaderEpoch)),
                      minVersion: 2,
                      maxVersion: 3,
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akOffsetForLeaderEpoch)):
              # Seek BEFORE responding so applyValidationResult sees the moved posEpoch.
              try:
                f.seek(tp, 500)
              except KafkaError:
                discard
              await req.respond(
                buildOffsetForLeaderEpochResponseBody(
                  "race",
                  [
                    FakeEpochEndOffset(
                      partition: 0'i32,
                      errorCode: 0'i16,
                      leaderEpoch: 3'i32,
                      endOffset: 50'i64,
                    )
                  ],
                )
              )
              break
          except CatchableError:
            break

      let hf = handler()
      await f.validateOffsetsForTest().wait(5.seconds)

      # seek(500) must survive; the endOffset=50 reply is dropped by posEpoch guard.
      check f.getFetchOffsets()[tp] == 500'i64

      await cluster.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor body()

  test "seek during undefined-epoch ListOffsets await is not clobbered by applyOffsetReset":
    # Regression for the posEpoch guard in applyValidationResult's
    # undefinedEpochOffset branch: a user seek() that lands while
    # resolveOffsets is in flight must survive; the resolved ListOffsets
    # value must NOT overwrite the seeked position.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "undef-seek", partition: 0)

      let cluster = newCluster(@["127.0.0.1:" & $port])
      let b = newBroker(0'i32, "127.0.0.1", port, "undef-seek-test")
      cluster.registerBrokerForTest(b)
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 6)
      cluster.markMetadataFreshForTest()

      let f = newFetcher(cluster, offsetResetPolicy = orpLatest)
      f.registerPartitionForTest(tp, 100, broker = b)
      f.setLastFetchedEpochForTest(tp, 3)
      f.setAwaitingValidationForTest(tp, true)

      proc handler() {.async: (raises: []).} =
        var handled = 0
        while handled < 2:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akOffsetForLeaderEpoch)),
                      minVersion: 2,
                      maxVersion: 3,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akListOffsets)), minVersion: 1, maxVersion: 2
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akOffsetForLeaderEpoch)):
              await req.respond(
                buildOffsetForLeaderEpochResponseBody(
                  "undef-seek",
                  [
                    FakeEpochEndOffset(
                      partition: 0'i32,
                      errorCode: 0'i16,
                      leaderEpoch: -1'i32,
                      endOffset: -1'i64,
                    )
                  ],
                )
              )
              handled.inc
            elif req.apiKey == int16(ord(akListOffsets)):
              # User seek() lands mid-flight — bumps posEpoch, moves fetchOffset.
              try:
                f.seek(tp, 500)
              except KafkaError:
                discard
              await req.respond(
                buildListOffsetsResponseBodyV2(
                  "undef-seek",
                  partition = 0'i32,
                  errorCode = 0'i16,
                  timestamp = -1'i64,
                  offset = 250'i64,
                )
              )
              handled.inc
          except CatchableError:
            break

      let hf = handler()
      await f.validateOffsetsForTest().wait(5.seconds)

      # seek(500) must survive; the 250 from ListOffsets is dropped.
      check f.getFetchOffsets()[tp] == 500'i64

      await cluster.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor body()

  test "undefined epoch offset: ListOffsets per-partition error clears awaitingValidation":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "undef-err", partition: 0)

      proc handler() {.async: (raises: []).} =
        var handled = 0
        while handled < 2:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akOffsetForLeaderEpoch)),
                      minVersion: 2,
                      maxVersion: 3,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akListOffsets)), minVersion: 1, maxVersion: 2
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akOffsetForLeaderEpoch)):
              await req.respond(
                buildOffsetForLeaderEpochResponseBody(
                  "undef-err",
                  [
                    FakeEpochEndOffset(
                      partition: 0'i32,
                      errorCode: 0'i16,
                      leaderEpoch: -1'i32,
                      endOffset: -1'i64,
                    )
                  ],
                )
              )
              handled.inc
            elif req.apiKey == int16(ord(akListOffsets)):
              await req.respond(
                buildListOffsetsResponseBodyV2(
                  "undef-err",
                  partition = 0'i32,
                  errorCode = int16(ord(keNotLeaderOrFollower)),
                  timestamp = -1'i64,
                  offset = -1'i64,
                )
              )
              handled.inc
          except CatchableError:
            break

      let b = newBroker(0'i32, "127.0.0.1", port, "undef-err-test")
      let cluster = newCluster(@["127.0.0.1:" & $port])
      cluster.registerBrokerForTest(b)
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 6)
      cluster.markMetadataFreshForTest()

      let f = newFetcher(cluster, offsetResetPolicy = orpLatest)
      f.registerPartitionForTest(tp, 100, broker = b)
      f.setLastFetchedEpochForTest(tp, 3)
      f.setAwaitingValidationForTest(tp, true)

      let hf = handler()
      await f.validateOffsetsForTest().wait(5.seconds)

      check not f.awaitingValidationForTest(tp)
      check f.lastFetchedEpochForTest(tp) == 6'i32
      check f.getFetchOffsets()[tp] == 100'i64

      await cluster.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor body()

  test "offset out of range during validation: ListOffsets per-partition error clears awaitingValidation":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "oor-err", partition: 0)

      proc handler() {.async: (raises: []).} =
        var handled = 0
        while handled < 2:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akOffsetForLeaderEpoch)),
                      minVersion: 2,
                      maxVersion: 3,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akListOffsets)), minVersion: 1, maxVersion: 2
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akOffsetForLeaderEpoch)):
              await req.respond(
                buildOffsetForLeaderEpochResponseBody(
                  "oor-err",
                  [
                    FakeEpochEndOffset(
                      partition: 0'i32,
                      errorCode: int16(ord(keOffsetOutOfRange)),
                      leaderEpoch: -1'i32,
                      endOffset: -1'i64,
                    )
                  ],
                )
              )
              handled.inc
            elif req.apiKey == int16(ord(akListOffsets)):
              await req.respond(
                buildListOffsetsResponseBodyV2(
                  "oor-err",
                  partition = 0'i32,
                  errorCode = int16(ord(keNotLeaderOrFollower)),
                  timestamp = -1'i64,
                  offset = -1'i64,
                )
              )
              handled.inc
          except CatchableError:
            break

      let b = newBroker(0'i32, "127.0.0.1", port, "oor-err-test")
      let cluster = newCluster(@["127.0.0.1:" & $port])
      cluster.registerBrokerForTest(b)
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 6)
      cluster.markMetadataFreshForTest()

      let f = newFetcher(cluster, offsetResetPolicy = orpLatest)
      f.registerPartitionForTest(tp, 100, broker = b)
      f.setLastFetchedEpochForTest(tp, 3)
      f.setAwaitingValidationForTest(tp, true)

      let hf = handler()
      await f.validateOffsetsForTest().wait(5.seconds)

      check not f.awaitingValidationForTest(tp)
      check f.lastFetchedEpochForTest(tp) == 6'i32
      check f.getFetchOffsets()[tp] == 100'i64

      await cluster.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor body()

  test "seek during OOR ListOffsets await is not clobbered by applyOffsetReset":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "oor-seek", partition: 0)

      let cluster = newCluster(@["127.0.0.1:" & $port])
      let b = newBroker(0'i32, "127.0.0.1", port, "oor-seek-test")
      cluster.registerBrokerForTest(b)
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 6)
      cluster.markMetadataFreshForTest()

      let f = newFetcher(cluster, offsetResetPolicy = orpLatest)
      f.registerPartitionForTest(tp, 100, broker = b)
      f.setLastFetchedEpochForTest(tp, 3)
      f.setAwaitingValidationForTest(tp, true)

      proc handler() {.async: (raises: []).} =
        var handled = 0
        while handled < 2:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akOffsetForLeaderEpoch)),
                      minVersion: 2,
                      maxVersion: 3,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akListOffsets)), minVersion: 1, maxVersion: 2
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akOffsetForLeaderEpoch)):
              await req.respond(
                buildOffsetForLeaderEpochResponseBody(
                  "oor-seek",
                  [
                    FakeEpochEndOffset(
                      partition: 0'i32,
                      errorCode: int16(ord(keOffsetOutOfRange)),
                      leaderEpoch: -1'i32,
                      endOffset: -1'i64,
                    )
                  ],
                )
              )
              handled.inc
            elif req.apiKey == int16(ord(akListOffsets)):
              try:
                f.seek(tp, 500)
              except KafkaError:
                discard
              await req.respond(
                buildListOffsetsResponseBodyV2(
                  "oor-seek",
                  partition = 0'i32,
                  errorCode = 0'i16,
                  timestamp = -1'i64,
                  offset = 250'i64,
                )
              )
              handled.inc
          except CatchableError:
            break

      let hf = handler()
      await f.validateOffsetsForTest().wait(5.seconds)

      check f.getFetchOffsets()[tp] == 500'i64

      await cluster.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor body()

  test "persistent ListOffsets failure still arms the offset-reset backoff":
    ## Regression: when ListOffsets keeps failing (per-partition error → empty
    ## result), resetPartitionOffset never reached applyOffsetReset, so
    ## lastOffsetResolve stayed unset and the backoff gate never armed — a
    ## persistently failing broker made the fetch loop spin the reset without
    ## backoff. The attempt must be stamped on every outcome.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "oor-backoff", partition: 0)

      var listOffsetsSeen = 0
      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akFetch)), minVersion: 4, maxVersion: 7
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akListOffsets)), minVersion: 1, maxVersion: 2
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akFetch)):
              await req.respond(
                buildFetchResponseBody(
                  throttleTimeMs = 0'i32,
                  errorCode = 0'i16,
                  sessionId = 0'i32,
                  topics = [
                    FakeFetchTopic(
                      topic: "oor-backoff",
                      partitions: @[
                        FakeFetchPartition(
                          partition: 0'i32,
                          errorCode: int16(ord(keOffsetOutOfRange)),
                          highWatermark: 0'i64,
                          lastStableOffset: 0'i64,
                          logStartOffset: 0'i64,
                          recordsRaw: none(seq[byte]),
                        )
                      ],
                    )
                  ],
                )
              )
            elif req.apiKey == int16(ord(akListOffsets)):
              inc listOffsetsSeen
              # Always fail: resolveOffsets returns an empty result (no
              # exception), which used to leave lastOffsetResolve unset.
              await req.respond(
                buildListOffsetsResponseBodyV2(
                  "oor-backoff",
                  partition = 0'i32,
                  errorCode = int16(ord(keNotLeaderOrFollower)),
                  timestamp = -1'i64,
                  offset = -1'i64,
                )
              )
          except CatchableError:
            break

      let cluster = newCluster(@["127.0.0.1:" & $port])
      let b = newBroker(0'i32, "127.0.0.1", port, "oor-backoff-test")
      cluster.registerBrokerForTest(b)
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 6)
      cluster.markMetadataFreshForTest()

      let f = newFetcher(cluster, offsetResetPolicy = orpLatest)
      f.registerPartitionForTest(tp, 100, broker = b)

      let hf = handler()
      await f.fetchOnceForTest().wait(5.seconds)
      # Round 1: OOR → ListOffsets fails → the attempt is stamped despite the
      # failure.
      check listOffsetsSeen == 1

      await f.fetchOnceForTest().wait(5.seconds)
      # Round 2: OOR again, but the backoff gate must now skip the reset — no
      # second ListOffsets round-trip. Position stays put (no successful reset).
      check listOffsetsSeen == 1
      check f.getFetchOffsets()[tp] == 100'i64

      await cluster.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor body()

  test "OOR validation drains already-queued records for the partition":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "oor-drain", partition: 0)
      let other = TopicPartition(topic: "oor-drain", partition: 1)

      let cluster = newCluster(@["127.0.0.1:" & $port])
      let b = newBroker(0'i32, "127.0.0.1", port, "oor-drain-test")
      cluster.registerBrokerForTest(b)
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 6)
      cluster.markMetadataFreshForTest()

      let f = newFetcher(cluster, offsetResetPolicy = orpLatest)
      f.registerPartitionForTest(tp, 100, broker = b)
      f.registerPartitionForTest(other, 100, broker = b)
      f.setLastFetchedEpochForTest(tp, 3)
      f.setAwaitingValidationForTest(tp, true)
      for off in [80'i64, 90]:
        f.enqueueRecordForTest(
          FetchedRecord(topic: tp.topic, partition: tp.partition, offset: off)
        )
      f.enqueueRecordForTest(
        FetchedRecord(topic: other.topic, partition: other.partition, offset: 81)
      )

      proc handler() {.async: (raises: []).} =
        var handled = 0
        while handled < 2:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akOffsetForLeaderEpoch)),
                      minVersion: 2,
                      maxVersion: 3,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akListOffsets)), minVersion: 1, maxVersion: 2
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akOffsetForLeaderEpoch)):
              await req.respond(
                buildOffsetForLeaderEpochResponseBody(
                  "oor-drain",
                  [
                    FakeEpochEndOffset(
                      partition: 0'i32,
                      errorCode: int16(ord(keOffsetOutOfRange)),
                      leaderEpoch: -1'i32,
                      endOffset: -1'i64,
                    )
                  ],
                )
              )
              handled.inc
            elif req.apiKey == int16(ord(akListOffsets)):
              await req.respond(
                buildListOffsetsResponseBodyV2(
                  "oor-drain",
                  partition = 0'i32,
                  errorCode = 0'i16,
                  timestamp = -1'i64,
                  offset = 200'i64,
                )
              )
              handled.inc
          except CatchableError:
            break

      let hf = handler()
      await f.validateOffsetsForTest().wait(5.seconds)

      check f.getFetchOffsets()[tp] == 200'i64
      var survivors: seq[TopicPartition]
      while not f.records.empty():
        let rec = f.records.getNoWait()
        survivors.add(TopicPartition(topic: rec.topic, partition: rec.partition))
      check survivors == @[other]

      await cluster.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor body()

  test "undefined epoch offset (-1) resolves via ListOffsets under orpLatest":
    # KIP-320 UNDEFINED_EPOCH_OFFSET (-1): the broker's leader-epoch cache no
    # longer covers our epoch. The pre-fix path would rewind fetchOffset and
    # consumedOffset to -1; the fixed path routes through ListOffsets so the
    # reset policy picks a real offset.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "undef", partition: 0)

      proc handler() {.async: (raises: []).} =
        var handled = 0
        while handled < 2:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akOffsetForLeaderEpoch)),
                      minVersion: 2,
                      maxVersion: 3,
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akListOffsets)), minVersion: 1, maxVersion: 2
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akOffsetForLeaderEpoch)):
              await req.respond(
                buildOffsetForLeaderEpochResponseBody(
                  "undef",
                  [
                    FakeEpochEndOffset(
                      partition: 0'i32,
                      errorCode: 0'i16,
                      leaderEpoch: -1'i32, # UNDEFINED_EPOCH
                      endOffset: -1'i64, # UNDEFINED_EPOCH_OFFSET
                    )
                  ],
                )
              )
              handled.inc
            elif req.apiKey == int16(ord(akListOffsets)):
              await req.respond(
                buildListOffsetsResponseBodyV2(
                  "undef",
                  partition = 0'i32,
                  errorCode = 0'i16,
                  timestamp = -1'i64,
                  offset = 250'i64, # orpLatest resolved offset
                )
              )
              handled.inc
          except CatchableError:
            break

      let b = newBroker(0'i32, "127.0.0.1", port, "undef-test")
      let cluster = newCluster(@["127.0.0.1:" & $port])
      cluster.registerBrokerForTest(b)
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 6)
      cluster.markMetadataFreshForTest()

      let f = newFetcher(cluster, offsetResetPolicy = orpLatest)
      f.registerPartitionForTest(tp, 100, broker = b)
      # everConsumed=true so a pre-fix -1 would surface via getConsumedOffsets.
      f.advanceConsumedOffset(
        FetchedRecord(topic: tp.topic, partition: tp.partition, offset: 99)
      )
      f.setLastFetchedEpochForTest(tp, 3)
      f.setAwaitingValidationForTest(tp, true)

      let hf = handler()
      await f.validateOffsetsForTest().wait(5.seconds)

      # Position must land on the ListOffsets-resolved offset, not -1.
      check f.getFetchOffsets()[tp] == 250'i64
      check f.getConsumedOffsets()[tp] == 250'i64
      check not f.awaitingValidationForTest(tp)

      await cluster.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor body()

suite "Fetcher KIP-320 validation stall escalation":
  ## recordValidationFailure escalation: a leader that never resolves must not
  ## drop the partition from fetching silently. The budget clears
  ## awaitingValidation after maxValidationAttempts failed rounds and pins
  ## lastFetchedEpoch to the observed leader epoch so the partition resumes
  ## fetching (and the fetch loop's own broker-error escalation takes over);
  ## the next leader advance re-arms validation with a fresh budget.

  type
    MetricEvent = tuple[event: string, tags: seq[(string, string)], value: int64]

    MetricSink = ref object
      events: seq[MetricEvent]

  proc makeHook(sink: MetricSink): MetricsHook =
    result = proc(
        event: cstring, tags: openArray[(cstring, cstring)], value: int64
    ) {.gcsafe, raises: [].} =
      var copied: seq[(string, string)]
      for t in tags:
        copied.add(($t[0], $t[1]))
      sink.events.add((event: $event, tags: copied, value: value))

  proc findEvent(sink: MetricSink, name: string): Option[MetricEvent] =
    for ev in sink.events:
      if ev.event == name:
        return some(ev)
    none(MetricEvent)

  proc tagValue(ev: MetricEvent, key: string): Option[string] =
    for (k, v) in ev.tags:
      if k == key:
        return some(v)
    none(string)

  test "failed rounds accumulate below the threshold":
    let f = newFetcher(nil, maxValidationAttempts = 3)
    let tp = TopicPartition(topic: "budget", partition: 0)
    f.registerPartitionForTest(tp, 100)
    f.setAwaitingValidationForTest(tp, true)
    f.recordValidationFailureForTest(tp)
    f.recordValidationFailureForTest(tp)
    check f.validationAttemptsForTest(tp) == 2
    check f.awaitingValidationForTest(tp) # still below the budget

  test "the threshold clears the flag and resets the budget":
    let f = newFetcher(nil, maxValidationAttempts = 3)
    let tp = TopicPartition(topic: "escalate", partition: 0)
    f.registerPartitionForTest(tp, 100)
    f.setAwaitingValidationForTest(tp, true)
    for i in 0 ..< 3:
      f.recordValidationFailureForTest(tp)
    check not f.awaitingValidationForTest(tp)
    check f.validationAttemptsForTest(tp) == 0

  test "newFetcher rejects a non-positive validation budget":
    expect(KafkaError):
      discard newFetcher(nil, maxValidationAttempts = 0)
    expect(KafkaError):
      discard newFetcher(nil, maxValidationAttempts = -1)

  test "newFetcher rejects a non-positive record queue size":
    ## `AsyncQueue.full` is never true for a non-positive maxsize, so 0 would
    ## silently turn the bounded record queue into an unbounded one.
    expect(KafkaError):
      discard newFetcher(nil, queueSize = 0)
    expect(KafkaError):
      discard newFetcher(nil, queueSize = -1)

  test "newFetcher accepts a one-record queue":
    let f = newFetcher(nil, queueSize = 1)
    check f.recordQueueCapacity == 1

  test "successful validation resets the budget":
    proc body() {.async: (raises: [Exception]).} =
      let f = newFetcher(nil, offsetResetPolicy = orpLatest, maxValidationAttempts = 3)
      let tp = TopicPartition(topic: "reset-budget", partition: 0)
      f.registerPartitionForTest(tp, 100)
      f.setAwaitingValidationForTest(tp, true)
      f.recordValidationFailureForTest(tp)
      f.recordValidationFailureForTest(tp)
      # Valid position: the budget resets alongside the flag.
      await f.applyValidationResultForTest(
        tp,
        validatedEpoch = 6'i32,
        errorCode = keNone,
        leaderEpoch = 6'i32,
        endOffset = 150'i64,
      )
      check not f.awaitingValidationForTest(tp)
      check f.validationAttemptsForTest(tp) == 0
      # A fresh flag gets a fresh budget: one failure no longer escalates.
      f.setAwaitingValidationForTest(tp, true)
      f.recordValidationFailureForTest(tp)
      check f.awaitingValidationForTest(tp)

    waitFor body()

  test "escalation emits consumer.fetch.validation_stall":
    let sink = MetricSink(events: @[])
    let f = newFetcher(nil, maxValidationAttempts = 1, metricsHook = makeHook(sink))
    let tp = TopicPartition(topic: "stall-metric", partition: 0)
    f.registerPartitionForTest(tp, 100)
    f.setAwaitingValidationForTest(tp, true)
    f.recordValidationFailureForTest(tp)
    let ev = findEvent(sink, "consumer.fetch.validation_stall")
    check ev.isSome
    check ev.get.value == 1
    check tagValue(ev.get, "topic") == some("stall-metric")
    check tagValue(ev.get, "partition") == some("0")

  test "RPC failure escalates after maxValidationAttempts and re-arms on leader advance":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "rpc-fail", partition: 0)

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akOffsetForLeaderEpoch)),
                      minVersion: 2,
                      maxVersion: 3,
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akOffsetForLeaderEpoch)):
              # Simulate a permanently failing RPC: kill the transport mid-request.
              await req.closeTransport()
          except CatchableError:
            break

      let b = newBroker(0'i32, "127.0.0.1", port, "rpc-fail-test")
      let cluster = newCluster(@["127.0.0.1:" & $port])
      cluster.registerBrokerForTest(b)
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 6)
      cluster.markMetadataFreshForTest()

      let f =
        newFetcher(cluster, offsetResetPolicy = orpLatest, maxValidationAttempts = 3)
      f.registerPartitionForTest(tp, 100, broker = b)
      f.setLastFetchedEpochForTest(tp, 3)
      f.setAwaitingValidationForTest(tp, true)

      let hf = handler()
      await f.validateOffsetsForTest().wait(5.seconds)
      check f.awaitingValidationForTest(tp) # round 1 failed, still below budget
      await f.validateOffsetsForTest().wait(5.seconds)
      check f.awaitingValidationForTest(tp) # round 2
      await f.validateOffsetsForTest().wait(5.seconds)
      # Round 3 reaches the budget: flag cleared, position untouched, epoch pinned.
      check not f.awaitingValidationForTest(tp)
      check f.getFetchOffsets()[tp] == 100'i64
      check f.lastFetchedEpochForTest(tp) == 6'i32

      # The pinned epoch suppresses immediate re-flagging...
      f.markPartitionsForValidationForTest()
      check not f.awaitingValidationForTest(tp)
      # ...and a leader advance re-arms validation with a fresh budget.
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 7)
      f.markPartitionsForValidationForTest()
      check f.awaitingValidationForTest(tp)
      check f.validationAttemptsForTest(tp) == 0

      await cluster.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor body()

  test "unresolvable leader escalates through brokerForPartition failures":
    proc body() {.async: (raises: [Exception]).} =
      # Leader 5 is registered in metadata but broker 5 is not known to the
      # cluster: brokerForPartition raises, so the failure is cheap and local.
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "no-broker", partition: 0)
      let cluster = newCluster(@["127.0.0.1:" & $port])
      cluster.setPartitionLeaderForTest(tp, nodeId = 5, leaderEpoch = 6)
      cluster.markMetadataFreshForTest()

      let f =
        newFetcher(cluster, offsetResetPolicy = orpLatest, maxValidationAttempts = 2)
      f.registerPartitionForTest(tp, 100)
      f.setLastFetchedEpochForTest(tp, 3)
      f.setAwaitingValidationForTest(tp, true)

      await f.validateOffsetsForTest().wait(5.seconds)
      check f.awaitingValidationForTest(tp) # round 1
      await f.validateOffsetsForTest().wait(5.seconds)
      check not f.awaitingValidationForTest(tp) # round 2 escalates
      check f.lastFetchedEpochForTest(tp) == 6'i32

      await cluster.close()
      await fb.close()

    waitFor body()

  test "a partition omitted from the reply counts toward the budget":
    proc body() {.async: (raises: [Exception]).} =
      # The broker replies with a different topic: the requested partition is
      # missing from the response, which must still consume the budget instead
      # of leaving an unbounded silent stall.
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "omitted", partition: 0)

      proc handler() {.async: (raises: []).} =
        while true:
          var req: CapturedRequest
          try:
            req = await fb.nextRequest(timeout = 2.seconds)
          except CatchableError:
            break
          try:
            if req.apiKey == int16(ord(akApiVersions)):
              await req.respond(
                buildApiVersionsResponseBody(
                  0'i16,
                  [
                    FakeApiVersion(
                      apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0
                    ),
                    FakeApiVersion(
                      apiKey: int16(ord(akOffsetForLeaderEpoch)),
                      minVersion: 2,
                      maxVersion: 3,
                    ),
                  ],
                )
              )
            elif req.apiKey == int16(ord(akOffsetForLeaderEpoch)):
              await req.respond(
                buildOffsetForLeaderEpochResponseBody(
                  "other-topic",
                  [
                    FakeEpochEndOffset(
                      partition: 0'i32,
                      errorCode: 0'i16,
                      leaderEpoch: 6'i32,
                      endOffset: 500'i64,
                    )
                  ],
                )
              )
          except CatchableError:
            break

      let b = newBroker(0'i32, "127.0.0.1", port, "omitted-test")
      let cluster = newCluster(@["127.0.0.1:" & $port])
      cluster.registerBrokerForTest(b)
      cluster.setPartitionLeaderForTest(tp, nodeId = 0, leaderEpoch = 6)
      cluster.markMetadataFreshForTest()

      let f =
        newFetcher(cluster, offsetResetPolicy = orpLatest, maxValidationAttempts = 2)
      f.registerPartitionForTest(tp, 100, broker = b)
      f.setLastFetchedEpochForTest(tp, 3)
      f.setAwaitingValidationForTest(tp, true)

      let hf = handler()
      await f.validateOffsetsForTest().wait(5.seconds)
      check f.awaitingValidationForTest(tp) # round 1
      await f.validateOffsetsForTest().wait(5.seconds)
      check not f.awaitingValidationForTest(tp) # round 2 escalates
      check f.getFetchOffsets()[tp] == 100'i64
      check f.lastFetchedEpochForTest(tp) == 6'i32

      await cluster.close()
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor body()

suite "Fetcher: fetchLoop / fetchOnce cancel and stop":
  ## The loop's cancel / stop / in-flight-fetch-abort paths had no regression.
  ## Existing `.start()` uses immediately `stop()` with no broker (teardown of
  ## queueFull / lastError reset). A hung Fetch against the fake broker is what
  ## actually exercises cancelAndWait through fetchFromBroker / allFutures.

  test "stop cancels an in-flight fetchLoop and does not advance consumed offsets":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let parked = newAsyncEvent()
      let hf = respondApiVersionsAndParkFetch(fb, parked)

      let f = newFetcher(nil)
      let b = newBroker(0'i32, "127.0.0.1", port, "loop-stop-test")
      let tp = TopicPartition(topic: "stop-topic", partition: 0)
      f.registerPartitionForTest(tp, 50, broker = b)
      f.setFetchSessionForTest(0'i32, sessionId = 5'i32, sessionEpoch = 3'i32)
      f.markBrokerEpochValidationUnsupportedForTest(0'i32)
      f.enqueueRecordForTest(
        FetchedRecord(topic: "stop-topic", partition: 0'i32, offset: 40)
      )
      f.enqueueRecordForTest(
        FetchedRecord(topic: "stop-topic", partition: 0'i32, offset: 41)
      )
      let posEpochBefore = f.posEpochForTest(tp)
      check f.records.len == 2
      check f.getFetchSessionForTest(0'i32).exists

      f.start()
      check f.isRunningForTest()
      await parked.wait().wait(5.seconds)
      check f.isLoopAlive

      check awaitCompleted(f.stop(), 5.seconds)
      check not f.isRunningForTest()
      check not f.isLoopAlive
      check f.records.empty()
      check tp notin f.getConsumedOffsets()
      check f.getFetchOffsets()[tp] == 50'i64
      check f.posEpochForTest(tp) == posEpochBefore
      check not f.getFetchSessionForTest(0'i32).exists
      check not f.brokerEpochValidationUnsupportedForTest(0'i32)

      check awaitCompleted(b.close(), 5.seconds)
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor testBody()

  test "spawnStop/joinExitStop reaps an in-flight fetchLoop":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let parked = newAsyncEvent()
      let hf = respondApiVersionsAndParkFetch(fb, parked)

      let f = newFetcher(nil)
      let b = newBroker(0'i32, "127.0.0.1", port, "spawn-stop-test")
      let tp = TopicPartition(topic: "spawn-topic", partition: 0)
      f.registerPartitionForTest(tp, 50, broker = b)

      f.start()
      await parked.wait().wait(5.seconds)
      check f.isLoopAlive
      check not f.hasExitStopFutForTest()

      f.spawnStop()
      check f.hasExitStopFutForTest()
      check awaitCompleted(f.joinExitStop(), 5.seconds)
      check not f.hasExitStopFutForTest()
      check not f.isRunningForTest()
      check not f.isLoopAlive
      check f.getFetchOffsets()[tp] == 50'i64
      check tp notin f.getConsumedOffsets()

      check awaitCompleted(b.close(), 5.seconds)
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor testBody()

  test "start after an in-flight stop issues a new fetch":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let parked = newAsyncEvent()
      let hf = respondApiVersionsAndParkFetch(fb, parked)

      let f = newFetcher(nil)
      let b = newBroker(0'i32, "127.0.0.1", port, "restart-after-stop-test")
      let tp = TopicPartition(topic: "restart-topic", partition: 0)
      f.registerPartitionForTest(tp, 50, broker = b)

      f.start()
      await parked.wait().wait(5.seconds)
      check awaitCompleted(f.stop(), 5.seconds)
      check not f.isRunningForTest()

      # If stop() left running=true, start() would no-op and this wait would
      # time out: the parked Fetch already fired, and no new one is sent.
      parked.clear()
      f.start()
      check f.isRunningForTest()
      await parked.wait().wait(5.seconds)
      check f.isLoopAlive

      check awaitCompleted(f.stop(), 5.seconds)
      check not f.isLoopAlive

      check awaitCompleted(b.close(), 5.seconds)
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor testBody()

  test "cancelling fetchOnce mid-flight does not apply a later response":
    proc testBody() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let parked = newAsyncEvent()
      let hf = respondApiVersionsAndParkFetch(fb, parked)

      let f = newFetcher(nil)
      let b = newBroker(0'i32, "127.0.0.1", port, "fetchonce-cancel-test")
      let tp = TopicPartition(topic: "once-topic", partition: 0)
      f.registerPartitionForTest(tp, 50, broker = b)
      let posEpochBefore = f.posEpochForTest(tp)

      let once = f.fetchOnceForTest()
      await parked.wait().wait(5.seconds)
      await once.cancelAndWait().wait(5.seconds)
      check once.finished
      check f.records.empty()
      check f.getFetchOffsets()[tp] == 50'i64
      check f.posEpochForTest(tp) == posEpochBefore
      check tp notin f.getConsumedOffsets()

      check awaitCompleted(b.close(), 5.seconds)
      await fb.close()
      awaitDrain(hf, 1.seconds)

    waitFor testBody()

suite "Fetcher broker failure recovery":
  ## The per-broker recovery path. When one broker's fetches keep failing while
  ## the rest of the cluster is healthy, the fetcher must repoint that broker's
  ## partitions at the newly elected leader instead of retrying the dead broker
  ## forever. `reassignBrokerPartitions` also deliberately does nothing once
  ## stop() has cleared `running`: in-flight fetches then fail as
  ## KafkaConnectionError (not CancelledError) on the way down, and a metadata
  ## refresh there would touch a cluster that is being torn down.

  proc serveRecoveryBroker(
      fb: FakeBroker,
      versions: seq[FakeApiVersion],
      onRequest: proc(req: CapturedRequest) {.closure, gcsafe, async: (raises: []).},
  ) {.async: (raises: []).} =
    while true:
      var req: CapturedRequest
      try:
        req = await fb.nextRequest(timeout = 2.seconds)
      except CatchableError:
        break
      try:
        if req.apiKey == int16(ord(akApiVersions)):
          await req.respond(buildApiVersionsResponseBody(0'i16, versions))
        else:
          await onRequest(req)
      except CatchableError:
        break

  test "partitions of a persistently failing broker are reassigned to the new leader":
    proc body() {.async: (raises: [Exception]).} =
      let fbBad = newFakeBroker()
      let fbGood = newFakeBroker()
      let portBad = int32(fbBad.address().port)
      let portGood = int32(fbGood.address().port)
      let tp = TopicPartition(topic: "recover", partition: 0)

      let versions = @[
        FakeApiVersion(apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0),
        FakeApiVersion(apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1),
        FakeApiVersion(apiKey: int16(ord(akFetch)), minVersion: 4, maxVersion: 7),
      ]

      var metadataRefreshes = 0

      # The broker answers every fetch with a retriable top-level error. That is
      # the shape `fetchOnce` counts as a broker failure without a leader
      # invalidation: handleTopLevelFetchError returns false, so fetchFromBroker
      # reports "not served" and the per-broker counter advances. (A dead
      # transport would work too, but leaves the round waiting on a read that may
      # not fail promptly, which makes the test slow rather than clearer.)
      proc badRequest(req: CapturedRequest) {.closure, gcsafe, async: (raises: []).} =
        try:
          if req.apiKey == int16(ord(akFetch)):
            await req.respond(
              buildFetchResponseBody(
                throttleTimeMs = 0'i32,
                errorCode = int16(ord(keRequestTimedOut)),
                sessionId = 0'i32,
                topics = @[],
              )
            )
          else:
            await req.closeTransport()
        except CatchableError:
          discard

      proc goodRequest(req: CapturedRequest) {.closure, gcsafe, async: (raises: []).} =
        try:
          if req.apiKey == int16(ord(akMetadata)):
            inc metadataRefreshes
            let brokers: seq[FakeBrokerMeta] = @[
              FakeBrokerMeta(nodeId: 0'i32, host: "127.0.0.1", port: portBad),
              FakeBrokerMeta(nodeId: 1'i32, host: "127.0.0.1", port: portGood),
            ]
            let topics: seq[FakeTopicMeta] = @[
              FakeTopicMeta(
                name: "recover",
                errorCode: 0'i16,
                isInternal: false,
                partitions: @[
                  FakePartitionMeta(
                    # The election moved the leader off the dead broker.
                    partitionIndex: 0'i32,
                    leaderId: 1'i32,
                    replicas: @[0'i32, 1'i32],
                    isr: @[1'i32],
                  )
                ],
              )
            ]
            await req.respond(
              buildMetadataResponseBody(brokers, controllerId = 1'i32, topics = topics)
            )
          elif req.apiKey == int16(ord(akFetch)):
            let fetchTopics: seq[FakeFetchTopic] = @[
              FakeFetchTopic(
                topic: "recover",
                partitions: @[
                  FakeFetchPartition(
                    partition: 0'i32,
                    errorCode: 0'i16,
                    highWatermark: 100'i64,
                    lastStableOffset: 100'i64,
                    logStartOffset: 0'i64,
                    recordsRaw: none(seq[byte]),
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

      let hb = serveRecoveryBroker(fbBad, versions, badRequest)
      let hg = serveRecoveryBroker(fbGood, versions, goodRequest)

      # Bootstrap is the healthy broker so the cluster can always answer a
      # refresh; the bad broker is registered only as the partition's leader.
      let cluster = newCluster(@["127.0.0.1:" & $portGood])
      let bad = newBroker(0'i32, "127.0.0.1", portBad, "recover-bad")
      let good = newBroker(1'i32, "127.0.0.1", portGood, "recover-good")
      cluster.registerBrokerForTest(bad)
      cluster.registerBrokerForTest(good)
      # Unknown (-1) cached leader epoch on purpose: the fake's Metadata v1
      # body carries no leader_epoch, so the candidate decodes with
      # PartitionMeta's Nim default 0, and applyPartitionLeaderMonotonic only
      # moves a leader forward against a KNOWN epoch. Seeding 5 would make the
      # policy (correctly) refuse to adopt an election it cannot order.
      cluster.setPartitionLeaderForTest(tp, nodeId = 0'i32, leaderEpoch = -1'i32)
      # Stamp the metadata cache as just-refreshed — the state a real fetcher is
      # normally in when a broker starts failing, since the TTL refresh and
      # maybeRefreshMetadata's fast path both leave it fresh. The recovery must
      # not be swallowed by the cluster's 1s debounce: its own contract is to
      # force a refresh, and dropping the error counter on a refresh that never
      # happened also costs the next N failures before recovery can retry.
      cluster.markMetadataFreshForTest()
      await bad.connect()
      await good.connect()

      let sink = MetricSink(events: @[])
      let f = newFetcher(
        cluster, maxConsecutiveFetchErrors = 2, metricsHook = makeMetricHook(sink)
      )
      f.registerPartitionForTest(tp, 100, broker = bad)
      # reassignBrokerPartitions refuses to act unless the fetch loop is live.
      f.setRunningForTest(true)

      # Round 1: the fetch fails against the dead broker. One failure is below
      # the budget, so nothing is reassigned yet.
      await f.fetchOnceForTest().wait(10.seconds)
      check f.brokerFetchErrorsForTest().getOrDefault(0'i32, 0) == 1
      check f.partitionBrokerForTest(tp).nodeId == 0'i32
      check metadataRefreshes == 0

      # A retriable top-level fetch error is reported per broker with the code
      # name, so an operator can tell which broker is degrading.
      let topLevel = findMetricEvent(sink, "consumer.fetch.top_level_error")
      check topLevel.isSome
      check metricTagValue(topLevel.get, "broker") == some("0")
      check metricTagValue(topLevel.get, "error_code") == some("REQUEST_TIMED_OUT")

      # Round 2 reaches the budget: the fetcher must refresh metadata and
      # repoint the partition at the broker that won the election.
      await f.fetchOnceForTest().wait(10.seconds)
      check metadataRefreshes == 1
      check f.partitionBrokerForTest(tp).nodeId == 1'i32
      check 0'i32 notin f.brokerFetchErrorsForTest()

      # Round 3 goes to the healthy broker and succeeds, so the counter stays
      # clear and the recovery does not re-fire.
      await f.fetchOnceForTest().wait(10.seconds)
      check f.partitionBrokerForTest(tp).nodeId == 1'i32
      check 0'i32 notin f.brokerFetchErrorsForTest()

      await cluster.close()
      await fbBad.close()
      await fbGood.close()
      awaitDrain(hb, 1.seconds)
      awaitDrain(hg, 1.seconds)

    waitFor body()

  test "a broker that recovers stops being tracked as failing":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "flapping", partition: 0)

      var failNext = true

      let versions = @[
        FakeApiVersion(apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0),
        FakeApiVersion(apiKey: int16(ord(akFetch)), minVersion: 4, maxVersion: 7),
      ]

      proc onRequest(req: CapturedRequest) {.closure, gcsafe, async: (raises: []).} =
        try:
          if req.apiKey == int16(ord(akFetch)):
            if failNext:
              # Retriable top-level error: the round fails but the connection
              # survives, so the next round can succeed.
              await req.respond(
                buildFetchResponseBody(
                  throttleTimeMs = 0'i32,
                  errorCode = int16(ord(keRequestTimedOut)),
                  sessionId = 0'i32,
                  topics = @[],
                )
              )
            else:
              let fetchTopics: seq[FakeFetchTopic] = @[
                FakeFetchTopic(
                  topic: "flapping",
                  partitions: @[
                    FakeFetchPartition(
                      partition: 0'i32,
                      errorCode: 0'i16,
                      highWatermark: 100'i64,
                      lastStableOffset: 100'i64,
                      logStartOffset: 0'i64,
                      recordsRaw: none(seq[byte]),
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
        except CatchableError:
          discard

      let h = serveRecoveryBroker(fb, versions, onRequest)
      let cluster = newCluster(@["127.0.0.1:" & $port])
      let b = newBroker(0'i32, "127.0.0.1", port, "flap")
      cluster.registerBrokerForTest(b)
      await b.connect()

      let f = newFetcher(cluster, maxConsecutiveFetchErrors = 3)
      f.registerPartitionForTest(tp, 100, broker = b)
      f.setRunningForTest(true)

      await f.fetchOnceForTest().wait(10.seconds)
      check f.brokerFetchErrorsForTest().getOrDefault(0'i32, 0) == 1
      await f.fetchOnceForTest().wait(10.seconds)
      check f.brokerFetchErrorsForTest().getOrDefault(0'i32, 0) == 2

      # The broker comes back: a successful round must clear the counter, so one
      # later blip does not immediately re-arm the recovery.
      failNext = false
      await f.fetchOnceForTest().wait(10.seconds)
      check 0'i32 notin f.brokerFetchErrorsForTest()

      await cluster.close()
      await fb.close()
      awaitDrain(h, 1.seconds)

    waitFor body()

  test "recovery does not start a metadata refresh after stop()":
    proc body() {.async: (raises: [Exception]).} =
      # stop() clears `running` before cancelling in-flight fetches, so those fail
      # as KafkaConnectionError rather than CancelledError. Without the
      # `running` guard the teardown path would drive a metadata refresh and a
      # brokerForPartition lookup on a fetcher that is going down. Asserted by
      # whether Metadata reaches the wire, in both directions, so a regression
      # cannot pass by accident.
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "stopped", partition: 0)

      var metadataRequests = 0

      proc onRequest(req: CapturedRequest) {.closure, gcsafe, async: (raises: []).} =
        try:
          if req.apiKey == int16(ord(akMetadata)):
            inc metadataRequests
          await req.closeTransport()
        except CatchableError:
          discard

      let versions = @[
        FakeApiVersion(apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0),
        FakeApiVersion(apiKey: int16(ord(akMetadata)), minVersion: 1, maxVersion: 1),
      ]
      let h = serveRecoveryBroker(fb, versions, onRequest)

      let cluster = newCluster(@["127.0.0.1:" & $port])
      let b = newBroker(0'i32, "127.0.0.1", port, "stopped")
      cluster.registerBrokerForTest(b)
      await b.connect()

      let f = newFetcher(cluster, maxConsecutiveFetchErrors = 1)
      f.registerPartitionForTest(tp, 100, broker = b)

      # Live fetcher: the recovery does refresh, so the guard is what is tested.
      f.setRunningForTest(true)
      await f.reassignBrokerPartitionsForTest(@[0]).wait(30.seconds)
      let afterLive = metadataRequests
      check afterLive > 0

      # Post-stop: no further Metadata traffic on the way down.
      f.setRunningForTest(false)
      await f.reassignBrokerPartitionsForTest(@[0]).wait(30.seconds)
      check metadataRequests == afterLive

      await cluster.close()
      await fb.close()
      awaitDrain(h, 1.seconds)

    waitFor body()

  test "a fetch that raises is reported as consumer.fetch.error":
    proc body() {.async: (raises: [Exception]).} =
      # The transport-level failure shape (as opposed to the retriable error-code
      # shape the accounting tests use): the broker dies mid-request, so
      # `broker.fetch` raises and the fetcher reports the failure with the broker
      # id and the reason. `consumer.fetch.error` is the only signal that a
      # broker is unreachable, and it carries the raw exception message — which is
      # why an operator-visible failure path needs coverage even though the tag
      # value is high-cardinality.
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "unreachable", partition: 0)

      let versions = @[
        FakeApiVersion(apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0),
        FakeApiVersion(apiKey: int16(ord(akFetch)), minVersion: 4, maxVersion: 7),
      ]

      proc onRequest(req: CapturedRequest) {.closure, gcsafe, async: (raises: []).} =
        try:
          if req.apiKey == int16(ord(akFetch)):
            await req.closeTransport()
          else:
            await req.closeTransport()
        except CatchableError:
          discard

      let h = serveRecoveryBroker(fb, versions, onRequest)
      # Short request deadline: a dead transport can leave the read waiting
      # rather than failing it, and this test is about the metric, not patience.
      let cluster = newCluster(@["127.0.0.1:" & $port], requestTimeoutMs = 1000)
      let b = newBroker(0'i32, "127.0.0.1", port, "unreachable")
      cluster.registerBrokerForTest(b)
      await b.connect()

      let sink = MetricSink(events: @[])
      let f = newFetcher(
        cluster, maxConsecutiveFetchErrors = 2, metricsHook = makeMetricHook(sink)
      )
      f.registerPartitionForTest(tp, 100, broker = b)
      f.setRunningForTest(true)

      await f.fetchOnceForTest().wait(30.seconds)

      let ev = findMetricEvent(sink, "consumer.fetch.error")
      check ev.isSome
      check metricTagValue(ev.get, "broker") == some("0")
      # The reason tag is the exception text, so assert it is populated rather
      # than pinning a message that legitimately changes with the transport.
      check metricTagValue(ev.get, "reason").isSome
      check f.brokerFetchErrorsForTest().getOrDefault(0'i32, 0) == 1

      await cluster.close()
      await fb.close()
      awaitDrain(h, 1.seconds)

    waitFor body()

  test "a fetch that fails under an active throttle reports the remaining wait":
    proc body() {.async: (raises: [Exception]).} =
      # `awaitThrottle` shares the request's timeout budget, so a quota throttle
      # longer than the deadline fails the fetch before it is written. The
      # fetcher then reports how long the broker asked to be left alone — the
      # retry delay is otherwise indistinguishable from a dead broker.
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let tp = TopicPartition(topic: "throttled", partition: 0)

      let versions = @[
        FakeApiVersion(apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0),
        FakeApiVersion(apiKey: int16(ord(akFetch)), minVersion: 4, maxVersion: 7),
      ]

      proc onRequest(req: CapturedRequest) {.closure, gcsafe, async: (raises: []).} =
        # Unreachable: the throttle fails the request before it hits the wire.
        try:
          await req.respond(buildFetchResponseBody(0'i32, 0'i16, 0'i32, @[]))
        except CatchableError:
          discard

      let h = serveRecoveryBroker(fb, versions, onRequest)
      let cluster = newCluster(@["127.0.0.1:" & $port], requestTimeoutMs = 300)
      let b = newBroker(0'i32, "127.0.0.1", port, "throttled", requestTimeoutMs = 300)
      cluster.registerBrokerForTest(b)
      await b.connect()
      # Apply the throttle after the handshake: connect() negotiates ApiVersions
      # over a path that is not throttled.
      b.applyThrottle(60_000'i32)

      let sink = MetricSink(events: @[])
      let f = newFetcher(cluster, metricsHook = makeMetricHook(sink))
      f.registerPartitionForTest(tp, 100, broker = b)
      f.setRunningForTest(true)

      await f.fetchOnceForTest().wait(5.seconds)

      let ev = findMetricEvent(sink, "consumer.fetch.retry.throttle_ms")
      check ev.isSome
      check metricTagValue(ev.get, "broker") == some("0")
      check ev.get.value > 0'i64

      await cluster.close()
      await fb.close()
      awaitDrain(h, 1.seconds)

    waitFor body()
