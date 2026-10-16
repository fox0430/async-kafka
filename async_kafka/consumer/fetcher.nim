import std/[tables, sets, options, algorithm]

import ../[async_backend, errors, cluster]
import ../broker as brokerMod
import ../protocol/[messages, primitives, crc32c]
import ../internal/[backoff, cancel, metrics, retry, safe_logging]

type
  OffsetResetPolicy* = enum
    ## Where to restart a partition with no committed (or an out-of-range)
    ## offset: the log end, the log start, or raise instead of guessing.
    orpLatest
    orpEarliest
    orpNone

  FetchedRecord* = object
    ## One consumed record: payload plus its topic/partition/offset position.
    ## The private stamps below are set by the fetch pipeline; applications
    ## receive them but never construct them.
    topic*: string
    partition*: int32
    offset*: int64
    timestamp*: int64
    key*: Option[seq[byte]]
    value*: Option[seq[byte]]
    headers*: seq[RecordHeader]
    generation: int32
      ## The fetcher's `assignmentGeneration` at enqueue time; the poll path drops
      ## records whose tag predates the current group generationId (pre-rebalance
      ## leftovers). Defaults to -1 (no generation) for test-built records or
      ## before any group joined.
    posEpoch: int64
      ## Enqueue snapshot of the partition's `posEpoch`, which every reposition
      ## (seek / offset reset / KIP-320 truncation) bumps: `advanceConsumedOffset`
      ## rejects a mismatching stamp, so a record polled before the reposition cannot
      ## clobber the realigned `consumedOffset`. 0 = the initial epoch (test records).
    leaderEpoch: int32
      ## The batch's `partitionLeaderEpoch`, propagated into
      ## `PartitionState.consumedLeaderEpoch` and sent as OffsetCommit's
      ## `committedLeaderEpoch` so KIP-320 sees the record's true epoch, not
      ## cluster-metadata's current leader. -1 = unknown.

  PartitionState* = object
    tp*: TopicPartition
    fetchOffset*: int64
    consumedOffset*: int64
    consumedLeaderEpoch*: int32
      ## Leader epoch of the newest consumed record — OffsetCommit's
      ## `committedLeaderEpoch`. -1 until an advance sets it, or after a
      ## seek / OOR reset / truncation rewind invalidates it (commit then falls
      ## back to metadata).
    everConsumed*: bool
      ## `true` once `consumedOffset` reflects a user observation
      ## (advanceConsumedOffset) or explicit seek. Until then it is just a copy
      ## of the initial fetch offset and must not be committed: that would write
      ## a future offset (orpLatest) or revert another consumer's progress.
    broker*: Broker
    lastOffsetResolve*: Moment
    paused*: bool ## fetchOnce skips this partition until resume().
    posEpoch*: int64
      ## Bumped by every explicit reposition (seek, offset reset). Fetches
      ## snapshot it and skip partitions whose epoch moved in flight: that
      ## response was built against a stale position (pre-seek records, plus
      ## computeNextFetchOffset's never-backward clamp undoing a seek-backward).
    lastFetchedEpoch*: int32
      ## Leader epoch our fetch position is known valid under: the
      ## partitionLeaderEpoch of the newest batch consumed, or the epoch a
      ## successful OffsetForLeaderEpoch confirmed. -1 = unknown (never consumed /
      ## pre-KIP-320). Sent as OffsetForLeaderEpoch.leaderEpoch.
    awaitingValidation*: bool
      ## A leader change advanced the cached leaderEpoch past lastFetchedEpoch, so
      ## this partition needs an OffsetForLeaderEpoch check before the next fetch.
      ## While true, fetchOnce excludes it from fetching (see validateOffsets).
    validationAttempts*: int
      ## Consecutive failed validation rounds while awaitingValidation (see
      ## recordValidationFailure). A permanently unreachable leader must not stall
      ## the partition silently, so after maxValidationAttempts the flag is cleared
      ## and fetching resumes. Reset on every validation success / reposition, so a
      ## fresh flag gets a fresh budget.

  RebalanceCheck* = proc(): bool {.gcsafe, raises: [].}

  FetchSession = object
    ## Per-broker KIP-227 fetch-session state. `sessionId == 0` marks a new
    ## session; once the broker assigns an id we reuse it. `sessionEpoch` pairs 0
    ## with sessionId=0 ("create"), becomes 1 on the first non-zero sessionId,
    ## then increments per successful response. We never send `forgottenTopicsData`,
    ## so every request carries the full active list — and since omission is not
    ## removal under KIP-227, the only way to stop the broker serving a partition
    ## mid-session is to abandon the session. So `pause()` drops the affected
    ## broker session and `stop()` clears every session.
    sessionId: int32
    sessionEpoch: int32

  Fetcher* = ref object
    cluster: Cluster
    partitions*: seq[PartitionState]
    partitionIndex*: Table[TopicPartition, int]
    maxWaitMs*: int32
    minBytes*: int32
    maxBytes*: int32
    maxPartitionBytes*: int32
    offsetResetPolicy*: OffsetResetPolicy
    isolationLevel*: IsolationLevel
    records*: AsyncQueue[FetchedRecord]
    recordQueueMaxSize: int
      ## Mirrors newAsyncQueue's capacity; chronos doesn't expose its `maxsize`.
    running: bool
    fetchLoopFut: Future[void]
    exitStopFut: Future[void].Raising([])
      ## The stop() dispatched off the heartbeat permanent-error hook via
      ## spawnStop(); retained so consumer.close() can await it (joinExitStop)
      ## instead of leaking an untracked coroutine. Raising([]) matches stop().
    rebalanceCheck: RebalanceCheck
    queueFull*: bool
    lastError*: ref KafkaError
    consecutiveFetchErrors*: int
    maxConsecutiveFetchErrors*: int
    lastRestartAttempt*: Moment
    consecutiveRestartAttempts*: int
      ## tryRestartFetcher() recovery attempts since the last successful fetch
      ## (reset to 0 when fetchOnce sees a partition succeed). Consumer.poll
      ## compares it with maxFetcherRestartAttempts to escalate a persistently
      ## failing fetcher into a poll-side KafkaError instead of looping forever.
    offsetResolveBackoffMs*: int32
    retriableFetchAttempts: int
      ## Consecutive fetch rounds whose response carried a retriable per-partition
      ## error. Scales the fetch retry backoff and resets on a round without one.
    maxValidationAttempts: int
      ## Consecutive failed OffsetForLeaderEpoch rounds (no broker resolvable,
      ## RPC failing, or a stale-leader reply) after which validation is abandoned
      ## so fetching resumes instead of stalling forever (see
      ## recordValidationFailure). Must be positive; re-armed on the next leader
      ## epoch change, which restarts the budget.
    maxTotalDecompressedBytes*: int
      ## Cumulative decompressed-byte budget for one fetch response, shared across its
      ## partitions. Must be positive; ConsumerConfig maps 0 to the default first.
    brokerFetchErrors: Table[int32, int]
    fetchSessions: Table[int32, FetchSession]
    pauseEpochs: Table[int32, int64]
      ## Per-broker counter bumped whenever `pause()` invalidates a partition on
      ## that broker. `fetchFromBroker` snapshots it before sending; if it moved by
      ## response time, pause() ran mid-flight and the response must not
      ## re-establish the KIP-227 session pause() cleared, or the broker-side
      ## session would keep serving the paused partition (omission ≠ removal).
    epochValidationUnsupportedBrokers: HashSet[int32]
      ## Brokers that returned keUnsupportedVersion for OffsetForLeaderEpoch.
      ## KIP-320 support is broker-lifetime-stable (ApiVersions doesn't change
      ## without a reconnect), so validation RPCs to them are skipped. Cleared in
      ## stop(); a fresh fetcher re-probes.
    lastLeaderEpoch: Table[TopicPartition, int32]
      ## Monotone ceiling of the leaderEpoch ever sent on a FetchRequest for this
      ## partition. KIP-320 fences fetches whose epoch is older than metadata already
      ## observed, so this keeps a stale metadata read from regressing the value.
    assignmentGeneration*: int32
      ## Mirrors the group's current generationId; every enqueued record is
      ## tagged with it so the poll path can drop leftovers from a previous
      ## generation. -1 until the first JoinGroup completes.
    metricsHook*: MetricsHook

const defaultMaxTotalDecompressedBytes* = 1 shl 30
  ## Default ceiling for the cumulative decompressed bytes of one fetch response.
  ## Per-batch caps alone leave the response total unbounded, so the shared budget
  ## (see `DecompressionBudget`) caps all partitions together: generous for
  ## legitimate high-ratio topics, bounding a malicious broker's amplification.

const FetchRetryBackoffMs = 100'i32
  ## Base delay before a fetch round is retried after a retriable per-partition
  ## error, jittered and escalated per consecutive unsuccessful round. Matches
  ## Java's `retry.backoff.ms` default; a broker that answers retriable codes as
  ## fast as it receives requests must not drive the fetch loop at RPC rate.

const MaxListOffsetsAttempts = 3
  ## Attempts of one ListOffsets request before an unresolved partition is left to
  ## the caller's "no committed offset" error. Retries only retriable per-partition
  ## codes (see resolveOffsets).

const ListOffsetsRetryBackoffMs = 100'i32
  ## Base delay between those attempts, jittered and escalated.

proc newFetcher*(
    cluster: Cluster,
    maxWaitMs: int32 = 500,
    minBytes: int32 = 1,
    maxBytes: int32 = 52428800,
    maxPartitionBytes: int32 = 1048576,
    queueSize: int = 1000,
    offsetResetPolicy: OffsetResetPolicy = orpLatest,
    isolationLevel: IsolationLevel = ilReadUncommitted,
    rebalanceCheck: RebalanceCheck = nil,
    maxConsecutiveFetchErrors: int = 10,
    offsetResolveBackoffMs: int32 = 5000,
    maxTotalDecompressedBytes: int = defaultMaxTotalDecompressedBytes,
    metricsHook: MetricsHook = nil,
    maxValidationAttempts: int = 10,
): Fetcher {.raises: [KafkaError].} =
  # A non-positive maxsize makes AsyncQueue.full always false: the queue would be
  # unbounded, dropping the backpressure that bounds fetcher memory.
  if queueSize <= 0:
    raise newException(KafkaError, "queueSize must be > 0, got " & $queueSize)
  # A non-positive budget makes fetchFromBroker pass a nil DecompressionBudget,
  # skipping the response-wide zip-bomb cap (0 is mapped by withDefaults already).
  if maxTotalDecompressedBytes <= 0:
    raise newException(
      KafkaError,
      "maxTotalDecompressedBytes must be > 0, got " & $maxTotalDecompressedBytes,
    )
  # A non-positive budget would leave partitions permanently awaiting leader
  # validation instead of letting the fetch loop recover them.
  if maxValidationAttempts <= 0:
    raise newException(
      KafkaError, "maxValidationAttempts must be > 0, got " & $maxValidationAttempts
    )
  # A negative wait or byte count reaches the broker as-is and comes back as
  # INVALID_REQUEST on every fetch; reject it while it is still a config error. 0
  # keeps its own meaning where it has one: answer as soon as anything is available
  # for the two broker-side waits, and no cap for the consecutive-error budget.
  if maxWaitMs < 0:
    raise newException(KafkaError, "maxWaitMs must be >= 0, got " & $maxWaitMs)
  if minBytes < 0:
    raise newException(KafkaError, "minBytes must be >= 0, got " & $minBytes)
  if maxBytes <= 0:
    raise newException(KafkaError, "maxBytes must be > 0, got " & $maxBytes)
  if maxPartitionBytes <= 0:
    raise newException(
      KafkaError, "maxPartitionBytes must be > 0, got " & $maxPartitionBytes
    )
  if maxConsecutiveFetchErrors < 0:
    raise newException(
      KafkaError,
      "maxConsecutiveFetchErrors must be >= 0, got " & $maxConsecutiveFetchErrors,
    )
  if offsetResolveBackoffMs < 0:
    raise newException(
      KafkaError, "offsetResolveBackoffMs must be >= 0, got " & $offsetResolveBackoffMs
    )
  Fetcher(
    cluster: cluster,
    maxWaitMs: maxWaitMs,
    minBytes: minBytes,
    maxBytes: maxBytes,
    maxPartitionBytes: maxPartitionBytes,
    offsetResetPolicy: offsetResetPolicy,
    isolationLevel: isolationLevel,
    records: newAsyncQueue[FetchedRecord](queueSize),
    recordQueueMaxSize: queueSize,
    running: false,
    rebalanceCheck: rebalanceCheck,
    consecutiveFetchErrors: 0,
    maxConsecutiveFetchErrors: maxConsecutiveFetchErrors,
    offsetResolveBackoffMs: offsetResolveBackoffMs,
    maxTotalDecompressedBytes: maxTotalDecompressedBytes,
    assignmentGeneration: -1'i32,
    metricsHook: metricsHook,
    maxValidationAttempts: maxValidationAttempts,
  )

proc maxValidationAttempts*(f: Fetcher): int {.inline, raises: [].} =
  ## Positive validation-failure budget selected at construction.
  f.maxValidationAttempts

proc bumpGeneration*(f: Fetcher, newGeneration: int32) {.raises: [].} =
  ## Sync the fetcher's view of the group generation; called after SyncGroup so
  ## later enqueues carry the live generation. Synchronous: chronos cannot
  ## interleave another coroutine between the JoinGroup handler and this write.
  f.assignmentGeneration = newGeneration

proc isRecordStaleGeneration*(
    record: FetchedRecord, currentGeneration: int32
): bool {.raises: [].} =
  ## `true` when `record.generation` predates the current group generation and the poll
  ## path should drop it. Non-positive tags (0 = Nim default, -1 = pre-bump sentinel)
  ## have no generation context and are never stale: real generationIds start at 1.
  record.generation > 0 and record.generation != currentGeneration

proc currentPosEpoch*(f: Fetcher, tp: TopicPartition): Option[int64] {.raises: [].} =
  ## Live `posEpoch` for `tp`, or `none` when unassigned.
  f.partitionIndex.withValue(tp, idx):
    return some(f.partitions[idx[]].posEpoch)
  none(int64)

proc isRecordStalePosEpoch*(
    record: FetchedRecord, current: Option[int64]
): bool {.raises: [].} =
  ## `true` when the tracked partition's live posEpoch has advanced past the record's
  ## enqueue snapshot — closes the enqueuePartition put/re-check race by dropping
  ## pre-seek records at delivery time. An untracked partition is deliberately not stale
  ## (test injectors push into untracked tps); those records fall to
  ## isRecordStaleGeneration.
  if current.isNone:
    return false
  record.posEpoch != current.get

proc stampEnqueuedRecord(
    f: Fetcher, record: var FetchedRecord, tp: TopicPartition
) {.raises: [].} =
  ## Stamp `record` exactly as enqueuePartition does: the live `assignmentGeneration`
  ## and (for a tracked partition) the current `posEpoch`. Single source of truth shared
  ## with the test-only injector, so the delivery-side filters cannot drift.
  record.generation = f.assignmentGeneration
  let live = f.currentPosEpoch(tp)
  if live.isSome:
    record.posEpoch = live.get

proc trackFetchError(f: Fetcher, msg: string) =
  f.consecutiveFetchErrors.inc
  if f.consecutiveFetchErrors >= f.maxConsecutiveFetchErrors and f.lastError == nil:
    f.lastError = newException(
      KafkaError,
      "persistent fetch failure after " & $f.consecutiveFetchErrors &
        " consecutive errors: " & msg,
    )

proc handleTopLevelFetchError(
    f: Fetcher, code: KafkaErrorCode, brokerId: int32
): bool {.raises: [KafkaError].} =
  ## FetchResponse top-level errorCode (v7+): true to continue (keNone), false to
  ## skip a retriable session-level error this round, raise for fatal codes. For
  ## keFetchSessionIdNotFound / keInvalidFetchSessionEpoch (KIP-227) the stored
  ## session is dropped so the next fetch starts fresh with sessionId=0.
  if code == keNone:
    return true

  case errorClass(code)
  of ecRetriable:
    if code == keFetchSessionIdNotFound or code == keInvalidFetchSessionEpoch or
        code == keFetchSessionTopicIdError:
      f.fetchSessions.del(brokerId)
      warn "Fetch session invalidated on broker ",
        brokerId, " (", kafkaErrorName(code), "); restarting session"
    else:
      warn "Transient top-level fetch error on broker ",
        brokerId, ": ", kafkaErrorName(code)
    f.trackFetchError(kafkaErrorName(code))
    let brokerStr = $brokerId
    let errName = kafkaErrorName(code)
    emitMetric(
      f.metricsHook,
      meConsumerFetchTopLevelError,
      [(cstring"broker", cstring(brokerStr)), (cstring"error_code", cstring(errName))],
      1,
    )
    return false
  else:
    raise newKafkaApiError(
      code, "fetch top-level error on broker " & $brokerId & ": " & kafkaErrorName(code)
    )

proc resolveOffsets(
    f: Fetcher, partitions: seq[TopicPartition]
): Future[Table[TopicPartition, int64]] {.
    async: (raises: [KafkaError, CancelledError])
.} =
  if partitions.len == 0 or f.offsetResetPolicy == orpNone:
    return initTable[TopicPartition, int64]()

  let timestamp: int64 = if f.offsetResetPolicy == orpLatest: -1'i64 else: -2'i64

  var byBroker = initTable[int32, seq[TopicPartition]]()
  var brokerMap = initTable[int32, Broker]()
  for tp in partitions:
    let broker = await f.cluster.brokerForPartition(tp)
    let nodeId = broker.nodeId
    brokerMap[nodeId] = broker
    byBroker.mgetOrPut(nodeId, @[]).add(tp)

  for nodeId, tps in byBroker:
    var broker: Broker
    brokerMap.withValue(nodeId, b):
      broker = b[]
    do:
      continue

    # Retry the partitions the broker answered retriably for (OFFSET_NOT_AVAILABLE
    # right after an unclean election, a leader still catching up): without it a
    # transient per-partition answer made setPartitions raise "refusing to fall
    # back to 0" and failed start() outright. Stale-leader codes are left to the
    # caller: they only recover after a metadata refresh, so re-asking the same
    # broker would just spend the retry budget.
    var pending = tps
    var attempt = 0
    while pending.len > 0:
      inc attempt
      var topicReqs = initTable[string, seq[ListOffsetsPartitionReq]]()
      for tp in pending:
        topicReqs.mgetOrPut(tp.topic, @[]).add(
          ListOffsetsPartitionReq(partition: tp.partition, timestamp: timestamp)
        )

      var topics: seq[ListOffsetsTopicReq]
      for topic, parts in topicReqs:
        topics.add(ListOffsetsTopicReq(topic: topic, partitions: parts))

      let req = ListOffsetsRequest(replicaId: -1, topics: topics)
      let resp = await broker.listOffsets(req)

      var retriable: seq[TopicPartition]
      for topicResp in resp.topics:
        for partResp in topicResp.partitions:
          let tp = TopicPartition(topic: topicResp.topic, partition: partResp.partition)
          if partResp.errorCode == keNone:
            result[tp] = partResp.offset
          else:
            let willRetry =
              attempt < MaxListOffsetsAttempts and partResp.errorCode.isRetriable and
              not partResp.errorCode.isStaleLeaderCode
            warn "ListOffsets error for ",
              topicResp.topic,
              "-",
              partResp.partition,
              ": ",
              kafkaErrorName(partResp.errorCode),
              (if willRetry: " (retrying)" else: "")
            if willRetry:
              retriable.add(tp)
      # A partition the response omitted stays unresolved and is reported by the
      # caller; only an explicitly retriable answer is retried, so this cannot spin.
      pending = retriable
      if pending.len > 0:
        try:
          await sleepAsync(
            milliseconds(jitteredBackoff(ListOffsetsRetryBackoffMs, attempt))
          )
        except CancelledError:
          raise

proc snapshotPausedFlags*(f: Fetcher): Table[TopicPartition, bool] {.raises: [].} =
  ## Paused flag per currently-assigned partition, used by `setPartitions` to
  ## carry pause state across a rebalance: a partition in both the snapshot and
  ## the new assignment stays paused.
  for ps in f.partitions:
    if ps.paused:
      result[ps.tp] = true

proc setPartitions*(
    f: Fetcher,
    partitions: seq[TopicPartition],
    initialOffsets: Table[TopicPartition, int64] = initTable[TopicPartition, int64](),
    initialEpochs: Table[TopicPartition, int32] = initTable[TopicPartition, int32](),
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## `initialEpochs` seeds `PartitionState.lastFetchedEpoch` from the epoch each
  ## committed offset was written under (OffsetFetchResponse v5+
  ## committedLeaderEpoch). Without it, validation cannot fire before the first
  ## successful fetch, and an unclean leader election in that window could deliver a
  ## divergent log from the committed offset. Absent entries or values < 0 leave -1.
  var needsReset: seq[TopicPartition]
  for tp in partitions:
    if not initialOffsets.hasKey(tp):
      needsReset.add(tp)

  var resolvedOffsets: Table[TopicPartition, int64]
  if needsReset.len > 0:
    if f.offsetResetPolicy == orpNone:
      raise newException(
        KafkaError,
        "no committed offset for " & $needsReset.len &
          " partition(s) and offset reset policy is 'none'",
      )
    resolvedOffsets = await f.resolveOffsets(needsReset)

  # Warm the metadata cache so later brokerForPartition calls are cache hits.
  var topics: seq[string]
  for tp in partitions:
    if tp.topic notin topics:
      topics.add(tp.topic)
  if topics.len > 0:
    await f.cluster.refreshMetadata(some(topics))

  # Carry paused flags across a rebalance (Java: pause scope is the consumer
  # instance, so a surviving partition stays paused until resume()).
  let prevPaused = f.snapshotPausedFlags()

  # Build into locals first so a broker-lookup failure leaves f.partitions /
  # f.partitionIndex untouched, never a half-initialized assignment.
  var newPartitions = newSeq[PartitionState](partitions.len)
  var newIndex = initTable[TopicPartition, int]()
  for i, tp in partitions:
    let broker = await f.cluster.brokerForPartition(tp)
    # Committed offsets come from the caller, unresolved ones from
    # resolvedOffsets; if both miss, surface it rather than replaying from 0.
    let offset =
      try:
        if tp in initialOffsets:
          initialOffsets[tp]
        elif tp in resolvedOffsets:
          resolvedOffsets[tp]
        else:
          raise newException(
            KafkaError,
            "offset resolution failed for " & tp.topic & "-" & $tp.partition &
              " (no committed offset and ListOffsets returned no result); " &
              "refusing to fall back to 0",
          )
      except KeyError:
        # Unreachable: the `in` checks guard both lookups and chronos is
        # single-threaded, so the tables cannot mutate between check and access.
        raise newException(
          KafkaError, "offset lookup raced for " & tp.topic & "-" & $tp.partition
        )
    # Only partitions whose offset came from a committed value (initialOffsets)
    # get seeded: reset-policy resolved offsets have no epoch to validate against.
    let seedEpoch =
      if tp in initialOffsets:
        let e = initialEpochs.getOrDefault(tp, -1'i32)
        if e >= 0: e else: -1'i32
      else:
        -1'i32
    newPartitions[i] = PartitionState(
      tp: tp,
      fetchOffset: offset,
      consumedOffset: offset,
      # Seed from the committed epoch too: the position WAS consumed under it (someone
      # committed it, we're resuming). A negative seedEpoch stays -1 so `commitOffsets`
      # falls back to cluster metadata until a real record advances the epoch.
      consumedLeaderEpoch: seedEpoch,
      broker: broker,
      paused: prevPaused.getOrDefault(tp, false),
      lastFetchedEpoch: seedEpoch,
      awaitingValidation: false,
    )
    newIndex[tp] = i

  # Atomic swap: only reached when every partition resolved successfully.
  f.partitions = newPartitions
  f.partitionIndex = newIndex

  # Partitions that left the assignment lose their leaderEpoch ceiling, so a
  # future re-join restarts from whatever the cluster reports.
  var staleTps: seq[TopicPartition]
  for tp in f.lastLeaderEpoch.keys:
    if tp notin newIndex:
      staleTps.add(tp)
  for tp in staleTps:
    f.lastLeaderEpoch.del(tp)

proc filterBatches*(
    batches: seq[RecordBatch],
    abortedTransactions: sink seq[AbortedTransaction],
    isolationLevel: IsolationLevel,
): seq[RecordBatch] =
  ## Filter record batches before delivery. Control batches (transaction markers)
  ## are NEVER delivered at any isolation level (matching the Java client); under
  ## read_committed aborted-txn batches are dropped too.
  if isolationLevel != ilReadCommitted or abortedTransactions.len == 0:
    for batch in batches:
      if (batch.attributes and 0x20'i16) == 0:
        result.add(batch)
    return

  var sortedAborted = abortedTransactions
  if sortedAborted.len > 1:
    sortedAborted.sort(
      proc(a, b: AbortedTransaction): int =
        cmp(a.firstOffset, b.firstOffset)
    )

  var activeAborted: HashSet[int64]
  var abortedIdx = 0
  for batch in batches:
    # Activate aborted producers whose firstOffset falls at or before this batch.
    while abortedIdx < sortedAborted.len and
        sortedAborted[abortedIdx].firstOffset <=
        batch.baseOffset + int64(batch.lastOffsetDelta)
    :
      activeAborted.incl(sortedAborted[abortedIdx].producerId)
      abortedIdx.inc

    let isTransactional = (batch.attributes and 0x10'i16) != 0
    let isControl = (batch.attributes and 0x20'i16) != 0

    if isControl:
      activeAborted.excl(batch.producerId)
      continue

    if isTransactional and batch.producerId in activeAborted:
      continue

    result.add(batch)

proc carriedBatchEnd(b: RecordBatch): int64 {.raises: [].} =
  ## First offset the batch does not carry, as far as the decoded bytes show.
  ##
  ## `lastOffsetDelta` is a header claim, and an overstated span would move the cursor
  ## past never-delivered records — the next fetch starts after them, so the loss is
  ## permanent. The decoder cannot reject that header: the encoder legitimately emits
  ## non-dense batches (records at baseOffset and baseOffset+2 with lastOffsetDelta 2),
  ## so the clamp belongs where the cursor moves; it bites only when the header claims
  ## more than the payload carries (the records section is checked against `numRecords`).
  ## Two cases keep the header
  ## value for lack of a record to clamp against: a v2 batch that decoded zero records
  ## (fully compacted / control — it names the range it once covered) and a legacy
  ## (magic != 2) message, skipped whole and occupying exactly the one offset it names.
  result = b.baseOffset + int64(b.lastOffsetDelta) + 1
  if b.magic == 2 and b.records.len > 0:
    var maxOffset = b.baseOffset
    for rec in b.records:
      if rec.offset > maxOffset:
        maxOffset = rec.offset
    if maxOffset + 1 < result:
      result = maxOffset + 1

proc computeNextFetchOffset*(
    batches: seq[RecordBatch],
    lastEnqueuedOffset: int64,
    queueFull: bool,
    currentFetchOffset: int64,
    corruptGapEnd: int64 = -1,
): int64 {.raises: [].} =
  ## Decide where the next fetch for a partition should start.
  ##
  ## Advancing only past delivered records would pin `fetchOffset` forever on a batch
  ## that occupies offsets but yields nothing (control marker, fully aborted, fully
  ## compacted), so on full processing we advance past the end of every decoded batch
  ## (`baseOffset + lastOffsetDelta + 1`, Java's `nextFetchOffset`), control/aborted
  ## batches included — hence `batches` must be the UNFILTERED decoded list. Under
  ## `queueFull` we stopped mid-partition, so advancing further would silently drop
  ## records never enqueued: advance only past the last enqueued record, and never
  ## move backwards from `currentFetchOffset` — compaction can return a batch whose
  ## baseOffset precedes our position.
  ##
  ## `corruptGapEnd` (>= 0) is the end of the corrupted range dropped by
  ## parseRecordBatches, always pinned by a CRC-verified batch and never read from the
  ## corrupt header; on full processing the cursor advances past it so those bytes are
  ## never re-fetched. Not applied under `queueFull`, where undelivered records before
  ## the gap would be skipped.
  result = currentFetchOffset
  if lastEnqueuedOffset >= 0 and lastEnqueuedOffset + 1 > result:
    result = lastEnqueuedOffset + 1
  if not queueFull:
    for b in batches:
      let batchEnd = carriedBatchEnd(b)
      if batchEnd > result:
        result = batchEnd
    if corruptGapEnd > result:
      result = corruptGapEnd

type
  BatchParseStatus* = enum
    bpsClean ## Every batch decoded cleanly.
    bpsSkipped
      ## A corrupt batch was dropped with its offset range recovered
      ## (`corruptGapEnd` valid), so the caller can advance past it; clean
      ## batches around it are kept.
    bpsUnknownGap
      ## A corrupt batch was dropped with no CRC-verified batch pinning its
      ## range (its header is part of the same untrusted bytes); the caller must
      ## reposition or the same bytes re-fetch forever.

  ParsedBatches* = object
    batches*: seq[RecordBatch]
    status*: BatchParseStatus
    corruptGapEnd*: int64
      ## First offset past the corrupted range when status == bpsSkipped; -1 else.
    corruptCount*: int ## Batches dropped to corruption.

proc peekUint32BE(data: openArray[byte], at: int): uint32 {.raises: [].} =
  ## Big-endian uint32 assembled from four bytes the caller has already bounded.
  let b0 = uint32(data[at])
  let b1 = uint32(data[at + 1])
  let b2 = uint32(data[at + 2])
  let b3 = uint32(data[at + 3])
  (b0 shl 24) or (b1 shl 16) or (b2 shl 8) or b3

proc legacyMessageCrcOk(data: openArray[byte], startPos: int, size: int32): bool =
  ## True when the bytes at `startPos` form a self-consistent legacy MessageSet
  ## message (pre-0.11, magic 0/1) whose CRC32C verifies.
  ##
  ## The decoder's tolerant `magic != 2` branch checks nothing, and
  ## `partitionLeaderEpoch` is not an epoch in either layout (v0: message CRC, v1:
  ## timestamp), so the fields read afterwards are unverified input; the message's own
  ## CRC32C must pass before its header may move the cursor:
  ##   v0: crc at +12 covers +16 .. +16+size-5 (size-4 bytes), magic 0 at +16
  ##   v1: magic 1 at +12, crc after the value covers +12 .. +12+size-5
  ## Both cover `size - 4` bytes and neither the leading offset; both are tried because
  ## their discriminators overlap (a v0 CRC starting with 0x01 looks like v1 magic, a v1
  ## timestamp byte of 0x00 like v0 magic). One extra CRC over bytes already in hand,
  ## rejecting nothing real.
  if size < 14 or startPos + 12 + int(size) > data.len:
    return false
  if data[startPos + 16] == 0'u8 and
      crc32c(data.toOpenArray(startPos + 16, startPos + 16 + int(size) - 5)) ==
      peekUint32BE(data, startPos + 12):
    return true
  if size >= 22 and data[startPos + 12] == 1'u8 and
      crc32c(data.toOpenArray(startPos + 12, startPos + 12 + int(size) - 5)) ==
      peekUint32BE(data, startPos + 12 + int(size) - 4):
    return true
  false

proc parseRecordBatches*(
    raw: seq[byte], budget: DecompressionBudget = nil
): ParsedBatches {.raises: [KafkaError].} =
  ## Parse a partition's record-bytes blob (FetchResponse NULLABLE_BYTES) into
  ## RecordBatch objects, tolerating corrupt batches. `budget` is the response-wide
  ## decompression budget shared by every partition blob; nil keeps the per-batch cap.
  ##
  ## A response can end in a partial batch when the data exceeds the fetch size. The
  ## batchLength prefix (int32 at baseOffset+8) gives the full size, so on fewer
  ## remaining bytes stop at the truncated tail instead of letting decode() raise and
  ## discard the complete batches already parsed; the caller advances only over
  ## complete batches, so the next fetch re-requests it in full (Java / librdkafka).
  ##
  ## A batch that fails to decode (CRC mismatch, malformed header, failed decompression)
  ## is dropped rather than aborting the blob: earlier batches are kept,
  ## resynchronisation uses its batchLength framing, later clean ones are kept too. Its
  ## range is reported as `corruptGapEnd`, pinned by the next clean batch's baseOffset
  ## and nothing else — the corrupt header's baseOffset / lastOffsetDelta sit OUTSIDE
  ## the CRC region, so one flipped byte can claim a multi-gigabyte span and adopting it
  ## would skip every record in between. No pinning batch (`bpsUnknownGap`) means the
  ## caller repositions per policy: a configured reset or a visible error, never a
  ## silent jump.
  result.corruptGapEnd = -1
  if raw.len == 0:
    return

  var rdr = initKafkaReader(raw)
  rdr.decompressionBudget = budget
  # A corruption happened and no clean batch has pinned its end yet.
  var corruptSeen = false
  while rdr.remaining >= 12:
    let batchLen = rdr.peekInt32At(rdr.pos + 8)
    if rdr.remaining < 12 + int(batchLen):
      break
    let startPos = rdr.pos
    var batch: RecordBatch
    var decoded = false
    try:
      batch = rdr.decode(RecordBatch)
      decoded = true
    except KafkaDecompressionLimitError:
      # A budget violation is NOT corruption: the batches are well-formed but the
      # response exceeds the configured ceiling. Re-raise (the fetch loop makes it
      # terminal); skipping them as corrupt would silently advance past unseen records.
      raise
    except KafkaError:
      discard
    if decoded and batch.magic != 2:
      # The legacy branch of the decoder consumed this message without a single
      # check, so its header is unverified input: demote it to the corruption path
      # unless the message's own CRC32C verifies.
      if not legacyMessageCrcOk(rdr.data, startPos, batchLen):
        decoded = false
      elif corruptSeen:
        # A verified legacy message inside a corrupted range is far more likely a
        # misframed fragment of it than a real MessageSet. Drop it (already consumed).
        result.corruptCount.inc
        continue
      else:
        result.batches.add(batch)
        continue
    if decoded:
      # Keep every decoded batch, including control / empty (fully-compacted)
      # ones: they occupy offsets, so computeNextFetchOffset needs them to clear a
      # trailing transaction marker. filterBatches drops them before delivery.
      if batch.baseOffset < 0:
        # CRC-valid, but a negative baseOffset is not a legal Kafka position:
        # treat it as corruption rather than let it poison the offset advance.
        result.corruptCount.inc
        corruptSeen = true
        continue
      if corruptSeen:
        # First clean batch after the corruption pins the gap's end: known-good
        # data starts at its baseOffset, so everything below it is skippable.
        if batch.baseOffset > result.corruptGapEnd:
          result.corruptGapEnd = batch.baseOffset
        corruptSeen = false
      result.batches.add(batch)
    else:
      result.corruptCount.inc
      corruptSeen = true
      # Resynchronise on the batchLength framing (outside the CRC region, so
      # usually intact even when the payload is not). An implausible length leaves
      # no trustworthy sync point — stop and keep what parsed.
      if batchLen >= 5:
        rdr.pos = startPos + 12 + int(batchLen)
      else:
        break
  result.status =
    if result.corruptCount == 0:
      bpsClean
    elif result.corruptGapEnd >= 0:
      bpsSkipped
    else:
      bpsUnknownGap

proc countLegacyMagicBatches*(batches: seq[RecordBatch]): int {.raises: [].} =
  ## Count legacy MessageSet v0/v1 batches (magic != 2) that passed the legacy
  ## CRC32C check — pre-0.11 segments the broker really sent. Their records are
  ## dropped silently and `lastOffsetDelta` stays 0, so this is the caller's only
  ## observability into them; unverified magic != 2 bytes are counted as corruption
  ## by parseRecordBatches and never reach here.
  for b in batches:
    if b.magic != 2:
      inc result

proc drainQueuedRecords(f: Fetcher, tp: TopicPartition) {.raises: [].} =
  ## Remove every queued record for `tp`, preserving other partitions. AsyncQueue
  ## has no in-place iteration, so pop everything and re-enqueue the survivors.
  ## Callers must run this synchronously (no awaits) so chronos cannot interleave
  ## a poll() that would observe partial state.
  var keep: seq[FetchedRecord]
  while not f.records.empty():
    try:
      let r = f.records.getNoWait()
      if not (r.topic == tp.topic and r.partition == tp.partition):
        keep.add(r)
    except AsyncQueueEmptyError:
      break

  for r in keep:
    try:
      f.records.addLastNoWait(r)
    except AsyncQueueFullError:
      # Re-add cannot fail (the queue can't have shrunk below its prior length);
      # warn rather than crash if a future change invalidates that.
      warn "dropped record for ",
        r.topic, "-", r.partition, " while requeueing (queue full)"
      break

proc applyOffsetReset(f: Fetcher, tp: TopicPartition, newOffset: int64) {.raises: [].} =
  ## Apply an OFFSET_OUT_OF_RANGE recovery to `tp`: move BOTH the fetch cursor and
  ## the committable position to `newOffset`, and drop already-queued pre-reset
  ## records (an earlier fetch may have enqueued them before the reset was decided;
  ## otherwise `advanceConsumedOffset` on the next poll would silently rewind
  ## `consumedOffset` below `newOffset`). Split out of fetchFromBroker so the reset
  ## policy is unit-testable in isolation.
  ##
  ## `everConsumed` is left untouched: an unconsumed partition stays out of
  ## `getConsumedOffsets` (never committing a placeholder), an already-consumed one
  ## just has its committable offset realigned.
  f.partitionIndex.withValue(tp, idx):
    f.partitions[idx[]].fetchOffset = newOffset
    f.partitions[idx[]].consumedOffset = newOffset
    # Reset target has no known epoch; commit falls back to metadata.
    f.partitions[idx[]].consumedLeaderEpoch = -1'i32
    f.partitions[idx[]].lastOffsetResolve = Moment.now()
    # Reposition: invalidate any response in flight against the pre-reset
    # position (see PartitionState.posEpoch).
    f.partitions[idx[]].posEpoch += 1
    f.partitions[idx[]].lastFetchedEpoch = -1'i32
    f.partitions[idx[]].awaitingValidation = false
    f.partitions[idx[]].validationAttempts = 0
  f.drainQueuedRecords(tp)

proc enqueuePartition(
    f: Fetcher,
    tp: TopicPartition,
    filteredBatches: seq[RecordBatch],
    batches: seq[RecordBatch],
    corruptGapEnd: int64 = -1,
): Future[bool] {.async: (raises: [CancelledError]).} =
  ## Enqueue a partition's deliverable records and advance its fetchOffset; returns
  ## true when the queue filled mid-partition (caller stops).
  ##
  ## Track the highest enqueued offset so the advance is atomic per partition and a
  ## queue-full mid-batch cannot duplicate records. Brokers return data at BATCH
  ## granularity, so the first batch can carry records BELOW the requested fetchOffset
  ## (committed offsets rarely land on a boundary; compaction can start a batch before
  ## the position): skip those (Java's position filter) and read the CURRENT
  ## fetchOffset so an in-flight seek-forward is honoured. A partition absent from the
  ## index (KIP-227 leftover) is filtered by the stale-generation check.
  ##
  ## The loop awaits on a full queue, and a concurrent seek / offset reset can
  ## reposition mid-await: it drains the queue (freeing the parked put's slot) and bumps
  ## posEpoch, so snapshot posEpoch before the loop and re-check after every put. On a
  ## move, everything from this record on is pre-reposition data — stop, drop what
  ## slipped into the drained queue, and skip the advance so the never-backward clamp
  ## cannot undo a seek-backward. The top-of-partition guard only catches a reposition
  ## landing BEFORE processing (see PartitionState.posEpoch).
  var positionOffset = int64(-1)
  var startEpoch = int64(0)
  f.partitionIndex.withValue(tp, idx):
    positionOffset = f.partitions[idx[]].fetchOffset
    startEpoch = f.partitions[idx[]].posEpoch
  var lastEnqueuedOffset = int64(-1)
  var queueFull = false
  var enqueuedCount = 0
  var repositioned = false
  block enqueue:
    for batch in filteredBatches:
      for rec in batch.records:
        if positionOffset >= 0 and rec.offset < positionOffset:
          continue
        # Pre-put posEpoch check narrows the reposition-during-loop window.
        f.partitionIndex.withValue(tp, idx):
          if f.partitions[idx[]].posEpoch != startEpoch:
            repositioned = true
        if repositioned:
          break enqueue
        var fetched = FetchedRecord(
          topic: tp.topic,
          partition: tp.partition,
          offset: rec.offset,
          timestamp: rec.timestamp,
          key: rec.key,
          value: rec.value,
          headers: rec.headers,
          leaderEpoch: batch.partitionLeaderEpoch,
        )
        f.stampEnqueuedRecord(fetched, tp)
        # No-duplicate invariant: on AsyncTimeoutError the record counts as NOT
        # enqueued (lastEnqueuedOffset unchanged, so it is re-fetched). Holds only
        # because chronos cancels the inner `put` before addLastNoWait, and a
        # cancelled put lands nothing; otherwise a timed-out put could enqueue while
        # we re-fetch, duplicating the record.
        try:
          await f.records.put(fetched).wait(milliseconds(f.maxWaitMs))
        except AsyncTimeoutError:
          queueFull = true
          break enqueue
        f.partitionIndex.withValue(tp, idx):
          if f.partitions[idx[]].posEpoch != startEpoch:
            repositioned = true
        if repositioned:
          break enqueue
        lastEnqueuedOffset = rec.offset
        inc enqueuedCount

  if repositioned:
    # A seek / offset reset moved this partition while a put was parked; its drain
    # ran before these records landed. Drop them and leave fetchOffset to the
    # reposition that now owns it.
    f.drainQueuedRecords(tp)
    return false

  # Advance past delivered records AND any control / aborted / fully-compacted
  # batches that yielded nothing, so a read_committed consumer cannot get stuck
  # on a transaction marker. Unfiltered `batches` make those offsets count;
  # `queueFull` confines advancement to enqueued records (no data loss).
  f.partitionIndex.withValue(tp, idx):
    let next = computeNextFetchOffset(
      batches,
      lastEnqueuedOffset,
      queueFull,
      f.partitions[idx[]].fetchOffset,
      corruptGapEnd,
    )
    if next > f.partitions[idx[]].fetchOffset:
      f.partitions[idx[]].fetchOffset = next
      # KIP-320: track the leader epoch of the newest batch advanced past. Epochs
      # are monotonic along the log, so the last batch below `next` holds the max;
      # a later OffsetForLeaderEpoch query sends it to detect truncation.
      var epoch = f.partitions[idx[]].lastFetchedEpoch
      for b in batches:
        # A legacy (magic != 2) message carries no leader epoch — those four bytes
        # are the message CRC (v0) or part of its timestamp (v1). The ceiling is
        # monotone, so adopting one garbage value would keep it above every real
        # epoch and silently disable the KIP-320 truncation check for good.
        if b.magic != 2:
          continue
        if b.baseOffset < next and b.partitionLeaderEpoch > epoch:
          epoch = b.partitionLeaderEpoch
      f.partitions[idx[]].lastFetchedEpoch = epoch

  if enqueuedCount > 0:
    let partStr = $tp.partition
    emitMetric(
      f.metricsHook,
      meConsumerFetchRecords,
      [(cstring"topic", cstring(tp.topic)), (cstring"partition", cstring(partStr))],
      int64(enqueuedCount),
    )
  return queueFull

proc resetPartitionOffset(
    f: Fetcher, tp: TopicPartition, reason: string
): Future[void] {.async: (raises: [KafkaError, CancelledError]).} =
  ## Reposition `tp` via ListOffsets per the reset policy; shared by the
  ## OFFSET_OUT_OF_RANGE and unrecoverable-corruption paths (both mean "the current
  ## position is unconsumable"). Raises for orpNone: the caller promised no reset.
  if f.offsetResetPolicy == orpNone:
    raise newKafkaApiError(
      keOffsetOutOfRange,
      reason & " for " & tp.topic & "-" & $tp.partition & " and reset policy is none",
    )
  var skipPartition = false
  f.partitionIndex.withValue(tp, idx):
    let ps = f.partitions[idx[]]
    let now = Moment.now()
    if ps.lastOffsetResolve != default(Moment) and
        (now - ps.lastOffsetResolve) < milliseconds(f.offsetResolveBackoffMs):
      warn "Offset reset for ",
        tp.topic, "-", tp.partition,
        " shortly after the last one, backing off this partition"
      skipPartition = true
  if skipPartition:
    return
  # Capture posEpoch before the resolve await so a user seek() (or a concurrent
  # reset) that lands in between wins: applyOffsetReset would otherwise clobber
  # the fresh fetchOffset.
  var resetEpoch = 0'i64
  var haveEpoch = false
  f.partitionIndex.withValue(tp, idx):
    resetEpoch = f.partitions[idx[]].posEpoch
    haveEpoch = true
  if not haveEpoch:
    return
  try:
    let resolved = await f.resolveOffsets(@[tp])
    var stillMatches = false
    f.partitionIndex.withValue(tp, idx):
      stillMatches = f.partitions[idx[]].posEpoch == resetEpoch
    if stillMatches:
      for resolvedTp, newOffset in resolved:
        f.applyOffsetReset(resolvedTp, newOffset)
  except KafkaError as e:
    warn "Offset reset for ", tp.topic, "-", tp.partition, " failed: ", e.msg
  # Stamp the attempt on every outcome so a persistent ListOffsets failure
  # cannot spin the reset without backoff (applyOffsetReset covers success).
  f.partitionIndex.withValue(tp, idx):
    f.partitions[idx[]].lastOffsetResolve = Moment.now()

proc reassignBrokerPartitions(
    f: Fetcher, partIndices: seq[int]
) {.async: (raises: [CancelledError]).} =
  ## Force a metadata refresh and repoint broker references for `partIndices`,
  ## called when one broker persistently fails while the cluster is healthy.
  #
  # nil cluster is allowed in unit tests. stop() clears `running` before cancelling
  # in-flight fetches, which often fail as KafkaConnectionError rather than
  # CancelledError and must not start a refresh (or nil-deref) on the way down.
  if f.cluster == nil or not f.running:
    return
  try:
    # force = true: without it the call is swallowed whenever the TTL refresh or
    # maybeRefreshMetadata's fast path left the cache fresh — the normal state when a
    # broker first starts failing — so recovery would drop the broker's error counter
    # without repointing anything and have to re-accumulate maxConsecutiveFetchErrors
    # rounds before trying again. The other forced-refresh callers
    # (brokerForPartition, checkPartitionGrowth, performAssignment) pass it too.
    await f.cluster.refreshMetadata(force = true)
    for idx in partIndices:
      f.partitions[idx].broker =
        await f.cluster.brokerForPartition(f.partitions[idx].tp)
  except KafkaError as e:
    warn "Metadata refresh for failing broker partitions failed: ", e.msg

proc fetchFromBroker(
    f: Fetcher, partIndices: seq[int]
): Future[bool] {.async: (raises: [KafkaError, CancelledError]).} =
  # `currentLeaderEpoch` comes from cached metadata so a stale fetch view fails
  # fast with FENCED_LEADER_EPOCH after an unclean leader election (KIP-320); the
  # encoder gates the field at v9+, so older versions drop it.
  var topicReqs = initTable[string, seq[FetchPartitionReq]]()

  # Request-time position-epoch snapshot: response processing compares it to the
  # live posEpoch to discard data fetched for a position a seek / offset reset
  # moved while the request was in flight.
  var posEpochs = initTable[TopicPartition, int64]()
  for idx in partIndices:
    let ps = f.partitions[idx]
    posEpochs[ps.tp] = ps.posEpoch
    # `f.cluster` is nil in unit tests that exercise bookkeeping without a real
    # fetch; the negotiated version is the same either way.
    let observedEpoch =
      if f.cluster == nil:
        -1'i32
      else:
        let leaderInfo = f.cluster.getPartitionLeader(ps.tp)
        if leaderInfo.isSome: leaderInfo.get.leaderEpoch else: -1'i32
    # Clamp to the monotone ceiling so a transient stale metadata read
    # cannot regress the leaderEpoch we send (KIP-320).
    let prevEpoch = f.lastLeaderEpoch.getOrDefault(ps.tp, -1'i32)
    let leaderEpoch = max(observedEpoch, prevEpoch)
    if leaderEpoch != prevEpoch:
      f.lastLeaderEpoch[ps.tp] = leaderEpoch
    topicReqs.mgetOrPut(ps.tp.topic, @[]).add(
      FetchPartitionReq(
        partition: ps.tp.partition,
        currentLeaderEpoch: leaderEpoch,
        fetchOffset: ps.fetchOffset,
        maxBytes: f.maxPartitionBytes,
      )
    )

  var fetchTopics: seq[FetchTopicReq]
  for topic, parts in topicReqs:
    fetchTopics.add(FetchTopicReq(topic: topic, partitions: parts))

  let broker = f.partitions[partIndices[0]].broker
  # KIP-227: send the stored sessionId for this broker (0 = request a new session). On
  # v4-v6 brokers the schema-generator silently drops the field, so it is always safe.
  let session = f.fetchSessions.getOrDefault(broker.nodeId, FetchSession())
  # Snapshot the pause epoch so response processing can tell that pause() ran mid-flight
  # and dropped this broker's session (see the fetchSessions gating below).
  let pauseEpochAtSend = f.pauseEpochs.getOrDefault(broker.nodeId, 0'i64)
  let fetchReq = FetchRequest(
    replicaId: -1,
    maxWaitMs: f.maxWaitMs,
    minBytes: f.minBytes,
    maxBytes: f.maxBytes,
    isolationLevel: f.isolationLevel,
    sessionId: session.sessionId,
    sessionEpoch: session.sessionEpoch,
    topics: fetchTopics,
  )

  let fetchStart = Moment.now()
  var fetchResp: FetchResponse
  try:
    fetchResp = await broker.fetch(fetchReq)
  except KafkaError as fetchErr:
    f.trackFetchError(fetchErr.msg)
    let brokerIdStr = $broker.nodeId
    let reasonStr = fetchErr.msg
    emitMetric(
      f.metricsHook,
      meConsumerFetchError,
      [(cstring"broker", cstring(brokerIdStr)), (cstring"reason", cstring(reasonStr))],
      1,
    )
    # Surface any still-active throttle for observability only: the wait is owned by the
    # broker layer (writeTypedRequest.awaitThrottle absorbs it on the next fetch), so
    # sleeping here would just add latency. The metric keeps the backoff signal.
    let throttleMs = broker.throttleRemainingMs()
    if throttleMs > 0:
      emitMetric(
        f.metricsHook,
        meConsumerFetchRetryThrottleMs,
        [(cstring"broker", cstring(brokerIdStr))],
        int64(throttleMs),
      )
    await f.reassignBrokerPartitions(partIndices)
    return false

  if not f.handleTopLevelFetchError(fetchResp.errorCode, broker.nodeId):
    # Without the top-level errorCode (v7+) check the consumer no-ops on session-level
    # failures like FETCH_SESSION_ID_NOT_FOUND, busy-looping with empty fetches.
    return false

  # Skip session restoration if pause() invalidated this broker's session while the
  # fetch was in flight: reinstalling it would leave the broker-side session holding
  # the paused partition, and the next incremental fetch would keep receiving its
  # records (KIP-227: omission ≠ removal). pause() already `del`d the entry; this del
  # also drops any entry a racing restore put back.
  let pauseRacedFetch =
    f.pauseEpochs.getOrDefault(broker.nodeId, 0'i64) != pauseEpochAtSend
  if pauseRacedFetch:
    f.fetchSessions.del(broker.nodeId)
  elif fetchResp.sessionId != 0:
    # Persist the broker-assigned sessionId and bump the epoch. KIP-227
    # FetchSessionCache: sessionId=0/epoch=0 creates, sessionId>0/epoch=0 is rejected
    # (INVALID_FETCH_SESSION_EPOCH), sessionId>0/epoch>0 is incremental with a
    # monotonically increasing epoch — so a held sessionId must leave epoch zero.
    # Same sessionId → bump, new sessionId → reset to 1. v4-v6 leave sessionId at 0.
    let nextEpoch =
      if session.sessionId == fetchResp.sessionId and session.sessionEpoch > 0:
        session.sessionEpoch + 1
      else:
        1'i32
    f.fetchSessions[broker.nodeId] =
      FetchSession(sessionId: fetchResp.sessionId, sessionEpoch: nextEpoch)

  # Route stale-leader / stale-topic / KIP-320 epoch codes through `invalidateLeader`,
  # which owns the invalidate → coalesced-refresh sequence, so
  # concurrent calls on one topic share an RPC; the retriable branch below skips them.
  var invalidatedTopics: HashSet[string]
  try:
    for topicResp in fetchResp.topics:
      for partResp in topicResp.partitions:
        let tp = TopicPartition(topic: topicResp.topic, partition: partResp.partition)
        if tp notin posEpochs:
          warn "Fetch response for unrequested partition ",
            topicResp.topic, "-", partResp.partition, ": skipping"
          continue
        if await f.cluster.invalidateLeader(tp, partResp.errorCode):
          invalidatedTopics.incl(topicResp.topic)
    for i, ps in f.partitions:
      if ps.tp.topic in invalidatedTopics:
        f.partitions[i].broker = await f.cluster.brokerForPartition(ps.tp)
  except KafkaError as e:
    warn "Leader refresh failed: ", e.msg

  # One decompression budget shared by every partition of this response: decode
  # caps a batch, but the sum across batches/partitions would otherwise be
  # unbounded. Fresh on every round.
  let decompressionBudget =
    if f.maxTotalDecompressedBytes <= 0:
      nil
    else:
      DecompressionBudget(remaining: f.maxTotalDecompressedBytes)

  # Partitions of this response that answered with a retriable error, used to back
  # off once per response and to report a round that made no progress at all.
  var retriableSkipped = 0

  for topicResp in fetchResp.topics:
    for partResp in topicResp.partitions:
      let tp = TopicPartition(topic: topicResp.topic, partition: partResp.partition)

      # Echo check: the broker answered for a partition this fetch never requested,
      # so skip it before the epoch guard and every side effect below — its bytes
      # must not move any cursor.
      if tp notin posEpochs:
        warn "Fetch response for unrequested partition ",
          topicResp.topic, "-", partResp.partition, ": skipping"
        continue

      # Position-epoch guard: a seek / offset reset moved this partition while the
      # fetch was in flight, so skip it — no enqueue, no offset advance, and no
      # error handling (an OFFSET_OUT_OF_RANGE for the pre-seek offset must not
      # trigger applyOffsetReset over the fresh seek). The next round re-requests
      # from the new position; partitions absent from the snapshot (KIP-227 leftovers)
      # keep the old behaviour and are filtered by the delivery-side stale-generation
      # check later.
      var staleEpoch = false
      posEpochs.withValue(tp, snap):
        f.partitionIndex.withValue(tp, idx):
          if f.partitions[idx[]].posEpoch != snap[]:
            staleEpoch = true
      if staleEpoch:
        continue

      # Pause-mid-flight guard: pause() ran after this fetch was built, so discard
      # the response (no enqueue, no fetchOffset advance); poll() then delivers no
      # records for the paused partition (Java KafkaConsumer.pause contract) and the
      # next fetch after resume() refetches from the unchanged offset.
      var pausedNow = false
      f.partitionIndex.withValue(tp, idx):
        if f.partitions[idx[]].paused:
          pausedNow = true
      if pausedNow:
        continue

      if partResp.errorCode != keNone:
        # Every per-partition error code goes through the single classifier
        # (`classifyCode`), which owns the decision — retry, reset offset, or
        # surface; the fetch loop owns only the side effects.
        let fetchCtx = RetryContext(isFetchPartition: true)
        case classifyCode(partResp.errorCode, fetchCtx)
        of raResetOffset:
          await f.resetPartitionOffset(tp, "Offset out of range")
          continue
        of raRetrySame:
          # Retriable / rebalance-required: skip the partition and back off once per
          # response (below) before the loop re-asks. The next fetch iteration (or the
          # heartbeat loop's rejoin) picks the partition up.
          inc retriableSkipped
          warn "Transient fetch error for ",
            topicResp.topic,
            "-",
            partResp.partition,
            ": ",
            kafkaErrorName(partResp.errorCode),
            " — will retry"
          continue
        of raTerminal:
          raise newKafkaApiError(
            partResp.errorCode,
            "fetch error for " & topicResp.topic & "-" & $partResp.partition & ": " &
              kafkaErrorName(partResp.errorCode),
          )
        of raProceed, raDuplicate, raRediscover, raResetProducerId:
          # Unreachable in the fetch-partition context: the send-loop
          # (raDuplicate / raResetProducerId) and coordinator-RPC (raRediscover)
          # actions require flags this context never sets.
          raise newKafkaApiError(
            partResp.errorCode,
            "unexpected classifier action for " & topicResp.topic & "-" &
              $partResp.partition & ": " & kafkaErrorName(partResp.errorCode),
          )

      # Parse the per-partition record-bytes blob (NULLABLE_BYTES) into RecordBatches;
      # v4-v11 emit a non-null blob even for empty partitions, so None means "no data".
      # Corrupt batches are dropped with their range recovered best-effort (see
      # parseRecordBatches), letting the cursor skip them instead of re-fetching forever.
      var parsed: ParsedBatches
      if partResp.recordsRaw.isSome:
        parsed = parseRecordBatches(partResp.recordsRaw.get, decompressionBudget)
      if parsed.status != bpsClean:
        let corruptPartStr = $partResp.partition
        if parsed.status == bpsSkipped:
          warn "Corrupt record batch(es) in fetch for ",
            topicResp.topic, "-", partResp.partition, ": ", parsed.corruptCount,
            " batch(es) undecodable, skipping offsets up to ", parsed.corruptGapEnd
          emitMetric(
            f.metricsHook,
            meConsumerFetchCorruptBatchSkipped,
            [
              (cstring"topic", cstring(topicResp.topic)),
              (cstring"partition", cstring(corruptPartStr)),
            ],
            int64(parsed.corruptCount),
          )
        else:
          # bpsUnknownGap: no CRC-verified batch pins the corrupted range's end, so there
          # is no skip target — the failed batch's own header is not a source of truth.
          # Reposition per the reset policy (like OFFSET_OUT_OF_RANGE); anything less
          # re-fetches the same bytes forever. The clean batches from this response are
          # dropped too, since the reset invalidates the position and drains the queue.
          # Under orpLatest everything the response covered is skipped loudly (warning +
          # reset metric) rather than by trusting unverified bytes.
          warn "Corrupt record batch(es) in fetch for ",
            topicResp.topic,
            "-",
            partResp.partition,
            ": ",
            parsed.corruptCount,
            " batch(es) undecodable with unrecoverable offset range; " &
              "resetting position per policy"
          emitMetric(
            f.metricsHook,
            meConsumerFetchCorruptBatchReset,
            [
              (cstring"topic", cstring(topicResp.topic)),
              (cstring"partition", cstring(corruptPartStr)),
            ],
            int64(parsed.corruptCount),
          )
          await f.resetPartitionOffset(
            tp, "undecodable record batch with unrecoverable offset range"
          )
          continue
      let batches = parsed.batches
      let legacyCount = countLegacyMagicBatches(batches)
      if legacyCount > 0:
        warn "Skipped ",
          legacyCount, " legacy MessageSet batch(es) for ", topicResp.topic, "-",
          partResp.partition,
          "; pre-0.11 segments are decoded as empty (records dropped)"
        let partStr = $partResp.partition
        emitMetric(
          f.metricsHook,
          meConsumerBatchLegacySkipped,
          [("topic", topicResp.topic), ("partition", partStr)],
          legacyCount.int64,
        )
      let filteredBatches =
        filterBatches(batches, partResp.abortedTransactions.get(@[]), f.isolationLevel)

      # Advance fetchOffset and stop the fetch if the queue filled (backpressure);
      # the mid-enqueue posEpoch re-check lives in enqueuePartition.
      if await f.enqueuePartition(tp, filteredBatches, batches, parsed.corruptGapEnd):
        f.queueFull = true
        let brokerStr = $broker.nodeId
        emitMetric(
          f.metricsHook,
          meConsumerFetchLatencyMs,
          [(cstring"broker", cstring(brokerStr))],
          (Moment.now() - fetchStart).milliseconds,
        )
        return true

  let brokerEndStr = $broker.nodeId
  emitMetric(
    f.metricsHook,
    meConsumerFetchLatencyMs,
    [(cstring"broker", cstring(brokerEndStr))],
    (Moment.now() - fetchStart).milliseconds,
  )

  if retriableSkipped > 0:
    # A broker answering retriable codes as fast as it receives requests would
    # otherwise spin this loop at RPC rate. Sleep once per response (not once per
    # partition) with the shared jittered backoff, escalating while consecutive
    # responses keep making no progress.
    inc f.retriableFetchAttempts
    try:
      await sleepAsync(
        milliseconds(jitteredBackoff(FetchRetryBackoffMs, f.retriableFetchAttempts))
      )
    except CancelledError:
      raise
    if retriableSkipped == partIndices.len:
      # Every requested partition failed retriably, so this round made no progress:
      # report it as a broker failure so the existing per-broker escalation
      # (brokerFetchErrors -> metadata refresh) sees it, instead of the loop
      # re-asking the same broker forever.
      return false
  else:
    f.retriableFetchAttempts = 0
  return true

proc recordValidationFailure(f: Fetcher, tp: TopicPartition) {.raises: [].} =
  ## Count one failed OffsetForLeaderEpoch round for `tp` (no resolvable broker, RPC
  ## failure, stale-leader reply, or omission from the reply). After
  ## `maxValidationAttempts` consecutive failures validation is abandoned:
  ## `awaitingValidation` is cleared so fetching resumes, and `lastFetchedEpoch` is
  ## pinned to the observed leader epoch — the ceiling is monotone, so this equals a
  ## successful validation — so markPartitionsForValidation cannot re-flag and re-stall
  ## the partition. The next leader advance re-arms a fresh budget; a metric and warning
  ## surface it, and the loop's own escalation (brokerFetchErrors -> metadata refresh)
  ## covers the resumed fetch.
  var escalate = false
  f.partitionIndex.withValue(tp, idx):
    f.partitions[idx[]].validationAttempts.inc
    if f.maxValidationAttempts > 0 and
        f.partitions[idx[]].validationAttempts >= f.maxValidationAttempts:
      f.partitions[idx[]].validationAttempts = 0
      f.partitions[idx[]].awaitingValidation = false
      if f.cluster != nil:
        let leaderOpt = f.cluster.getPartitionLeader(tp)
        if leaderOpt.isSome:
          f.partitions[idx[]].lastFetchedEpoch =
            max(f.partitions[idx[]].lastFetchedEpoch, leaderOpt.get.leaderEpoch)
      escalate = true
  if escalate:
    let partStr = $tp.partition
    emitMetric(
      f.metricsHook,
      meConsumerFetchValidationStall,
      [(cstring"topic", cstring(tp.topic)), (cstring"partition", cstring(partStr))],
      1,
    )
    warn "KIP-320 validation for ",
      tp.topic,
      "-",
      tp.partition,
      " stalled after ",
      f.maxValidationAttempts,
      " consecutive failures; resuming fetch without truncation validation " &
        "(re-armed on the next leader epoch change)"

proc applyValidationResult(
    f: Fetcher,
    tp: TopicPartition,
    validatedEpoch: int32,
    partResp: OffsetForLeaderPartitionResp,
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Act on one OffsetForLeaderEpoch partition result (KIP-320). On keNone,
  ## `endOffset` below our fetch position means the log was truncated under us:
  ## rewind to that safe point (orpLatest/orpEarliest) or surface it (orpNone).
  ## Stale-leader codes refresh metadata and retry; OFFSET_OUT_OF_RANGE falls to
  ## the reset path; anything else disables validation for the partition.
  if partResp.errorCode == keNone:
    var truncated = false
    var undefinedEpochOffset = false
    let endOffset = partResp.endOffset
    # `validatedEpoch` (= sent currentLeaderEpoch) is used instead of
    # partResp.leaderEpoch: suppresses re-validation until the leader advances.
    f.partitionIndex.withValue(tp, idx):
      if endOffset >= f.partitions[idx[]].fetchOffset:
        # Position still within the leader's log — record the confirmed epoch so
        # the next round does not re-validate an unchanged position.
        f.partitions[idx[]].lastFetchedEpoch =
          max(f.partitions[idx[]].lastFetchedEpoch, validatedEpoch)
        f.partitions[idx[]].awaitingValidation = false
        f.partitions[idx[]].validationAttempts = 0
        return
      truncated = true
      # UNDEFINED_EPOCH_OFFSET (-1): the broker's leader-epoch cache no longer
      # covers our epoch (purged after compaction, or the epoch is older than
      # log start). No safe rewind point; route through ListOffsets like OOR.
      if endOffset < 0:
        undefinedEpochOffset = true
    if not truncated:
      return # partition left the assignment mid-flight

    if f.offsetResetPolicy == orpNone:
      # Leave awaitingValidation set so the partition stays out of the fetch;
      # the raise surfaces via the fetch loop's error accounting.
      raise newKafkaApiError(
        keOffsetOutOfRange,
        "log truncation detected for " & tp.topic & "-" & $tp.partition &
          ": leader end offset " & $endOffset &
          " precedes fetch position and reset policy is none",
      )

    if undefinedEpochOffset:
      # Cannot use endOffset (-1) as a rewind target: resolve via ListOffsets per
      # the reset policy, then apply. `applyOffsetReset` clears awaitingValidation
      # and lastFetchedEpoch, bumps posEpoch, and refreshes lastOffsetResolve; the
      # drain drops pre-truncation queued records. As in the fetch-loop reset path,
      # a posEpoch move during the await (user seek) skips the apply.
      var validateEpoch = 0'i64
      var haveEpoch = false
      f.partitionIndex.withValue(tp, idx):
        validateEpoch = f.partitions[idx[]].posEpoch
        haveEpoch = true
      if not haveEpoch:
        return
      try:
        let resolved = await f.resolveOffsets(@[tp])
        var stillMatches = false
        f.partitionIndex.withValue(tp, idx):
          stillMatches = f.partitions[idx[]].posEpoch == validateEpoch
        if stillMatches:
          if tp in resolved:
            for resolvedTp, newOffset in resolved:
              f.applyOffsetReset(resolvedTp, newOffset)
          else:
            warn "ListOffsets returned no offset for ",
              tp.topic, "-", tp.partition,
              "; clearing validation flag to avoid retry loop"
            f.partitionIndex.withValue(tp, idx):
              f.partitions[idx[]].lastFetchedEpoch =
                max(f.partitions[idx[]].lastFetchedEpoch, validatedEpoch)
              f.partitions[idx[]].awaitingValidation = false
              f.partitions[idx[]].validationAttempts = 0
      except KafkaError as e:
        warn "Offset reset during validation for ",
          tp.topic, "-", tp.partition, " failed: ", e.msg
        f.partitionIndex.withValue(tp, idx):
          f.partitions[idx[]].awaitingValidation = false
          f.partitions[idx[]].validationAttempts = 0
      f.drainQueuedRecords(tp)
      warn "Log truncation detected for ",
        tp.topic, "-", tp.partition,
        " (undefined epoch offset); resetting via ListOffsets"
      return

    f.partitionIndex.withValue(tp, idx):
      f.partitions[idx[]].fetchOffset = endOffset
      f.partitions[idx[]].consumedOffset = endOffset
      # The rewound position sits at a log cut the previous leader epoch does
      # not cover; commit falls back to metadata (see applyOffsetReset/seek).
      f.partitions[idx[]].consumedLeaderEpoch = -1'i32
      f.partitions[idx[]].lastFetchedEpoch =
        max(f.partitions[idx[]].lastFetchedEpoch, validatedEpoch)
      f.partitions[idx[]].lastOffsetResolve = Moment.now()
      # Reposition, so bump posEpoch (invalidate in-flight fetches) as seek does.
      f.partitions[idx[]].posEpoch += 1
      f.partitions[idx[]].awaitingValidation = false
      f.partitions[idx[]].validationAttempts = 0
    f.drainQueuedRecords(tp)
    let partStr = $tp.partition
    emitMetric(
      f.metricsHook,
      meConsumerFetchTruncationDetected,
      [(cstring"topic", cstring(tp.topic)), (cstring"partition", cstring(partStr))],
      endOffset,
    )
    warn "Log truncation detected for ",
      tp.topic, "-", tp.partition, "; rewound fetch offset to ", endOffset
  elif isStaleLeaderCode(partResp.errorCode):
    # Fenced/unknown leader epoch: refresh metadata and retry next round with a fresher
    # currentLeaderEpoch (awaitingValidation stays set); the round counts toward the
    # escalation budget so a leader that never resolves cannot stall silently.
    discard await f.cluster.invalidateLeader(tp, partResp.errorCode)
    warn "OffsetForLeaderEpoch for ",
      tp.topic,
      "-",
      tp.partition,
      " returned ",
      kafkaErrorName(partResp.errorCode),
      "; refreshing metadata and retrying"
    f.recordValidationFailure(tp)
  elif partResp.errorCode == keOffsetOutOfRange:
    if f.offsetResetPolicy == orpNone:
      raise newKafkaApiError(
        keOffsetOutOfRange,
        "offset out of range during validation for " & tp.topic & "-" & $tp.partition &
          " and reset policy is none",
      )
    var validateEpoch = 0'i64
    var haveEpoch = false
    f.partitionIndex.withValue(tp, idx):
      validateEpoch = f.partitions[idx[]].posEpoch
      haveEpoch = true
    if not haveEpoch:
      return
    try:
      let resolved = await f.resolveOffsets(@[tp])
      var stillMatches = false
      f.partitionIndex.withValue(tp, idx):
        stillMatches = f.partitions[idx[]].posEpoch == validateEpoch
      if stillMatches:
        if tp in resolved:
          for resolvedTp, newOffset in resolved:
            f.applyOffsetReset(resolvedTp, newOffset)
        else:
          warn "ListOffsets returned no offset for ",
            tp.topic, "-", tp.partition,
            "; clearing validation flag to avoid retry loop"
          f.partitionIndex.withValue(tp, idx):
            f.partitions[idx[]].lastFetchedEpoch =
              max(f.partitions[idx[]].lastFetchedEpoch, validatedEpoch)
            f.partitions[idx[]].awaitingValidation = false
            f.partitions[idx[]].validationAttempts = 0
    except KafkaError as e:
      warn "Offset reset during validation for ",
        tp.topic, "-", tp.partition, " failed: ", e.msg
      f.partitionIndex.withValue(tp, idx):
        f.partitions[idx[]].awaitingValidation = false
        f.partitions[idx[]].validationAttempts = 0
    f.drainQueuedRecords(tp)
  else:
    # Unexpected per-partition code (e.g. the API reported unsupported per-partition):
    # skip this round so we don't loop; markPartitionsForValidation re-sets the flag on
    # the next leader-epoch advance.
    warn "OffsetForLeaderEpoch for ",
      tp.topic,
      "-",
      tp.partition,
      " returned ",
      kafkaErrorName(partResp.errorCode),
      "; skipping validation this round"
    f.partitionIndex.withValue(tp, idx):
      f.partitions[idx[]].awaitingValidation = false
      f.partitions[idx[]].validationAttempts = 0

proc validateOffsets(f: Fetcher) {.async: (raises: [KafkaError, CancelledError]).} =
  ## KIP-320 truncation check for partitions flagged `awaitingValidation`. Groups
  ## them per broker (like resolveOffsets) and sends OffsetForLeaderEpoch with the
  ## last consumed epoch; applyValidationResult acts on each reply. A pre-KIP-320
  ## broker (keUnsupportedVersion) disables validation for its partitions.
  if f.cluster == nil:
    return

  # Snapshot TPs up front so a concurrent setPartitions cannot invalidate our
  # indices mid-loop. Reads below re-resolve via partitionIndex.
  var candidateTps: seq[TopicPartition]
  for i in 0 ..< f.partitions.len:
    if f.partitions[i].awaitingValidation:
      candidateTps.add(f.partitions[i].tp)

  var byBroker = initTable[int32, seq[TopicPartition]]()
  var brokerMap = initTable[int32, Broker]()
  for tp in candidateTps:
    let broker =
      try:
        await f.cluster.brokerForPartition(tp)
      except KafkaError as e:
        warn "validateOffsets: no broker for ", tp.topic, "-", tp.partition, ": ", e.msg
        f.recordValidationFailure(tp)
        continue
    # Drop partitions whose current leader has already refused OffsetForLeaderEpoch;
    # clearing awaitingValidation lets fetchOnce include them normally, and a future
    # leader change to a supported broker re-flags them via markPartitionsForValidation.
    if broker.nodeId in f.epochValidationUnsupportedBrokers:
      f.partitionIndex.withValue(tp, idx):
        f.partitions[idx[]].awaitingValidation = false
        f.partitions[idx[]].validationAttempts = 0
      continue
    brokerMap[broker.nodeId] = broker
    byBroker.mgetOrPut(broker.nodeId, @[]).add(tp)

  for nodeId, tps in byBroker:
    var broker: Broker
    brokerMap.withValue(nodeId, b):
      broker = b[]
    do:
      continue

    var topicReqs = initTable[string, seq[OffsetForLeaderPartitionReq]]()
    var sentEpoch = initTable[TopicPartition, int32]()
    # posEpoch snapshot: replies for a partition repositioned mid-flight are dropped.
    var sentPosEpoch = initTable[TopicPartition, int64]()
    for tp in tps:
      let leaderOpt = f.cluster.getPartitionLeader(tp)
      let observedEpoch = if leaderOpt.isSome: leaderOpt.get.leaderEpoch else: -1'i32
      # Clamp to the monotone ceiling (same as fetchFromBroker).
      let prevEpoch = f.lastLeaderEpoch.getOrDefault(tp, -1'i32)
      let curEpoch = max(observedEpoch, prevEpoch)
      if curEpoch != prevEpoch:
        f.lastLeaderEpoch[tp] = curEpoch
      var lastEpoch = -1'i32
      f.partitionIndex.withValue(tp, idx):
        lastEpoch = f.partitions[idx[]].lastFetchedEpoch
        sentPosEpoch[tp] = f.partitions[idx[]].posEpoch
      sentEpoch[tp] = curEpoch
      topicReqs.mgetOrPut(tp.topic, @[]).add(
        OffsetForLeaderPartitionReq(
          partition: tp.partition, currentLeaderEpoch: curEpoch, leaderEpoch: lastEpoch
        )
      )

    var topics: seq[OffsetForLeaderTopicReq]
    for topic, parts in topicReqs:
      topics.add(OffsetForLeaderTopicReq(topic: topic, partitions: parts))

    let req = OffsetForLeaderEpochRequest(replicaId: -2, topics: topics)
    var resp: OffsetForLeaderEpochResponse
    var gotResp = false
    try:
      resp = await broker.offsetForLeaderEpoch(req)
      gotResp = true
    except KafkaApiError as e:
      if e.errorCode == keUnsupportedVersion:
        # Record broker-level unsupported so future rounds skip it entirely
        # (markPartitionsForValidation / the candidate filter above).
        f.epochValidationUnsupportedBrokers.incl(nodeId)
        for tp in tps:
          f.partitionIndex.withValue(tp, idx):
            f.partitions[idx[]].awaitingValidation = false
            f.partitions[idx[]].validationAttempts = 0
        warn "OffsetForLeaderEpoch unsupported on broker ",
          nodeId, "; disabling truncation validation for its partitions"
      else:
        warn "OffsetForLeaderEpoch RPC failed on broker ",
          nodeId, ": ", e.msg, "; will retry"
        for tp in tps:
          f.recordValidationFailure(tp)
    except KafkaError as e:
      warn "OffsetForLeaderEpoch RPC failed on broker ",
        nodeId, ": ", e.msg, "; will retry"
      for tp in tps:
        f.recordValidationFailure(tp)
    if not gotResp:
      continue

    var respondedTps: HashSet[TopicPartition]
    for topicResp in resp.topics:
      for partResp in topicResp.partitions:
        let tp = TopicPartition(topic: topicResp.topic, partition: partResp.partition)
        # Drop unrequested or repositioned partitions (see sentPosEpoch above).
        # -1 sentinel is safe since posEpoch is monotone non-negative.
        let snap = sentPosEpoch.getOrDefault(tp, -1'i64)
        if snap < 0:
          continue
        respondedTps.incl(tp)
        var stale = true
        f.partitionIndex.withValue(tp, idx):
          if f.partitions[idx[]].posEpoch == snap:
            stale = false
        if stale:
          continue
        await f.applyValidationResult(tp, sentEpoch.getOrDefault(tp, -1'i32), partResp)

    # A requested partition the broker omitted from the reply would otherwise
    # stay flagged with no round counted against it — an unbounded stall with
    # no RPC failure to trigger the budget. Count it like any other failure.
    for tp in tps:
      if tp notin respondedTps:
        f.recordValidationFailure(tp)

proc markPartitionsForValidation(f: Fetcher) {.raises: [].} =
  ## KIP-320: flag any partition whose cached leaderEpoch advanced past
  ## `lastFetchedEpoch` (its position's known-valid epoch) so a leader change is
  ## validated before fetching again. Skips paused / unknown-epoch / unsupported.
  if f.cluster == nil:
    return
  for i in 0 ..< f.partitions.len:
    let ps = f.partitions[i]
    if ps.paused or ps.awaitingValidation or ps.lastFetchedEpoch < 0:
      continue
    let leaderOpt = f.cluster.getPartitionLeader(ps.tp)
    if leaderOpt.isNone:
      continue
    let leader = leaderOpt.get
    if leader.nodeId in f.epochValidationUnsupportedBrokers:
      continue
    if leader.leaderEpoch > ps.lastFetchedEpoch:
      f.partitions[i].awaitingValidation = true

proc fetchOnce(f: Fetcher) {.async: (raises: [KafkaError, CancelledError]).} =
  if f.partitions.len == 0:
    await sleepAsync(milliseconds(100))
    return

  # KIP-320: flag partitions whose leader epoch advanced and validate them
  # (OffsetForLeaderEpoch) before fetching; flagged ones stay out of the fetch below
  # until validation clears them, catching truncation before rolled-back records ship.
  f.markPartitionsForValidation()
  try:
    await f.validateOffsets()
  except CancelledError:
    raise
  except KafkaApiError as e:
    if isPermanent(e.errorCode):
      raise
    f.trackFetchError(e.msg)
    warn "Offset validation error: ", e.msg
  except KafkaError as e:
    f.trackFetchError(e.msg)
    warn "Offset validation error: ", e.msg

  # Group partitions by broker, preserving order. Paused partitions are excluded
  # so they don't consume the broker's per-fetch budget.
  var brokerIds: seq[int32]
  var brokerParts: seq[seq[int]]
  var brokerSlot = initTable[int32, int]() # nodeId -> index into brokerIds
  for i, ps in f.partitions:
    if ps.paused or ps.awaitingValidation:
      continue
    let nodeId = ps.broker.nodeId
    brokerSlot.withValue(nodeId, idx):
      brokerParts[idx[]].add(i)
    do:
      brokerSlot[nodeId] = brokerIds.len
      brokerIds.add(nodeId)
      brokerParts.add(@[i])

  # All partitions paused: sleep to avoid busy-looping, capped at 100ms (not the
  # full maxWaitMs) so resume() takes effect quickly without an interrupt path. A
  # maxWaitMs below 100ms is still honoured.
  if brokerIds.len == 0:
    let pausedSleepMs = min(f.maxWaitMs, 100'i32)
    await sleepAsync(milliseconds(pausedSleepMs))
    return

  var futures: seq[Future[bool]]
  for idx in 0 ..< brokerIds.len:
    futures.add(fetchFromBroker(f, brokerParts[idx]))
  await allFutures(futures)

  # Re-raise only cancellation and permanent errors; log transient ones. Track
  # per-broker success/failure to detect persistently failing brokers even when the
  # global round partially succeeds.
  var anySucceeded = false
  for i, fut in futures:
    let nodeId = brokerIds[i]
    if fut.failed:
      let err = fut.error
      if err of CancelledError:
        raise (ref CancelledError)(err)
      elif err of KafkaApiError and isPermanent((ref KafkaApiError)(err).errorCode):
        raise (ref KafkaError)(err)
      elif err of KafkaDecompressionLimitError:
        # Budget violations are fatal, not transient: retrying cannot change the
        # response's decompressed size. Re-raise so fetchLoop surfaces the error
        # instead of retrying forever.
        raise (ref KafkaError)(err)
      elif err of KafkaError:
        f.trackFetchError(err.msg)
        warn "Fetch error (non-fatal): ", err.msg
      f.brokerFetchErrors.mgetOrPut(nodeId, 0).inc
    else:
      # `allFutures` guarantees every child is finished. Cancellation remains a
      # real fetch outcome, while `value` is safe after excluding that state.
      if fut.cancelled:
        raise (ref CancelledError)(fut.error)
      let succeeded = fut.value
      if succeeded:
        anySucceeded = true
        f.brokerFetchErrors.del(nodeId)
      else:
        f.brokerFetchErrors.mgetOrPut(nodeId, 0).inc

  if anySucceeded:
    f.consecutiveFetchErrors = 0
    f.lastError = nil
    f.consecutiveRestartAttempts = 0

  # Recover persistently failing brokers: force metadata refresh so their
  # partitions can be reassigned to a new leader (if one has been elected).
  for i, nodeId in brokerIds:
    let errCount = f.brokerFetchErrors.getOrDefault(nodeId, 0)
    if errCount >= f.maxConsecutiveFetchErrors:
      warn "Broker ",
        nodeId, " has ", errCount,
        " consecutive fetch failures; refreshing metadata for its partitions"
      await f.reassignBrokerPartitions(brokerParts[i])
      f.brokerFetchErrors.del(nodeId)

proc fetchLoop(f: Fetcher) {.async: (raises: []).} =
  while f.running:
    if f.rebalanceCheck != nil and f.rebalanceCheck():
      try:
        await sleepAsync(milliseconds(100))
      except CancelledError:
        return
      continue

    # Backpressure: wait for queue space in short steps instead of sleeping the
    # full maxWaitMs, which would add unnecessary latency.
    if f.queueFull:
      f.queueFull = false
      const backpressureCheckMs = 50
      var waited = 0'i32
      while f.running and f.records.full():
        try:
          await sleepAsync(milliseconds(backpressureCheckMs))
        except CancelledError:
          return
        waited += backpressureCheckMs
        if waited >= f.maxWaitMs:
          break

    try:
      await f.fetchOnce()
    except KafkaError as e:
      if e of KafkaApiError and isPermanent((ref KafkaApiError)(e).errorCode):
        error "Permanent fetch error, stopping: ", e.msg
        f.lastError = e
        return
      elif e of KafkaDecompressionLimitError:
        error "Fetch response exceeded the total decompression budget, stopping: ",
          e.msg
        f.lastError = e
        return
      try:
        await sleepAsync(milliseconds(1000))
      except CancelledError:
        return
    except CancelledError:
      return

proc isLoopAlive*(f: Fetcher): bool {.raises: [].} =
  ## True when the fetch loop is armed and its coroutine has not returned. Used by
  ## consumer.tryRestartFetcher to distinguish "still retrying" from "loop returned
  ## but running flag stuck" (the two paths need opposite handling).
  f.running and f.fetchLoopFut != nil and not f.fetchLoopFut.finished

proc forceRunningFalse*(f: Fetcher) {.raises: [].} =
  ## Clear the running flag from outside the loop; tryRestartFetcher uses it to
  ## normalize state after fetchLoop returned without resetting the flag, so start()
  ## actually restarts instead of no-opping.
  f.running = false

proc start*(f: Fetcher) =
  if f.running:
    return
  f.running = true
  f.lastError = nil
  f.consecutiveFetchErrors = 0
  f.brokerFetchErrors.clear()
  f.fetchLoopFut = fetchLoop(f)

proc advanceConsumedOffset*(f: Fetcher, rec: FetchedRecord) =
  ## Update the consumed offset for the partition of the given record; call after the
  ## user polls it. Rejects a record whose stamped `posEpoch` no longer matches the
  ## partition's: a reposition (seek / offset reset / KIP-320 truncation) landed since
  ## it was built, and applying its offset would clobber the realigned `consumedOffset`.
  ## Records already in the consumer's `pendingAdvance` are unreachable by
  ## `drainQueuedRecords`, so the guard must live here.
  ##
  ## The max() clamp against the current `consumedOffset` cheaply prevents a silent
  ## rewind should a future refactor reorder records or forget the epoch bump.
  let tp = TopicPartition(topic: rec.topic, partition: rec.partition)
  let nextOffset = rec.offset + 1
  f.partitionIndex.withValue(tp, idx):
    if rec.posEpoch != f.partitions[idx[]].posEpoch:
      return
    if nextOffset > f.partitions[idx[]].consumedOffset:
      f.partitions[idx[]].consumedOffset = nextOffset
      # Only the newest-offset winner sets the epoch (same clamp as offset).
      f.partitions[idx[]].consumedLeaderEpoch = rec.leaderEpoch
    f.partitions[idx[]].everConsumed = true

proc advanceConsumedOffsets*(f: Fetcher, recs: openArray[FetchedRecord]) =
  ## Bulk advance for a batch returned by pollBatch / pollBatchStrict. Matches
  ## Java's Fetcher.fetchedRecords() semantics: position moves on the NEXT poll, not
  ## while records are collected, so a user-cancelled / timed-out batch cannot mark
  ## records consumed without the caller observing them.
  for rec in recs:
    f.advanceConsumedOffset(rec)

proc pause*(f: Fetcher, tps: openArray[TopicPartition]) {.raises: [KafkaError].} =
  ## Pause fetching for `tps`; later fetchOnce rounds skip them until `resume`. Already
  ## queued records are still delivered (pause affects future fetches, not the buffer).
  ## Raises `KafkaError` for a partition not in the current assignment; also drops each
  ## paused partition's broker KIP-227 fetch session (see `FetchSession`), since under
  ## an active session the broker RETAINS an omitted partition (removal needs
  ## `forgottenTopicsData`, which we never emit) and excluding it would not stop its
  ## records. `resume()` needs no equivalent.
  for tp in tps:
    if tp notin f.partitionIndex:
      raise newException(
        KafkaError,
        "pause: partition " & tp.topic & "-" & $tp.partition &
          " is not in the current assignment",
      )

  for tp in tps:
    f.partitionIndex.withValue(tp, idx):
      f.partitions[idx[]].paused = true
      let b = f.partitions[idx[]].broker
      if b != nil:
        f.fetchSessions.del(b.nodeId)
        # Bump the per-broker pause epoch so an in-flight fetch's response handler
        # can tell pause() ran mid-flight and skip re-installing the KIP-227 session
        # it just cleared; otherwise the response's sessionId would revive the
        # broker-side session (still holding the paused partition).
        f.pauseEpochs[b.nodeId] = f.pauseEpochs.getOrDefault(b.nodeId, 0'i64) + 1

proc resume*(f: Fetcher, tps: openArray[TopicPartition]) {.raises: [KafkaError].} =
  ## Resume fetching for the given partitions: the next fetchOnce round includes
  ## them at their current `fetchOffset`. Raises `KafkaError` for a partition not in
  ## the current assignment.
  for tp in tps:
    if tp notin f.partitionIndex:
      raise newException(
        KafkaError,
        "resume: partition " & tp.topic & "-" & $tp.partition &
          " is not in the current assignment",
      )

  for tp in tps:
    f.partitionIndex.withValue(tp, idx):
      f.partitions[idx[]].paused = false

proc seek*(f: Fetcher, tp: TopicPartition, offset: int64) {.raises: [KafkaError].} =
  ## Set the next fetch position for `tp` to `offset`, dropping already-queued
  ## records for `tp` (other partitions are preserved) so no pre-seek data is
  ## delivered. Raises `KafkaError` if `tp` is unassigned or `offset` is negative.
  if offset < 0:
    raise
      newException(KafkaError, "seek: negative offset " & $offset & " for " & tp.topic)

  if tp notin f.partitionIndex:
    raise newException(
      KafkaError,
      "seek: partition " & tp.topic & "-" & $tp.partition &
        " is not in the current assignment",
    )

  f.partitionIndex.withValue(tp, idx):
    f.partitions[idx[]].fetchOffset = offset
    # Match the Java client: seek() is an explicit choice, so the offset is immediately
    # committable with no intervening poll(). `everConsumed` makes getConsumedOffsets
    # (and the next commit) return it — the "rewind/forward then commit" workflow.
    f.partitions[idx[]].consumedOffset = offset
    # seek() carries no epoch; commit falls back to metadata.
    f.partitions[idx[]].consumedLeaderEpoch = -1'i32
    f.partitions[idx[]].everConsumed = true
    f.partitions[idx[]].lastOffsetResolve = default(Moment)
    # Invalidate any in-flight response built against the pre-seek position (see
    # PartitionState.posEpoch): the drain below removes already-enqueued stale
    # records, the epoch bump removes in-flight ones, without which a late response
    # would clobber fetchOffset back via computeNextFetchOffset.
    f.partitions[idx[]].posEpoch += 1
    # The user chose this position explicitly, so any pending KIP-320 validation
    # of the old position is moot; restart truncation tracking from scratch.
    f.partitions[idx[]].lastFetchedEpoch = -1'i32
    f.partitions[idx[]].awaitingValidation = false
    f.partitions[idx[]].validationAttempts = 0

  # Synchronous (no awaits), so chronos cannot interleave a poll() observing partial
  # state.
  f.drainQueuedRecords(tp)

proc isPaused*(f: Fetcher, tp: TopicPartition): bool {.raises: [].} =
  f.partitionIndex.withValue(tp, idx):
    return f.partitions[idx[]].paused
  false

proc pausedPartitions*(f: Fetcher): seq[TopicPartition] {.raises: [].} =
  for ps in f.partitions:
    if ps.paused:
      result.add(ps.tp)

proc stop*(f: Fetcher) {.async: (raises: []).} =
  f.running = false
  if f.fetchLoopFut != nil and not f.fetchLoopFut.finished:
    # Reaping the loop absorbs a cancellation rather than unwinding: `stop()` is
    # called from `close()`, which never re-runs its teardown, so a cancel landing
    # here would leave the consumer in `lsClosing` and the loop to unwind against a
    # cluster being closed. The loop is already scheduled for cancellation and never
    # fails, so waiting again cannot raise and cannot lose the cancel.
    let loopFut = f.fetchLoopFut
    while not loopFut.finished:
      try:
        await loopFut.cancelAndWait()
      except CancelledError:
        absorbDuringTeardown("fetcher::stop", "cancel reaping the fetch loop")
    f.fetchLoopFut = nil

  # Clear residual error: a parked rejoin's next poll would otherwise
  # tryRestartFetcher under the OLD assignment (delivery gap). Outer callers
  # that need to escalate a failure call `setLastError` AFTER stop().
  f.lastError = nil

  # Drop KIP-227 fetch sessions: after a rebalance/restart the assignment may
  # differ, and reusing a broker session would need a forgottenTopics diff we
  # don't emit. Starting fresh next start() is cheap (one full fetch per broker).
  f.fetchSessions.clear()
  f.pauseEpochs.clear()

  # Reset broker-level OffsetForLeaderEpoch suppression so a fresh start()
  # re-probes each broker's API support.
  f.epochValidationUnsupportedBrokers.clear()

  # Drain remaining records WITHOUT advancing consumed offsets: only records the
  # user actually polled may be committed, and advancing here would mark unprocessed
  # records consumed and skip them after a rebalance (data loss).
  while not f.records.empty():
    try:
      discard f.records.getNoWait()
    except AsyncQueueEmptyError:
      break

proc spawnStop*(f: Fetcher) {.gcsafe, raises: [].} =
  ## Start a stop() from a synchronous, non-awaiting caller (the heartbeat
  ## permanent-error hook) and retain its future so consumer.close() can
  ## reclaim it via joinExitStop() instead of leaving it untracked. stop() is
  ## idempotent, so racing this against close()'s own stop() is safe.
  f.exitStopFut = f.stop()

proc joinExitStop*(f: Fetcher) {.async: (raises: []).} =
  ## Await a stop() previously started via spawnStop(), if any, so no fetcher
  ## stop coroutine outlives consumer.close().
  if f.exitStopFut != nil and not f.exitStopFut.finished:
    await f.exitStopFut
  f.exitStopFut = nil

proc fetchErrorCount*(f: Fetcher): int =
  ## Consecutive fetch errors since the last success.
  f.consecutiveFetchErrors

proc recordQueueCapacity*(f: Fetcher): int {.raises: [].} =
  ## Records the internal AsyncQueue holds before fetchOnce stops issuing fetches;
  ## cached at construction because chronos's AsyncQueue hides its `maxsize`.
  f.recordQueueMaxSize

proc setLastError*(f: Fetcher, e: ref KafkaError) {.raises: [].} =
  ## Mark the fetcher as failed with `e`. tryRestartFetcher reads it so an outer
  ## error (e.g. rebalance retry exhaustion) that stopped the loop without an
  ## internal failure still triggers an automatic restart on the next poll.
  f.lastError = e

proc getFetchOffsets*(f: Fetcher): Table[TopicPartition, int64] =
  for ps in f.partitions:
    result[ps.tp] = ps.fetchOffset

proc getConsumedOffsets*(f: Fetcher): Table[TopicPartition, int64] =
  # Only partitions whose consumedOffset reflects a user observation or explicit
  # seek: skipping the placeholder copies written by setPartitions prevents
  # commitSync after a poll-less window from writing future offsets (orpLatest) or
  # reverting another consumer's work.
  for ps in f.partitions:
    if ps.everConsumed:
      result[ps.tp] = ps.consumedOffset

proc getConsumedLeaderEpochs*(f: Fetcher): Table[TopicPartition, int32] =
  ## Companion to `getConsumedOffsets`: `committedLeaderEpoch` per partition, epochs
  ## >= 0 only; missing entries fall back to cluster metadata on the commit side.
  for ps in f.partitions:
    if ps.everConsumed and ps.consumedLeaderEpoch >= 0:
      result[ps.tp] = ps.consumedLeaderEpoch

proc consumedOffsetsAfter*(
    f: Fetcher, recs: openArray[FetchedRecord]
): Table[TopicPartition, int64] =
  ## The committable offsets `advanceConsumedOffsets(recs)` then `getConsumedOffsets()`
  ## would yield, WITHOUT mutating the live position, so commitSync can commit the
  ## post-advance position but defer the advance until the broker accepts. Mirrors
  ## advanceConsumedOffset's posEpoch gate and max() clamp so a reposition landing
  ## between poll and commitSync never surfaces a pre-reposition offset.
  result = f.getConsumedOffsets()
  for rec in recs:
    let tp = TopicPartition(topic: rec.topic, partition: rec.partition)
    f.partitionIndex.withValue(tp, idx):
      if rec.posEpoch != f.partitions[idx[]].posEpoch:
        continue
      let nextOffset = rec.offset + 1
      # max() clamp mirrors advanceConsumedOffset — never rewind, even in the
      # unexpected case of a same-epoch out-of-order batch element.
      let cur = result.getOrDefault(tp, low(int64))
      if nextOffset > cur:
        result[tp] = nextOffset

proc consumedLeaderEpochsAfter*(
    f: Fetcher, recs: openArray[FetchedRecord]
): Table[TopicPartition, int32] =
  ## Epoch snapshot paired with `consumedOffsetsAfter(recs)`: the newest-offset
  ## record's leaderEpoch wins per partition, posEpoch mismatches are dropped, and
  ## epoch < 0 is omitted so commit falls back to cluster metadata.
  result = f.getConsumedLeaderEpochs()
  var maxOffset = initTable[TopicPartition, int64]()
  for tp, off in f.getConsumedOffsets():
    maxOffset[tp] = off
  for rec in recs:
    let tp = TopicPartition(topic: rec.topic, partition: rec.partition)
    f.partitionIndex.withValue(tp, idx):
      if rec.posEpoch != f.partitions[idx[]].posEpoch:
        continue
      let nextOffset = rec.offset + 1
      let cur = maxOffset.getOrDefault(tp, low(int64))
      if nextOffset > cur:
        maxOffset[tp] = nextOffset
        if rec.leaderEpoch >= 0:
          result[tp] = rec.leaderEpoch
        else:
          result.del(tp)
