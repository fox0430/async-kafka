## Shared producer types and low-level helpers. The facade re-exports only the
## application-facing ones; the rest are internal and carry no SemVer guarantee.

import std/[tables, options, sets]
from std/times import epochTime

import ../[async_backend, errors, broker, cluster, group_metadata]
import ../protocol/[messages, compression]
import ../internal/[callback_guard, metrics]

export metrics.MetricsHook
export group_metadata

type
  TransactionState* = enum
    ## Producer transaction state machine. `canTransition` holds the legal edges;
    ## `tsFatal` is terminal and the producer must be closed. A fatal producer
    ## refuses `abortTransaction`, and `close` does not abort on its behalf, so a
    ## transaction left open in that state is reaped by the coordinator at
    ## `transactionTimeoutMs`.
    tsUninitialized
    tsReady
    tsInTransaction
    tsCommitting
    tsAborting
    tsError
    tsFatal

  PartitionStrategy* = enum
    ## How `send` picks a partition when none is given.
    psRoundRobin ## Cycle through partitions (default)
    psKeyHash ## Kafka murmur2 of the key; round-robin when keyless
    psSticky
      ## One partition until its batch flushes; key hash when a key is present.
      ## Meant for `lingerMs > 0`: with `lingerMs <= 0` every send is its own
      ## batch, so keyless records rotate on every successful send (round-robin).

  CustomPartitioner* = proc(
    topic: string, key: Option[seq[byte]], partitionCount: int32
  ): int32 {.closure, gcsafe, raises: [].}
    ## Runs on the send path: must be pure, cheap, and non-raising. An out-of-range
    ## result fails the send, as does a `CatchableError` raised anyway (wrapped in a
    ## `KafkaError` as its `parent`); a `Defect` is not caught.

  PendingMessage* = object
    record*: Record
    future*: Future[ProduceResult]
    tp*: TopicPartition
    estimatedBytes*: int32
      ## On-wire size estimate, summed by the byte-cap flush trigger.

  InflightBatch* = object
    ## A batch allocated a baseSequence and possibly in flight. KIP-185 requires
    ## the sequence to be frozen at first encode so retries reuse the same wire
    ## bytes; a producer-id reset re-sequences the queues from 0 via
    ## `reassignSequencesAfterReset`, which invalidates `encodedBytes`, while an
    ## epoch bump clears the sequence table.
    id*: int64
      ## Lets a retry coroutine re-locate its batch after a re-sequence;
      ## position-based lookup is unsafe once `maxInFlight > 1`.
    records*: seq[Record]
    firstTimestamp*: int64
    baseSequence*: int32
    producerIdAtEncode*: int64
    producerEpochAtEncode*: int16
    resetEpochAtEncode*: int64
    encodedBytes*: seq[byte]

  KafkaProducer* = ref object
    ## Bound to the chronos event loop that built it, and safe to share between the
    ## coroutines on that loop: `send` and the transaction calls serialize
    ## internally, and a second `close` waits for the teardown already in progress.
    ##
    ## Per-partition state is reclaimed once a partition goes idle, so a long-lived
    ## producer over a churning set of partitions does not accumulate it. The
    ## exception is the idempotent producer's sequence number, which the broker
    ## expects to continue per `(producerId, partition)`: it is kept for every
    ## partition written under the current producer-id, as in the Java client, and
    ## cleared on `close()` or a producer-id reset.

    # Locking rules:
    #   - txnLock → initLock → batchLock is the nesting order. initLock covers the
    #     idempotence bootstrap (InitProducerId); the epoch bump deliberately does
    #     not take it, running its I/O lock-free and applying in one synchronous
    #     block. Never await on txnLock / initLock while batchLock is held.
    #   - pendingRecordsSem is taken before batchLock (backpressure queues
    #     outside the critical section) and must never be taken after it.
    #   - `inflightSemaphores[tp]` is per-partition and taken outside batchLock.
    #     `writeChainTail[tp]` is entered OUTSIDE txnLock/initLock/batchLock: it
    #     awaits its predecessor, then allocateInflight takes initLock inside it.
    #     Never take the gate while holding those locks.
    #   - fireAndForgetSem is held across the whole background task, which calls
    #     `send` and so acquires the locks above: it is outermost, and must never
    #     be taken while holding them.
    #
    # The DLQ fields are not lock protected: `setDeadLetterQueue` swaps all
    # three in one synchronous turn and every reader captures them before any
    # await.
    cluster: Cluster
    acks: int16
    timeoutMs: int32
    compression: CompressionCodec
    lingerMs: int32
    maxBatchRecords: int32
    maxBatchBytes: int32
      ## Kafka Java's `batch.size`, enforced against the per-record
      ## `estimateRecordSize` (so the encoded batch may differ slightly): a record
      ## that would push the batch over the cap seals and flushes the in-progress
      ## batch first, and a single oversized record is rejected at send() instead of
      ## by the broker.
    maxRetries: int32
    deliveryTimeoutMs: int32
      ## Kafka Java's `delivery.timeout.ms`: wall-clock bound on one send across all
      ## retries, needed because idempotent/transactional producers force
      ## `maxRetries = int32.high`. `<= 0` means the count cap alone applies.
    retryBackoffMs: int32
    maxInFlightRequests: int32
    enableIdempotence: bool
    producerId: int64
    producerEpoch: int16
    sequenceNumbers: Table[TopicPartition, int32]
      ## Next baseSequence per partition (idempotent only). Not reclaimed when a
      ## partition goes idle: restarting it at 0 would be fenced as
      ## OUT_OF_ORDER_SEQUENCE_NUMBER. Cleared on close, resetProducerId, and every
      ## epoch bump.
    idempotenceInitialized: bool
    initLock: AsyncLock
    partitionCounters: Table[string, int32]
    batches: Table[TopicPartition, seq[PendingMessage]]
    pendingBatchBytes: Table[TopicPartition, int64]
      ## Sum of `estimatedBytes` in `batches[tp]`, kept under `batchLock` so the
      ## byte-cap flush trigger reads an O(1) total.
    lingerTimers: Table[TopicPartition, Future[void]]
    lingerTimerEpoch: Table[TopicPartition, uint64]
      ## Bumped by renewLingerTimer. A lingerTimeout that finds its epoch stale
      ## returns without side effects, so it cannot drop a successor's timer entry.
    batchLock: AsyncLock
    inflightSemaphores: Table[TopicPartition, AsyncSemaphore]
    writeChainTail: Table[TopicPartition, Future[void]]
      ## Per-partition write chain (KIP-185 in-order writes with `maxInFlight > 1`,
      ## idempotence only): a send awaits its predecessor before allocating a
      ## baseSequence, then completes its own once the write is enqueued.
    completionChainTail: Table[TopicPartition, Future[void]]
      ## Orders user-visible completions the way the write chain orders writes, so
      ## a batch is never acked ahead of an earlier-sequence in-flight one. Markers
      ## are only ever completed, so the chain cannot deadlock. Idempotence only.
    state: LifecycleState
    clusterOwned: bool ## True when this producer built its Cluster and must close it.
    closeFuture: Future[void]
      ## The in-progress close() coroutine, so racing close() calls resolve to one
      ## teardown sequence instead of two drains against the same tables.
    when defined(kafkaCloseTestHooks):
      closeLingerPark: Future[void].Raising([CancelledError])
        ## Test-only. When armed, the linger-reaping stage of close() waits on it,
        ## so a test can cancel the close while it is inside that stage.
      closeLingerParked: bool
        ## Test-only. True while close() waits on `closeLingerPark`.
    transactionalId: Option[string]
      ## Kafka transaction id (KIP-98); non-empty enables the transactional API.
      ## Validated at construction; `none` means a plain or idempotent producer.
    transactionTimeoutMs: int32
    txnState: TransactionState
    txnSeq: int64
      ## Bumped by every `beginTransaction`, so an awaiting call can tell its txn from
      ## a later one (the state alone cannot).
    txnUnjoinedSends: int
      ## `send` calls of the current txn whose record has yet to join it. A commit
      ## refuses while any remain: once it has begun they can no longer join.
    txnLock: AsyncLock
    txnCoordinator: Broker
    txnPartitions: HashSet[TopicPartition]
    txnPendingPartitions: HashSet[TopicPartition]
    txnStarted: bool
      ## True once the coordinator has the transaction (AddPartitionsToTxn /
      ## AddOffsetsToTxn moved it out of `Empty`). When false, commit/abort skip
      ## the EndTxn round-trip, which the coordinator would reject with
      ## INVALID_TXN_STATE.
    inflightFlushFutures: seq[Future[void]]
      ## Markers for flush/produce calls that must settle before flush(), commit, or
      ## abort returns; registered before the batch leaves `batches`, so flush() can
      ## await a size-/linger-triggered flush it no longer sees.
    pendingEpochBump: bool
      ## Set when EndTxn succeeded but the immediate epoch bump ran out of retries;
      ## the bump is deferred to the next send/commit/abort/sendOffsetsToTransaction
      ## (beginTransaction cannot await).
    epochBumpInflight: bool
      ## True while a single-flight epoch bump runs; concurrent callers wait on
      ## `epochBumpDone` instead of firing a duplicate InitProducerId.
    epochBumpDone: Future[void]
      ## Completes when the single-flight bump settles; nil while none is in flight.
    errorHandler: ErrorHandler
    errorHandlerFailures: CallbackFailureLog
      ## Reports an `errorHandler` that raises despite its `raises: []` type.
    produceCallbackFailures: CallbackFailureLog
      ## Same for a `sendFireAndForget` callback.
    dlqTopic: string
    dlqProducer: KafkaProducer
    ownsDlqProducer: bool
      ## When true, `close()` cascades into `dlqProducer.close()`; a shared DLQ
      ## producer stays the caller's to close.
    partitionStrategy: PartitionStrategy
    customPartitioner: CustomPartitioner
    stickyPartitions: Table[string, int32]
    fireAndForgetSem: AsyncSemaphore
    pendingFireAndForget: seq[Future[void]]
    resetEpoch: int64
    nextInflightId: int64
      ## Gives each queued `InflightBatch` a stable id through a re-sequence.
    inflightBatches: Table[TopicPartition, seq[InflightBatch]]
      ## Batches allocated a baseSequence and possibly in flight (KIP-185,
      ## idempotence only). A Produce response dequeues the batch by `id`, not the
      ## queue front, which may belong to another in-flight batch; a producer-id
      ## reset re-sequences the queue.
    # Cross-partition cap on records queued by batched send() calls: bounds memory
    # when the broker is slower than the workload. Unrelated to maxBatchRecords.
    pendingRecordsSem: AsyncSemaphore
    maxPendingRecords: int32
    metricsHook: MetricsHook

  ProduceStatus* = enum
    ## Per-record outcome. Distinct prefix from `PartitionStrategy`'s `ps*`.
    pstSuccess ## The broker acknowledged the record.
    pstDropped ## An `eaDrop` handler discarded the failed record.
    pstDeadLettered ## The failed record was redirected to the DLQ.
    pstDeadLetterFailed
      ## `eaDeadLetter` was selected but the DLQ send failed, so the original
      ## failure was not redirected.

  ProduceResult* = object ## Send outcome for one record.
    topic*: string
    partition*: int32
    offset*: int64
    status*: ProduceStatus

  ProduceCallback* =
    proc(result: ProduceResult, error: ref KafkaError) {.closure, gcsafe, raises: [].}
    ## Exactly one of `result` / `error` signals the outcome. Runs on the
    ## completion path: keep it short and non-raising (a `CatchableError` raised
    ## anyway is logged along with `error`; a `Defect` is not caught).

  ErrorAction* = enum
    ## Failed-record routing.
    eaFail
    eaDrop
    eaDeadLetter

  FailedRecord* = object ## Read-only input to `ErrorHandler`.
    topic*: string
    partition*: int32
    key*: Option[seq[byte]]
    value*: Option[seq[byte]]
    headers*: seq[RecordHeader]
    error*: ref KafkaError
    attempts*: int32
    timestamp*: int64

  ErrorHandler* =
    proc(record: FailedRecord): ErrorAction {.closure, gcsafe, raises: [].}
    ## Must not raise (a raise is logged and falls back to `eaFail`, as with no
    ## handler); transactional sends bypass it entirely, so the failure surfaces to
    ## `send` and the transaction cannot commit without the record.

proc toBytes*(s: string): seq[byte] =
  ## `s` as a `seq[byte]` copy, for the record value/key arguments. Mutating the
  ## result cannot reach `s`, unlike a `cast` of the same string.
  @(s.toOpenArrayByte(0, s.high))

proc resolveTimestamp*(timestamp: int64): int64 =
  ## Current epoch milliseconds when `timestamp` is 0, else `timestamp`.
  if timestamp == 0:
    int64(epochTime() * 1000)
  else:
    timestamp

proc txnErrorTags*(
    errorCode, errorClass: string
): seq[(string, string)] {.raises: [].} =
  ## Tag shape of every `producer.txn.*.error` event, kept in one place so the
  ## commit/abort twins cannot drift key by key. A failure with no broker error
  ## code still passes a client-side `error_code`, so the dimension is never missing.
  @[("error_code", errorCode), ("error_class", errorClass)]

proc releasePendingRecordPermit*(p: KafkaProducer) {.raises: [].} =
  ## Paired with the acquire in the batched-send path; every site that resolves a
  ## PendingMessage.future releases, so callers unblock on cancel/close/DLQ too.
  p.pendingRecordsSem.releaseSafely("producer.releasePendingRecordPermit")

proc finalizeTxnTracking*(p: KafkaProducer, owesEpochBump: bool) {.raises: [].} =
  ## Single owner of the post-EndTxn reset: the txn tracking fields are read as a
  ## set by commit, abort, close, and registerPendingTxnPartitions, so they must be
  ## cleared on every exit path. `owesEpochBump` records an owed bump for the next
  ## `ensureEpochBumped` instead of failing a call the broker already finalized, and
  ## is stated at every call site rather than defaulted, so the two cases cannot be
  ## conflated by omission.
  if owesEpochBump:
    p.pendingEpochBump = true
  p.txnPartitions.clear()
  p.txnPendingPartitions.clear()
  p.txnStarted = false

template requireRunning*(p: KafkaProducer) =
  ## send, beginTransaction, and sendFireAndForget reject the same way once the
  ## producer is closing or closed.
  if p.state != lsRunning:
    raise newException(KafkaClosedError, "producer is closed")
