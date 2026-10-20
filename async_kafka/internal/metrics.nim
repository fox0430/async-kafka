import callback_guard

type MetricsHook* = proc(
  event: cstring, tags: openArray[(cstring, cstring)], value: int64
) {.closure, gcsafe, raises: [].}
  ## Optional callback for emitting client-internal metrics.
  ##
  ## `event` is a stable dotted name from `metricEventNames`; `tags` carries
  ## dimensions (`("topic", "orders")`) whose keys are `cstring`s (literals at most
  ## call sites, converted `string`s at the string overload) and values are views
  ## into caller-owned `string` storage; `value` is per event a count, a
  ## milliseconds reading, an error code, an offset, or an id. The cstring types
  ## keep hot paths (e.g. `connection.orphan_response`) allocation-free apart from
  ## dynamic tag values.
  ##
  ## Runs synchronously in the producer/consumer path: it MUST NOT raise, MUST be
  ## cheap, MUST NOT block the chronos scheduler, and MUST NOT call back into the
  ## client whose operation fired it. A `CatchableError` raised anyway drops the
  ## event and is logged, rate-limited per client; a `Defect` is not caught.
  ##
  ## **Tag lifetime**: each `cstring` borrows a `string` that ends when the call
  ## returns — to retain tags past the call, copy each pair (e.g. via `$`) first.
  ##
  ## **Hot path**: `connection.orphan_response` fires from the chronos readLoop
  ## coroutine owning the connection (the connect path installs `setOnOrphan`); heavy
  ## work there stalls response dispatch on that connection.

type MetricEvent* = enum
  ## The metric vocabulary: one value per emittable event name. Names live once in
  ## `metricEventNames`, so a misspelled event is a compile error and every name is
  ## emitted only through the table the gate test reads.
  meBrokerApiVersionsNegotiated
  meBrokerConnectionDetached
  meBrokerReconnectAttempt
  meBrokerReconnectFailed
  meBrokerSaslAuthenticated
  meBrokerSaslMechanismRejected
  meBrokerThrottleClamped
  meBrokerThrottleWaitedMs
  meClusterBrokerForcePruned
  meClusterCoordinatorLookupAttemptFailed
  meClusterCoordinatorLookupFailed
  meClusterMetadataRefresh
  meClusterMetadataRefreshAttemptFailed
  meClusterMetadataRefreshFailed
  meClusterMetadataStaleLeaderEpoch
  meConnectionOrphanResponse
  meConsumerBatchLegacySkipped
  meConsumerFetchCorruptBatchReset
  meConsumerFetchCorruptBatchSkipped
  meConsumerFetchError
  meConsumerFetchLatencyMs
  meConsumerFetchRecords
  meConsumerFetchRetryThrottleMs
  meConsumerFetchTopLevelError
  meConsumerFetchTruncationDetected
  meConsumerFetchValidationStall
  meConsumerRebalanceStaleRecordsDropped
  meConsumerRepositionStaleRecordsDropped
  meProducerBatchFlush
  meProducerCloseAbortFailed
  meProducerCloseDlqFailed
  meProducerCloseDrainCancelled
  meProducerCloseDurationMs
  meProducerCloseFlushFailed
  meProducerCloseLingerCancelled
  meProducerCloseUnflushedBatches
  meProducerDlqError
  meProducerDlqSent
  meProducerDropped
  meProducerErrorHandlerException
  meProducerInflightResequenced
  meProducerSendDeduped
  meProducerSendDeliveryTimeout
  meProducerSendError
  meProducerSendOk
  meProducerSendRetry
  meProducerSendRetryBackoffMs
  meProducerSendRetryThrottleMs
  meProducerTxnAbortError
  meProducerTxnAbortOk
  meProducerTxnBegin
  meProducerTxnBumpEpoch
  meProducerTxnBumpEpochDeferred
  meProducerTxnCommitError
  meProducerTxnCommitOk
  meProducerTxnError
  meProducerTxnFatal
  meProducerTxnInit
  meProducerTxnSendOffsets
  meProducerTxnSendOffsetsError

const metricEventNames*: array[MetricEvent, cstring] = [
  # Total over the enum: a new value without a name here does not compile.
  meBrokerApiVersionsNegotiated: cstring"broker.api_versions.negotiated",
  meBrokerConnectionDetached: cstring"broker.connection.detached",
  meBrokerReconnectAttempt: cstring"broker.reconnect.attempt",
  meBrokerReconnectFailed: cstring"broker.reconnect.failed",
  meBrokerSaslAuthenticated: cstring"broker.sasl.authenticated",
  meBrokerSaslMechanismRejected: cstring"broker.sasl.mechanism_rejected",
  meBrokerThrottleClamped: cstring"broker.throttle.clamped",
  meBrokerThrottleWaitedMs: cstring"broker.throttle.waited_ms",
  meClusterBrokerForcePruned: cstring"cluster.broker.force_pruned",
  meClusterCoordinatorLookupAttemptFailed:
    cstring"cluster.coordinator.lookup.attempt.failed",
  meClusterCoordinatorLookupFailed: cstring"cluster.coordinator.lookup.failed",
  meClusterMetadataRefresh: cstring"cluster.metadata.refresh",
  meClusterMetadataRefreshAttemptFailed:
    cstring"cluster.metadata.refresh.attempt.failed",
  meClusterMetadataRefreshFailed: cstring"cluster.metadata.refresh.failed",
  meClusterMetadataStaleLeaderEpoch: cstring"cluster.metadata.stale_leader_epoch",
  meConnectionOrphanResponse: cstring"connection.orphan_response",
  meConsumerBatchLegacySkipped: cstring"consumer.batch.legacy_skipped",
  meConsumerFetchCorruptBatchReset: cstring"consumer.fetch.corrupt_batch_reset",
  meConsumerFetchCorruptBatchSkipped: cstring"consumer.fetch.corrupt_batch_skipped",
  meConsumerFetchError: cstring"consumer.fetch.error",
  meConsumerFetchLatencyMs: cstring"consumer.fetch.latency_ms",
  meConsumerFetchRecords: cstring"consumer.fetch.records",
  meConsumerFetchRetryThrottleMs: cstring"consumer.fetch.retry.throttle_ms",
  meConsumerFetchTopLevelError: cstring"consumer.fetch.top_level_error",
  meConsumerFetchTruncationDetected: cstring"consumer.fetch.truncation_detected",
  meConsumerFetchValidationStall: cstring"consumer.fetch.validation_stall",
  meConsumerRebalanceStaleRecordsDropped:
    cstring"consumer.rebalance.stale_records_dropped",
  meConsumerRepositionStaleRecordsDropped:
    cstring"consumer.reposition.stale_records_dropped",
  meProducerBatchFlush: cstring"producer.batch.flush",
  meProducerCloseAbortFailed: cstring"producer.close.abort.failed",
  meProducerCloseDlqFailed: cstring"producer.close.dlq.failed",
  meProducerCloseDrainCancelled: cstring"producer.close.drain.cancelled",
  meProducerCloseDurationMs: cstring"producer.close.duration_ms",
  meProducerCloseFlushFailed: cstring"producer.close.flush.failed",
  meProducerCloseLingerCancelled: cstring"producer.close.linger.cancelled",
  meProducerCloseUnflushedBatches: cstring"producer.close.unflushed_batches",
  meProducerDlqError: cstring"producer.dlq.error",
  meProducerDlqSent: cstring"producer.dlq.sent",
  meProducerDropped: cstring"producer.dropped",
  meProducerErrorHandlerException: cstring"producer.error_handler.exception",
  meProducerInflightResequenced: cstring"producer.inflight.resequenced",
  meProducerSendDeduped: cstring"producer.send.deduped",
  meProducerSendDeliveryTimeout: cstring"producer.send.delivery_timeout",
  meProducerSendError: cstring"producer.send.error",
  meProducerSendOk: cstring"producer.send.ok",
  meProducerSendRetry: cstring"producer.send.retry",
  meProducerSendRetryBackoffMs: cstring"producer.send.retry.backoff_ms",
  meProducerSendRetryThrottleMs: cstring"producer.send.retry.throttle_ms",
  meProducerTxnAbortError: cstring"producer.txn.abort.error",
  meProducerTxnAbortOk: cstring"producer.txn.abort.ok",
  meProducerTxnBegin: cstring"producer.txn.begin",
  meProducerTxnBumpEpoch: cstring"producer.txn.bump_epoch",
  meProducerTxnBumpEpochDeferred: cstring"producer.txn.bump_epoch.deferred",
  meProducerTxnCommitError: cstring"producer.txn.commit.error",
  meProducerTxnCommitOk: cstring"producer.txn.commit.ok",
  meProducerTxnError: cstring"producer.txn.error",
  meProducerTxnFatal: cstring"producer.txn.fatal",
  meProducerTxnInit: cstring"producer.txn.init",
  meProducerTxnSendOffsets: cstring"producer.txn.send_offsets",
  meProducerTxnSendOffsetsError: cstring"producer.txn.send_offsets.error",
]

const noTags*: array[0, (cstring, cstring)] = []
  ## Empty tag list: an emit site with nothing to report must not spell `[]`, which
  ## would convert to either `emitMetric` overload and make the call ambiguous.

proc guardMetricsHook*(hook: MetricsHook): MetricsHook =
  ## `hook` behind a CatchableError guard with its own report window, so one client's
  ## failing hook does not hide another's. A failure drops the event. `nil` stays
  ## `nil`, keeping `emitMetric` free for clients without a hook.
  if hook == nil:
    return nil
  let failures = new CallbackFailureLog
  result = proc(
      event: cstring, tags: openArray[(cstring, cstring)], value: int64
  ) {.closure, gcsafe, raises: [].} =
    discard failures[].guardCallback("metricsHook", "dropped " & $event):
      hook(event, tags, value)

var unguardedHookFailures {.threadvar.}: CallbackFailureLog
  ## Reports for hooks that reached `emitMetric` without `guardMetricsHook`, i.e. set
  ## directly on a lower-level component. Shared by every such hook on the thread.

proc invokeHook(
    hook: MetricsHook,
    event: MetricEvent,
    tagPairs: openArray[(cstring, cstring)],
    v: int64,
) {.inline, gcsafe, raises: [].} =
  ## Backstop for a hook that skipped `guardMetricsHook`: a leak would unwind through
  ## the chronos dispatcher and abandon the coroutine that fired the event (the
  ## connection readLoop among them). The event is dropped and the failure reported.
  ## Unlike `guardCallback`, a completed call skips the threadvar, so a guarded hook
  ## pays only for the `try`. A Defect stays fatal.
  try:
    hook(metricEventNames[event], tagPairs, v)
  except CatchableError as e:
    unguardedHookFailures.callbackRaised(
      "metricsHook", e, "dropped " & $metricEventNames[event]
    )

template emitMetric*(
    hook: MetricsHook,
    event: MetricEvent,
    tagPairs: openArray[(cstring, cstring)],
    v: int64,
) =
  ## Zero-cost when `hook` is nil. `hook` is bound to a local first so a
  ## side-effecting hook argument is evaluated exactly once, and `tagPairs`/`v` stay
  ## inside the guard so they are not evaluated without a hook.
  let metricHook {.gensym.} = hook
  if metricHook != nil:
    invokeHook(metricHook, event, tagPairs, v)

template emitMetric*(
    hook: MetricsHook,
    event: MetricEvent,
    tagPairs: openArray[(string, string)],
    v: int64,
) =
  ## Convenience overload for call sites building tag pairs from `$expr`: the event
  ## name comes from the `metricEventNames` table and outlives the call, so only the
  ## tags need the local seq that keeps each `cstring` view valid until the hook
  ## returns (and so the array is not rebuilt per textual reference).
  let metricHook {.gensym.} = hook
  if metricHook != nil:
    let stableTags {.gensym.}: seq[(string, string)] = @tagPairs
    var cstrTags {.gensym.} = newSeq[(cstring, cstring)](stableTags.len)
    for i in 0 ..< stableTags.len:
      cstrTags[i] = (cstring(stableTags[i][0]), cstring(stableTags[i][1]))
    invokeHook(metricHook, event, cstrTags, v)
