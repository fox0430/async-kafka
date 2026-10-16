# Metrics vocabulary

`MetricsHook` is the optional callback (`async_kafka/internal/metrics.nim`) the
client invokes for every internal event. Install one on `newCluster` /
`newKafkaProducer` / `newKafkaConsumer`; a `nil` hook makes every emit a no-op.
This file is the vocabulary contract: **the table below and the emit sites are
checked against each other by `tests/test_metric_events.nim`** — an event or a
tag key that exists on only one side fails that test.

## Conventions

- **Event names** are stable dotted lowercase identifiers, grouped by the layer
  that owns the decision (`producer.` / `consumer.` / `broker.` / `cluster.` /
  `connection.`). A name is emitted, never constructed at runtime.
- **`value`** is a count, an attempt number, a duration in milliseconds, or a
  record/batch/partition count. The `Value` column states which for each event.
- **Tag keys** are lowercase snake_case. `topic` and `partition` come from the
  shared per-partition helper, so they appear together.
- **`error_code`** is SCREAMING_SNAKE, so one `GROUP BY error_code` covers every
  failure path: a Kafka protocol error name (`kafkaErrorName`, e.g.
  `UNKNOWN_TOPIC_OR_PARTITION`), a client-side tag (`kafkaErrorTag`:
  `CONNECTION_ERROR` / `TIMEOUT` / `PROTOCOL_ERROR` / `KAFKA_ERROR`), or
  `CANCELLED` for a transaction finalize that was cancelled. The lowercase
  spellings (`connection`, `timeout`, ...) belonged to an earlier revision and
  are gone; `reason` carries that descriptive spelling now.
- **`error_class`** is the `ErrorClass` label from `errorClassToCstring`
  (`retriable`, `rebalance_required`, `abortable`, `fatal`, `duplicate`,
  `needs_producer_id_reset`, `unknown`). It is `unknown` when the failure
  carries no broker error code (transport drop, cancellation).
- **`reason`** is the lowercase descriptive counterpart: why this attempt or
  decision happened (`transport`, `cancelled`, `producer_id_reset`,
  `no_broker`, ...). Use `error_code` for grouping and `reason` for context.
- **Tag lifetime**: each `cstring` in `tags` borrows storage that dies when the
  call returns. Copy each pair (e.g. with `$`) before retaining it in an async
  sink or a background queue.
- The hook runs synchronously on the client's hot path: it must not raise,
  block, or do non-trivial work.

## Installing a hook

```nim
import async_kafka
import std/tables

proc onMetric(event: cstring, tags: openArray[(cstring, cstring)], value: int64)
    {.gcsafe, raises: [].} =
  var line = $event & " = " & $value
  for (k, v) in tags:
    line.add " " & $k & "=" & $v
  echo line   # replace with your metrics client

let cluster = newCluster(
  @["127.0.0.1:9092"], metricsHook = onMetric
)
```

## Vocabulary

A `Tags` cell listing two alternatives separated by `;` means the event is
emitted with either shape depending on the path (the alternatives are mutually
exclusive, not additive).

### Broker

| Event | Value | Tags | Meaning |
| --- | --- | --- | --- |
| `broker.api_versions.negotiated` | API key count | `host` + `port` | Emitted after ApiVersions negotiation succeeds in `negotiateAndAuth`; value is the number of API keys the broker advertised. |
| `broker.connection.detached` | count (1) | `host` + `port` | `detachConn` emits when an address change discards a live connection; a nil or already-closed connection is a silent no-op. |
| `broker.reconnect.attempt` | attempt number (1-based) | `host` + `port` | Emitted once per `connectImpl` call before dialing; value is `consecutiveFailures + 1`, and SASL candidate cycling is not counted separately. |
| `broker.reconnect.failed` | consecutive failure count | `host` + `port` + `stage` | `recordFailure` emits for the failed stage (`tcp_connect`, `dns_resolve`, `handshake`, `sasl_no_mechanism`); value is the capped per-broker failure counter. An allowlist refusal is not recorded: it is a policy decision, not a transport failure. |
| `broker.sasl.authenticated` | count (1) | `host` + `mechanism` + `port` | Emitted after `performSaslAuthOne` succeeds, only when a SASL mechanism was configured; the mechanism name is a tag. |
| `broker.sasl.mechanism_rejected` | Kafka error code | `host` + `mechanism` + `port` | In a multi-candidate SASL cycle the broker returned `UNSUPPORTED_SASL_MECHANISM`; value is that raw error code. |
| `broker.throttle.clamped` | requested milliseconds | `host` + `port` + `requested_ms` | `applyThrottle` emits when the broker's `throttleTimeMs` exceeds `maxThrottleMs`; value is the original request, before clamping. |
| `broker.throttle.waited_ms` | milliseconds | `host` + `port` | `awaitThrottle`'s wall-clock sleep, emitted once after at least one sleep; timeout and cancellation exits do not emit. |

### Cluster

| Event | Value | Tags | Meaning |
| --- | --- | --- | --- |
| `cluster.broker.force_pruned` | count (1) | `nodeId` | `pruneStaleBrokers` emits only on a forced removal: broker stale past the grace window with in-flight requests and all deadlines passed. |
| `cluster.coordinator.lookup.attempt.failed` | count (1) | `host` + `port` | Per-candidate `FindCoordinator` failure, first over known brokers then bootstrap addresses; `CancelledError` is not counted. |
| `cluster.coordinator.lookup.failed` | count (1) | `reason` | Whole lookup failed; the `reason` tag is `no_broker` (all candidates failed) or `api_error` (the `FindCoordinator` response carried an error). |
| `cluster.metadata.refresh` | count (1) | `brokers` + `topics` | Emitted at the end of a successful refresh; `brokers` and `topics` tags hold the post-apply cache sizes. |
| `cluster.metadata.refresh.attempt.failed` | count (1) | `host` + `port` | Per-candidate metadata RPC failure, over known brokers then bootstrap addresses; `CancelledError` does not emit. |
| `cluster.metadata.refresh.failed` | count (1) | `reason` | Whole refresh failed; `reason` is `no_broker` (every candidate failed) or `empty_brokers` (the response listed no brokers). |
| `cluster.metadata.stale_leader_epoch` | count (1) | `kept` + `partition` + `stale` + `topic` | `applyPartitionLeaderMonotonic` rejects a response whose `leaderEpoch` is below the cached epoch; the cached leader is kept. |

### Connection

| Event | Value | Tags | Meaning |
| --- | --- | --- | --- |
| `connection.orphan_response` | correlation id | `host` + `port` | The read loop got a response frame with no live slot (timed out, cancelled, untracked); value is the orphaned `corrId`, not a count. |

### Consumer

| Event | Value | Tags | Meaning |
| --- | --- | --- | --- |
| `consumer.batch.legacy_skipped` | batch count | `partition` + `topic` | Per partition response, pre-0.11 (`magic != 2`) batches were decoded as empty; value is how many, whose records are dropped. |
| `consumer.fetch.corrupt_batch_reset` | corrupt batch count | `partition` + `topic` | `bpsUnknownGap`: undecodable batches with no recoverable offset range, so the partition is repositioned per the reset policy. |
| `consumer.fetch.corrupt_batch_skipped` | corrupt batch count | `partition` + `topic` | `bpsSkipped`: undecodable batches whose offset range was recovered best-effort, so the cursor skips past them. |
| `consumer.fetch.error` | count (1) | `broker` + `reason` | `broker.fetch` raised a `KafkaError`; tags carry the broker id and error message, and the broker's partitions are reassigned. |
| `consumer.fetch.latency_ms` | milliseconds | `broker` | Duration of one `fetchFromBroker` call (RPC plus response processing); emitted on the queue-full early return and at normal completion. |
| `consumer.fetch.records` | records | `partition` + `topic` | `enqueuePartition` delivered records to the consumer queue; value is the enqueued count, and the event is skipped when zero. |
| `consumer.fetch.retry.throttle_ms` | milliseconds | `broker` | After a fetch error, records the leader's still-active throttle for observability; the broker layer owns the actual wait. |
| `consumer.fetch.top_level_error` | count (1) | `broker` + `error_code` | Retriable top-level `FetchResponse` `errorCode` (v7+): that response is skipped this round, and session-invalid codes drop the fetch session. |
| `consumer.fetch.truncation_detected` | end offset | `partition` + `topic` | KIP-320 validation found the leader end offset below the fetch position; value is the rewound offset. The undefined-epoch (-1) path resets silently instead. |
| `consumer.fetch.validation_stall` | count (1) | `partition` + `topic` | After `maxValidationAttempts` consecutive `OffsetForLeaderEpoch` failures, validation is abandoned and the partition resumes fetching unvalidated. |
| `consumer.rebalance.stale_records_dropped` | count (1) | `current_generation` + `partition` + `record_generation` + `topic` | The poll path withholds a record tagged with a previous group generation; one event per dropped record. |
| `consumer.reposition.stale_records_dropped` | count (1) | `current_pos_epoch` + `partition` + `record_pos_epoch` + `topic` | The poll path withholds a record superseded by a seek or offset reset (stale `posEpoch`); one event per record. |

### Producer

| Event | Value | Tags | Meaning |
| --- | --- | --- | --- |
| `producer.batch.flush` | records | `partition` + `topic` | `flushBatch` emits after extracting a non-empty partition batch for sending; value is that batch's record count. |
| `producer.close.abort.failed` | count (1) | `reason` | The close-time transaction abort exceeded `timeoutMs` (`reason=timeout`) and the producer is moved to `tsError`. |
| `producer.close.dlq.failed` | count (1) | `reason` | Cascade close of an owned DLQ producer did not finish within `timeoutMs`; `reason=timeout` and that close is abandoned. |
| `producer.close.drain.cancelled` | cancelled task count | — | The fire-and-forget drain budget elapsed with tasks still running, so their futures were cancelled. |
| `producer.close.duration_ms` | milliseconds | — | Total wall-clock duration of `closeImpl`, emitted just before the producer is marked closed. |
| `producer.close.flush.failed` | count (1) | `reason` | The final flush did not finish within `timeoutMs` during close; `reason=timeout` and the flush future is cancelled. |
| `producer.close.linger.cancelled` | cancelled timer count | — | Linger timers still pending at close were cancelled; emitted only when at least one was. |
| `producer.close.unflushed_batches` | partition count | — | Batches `flush` could not send are failed during close; value is the number of partitions left with batches. |
| `producer.dlq.error` | count (1) | `partition` + `status` + `topic` | DLQ routing failed: the send raised (`status=exception`), returned non-success, or no DLQ is configured; the record reports `pstDeadLetterFailed`. |
| `producer.dlq.sent` | count (1) | `dlq_topic` + `original_partition` + `original_topic` | The failed record was successfully produced to the DLQ topic; tags name the DLQ and the original topic/partition. |
| `producer.dropped` | count (1) | `partition` + `reason` + `topic` | The user `errorHandler` returned `eaDrop`, so the failed record was discarded (`pstDropped`) instead of redirected. |
| `producer.error_handler.exception` | count (1) | `exception` + `partition` + `topic` | The user `errorHandler` raised despite its `raises: []` contract; it is logged, and the action falls back to `eaFail`. |
| `producer.inflight.resequenced` | batch count | — | `reassignSequencesAfterReset` re-encoded in-flight batches under a new producer id/epoch; value counts batches actually re-encoded. |
| `producer.send.deduped` | records | `partition` + `topic` | The broker reported a duplicate sequence, so the batch is treated as success with offset -1; emitted instead of `producer.send.ok`. |
| `producer.send.delivery_timeout` | attempt number | `error_code` + `partition` + `topic` | A retriable, producer-id-reset, or transport failure occurred past `delivery.timeout.ms`; the send then finalizes terminally. |
| `producer.send.error` | attempt count | `error_class` + `error_code` + `partition` + `topic` | Terminal send failure: a non-retriable error or an exhausted retry/delivery budget; tags carry `error_code` and `error_class`. |
| `producer.send.ok` | records | `partition` + `topic` | The broker acked the produce batch; value is the number of records it contained. |
| `producer.send.retry` | attempt number | `error_code` + `partition` + `reason` + `topic` | Emitted before a retry: transient, sibling-observed, or self-driven producer-id reset; tags carry `error_code` and `reason`. |
| `producer.send.retry.backoff_ms` | milliseconds | `partition` + `reason` + `topic` | The sleep chosen before a retry: jittered backoff folded with the leader's throttle when the path includes it. |
| `producer.send.retry.throttle_ms` | milliseconds | `partition` + `reason` + `topic` | Emitted on transient retries when the leader's quota throttle is still active; reset paths pass `includeThrottle = false` and omit it. |
| `producer.txn.abort.error` | count (1) | `error_class` + `error_code` | `EndTxn`(abort) failed or was cancelled; tags carry `error_code` (broker name, `CANCELLED`, or a transport tag) and `error_class`. |
| `producer.txn.abort.ok` | count (1) | `deferred_bump`; `empty` | Emitted after a successful abort: tags `empty=true` for a locally finalized empty txn, else `deferred_bump`. |
| `producer.txn.begin` | count (1) | — | `beginTransaction` transitioned `tsReady` to `tsInTransaction`; refused begins raise and do not emit. |
| `producer.txn.bump_epoch` | count (1) | — | `InitProducerId` bump succeeded and the new identity was installed; deferred or abandoned bumps do not emit. |
| `producer.txn.bump_epoch.deferred` | count (1) | `error_code` + `retriable` | Bump not applied and `pendingEpochBump` set: last transport attempt, non-retriable API error (`retriable=false`), or exhausted retry budget. |
| `producer.txn.commit.error` | count (1) | `error_class` + `error_code` | `EndTxn`(commit) failed or was cancelled; tags carry `error_code` (broker name, `CANCELLED`, or a transport tag) and `error_class`. |
| `producer.txn.commit.ok` | count (1) | `deferred_bump`; `empty` | Commit finished: tags `empty=true` when nothing reached the coordinator, else `deferred_bump` after a successful `EndTxn`. |
| `producer.txn.error` | count (1) | `error_code` | `transitionToError` moved the producer to `tsError` (abortable, non-fatal code); self-loops do not re-emit. |
| `producer.txn.fatal` | count (1) | `error_code` | `transitionToError` moved the producer to terminal `tsFatal` for a fatal error class; only real state changes emit. |
| `producer.txn.init` | count (1) | — | `initTransactions` completed `InitProducerId` and reached `tsReady`; refused inits raise and do not emit. |
| `producer.txn.send_offsets` | count (1) | — | `AddOffsetsToTxn` and `TxnOffsetCommit` both succeeded; API failures raise earlier and do not emit this. |
| `producer.txn.send_offsets.error` | count (1) | `error_class` + `error_code` | Offset commit poisoned the txn while `tsInTransaction`: a coordinator RPC failed (transport `error_code`) or was cancelled (`CANCELLED`). |

