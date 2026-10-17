# async-kafka

[![CI](https://github.com/fox0430/async-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/fox0430/async-kafka/actions/workflows/ci.yml)

Async Apache Kafka client library in Nim

## Features

- Producer with configurable acks, compression, and batching (linger.ms style)
- Idempotent and transactional producer (KIP-98 / KIP-360 epoch bump)
- Consumer with group coordination, automatic rebalancing, and offset management
- KIP-320 log-truncation detection (OffsetForLeaderEpoch) on leader changes
- Compression: Gzip, Snappy, LZ4, Zstd
- TLS/SSL support, with trust anchors loaded from PEM
- SASL: PLAIN and SCRAM-SHA-256 / SCRAM-SHA-512, over plaintext or TLS
- RecordBatch v2 format with CRC32C validation
- API version negotiation with brokers
- Partition assignment strategies: Range and RoundRobin

## Requirements

- Nim >= 2.2.8
- LZ4 (`liblz4`), only for LZ4 compression

## Platform support

- Linux

## Async runtime

- [chronos](https://github.com/status-im/nim-chronos) only

## Threading

The client targets chronos's single-threaded scheduler: a handle is bound to the
event loop that built it, and its state carries no cross-thread synchronization.
Drive it from one thread, or serialize access yourself; a multi-threaded program
keeps a client per thread.

Within that loop the concurrency contract is per handle, and each symbol's
docstring is the authority: a `Cluster` is meant to be shared by the producers
and consumers on the loop and closed after them, a `KafkaProducer` is safe to
share between coroutines, while a `KafkaConsumer` must be driven from one.
Metrics hooks and rebalance callbacks run inline on that loop, so keep them
short, non-blocking, and free of calls back into the client.

## API stability

The supported API is the explicit export list of `pkg/async_kafka`. Its symbols,
signatures, public fields, and documented behavior follow Semantic Versioning:

- incompatible changes require a major release;
- compatible API additions require a minor release;
- compatible fixes require a patch release.

Full-path imports such as `pkg/async_kafka/connection`,
`pkg/async_kafka/broker`, `pkg/async_kafka/consumer/group`, and
`pkg/async_kafka/protocol/primitives` are unstable advanced/in-repo escape
hatches. Their `*` declarations, field visibility, helper procs, generated
codecs, and transitive re-exports may change in any release and are not covered
by the package's SemVer contract.

## Usage

The programs in [`examples/`](examples/) are complete and type-checked in CI.
They default to the broker from `docker-compose.yml`, take its address from
`KAFKA_BROKERS`, and are compiled and run from the repository root:

```sh
nim c -r examples/transactions.nim
```

Each file's header comment names what it needs — topics, a CA, a credential.

### Producer

```nim
import pkg/chronos
import pkg/async_kafka

proc main() {.async.} =
  let cluster = newCluster(@["localhost:9092"])
  let producer = newKafkaProducer(cluster, acks = 1)

  let produced = await producer.send(
    topic = "my-topic",
    value = toBytes("hello kafka"),
  )
  echo "Produced to partition ", produced.partition, " at offset ", produced.offset

  await producer.close()
  await cluster.close()

waitFor main()
```

Batching, compression, and the partition strategies, with the fire-and-forget
form of `send`, are in
[`examples/producer_settings.nim`](examples/producer_settings.nim).

### Consumer

```nim
import pkg/chronos
import pkg/async_kafka

proc main() {.async.} =
  var config = initConsumerConfig(
    bootstrapBrokers = @["localhost:9092"],
    groupId = "my-group",
    topics = @["my-topic"],
  )
  config.offsetResetPolicy = orpEarliest

  let consumer = newKafkaConsumer(config)
  await consumer.start()

  let records = await consumer.pollBatch(maxRecords = 100, timeout = 1.seconds)
  for rec in records:
    echo "Got message at offset ", rec.offset

  await consumer.close()

waitFor main()
```

A manual commit during a rebalance raises `KafkaCommitSkippedError` from
`commitSync` (or hands it to the `commitAsync` callback). It is non-fatal: catch
it and continue polling. [`examples/consumer.nim`](examples/consumer.nim) runs
that loop and adds `pause`, `resume`, and `seek`; the assignment strategy and the
rebalance callbacks are in
[`examples/consumer_group.nim`](examples/consumer_group.nim).

### Transactions (exactly-once)

`transactionalId` switches a producer into transactional mode. A
consume-process-produce loop commits the consumed offsets inside the same
transaction; [`examples/transactions.nim`](examples/transactions.nim) is the
complete program.

### TLS

Certificate verification is ON by default; `skipVerify = true` disables it
(dev / self-signed setups only — never in production). A private or self-signed
CA is supplied as PEM-encoded certificates, and `newTrustAnchorStoreFromPem`
builds the trust anchors from them.
[`examples/tls.nim`](examples/tls.nim) builds them from a PEM file and connects
under them; it targets the local broker's SASL_SSL listener, which offers PLAIN
only, so it configures that mechanism as well — the transport and the mechanism
are independent knobs for any other listener.

The same `tlsConfig` is accepted by `newKafkaProducer(bootstrapBrokers, ...)`
and by `ConsumerConfig`, where it configures the Cluster those constructors
build; the verification details (the `dNSName`-only SAN check, the trust-anchor
fallback) are on `TlsConfig`.

### SASL

`newSaslScram(user, password, scramSha256 | scramSha512)` and
`newSaslPlain(user, password)` build the mechanisms, which are passed where the
Cluster is built; [`examples/sasl.nim`](examples/sasl.nim) runs the handshake
end to end. PLAIN sends the password in the clear, so it is refused unless the
connection is TLS with verification on; `allowPlaintext = true` opts out for a
local or trusted network. Credentials must be printable ASCII (SASLprep is not
implemented).

`newCluster(..., saslMechanisms = @[...])` is the multi-mechanism form; the
convenience constructors take a single mechanism for the Cluster they build.

### Host allowlist

A metadata response can redirect the client to any address the peer can reach,
and under TLS that peer can present a certificate valid *for the address it
chose*. TLS cannot bound that on its own, so the allowlist is a
**cluster-level** policy, checked before any socket is opened — plaintext
connections included. [`examples/host_allowlist.nim`](examples/host_allowlist.nim)
builds one and connects under it.

Rules come from `allowHost`, `allowCidr`, and `allowPrivateHosts`, combined by
`newHostAllowlist`: a rule names an address, an advertised name is resolved with
every answer checked, the check fails closed, and omitting the parameter permits
every advertised address. `allowPrivateHosts` covers RFC1918 and loopback
(127/8 and ::1/128), excluding link-local and IPv6 unique-local addresses;
combine it with `allowCidr` for other IPv6 ranges. The convenience constructors
accept the same rules for the Cluster they build (`HostAllowlist`).

### Ownership and shutdown order

`newCluster(...)` creates a `Cluster`; `newKafkaProducer(cluster, ...)` and
`newKafkaConsumer(config, cluster)` **borrow** it and never close it, so the
caller closes what it created, last.
[`examples/ownership.nim`](examples/ownership.nim) closes both kinds of handle.

The convenience constructors — `newKafkaProducer(bootstrapBrokers, ...)` and
`newKafkaConsumer(config)` — create a Cluster and own it, so their `close()`
releases it; `ownsCluster` reports which case you are in. TLS, SASL, and the host
allowlist belong to the Cluster, and a consumer built on an injected one rejects
a `config.tlsConfig`, `config.saslMechanism`, or `config.hostAllowlist` rather
than dialing without them.

### Dead-letter queue

A producer built with an `errorHandler` that returns `eaDeadLetter` diverts
records it fails to deliver to a dead-letter topic, sent through a second
producer — the destination comes from `setDeadLetterQueue`, and without it the
send resolves as `pstDeadLetterFailed` instead. A diverted `send` reports the
outcome through `ProduceResult.status` instead of raising, and a transactional
producer never diverts: the failure surfaces so the transaction cannot commit
without the record.
[`examples/dead_letter_queue.nim`](examples/dead_letter_queue.nim) wires the two
together. `deadLetterOnPermanent()`, `alwaysDeadLetter()`, and `alwaysDrop()` are
the built-in handlers; an `ErrorHandler` can choose per `FailedRecord`.

## Observability

### Logging

Diagnostics go through `std/logging`, and **the client never installs a
handler**. `std/logging` starts with no handlers, so out of the box every line
is discarded — enabling logs is the application's job, and
[`examples/observability.nim`](examples/observability.nim) installs a console
handler. What is emitted, and at which level:

- `debug` — per-attempt detail: each metadata candidate that failed and why,
  stale leader epochs that were rejected, coordinator cache invalidations and
  lookups, and the bookkeeping a cancelled close swallows.
- `info` — connection and membership lifecycle: reconnects after failures, the
  SASL candidate a broker rejected before the next candidate is tried, coordinator
  resolution, a subscribed topic gaining partitions, and a successful group join.
- `warn` — a metadata refresh or coordinator lookup failed on every candidate,
  a handshake failed, or an acquire/release pairing was violated. The
  pairing warn names the call site (`producer.flushBatch.txn`, ...) so the
  caller can be found without a debugger. Offset resets, corrupt batches, and
  truncated logs land here too.
- `error` — a background loop gives up: the fetcher stops on a permanent error
  or on a response that exceeded the decompression budget, and the consumer
  reports that it will not restart it.

The level split is the contract; the individual lines above are the current emit
sites, so a line can move between levels or be added in a release.

A failed metadata refresh, coordinator lookup, or request that can go to any
broker raises an error that names the most diagnostic candidate failure —
endpoint, broker error code, and the broker's own message:

```
failed to fetch metadata from any broker (cause: 10.0.0.7:9094: SASL_AUTHENTICATION_FAILED (58): SASL auth failed for SCRAM-SHA-256: ...)
```

so a wrong password is distinguishable from an unreachable port without
enabling debug logs.

A handler that raises cannot break the client: the failure is absorbed (Defects
still propagate). The library's lines carry no request id; correlate them with
the metric stream and the broker/topic/partition named in the message.

### Metrics

`MetricsHook` is an optional callback that receives every internal event
(`producer.send.retry`, `consumer.fetch.error`, ...) with its tags and value.
Pass one to `newCluster` and `newKafkaProducer`, and set
`ConsumerConfig.metricsHook` for a consumer; a `nil` hook makes each emit a
no-op, and
[`examples/observability.nim`](examples/observability.nim) forwards the stream
to a callback.

The full vocabulary — every event, its value semantics, and the tag keys it
carries, and how to group by `error_code` and `reason` — is in
[`docs/metrics.md`](docs/metrics.md).

## Notes and known limitations

### Idempotent producer sequence wrap (KIP-98)

A wire-level wrap of the per-partition sequence number against a real broker
(`~2^31` records on one producer-id) is not exercised in CI, so deployments that
sustain a single producer-id for billions of records should validate the broker's
wrap acceptance in their own environment. The mechanism is documented on
`newKafkaProducer` (`enableIdempotence`).

### Producer per-partition memory

Idle-partition state is reclaimed; the idempotent producer's sequence number is
kept for the life of the producer-id. See `KafkaProducer`.

### Producer in-flight window vs per-partition parallelism

Each broker connection's in-flight window is fixed at 5 requests and shared by
every partition that broker leads, so produce parallelism to one broker can stay
below what the per-partition `maxInFlightRequests` suggests. Ordering and
idempotence are unaffected, and the connection window is not user-tunable.

### Consumer group membership and rebalancing

Dynamic membership only (KIP-345) and eager rebalancing only (KIP-429): a
restarting member rejoins as a new member rather than resuming its assignment,
and every rebalance revokes all currently assigned partitions. See
`KafkaConsumer`.

### Synchronous DNS resolution

A broker advertised by host name is resolved synchronously, which stalls the
event loop for the lookup. See `newCluster`.

### SASL/SCRAM channel binding

SCRAM-SHA-256 / SHA-512 are implemented but RFC 5929 channel binding
(`tls-server-end-point`) is not. Run SCRAM over TLS for transport
confidentiality; without channel binding the SCRAM exchange itself
cannot detect TLS-layer MITM. Other SASL mechanisms beyond PLAIN and
SCRAM (e.g. OAUTHBEARER, GSSAPI) are not implemented.

### SASL periodic re-authentication (KIP-368)

`sessionLifetimeMs` is ignored, so a broker with `connections.max.reauth.ms > 0`
closes the connection at the session boundary and the next request reconnects —
transient rather than fatal, but a periodic reconnection latency. See
`newCluster`.

### Legacy MessageSet v0/v1 batches (pre-0.11 segments)

Records in a legacy batch are dropped and the cursor advances by one offset past
`baseOffset`; a `warn` log and the `consumer.batch.legacy_skipped` metric (tagged
with `topic` / `partition`) report the gap. This only affects clusters that still
hold segments written in the pre-0.11 message format. See `poll`.

### chronos AsyncLock cancellation hazard

chronos hands an `AsyncLock` to the next waiter before that waiter resumes, so a
cancellation landing in between leaves the lock held by a coroutine that unwinds
without releasing it. The broker connect path, the cluster metadata refresh, and
the consumer's offset commit each serialize behind such a lock, and a wedged one
hangs every later call on that object. In practice these paths are cancelled only
on shutdown, where the object is being torn down anyway, and the consumer's final
commit in `close()` bounds its wait, so close cannot hang on it.

## Supported Kafka API versions

The integration suite runs against Apache Kafka 3.7.0. Versions are negotiated
per broker through ApiVersions, within these ranges:

| Message | Versions | flexibleSince |
| --- | --- | --- |
| ApiVersionsResponse | v0–v1 | — |
| MetadataRequest / Response | v1–v12 | v9 |
| FetchRequest / Response | v4–v11 | — |
| ListOffsetsRequest / Response | v1–v2 | — |
| OffsetForLeaderEpochRequest / Response | v2–v3 | — |
| ProduceRequest / Response | v3–v7 | — |
| OffsetFetchRequest / Response | v1–v5 | — |
| OffsetCommitRequest / Response | v2–v8 | v8 |
| FindCoordinatorRequest / Response | v0–v1 | — |
| JoinGroupRequest / Response | v1–v2 | — |
| SyncGroupRequest / Response | v0–v1 | — |
| LeaveGroupRequest / Response | v0–v1 | — |
| HeartbeatRequest / Response | v0–v1 | — |
| InitProducerIdRequest / Response | v0–v3 | v2 |
| EndTxnRequest / Response | v0–v3 | v3 |
| AddPartitionsToTxn / AddOffsetsToTxn / TxnOffsetCommit | v0–v3 | v3 |
| SaslHandshakeRequest / Response | v1 | — |
| SaslAuthenticateRequest / Response | v1 | — |
| Record / RecordHeader / RecordBatch (envelope: v2) | — | — |

How these codecs are generated from one compile-time schema, and how a message
is added, is in [`docs/codegen.md`](docs/codegen.md).

## License

MIT — see [`LICENSE`](LICENSE). The licenses of the C sources the compression
and TLS dependencies compile into a build are reproduced in [`THIRD-PARTY-NOTICES.md`](THIRD-PARTY-NOTICES.md).
