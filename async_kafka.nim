## async-kafka — an asynchronous `Apache Kafka <https://kafka.apache.org/>`_
## client for Nim, built on `chronos <https://github.com/status-im/nim-chronos>`_.
## The library speaks the Kafka wire protocol directly (no librdkafka dependency)
## and exposes three high-level entry points:
##
## - `newCluster <async_kafka/cluster.html#newCluster>`_ — a connection pool that
##   discovers brokers, negotiates API versions, and refreshes metadata.
## - `newKafkaProducer <async_kafka/producer/producer.html>`_ — a producer with
##   configurable `acks`, compression, and `linger.ms`-style batching, plus
##   optional idempotent and transactional delivery (KIP-98 / KIP-360).
## - `newKafkaConsumer <async_kafka/consumer/consumer.html>`_ — a consumer with
##   group coordination, automatic rebalancing, and offset management.
##
## Importing this module re-exports the supported application API; the explicit
## export list below is the package's SemVer contract. The lower-level modules
## (`connection`, `broker`, `group`, `assignment`, …) stay reachable through
## their full paths for advanced or in-repo use, but symbols exposed only by
## them — including their `*` declarations and transitive re-exports — are
## unstable and have no compatibility guarantee. Cluster configuration is
## selected by `newCluster` and is not mutable through the returned `Cluster`;
## `FetchedRecord` exposes payload data while its delivery stamps stay internal.
##
## Notable behaviour: Kafka protocol I/O is asynchronous and driven by `chronos`; TLS
## verifies certificates by default; SASL supports PLAIN and SCRAM-SHA-256/512;
## compression covers Gzip, Snappy, LZ4 and Zstd; API versions are negotiated per
## broker; RecordBatch v2 uses CRC32C validation; and assignment strategies include
## Range and RoundRobin.
##
## A producer or consumer built from an existing `Cluster` borrows it and never
## closes it (`ownsCluster` reports which case you are in); the convenience
## constructors that take bootstrap addresses create a Cluster and release it in
## their own `close()`. `newHostAllowlist` bounds which advertised broker
## addresses the client will dial, for plaintext connections as well as TLS.
##
## The client targets `chronos`'s single-threaded scheduler: a handle belongs to the
## event loop that built it, the consumer's `poll` family must be driven from one
## coroutine, and the metrics and rebalance callbacks run inline on that loop.
##
## Behavior notes live on the symbol they belong to — the idempotent sequence wrap
## on `newKafkaProducer`, the rebalancing model on `KafkaConsumer`, and so on;
## package-wide hazards, such as the chronos AsyncLock cancellation hazard, sit on
## the call sites that serialize behind such a lock.

# Public facade: only application-facing symbols are re-exported here — protocol
# codecs, broker accessors, fetcher state, and consumer recovery helpers stay behind
# their concrete module imports, and the producer implementation siblings are private.
# The producer facade's allowlist is mirrored rather than re-exported as a namespace,
# in both directions: every symbol it re-exports appears here, as `producer.X` or
# through the `metrics` / `consumerFacade` imports this module already has, and every
# `producer.X` here is one of its re-exports. A facade test pins that, so a one-sided
# change fails instead of drifting.
import
  async_kafka/[errors, cluster, connection, tls_anchors, sasl, sasl_plain, sasl_scram]
import async_kafka/protocol/[messages, compression]
import async_kafka/consumer/[fetcher, consumer as consumerFacade]
import async_kafka/internal/metrics
import async_kafka/producer/producer

export
  # Error contract
  errors.KafkaErrorCode,
  errors.KafkaError,
  errors.KafkaConnectionError,
  errors.KafkaProtocolError,
  errors.KafkaDecompressionLimitError,
  errors.KafkaApiError,
  errors.KafkaTimeoutError,
  errors.KafkaClosedError,
  errors.PartitionCommitError,
  errors.KafkaCommitError,
  errors.KafkaCommitSkippedError,
  errors.KafkaOffsetFetchError,
  errors.KafkaRebalanceCallbackError,
  errors.LifecycleState,
  errors.ErrorClass,
  errors.isRunning,
  errors.isClosingOrClosed,
  errors.toInt16,
  errors.errorClass,
  errors.isRetriable,
  errors.isRebalanceRequired,
  errors.isAbortable,
  errors.isFatal,
  errors.isDuplicateSequence,
  errors.isPermanent,
  errors.isInstanceFatal,
  errors.isClassified,
  errors.kafkaErrorName,
  errors.`==`,
  errors.`$`,

  # Kafka error codes (explicit so new protocol codes remain opt-in)
  errors.keNone,
  errors.keOffsetOutOfRange,
  errors.keCorruptMessage,
  errors.keUnknownTopicOrPartition,
  errors.keLeaderNotAvailable,
  errors.keNotLeaderOrFollower,
  errors.keRequestTimedOut,
  errors.keMessageTooLarge,
  errors.keNetworkException,
  errors.keCoordinatorLoadInProgress,
  errors.keGroupCoordinatorNotAvailable,
  errors.keNotCoordinator,
  errors.keInvalidTopicException,
  errors.keRecordListTooLarge,
  errors.keNotEnoughReplicas,
  errors.keNotEnoughReplicasAfterAppend,
  errors.keInvalidRequiredAcks,
  errors.keIllegalGeneration,
  errors.keInconsistentGroupProtocol,
  errors.keUnknownMemberId,
  errors.keInvalidSessionTimeout,
  errors.keRebalanceInProgress,
  errors.keTopicAuthorizationFailed,
  errors.keGroupAuthorizationFailed,
  errors.keClusterAuthorizationFailed,
  errors.keInvalidTimestamp,
  errors.keUnsupportedSaslMechanism,
  errors.keIllegalSaslState,
  errors.keUnsupportedVersion,
  errors.keTopicAlreadyExists,
  errors.keInvalidRequest,
  errors.keUnsupportedForMessageFormat,
  errors.keOutOfOrderSequenceNumber,
  errors.keDuplicateSequenceNumber,
  errors.keInvalidProducerEpoch,
  errors.keInvalidTxnState,
  errors.keInvalidProducerIdMapping,
  errors.keConcurrentTransactions,
  errors.keTransactionCoordinatorFenced,
  errors.keTransactionalIdAuthorizationFailed,
  errors.keSecurityDisabled,
  errors.keOperationNotAttempted,
  errors.keKafkaStorageError,
  errors.keSaslAuthenticationFailed,
  errors.keUnknownProducerId,
  errors.keFetchSessionIdNotFound,
  errors.keInvalidFetchSessionEpoch,
  errors.keListenerNotFound,
  errors.keGroupMaxSizeReached,
  errors.keFencedInstanceId,
  errors.keMemberIdRequired,
  errors.keProducerFenced,
  errors.keFencedLeaderEpoch,
  errors.keUnknownLeaderEpoch,
  errors.keTopicDeletionDisabled,
  errors.keUnsupportedCompressionType,
  errors.keInvalidRecord,
  errors.keInvalidMsgSize,
  errors.keBrokerNotAvailable,
  errors.keReplicaNotAvailable,
  errors.keStaleControllerEpoch,
  errors.keOffsetMetadataTooLarge,
  errors.keInvalidGroupId,
  errors.keNotController,
  errors.keInvalidTransactionTimeout,
  errors.kePolicyViolation,
  errors.keGroupIdNotFound,
  errors.keStaleBrokerEpoch,
  errors.keUnstableOffsetCommit,
  errors.keThrottlingQuotaExceeded,
  errors.keResourceNotFound,
  errors.keDuplicateResource,
  errors.keUnacceptableCredential,
  errors.keInconsistentVoterSet,
  errors.keInvalidUpdateVersion,
  errors.keFeatureUpdateFailed,
  errors.keUnknownTopicId,
  errors.keTransactionalIdNotFound,
  errors.keFencedMemberEpoch,
  errors.keStaleMemberEpoch,
  errors.keTransactionAbortable,
  errors.kePreferredLeaderNotAvailable,
  errors.keFetchSessionTopicIdError,
  errors.keOffsetNotAvailable,
  errors.keEligibleLeadersNotAvailable,
  errors.keElectionNotNeeded,
  errors.keNewLeaderElected,
  errors.keOffsetMovedToTieredStorage,
  errors.keUnknownServerError,

  # Cluster facade
  cluster.Cluster,
  cluster.newCluster,
  cluster.isClosed,
  cluster.state,
  cluster.refreshMetadata,
  cluster.close,

  # Shared value and callback types used by public APIs
  messages.TopicPartition,
  messages.hash,
  messages.RecordHeader,
  messages.IsolationLevel,
  compression.CompressionCodec,
  metrics.MetricsHook,

  # Authentication and TLS configuration
  sasl.SaslMechanism,
  sasl.name,
  sasl.burnString,
  sasl.requireTls,
  sasl_plain.newSaslPlain,
  sasl_scram.ScramHash,
  sasl_scram.newSaslScram,
  connection.TrustAnchorStore,
  connection.TlsConfig,
  connection.HostAllowlist,
  connection.HostRule,
  connection.newTlsConfig,
  tls_anchors.newTrustAnchorStoreFromPem,
  connection.newHostAllowlist,
  connection.allowHost,
  connection.allowCidr,
  connection.allowPrivateHosts,
  connection.hostAllowed,

  # Consumer facade
  consumerFacade.AssignmentStrategy,
  fetcher.OffsetResetPolicy,
  fetcher.FetchedRecord,
  consumerFacade.RebalanceCallback,
  consumerFacade.ConsumerConfig,
  consumerFacade.KafkaConsumer,
  consumerFacade.CommitCallback,
  consumerFacade.ConsumerGroupMetadata,
  consumerFacade.initConsumerGroupMetadata,
  consumerFacade.initUnfencedConsumerGroupMetadata,
  consumerFacade.initConsumerConfig,
  consumerFacade.newKafkaConsumer,
  consumerFacade.isClosed,
  consumerFacade.state,
  consumerFacade.ownsCluster,
  consumerFacade.groupMetadata,
  consumerFacade.start,
  consumerFacade.poll,
  consumerFacade.tryPoll,
  consumerFacade.pollBatch,
  consumerFacade.pollBatchStrict,
  consumerFacade.commitSync,
  consumerFacade.pause,
  consumerFacade.resume,
  consumerFacade.seek,
  consumerFacade.isPaused,
  consumerFacade.pausedPartitions,
  consumerFacade.commitAsync,
  consumerFacade.close,

  # Producer facade
  producer.KafkaProducer,
  producer.PartitionStrategy,
  producer.CustomPartitioner,
  producer.ProduceStatus,
  producer.ProduceResult,
  producer.ProduceCallback,
  producer.ErrorAction,
  producer.FailedRecord,
  producer.ErrorHandler,
  producer.toBytes,
  producer.newKafkaProducer,
  producer.isClosed,
  producer.state,
  producer.ownsCluster,
  producer.close,
  producer.send,
  producer.flush,
  producer.commitTransaction,
  producer.abortTransaction,
  producer.sendFireAndForget,
  producer.initTransactions,
  producer.beginTransaction,
  producer.sendOffsetsToTransaction,
  producer.setDeadLetterQueue,
  producer.alwaysDeadLetter,
  producer.alwaysDrop,
  producer.deadLetterOnPermanent
