## Full-path compatibility facade for the producer module. The supported API is
## re-exported by the package root; this module path itself is unstable and not
## covered by the package's SemVer contract.
##
## Keep this facade an explicit allowlist: internal state-machine mutators,
## batching helpers, and test hooks must stay reachable only from their concrete
## sibling module, so the normal producer import cannot bypass producer
## invariants. The implementation modules are imported so the qualified
## re-exports resolve.
import types, transaction, dlq, batch, lifecycle
import ../group_metadata
import ../internal/metrics

export
  types.KafkaProducer, types.PartitionStrategy, types.CustomPartitioner,
  types.ProduceStatus, types.ProduceResult, types.ProduceCallback, types.ErrorAction,
  types.FailedRecord, types.ErrorHandler, types.toBytes, lifecycle.newKafkaProducer,
  lifecycle.isClosed, lifecycle.state, lifecycle.ownsCluster, lifecycle.close,
  lifecycle.setDeadLetterQueue, batch.send, batch.flush, batch.commitTransaction,
  batch.abortTransaction, batch.sendFireAndForget, transaction.initTransactions,
  transaction.beginTransaction, transaction.sendOffsetsToTransaction,
  dlq.alwaysDeadLetter, dlq.alwaysDrop, dlq.deadLetterOnPermanent, metrics.MetricsHook,
  group_metadata.ConsumerGroupMetadata, group_metadata.initConsumerGroupMetadata,
  group_metadata.initUnfencedConsumerGroupMetadata
