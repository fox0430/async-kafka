## Test-only accessors for `Cluster` internals. Private procs via the `{.all.}`
## import; private fields via `std/importutils`' `privateAccess`.

import std/[importutils, options, tables]

import chronos

import async_kafka/cluster {.all.}
import async_kafka/broker
import async_kafka/protocol/messages

proc setPartitionLeaderForTest*(
    c: Cluster, tp: TopicPartition, nodeId: int32, leaderEpoch: int32 = -1
) =
  ## Test-only: directly seed a partition-leader entry. Lets tests assert
  ## downstream behaviour (commit / fetch carrying leaderEpoch) without
  ## driving a full Metadata round-trip through fake_broker.
  privateAccess(Cluster)
  c.partitionLeaders[tp] = PartitionLeader(nodeId: nodeId, leaderEpoch: leaderEpoch)

proc applyPartitionLeaderMonotonicForTest*(
    c: Cluster,
    tp: TopicPartition,
    candidate: PartitionLeader,
    prev: Option[PartitionLeader],
    topic: string,
) =
  ## Exposes the monotonicity policy that `refreshMetadataUnlocked` uses
  ## so the KIP-320 stale-epoch test can drive it directly without
  ## standing up a fake broker / full MetadataResponse round trip.
  c.applyPartitionLeaderMonotonic(tp, candidate, prev, topic)

proc seedCoordinatorForTest*(c: Cluster, groupId: string, b: Broker) =
  ## Test-only: register `b` under its nodeId and seed the group-coordinator cache
  ## so `coordinatorBroker(groupId)` resolves to `b` without a FindCoordinator
  ## round trip (the cached path dials an unconnected broker itself).
  privateAccess(Cluster)
  c.brokers[b.nodeId] = b
  c.coordinators[groupId] = b.nodeId

proc hasCachedCoordinatorForTest*(c: Cluster, groupId: string): bool =
  ## Test-only: whether the group-coordinator cache still holds an entry for
  ## `groupId`. Lets regression tests assert that a NOT_COORDINATOR response
  ## invalidated the cached mapping.
  privateAccess(Cluster)
  groupId in c.coordinators

proc brokerCountForTest*(c: Cluster): int =
  ## Test-only: number of brokers in the registry. Asserts a metadata refresh
  ## adopting a bootstrap broker cannot repopulate the table after close()
  ## cleared it.
  privateAccess(Cluster)
  c.brokers.len

proc registerBrokerForTest*(c: Cluster, b: Broker) =
  ## Test-only: register `b` under its nodeId so `getBrokerById` /
  ## `brokerForPartition` resolve it without a Metadata round-trip.
  privateAccess(Cluster)
  c.brokers[b.nodeId] = b

proc markMetadataFreshForTest*(c: Cluster) =
  ## Test-only: stamp the metadata cache as just-refreshed so
  ## `maybeRefreshMetadata` short-circuits and `brokerForPartition` uses the
  ## seeded leaders instead of dialing bootstrap brokers.
  privateAccess(Cluster)
  c.lastMetadataRefresh = Moment.now()
