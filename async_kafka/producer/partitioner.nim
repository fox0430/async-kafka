## Producer module — partition resolution.

import std/[tables, options, importutils]

import ../[async_backend, errors, cluster]
import ../protocol/[messages, murmur2]

import types

privateAccess(KafkaProducer)

proc roundRobinIndex*(
    p: KafkaProducer, topic: string, partCount: int32
): int32 {.raises: [KafkaError].} =
  if partCount <= 0:
    raise newException(
      KafkaError, "roundRobinIndex: partCount must be > 0, got " & $partCount
    )
  let counter = p.partitionCounters.getOrDefault(topic, 0'i32)
  let idx = counter mod partCount
  # Widen before adding: int32 arithmetic would raise OverflowDefect at
  # int32.high (~2^31 sends). Same wrap-to-zero idiom as the sequence numbers.
  p.partitionCounters[topic] = int32((int64(counter) + 1) and int64(int32.high))
  return idx

proc resolvePartition*(
    p: KafkaProducer, topic: string, key: Option[seq[byte]]
): Future[int32] {.async: (raises: [KafkaError, CancelledError]).} =
  let (parts, metadataRefreshed) = await p.cluster.getTopicPartitionsRefreshing(topic)

  if parts.len == 0:
    # No partitions advertised (deleted upstream, unresolvable, or zero
    # partitions): drop the round-robin counter so a topic that stays gone leaves
    # no dead entry; a recreated topic re-seeds it on the next send.
    p.partitionCounters.del(topic)
    raise newException(KafkaError, "no partitions for topic: " & topic)

  # A refresh also happens on leader change, so blindly resetting the round-robin
  # counter would skew traffic to partition 0; `counter mod partCount`
  # self-rebalances, so only a sticky pick whose partition is gone is dropped.
  # Comparing that partition NUMBER against `parts.len` (a COUNT) is valid
  # because Kafka ids are contiguous 0..<n and never renumbered or deleted.
  if metadataRefreshed and topic in p.stickyPartitions:
    let live = int32(parts.len)
    let sp = p.stickyPartitions.getOrDefault(topic, 0'i32)
    if sp < 0 or sp >= live:
      p.stickyPartitions.del(topic)

  let partCount = int32(parts.len)

  # Key-hash / sticky index `parts[hash mod partCount]` and must equal Apache
  # Kafka's `toPositive(murmur2) % numPartitions` partition NUMBER. The cluster
  # sorts the cached list at refresh; an unsorted list here breaks key→partition
  # affinity.

  if p.customPartitioner != nil:
    # A raise despite `raises: []` would escape send's raises list, so it fails this
    # send instead. Always wrapped, so a partitioner bug cannot pass for a broker error
    # or a cancellation.
    let partitionId =
      try:
        p.customPartitioner(topic, key, partCount)
      except CatchableError as e:
        raise newException(
          KafkaError,
          "custom partitioner raised " & $e.name & " for topic " & topic & ": " & e.msg,
          e,
        )
    if partitionId < 0 or partitionId >= partCount:
      raise newException(
        KafkaError,
        "custom partitioner returned invalid partition " & $partitionId & " for topic " &
          topic & " (partitions: 0..<" & $partCount & ")",
      )
    result = partitionId
  else:
    case p.partitionStrategy
    of psRoundRobin:
      result = parts[p.roundRobinIndex(topic, partCount)].partitionIndex
    of psKeyHash:
      # A zero-length key is still a key: Java's DefaultPartitioner hashes it
      # (only null keyBytes fall through). Lumping some(@[]) with none() would
      # route the same key differently and break co-partitioning with Java.
      if key.isSome:
        result =
          parts[(murmur2(key.get) and 0x7fffffff'i32) mod partCount].partitionIndex
      else:
        result = parts[p.roundRobinIndex(topic, partCount)].partitionIndex
    of psSticky:
      if key.isSome:
        result =
          parts[(murmur2(key.get) and 0x7fffffff'i32) mod partCount].partitionIndex
      elif topic in p.stickyPartitions:
        p.stickyPartitions.withValue(topic, v):
          result = v[]
      else:
        let idx = p.roundRobinIndex(topic, partCount)
        let partitionId = parts[idx].partitionIndex
        p.stickyPartitions[topic] = partitionId
        result = partitionId
