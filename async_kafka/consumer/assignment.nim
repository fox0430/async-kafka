import std/[algorithm, tables]

import ../errors
import ../protocol/[primitives, messages]

type
  AssignmentStrategy* = enum
    ## How a consumer group spreads partitions across members: contiguous
    ## ranges per member, or an even round-robin spread.
    asRange
    asRoundRobin

  MemberSubscription* = object
    memberId*: string
    topics*: seq[string]

  MemberAssignment* = object
    memberId*: string
    partitions*: seq[TopicPartition]

const
  ConsumerProtocolVersion = 0'i16
  ConsumerProtocolMinVersion = 0'i16

proc assignRange*(
    members: seq[MemberSubscription], topicPartitions: Table[string, seq[int32]]
): seq[MemberAssignment] {.raises: [].} =
  # Sorted by memberId for a canonical assignment,
  # Java-RangeAssignor-identical: the same membership yields the same placement
  # across rebalances.
  var sortedMembers = members
  sortedMembers.sort(
    proc(a, b: MemberSubscription): int =
      cmp(a.memberId, b.memberId)
  )
  result = newSeq[MemberAssignment](sortedMembers.len)
  for i, m in sortedMembers:
    result[i].memberId = m.memberId

  var allTopics: seq[string]
  for topic, _ in topicPartitions:
    allTopics.add(topic)
  allTopics.sort()

  for topic in allTopics:
    let partitions = topicPartitions.getOrDefault(topic)
    if partitions.len == 0:
      continue

    var sortedPartitions = partitions
    sortedPartitions.sort()

    var subscribedIndices: seq[int]
    for i, m in sortedMembers:
      if topic in m.topics:
        subscribedIndices.add(i)

    if subscribedIndices.len == 0:
      continue

    let numPartitions = sortedPartitions.len
    let numMembers = subscribedIndices.len
    let perMember = numPartitions div numMembers
    let extra = numPartitions mod numMembers

    var pos = 0
    for j, idx in subscribedIndices:
      let count = perMember + (if j < extra: 1 else: 0)
      for k in pos ..< pos + count:
        result[idx].partitions.add(
          TopicPartition(topic: topic, partition: sortedPartitions[k])
        )
      pos += count

proc assignRoundRobin*(
    members: seq[MemberSubscription], topicPartitions: Table[string, seq[int32]]
): seq[MemberAssignment] {.raises: [].} =
  # Sorted by memberId for a canonical circular walk.
  #
  # Java-identical (RoundRobinAssignor): the circular walk sorts members by
  # memberId and skips members that did not subscribe to the topic, so
  # heterogeneous subscriptions can leave the placement imbalanced, as in Java.
  var sortedMembers = members
  sortedMembers.sort(
    proc(a, b: MemberSubscription): int =
      cmp(a.memberId, b.memberId)
  )
  result = newSeq[MemberAssignment](sortedMembers.len)
  for i, m in sortedMembers:
    result[i].memberId = m.memberId

  var allTPs: seq[TopicPartition]
  for topic, partitions in topicPartitions:
    for p in partitions:
      allTPs.add(TopicPartition(topic: topic, partition: p))
  allTPs.sort(
    proc(a, b: TopicPartition): int =
      result = cmp(a.topic, b.topic)
      if result == 0:
        result = cmp(a.partition, b.partition)
  )

  var idx = 0
  for tp in allTPs:
    var assigned = false
    for attempt in 0 ..< sortedMembers.len:
      let memberIdx = (idx + attempt) mod sortedMembers.len
      if tp.topic in sortedMembers[memberIdx].topics:
        result[memberIdx].partitions.add(tp)
        idx = (memberIdx + 1) mod sortedMembers.len
        assigned = true
        break
    if not assigned:
      discard

proc assignPartitions*(
    strategy: AssignmentStrategy,
    members: seq[MemberSubscription],
    topicPartitions: Table[string, seq[int32]],
): seq[MemberAssignment] {.raises: [].} =
  case strategy
  of asRange:
    assignRange(members, topicPartitions)
  of asRoundRobin:
    assignRoundRobin(members, topicPartitions)

proc encodeSubscription*(
    topics: seq[string], userData: seq[byte] = @[]
): seq[byte] {.raises: [KafkaProtocolError].} =
  var w = initKafkaWriter()
  w.writeInt16(ConsumerProtocolVersion)
  w.writeArrayLen(int32(topics.len))
  for t in topics:
    w.writeString(t)
  w.writeBytes(userData)
  result = w.buf

proc decodeSubscription*(
    data: seq[byte]
): seq[string] {.raises: [KafkaProtocolError].} =
  if data.len == 0:
    return @[]

  var r = initKafkaReader(data)
  let version = r.readInt16()
  if version < ConsumerProtocolMinVersion:
    raise newException(
      KafkaProtocolError, "invalid consumer protocol subscription version: " & $version
    )
  # ownedPartitions arrives in v1, generationId in v2, and rackId in v3, all
  # after the v0 prefix, so read the prefix and let trailing bytes fall off the
  # reader window.
  let numTopics = r.readArrayLen()
  # A topic name costs at least its int16 length prefix and far more in memory,
  # so the divisor is 2 (not the scalar default 1). Capacity is capped and the
  # loop appends, so the prefix still cannot reserve more than the buffer
  # holds.
  r.checkArrayLenFitsBuffer(numTopics, 2)
  result = newSeqOfCap[string](min(int(numTopics), 1024))
  for i in 0 ..< numTopics:
    result.add(r.readString())

proc encodeAssignment*(
    partitions: seq[TopicPartition], userData: seq[byte] = @[]
): seq[byte] {.raises: [KafkaProtocolError].} =
  var w = initKafkaWriter()
  w.writeInt16(ConsumerProtocolVersion)

  var byTopic = initTable[string, seq[int32]]()
  for tp in partitions:
    byTopic.mgetOrPut(tp.topic, @[]).add(tp.partition)

  w.writeArrayLen(int32(byTopic.len))
  for topic, parts in byTopic:
    w.writeString(topic)
    w.writeArrayLen(int32(parts.len))
    for p in parts:
      w.writeInt32(p)
  w.writeBytes(userData)
  result = w.buf

proc decodeAssignment*(
    data: seq[byte]
): seq[TopicPartition] {.raises: [KafkaProtocolError].} =
  if data.len == 0:
    return @[]

  var r = initKafkaReader(data)
  let version = r.readInt16()
  if version < ConsumerProtocolMinVersion:
    raise newException(
      KafkaProtocolError, "invalid consumer protocol assignment version: " & $version
    )
  # The v0 layout [assignedPartitions, userData] prefixes every later version;
  # read it and ignore trailing bytes.
  let numTopics = r.readArrayLen()
  r.checkArrayLenFitsBuffer(numTopics, 2)
  for i in 0 ..< numTopics:
    let topic = r.readString()
    let numPartitions = r.readArrayLen()
    # Each partition is a fixed int32, so 4 bytes is exact, not a floor.
    r.checkArrayLenFitsBuffer(numPartitions, 4)
    for j in 0 ..< numPartitions:
      let partition = r.readInt32()
      result.add(TopicPartition(topic: topic, partition: partition))
