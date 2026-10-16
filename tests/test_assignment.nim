import std/[unittest, tables]

import async_kafka/consumer/assignment
import async_kafka/protocol/messages

suite "Partition assignment":
  test "range assignment":
    let members = @[
      MemberSubscription(memberId: "m1", topics: @["t1"]),
      MemberSubscription(memberId: "m2", topics: @["t1"]),
    ]
    let topicParts = {"t1": @[0'i32, 1, 2, 3, 4]}.toTable
    let result = assignPartitions(asRange, members, topicParts)
    check result.len == 2
    check result[0].memberId == "m1"
    check result[0].partitions.len == 3 # 5/2 = 2 + 1 extra
    check result[1].memberId == "m2"
    check result[1].partitions.len == 2

  test "round-robin assignment":
    let members = @[
      MemberSubscription(memberId: "m1", topics: @["t1"]),
      MemberSubscription(memberId: "m2", topics: @["t1"]),
    ]
    let topicParts = {"t1": @[0'i32, 1, 2, 3]}.toTable
    let result = assignPartitions(asRoundRobin, members, topicParts)
    check result.len == 2
    check result[0].partitions.len == 2
    check result[1].partitions.len == 2

  test "subscription encode/decode round-trip":
    let topics = @["topic1", "topic2", "topic3"]
    let encoded = encodeSubscription(topics)
    let decoded = decodeSubscription(encoded)
    check decoded == topics

  test "range assignment single member":
    let members = @[MemberSubscription(memberId: "m1", topics: @["t1"])]
    let topicParts = {"t1": @[0'i32, 1, 2]}.toTable
    let result = assignPartitions(asRange, members, topicParts)
    check result.len == 1
    check result[0].partitions.len == 3

  test "range assignment more members than partitions":
    let members = @[
      MemberSubscription(memberId: "m1", topics: @["t1"]),
      MemberSubscription(memberId: "m2", topics: @["t1"]),
      MemberSubscription(memberId: "m3", topics: @["t1"]),
    ]
    let topicParts = {"t1": @[0'i32, 1]}.toTable
    let result = assignPartitions(asRange, members, topicParts)
    check result[0].partitions.len == 1
    check result[1].partitions.len == 1
    check result[2].partitions.len == 0

  test "round-robin multi-topic":
    let members = @[
      MemberSubscription(memberId: "m1", topics: @["t1", "t2"]),
      MemberSubscription(memberId: "m2", topics: @["t1", "t2"]),
    ]
    let topicParts = {"t1": @[0'i32, 1], "t2": @[0'i32, 1]}.toTable
    let result = assignPartitions(asRoundRobin, members, topicParts)
    check result[0].partitions.len == 2
    check result[1].partitions.len == 2

  test "empty subscription decode":
    let decoded = decodeSubscription(@[])
    check decoded.len == 0

  test "empty assignment decode":
    let decoded = decodeAssignment(@[])
    check decoded.len == 0

  test "assignment encode/decode round-trip":
    let partitions = @[
      TopicPartition(topic: "t1", partition: 0),
      TopicPartition(topic: "t1", partition: 1),
      TopicPartition(topic: "t2", partition: 0),
    ]
    let encoded = encodeAssignment(partitions)
    let decoded = decodeAssignment(encoded)
    check decoded.len == 3
    for tp in partitions:
      check tp in decoded

proc tp(topic: string, partition: int32): TopicPartition =
  TopicPartition(topic: topic, partition: partition)

suite "Partition assignment: Java-compat pin tests":
  test "RoundRobin: asymmetric topics, identical subscriptions":
    # Java RoundRobinAssignor with C0,C1 subscribed to [t0,t1,t2]:
    #   sorted TPs: t0-0, t1-0, t1-1, t2-0, t2-1, t2-2
    #   round-robin alternates members → C0 gets indexes 0,2,4; C1 gets 1,3,5.
    let members = @[
      MemberSubscription(memberId: "C0", topics: @["t0", "t1", "t2"]),
      MemberSubscription(memberId: "C1", topics: @["t0", "t1", "t2"]),
    ]
    let topicParts = {"t0": @[0'i32], "t1": @[0'i32, 1], "t2": @[0'i32, 1, 2]}.toTable
    let res = assignPartitions(asRoundRobin, members, topicParts)
    check res.len == 2
    check res[0].memberId == "C0"
    check res[0].partitions == @[tp("t0", 0), tp("t1", 1), tp("t2", 1)]
    check res[1].memberId == "C1"
    check res[1].partitions == @[tp("t1", 0), tp("t2", 0), tp("t2", 2)]

  test "Range: asymmetric topics, identical subscriptions":
    # Java RangeAssignor divides each topic independently:
    #   t0 (1 part, 2 subs): C0=[t0-0], C1=[]
    #   t1 (2 parts, 2 subs): C0=[t1-0], C1=[t1-1]
    #   t2 (3 parts, 2 subs): C0=[t2-0,t2-1] (extra 1), C1=[t2-2]
    let members = @[
      MemberSubscription(memberId: "C0", topics: @["t0", "t1", "t2"]),
      MemberSubscription(memberId: "C1", topics: @["t0", "t1", "t2"]),
    ]
    let topicParts = {"t0": @[0'i32], "t1": @[0'i32, 1], "t2": @[0'i32, 1, 2]}.toTable
    let res = assignPartitions(asRange, members, topicParts)
    check res[0].memberId == "C0"
    check res[0].partitions == @[tp("t0", 0), tp("t1", 0), tp("t2", 0), tp("t2", 1)]
    check res[1].memberId == "C1"
    check res[1].partitions == @[tp("t1", 1), tp("t2", 2)]

  test "RoundRobin: single member single topic":
    let members = @[MemberSubscription(memberId: "C0", topics: @["t0"])]
    let topicParts = {"t0": @[0'i32, 1, 2]}.toTable
    let res = assignPartitions(asRoundRobin, members, topicParts)
    check res[0].partitions == @[tp("t0", 0), tp("t0", 1), tp("t0", 2)]

  test "RoundRobin: even partition count and even consumer count":
    # 2 consumers, 4 partitions → each consumer gets 2.
    let members = @[
      MemberSubscription(memberId: "C0", topics: @["t0"]),
      MemberSubscription(memberId: "C1", topics: @["t0"]),
    ]
    let topicParts = {"t0": @[0'i32, 1, 2, 3]}.toTable
    let res = assignPartitions(asRoundRobin, members, topicParts)
    check res[0].partitions == @[tp("t0", 0), tp("t0", 2)]
    check res[1].partitions == @[tp("t0", 1), tp("t0", 3)]

  test "Range: odd partition count is left-skewed":
    # 5 partitions, 2 consumers → 5/2=2, extra=1 → C0=3, C1=2.
    let members = @[
      MemberSubscription(memberId: "C0", topics: @["t0"]),
      MemberSubscription(memberId: "C1", topics: @["t0"]),
    ]
    let topicParts = {"t0": @[0'i32, 1, 2, 3, 4]}.toTable
    let res = assignPartitions(asRange, members, topicParts)
    check res[0].partitions == @[tp("t0", 0), tp("t0", 1), tp("t0", 2)]
    check res[1].partitions == @[tp("t0", 3), tp("t0", 4)]

  test "Range: member not subscribed to a topic is skipped":
    # C0 subscribed to t0, C1 not — all t0 partitions go to C0; t1 split.
    let members = @[
      MemberSubscription(memberId: "C0", topics: @["t0", "t1"]),
      MemberSubscription(memberId: "C1", topics: @["t1"]),
    ]
    let topicParts = {"t0": @[0'i32, 1], "t1": @[0'i32, 1]}.toTable
    let res = assignPartitions(asRange, members, topicParts)
    check res[0].partitions == @[tp("t0", 0), tp("t0", 1), tp("t1", 0)]
    check res[1].partitions == @[tp("t1", 1)]

  test "RoundRobin: member not subscribed to a topic is skipped":
    # Java behavior: traverse sorted TPs; assign each to the next member
    # subscribed to that TP's topic. C0 subscribes to t0+t1, C1 only t1.
    # Sorted TPs: t0-0, t0-1, t1-0, t1-1.
    # idx starts at 0:
    #   t0-0 → C0 (only sub); idx → 1
    #   t0-1 → start at C1, C1 not subscribed, fall through to C0; idx → 1
    #   t1-0 → start at C1, subscribed; idx → 0
    #   t1-1 → start at C0, subscribed; idx → 1
    # → C0: [t0-0, t0-1, t1-1], C1: [t1-0]
    let members = @[
      MemberSubscription(memberId: "C0", topics: @["t0", "t1"]),
      MemberSubscription(memberId: "C1", topics: @["t1"]),
    ]
    let topicParts = {"t0": @[0'i32, 1], "t1": @[0'i32, 1]}.toTable
    let res = assignPartitions(asRoundRobin, members, topicParts)
    check res[0].partitions == @[tp("t0", 0), tp("t0", 1), tp("t1", 1)]
    check res[1].partitions == @[tp("t1", 0)]

  test "Range / RoundRobin: zero subscribed members for a topic leaves it unassigned":
    let members = @[MemberSubscription(memberId: "C0", topics: @["t0"])]
    let topicParts = {"t0": @[0'i32], "tX": @[0'i32, 1]}.toTable
    let rangeRes = assignPartitions(asRange, members, topicParts)
    let rrRes = assignPartitions(asRoundRobin, members, topicParts)
    check rangeRes[0].partitions == @[tp("t0", 0)]
    check rrRes[0].partitions == @[tp("t0", 0)]

suite "Partition assignment: member-order determinism":
  # The assignment must not depend on the order in which the broker returned
  # members in JoinGroupResponse — both assignors sort by memberId internally,
  # matching Apache Kafka, so the same membership yields the same result.
  let topicParts = {"t0": @[0'i32, 1, 2, 3]}.toTable
  let forward = @[
    MemberSubscription(memberId: "C0", topics: @["t0"]),
    MemberSubscription(memberId: "C1", topics: @["t0"]),
    MemberSubscription(memberId: "C2", topics: @["t0"]),
  ]
  let reversed = @[
    MemberSubscription(memberId: "C2", topics: @["t0"]),
    MemberSubscription(memberId: "C1", topics: @["t0"]),
    MemberSubscription(memberId: "C0", topics: @["t0"]),
  ]

  test "Range assignment is independent of input member order":
    let a = assignPartitions(asRange, forward, topicParts)
    let b = assignPartitions(asRange, reversed, topicParts)
    check a == b
    # Canonical (sorted) order: C0 gets the extra partition (left-skewed).
    check a[0].memberId == "C0"
    check a[0].partitions == @[tp("t0", 0), tp("t0", 1)]
    check a[1].memberId == "C1"
    check a[1].partitions == @[tp("t0", 2)]
    check a[2].memberId == "C2"
    check a[2].partitions == @[tp("t0", 3)]

  test "RoundRobin assignment is independent of input member order":
    let a = assignPartitions(asRoundRobin, forward, topicParts)
    let b = assignPartitions(asRoundRobin, reversed, topicParts)
    check a == b
    check a[0].memberId == "C0"
    check a[0].partitions == @[tp("t0", 0), tp("t0", 3)]
    check a[1].memberId == "C1"
    check a[1].partitions == @[tp("t0", 1)]
    check a[2].memberId == "C2"
    check a[2].partitions == @[tp("t0", 2)]
