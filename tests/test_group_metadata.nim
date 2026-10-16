import std/[unittest, options]

import async_kafka/group_metadata
import async_kafka/errors

suite "ConsumerGroupMetadata":
  test "the unfenced constructor sets exactly the KIP-447 sentinels":
    let m = initUnfencedConsumerGroupMetadata("group-a")
    check m.groupId == "group-a"
    check m.generationId == -1'i32
    check m.memberId == ""
    check m.groupInstanceId.isNone

  test "fenced fields are preserved":
    let m = initConsumerGroupMetadata(
      "group-b",
      generationId = 7,
      memberId = "member-42",
      groupInstanceId = some("instance-1"),
    )
    check m.groupId == "group-b"
    check m.generationId == 7'i32
    check m.memberId == "member-42"
    check m.groupInstanceId == some("instance-1")

  test "the fenced constructor has no generation/member defaults":
    # A default would let `initConsumerGroupMetadata("group")` compile and
    # silently send the unfenced sentinels from a caller who believes EOS is in
    # force. Choosing no fencing is `initUnfencedConsumerGroupMetadata`'s job.
    check not compiles(initConsumerGroupMetadata("group-a"))
    check not compiles(initConsumerGroupMetadata("group-a", generationId = 1))

  test "dynamic member has no groupInstanceId":
    let m = initConsumerGroupMetadata("group-d", generationId = 3, memberId = "m")
    check m.groupInstanceId.isNone

  test "static member carries groupInstanceId (KIP-345)":
    let m = initConsumerGroupMetadata(
      "group-e", generationId = 4, memberId = "m", groupInstanceId = some("static-1")
    )
    check m.groupInstanceId.isSome
    check m.groupInstanceId.get == "static-1"

  test "an empty groupId is rejected by both constructors":
    expect KafkaError:
      discard initConsumerGroupMetadata("", generationId = 1, memberId = "m")
    expect KafkaError:
      discard initUnfencedConsumerGroupMetadata("")

  test "a generation without a memberId is rejected":
    # The dangerous half-set pair: generationId >= 0 looks fenced to the caller,
    # but the broker has no member to fence against.
    expect KafkaError:
      discard initConsumerGroupMetadata("group-f", generationId = 3, memberId = "")
    let handBuilt = ConsumerGroupMetadata(groupId: "group-f", generationId: 3)
    expect KafkaError:
      validateConsumerGroupMetadata(handBuilt)

  test "the unfenced sentinel refuses a memberId or a groupInstanceId":
    # generationId = -1 means "nothing to fence", so carrying member identity
    # alongside it is a contradiction, not a no-op.
    expect KafkaError:
      discard initConsumerGroupMetadata("group-g", generationId = -1, memberId = "m")
    expect KafkaError:
      discard initConsumerGroupMetadata(
        "group-g", generationId = -1, memberId = "", groupInstanceId = some("static")
      )

  test "a generation below the -1 sentinel is rejected":
    expect KafkaError:
      discard initConsumerGroupMetadata("group-h", generationId = -2, memberId = "m")
    expect KafkaError:
      validateConsumerGroupMetadata(
        ConsumerGroupMetadata(groupId: "group-h", generationId: -9, memberId: "m")
      )

  test "validation re-checks mutations made after construction":
    var m = initConsumerGroupMetadata("group-i", generationId = 5, memberId = "m")
    validateConsumerGroupMetadata(m)
    m.memberId = ""
    expect KafkaError:
      validateConsumerGroupMetadata(m)
