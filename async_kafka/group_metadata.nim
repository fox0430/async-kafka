## Shared consumer-group identity used by the EOS (exactly-once) path: a consumer
## hands its membership to a transactional producer via `sendOffsetsToTransaction`
## so the broker can fence a TxnOffsetCommit (KIP-447) against a stale generation. A
## leaf module so consumer and producer can both depend on it without a cycle.

import std/options

import errors

const unfencedGenerationId = -1'i32
  ## KIP-447 "no fencing": what a pre-fencing client sends. Built explicitly by
  ## `initUnfencedConsumerGroupMetadata`; validation rejects the sentinel combined
  ## with a member id or a group instance id.

type ConsumerGroupMetadata* = object
  ## Consumer-group identity for the EOS path (see module doc), built by
  ## `initConsumerGroupMetadata` (fenced) / `initUnfencedConsumerGroupMetadata`,
  ## preferably from `consumer.groupMetadata`. The fields stay public, but the
  ## producer re-validates with `validateConsumerGroupMetadata` before the wire, so a
  ## hand-mutated half-set pair is refused rather than silently unfenced.
  groupId*: string
  generationId*: int32
    ## `-1` means "no fencing": the legacy `sendOffsetsToTransaction(offsets,
    ## groupId)` overload sends this so the broker treats the commit like a
    ## pre-KIP-447 client.
  memberId*: string
  groupInstanceId*: Option[string]
    ## KIP-345 static membership id; `none` for dynamic members.

proc validateConsumerGroupMetadata*(m: ConsumerGroupMetadata) {.raises: [KafkaError].} =
  ## Raise on a group identity the broker would have to interpret: an empty groupId,
  ## a generation below the `-1` sentinel, or a half-set generation / member pair.
  ## A `generationId >= 0` with an empty `memberId` is the dangerous one — it looks
  ## fenced to the caller while the broker has nothing to fence against — so it is
  ## rejected at construction and re-checked here before the wire, the fields being
  ## public and mutable after construction.
  if m.groupId.len == 0:
    raise newException(KafkaError, "consumer group metadata: groupId must not be empty")
  if m.generationId < unfencedGenerationId:
    raise newException(
      KafkaError,
      "consumer group metadata: generationId must be -1 (no fencing) or >= 0, got " &
        $m.generationId,
    )
  if m.generationId == unfencedGenerationId:
    if m.memberId.len > 0:
      raise newException(
        KafkaError,
        "consumer group metadata: memberId must be empty when generationId is -1 " &
          "(nothing to fence a member against)",
      )
    if m.groupInstanceId.isSome:
      raise newException(
        KafkaError,
        "consumer group metadata: groupInstanceId must be none when generationId is -1",
      )
  elif m.memberId.len == 0:
    raise newException(
      KafkaError,
      "consumer group metadata: memberId must not be empty when generationId is " &
        $m.generationId & " — pass it from consumer.groupMetadata() for a fenced " &
        "commit, or use initUnfencedConsumerGroupMetadata for a pre-KIP-447 commit",
    )

proc initConsumerGroupMetadata*(
    groupId: string,
    generationId: int32,
    memberId: string,
    groupInstanceId: Option[string] = none(string),
): ConsumerGroupMetadata {.raises: [KafkaError].} =
  ## Build the fenced group identity for the EOS path from live generation / member
  ## ids (`consumer.groupMetadata`). There is no default: silently sending the
  ## `-1` / `""` sentinel would disable broker-side fencing for a caller who believes
  ## exactly-once is in force — use `initUnfencedConsumerGroupMetadata` for that
  ## legacy commit. Raises on an inconsistent pair.
  result = ConsumerGroupMetadata(
    groupId: groupId,
    generationId: generationId,
    memberId: memberId,
    groupInstanceId: groupInstanceId,
  )
  validateConsumerGroupMetadata(result)

proc initUnfencedConsumerGroupMetadata*(
    groupId: string
): ConsumerGroupMetadata {.raises: [KafkaError].} =
  ## Build the pre-KIP-447 identity: no generation, no member — what the legacy
  ## `sendOffsetsToTransaction(offsets, groupId)` overload asks for, so the broker
  ## accepts the commit unfenced. Prefer `consumer.groupMetadata()` once the consumer
  ## has joined (the fenced path).
  result = ConsumerGroupMetadata(
    groupId: groupId,
    generationId: unfencedGenerationId,
    memberId: "",
    groupInstanceId: none(string),
  )
  validateConsumerGroupMetadata(result)
