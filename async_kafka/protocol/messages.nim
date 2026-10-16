import std/[options, hashes]

import pkg/stew/endians2

import api_keys, primitives, schema, crc32c, compression
import ../errors

type
  IsolationLevel* = enum
    ## Fetch isolation: uncommitted reads see everything, committed reads hide
    ## transactional records until the transaction commits or aborts.
    ilReadUncommitted = 0
    ilReadCommitted = 1

  RequestHeader* = object
    apiKey*: ApiKey
    apiVersion*: int16
    correlationId*: int32
    clientId*: Option[string]

  ResponseHeader* = object
    correlationId*: int32

  ApiVersionsRequest* = object
    ## Hand-written, unlike every other request: this is the exchange that
    ## establishes the version ranges the generated codecs select from.
    discard

  TopicPartition* = object
    ## Topic name plus partition index. Hashable and comparable; the identity
    ## key for offsets, fetch positions, and assignments.
    topic*: string
    partition*: int32

proc toIsolationLevel*(raw: int8): IsolationLevel {.raises: [KafkaError].} =
  case raw
  of 0'i8:
    ilReadUncommitted
  of 1'i8:
    ilReadCommitted
  else:
    raise newException(KafkaProtocolError, "invalid IsolationLevel wire value: " & $raw)

proc hash*(tp: TopicPartition): Hash =
  ## Hash for table keys (mixes topic and partition).
  var h: Hash = 0
  h = h !& hash(tp.topic)
  h = h !& hash(tp.partition)
  result = !$h

proc readResponseHeader*(r: var KafkaReader): ResponseHeader =
  ## Reads the non-flexible response header (just the correlation id). Flexible
  ## headers' trailing empty tagged-fields block is consumed separately by
  ## `skipResponseHeaderTaggedFields`.
  result.correlationId = r.readInt32()

# ApiVersions Response (v0-v1), schema-generated. v1 adds throttleTimeMs at the
# END (after the apiKeys array).

const apiVersionsResponseSchema = MessageDef(
  name: "ApiVersionsResponse",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: -1,
  fields: @[
    FieldDef(
      name: "errorCode",
      kind: fkInt16,
      nimType: "KafkaErrorCode",
      sinceVersion: -1,
      untilVersion: -1,
    ),
    FieldDef(
      name: "apiKeys",
      kind: fkArray,
      nestedTypeName: "ApiVersionsResponseKey",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "apiKey", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
        FieldDef(name: "minVersion", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
        FieldDef(name: "maxVersion", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
      ],
    ),
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: 1, untilVersion: -1),
  ],
)

defineKafkaMessage(apiVersionsResponseSchema)

# Metadata API (v1-v12), schema-generated. topics is a struct array of
# MetadataRequestTopic (not the legacy seq[string]) so it can carry KIP-516
# topicId at v10+ without changing field types; callers build it from topic names.

const metadataRequestSchema = MessageDef(
  name: "MetadataRequest",
  apiKey: 3,
  maxVersion: 12,
  flexibleSince: 9,
  fields: @[
    FieldDef(
      name: "topics",
      kind: fkArray,
      nullable: true,
      nestedTypeName: "MetadataRequestTopic",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topicId", kind: fkUuid, sinceVersion: 10, untilVersion: -1),
        # Upstream allows a null Name at v10+ (topic-id requests); this client
        # addresses topics by name only (see `Broker.metadata`).
        FieldDef(name: "name", kind: fkString, sinceVersion: -1, untilVersion: -1),
      ],
    ),
    FieldDef(
      name: "allowAutoTopicCreation", kind: fkBool, sinceVersion: 4, untilVersion: -1
    ),
    FieldDef(
      name: "includeClusterAuthorizedOperations",
      kind: fkBool,
      sinceVersion: 8,
      untilVersion: 10,
    ),
    FieldDef(
      name: "includeTopicAuthorizedOperations",
      kind: fkBool,
      sinceVersion: 8,
      untilVersion: -1,
    ),
  ],
)

defineKafkaMessage(metadataRequestSchema)

const metadataResponseSchema = MessageDef(
  name: "MetadataResponse",
  apiKey: -1,
  maxVersion: 12,
  flexibleSince: 9,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: 3, untilVersion: -1),
    FieldDef(
      name: "brokers",
      kind: fkArray,
      nestedTypeName: "BrokerMeta",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "nodeId", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
        FieldDef(name: "host", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(name: "port", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "rack", kind: fkNullableString, sinceVersion: -1, untilVersion: -1
        ),
      ],
    ),
    FieldDef(
      name: "clusterId", kind: fkNullableString, sinceVersion: 2, untilVersion: -1
    ),
    FieldDef(name: "controllerId", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "TopicMeta",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(
          name: "errorCode",
          kind: fkInt16,
          nimType: "KafkaErrorCode",
          sinceVersion: -1,
          untilVersion: -1,
        ),
        # Upstream allows a null Name at v12+ in answers to topic-id requests. This
        # client sends names, so a null fails the decode loudly rather than
        # mis-keying the metadata cache.
        FieldDef(name: "name", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(name: "topicId", kind: fkUuid, sinceVersion: 10, untilVersion: -1),
        FieldDef(name: "isInternal", kind: fkBool, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "PartitionMeta",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            FieldDef(
              name: "errorCode",
              kind: fkInt16,
              nimType: "KafkaErrorCode",
              sinceVersion: -1,
              untilVersion: -1,
            ),
            FieldDef(
              name: "partitionIndex", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "leaderId", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "leaderEpoch", kind: fkInt32, sinceVersion: 7, untilVersion: -1
            ),
            FieldDef(
              name: "replicaNodes",
              kind: fkArray,
              elementKind: fkInt32,
              sinceVersion: -1,
              untilVersion: -1,
            ),
            FieldDef(
              name: "isrNodes",
              kind: fkArray,
              elementKind: fkInt32,
              sinceVersion: -1,
              untilVersion: -1,
            ),
            FieldDef(
              name: "offlineReplicas",
              kind: fkArray,
              elementKind: fkInt32,
              sinceVersion: 5,
              untilVersion: -1,
            ),
          ],
        ),
        FieldDef(
          name: "topicAuthorizedOperations",
          kind: fkInt32,
          sinceVersion: 8,
          untilVersion: -1,
        ),
      ],
    ),
    FieldDef(
      name: "clusterAuthorizedOperations",
      kind: fkInt32,
      sinceVersion: 8,
      untilVersion: 10,
    ),
  ],
)

defineKafkaMessage(metadataResponseSchema)

# Record / RecordHeader / RecordBatch (v2 wire format, KIP-98).
#
# Record's `offset`/`timestamp` are delta-encoded against the enclosing batch via
# the baseOffset/firstTimestamp `contextParams` + `deltaFrom`; `attributes` carries
# the spec-mandated zero byte but is `discardOnRead` (absent from the Nim type).
#
# RecordBatch uses `envelope: ekRecordBatchV2`: v2 fixed header + CRC32C verification
# + optional records-section compression (gzip/snappy/lz4/zstd). The field list
# describes the Nim object shape only; the envelope walks `records`, recursing into
# Record's generated encode/decode.

const recordSchema = MessageDef(
  name: "Record",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  contextParams: @[
    ContextParam(name: "baseOffset", typ: "int64"),
    ContextParam(name: "firstTimestamp", typ: "int64"),
  ],
  fields: @[
    FieldDef(
      name: "attributes",
      kind: fkInt8,
      sinceVersion: -1,
      untilVersion: -1,
      discardOnRead: true,
    ),
    FieldDef(
      name: "timestamp",
      kind: fkVarInt,
      sinceVersion: -1,
      untilVersion: -1,
      deltaFrom: "firstTimestamp",
    ),
    FieldDef(
      name: "offset",
      kind: fkVarInt,
      sinceVersion: -1,
      untilVersion: -1,
      deltaFrom: "baseOffset",
    ),
    FieldDef(name: "key", kind: fkNullableVarBytes, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "value", kind: fkNullableVarBytes, sinceVersion: -1, untilVersion: -1
    ),
    FieldDef(
      name: "headers",
      kind: fkArray,
      sinceVersion: -1,
      untilVersion: -1,
      arrayCountKind: ackVarint,
      nestedTypeName: "RecordHeader",
      nested: @[
        FieldDef(name: "key", kind: fkVarString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "value", kind: fkNullableVarBytes, sinceVersion: -1, untilVersion: -1
        ),
      ],
    ),
  ],
)

defineKafkaMessage(recordSchema)

const recordBatchSchema = MessageDef(
  name: "RecordBatch",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  envelope: ekRecordBatchV2,
  fields: @[
    FieldDef(name: "baseOffset", kind: fkInt64, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "batchLength", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "partitionLeaderEpoch", kind: fkInt32, sinceVersion: -1, untilVersion: -1
    ),
    FieldDef(name: "magic", kind: fkInt8, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "crc", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "attributes", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "lastOffsetDelta", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "firstTimestamp", kind: fkInt64, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "maxTimestamp", kind: fkInt64, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "producerId", kind: fkInt64, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "producerEpoch", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "baseSequence", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "records",
      kind: fkArray,
      sinceVersion: -1,
      untilVersion: -1,
      elementTypeName: "Record",
    ),
  ],
)

defineKafkaMessage(recordBatchSchema)

# Fetch API (v4-v11), schema-generated. The response's per-partition records field
# is read as length-prefixed bytes into `recordsRaw`; the fetcher parses them into
# RecordBatches post-fetch via the generated `decode(rdr, RecordBatch)`. Flexible
# (v12+) and KIP-516 topicId (v13+) are out of scope: they need topic→topicId churn
# and tagged-fields-resident struct payloads the macro does not yet emit.

const fetchRequestSchema = MessageDef(
  name: "FetchRequest",
  apiKey: 1,
  maxVersion: 11,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "replicaId", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "maxWaitMs", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "minBytes", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "maxBytes", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "isolationLevel",
      kind: fkInt8,
      nimType: "IsolationLevel",
      nimTypeCtor: "toIsolationLevel",
      sinceVersion: -1,
      untilVersion: -1,
    ),
    FieldDef(name: "sessionId", kind: fkInt32, sinceVersion: 7, untilVersion: -1),
    FieldDef(name: "sessionEpoch", kind: fkInt32, sinceVersion: 7, untilVersion: -1),
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "FetchTopicReq",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "FetchPartitionReq",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            FieldDef(
              name: "partition", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "currentLeaderEpoch",
              kind: fkInt32,
              sinceVersion: 9,
              untilVersion: -1,
            ),
            FieldDef(
              name: "fetchOffset", kind: fkInt64, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "logStartOffset", kind: fkInt64, sinceVersion: 5, untilVersion: -1
            ),
            FieldDef(
              name: "maxBytes", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
          ],
        ),
      ],
    ),
    FieldDef(
      name: "forgottenTopicsData",
      kind: fkArray,
      nestedTypeName: "ForgottenTopic",
      sinceVersion: 7,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          elementKind: fkInt32,
          sinceVersion: -1,
          untilVersion: -1,
        ),
      ],
    ),
    FieldDef(name: "rackId", kind: fkString, sinceVersion: 11, untilVersion: -1),
  ],
)

defineKafkaMessage(fetchRequestSchema)

const fetchResponseSchema = MessageDef(
  name: "FetchResponse",
  apiKey: -1,
  maxVersion: 11,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "errorCode",
      kind: fkInt16,
      nimType: "KafkaErrorCode",
      sinceVersion: 7,
      untilVersion: -1,
    ),
    FieldDef(name: "sessionId", kind: fkInt32, sinceVersion: 7, untilVersion: -1),
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "FetchTopicResp",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "FetchPartitionResp",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            FieldDef(
              name: "partition", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "errorCode",
              kind: fkInt16,
              nimType: "KafkaErrorCode",
              sinceVersion: -1,
              untilVersion: -1,
            ),
            FieldDef(
              name: "highWatermark", kind: fkInt64, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "lastStableOffset",
              kind: fkInt64,
              sinceVersion: -1,
              untilVersion: -1,
            ),
            FieldDef(
              name: "logStartOffset", kind: fkInt64, sinceVersion: 5, untilVersion: -1
            ),
            FieldDef(
              name: "abortedTransactions",
              kind: fkArray,
              nullable: true,
              nestedTypeName: "AbortedTransaction",
              sinceVersion: -1,
              untilVersion: -1,
              nested: @[
                FieldDef(
                  name: "producerId", kind: fkInt64, sinceVersion: -1, untilVersion: -1
                ),
                FieldDef(
                  name: "firstOffset", kind: fkInt64, sinceVersion: -1, untilVersion: -1
                ),
              ],
            ),
            FieldDef(
              name: "preferredReadReplica",
              kind: fkInt32,
              sinceVersion: 11,
              untilVersion: -1,
            ),
            FieldDef(
              name: "recordsRaw",
              kind: fkNullableBytes,
              sinceVersion: -1,
              untilVersion: -1,
            ),
          ],
        ),
      ],
    ),
  ],
)

defineKafkaMessage(fetchResponseSchema)

# OffsetCommit API (v2-v8), schema-generated. v0/v1 are intentionally not modelled
# (the negotiated minimum is v2). Additive fields: v6 (KIP-320)
# committedLeaderEpoch fences commits made against a stale view after an unclean
# leader election; v7 (KIP-345) groupInstanceId (static membership, always sent as
# none(string), not exposed by ConsumerConfig); v8 (KIP-482) flexible strings +
# tagged_fields. retentionTimeMs was removed at v5 (untilVersion: 4 gates v2-v4).

const offsetCommitRequestSchema = MessageDef(
  name: "OffsetCommitRequest",
  apiKey: 8,
  maxVersion: 8,
  flexibleSince: 8,
  fields: @[
    FieldDef(name: "groupId", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "generationId", kind: fkInt32, sinceVersion: 1, untilVersion: -1),
    FieldDef(name: "memberId", kind: fkString, sinceVersion: 1, untilVersion: -1),
    FieldDef(
      name: "groupInstanceId", kind: fkNullableString, sinceVersion: 7, untilVersion: -1
    ),
    FieldDef(name: "retentionTimeMs", kind: fkInt64, sinceVersion: 2, untilVersion: 4),
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "OffsetCommitTopicReq",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "OffsetCommitPartitionReq",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            FieldDef(
              name: "partition", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(name: "offset", kind: fkInt64, sinceVersion: -1, untilVersion: -1),
            FieldDef(
              name: "committedLeaderEpoch",
              kind: fkInt32,
              sinceVersion: 6,
              untilVersion: -1,
            ),
            FieldDef(
              name: "metadata",
              kind: fkNullableString,
              sinceVersion: -1,
              untilVersion: -1,
            ),
          ],
        ),
      ],
    ),
  ],
)

defineKafkaMessage(offsetCommitRequestSchema)

const offsetCommitResponseSchema = MessageDef(
  name: "OffsetCommitResponse",
  apiKey: -1,
  maxVersion: 8,
  flexibleSince: 8,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: 3, untilVersion: -1),
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "OffsetCommitTopicResp",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "OffsetCommitPartitionResp",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            FieldDef(
              name: "partition", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "errorCode",
              kind: fkInt16,
              nimType: "KafkaErrorCode",
              sinceVersion: -1,
              untilVersion: -1,
            ),
          ],
        ),
      ],
    ),
  ],
)

defineKafkaMessage(offsetCommitResponseSchema)

# Heartbeat / InitProducerId / EndTxn requests, schema-generated. Heartbeat is
# non-flexible through v1. InitProducerId is flexible at v2 and EndTxn at v3;
# InitProducerId v3 adds producerId/producerEpoch, gated so its v0/v2 request wire
# is unchanged.

const heartbeatRequestSchema = MessageDef(
  name: "HeartbeatRequest",
  apiKey: 12,
  maxVersion: 1,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "groupId", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "generationId", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "memberId", kind: fkString, sinceVersion: -1, untilVersion: -1),
  ],
)

defineKafkaMessage(heartbeatRequestSchema)

const heartbeatResponseSchema = MessageDef(
  name: "HeartbeatResponse",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: 1, untilVersion: -1),
    FieldDef(
      name: "errorCode",
      kind: fkInt16,
      nimType: "KafkaErrorCode",
      sinceVersion: -1,
      untilVersion: -1,
    ),
  ],
)

defineKafkaMessage(heartbeatResponseSchema)

const initProducerIdRequestSchema = MessageDef(
  name: "InitProducerIdRequest",
  apiKey: 22,
  maxVersion: 3,
  flexibleSince: 2,
  fields: @[
    FieldDef(
      name: "transactionalId",
      kind: fkNullableString,
      sinceVersion: -1,
      untilVersion: -1,
    ),
    FieldDef(
      name: "transactionTimeoutMs", kind: fkInt32, sinceVersion: -1, untilVersion: -1
    ),
    FieldDef(name: "producerId", kind: fkInt64, sinceVersion: 3, untilVersion: -1),
    FieldDef(name: "producerEpoch", kind: fkInt16, sinceVersion: 3, untilVersion: -1),
  ],
)

defineKafkaMessage(initProducerIdRequestSchema)

const initProducerIdResponseSchema = MessageDef(
  name: "InitProducerIdResponse",
  apiKey: -1,
  maxVersion: 3,
  flexibleSince: 2,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "errorCode",
      kind: fkInt16,
      nimType: "KafkaErrorCode",
      sinceVersion: -1,
      untilVersion: -1,
    ),
    FieldDef(name: "producerId", kind: fkInt64, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "producerEpoch", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
  ],
)

defineKafkaMessage(initProducerIdResponseSchema)

const endTxnRequestSchema = MessageDef(
  name: "EndTxnRequest",
  apiKey: 26,
  maxVersion: 3,
  flexibleSince: 3,
  fields: @[
    FieldDef(
      name: "transactionalId", kind: fkString, sinceVersion: -1, untilVersion: -1
    ),
    FieldDef(name: "producerId", kind: fkInt64, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "producerEpoch", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "committed", kind: fkBool, sinceVersion: -1, untilVersion: -1),
  ],
)

defineKafkaMessage(endTxnRequestSchema)

const endTxnResponseSchema = MessageDef(
  name: "EndTxnResponse",
  apiKey: -1,
  maxVersion: 3,
  flexibleSince: 3,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "errorCode",
      kind: fkInt16,
      nimType: "KafkaErrorCode",
      sinceVersion: -1,
      untilVersion: -1,
    ),
  ],
)

defineKafkaMessage(endTxnResponseSchema)

# LeaveGroup API (v0-v1), schema-generated. v1 prepends throttle_time_ms to the
# response; the request wire is unchanged (v3+ members-array is a different schema).

const leaveGroupRequestSchema = MessageDef(
  name: "LeaveGroupRequest",
  apiKey: 13,
  maxVersion: 1,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "groupId", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "memberId", kind: fkString, sinceVersion: -1, untilVersion: -1),
  ],
)

defineKafkaMessage(leaveGroupRequestSchema)

const leaveGroupResponseSchema = MessageDef(
  name: "LeaveGroupResponse",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: 1, untilVersion: -1),
    FieldDef(
      name: "errorCode",
      kind: fkInt16,
      nimType: "KafkaErrorCode",
      sinceVersion: -1,
      untilVersion: -1,
    ),
  ],
)

defineKafkaMessage(leaveGroupResponseSchema)

# SyncGroup API (v0-v1), schema-generated. v1 prepends throttle_time_ms to the
# response; the request carries (memberId, assignment) tuples from the leader.

const syncGroupRequestSchema = MessageDef(
  name: "SyncGroupRequest",
  apiKey: 14,
  maxVersion: 1,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "groupId", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "generationId", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "memberId", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "assignments",
      kind: fkArray,
      nestedTypeName: "SyncGroupAssignment",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "memberId", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(name: "assignment", kind: fkBytes, sinceVersion: -1, untilVersion: -1),
      ],
    ),
  ],
)

defineKafkaMessage(syncGroupRequestSchema)

const syncGroupResponseSchema = MessageDef(
  name: "SyncGroupResponse",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: 1, untilVersion: -1),
    FieldDef(
      name: "errorCode",
      kind: fkInt16,
      nimType: "KafkaErrorCode",
      sinceVersion: -1,
      untilVersion: -1,
    ),
    # Brokers send a null assignment with rebalance-in-progress errors; modelled as
    # nullable bytes so the wire null is accepted (the consumer ignores it via the
    # errorCode early-return).
    FieldDef(
      name: "assignment", kind: fkNullableBytes, sinceVersion: -1, untilVersion: -1
    ),
  ],
)

defineKafkaMessage(syncGroupResponseSchema)

# JoinGroup API (v1-v2), schema-generated. v2 prepends throttle_time_ms to the
# response. request.protocols lists protocol candidates; response.members
# (populated only for the elected leader) carries each member's subscription.

const joinGroupRequestSchema = MessageDef(
  name: "JoinGroupRequest",
  apiKey: 11,
  maxVersion: 2,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "groupId", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "sessionTimeoutMs", kind: fkInt32, sinceVersion: -1, untilVersion: -1
    ),
    FieldDef(
      name: "rebalanceTimeoutMs", kind: fkInt32, sinceVersion: -1, untilVersion: -1
    ),
    FieldDef(name: "memberId", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "protocolType", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "protocols",
      kind: fkArray,
      nestedTypeName: "JoinGroupProtocol",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "name", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(name: "metadata", kind: fkBytes, sinceVersion: -1, untilVersion: -1),
      ],
    ),
  ],
)

defineKafkaMessage(joinGroupRequestSchema)

const joinGroupResponseSchema = MessageDef(
  name: "JoinGroupResponse",
  apiKey: -1,
  maxVersion: 2,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: 2, untilVersion: -1),
    FieldDef(
      name: "errorCode",
      kind: fkInt16,
      nimType: "KafkaErrorCode",
      sinceVersion: -1,
      untilVersion: -1,
    ),
    FieldDef(name: "generationId", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "protocolName", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "leader", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "memberId", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "members",
      kind: fkArray,
      nestedTypeName: "JoinGroupMember",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "memberId", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(name: "metadata", kind: fkBytes, sinceVersion: -1, untilVersion: -1),
      ],
    ),
  ],
)

defineKafkaMessage(joinGroupResponseSchema)

# FindCoordinator API (v0-v1), schema-generated. v1 adds coordinatorType
# (request: 0=group, 1=transaction), throttle_time_ms, and error_message.
# error_message is discardOnRead — errorCode already names the failure.

const findCoordinatorRequestSchema = MessageDef(
  name: "FindCoordinatorRequest",
  apiKey: 10,
  maxVersion: 1,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "key", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "coordinatorType", kind: fkInt8, sinceVersion: 1, untilVersion: -1),
  ],
)

defineKafkaMessage(findCoordinatorRequestSchema)

const findCoordinatorResponseSchema = MessageDef(
  name: "FindCoordinatorResponse",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: 1, untilVersion: -1),
    FieldDef(
      name: "errorCode",
      kind: fkInt16,
      nimType: "KafkaErrorCode",
      sinceVersion: -1,
      untilVersion: -1,
    ),
    FieldDef(
      name: "errorMessage",
      kind: fkNullableString,
      sinceVersion: 1,
      untilVersion: -1,
      discardOnRead: true,
    ),
    FieldDef(name: "nodeId", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "host", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "port", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
  ],
)

defineKafkaMessage(findCoordinatorResponseSchema)

# ListOffsets API (v1-v2), schema-generated. v2 adds isolation_level (int8 wire,
# IsolationLevel enum) to the request and throttle_time_ms to the response.
# ilReadUncommitted (= 0) matches the broker default so v1 stays byte-identical.

const listOffsetsRequestSchema = MessageDef(
  name: "ListOffsetsRequest",
  apiKey: 2,
  maxVersion: 2,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "replicaId", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "isolationLevel",
      kind: fkInt8,
      nimType: "IsolationLevel",
      nimTypeCtor: "toIsolationLevel",
      sinceVersion: 2,
      untilVersion: -1,
    ),
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "ListOffsetsTopicReq",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "ListOffsetsPartitionReq",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            FieldDef(
              name: "partition", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "timestamp", kind: fkInt64, sinceVersion: -1, untilVersion: -1
            ),
          ],
        ),
      ],
    ),
  ],
)

defineKafkaMessage(listOffsetsRequestSchema)

const listOffsetsResponseSchema = MessageDef(
  name: "ListOffsetsResponse",
  apiKey: -1,
  maxVersion: 2,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: 2, untilVersion: -1),
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "ListOffsetsTopicResp",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "ListOffsetsPartitionResp",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            FieldDef(
              name: "partition", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "errorCode",
              kind: fkInt16,
              nimType: "KafkaErrorCode",
              sinceVersion: -1,
              untilVersion: -1,
            ),
            FieldDef(
              name: "timestamp", kind: fkInt64, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(name: "offset", kind: fkInt64, sinceVersion: -1, untilVersion: -1),
          ],
        ),
      ],
    ),
  ],
)

defineKafkaMessage(listOffsetsResponseSchema)

# OffsetForLeaderEpoch API (v2-v3), schema-generated. KIP-320 truncation detection.
# v2 adds currentLeaderEpoch (request) + throttle_time_ms (response); leaderEpoch
# exists since v1, and v3 adds replicaId (consumer sends -2). v4 (flexible) is not
# implemented.

const offsetForLeaderEpochRequestSchema = MessageDef(
  name: "OffsetForLeaderEpochRequest",
  apiKey: 23,
  maxVersion: 3,
  flexibleSince: -1,
  fields: @[
    # replicaId is v3+ only and sits before topics on the wire. Consumer sends -2.
    FieldDef(name: "replicaId", kind: fkInt32, sinceVersion: 3, untilVersion: -1),
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "OffsetForLeaderTopicReq",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "OffsetForLeaderPartitionReq",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            FieldDef(
              name: "partition", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "currentLeaderEpoch",
              kind: fkInt32,
              sinceVersion: 2,
              untilVersion: -1,
            ),
            FieldDef(
              name: "leaderEpoch", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
          ],
        ),
      ],
    ),
  ],
)

defineKafkaMessage(offsetForLeaderEpochRequestSchema)

const offsetForLeaderEpochResponseSchema = MessageDef(
  name: "OffsetForLeaderEpochResponse",
  apiKey: -1,
  maxVersion: 3,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: 2, untilVersion: -1),
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "OffsetForLeaderTopicResp",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "OffsetForLeaderPartitionResp",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            # errorCode precedes partition here (reverse of ListOffsets).
            FieldDef(
              name: "errorCode",
              kind: fkInt16,
              nimType: "KafkaErrorCode",
              sinceVersion: -1,
              untilVersion: -1,
            ),
            FieldDef(
              name: "partition", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "leaderEpoch", kind: fkInt32, sinceVersion: 1, untilVersion: -1
            ),
            FieldDef(
              name: "endOffset", kind: fkInt64, sinceVersion: -1, untilVersion: -1
            ),
          ],
        ),
      ],
    ),
  ],
)

defineKafkaMessage(offsetForLeaderEpochResponseSchema)

# OffsetFetch API (v1-v5), schema-generated. Field order mirrors the wire:
# throttleTimeMs (v3+ prefix) → topics → errorCode (v2+ trailer), so a single
# r.decode(OffsetFetchResponse, version) suffices. v5 adds committedLeaderEpoch
# between offset and metadata, seeding KIP-320 truncation validation.

const offsetFetchRequestSchema = MessageDef(
  name: "OffsetFetchRequest",
  apiKey: 9,
  maxVersion: 5,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "groupId", kind: fkString, sinceVersion: -1, untilVersion: -1),
    # Upstream makes `topics` nullable at v2-7 (null = every topic). This client
    # always sends a concrete list, so it never needs the null marker.
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "OffsetFetchTopicReq",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          elementKind: fkInt32,
          sinceVersion: -1,
          untilVersion: -1,
        ),
      ],
    ),
  ],
)

defineKafkaMessage(offsetFetchRequestSchema)

const offsetFetchResponseSchema = MessageDef(
  name: "OffsetFetchResponse",
  apiKey: -1,
  maxVersion: 5,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: 3, untilVersion: -1),
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "OffsetFetchTopicResp",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "OffsetFetchPartitionResp",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            FieldDef(
              name: "partition", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(name: "offset", kind: fkInt64, sinceVersion: -1, untilVersion: -1),
            # KIP-320 committed leader epoch, added at v5. Below v5 the wire has no
            # such field; broker.offsetFetch normalizes the decoded zero to -1 so the
            # sentinel is uniform for callers.
            FieldDef(
              name: "committedLeaderEpoch",
              kind: fkInt32,
              sinceVersion: 5,
              untilVersion: -1,
            ),
            FieldDef(
              name: "metadata",
              kind: fkNullableString,
              sinceVersion: -1,
              untilVersion: -1,
            ),
            FieldDef(
              name: "errorCode",
              kind: fkInt16,
              nimType: "KafkaErrorCode",
              sinceVersion: -1,
              untilVersion: -1,
            ),
          ],
        ),
      ],
    ),
    FieldDef(
      name: "errorCode",
      kind: fkInt16,
      nimType: "KafkaErrorCode",
      sinceVersion: 2,
      untilVersion: -1,
    ),
  ],
)

defineKafkaMessage(offsetFetchResponseSchema)

# Produce API (v3), schema-generated. ProduceResponse carries throttle_time_ms at
# the END (after topics), which the schema field order mirrors. recordSet is
# non-nullable fkBytes — the producer materialises the RecordBatch bytes first.

const produceRequestSchema = MessageDef(
  name: "ProduceRequest",
  apiKey: 0,
  maxVersion: 7,
  flexibleSince: -1,
  fields: @[
    FieldDef(
      name: "transactionalId",
      kind: fkNullableString,
      sinceVersion: -1,
      untilVersion: -1,
    ),
    FieldDef(name: "acks", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "timeoutMs", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "ProduceTopicReq",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "ProducePartitionReq",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            FieldDef(
              name: "partition", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "recordSet", kind: fkBytes, sinceVersion: -1, untilVersion: -1
            ),
          ],
        ),
      ],
    ),
  ],
)

defineKafkaMessage(produceRequestSchema)

const produceResponseSchema = MessageDef(
  name: "ProduceResponse",
  apiKey: -1,
  maxVersion: 7,
  flexibleSince: -1,
  fields: @[
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "ProduceTopicResp",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "ProducePartitionResp",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            FieldDef(
              name: "partition", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "errorCode",
              kind: fkInt16,
              nimType: "KafkaErrorCode",
              sinceVersion: -1,
              untilVersion: -1,
            ),
            FieldDef(
              name: "baseOffset", kind: fkInt64, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "timestampMs", kind: fkInt64, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              # KIP-31/KIP-227: logStartOffset added at v5; older versions skip it,
              # keeping the wire byte-identical to v3/v4.
              name: "logStartOffset",
              kind: fkInt64,
              sinceVersion: 5,
              untilVersion: -1,
            ),
          ],
        ),
      ],
    ),
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
  ],
)

defineKafkaMessage(produceResponseSchema)

# AddPartitionsToTxn API (v0-v3), schema-generated. The response's nested topic
# array is named `errors` (not `topics`), matching upstream protocol naming.

const addPartitionsToTxnRequestSchema = MessageDef(
  name: "AddPartitionsToTxnRequest",
  apiKey: 24,
  maxVersion: 3,
  flexibleSince: 3,
  fields: @[
    FieldDef(
      name: "transactionalId", kind: fkString, sinceVersion: -1, untilVersion: -1
    ),
    FieldDef(name: "producerId", kind: fkInt64, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "producerEpoch", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "AddPartitionsToTxnTopicReq",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          elementKind: fkInt32,
          sinceVersion: -1,
          untilVersion: -1,
        ),
      ],
    ),
  ],
)

defineKafkaMessage(addPartitionsToTxnRequestSchema)

const addPartitionsToTxnResponseSchema = MessageDef(
  name: "AddPartitionsToTxnResponse",
  apiKey: -1,
  maxVersion: 3,
  flexibleSince: 3,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "errors",
      kind: fkArray,
      nestedTypeName: "AddPartitionsToTxnTopicResp",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "AddPartitionsToTxnPartitionResp",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            FieldDef(
              name: "partition", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "errorCode",
              kind: fkInt16,
              nimType: "KafkaErrorCode",
              sinceVersion: -1,
              untilVersion: -1,
            ),
          ],
        ),
      ],
    ),
  ],
)

defineKafkaMessage(addPartitionsToTxnResponseSchema)

const addOffsetsToTxnRequestSchema = MessageDef(
  name: "AddOffsetsToTxnRequest",
  apiKey: 25,
  maxVersion: 3,
  flexibleSince: 3,
  fields: @[
    FieldDef(
      name: "transactionalId", kind: fkString, sinceVersion: -1, untilVersion: -1
    ),
    FieldDef(name: "producerId", kind: fkInt64, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "producerEpoch", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "groupId", kind: fkString, sinceVersion: -1, untilVersion: -1),
  ],
)

defineKafkaMessage(addOffsetsToTxnRequestSchema)

const addOffsetsToTxnResponseSchema = MessageDef(
  name: "AddOffsetsToTxnResponse",
  apiKey: -1,
  maxVersion: 3,
  flexibleSince: 3,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "errorCode",
      kind: fkInt16,
      nimType: "KafkaErrorCode",
      sinceVersion: -1,
      untilVersion: -1,
    ),
  ],
)

defineKafkaMessage(addOffsetsToTxnResponseSchema)

const txnOffsetCommitRequestSchema = MessageDef(
  name: "TxnOffsetCommitRequest",
  apiKey: 28,
  maxVersion: 3,
  flexibleSince: 3,
  fields: @[
    FieldDef(
      name: "transactionalId", kind: fkString, sinceVersion: -1, untilVersion: -1
    ),
    FieldDef(name: "groupId", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "producerId", kind: fkInt64, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "producerEpoch", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
    # KIP-447 consumer-group fencing fields, added at v3. Sentinels
    # (generationId = -1, memberId = "") mean "no fencing" and are what the
    # legacy sendOffsetsToTransaction(offsets, groupId) overload sends.
    FieldDef(name: "generationId", kind: fkInt32, sinceVersion: 3, untilVersion: -1),
    FieldDef(name: "memberId", kind: fkString, sinceVersion: 3, untilVersion: -1),
    FieldDef(
      name: "groupInstanceId", kind: fkNullableString, sinceVersion: 3, untilVersion: -1
    ),
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "TxnOffsetCommitTopicReq",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "TxnOffsetCommitPartitionReq",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            FieldDef(
              name: "partition", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(name: "offset", kind: fkInt64, sinceVersion: -1, untilVersion: -1),
            # KIP-320 committed leader epoch, added at v2 (between offset and
            # metadata on the wire). Older negotiated versions skip it.
            FieldDef(
              name: "committedLeaderEpoch",
              kind: fkInt32,
              sinceVersion: 2,
              untilVersion: -1,
            ),
            FieldDef(
              name: "metadata",
              kind: fkNullableString,
              sinceVersion: -1,
              untilVersion: -1,
            ),
          ],
        ),
      ],
    ),
  ],
)

defineKafkaMessage(txnOffsetCommitRequestSchema)

const txnOffsetCommitResponseSchema = MessageDef(
  name: "TxnOffsetCommitResponse",
  apiKey: -1,
  maxVersion: 3,
  flexibleSince: 3,
  fields: @[
    FieldDef(name: "throttleTimeMs", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "topics",
      kind: fkArray,
      nestedTypeName: "TxnOffsetCommitTopicResp",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "topic", kind: fkString, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "partitions",
          kind: fkArray,
          nestedTypeName: "TxnOffsetCommitPartitionResp",
          sinceVersion: -1,
          untilVersion: -1,
          nested: @[
            FieldDef(
              name: "partition", kind: fkInt32, sinceVersion: -1, untilVersion: -1
            ),
            FieldDef(
              name: "errorCode",
              kind: fkInt16,
              nimType: "KafkaErrorCode",
              sinceVersion: -1,
              untilVersion: -1,
            ),
          ],
        ),
      ],
    ),
  ],
)

defineKafkaMessage(txnOffsetCommitResponseSchema)

# SASL APIs (SaslHandshake v1, SaslAuthenticate v1), schema-generated. Both are
# non-flexible at v1 (KIP-482 made them flexible at v2). sessionLifetimeMs on the
# authenticate response is kept as a real field though the broker layer ignores it
# today, leaving room for re-auth logic later.

const saslHandshakeRequestSchema = MessageDef(
  name: "SaslHandshakeRequest",
  apiKey: 17,
  maxVersion: 1,
  flexibleSince: -1,
  fields:
    @[FieldDef(name: "mechanism", kind: fkString, sinceVersion: -1, untilVersion: -1)],
)

defineKafkaMessage(saslHandshakeRequestSchema)

const saslHandshakeResponseSchema = MessageDef(
  name: "SaslHandshakeResponse",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: -1,
  fields: @[
    FieldDef(
      name: "errorCode",
      kind: fkInt16,
      nimType: "KafkaErrorCode",
      sinceVersion: -1,
      untilVersion: -1,
    ),
    FieldDef(
      name: "mechanisms",
      kind: fkArray,
      elementKind: fkString,
      sinceVersion: -1,
      untilVersion: -1,
    ),
  ],
)

defineKafkaMessage(saslHandshakeResponseSchema)

const saslAuthenticateRequestSchema = MessageDef(
  name: "SaslAuthenticateRequest",
  apiKey: 36,
  maxVersion: 1,
  flexibleSince: -1,
  fields:
    @[FieldDef(name: "authBytes", kind: fkBytes, sinceVersion: -1, untilVersion: -1)],
)

defineKafkaMessage(saslAuthenticateRequestSchema)

const saslAuthenticateResponseSchema = MessageDef(
  name: "SaslAuthenticateResponse",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: -1,
  fields: @[
    FieldDef(
      name: "errorCode",
      kind: fkInt16,
      nimType: "KafkaErrorCode",
      sinceVersion: -1,
      untilVersion: -1,
    ),
    FieldDef(
      name: "errorMessage", kind: fkNullableString, sinceVersion: -1, untilVersion: -1
    ),
    FieldDef(name: "authBytes", kind: fkBytes, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "sessionLifetimeMs", kind: fkInt64, sinceVersion: 1, untilVersion: -1
    ),
  ],
)

defineKafkaMessage(saslAuthenticateResponseSchema)

static:
  # Compile-time guard: client maxVersion must not exceed each schema's maxVersion.
  # `negotiateVersion` returns min(clientMax, brokerMax) and the codec stops gating at
  # its own maxVersion, so an over-fat client range would silently emit or
  # parse a frame missing the newer fields; failing the build beats shipping it.
  #
  # `reqKey` pins each request schema's `apiKey` literal to `ord(k)`: the macro never
  # reads `MessageDef.apiKey`, so a drifted literal would otherwise compile cleanly.
  const versionChecks: array[19, tuple[k: ApiKey, reqMax, respMax, reqKey: int]] = [
    (
      akProduce, produceRequestSchema.maxVersion, produceResponseSchema.maxVersion,
      produceRequestSchema.apiKey,
    ),
    (
      akFetch, fetchRequestSchema.maxVersion, fetchResponseSchema.maxVersion,
      fetchRequestSchema.apiKey,
    ),
    (
      akListOffsets, listOffsetsRequestSchema.maxVersion,
      listOffsetsResponseSchema.maxVersion, listOffsetsRequestSchema.apiKey,
    ),
    (
      akMetadata, metadataRequestSchema.maxVersion, metadataResponseSchema.maxVersion,
      metadataRequestSchema.apiKey,
    ),
    (
      akOffsetCommit, offsetCommitRequestSchema.maxVersion,
      offsetCommitResponseSchema.maxVersion, offsetCommitRequestSchema.apiKey,
    ),
    (
      akOffsetFetch, offsetFetchRequestSchema.maxVersion,
      offsetFetchResponseSchema.maxVersion, offsetFetchRequestSchema.apiKey,
    ),
    (
      akFindCoordinator, findCoordinatorRequestSchema.maxVersion,
      findCoordinatorResponseSchema.maxVersion, findCoordinatorRequestSchema.apiKey,
    ),
    (
      akJoinGroup, joinGroupRequestSchema.maxVersion,
      joinGroupResponseSchema.maxVersion, joinGroupRequestSchema.apiKey,
    ),
    (
      akHeartbeat, heartbeatRequestSchema.maxVersion,
      heartbeatResponseSchema.maxVersion, heartbeatRequestSchema.apiKey,
    ),
    (
      akLeaveGroup, leaveGroupRequestSchema.maxVersion,
      leaveGroupResponseSchema.maxVersion, leaveGroupRequestSchema.apiKey,
    ),
    (
      akSyncGroup, syncGroupRequestSchema.maxVersion,
      syncGroupResponseSchema.maxVersion, syncGroupRequestSchema.apiKey,
    ),
    (
      akInitProducerId, initProducerIdRequestSchema.maxVersion,
      initProducerIdResponseSchema.maxVersion, initProducerIdRequestSchema.apiKey,
    ),
    (
      akOffsetForLeaderEpoch, offsetForLeaderEpochRequestSchema.maxVersion,
      offsetForLeaderEpochResponseSchema.maxVersion,
      offsetForLeaderEpochRequestSchema.apiKey,
    ),
    (
      akAddPartitionsToTxn, addPartitionsToTxnRequestSchema.maxVersion,
      addPartitionsToTxnResponseSchema.maxVersion,
      addPartitionsToTxnRequestSchema.apiKey,
    ),
    (
      akAddOffsetsToTxn, addOffsetsToTxnRequestSchema.maxVersion,
      addOffsetsToTxnResponseSchema.maxVersion, addOffsetsToTxnRequestSchema.apiKey,
    ),
    (
      akEndTxn, endTxnRequestSchema.maxVersion, endTxnResponseSchema.maxVersion,
      endTxnRequestSchema.apiKey,
    ),
    (
      akTxnOffsetCommit, txnOffsetCommitRequestSchema.maxVersion,
      txnOffsetCommitResponseSchema.maxVersion, txnOffsetCommitRequestSchema.apiKey,
    ),
    (
      akSaslHandshake, saslHandshakeRequestSchema.maxVersion,
      saslHandshakeResponseSchema.maxVersion, saslHandshakeRequestSchema.apiKey,
    ),
    (
      akSaslAuthenticate, saslAuthenticateRequestSchema.maxVersion,
      saslAuthenticateResponseSchema.maxVersion, saslAuthenticateRequestSchema.apiKey,
    ),
  ]
  for c in versionChecks:
    doAssert c.reqKey == ord(c.k),
      "request schema apiKey " & $c.reqKey & " for " & $c.k & " does not match enum ord " &
        $ord(c.k)
    let clientRange = supportedVersion(c.k)
    let clientMax = clientRange.maxVersion.int
    doAssert clientMax <= c.reqMax,
      "client maxVersion " & $clientMax & " for " & $c.k &
        " exceeds its request schema maxVersion " & $c.reqMax &
        " — extend the schema in messages.nim before raising the range in api_keys.nim"
    doAssert clientMax <= c.respMax,
      "client maxVersion " & $clientMax & " for " & $c.k &
        " exceeds its response schema maxVersion " & $c.respMax &
        " — extend the schema in messages.nim before raising the range in api_keys.nim"
    # Pin the client minimum at the v0 schema floor: an inverted or negative
    # range would negotiate nothing or parse a shape the codec never defined.
    let clientMin = clientRange.minVersion.int
    doAssert clientMin >= 0,
      "client minVersion " & $clientMin & " for " & $c.k &
        " is negative — schemas model from v0"
    doAssert clientMin <= clientMax,
      "client version range for " & $c.k & " is inverted (min " & $clientMin & " > max " &
        $clientMax & ")"
  # ApiVersions has no request schema (the request body is empty and built by
  # hand); guard the response only. The client pins ApiVersions to v0.
  doAssert supportedVersion(akApiVersions).maxVersion.int <=
    apiVersionsResponseSchema.maxVersion,
    "ApiVersions client maxVersion exceeds its response schema maxVersion"
