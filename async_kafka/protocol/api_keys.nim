type
  ApiKey* = enum
    akProduce = (0, "Produce")
    akFetch = (1, "Fetch")
    akListOffsets = (2, "ListOffsets")
    akMetadata = (3, "Metadata")
    akOffsetCommit = (8, "OffsetCommit")
    akOffsetFetch = (9, "OffsetFetch")
    akFindCoordinator = (10, "FindCoordinator")
    akJoinGroup = (11, "JoinGroup")
    akHeartbeat = (12, "Heartbeat")
    akLeaveGroup = (13, "LeaveGroup")
    akSyncGroup = (14, "SyncGroup")
    akSaslHandshake = (17, "SaslHandshake")
    akApiVersions = (18, "ApiVersions")
    akInitProducerId = (22, "InitProducerId")
    akOffsetForLeaderEpoch = (23, "OffsetForLeaderEpoch")
    akAddPartitionsToTxn = (24, "AddPartitionsToTxn")
    akAddOffsetsToTxn = (25, "AddOffsetsToTxn")
    akEndTxn = (26, "EndTxn")
    akWriteTxnMarkers = (27, "WriteTxnMarkers")
    akTxnOffsetCommit = (28, "TxnOffsetCommit")
    akSaslAuthenticate = (36, "SaslAuthenticate")

  ApiVersionRange* = object
    minVersion*: int16
    maxVersion*: int16

proc supportedVersion*(apiKey: ApiKey): ApiVersionRange {.raises: [].} =
  case apiKey
  of akProduce:
    # v3 is the RecordBatch-v2 baseline; v5 adds logStartOffset to the partition
    # response. v8 (recordErrors) and v9 (flexible) are unimplemented.
    ApiVersionRange(minVersion: 3, maxVersion: 7)
  of akFetch:
    ApiVersionRange(minVersion: 4, maxVersion: 11)
  of akListOffsets:
    # v2 added isolation_level (request) and throttle_time_ms (response).
    ApiVersionRange(minVersion: 1, maxVersion: 2)
  of akMetadata:
    ApiVersionRange(minVersion: 1, maxVersion: 12)
  of akOffsetCommit:
    # v3 added throttle_time_ms (response); v5 dropped retentionTimeMs, v6 added
    # committedLeaderEpoch (KIP-320), v7 groupInstanceId (KIP-345), v8 became
    # flexible (KIP-482).
    ApiVersionRange(minVersion: 2, maxVersion: 8)
  of akOffsetFetch:
    # v2 added a top-level error_code (response) and nullable topics (request; we
    # always send a non-null array, wire-compatible); v3 added throttle_time_ms;
    # v5 added committedLeaderEpoch (KIP-320) so an assigned consumer can seed
    # OffsetForLeaderEpoch validation from the committed position. Request wire
    # unchanged v3→v5.
    ApiVersionRange(minVersion: 1, maxVersion: 5)
  of akFindCoordinator:
    ApiVersionRange(minVersion: 0, maxVersion: 1)
  of akJoinGroup:
    # v2 added throttle_time_ms (response); request wire unchanged.
    ApiVersionRange(minVersion: 1, maxVersion: 2)
  of akHeartbeat:
    # v1 added throttle_time_ms (response); request wire unchanged.
    ApiVersionRange(minVersion: 0, maxVersion: 1)
  of akLeaveGroup:
    # v1 added throttle_time_ms; v3+ changes the request shape.
    ApiVersionRange(minVersion: 0, maxVersion: 1)
  of akSyncGroup:
    # v1 added throttle_time_ms (response); request wire unchanged.
    ApiVersionRange(minVersion: 0, maxVersion: 1)
  of akSaslHandshake:
    ApiVersionRange(minVersion: 1, maxVersion: 1)
  of akApiVersions:
    ApiVersionRange(minVersion: 0, maxVersion: 0)
  of akInitProducerId:
    ApiVersionRange(minVersion: 0, maxVersion: 3)
  of akOffsetForLeaderEpoch:
    # v2 adds currentLeaderEpoch (request, KIP-320) and throttle_time_ms
    # (response); leaderEpoch exists since v1, and v3 adds replicaId. v4 (flexible,
    # KIP-482) is unimplemented.
    ApiVersionRange(minVersion: 2, maxVersion: 3)
  of akAddPartitionsToTxn:
    # v3 is flexible (KIP-482), the last version before KIP-890 reshaped this
    # into a broker-side verification API. Fields unchanged across v0-v3.
    ApiVersionRange(minVersion: 0, maxVersion: 3)
  of akAddOffsetsToTxn:
    # v3 is flexible (KIP-482); wire otherwise unchanged across v0-v3.
    ApiVersionRange(minVersion: 0, maxVersion: 3)
  of akEndTxn:
    # v3 is flexible (KIP-482); wire otherwise unchanged across v0-v3.
    ApiVersionRange(minVersion: 0, maxVersion: 3)
  of akWriteTxnMarkers:
    # Inter-broker only (KIP-98); listed for completeness and never sent. The
    # empty range negotiates to -1 (keUnsupportedVersion).
    ApiVersionRange(minVersion: 1, maxVersion: 0)
  of akTxnOffsetCommit:
    # v2 adds committedLeaderEpoch (KIP-320); v3 adds the group-fencing fields
    # and is flexible (KIP-447 / KIP-482).
    ApiVersionRange(minVersion: 0, maxVersion: 3)
  of akSaslAuthenticate:
    ApiVersionRange(minVersion: 1, maxVersion: 1)

proc negotiateVersion*(clientRange, brokerRange: ApiVersionRange): int16 =
  let maxVer = min(clientRange.maxVersion, brokerRange.maxVersion)
  let minVer = max(clientRange.minVersion, brokerRange.minVersion)
  if maxVer < minVer:
    return -1
  return maxVer
