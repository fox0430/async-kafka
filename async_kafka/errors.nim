type
  KafkaErrorCode* = distinct int16
    ## Wire error code from broker responses. The `ke*` constants cover the known
    ## registry: `kafkaErrorName` renders them and `errorClass` classifies them, so
    ## branch on the classification predicates rather than raw values. An unlisted
    ## code still decodes and classifies as `ecUnknown` instead of breaking against
    ## a newer broker.

  KafkaError* = object of CatchableError
    ## Base of every error this library raises. Catch it to handle all client
    ## failures in one place; use the subclasses below for narrower handling.

  KafkaConnectionError* = object of KafkaError
    ## Transport failure: the connection dropped, timed out, or was never
    ## established. Retriable; the coordinator driver treats it as a move.

  KafkaProtocolError* = object of KafkaError
    ## Malformed or unexpected bytes on the wire (bad frame length, undecodable
    ## response). Never retried blindly.

  KafkaDecompressionLimitError* = object of KafkaProtocolError
    ## A fetch response's cumulative decompressed bytes exceeded the budget for
    ## one response (per-batch caps alone leave the sum unbounded). Distinct from
    ## KafkaProtocolError so the fetcher can tell a policy violation from corrupt
    ## data: a corrupt batch is skipped, a budget violation fails the fetch.

  KafkaApiError* = object of KafkaError
    ## Broker-rejected request carrying the broker's error code. Retry behaviour
    ## comes from `errorClass(errorCode)`, not the exception type; the txn and
    ## idempotent poison paths branch on the raw code, so it must stay faithful.
    errorCode*: KafkaErrorCode

  KafkaTimeoutError* = object of KafkaError
    ## A request outlived its deadline. It may still have executed broker-side;
    ## the idempotent and transactional paths account for that ambiguity.

  KafkaClosedError* = object of KafkaError
    ## Raised on API calls to a closing/closed Producer/Consumer. Distinct from
    ## KafkaConnectionError so callers can tell "user closed me" from "network died".

  KafkaRecordRejectedError* = object of KafkaError
    ## A producer `send` refused the record client-side, before it touched a broker
    ## or the open transaction: it exceeds `maxBatchBytes`. Not retriable; unlike
    ## other send failures it leaves an open transaction committable, so a caller may
    ## skip the record (or dead-letter it in the same transaction) and still commit.
    ## A partition out of range of cached metadata stays a plain `KafkaError`, since
    ## the topic may have grown; broker-side limit failures poison the transaction.

  PartitionCommitError* = object
    ## Reused by KafkaCommitError and KafkaOffsetFetchError; "Commit" predates the reuse.
    topic*: string
    partition*: int32
    errorCode*: KafkaErrorCode

  KafkaCommitError* = object of KafkaApiError
    ## Offset commit rejected in whole or part. `errorCode` is the first
    ## non-`keNone` per-partition code; `partitionErrors` carries them all.
    partitionErrors*: seq[PartitionCommitError]

  KafkaCommitSkippedError* = object of KafkaError
    ## A consumer offset commit refused locally, with no broker round-trip, because
    ## the group generation makes it unsafe: no active generation, the fetched
    ## assignment lagging the group's generation (a rebalance window), or a
    ## `commitAsync` snapshot whose generation no longer matches the live one.
    ##
    ## A normal, non-fatal outcome, not a reason to recreate the consumer: the
    ## offsets were not committed, so whichever member owns the partition after the
    ## rebalance resumes from the last committed offset and re-fetches the records.
    ## Catch it and retry after the next poll, as with Java's
    ## `CommitFailedException`; the auto-commit loop already swallows it.

  KafkaOffsetFetchError* = object of KafkaApiError
    ## `errorCode` is the first non-`keNone` partition code; `partitionErrors`
    ## carries every one so the recovery driver can aggregate a mixed batch.
    partitionErrors*: seq[PartitionCommitError]

  KafkaRebalanceCallbackError* = object of KafkaError
    ## A user rebalance listener raised. Re-thrown from the poll()/start() that
    ## drove the rebalance, after the bookkeeping completed (mirroring the Java
    ## client): the exception reaches the caller but does not abort the
    ## rebalance. The original exception is in `parent`.

  LifecycleState* = enum
    ## Lifecycle of a Producer/Consumer. A component with an explicit `start()`
    ## stays lsUninitialized until it succeeds; one usable from construction
    ## starts in lsRunning. Close is four-stage: lsRunning→lsClosing (reject new
    ## calls), stop+join loops, drain in-flight work, release→lsClosed.
    lsUninitialized
    lsRunning
    lsClosing
    lsClosed

  ErrorClass* = enum
    ## Top-level retry classification of a Kafka error code, modelled on the
    ## Apache Kafka Java client:
    ##   ecNone     – success
    ##   ecRetriable – transient; retry after backoff
    ##   ecRebalanceRequired – group membership invalid; rejoin
    ##   ecAbortable – abort the in-progress transaction, then start a fresh one
    ##   ecFatal    – never retry. Mixes instance-level codes (PRODUCER_FENCED,
    ##                UNSUPPORTED_VERSION) with record-level ones (MESSAGE_TOO_LARGE,
    ##                INVALID_MSG_SIZE), so check `isInstanceFatal` before closing
    ##   ecDuplicate – duplicate sequence; the original idempotent write succeeded
    ##   ecNeedsProducerIdReset – reset the producer id before retrying (KIP-360)
    ##   ecUnknown  – unknown, or handled via a dedicated path (OFFSET_OUT_OF_RANGE)
    ecNone
    ecRetriable
    ecRebalanceRequired
    ecAbortable
    ecFatal
    ecDuplicate
    ecNeedsProducerIdReset
    ecUnknown

const
  # Values are the broker's protocol error codes and must stay aligned with them.
  keNone* = KafkaErrorCode(0)
  keOffsetOutOfRange* = KafkaErrorCode(1)
  keCorruptMessage* = KafkaErrorCode(2)
  keUnknownTopicOrPartition* = KafkaErrorCode(3)
  keLeaderNotAvailable* = KafkaErrorCode(5)
  keNotLeaderOrFollower* = KafkaErrorCode(6)
  keRequestTimedOut* = KafkaErrorCode(7)
  keMessageTooLarge* = KafkaErrorCode(10)
  keNetworkException* = KafkaErrorCode(13)
  keCoordinatorLoadInProgress* = KafkaErrorCode(14)
  keGroupCoordinatorNotAvailable* = KafkaErrorCode(15)
  keNotCoordinator* = KafkaErrorCode(16)
  keInvalidTopicException* = KafkaErrorCode(17)
  keRecordListTooLarge* = KafkaErrorCode(18)
  keNotEnoughReplicas* = KafkaErrorCode(19)
  keNotEnoughReplicasAfterAppend* = KafkaErrorCode(20)
  keInvalidRequiredAcks* = KafkaErrorCode(21)
  keIllegalGeneration* = KafkaErrorCode(22)
  keInconsistentGroupProtocol* = KafkaErrorCode(23)
  keUnknownMemberId* = KafkaErrorCode(25)
  keInvalidSessionTimeout* = KafkaErrorCode(26)
  keRebalanceInProgress* = KafkaErrorCode(27)
  keTopicAuthorizationFailed* = KafkaErrorCode(29)
  keGroupAuthorizationFailed* = KafkaErrorCode(30)
  keClusterAuthorizationFailed* = KafkaErrorCode(31)
  keInvalidTimestamp* = KafkaErrorCode(32)
  keUnsupportedSaslMechanism* = KafkaErrorCode(33)
  keIllegalSaslState* = KafkaErrorCode(34)
  keUnsupportedVersion* = KafkaErrorCode(35)
  keTopicAlreadyExists* = KafkaErrorCode(36)
  keInvalidRequest* = KafkaErrorCode(42)
  keUnsupportedForMessageFormat* = KafkaErrorCode(43)
  keOutOfOrderSequenceNumber* = KafkaErrorCode(45)
  keDuplicateSequenceNumber* = KafkaErrorCode(46)
  keInvalidProducerEpoch* = KafkaErrorCode(47)
  keInvalidTxnState* = KafkaErrorCode(48)
  keInvalidProducerIdMapping* = KafkaErrorCode(49)
  keConcurrentTransactions* = KafkaErrorCode(51)
    ## The broker's txn-state machine has not finished the previous transition; retry.
  keTransactionCoordinatorFenced* = KafkaErrorCode(52)
  keTransactionalIdAuthorizationFailed* = KafkaErrorCode(53)
  keSecurityDisabled* = KafkaErrorCode(54)
  keOperationNotAttempted* = KafkaErrorCode(55)
  keKafkaStorageError* = KafkaErrorCode(56)
  keSaslAuthenticationFailed* = KafkaErrorCode(58)
  keUnknownProducerId* = KafkaErrorCode(59)
  keFetchSessionIdNotFound* = KafkaErrorCode(70)
  keInvalidFetchSessionEpoch* = KafkaErrorCode(71)
  keListenerNotFound* = KafkaErrorCode(72)
    ## KIP-103: the requested listener (advertised security protocol) is not yet
    ## known cluster-wide; refresh metadata and retry.
  keGroupMaxSizeReached* = KafkaErrorCode(81)
  keFencedInstanceId* = KafkaErrorCode(82)
  keMemberIdRequired* = KafkaErrorCode(79)
  keProducerFenced* = KafkaErrorCode(90)
  keFencedLeaderEpoch* = KafkaErrorCode(74)
  keUnknownLeaderEpoch* = KafkaErrorCode(75)
  keTopicDeletionDisabled* = KafkaErrorCode(73)
  keUnsupportedCompressionType* = KafkaErrorCode(76)
    ## KIP-110: e.g. a zstd batch on an API version below the zstd floor
    ## (Produce < v7 / Fetch < v10).
  keInvalidRecord* = KafkaErrorCode(87)
    ## KIP-467: the broker rejected one or more records (validation failure).
    ## Distinct from CORRUPT_MESSAGE (2): transit corruption is retriable, a
    ## record rejected on its content is fatal.

  # Codes added for classification completeness
  keInvalidMsgSize* = KafkaErrorCode(4)
  keBrokerNotAvailable* = KafkaErrorCode(8)
  keReplicaNotAvailable* = KafkaErrorCode(9)
  keStaleControllerEpoch* = KafkaErrorCode(11)
  keOffsetMetadataTooLarge* = KafkaErrorCode(12)
  keInvalidGroupId* = KafkaErrorCode(24)
  keNotController* = KafkaErrorCode(41)
  keInvalidTransactionTimeout* = KafkaErrorCode(50)
  kePolicyViolation* = KafkaErrorCode(44)
  keGroupIdNotFound* = KafkaErrorCode(69)
  keStaleBrokerEpoch* = KafkaErrorCode(77)
  keUnstableOffsetCommit* = KafkaErrorCode(88)
  keThrottlingQuotaExceeded* = KafkaErrorCode(89)
  keResourceNotFound* = KafkaErrorCode(91)
  keDuplicateResource* = KafkaErrorCode(92)
  keUnacceptableCredential* = KafkaErrorCode(93)
  keInconsistentVoterSet* = KafkaErrorCode(94)
  keInvalidUpdateVersion* = KafkaErrorCode(95)
  keFeatureUpdateFailed* = KafkaErrorCode(96)
  keUnknownTopicId* = KafkaErrorCode(100)
  keTransactionalIdNotFound* = KafkaErrorCode(105)
  keFencedMemberEpoch* = KafkaErrorCode(110)
  keStaleMemberEpoch* = KafkaErrorCode(113)
  keTransactionAbortable* = KafkaErrorCode(120)
  kePreferredLeaderNotAvailable* = KafkaErrorCode(80)
    ## InvalidMetadataException (retriable): preferred leader election in progress.
  keFetchSessionTopicIdError* = KafkaErrorCode(106)
    ## KIP-516: fetch session has inconsistent topic IDs; drop it and start fresh.
  keOffsetNotAvailable* = KafkaErrorCode(78)
    ## KIP-207: the leader's high watermark lags after an election; retriable.
  keEligibleLeadersNotAvailable* = KafkaErrorCode(83)
    ## electLeaders Admin API only; never produced by producer/consumer RPCs.
  keElectionNotNeeded* = KafkaErrorCode(84)
    ## electLeaders Admin API only; never produced by producer/consumer RPCs.
  keNewLeaderElected* = KafkaErrorCode(108)
    ## AlterPartition only: leadership changed mid-operation; refresh and retry.
  keOffsetMovedToTieredStorage* = KafkaErrorCode(109)
    ## KIP-405: the offset's data has migrated to tiered storage. This client
    ## has no tiered-storage fetch path, so the code is terminal for the fetch.

  # Negative error codes
  keUnknownServerError* = KafkaErrorCode(-1)

  kafkaErrorNamePairs: array[88, (int16, string)] = [
    (0'i16, "NONE"),
    (1'i16, "OFFSET_OUT_OF_RANGE"),
    (2'i16, "CORRUPT_MESSAGE"),
    (3'i16, "UNKNOWN_TOPIC_OR_PARTITION"),
    (4'i16, "INVALID_MSG_SIZE"),
    (5'i16, "LEADER_NOT_AVAILABLE"),
    (6'i16, "NOT_LEADER_OR_FOLLOWER"),
    (7'i16, "REQUEST_TIMED_OUT"),
    (8'i16, "BROKER_NOT_AVAILABLE"),
    (9'i16, "REPLICA_NOT_AVAILABLE"),
    (10'i16, "MESSAGE_TOO_LARGE"),
    (11'i16, "STALE_CONTROLLER_EPOCH"),
    (12'i16, "OFFSET_METADATA_TOO_LARGE"),
    (13'i16, "NETWORK_EXCEPTION"),
    (14'i16, "COORDINATOR_LOAD_IN_PROGRESS"),
    (15'i16, "GROUP_COORDINATOR_NOT_AVAILABLE"),
    (16'i16, "NOT_COORDINATOR"),
    (17'i16, "INVALID_TOPIC_EXCEPTION"),
    (18'i16, "RECORD_LIST_TOO_LARGE"),
    (19'i16, "NOT_ENOUGH_REPLICAS"),
    (20'i16, "NOT_ENOUGH_REPLICAS_AFTER_APPEND"),
    (21'i16, "INVALID_REQUIRED_ACKS"),
    (22'i16, "ILLEGAL_GENERATION"),
    (23'i16, "INCONSISTENT_GROUP_PROTOCOL"),
    (24'i16, "INVALID_GROUP_ID"),
    (25'i16, "UNKNOWN_MEMBER_ID"),
    (26'i16, "INVALID_SESSION_TIMEOUT"),
    (27'i16, "REBALANCE_IN_PROGRESS"),
    (29'i16, "TOPIC_AUTHORIZATION_FAILED"),
    (30'i16, "GROUP_AUTHORIZATION_FAILED"),
    (31'i16, "CLUSTER_AUTHORIZATION_FAILED"),
    (32'i16, "INVALID_TIMESTAMP"),
    (33'i16, "UNSUPPORTED_SASL_MECHANISM"),
    (34'i16, "ILLEGAL_SASL_STATE"),
    (35'i16, "UNSUPPORTED_VERSION"),
    (36'i16, "TOPIC_ALREADY_EXISTS"),
    (41'i16, "NOT_CONTROLLER"),
    (42'i16, "INVALID_REQUEST"),
    (43'i16, "UNSUPPORTED_FOR_MESSAGE_FORMAT"),
    (44'i16, "POLICY_VIOLATION"),
    (45'i16, "OUT_OF_ORDER_SEQUENCE_NUMBER"),
    (46'i16, "DUPLICATE_SEQUENCE_NUMBER"),
    (47'i16, "INVALID_PRODUCER_EPOCH"),
    (48'i16, "INVALID_TXN_STATE"),
    (49'i16, "INVALID_PRODUCER_ID_MAPPING"),
    (50'i16, "INVALID_TRANSACTION_TIMEOUT"),
    (51'i16, "CONCURRENT_TRANSACTIONS"),
    (52'i16, "TRANSACTION_COORDINATOR_FENCED"),
    (53'i16, "TRANSACTIONAL_ID_AUTHORIZATION_FAILED"),
    (54'i16, "SECURITY_DISABLED"),
    (55'i16, "OPERATION_NOT_ATTEMPTED"),
    (56'i16, "KAFKA_STORAGE_ERROR"),
    (58'i16, "SASL_AUTHENTICATION_FAILED"),
    (59'i16, "UNKNOWN_PRODUCER_ID"),
    (69'i16, "GROUP_ID_NOT_FOUND"),
    (70'i16, "FETCH_SESSION_ID_NOT_FOUND"),
    (71'i16, "INVALID_FETCH_SESSION_EPOCH"),
    (72'i16, "LISTENER_NOT_FOUND"),
    (73'i16, "TOPIC_DELETION_DISABLED"),
    (74'i16, "FENCED_LEADER_EPOCH"),
    (75'i16, "UNKNOWN_LEADER_EPOCH"),
    (76'i16, "UNSUPPORTED_COMPRESSION_TYPE"),
    (77'i16, "STALE_BROKER_EPOCH"),
    (78'i16, "OFFSET_NOT_AVAILABLE"),
    (79'i16, "MEMBER_ID_REQUIRED"),
    (80'i16, "PREFERRED_LEADER_NOT_AVAILABLE"),
    (81'i16, "GROUP_MAX_SIZE_REACHED"),
    (82'i16, "FENCED_INSTANCE_ID"),
    (83'i16, "ELIGIBLE_LEADERS_NOT_AVAILABLE"),
    (84'i16, "ELECTION_NOT_NEEDED"),
    (87'i16, "INVALID_RECORD"),
    (88'i16, "UNSTABLE_OFFSET_COMMIT"),
    (89'i16, "THROTTLING_QUOTA_EXCEEDED"),
    (90'i16, "PRODUCER_FENCED"),
    (91'i16, "RESOURCE_NOT_FOUND"),
    (92'i16, "DUPLICATE_RESOURCE"),
    (93'i16, "UNACCEPTABLE_CREDENTIAL"),
    (94'i16, "INCONSISTENT_VOTER_SET"),
    (95'i16, "INVALID_UPDATE_VERSION"),
    (96'i16, "FEATURE_UPDATE_FAILED"),
    (100'i16, "UNKNOWN_TOPIC_ID"),
    (105'i16, "TRANSACTIONAL_ID_NOT_FOUND"),
    (106'i16, "FETCH_SESSION_TOPIC_ID_ERROR"),
    (108'i16, "NEW_LEADER_ELECTED"),
    (109'i16, "OFFSET_MOVED_TO_TIERED_STORAGE"),
    (110'i16, "FENCED_MEMBER_EPOCH"),
    (113'i16, "STALE_MEMBER_EPOCH"),
    (120'i16, "TRANSACTION_ABORTABLE"),
  ]

  kafkaErrorNameLookup: array[121, string] = block:
    var a: array[121, string]
    for (code, name) in kafkaErrorNamePairs:
      a[code] = name
    a

let unclassifiedCodes* = {
  keOffsetOutOfRange, keEligibleLeadersNotAvailable, keElectionNotNeeded,
  keUnknownServerError,
}
  ## Every `ke*` constant that `errorClass` deliberately leaves `ecUnknown`, and
  ## the completeness anchor for the classification tables: a `ke*` must appear
  ## either in an `errorClass` branch or here. One in neither reads as `ecUnknown`
  ## and `isClassified` reports false, so callers treat it as non-retriable by
  ## default — a silent behaviour change. The classification tests pin this.
  ##
  ## Why each entry is not a retry/abort decision:
  ## - keOffsetOutOfRange: the fetcher's offset-reset policy owns it
  ## - keEligibleLeadersNotAvailable / keElectionNotNeeded: electLeaders API only
  ## - keUnknownServerError: the broker's catch-all, surfaced to the caller
  ##
  ## `keNone` is absent: `errorClass` short-circuits it to `ecNone`.

proc isRunning*(s: LifecycleState): bool =
  ## True while the component accepts work (before close starts).
  s == lsRunning

proc isClosingOrClosed*(s: LifecycleState): bool =
  ## True once close has started (`lsClosing`) or finished (`lsClosed`).
  s in {lsClosing, lsClosed}

## Code equality by wire value.
proc `==`*(a, b: KafkaErrorCode): bool {.borrow.}

proc toInt16*(c: KafkaErrorCode): int16 =
  ## Raw wire value of a code, for logging and untyped interop layers.
  int16(c)

proc errorClass*(code: KafkaErrorCode): ErrorClass {.raises: [].} =
  ## Single source of truth for Kafka error retry semantics, mirroring the Java
  ## client. Branches name `ke*` constants rather than numbers: `ecFatal` mixes
  ## instance-level and record-level codes, and `isInstanceFatal` draws that line
  ## by subtraction, so a bare number hides which side a code landed on.
  if code == keNone:
    return ecNone
  case code
  # Retriable: transient cluster state, retry after backoff
  of keCorruptMessage, # (corruption may be in transit only)
      keUnknownTopicOrPartition, # (metadata propagation; refresh and retry)
      keLeaderNotAvailable,
      keNotLeaderOrFollower,
      keRequestTimedOut,
      keBrokerNotAvailable,
      keReplicaNotAvailable,
      keStaleControllerEpoch,
      keNetworkException,
      keCoordinatorLoadInProgress,
      keGroupCoordinatorNotAvailable,
      keNotCoordinator,
      keNotEnoughReplicas,
      keNotEnoughReplicasAfterAppend,
      keNotController,
      keInvalidProducerIdMapping, # (retriable for non-txn idempotent)
      keConcurrentTransactions, # (broker txn-state still quiescing after EndTxn)
      keKafkaStorageError,
      keFetchSessionIdNotFound,
      keInvalidFetchSessionEpoch,
      keListenerNotFound, # (listener may still be propagating cluster-wide)
      keFencedLeaderEpoch,
      keUnknownLeaderEpoch,
      keStaleBrokerEpoch, # (broker registration retry)
      keOffsetNotAvailable, # (KIP-207; high watermark lags after an election)
      kePreferredLeaderNotAvailable, # (InvalidMetadataException; refresh + retry)
      keUnstableOffsetCommit, # (OffsetFetch during an in-flight txn; retry)
      keThrottlingQuotaExceeded, # (KIP-599; retry after backoff)
      keUnknownTopicId, # (KIP-516 metadata staleness)
      keTransactionalIdNotFound,
      keFetchSessionTopicIdError, # (KIP-516; drop session and refetch)
      keNewLeaderElected, # (AlterPartition only; refresh metadata and retry)
      keStaleMemberEpoch: # (KIP-848; retry with fresh epoch)
    return ecRetriable

  # Rebalance required: group membership is invalid; rejoin (a plain retry fails).
  of keIllegalGeneration,
      keUnknownMemberId,
      keRebalanceInProgress,
      keMemberIdRequired, # (the broker assigned a memberId on re-join; use it)
      keFencedMemberEpoch: # (KIP-848 group protocol)
    return ecRebalanceRequired

  # Abortable: abortTransaction(), then a new transaction can begin
  of keInvalidProducerEpoch,
      keInvalidTxnState,
      keOperationNotAttempted, # (a sibling partition was rejected; abort)
      keTransactionAbortable:
    return ecAbortable

  # Idempotent producer specific paths
  of keDuplicateSequenceNumber:
    return ecDuplicate
  of keOutOfOrderSequenceNumber, keUnknownProducerId:
    # (abortable inside a txn; see errorClassForContext)
    return ecNeedsProducerIdReset

  # Non-retriable (ecFatal): permanent data/config errors and authn/version
  # failures. The class mixes instance-level with record-level codes; the caller
  # decides — see `isInstanceFatal` below.
  of keInvalidMsgSize,
      keMessageTooLarge,
      keOffsetMetadataTooLarge,
      keInvalidTopicException,
      keRecordListTooLarge,
      keInvalidRequiredAcks,
      keInconsistentGroupProtocol,
      keInvalidGroupId,
      keInvalidSessionTimeout,
      keTopicAuthorizationFailed,
      keGroupAuthorizationFailed,
      keClusterAuthorizationFailed,
      keInvalidTimestamp,
      keUnsupportedSaslMechanism,
      keIllegalSaslState,
      keUnsupportedVersion,
      keTopicAlreadyExists,
      keInvalidRequest,
      keUnsupportedForMessageFormat,
      keInvalidTransactionTimeout,
      keTransactionCoordinatorFenced, # (Java: fatal)
      keTransactionalIdAuthorizationFailed,
      keSecurityDisabled,
      keSaslAuthenticationFailed,
      keTopicDeletionDisabled,
      keUnsupportedCompressionType, # (KIP-110; a config error, never clears)
      keGroupMaxSizeReached,
      keFencedInstanceId,
      kePolicyViolation, # (broker rejected the request per configured policy)
      keGroupIdNotFound,
      keInvalidRecord, # (abortable in txn context; see errorClassForContext)
      keProducerFenced,
      keResourceNotFound,
      keDuplicateResource,
      keUnacceptableCredential,
      keInconsistentVoterSet,
      keInvalidUpdateVersion,
      keFeatureUpdateFailed,
      keOffsetMovedToTieredStorage:
    # (KIP-405; fatal for the offending fetch, not for the instance — see
    # isInstanceFatal's exclusion set.)
    return ecFatal

  # keOffsetOutOfRange is unclassified on purpose: the fetcher routes it through
  # the offset-reset policy.
  else:
    return ecUnknown

proc errorClassForContext*(
    code: KafkaErrorCode, isTransactional: bool
): ErrorClass {.raises: [].} =
  ## Context-aware variant of `errorClass`: identical except for the codes the
  ## Java client treats differently inside a transaction.
  ##
  ## - INVALID_PRODUCER_ID_MAPPING (49): retriable for non-transactional
  ##   idempotent producers; abortable in a txn.
  ## - INVALID_PRODUCER_EPOCH (47): abortable in a txn; for an idempotent-only
  ##   producer there is no txn to abort, so reset the producer id instead.
  ## - INVALID_RECORD (87): abortable in a txn, fatal context-free.
  ## - OUT_OF_ORDER_SEQUENCE_NUMBER (45) / UNKNOWN_PRODUCER_ID (59): producer-id
  ##   reset for non-transactional idempotent producers; abortable in a txn —
  ##   the Java TransactionManager never resets the PID mid-transaction.
  ##
  ## Everything else passes through to `errorClass`.
  if isTransactional and code == keInvalidProducerIdMapping:
    return ecAbortable
  if not isTransactional and code == keInvalidProducerEpoch:
    return ecNeedsProducerIdReset
  if isTransactional and code == keInvalidRecord:
    return ecAbortable
  if isTransactional and
      (code == keOutOfOrderSequenceNumber or code == keUnknownProducerId):
    return ecAbortable
  errorClass(code)

proc isRetriable*(code: KafkaErrorCode): bool {.raises: [].} =
  ## True only for `ecRetriable`.
  code.errorClass == ecRetriable

proc isRebalanceRequired*(code: KafkaErrorCode): bool {.raises: [].} =
  ## True for codes signalling that the group membership must be rejoined.
  code.errorClass == ecRebalanceRequired

proc isAbortable*(code: KafkaErrorCode): bool {.raises: [].} =
  ## True for `ecAbortable`: the transaction must abort, the producer survives.
  code.errorClass == ecAbortable

proc isFatal*(code: KafkaErrorCode): bool {.raises: [].} =
  ## True for `ecFatal` codes: never retry (instance lifetime is a separate
  ## question — see `isInstanceFatal`).
  code.errorClass == ecFatal

proc isDuplicateSequence*(code: KafkaErrorCode): bool {.raises: [].} =
  ## True for `ecDuplicate`: the broker already has the record.
  code.errorClass == ecDuplicate

proc errorClassToCstring*(ec: ErrorClass): cstring {.raises: [].} =
  ## Stable metric-tag label, emitted alongside the `error_code` tag.
  case ec
  of ecNone:
    cstring"none"
  of ecRetriable:
    cstring"retriable"
  of ecRebalanceRequired:
    cstring"rebalance_required"
  of ecAbortable:
    cstring"abortable"
  of ecFatal:
    cstring"fatal"
  of ecDuplicate:
    cstring"duplicate"
  of ecNeedsProducerIdReset:
    cstring"needs_producer_id_reset"
  of ecUnknown:
    cstring"unknown"

proc kafkaErrorTag*(e: ref KafkaError): string {.raises: [].} =
  ## Stable `error_code` metric tag for a failure with no broker error code,
  ## classified by exception type (the only signal a client-side failure has).
  ## Spelled SCREAMING_SNAKE like `kafkaErrorName` so one `GROUP BY error_code`
  ## covers broker-reported and client-side failures:
  ##
  ##   `CONNECTION_ERROR` – KafkaConnectionError
  ##   `TIMEOUT`          – KafkaTimeoutError
  ##   `PROTOCOL_ERROR`   – KafkaProtocolError
  ##   `KAFKA_ERROR`      – any other KafkaError without a broker code
  if e of KafkaConnectionError:
    "CONNECTION_ERROR"
  elif e of KafkaTimeoutError:
    "TIMEOUT"
  elif e of KafkaProtocolError:
    "PROTOCOL_ERROR"
  else:
    "KAFKA_ERROR"

proc requiresProducerIdReset*(code: KafkaErrorCode): bool {.raises: [].} =
  code.errorClass == ecNeedsProducerIdReset

proc isPermanent*(code: KafkaErrorCode): bool {.raises: [].} =
  ## True iff the code is `ecFatal` — "never retry". That covers record-level
  ## codes (MESSAGE_TOO_LARGE, INVALID_RECORD, ...) as well as the instance-level
  ## fatal set, so it is not a statement that the instance is unusable: branch on
  ## `isInstanceFatal` for that. Backwards-compat alias for `errorClass == ecFatal`.
  ## keOffsetOutOfRange returns false only because `errorClass` leaves it
  ## unclassified; the offset-reset policy recovers it.
  code.errorClass == ecFatal

proc isInstanceFatal*(code: KafkaErrorCode): bool {.raises: [].} =
  ## True when the code leaves the whole producer/consumer instance unusable, so
  ## the only recovery is closing it. A strict subset of `isPermanent`: the fatal
  ## class also carries record-level codes (MESSAGE_TOO_LARGE, INVALID_RECORD) that
  ## fail only the offending request. Callers deciding "close the instance" branch
  ## on this; callers deciding "fail this record for good" (e.g. the DLQ) use
  ## `isPermanent`.
  ##
  ## The list below is the record-level half of `ecFatal`: a new `ecFatal` code is
  ## instance-fatal until it is listed here. The consumer reads this predicate
  ## before its restart path, so a per-partition condition missing from this set
  ## takes the whole consumer down — `keOffsetMovedToTieredStorage` is the worked
  ## example.
  code.isFatal and
    code notin {
      keInvalidMsgSize, keMessageTooLarge, keOffsetMetadataTooLarge,
      keInvalidTopicException, keRecordListTooLarge, keInvalidRequiredAcks,
      keInvalidTimestamp, keTopicAlreadyExists, keInvalidRequest,
      keUnsupportedForMessageFormat, kePolicyViolation, keInvalidTransactionTimeout,
      keTopicDeletionDisabled, keUnsupportedCompressionType, keInvalidRecord,
      keOffsetMovedToTieredStorage,
    }

proc isClassified*(code: KafkaErrorCode): bool {.raises: [].} =
  ## False for unknown codes; treat those as non-retriable failures.
  code.errorClass != ecUnknown

proc newKafkaApiError*(code: KafkaErrorCode, msg: string = ""): ref KafkaApiError =
  let e = newException(KafkaApiError, msg)
  e.errorCode = code
  result = e

proc newKafkaCommitError*(
    code: KafkaErrorCode, msg: string, partitionErrors: seq[PartitionCommitError]
): ref KafkaCommitError =
  let e = newException(KafkaCommitError, msg)
  e.errorCode = code
  e.partitionErrors = partitionErrors
  result = e

proc newKafkaCommitSkippedError*(msg: string): ref KafkaCommitSkippedError =
  newException(KafkaCommitSkippedError, msg)

proc newKafkaOffsetFetchError*(
    code: KafkaErrorCode, msg: string, partitionErrors: seq[PartitionCommitError]
): ref KafkaOffsetFetchError =
  let e = newException(KafkaOffsetFetchError, msg)
  e.errorCode = code
  e.partitionErrors = partitionErrors
  result = e

proc newKafkaRebalanceCallbackError*(
    listener: string, cause: ref CatchableError
): ref KafkaRebalanceCallbackError =
  newException(
    KafkaRebalanceCallbackError, listener & " listener raised: " & cause.msg, cause
  )

proc kafkaErrorName*(code: KafkaErrorCode): string =
  ## Protocol name of a code (`OFFSET_OUT_OF_RANGE`, ...). Never raises on
  ## unknown input: `-1` renders `UNKNOWN_SERVER_ERROR`, anything else outside
  ## the registry renders `UNKNOWN_ERROR(n)`.
  let c = code.toInt16
  if c == -1:
    return "UNKNOWN_SERVER_ERROR"
  if c >= 0 and c <= 120 and kafkaErrorNameLookup[c].len > 0:
    return kafkaErrorNameLookup[c]
  "UNKNOWN_ERROR(" & $c & ")"

proc `$`*(c: KafkaErrorCode): string {.inline.} =
  ## Render a code by its Kafka name, falling back to "UNKNOWN_ERROR(n)". Needed
  ## because `KafkaErrorCode` is a `distinct int16`; without it, interpolating a
  ## code would not compile.
  kafkaErrorName(c)
