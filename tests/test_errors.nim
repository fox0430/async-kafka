import std/[macros, strutils, unittest]

import async_kafka/errors

## The classification tables are the one place in the package where a bare
## error-code number is meaningful, and the one place where a reader cannot
## tell an instance-level code from a request-level one without a lookup table.
## OFFSET_MOVED_TO_TIERED_STORAGE (109) sat in `ecFatal` without appearing in
## `isInstanceFatal`'s exclusion list for a whole commit cycle, and one
## tiered-storage partition took the whole consumer instance down with it. The
## `109` is what let that ship: nobody reads a number as "this looks like an
## instance problem", and the only test covering the split enumerated the
## carve-outs by hand, so adding a code to the fatal set needed no test change.
##
## The four invariants that would have caught it are mechanical, so they run at
## compile time — a regression fails the build instead of one test case:
##  1. the tables carry no bare error-code numbers
##  2. every `ke*` name in a table is a declared constant
##  3. every declared `ke*` constant is classified in a table, or is named in
##     `deliberatelyUnclassified` below
##  4. the instance-fatal exclusion only subtracts codes `errorClass`
##     classifies as fatal
const
  classificationProcs = ["errorClass", "errorClassForContext", "isInstanceFatal"]
    ## The three classification sites: errorClass's six branches, the
    ## transactional overrides, and the instance-fatal exclusion set.

  deliberatelyUnclassified = [
    "keOffsetOutOfRange", "keEligibleLeadersNotAvailable", "keElectionNotNeeded",
    "keUnknownServerError",
  ]
    ## The `ke*` constants `errorClass` must leave `ecUnknown`, mirroring
    ## `unclassifiedCodes` in errors.nim. Ke in sync by the "unclassified set"
    ## test below, which fails if either side gains an entry alone.
    ## - keOffsetOutOfRange: the fetcher resolves it through the offset-reset
    ##   policy — a position decision, not a retry decision
    ## - keEligibleLeadersNotAvailable / keElectionNotNeeded: electLeaders
    ##   Admin API only; never produced by producer/consumer RPCs
    ## - keUnknownServerError: the broker's internal catch-all, surfaced to the
    ##   caller rather than folded into a retry policy

proc collect(node: NimNode, kind: NimNodeKind): seq[NimNode] =
  if node.kind == kind:
    result.add(node)
  for child in node:
    result.add(collect(child, kind))

proc declaredName(node: NimNode): string =
  ## The identifier of a `nnkProcDef` / `nnkConstDef`, as a string. Both hold it
  ## as their first child rather than in a plain field, and `macros.name` only
  ## covers the callable defs — so read the child directly. An exported `ke*`
  ## constant is parsed as `nnkPostfix("*", name)`, so the name is the second
  ## child there and the first child of a proc def.
  let first = node[0]
  if first.kind == nnkPostfix:
    let exported = first[1]
    if exported.kind == nnkIdent:
      exported.strVal
    else:
      first[0].strVal
  elif first.kind == nnkIdent:
    first.strVal
  else:
    "" # a backtick-quoted operator (e.g. `==`) has no identifier to match on

proc bodyOf(stmts: NimNode, wanted: string): NimNode =
  for child in stmts:
    if child.kind == nnkProcDef and declaredName(child) == wanted:
      return child.body
  doAssert false, "proc " & wanted & " not found in errors.nim"

proc keConsts(stmts: NimNode): seq[NimNode] =
  ## Every `ke*` constant definition in the file.
  for section in collect(stmts, nnkConstSection):
    for definition in section:
      if definition.kind == nnkConstDef and declaredName(definition).startsWith("ke"):
        result.add(definition)

proc namesIn(node: NimNode): seq[string] =
  for ident in collect(node, nnkIdent):
    result.add(ident.strVal)

static:
  block classificationInvariants:
    let stmts = parseStmt(staticRead("../async_kafka/errors.nim"))
    let declared = keConsts(stmts)
    doAssert declared.len > 80, "expected the full ke* constant block"

    var classifiedNames: seq[string]
    var fatalNames: seq[string]
    for procName in classificationProcs:
      let body = bodyOf(stmts, procName)
      # (1) no bare numbers: a number hides which side of the split it is on.
      for literal in collect(body, nnkIntLit):
        doAssert false,
          procName & " contains the bare number " & $literal.intVal &
            "; name the ke* constant instead"
      # (2) every ke* name is a declared constant (no typos, no locals).
      for name in namesIn(body):
        if not name.startsWith("ke"):
          continue
        classifiedNames.add(name)
        var found = false
        for definition in declared:
          if declaredName(definition) == name:
            found = true
        doAssert found,
          procName & " names " & name & ", which is not a declared constant"

      # (4) locate errorClass's fatal branch: the exclusion set must only
      # subtract codes that branch classifies, or a carve-out is dead weight
      # and a fatal code missing from it stops the instance.
      if procName == "errorClass":
        var fatalBranch: NimNode
        # The `of` branches live inside the `case`, so walk the whole body
        # rather than only its top level.
        for ofBranch in collect(body, nnkOfBranch):
          # An `of` branch is [label, label, ..., nnkStmtList]: the labels are
          # the code values, the statement list is the body.
          let stmts = ofBranch[^1]
          if stmts.kind != nnkStmtList:
            continue
          for statement in stmts:
            if statement.kind == nnkReturnStmt and statement[0].kind == nnkIdent and
                statement[0].strVal == "ecFatal":
              doAssert fatalBranch == nil, "errorClass has two fatal branches"
              fatalBranch = ofBranch
        doAssert fatalBranch != nil, "no ecFatal branch in errorClass"
        for name in namesIn(fatalBranch):
          if name.startsWith("ke"):
            fatalNames.add(name)

    # (3) every constant is classified, or explicitly listed as unclassified.
    for definition in declared:
      let name = declaredName(definition)
      var classified = false
      for classifiedName in classifiedNames:
        if classifiedName == name:
          classified = true
      let excused = name in deliberatelyUnclassified
      doAssert classified != excused,
        name & " is " & (
          if classified: "classified AND listed unclassified" else: "classified nowhere"
        )

    # (4) every carve-out is a member of the fatal branch.
    var carveOuts = 0
    for name in namesIn(bodyOf(stmts, "isInstanceFatal")):
      if not name.startsWith("ke"):
        continue
      carveOuts.inc
      var isFatalMember = false
      for fatalName in fatalNames:
        if fatalName == name:
          isFatalMember = true
      doAssert isFatalMember,
        "isInstanceFatal carves out " & name &
          ", which errorClass does not classify fatal"
    doAssert carveOuts == 16, "instance-fatal carve-out count changed: " & $carveOuts

suite "Error classification table invariants":
  test "the runtime unclassified set matches deliberatelyUnclassified":
    # errors.nim's `unclassifiedCodes` and the `deliberatelyUnclassified` list
    # above are one decision written twice: once where the compile-time
    # invariant can read it, once where the set actually lives. An entry on
    # only one side means a constant is classified nowhere while the tables
    # look complete — the silence this suite exists to remove.
    check unclassifiedCodes == {
      keOffsetOutOfRange, keEligibleLeadersNotAvailable, keElectionNotNeeded,
      keUnknownServerError,
    }
    check unclassifiedCodes.len == deliberatelyUnclassified.len
    for code in unclassifiedCodes:
      check not isClassified(code)

  test "every other constant is classified through the public predicates":
    # The 77 classified constants enumerated here, not the complement of
    # `unclassifiedCodes`, asserted through the API callers use rather than
    # through the AST, so the two agree.
    for code in [
      keNone, keCorruptMessage, keUnknownTopicOrPartition, keLeaderNotAvailable,
      keNotLeaderOrFollower, keRequestTimedOut, keMessageTooLarge, keNetworkException,
      keCoordinatorLoadInProgress, keGroupCoordinatorNotAvailable, keNotCoordinator,
      keInvalidTopicException, keRecordListTooLarge, keNotEnoughReplicas,
      keNotEnoughReplicasAfterAppend, keInvalidRequiredAcks, keIllegalGeneration,
      keInconsistentGroupProtocol, keUnknownMemberId, keInvalidSessionTimeout,
      keRebalanceInProgress, keTopicAuthorizationFailed, keGroupAuthorizationFailed,
      keClusterAuthorizationFailed, keInvalidTimestamp, keUnsupportedSaslMechanism,
      keIllegalSaslState, keUnsupportedVersion, keTopicAlreadyExists, keInvalidRequest,
      keUnsupportedForMessageFormat, keOutOfOrderSequenceNumber,
      keDuplicateSequenceNumber, keInvalidProducerEpoch, keInvalidTxnState,
      keInvalidProducerIdMapping, keConcurrentTransactions,
      keTransactionCoordinatorFenced, keTransactionalIdAuthorizationFailed,
      keSecurityDisabled, keOperationNotAttempted, keKafkaStorageError,
      keSaslAuthenticationFailed, keUnknownProducerId, keFetchSessionIdNotFound,
      keInvalidFetchSessionEpoch, keListenerNotFound, keGroupMaxSizeReached,
      keFencedInstanceId, keMemberIdRequired, keFencedLeaderEpoch, keUnknownLeaderEpoch,
      keTopicDeletionDisabled, keUnsupportedCompressionType, keStaleBrokerEpoch,
      keOffsetNotAvailable, kePreferredLeaderNotAvailable, keUnstableOffsetCommit,
      keThrottlingQuotaExceeded, keProducerFenced, kePolicyViolation, keGroupIdNotFound,
      keInvalidRecord, keResourceNotFound, keDuplicateResource,
      keUnacceptableCredential, keInconsistentVoterSet, keInvalidUpdateVersion,
      keFeatureUpdateFailed, keUnknownTopicId, keTransactionalIdNotFound,
      keFetchSessionTopicIdError, keNewLeaderElected, keOffsetMovedToTieredStorage,
      keFencedMemberEpoch, keStaleMemberEpoch, keTransactionAbortable,
    ]:
      check isClassified(code)
      check code notin unclassifiedCodes

suite "Error utilities":
  test "a tiered-storage migration is not instance-fatal":
    # Regression: 109 was classified `ecFatal` but never appeared in the
    # isInstanceFatal exclusion set, so ONE partition with data migrated to
    # tiered storage made `isInstanceFatal` true. The fetcher-restart path reads
    # that predicate BEFORE the cooldown and raises straight out of poll()
    # ("Fetcher stopped on permanent error; not restarting") — the whole
    # consumer instance dies over a per-partition condition. Same commit
    # (483e605) added the code and the exclusion list; only the code stuck.
    # The client has no tiered-storage fetch path, so retrying cannot clear it
    # either: the code stays fatal for the offending REQUEST, which is exactly
    # the record/request-level half of ecFatal. Claiming the instance is not
    # implied by that.
    check errorClass(keOffsetMovedToTieredStorage) == ecFatal
    check isPermanent(keOffsetMovedToTieredStorage) == true
    check isInstanceFatal(keOffsetMovedToTieredStorage) == false

  test "kafkaErrorTag spells client-side failures like kafkaErrorName":
    ## `error_code` is the dimension dashboards GROUP BY. A client-side failure
    ## used to carry a lowercase label (`connection`, `timeout`, `protocol`)
    ## while a broker failure carried `kafkaErrorName`'s SCREAMING_SNAKE, so one
    ## GROUP BY split the same failure kind across two spellings. Every kind is
    ## SCREAMING_SNAKE now, e.g. `CONNECTION_ERROR` and `TIMEOUT`.
    check kafkaErrorTag(newException(KafkaConnectionError, "dropped")) ==
      "CONNECTION_ERROR"
    check kafkaErrorTag(newException(KafkaTimeoutError, "slow")) == "TIMEOUT"
    check kafkaErrorTag(newException(KafkaProtocolError, "bad frame")) ==
      "PROTOCOL_ERROR"
    check kafkaErrorTag(newException(KafkaError, "other")) == "KAFKA_ERROR"
    # The protocol names the other half of the dimension uses are SCREAMING_SNAKE
    # too; if that ever changes, one of the two conventions has to move.
    let brokerName = kafkaErrorName(keUnknownTopicOrPartition)
    check brokerName == brokerName.toUpperAscii()

  test "isRetriable":
    check isRetriable(keLeaderNotAvailable) == true
    check isRetriable(keNotLeaderOrFollower) == true
    check isRetriable(keRequestTimedOut) == true
    check isRetriable(keGroupCoordinatorNotAvailable) == true
    check isRetriable(keNotCoordinator) == true
    check isRetriable(keNone) == false
    check isRetriable(keMemberIdRequired) == false
    # Rebalance-required codes are NOT retriable — a plain retry will keep
    # failing until the consumer rejoins the group. See isRebalanceRequired.
    check isRetriable(keRebalanceInProgress) == false
    check isRetriable(keUnknownMemberId) == false
    check isRetriable(keIllegalGeneration) == false
    # OPERATION_NOT_ATTEMPTED moved to ecAbortable.
    check isRetriable(keOperationNotAttempted) == false

  test "CORRUPT_MESSAGE and LISTENER_NOT_FOUND are retriable (Java parity)":
    # CORRUPT_MESSAGE (2): CorruptRecordException extends RetriableException —
    # in-transit corruption can clear on a re-fetch / re-send, so it must not be
    # fatal (which would close the producer/consumer).
    check errorClass(keCorruptMessage) == ecRetriable
    check isRetriable(keCorruptMessage) == true
    # LISTENER_NOT_FOUND (72): ListenerNotFoundException extends
    # InvalidMetadataException — retriable while the listener propagates.
    check errorClass(KafkaErrorCode(72)) == ecRetriable
    check isRetriable(KafkaErrorCode(72)) == true

  test "PREFERRED_LEADER_NOT_AVAILABLE and FETCH_SESSION_TOPIC_ID_ERROR are retriable":
    # 80 PREFERRED_LEADER_NOT_AVAILABLE: InvalidMetadataException subclass —
    # refresh metadata and retry. Previously fell through to ecUnknown → no
    # retry, dropping requests on transient preferred-leader elections.
    check errorClass(kePreferredLeaderNotAvailable) == ecRetriable
    check isRetriable(kePreferredLeaderNotAvailable) == true
    check kafkaErrorName(kePreferredLeaderNotAvailable) ==
      "PREFERRED_LEADER_NOT_AVAILABLE"
    # 106 FETCH_SESSION_TOPIC_ID_ERROR: KIP-516 RetriableException — drop the
    # session and refetch from sessionId=0 (mirrors 70 / 71 handling).
    check errorClass(keFetchSessionTopicIdError) == ecRetriable
    check isRetriable(keFetchSessionTopicIdError) == true
    check kafkaErrorName(keFetchSessionTopicIdError) == "FETCH_SESSION_TOPIC_ID_ERROR"

  test "isRebalanceRequired":
    check isRebalanceRequired(keIllegalGeneration) == true
    check isRebalanceRequired(keUnknownMemberId) == true
    check isRebalanceRequired(keRebalanceInProgress) == true
    check isRebalanceRequired(keLeaderNotAvailable) == false
    check isRebalanceRequired(keNone) == false
    # MEMBER_ID_REQUIRED (79) is the broker telling us to re-issue
    # JoinGroup with the assigned memberId — Java client routes it through
    # the same rejoin path as ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID.
    check isRebalanceRequired(keMemberIdRequired) == true

  test "errorClassForContext flips INVALID_PRODUCER_ID_MAPPING to abortable in txn":
    # Java client treats keInvalidProducerIdMapping as retriable for the
    # non-transactional idempotent path, but abortable when a transaction
    # is in flight. errorClass (context-free) keeps the producer default.
    check errorClass(keInvalidProducerIdMapping) == ecRetriable
    check errorClassForContext(keInvalidProducerIdMapping, false) == ecRetriable
    check errorClassForContext(keInvalidProducerIdMapping, true) == ecAbortable
    # Other codes pass through unchanged regardless of context.
    check errorClassForContext(keLeaderNotAvailable, true) == ecRetriable
    check errorClassForContext(keMemberIdRequired, true) == ecRebalanceRequired
    check errorClassForContext(keProducerFenced, true) == ecFatal

  test "errorClassForContext maps INVALID_PRODUCER_EPOCH to PID reset outside a txn":
    # In a transaction a fenced epoch must abort (the context-free default).
    # For an idempotent-only producer there is no txn to abort, so the epoch
    # fence is recovered by resetting the producer id and retrying — matching
    # the Java client's idempotent path.
    check errorClass(keInvalidProducerEpoch) == ecAbortable
    check errorClassForContext(keInvalidProducerEpoch, true) == ecAbortable
    check errorClassForContext(keInvalidProducerEpoch, false) == ecNeedsProducerIdReset
    # The context-free predicates (used globally, e.g. the DLQ/consumer give-up
    # paths) are deliberately unchanged.
    check isAbortable(keInvalidProducerEpoch)
    check not requiresProducerIdReset(keInvalidProducerEpoch)

  test "errorClassForContext maps 45/59 to abortable inside a txn":
    # Java TransactionManager parity: a transactional producer whose batch
    # fails with OUT_OF_ORDER_SEQUENCE_NUMBER / UNKNOWN_PRODUCER_ID
    # transitions to an ABORTABLE error and recovers via an epoch bump after
    # abort — it never resets the producer id mid-transaction. The KIP-360
    # producer-id reset (ecNeedsProducerIdReset) applies only to
    # non-transactional idempotent producers.
    check errorClass(keOutOfOrderSequenceNumber) == ecNeedsProducerIdReset
    check errorClass(keUnknownProducerId) == ecNeedsProducerIdReset
    check errorClassForContext(keOutOfOrderSequenceNumber, false) ==
      ecNeedsProducerIdReset
    check errorClassForContext(keOutOfOrderSequenceNumber, true) == ecAbortable
    check errorClassForContext(keUnknownProducerId, false) == ecNeedsProducerIdReset
    check errorClassForContext(keUnknownProducerId, true) == ecAbortable
    # The context-free predicates stay reset-oriented.
    check requiresProducerIdReset(keOutOfOrderSequenceNumber)
    check requiresProducerIdReset(keUnknownProducerId)
    check not isAbortable(keOutOfOrderSequenceNumber)
    check not isAbortable(keUnknownProducerId)

  test "kafkaErrorName known codes":
    check kafkaErrorName(keNone) == "NONE"
    check kafkaErrorName(keOffsetOutOfRange) == "OFFSET_OUT_OF_RANGE"
    check kafkaErrorName(keCorruptMessage) == "CORRUPT_MESSAGE"
    check kafkaErrorName(keUnknownTopicOrPartition) == "UNKNOWN_TOPIC_OR_PARTITION"
    check kafkaErrorName(keRebalanceInProgress) == "REBALANCE_IN_PROGRESS"
    check kafkaErrorName(keMemberIdRequired) == "MEMBER_ID_REQUIRED"

  test "kafkaErrorName unknown code":
    check kafkaErrorName(KafkaErrorCode(999)) == "UNKNOWN_ERROR(999)"

  test "$ aliases kafkaErrorName":
    check $keNone == "NONE"
    check $keOffsetOutOfRange == "OFFSET_OUT_OF_RANGE"
    check $KafkaErrorCode(999) == "UNKNOWN_ERROR(999)"
    # Usable in string interpolation without an explicit kafkaErrorName call.
    check "code=" & $keRebalanceInProgress == "code=REBALANCE_IN_PROGRESS"

  test "isDuplicateSequence":
    check isDuplicateSequence(keDuplicateSequenceNumber) == true
    check isDuplicateSequence(keNone) == false
    check isDuplicateSequence(keOutOfOrderSequenceNumber) == false

  test "requiresProducerIdReset":
    check requiresProducerIdReset(keOutOfOrderSequenceNumber) == true
    check requiresProducerIdReset(keUnknownProducerId) == true
    check requiresProducerIdReset(keDuplicateSequenceNumber) == false
    check requiresProducerIdReset(keNone) == false

  test "keInvalidProducerEpoch is abortable, not permanent":
    check not isPermanent(keInvalidProducerEpoch)
    check isAbortable(keInvalidProducerEpoch)

  test "kafkaErrorName idempotent codes":
    check kafkaErrorName(keDuplicateSequenceNumber) == "DUPLICATE_SEQUENCE_NUMBER"
    check kafkaErrorName(keInvalidProducerEpoch) == "INVALID_PRODUCER_EPOCH"
    check kafkaErrorName(keOutOfOrderSequenceNumber) == "OUT_OF_ORDER_SEQUENCE_NUMBER"
    check kafkaErrorName(keUnknownProducerId) == "UNKNOWN_PRODUCER_ID"

  test "newKafkaApiError":
    let e = newKafkaApiError(keNotLeaderOrFollower, "test msg")
    check e.errorCode == keNotLeaderOrFollower
    check e.msg == "test msg"

  test "isRetriable includes replication errors":
    check isRetriable(keNotEnoughReplicas) == true
    check isRetriable(keNotEnoughReplicasAfterAppend) == true

  test "isPermanent includes message size and topic errors":
    check isPermanent(keMessageTooLarge) == true
    check isPermanent(keInvalidTopicException) == true
    check isPermanent(keGroupMaxSizeReached) == true

  test "new error codes are not misclassified":
    check isRetriable(keMessageTooLarge) == false
    check isRetriable(keGroupMaxSizeReached) == false
    check isPermanent(keNotEnoughReplicas) == false

  test "keOperationNotAttempted is abortable":
    # OPERATION_NOT_ATTEMPTED signals a sibling partition in the same
    # transactional batch was rejected — a plain retry of this partition
    # cannot succeed; the caller must abort the transaction.
    check isRetriable(keOperationNotAttempted) == false
    check isPermanent(keOperationNotAttempted) == false
    check isAbortable(keOperationNotAttempted) == true

  test "isPermanent includes record/topic/timestamp errors":
    check isPermanent(keRecordListTooLarge) == true
    check isPermanent(keUnsupportedForMessageFormat) == true
    check isPermanent(keInvalidTimestamp) == true
    check isPermanent(keTopicAlreadyExists) == true

  test "keOffsetOutOfRange is not permanent (recoverable by offset reset)":
    check isPermanent(keOffsetOutOfRange) == false

  test "kafkaErrorName new codes":
    check kafkaErrorName(keMessageTooLarge) == "MESSAGE_TOO_LARGE"
    check kafkaErrorName(keInvalidTopicException) == "INVALID_TOPIC_EXCEPTION"
    check kafkaErrorName(keRecordListTooLarge) == "RECORD_LIST_TOO_LARGE"
    check kafkaErrorName(keNotEnoughReplicas) == "NOT_ENOUGH_REPLICAS"
    check kafkaErrorName(keNotEnoughReplicasAfterAppend) ==
      "NOT_ENOUGH_REPLICAS_AFTER_APPEND"
    check kafkaErrorName(keUnsupportedForMessageFormat) ==
      "UNSUPPORTED_FOR_MESSAGE_FORMAT"
    check kafkaErrorName(keGroupMaxSizeReached) == "GROUP_MAX_SIZE_REACHED"

  test "isClassified for known codes":
    check isClassified(keNone) == true
    check isClassified(keLeaderNotAvailable) == true # retriable
    check isClassified(keCorruptMessage) == true # retriable
    check isClassified(keDuplicateSequenceNumber) == true # duplicate sequence
    check isClassified(keOutOfOrderSequenceNumber) == true # requires reset

  test "isClassified returns false for unknown codes":
    check isClassified(KafkaErrorCode(999)) == false
    check isClassified(KafkaErrorCode(200)) == false

  test "keOffsetOutOfRange is not classified as retriable or permanent":
    check isRetriable(keOffsetOutOfRange) == false
    check isPermanent(keOffsetOutOfRange) == false
    check isClassified(keOffsetOutOfRange) == false

  test "kafkaErrorName negative error codes":
    check kafkaErrorName(keUnknownServerError) == "UNKNOWN_SERVER_ERROR"
    check kafkaErrorName(KafkaErrorCode(-1)) == "UNKNOWN_SERVER_ERROR"
    check kafkaErrorName(KafkaErrorCode(-99)) == "UNKNOWN_ERROR(-99)"

  test "keUnknownServerError constant":
    check keUnknownServerError.toInt16 == -1'i16

  test "kafkaErrorName newly added codes":
    check kafkaErrorName(keInvalidMsgSize) == "INVALID_MSG_SIZE"
    check kafkaErrorName(keBrokerNotAvailable) == "BROKER_NOT_AVAILABLE"
    check kafkaErrorName(keReplicaNotAvailable) == "REPLICA_NOT_AVAILABLE"
    check kafkaErrorName(keStaleControllerEpoch) == "STALE_CONTROLLER_EPOCH"
    check kafkaErrorName(keOffsetMetadataTooLarge) == "OFFSET_METADATA_TOO_LARGE"
    check kafkaErrorName(keInvalidGroupId) == "INVALID_GROUP_ID"
    check kafkaErrorName(keNotController) == "NOT_CONTROLLER"
    check kafkaErrorName(keInvalidTransactionTimeout) == "INVALID_TRANSACTION_TIMEOUT"

  test "kafkaErrorName covers all defined error codes":
    check kafkaErrorName(keNone) == "NONE"
    check kafkaErrorName(keLeaderNotAvailable) == "LEADER_NOT_AVAILABLE"
    check kafkaErrorName(keNotLeaderOrFollower) == "NOT_LEADER_OR_FOLLOWER"
    check kafkaErrorName(keRequestTimedOut) == "REQUEST_TIMED_OUT"
    check kafkaErrorName(keGroupCoordinatorNotAvailable) ==
      "GROUP_COORDINATOR_NOT_AVAILABLE"
    check kafkaErrorName(keNotCoordinator) == "NOT_COORDINATOR"
    check kafkaErrorName(keInvalidRequiredAcks) == "INVALID_REQUIRED_ACKS"
    check kafkaErrorName(keIllegalGeneration) == "ILLEGAL_GENERATION"
    check kafkaErrorName(keInconsistentGroupProtocol) == "INCONSISTENT_GROUP_PROTOCOL"
    check kafkaErrorName(keUnknownMemberId) == "UNKNOWN_MEMBER_ID"
    check kafkaErrorName(keInvalidSessionTimeout) == "INVALID_SESSION_TIMEOUT"
    check kafkaErrorName(keRebalanceInProgress) == "REBALANCE_IN_PROGRESS"
    check kafkaErrorName(keTopicAuthorizationFailed) == "TOPIC_AUTHORIZATION_FAILED"
    check kafkaErrorName(keGroupAuthorizationFailed) == "GROUP_AUTHORIZATION_FAILED"
    check kafkaErrorName(keClusterAuthorizationFailed) == "CLUSTER_AUTHORIZATION_FAILED"
    check kafkaErrorName(keInvalidTimestamp) == "INVALID_TIMESTAMP"
    check kafkaErrorName(keTopicAlreadyExists) == "TOPIC_ALREADY_EXISTS"
    check kafkaErrorName(keInvalidTxnState) == "INVALID_TXN_STATE"
    check kafkaErrorName(keInvalidProducerIdMapping) == "INVALID_PRODUCER_ID_MAPPING"
    check kafkaErrorName(keTransactionalIdAuthorizationFailed) ==
      "TRANSACTIONAL_ID_AUTHORIZATION_FAILED"
    check kafkaErrorName(keOperationNotAttempted) == "OPERATION_NOT_ATTEMPTED"
    check kafkaErrorName(keFetchSessionIdNotFound) == "FETCH_SESSION_ID_NOT_FOUND"
    check kafkaErrorName(keProducerFenced) == "PRODUCER_FENCED"
    check kafkaErrorName(keConcurrentTransactions) == "CONCURRENT_TRANSACTIONS"
    check kafkaErrorName(keTransactionCoordinatorFenced) ==
      "TRANSACTION_COORDINATOR_FENCED"
    check kafkaErrorName(keInvalidRecord) == "INVALID_RECORD"

  test "wire values match the official protocol error table":
    # Regression: GROUP_MAX_SIZE_REACHED and FENCED_INSTANCE_ID were defined
    # as 73 / 76, which are actually TOPIC_DELETION_DISABLED and
    # UNSUPPORTED_COMPRESSION_TYPE on the wire — so a broker rejecting e.g. a
    # zstd batch sent below the KIP-110 version floor (code 76) was reported
    # as "FENCED_INSTANCE_ID", and the real codes 81 / 82 fell through to
    # ecUnknown. Pin the numeric values so a renumbering cannot regress.
    check keTopicDeletionDisabled.toInt16 == 73'i16
    check keFencedLeaderEpoch.toInt16 == 74'i16
    check keUnknownLeaderEpoch.toInt16 == 75'i16
    check keUnsupportedCompressionType.toInt16 == 76'i16
    check keMemberIdRequired.toInt16 == 79'i16
    check keGroupMaxSizeReached.toInt16 == 81'i16
    check keFencedInstanceId.toInt16 == 82'i16
    check keProducerFenced.toInt16 == 90'i16
    check kafkaErrorName(keTopicDeletionDisabled) == "TOPIC_DELETION_DISABLED"
    check kafkaErrorName(keUnsupportedCompressionType) == "UNSUPPORTED_COMPRESSION_TYPE"
    check kafkaErrorName(keGroupMaxSizeReached) == "GROUP_MAX_SIZE_REACHED"
    check kafkaErrorName(keFencedInstanceId) == "FENCED_INSTANCE_ID"
    # The raw wire numbers resolve to the same names (lookup-table parity).
    check kafkaErrorName(KafkaErrorCode(73)) == "TOPIC_DELETION_DISABLED"
    check kafkaErrorName(KafkaErrorCode(76)) == "UNSUPPORTED_COMPRESSION_TYPE"
    check kafkaErrorName(KafkaErrorCode(81)) == "GROUP_MAX_SIZE_REACHED"
    check kafkaErrorName(KafkaErrorCode(82)) == "FENCED_INSTANCE_ID"
    # All four are non-retriable config/membership errors (Java parity).
    check errorClass(keTopicDeletionDisabled) == ecFatal
    check errorClass(keUnsupportedCompressionType) == ecFatal
    check errorClass(keGroupMaxSizeReached) == ecFatal
    check errorClass(keFencedInstanceId) == ecFatal

  test "txn wire values match the official protocol error table (45/51/52/87)":
    # Regression: OUT_OF_ORDER_SEQUENCE_NUMBER was defined as 51, which is
    # actually CONCURRENT_TRANSACTIONS on the wire — so the real code 45 fell
    # through to ecUnknown (disabling the KIP-360 reset-and-resequence
    # recovery it exists for), while the routinely-transient code 51 was
    # treated as a producer-id reset and hard-failed transactions that Java
    # simply retries. Pin the numeric values so a renumbering cannot regress.
    check keOutOfOrderSequenceNumber.toInt16 == 45'i16
    check keConcurrentTransactions.toInt16 == 51'i16
    check keTransactionCoordinatorFenced.toInt16 == 52'i16
    check keInvalidRecord.toInt16 == 87'i16
    check kafkaErrorName(keOutOfOrderSequenceNumber) == "OUT_OF_ORDER_SEQUENCE_NUMBER"
    check kafkaErrorName(keConcurrentTransactions) == "CONCURRENT_TRANSACTIONS"
    check kafkaErrorName(keTransactionCoordinatorFenced) ==
      "TRANSACTION_COORDINATOR_FENCED"
    check kafkaErrorName(keInvalidRecord) == "INVALID_RECORD"
    # The raw wire numbers resolve to the same names (lookup-table parity).
    check kafkaErrorName(KafkaErrorCode(45)) == "OUT_OF_ORDER_SEQUENCE_NUMBER"
    check kafkaErrorName(KafkaErrorCode(51)) == "CONCURRENT_TRANSACTIONS"
    check kafkaErrorName(KafkaErrorCode(52)) == "TRANSACTION_COORDINATOR_FENCED"
    check kafkaErrorName(KafkaErrorCode(87)) == "INVALID_RECORD"
    # Classification (Java parity): 45 drives the KIP-360 PID reset, 51 is a
    # plain backoff-retry, 52 is fatal, 87 is fatal except inside a
    # transaction where the txn can be aborted and restarted.
    check errorClass(keOutOfOrderSequenceNumber) == ecNeedsProducerIdReset
    check errorClass(keConcurrentTransactions) == ecRetriable
    check errorClass(keTransactionCoordinatorFenced) == ecFatal
    check errorClass(keInvalidRecord) == ecFatal
    check errorClassForContext(keInvalidRecord, isTransactional = true) == ecAbortable
    check errorClassForContext(keInvalidRecord, isTransactional = false) == ecFatal
    check errorClassForContext(keConcurrentTransactions, isTransactional = true) ==
      ecRetriable

suite "ErrorClass classification":
  test "ecNone for keNone":
    check errorClass(keNone) == ecNone

  test "ecRetriable matrix":
    check errorClass(keUnknownTopicOrPartition) == ecRetriable
    check errorClass(keLeaderNotAvailable) == ecRetriable
    check errorClass(keNotLeaderOrFollower) == ecRetriable
    check errorClass(keRequestTimedOut) == ecRetriable
    check errorClass(keBrokerNotAvailable) == ecRetriable
    check errorClass(keReplicaNotAvailable) == ecRetriable
    check errorClass(keStaleControllerEpoch) == ecRetriable
    check errorClass(keNetworkException) == ecRetriable
    check errorClass(keCoordinatorLoadInProgress) == ecRetriable
    check errorClass(keGroupCoordinatorNotAvailable) == ecRetriable
    check errorClass(keNotCoordinator) == ecRetriable
    check errorClass(keNotEnoughReplicas) == ecRetriable
    check errorClass(keNotEnoughReplicasAfterAppend) == ecRetriable
    check errorClass(keNotController) == ecRetriable
    check errorClass(keInvalidProducerIdMapping) == ecRetriable
    check errorClass(keKafkaStorageError) == ecRetriable
    check errorClass(keFetchSessionIdNotFound) == ecRetriable
    check errorClass(keFencedLeaderEpoch) == ecRetriable
    check errorClass(keUnknownLeaderEpoch) == ecRetriable
    check errorClass(keConcurrentTransactions) == ecRetriable

  test "ecRebalanceRequired matrix":
    check errorClass(keIllegalGeneration) == ecRebalanceRequired
    check errorClass(keUnknownMemberId) == ecRebalanceRequired
    check errorClass(keRebalanceInProgress) == ecRebalanceRequired

  test "ecAbortable matrix":
    check errorClass(keInvalidProducerEpoch) == ecAbortable
    check errorClass(keInvalidTxnState) == ecAbortable
    check errorClass(keOperationNotAttempted) == ecAbortable

  test "ecFatal matrix":
    # CORRUPT_MESSAGE is retriable (see the dedicated test above), not fatal.
    check errorClass(keInvalidMsgSize) == ecFatal
    check errorClass(keMessageTooLarge) == ecFatal
    check errorClass(keOffsetMetadataTooLarge) == ecFatal
    check errorClass(keInvalidTopicException) == ecFatal
    check errorClass(keRecordListTooLarge) == ecFatal
    check errorClass(keInvalidRequiredAcks) == ecFatal
    check errorClass(keInconsistentGroupProtocol) == ecFatal
    check errorClass(keInvalidGroupId) == ecFatal
    check errorClass(keInvalidSessionTimeout) == ecFatal
    check errorClass(keTopicAuthorizationFailed) == ecFatal
    check errorClass(keGroupAuthorizationFailed) == ecFatal
    check errorClass(keClusterAuthorizationFailed) == ecFatal
    check errorClass(keUnsupportedSaslMechanism) == ecFatal
    check errorClass(keIllegalSaslState) == ecFatal
    check errorClass(keUnsupportedVersion) == ecFatal
    check errorClass(keTopicAlreadyExists) == ecFatal
    check errorClass(keInvalidRequest) == ecFatal
    check errorClass(keUnsupportedForMessageFormat) == ecFatal
    check errorClass(keInvalidTransactionTimeout) == ecFatal
    check errorClass(keTransactionalIdAuthorizationFailed) == ecFatal
    check errorClass(keSecurityDisabled) == ecFatal
    check errorClass(keSaslAuthenticationFailed) == ecFatal
    check errorClass(keGroupMaxSizeReached) == ecFatal
    check errorClass(keFencedInstanceId) == ecFatal
    check errorClass(keTransactionCoordinatorFenced) == ecFatal
    check errorClass(keInvalidRecord) == ecFatal
    check errorClass(keProducerFenced) == ecFatal

  test "ecDuplicate":
    check errorClass(keDuplicateSequenceNumber) == ecDuplicate

  test "ecNeedsProducerIdReset":
    check errorClass(keOutOfOrderSequenceNumber) == ecNeedsProducerIdReset
    check errorClass(keUnknownProducerId) == ecNeedsProducerIdReset

  test "ecUnknown for special-cased and unknown codes":
    # OFFSET_OUT_OF_RANGE is intentionally unclassified — fetcher routes it
    # through the offset-reset policy.
    check errorClass(keOffsetOutOfRange) == ecUnknown
    check errorClass(KafkaErrorCode(999)) == ecUnknown
    check errorClass(KafkaErrorCode(200)) == ecUnknown

  test "ecRebalanceRequired includes MEMBER_ID_REQUIRED (Java parity)":
    # MEMBER_ID_REQUIRED is routed through the same JoinGroup rejoin path
    # as ILLEGAL_GENERATION / UNKNOWN_MEMBER_ID; classifying it is what
    # lets the consumer's poll loop know to re-issue with the broker-
    # assigned memberId instead of treating it as an unknown failure.
    check errorClass(keMemberIdRequired) == ecRebalanceRequired

  test "isAbortable predicate":
    check isAbortable(keInvalidTxnState)
    check isAbortable(keInvalidProducerEpoch)
    check not isAbortable(keNotCoordinator)
    check not isAbortable(keProducerFenced)

  test "isFatal predicate":
    check isFatal(keProducerFenced)
    check isFatal(keClusterAuthorizationFailed)
    check not isFatal(keNotCoordinator)
    check not isFatal(keOffsetOutOfRange)
    # MEMBER_ID_REQUIRED routes through the rejoin path (not fatal, not
    # permanent — see the dedicated isRebalanceRequired test above).
    check not isFatal(keMemberIdRequired)
    check not isPermanent(keMemberIdRequired)

  test "errorClass and legacy isPermanent agree":
    # The legacy isPermanent maps onto ecFatal — both are "do not retry".
    for c in [
      keInvalidMsgSize, keMessageTooLarge, keGroupMaxSizeReached, keProducerFenced
    ]:
      check isPermanent(c) == (errorClass(c) == ecFatal)
    # Codes reclassified from ecFatal: no longer permanent
    check not isPermanent(keUnknownMemberId)
    check not isPermanent(keIllegalGeneration)
    check not isPermanent(keInvalidProducerEpoch)

  test "errorClass and legacy isRetriable agree":
    for c in [keLeaderNotAvailable, keRequestTimedOut, keNotCoordinator]:
      check isRetriable(c) == (errorClass(c) == ecRetriable)
    # Codes reclassified: isRetriable matches ecRetriable, no longer ecRetriable
    for c in [keRebalanceInProgress, keOperationNotAttempted, keIllegalGeneration]:
      check isRetriable(c) == false
      check errorClass(c) != ecRetriable

  test "78/108/109 classify instead of falling to ecUnknown":
    # 78 OFFSET_NOT_AVAILABLE: KIP-101 — high watermark lagging after an
    # election; Java OffsetNotAvailableException extends RetriableException.
    check errorClass(keOffsetNotAvailable) == ecRetriable
    check isRetriable(keOffsetNotAvailable) == true
    # 108 NEW_LEADER_ELECTED: AlterPartition-only; leadership changed — the same
    # metadata-staleness family as 74/75/80, refresh metadata and retry.
    check errorClass(keNewLeaderElected) == ecRetriable
    check isRetriable(keNewLeaderElected) == true
    # 109 OFFSET_MOVED_TO_TIERED_STORAGE: this client has no tiered-storage
    # fetch path, so retrying cannot clear it — classified fatal so a fetch
    # surfaces the error instead of silently treating it as unknown.
    check errorClass(keOffsetMovedToTieredStorage) == ecFatal
    check isPermanent(keOffsetMovedToTieredStorage) == true

  test "78/108/109 wire names match the official protocol error table":
    # Regression: 78 was previously mislabelled ELECTION_NOT_NEEDED on the wire,
    # which is actually 84 — a broker returning OFFSET_NOT_AVAILABLE would have
    # been misreported in logs. Pin the names and numbers.
    check keOffsetNotAvailable.toInt16 == 78'i16
    check keElectionNotNeeded.toInt16 == 84'i16
    check keNewLeaderElected.toInt16 == 108'i16
    check keOffsetMovedToTieredStorage.toInt16 == 109'i16
    check kafkaErrorName(keOffsetNotAvailable) == "OFFSET_NOT_AVAILABLE"
    check kafkaErrorName(KafkaErrorCode(78)) == "OFFSET_NOT_AVAILABLE"
    check kafkaErrorName(keElectionNotNeeded) == "ELECTION_NOT_NEEDED"
    check kafkaErrorName(KafkaErrorCode(84)) == "ELECTION_NOT_NEEDED"
    check kafkaErrorName(keNewLeaderElected) == "NEW_LEADER_ELECTED"
    check kafkaErrorName(KafkaErrorCode(108)) == "NEW_LEADER_ELECTED"
    check kafkaErrorName(keOffsetMovedToTieredStorage) ==
      "OFFSET_MOVED_TO_TIERED_STORAGE"
    check kafkaErrorName(KafkaErrorCode(109)) == "OFFSET_MOVED_TO_TIERED_STORAGE"

  test "isClassified now accepts the previously-unclassified codes":
    check isClassified(keOffsetNotAvailable) == true
    check isClassified(keNewLeaderElected) == true
    check isClassified(keOffsetMovedToTieredStorage) == true

  test "isInstanceFatal separates instance-level from record/request-level fatal":
    # Record/request-level fatal codes fail only the offending request — they
    # must not stop the whole consumer/producer instance.
    for c in [
      keInvalidMsgSize, keMessageTooLarge, keOffsetMetadataTooLarge,
      keInvalidTopicException, keRecordListTooLarge, keInvalidRequiredAcks,
      keInvalidTimestamp, keTopicAlreadyExists, keInvalidRequest,
      keUnsupportedForMessageFormat, kePolicyViolation, keInvalidTransactionTimeout,
      keTopicDeletionDisabled, keUnsupportedCompressionType, keInvalidRecord,
    ]:
      check isPermanent(c) == true
      check not isInstanceFatal(c)
    # Instance-level codes leave the instance unusable — closing is the recovery.
    for c in [
      keInconsistentGroupProtocol, keInvalidGroupId, keInvalidSessionTimeout,
      keTopicAuthorizationFailed, keGroupAuthorizationFailed,
      keClusterAuthorizationFailed, keUnsupportedSaslMechanism, keIllegalSaslState,
      keUnsupportedVersion, keSecurityDisabled, keSaslAuthenticationFailed,
      keGroupMaxSizeReached, keFencedInstanceId, keTransactionCoordinatorFenced,
    ]:
      check isInstanceFatal(c) == true
    check isInstanceFatal(keProducerFenced) == true
    check isInstanceFatal(keUnacceptableCredential) == true
    # Non-fatal codes are never instance-fatal.
    for c in [keLeaderNotAvailable, keNotCoordinator, keOffsetOutOfRange]:
      check not isInstanceFatal(c)

suite "KafkaCommitError":
  test "KafkaCommitError is catchable as KafkaApiError":
    let partErrors = @[
      PartitionCommitError(topic: "t1", partition: 0, errorCode: keNotCoordinator),
      PartitionCommitError(topic: "t1", partition: 1, errorCode: keRequestTimedOut),
    ]
    let err = newKafkaCommitError(keNotCoordinator, "commit failed", partErrors)
    var caughtAsApiError = false
    try:
      raise err
    except KafkaApiError as e:
      caughtAsApiError = true
      check e.errorCode == keNotCoordinator
    check caughtAsApiError

  test "KafkaCommitError partitionErrors accessible after downcast":
    let partErrors = @[
      PartitionCommitError(topic: "t1", partition: 0, errorCode: keNotCoordinator),
      PartitionCommitError(topic: "t2", partition: 3, errorCode: keRequestTimedOut),
    ]
    let err = newKafkaCommitError(keNotCoordinator, "commit failed", partErrors)
    try:
      raise err
    except KafkaCommitError as e:
      check e.partitionErrors.len == 2
      check e.partitionErrors[0].topic == "t1"
      check e.partitionErrors[0].partition == 0
      check e.partitionErrors[0].errorCode == keNotCoordinator
      check e.partitionErrors[1].topic == "t2"
      check e.partitionErrors[1].partition == 3
      check e.partitionErrors[1].errorCode == keRequestTimedOut

  test "KafkaCommitError is catchable as KafkaError":
    let err = newKafkaCommitError(keNone, "test", @[])
    var caught = false
    try:
      raise err
    except KafkaError:
      caught = true
    check caught
