import std/[algorithm, json, os, sequtils, sets, strutils, tables, unittest]

import async_kafka/protocol/api_keys
import async_kafka/protocol/schema
import async_kafka/protocol/messages {.all.}

## Gate: the hand-written wire declarations against
## the upstream Apache Kafka definitions they were transcribed from.
##
## The vendored upstream message definitions under the test fixtures are copies
## of apache/kafka's own message JSON at the 3.7.0 tag (the
## release CI's broker runs), so this runs offline on every test run. For each
## modelled message, over the version window this client can actually negotiate
## (`supportedVersion`):
##
##  1. apiKey, and request/response kind
##  2. `maxVersion` covers the client's range and stays inside upstream's
##     `validVersions`
##  3. `flexibleSince` matches upstream's `flexibleVersions` when the window
##     reaches it (and is unset when it does not)
##  4. the positional field list: same fields, same order, same wire kind, and
##     nullability wherever upstream permits a null inside the window
##  5. tagged fields carry the same tag id
##
## Deliberate deviations are listed in `knownDeviations` with a reason; an entry
## that is no longer needed fails the gate, so the list cannot rot into a
## blanket exemption.
##
## Deliberately NOT checked: Nim-side field names (they diverge on purpose —
## `KeyType`/`coordinatorType`, `LogAppendTimeMs`/`timestampMs`), and the
## `about` / `default` / `entityType` / `mapKey` / `ignorable` attributes: none
## of them change framing, and a broker always writes an ignorable positional
## field (only tagged fields may be omitted). Field *order* is compared instead
## of names because order is what the wire encodes.

type
  VersionRange = tuple[lo, hi: int]

  DeviationKind = enum
    dkStrictNull ## upstream permits a null inside the window, locally required
    dkLenientNull ## locally optional where upstream requires a value

  Deviation = tuple[key: string, kind: DeviationKind, reason: string]

  UpField = object
    name: string
    typeName: string
    versions: VersionRange
    nullable: VersionRange
    tagged: VersionRange
    tag: int
    fields: seq[UpField]

const
  projectRoot = parentDir(parentDir(currentSourcePath))
  fixtureDir = projectRoot / "tests" / "fixtures" / "kafka-3.7.0" / "message"

  # Local declarations by upstream message name. Every vendored file must appear
  # here and every modelled MessageDef must be either here or in
  # `noUpstreamSpec`; both directions are asserted below, so adding a message or
  # bumping a spec means touching these tables in the same commit.
  localSchemas = {
    "ApiVersionsResponse": apiVersionsResponseSchema,
    "MetadataRequest": metadataRequestSchema,
    "MetadataResponse": metadataResponseSchema,
    "FetchRequest": fetchRequestSchema,
    "FetchResponse": fetchResponseSchema,
    "OffsetCommitRequest": offsetCommitRequestSchema,
    "OffsetCommitResponse": offsetCommitResponseSchema,
    "HeartbeatRequest": heartbeatRequestSchema,
    "HeartbeatResponse": heartbeatResponseSchema,
    "InitProducerIdRequest": initProducerIdRequestSchema,
    "InitProducerIdResponse": initProducerIdResponseSchema,
    "EndTxnRequest": endTxnRequestSchema,
    "EndTxnResponse": endTxnResponseSchema,
    "LeaveGroupRequest": leaveGroupRequestSchema,
    "LeaveGroupResponse": leaveGroupResponseSchema,
    "SyncGroupRequest": syncGroupRequestSchema,
    "SyncGroupResponse": syncGroupResponseSchema,
    "JoinGroupRequest": joinGroupRequestSchema,
    "JoinGroupResponse": joinGroupResponseSchema,
    "FindCoordinatorRequest": findCoordinatorRequestSchema,
    "FindCoordinatorResponse": findCoordinatorResponseSchema,
    "ListOffsetsRequest": listOffsetsRequestSchema,
    "ListOffsetsResponse": listOffsetsResponseSchema,
    "OffsetForLeaderEpochRequest": offsetForLeaderEpochRequestSchema,
    "OffsetForLeaderEpochResponse": offsetForLeaderEpochResponseSchema,
    "OffsetFetchRequest": offsetFetchRequestSchema,
    "OffsetFetchResponse": offsetFetchResponseSchema,
    "ProduceRequest": produceRequestSchema,
    "ProduceResponse": produceResponseSchema,
    "AddPartitionsToTxnRequest": addPartitionsToTxnRequestSchema,
    "AddPartitionsToTxnResponse": addPartitionsToTxnResponseSchema,
    "AddOffsetsToTxnRequest": addOffsetsToTxnRequestSchema,
    "AddOffsetsToTxnResponse": addOffsetsToTxnResponseSchema,
    "TxnOffsetCommitRequest": txnOffsetCommitRequestSchema,
    "TxnOffsetCommitResponse": txnOffsetCommitResponseSchema,
    "SaslHandshakeRequest": saslHandshakeRequestSchema,
    "SaslHandshakeResponse": saslHandshakeResponseSchema,
    "SaslAuthenticateRequest": saslAuthenticateRequestSchema,
    "SaslAuthenticateResponse": saslAuthenticateResponseSchema,
  }.toTable

  # The record format is a log format, not an API message: apache/kafka has no
  # message JSON for these two, and their wire layout is covered by the
  # hand-written RecordBatch fixtures in the schema-generation suite.
  noUpstreamSpec = ["Record", "RecordBatch"]

  # Deviations that are deliberate, keyed by `<Message>:<field path>`.
  knownDeviations = [
    (
      key: "MetadataRequest:topics.name",
      kind: dkStrictNull,
      reason:
        "upstream allows a null Name at 10+ for topic-id requests; this client addresses topics by name only (Broker.metadata)",
    ),
    (
      key: "MetadataResponse:topics.name",
      kind: dkStrictNull,
      reason:
        "upstream allows a null Name at 12+ in answers to topic-id requests; this client sends names, so a broker never answers with one — a null fails the decode loudly instead of keying the metadata cache on an empty name",
    ),
    (
      key: "OffsetFetchRequest:topics",
      kind: dkStrictNull,
      reason:
        "upstream makes the array nullable at 2-7 (null = every topic); this client always sends a concrete list",
    ),
    (
      key: "ProduceRequest:topics.partitions.recordSet",
      kind: dkStrictNull,
      reason:
        "upstream marks the record set nullable; the producer always materialises bytes, so the null marker is never needed",
    ),
    (
      key: "SyncGroupResponse:assignment",
      kind: dkLenientNull,
      reason:
        "the local bytes are optional where upstream declares them non-null; the framing is identical (int32 length, -1 = null), so this tolerates a null the broker may send on a rebalance",
    ),
  ]

const
  neverVersion: VersionRange = (-1, -1)
  anyVersion: VersionRange = (0, high(int))

proc parseVersions(text: string): VersionRange =
  ## "0+", "8-10", "1" -> inclusive range. An absent attribute means every
  ## version: upstream omits `versions` on fields that exist in all of them.
  let stripped = text.strip
  if stripped.len == 0:
    return anyVersion
  if stripped.endsWith("+"):
    return (parseInt(stripped[0 ..< stripped.len - 1]), high(int))
  let dash = stripped.find('-')
  if dash < 0:
    let only = parseInt(stripped)
    return (only, only)
  (parseInt(stripped[0 ..< dash]), parseInt(stripped[dash + 1 .. ^1]))

proc overlaps(a, b: VersionRange): bool =
  a.lo <= b.hi and b.lo <= a.hi

proc stripJsonComments(text: string): string =
  ## Kafka's message JSONs carry `//` comments. String literals are respected so
  ## a URL inside an `about` value survives.
  var inString = false
  var escaped = false
  var i = 0
  while i < text.len:
    let c = text[i]
    if inString:
      if escaped:
        escaped = false
      elif c == '\\':
        escaped = true
      elif c == '"':
        inString = false
      result.add c
      inc i
    elif c == '"':
      inString = true
      result.add c
      inc i
    elif c == '/' and i + 1 < text.len and text[i + 1] == '/':
      while i < text.len and text[i] != '\n':
        inc i
    else:
      result.add c
      inc i

proc upstreamFields(node: JsonNode): seq[UpField] =
  if node.isNil or node.kind != JArray:
    return
  for item in node:
    var field = UpField(
      name: item["name"].getStr,
      # A common struct declaration carries no `type`; its fields are shared by
      # name and inlined by `resolveCommonStructs`.
      typeName: (if item.hasKey("type"): item["type"].getStr else: ""),
      versions: anyVersion,
      nullable: neverVersion,
      tagged: neverVersion,
      tag: -1,
    )
    if item.hasKey("versions"):
      field.versions = parseVersions(item["versions"].getStr)
    if item.hasKey("nullableVersions"):
      field.nullable = parseVersions(item["nullableVersions"].getStr)
    if item.hasKey("taggedVersions"):
      field.tagged = parseVersions(item["taggedVersions"].getStr)
    if item.hasKey("tag"):
      field.tag = item["tag"].getInt
    if item.hasKey("fields"):
      field.fields = upstreamFields(item["fields"])
    result.add field

proc resolveCommonStructs(
    fields: var seq[UpField], common: Table[string, seq[UpField]]
) =
  ## Kafka's JSON declares shared element structs once (`commonStructs`) instead
  ## of inlining them per use; inline them so the comparison sees one shape.
  for field in fields.mitems:
    if field.fields.len == 0:
      var key = field.typeName
      if key.startsWith("[]"):
        key = key[2 .. ^1]
      if key.len > 0 and key in common:
        field.fields = common[key]
    if field.fields.len > 0:
      resolveCommonStructs(field.fields, common)

proc upstreamMessageFields(spec: JsonNode): seq[UpField] =
  var common: Table[string, seq[UpField]]
  if spec.hasKey("commonStructs"):
    for struct in spec["commonStructs"]:
      common[struct["name"].getStr] = upstreamFields(struct["fields"])
  result = upstreamFields(spec["fields"])
  resolveCommonStructs(result, common)

proc localRange(f: FieldDef): VersionRange =
  (
    (if f.sinceVersion < 0: 0 else: f.sinceVersion),
    (if f.untilVersion < 0: high(int) else: f.untilVersion),
  )

proc baseKind(k: FieldKind): FieldKind =
  case k
  of fkNullableString: fkString
  of fkNullableBytes: fkBytes
  else: k

proc isNullable(f: FieldDef): bool =
  f.kind in {fkNullableString, fkNullableBytes} or (f.kind == fkArray and f.nullable)

proc wireKind(upstreamType: string): FieldKind =
  case upstreamType
  of "int8":
    fkInt8
  of "int16":
    fkInt16
  of "int32":
    fkInt32
  of "int64":
    fkInt64
  of "bool":
    fkBool
  of "string":
    fkString
  of "bytes":
    fkBytes
  of "uuid":
    fkUuid
  # This client carries a partition's record set as raw bytes and lets the
  # record-batch codec handle the contents.
  of "records":
    fkBytes
  else:
    raise newException(ValueError, "unmapped upstream type: " & upstreamType)

const
  # Every API this package sends; `ApiKey` carries tuple values, so it is not
  # ordinal-iterable and the set is spelled out.
  sendingApis = [
    akProduce, akFetch, akListOffsets, akMetadata, akOffsetCommit, akOffsetFetch,
    akFindCoordinator, akJoinGroup, akHeartbeat, akLeaveGroup, akSyncGroup,
    akSaslHandshake, akApiVersions, akInitProducerId, akOffsetForLeaderEpoch,
    akAddPartitionsToTxn, akAddOffsetsToTxn, akEndTxn, akTxnOffsetCommit,
    akSaslAuthenticate,
  ]

proc clientWindow(apiKey: int): VersionRange =
  for key in sendingApis:
    if int(ord(key)) == apiKey:
      let supported = supportedVersion(key)
      return (int(supported.minVersion), int(supported.maxVersion))
  raise newException(ValueError, "no ApiKey value for upstream key " & $apiKey)

proc messageSpec(path: string): JsonNode =
  parseJson(stripJsonComments(readFile(path)))

proc describe(r: VersionRange): string =
  if r.lo == r.hi:
    $r.lo
  elif r.hi == high(int):
    $r.lo & "+"
  else:
    $r.lo & "-" & $r.hi

proc deviationFor(
    message, path: string, kind: DeviationKind, used: var HashSet[string]
): string =
  ## The reason when this exact deviation is listed, otherwise empty (the caller
  ## reports the mismatch as a failure).
  let key = message & ":" & path
  for entry in knownDeviations:
    if entry.key == key and entry.kind == kind:
      used.incl key
      return entry.reason
  ""

proc compareFields(
    message, path: string,
    window: VersionRange,
    upstream: seq[UpField],
    local: seq[FieldDef],
    failures: var seq[string],
    used: var HashSet[string],
) =
  ## Positional comparison of one field list. Tagged fields live in the trailer
  ## and are matched by tag id instead.
  var upPositional, upTagged: seq[UpField]
  for field in upstream:
    if overlaps(field.versions, window) or overlaps(field.tagged, window):
      if overlaps(field.tagged, window):
        upTagged.add field
      else:
        upPositional.add field

  var localPositional: seq[FieldDef]
  for field in local:
    if not overlaps(localRange(field), window):
      continue
    if field.taggedId >= 0:
      var matched = false
      for up in upTagged:
        if up.tag == field.taggedId:
          matched = true
          if baseKind(field.kind) != wireKind(up.typeName):
            failures.add(
              message & ": " & path & "." & field.name & " is a tagged " & $field.kind &
                " locally but upstream tags " & up.typeName
            )
      if not matched:
        failures.add(
          message & ": local tagged field " & path & "." & field.name & " (tag " &
            $field.taggedId & ") has no upstream counterpart in " & describe(window)
        )
    else:
      localPositional.add field

  for up in upTagged:
    var matched = false
    for field in local:
      if field.taggedId == up.tag:
        matched = true
    if not matched:
      failures.add(
        message & ": upstream field " & path & "." & up.name & " is tagged " & $up.tag &
          " inside " & describe(window) & " but no local field carries it"
      )

  let count = min(upPositional.len, localPositional.len)
  for i in 0 ..< count:
    let up = upPositional[i]
    let field = localPositional[i]
    let here =
      if path.len == 0:
        field.name
      else:
        path & "." & field.name
    if up.typeName.startsWith("[]"):
      if baseKind(field.kind) != fkArray:
        failures.add(
          message & ": " & here & " is " & $field.kind & " locally but upstream " &
            up.name & " is " & up.typeName & " at " & describe(up.versions)
        )
      elif up.fields.len > 0:
        # Struct array: recurse into the element fields.
        compareFields(message, here, window, up.fields, field.nested, failures, used)
      else:
        let element = wireKind(up.typeName[2 .. ^1])
        if field.nested.len > 0 or field.elementKind != element:
          failures.add(
            message & ": " & here & " element is " & $field.elementKind &
              " locally but upstream " & up.name & " is " & up.typeName
          )
    elif baseKind(field.kind) != wireKind(up.typeName):
      failures.add(
        message & ": " & here & " is " & $field.kind & " locally but upstream " & up.name &
          " is " & up.typeName & " at " & describe(up.versions)
      )

    if overlaps(up.nullable, window) and not field.isNullable:
      let reason = deviationFor(message, here, dkStrictNull, used)
      if reason.len == 0:
        failures.add(
          message & ": " & here & " is required locally but upstream allows a null at " &
            describe(up.nullable) & " (window " & describe(window) &
            "); add it to knownDeviations with a reason if that is deliberate"
        )
    elif field.isNullable and not overlaps(up.nullable, window):
      let reason = deviationFor(message, here, dkLenientNull, used)
      if reason.len == 0:
        failures.add(
          message & ": " & here &
            " is optional locally but upstream declares no null at " & describe(window) &
            "; add it to knownDeviations with a reason if that is deliberate"
        )

  if upPositional.len > localPositional.len:
    for i in localPositional.len ..< upPositional.len:
      failures.add(
        message & ": upstream field " & path & "." & upPositional[i].name & " (" &
          upPositional[i].typeName & ", " & describe(upPositional[i].versions) &
          ") has no local counterpart in " & describe(window)
      )
  elif localPositional.len > upPositional.len:
    for i in upPositional.len ..< localPositional.len:
      failures.add(
        message & ": local field " & path & "." & localPositional[i].name &
          " has no upstream counterpart in " & describe(window)
      )

proc compareMessage(
    name: string,
    local: MessageDef,
    spec: JsonNode,
    failures: var seq[string],
    used: var HashSet[string],
) =
  let upstreamType = spec["type"].getStr
  let upstreamKey = spec["apiKey"].getInt
  let upstreamValid = parseVersions(spec["validVersions"].getStr)
  let window = clientWindow(upstreamKey)

  if local.apiKey >= 0:
    if upstreamType != "request":
      failures.add(
        name & ": modelled as a request but upstream declares a " & upstreamType
      )
    if local.apiKey != upstreamKey:
      failures.add(
        name & ": apiKey is " & $local.apiKey & " locally but " & $upstreamKey &
          " upstream"
      )
  elif upstreamType != "response":
    failures.add(
      name & ": modelled as a response but upstream declares a " & upstreamType
    )

  if window.hi > local.maxVersion:
    failures.add(
      name & ": the client negotiates up to v" & $window.hi &
        " but the schema models up to v" & $local.maxVersion
    )
  if local.maxVersion > upstreamValid.hi:
    failures.add(
      name & ": the schema models up to v" & $local.maxVersion &
        " but upstream validVersions ends at v" & $upstreamValid.hi
    )

  let upstreamFlexible =
    if spec.hasKey("flexibleVersions"):
      spec["flexibleVersions"].getStr.strip
    else:
      "none"
  if upstreamFlexible == "none":
    if local.flexibleSince >= 0:
      failures.add(name & ": upstream is never flexible but flexibleSince is set")
  else:
    let flexible = parseVersions(upstreamFlexible)
    if window.hi >= flexible.lo:
      if local.flexibleSince != flexible.lo:
        failures.add(
          name & ": the client reaches v" & $window.hi & ", so flexibleSince must be " &
            $flexible.lo & " (upstream flexibleVersions " & upstreamFlexible & "), got " &
            $local.flexibleSince
        )
    elif local.flexibleSince >= 0:
      failures.add(
        name & ": flexibleSince is " & $local.flexibleSince &
          " but the client never reaches upstream flexibleVersions " & upstreamFlexible
      )

  var localPositional: seq[FieldDef]
  for field in local.fields:
    localPositional.add field
  compareFields(
    name, "", window, upstreamMessageFields(spec), localPositional, failures, used
  )

proc modelledNames(): HashSet[string] =
  ## Every modelled message declaration, read from the source so a new schema
  ## cannot escape the gate by not being listed here.
  let source = readFile(projectRoot / "async_kafka" / "protocol" / "messages.nim")
  var inMessageDef = false
  for line in source.splitLines():
    let stripped = line.strip
    if stripped.startsWith("const ") and stripped.endsWith("= MessageDef("):
      inMessageDef = true
      continue
    if inMessageDef:
      if stripped.startsWith("name: \""):
        let start = stripped.find('"')
        let stop = stripped.find('"', start + 1)
        if stop > start:
          result.incl stripped[start + 1 ..< stop]
      inMessageDef = false

suite "Wire spec drift (apache/kafka 3.7.0)":
  test "every modelled message matches its vendored upstream definition":
    var failures: seq[string]
    var used: HashSet[string]
    for name in sorted(toSeq(localSchemas.keys)):
      let specPath = fixtureDir / (name & ".json")
      if not fileExists(specPath):
        failures.add(name & ": no vendored spec at " & specPath)
        continue
      try:
        compareMessage(name, localSchemas[name], messageSpec(specPath), failures, used)
      except CatchableError as e:
        failures.add(name & ": could not compare: " & e.msg)
    for entry in knownDeviations:
      if entry.key notin used:
        failures.add(
          "knownDeviations entry " & entry.key &
            " is no longer needed (or its kind changed): remove it"
        )
    if failures.len > 0:
      echo "wire declarations differ from the vendored apache/kafka 3.7.0 spec:"
      for failure in sorted(failures):
        echo "  ", failure
    check failures.len == 0

  test "the vendored specs and the local declarations cover each other":
    var failures: seq[string]
    var vendored: HashSet[string]
    for path in walkDir(fixtureDir):
      if not path.path.endsWith(".json"):
        continue
      let name = path.path.splitFile.name
      vendored.incl name
      if name notin localSchemas:
        failures.add(
          name & " is vendored but no local MessageDef is wired to it" &
            " (add it to localSchemas or delete the file)"
        )
    let modelled = modelledNames()
    for name in modelled:
      if name notin localSchemas and name notin noUpstreamSpec:
        failures.add(
          name & " has a MessageDef but no vendored spec and is not in noUpstreamSpec"
        )
    for name in localSchemas.keys:
      if name notin modelled:
        failures.add(
          name & " is wired in localSchemas but messages.nim has no such schema"
        )
    for name in noUpstreamSpec:
      if name notin modelled:
        failures.add(name & " is listed as having no upstream spec but is not modelled")
      if name in vendored:
        failures.add(name & " has a vendored spec after all; wire it to a MessageDef")
    if failures.len > 0:
      echo "vendored specs and local declarations do not cover each other:"
      for failure in sorted(failures):
        echo "  ", failure
    check failures.len == 0
