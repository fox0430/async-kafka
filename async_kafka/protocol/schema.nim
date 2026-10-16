## Compile-time DSL for declaring Kafka wire-format messages: `defineKafkaMessage`
## generates the Nim type plus encode/decode procs from `const` MessageDef data, so
## the compiler validates the schema and the macro walks it without a parser. Emits
## calls to the primitive codec; the per-API MessageDefs are declared separately.

import std/[algorithm, macros]

import primitives

type
  FieldKind* = enum
    fkInt8
    fkInt16
    fkInt32
    fkInt64
    fkBool
    fkString
    fkNullableString
    fkBytes
    fkNullableBytes
    fkUuid ## KIP-516 16-byte fixed UUID. Same wire layout in legacy and flex.
    fkVarInt
      ## Zigzag varint (int64 on the Nim side), used inside v2 RecordBatch /
      ## Record payloads. Distinct from KIP-482 compact scalars: always zigzag,
      ## never the +1-encoded unsigned form of the COMPACT_* length prefixes.
    fkNullableVarBytes
      ## Varint-length-prefixed bytes, -1 = null (`Option[seq[byte]]`); same
      ## framing as the record key/value fields inside a v2 RecordBatch.
    fkVarString
      ## Varint-length-prefixed UTF-8 string (record-header key in a v2
      ## RecordBatch; non-null on the wire).
    fkArray
      ## Struct array when `nested.len > 0` (element type = synthesized
      ## `nestedTypeName`); scalar array when `nested.len == 0` (element type
      ## derived from `elementKind`).

  ArrayCountKind* = enum
    ackInt32 ## default: int32 length prefix (legacy) / uvarint+1 (compact)
    ackVarint
      ## Zigzag varint length prefix, no null marker; used for the per-Record
      ## headers array inside a v2 RecordBatch (KIP-98).

  FieldTypeShape = enum
    ftsIdent
    ftsString
    ftsNullableString
    ftsBytes
    ftsNullableBytes
    ftsUuid
    ftsVarInt
    ftsVarString
    ftsArray

  FieldZeroKind = enum
    fzkNone
    fzkInt8
    fzkInt16
    fzkInt32
    fzkInt64
    fzkBool
    fzkString
    fzkNullableString
    fzkBytes
    fzkNullableBytes
    fzkUuid
    fzkVarInt
    fzkNullableVarBytes
    fzkVarString

  FieldKindSpec = object
    ## The complete compile-time codec contract for one FieldKind; keeping type,
    ## primitive, compact, and zero-value data in one record stops the encode and
    ## decode generators from drifting apart when a kind is added.
    nimType: string
    typeShape: FieldTypeShape
    readPrimitive: string
    writePrimitive: string
    compactReadPrimitive: string
    compactWritePrimitive: string
    wireType: string
    zeroKind: FieldZeroKind
    taggable: bool
    deltaCapable: bool

  ContextParam* = object
    name*: string
    typ*: string
      ## Nim type as a string (e.g. "int64"), idents into the generated proc
      ## signature: single-ident types only — alias composites first.

  FieldDef* = object
    name*: string
    kind*: FieldKind
    nimType*: string
      ## Optional Nim-type override for a distinct alias of an int wire kind
      ## (e.g. `KafkaErrorCode`). Empty = use the default from `kind`.
    nimTypeCtor*: string
      ## Optional decoder proc substituted for the default `<nimType>(<wire>)`
      ## conversion. Required for a real enum: the raw conversion raises
      ## uncatchable RangeDefect on an unknown wire value, violating the
      ## KafkaError contract, so the named proc must raise KafkaProtocolError.
    sinceVersion*: int ## -1 = field is present in every version
    untilVersion*: int ## -1 = forever
    nestedTypeName*: string ## for fkArray: name of the synthesized nested type
    nested*: seq[FieldDef] ## for fkArray with struct elements
    elementKind*: FieldKind
      ## For fkArray with `nested.len == 0` and no `elementTypeName`: scalar kind
      ## of each element (must be a non-array scalar). Ignored for struct arrays
      ## and external-typed arrays.
    elementTypeName*: string
      ## For fkArray with `nested.len == 0`: name of an existing top-level type
      ## used as the element type (e.g. "Record" for RecordBatch.records). Skips
      ## nested-type synthesis; an envelope hook encodes/decodes the array.
    nullable*: bool
      ## For fkArray only: `Option[seq[<elem>]]` with a wire null marker (int32
      ## -1 legacy, uvarint(0) compact). For nullable scalars use
      ## fkNullable{String,Bytes} instead.
    arrayCountKind*: ArrayCountKind
      ## For fkArray only: how the element count is framed. `ackInt32` (default)
      ## keeps the int32/compact behaviour governed by `flexFlag`; `ackVarint` is
      ## a zigzag varint with no null marker (Record.headers in a v2 batch).
    taggedId*: int = -1
      ## -1 = regular positional field (default). 0+ = field lives in the flexible
      ## tagged-fields trailer with this tag id; requires the schema's
      ## flexibleSince >= 0 and a source-level `Option[<scalar>]` (encoded when some).
    discardOnRead*: bool = false
      ## Read from the wire to keep the reader aligned but absent from the Nim
      ## type; encode writes a zero-value placeholder so a synthesized payload
      ## stays well-formed. Positional scalars only — `fkArray`/`taggedId >= 0`
      ## is a compile error.
    deltaFrom*: string
      ## Name of a `contextParams` entry; only valid on `fkVarInt`. The Nim value
      ## is absolute (e.g. record offset) while the wire carries `value -
      ## <deltaFrom>` (e.g. offset delta); decode reverses the operation.

  EnvelopeKind* = enum
    ekNone
    ekRecordBatchV2
      ## v2 RecordBatch envelope (KIP-98): the macro emits a hand-tuned
      ## encode/decode body framing the baseOffset/batchLength/CRC/compressed-
      ## records layout. `fields` only contribute the Nim object shape (standard
      ## codegen is bypassed); v2 is the only wire version.

  MessageDef* = object
    name*: string ## name of the generated top-level type
    apiKey*: int ## -1 for non-top-level structs / responses
    maxVersion*: int ## highest API version this schema covers
    flexibleSince*: int ## -1 = not flexible
    fields*: seq[FieldDef]
    contextParams*: seq[ContextParam]
      ## Extra params appended to the generated encode/decode signatures, threading
      ## per-batch values (baseOffset, firstTimestamp) into nested delta codecs.
    envelope*: EnvelopeKind
      ## `ekNone` (default) uses the standard codec; other envelopes replace the
      ## encode/decode bodies wholesale for formats outside the primitive model.

# Compile-time helpers

proc fieldKindSpec(kind: FieldKind): FieldKindSpec {.compileTime.} =
  ## Single source of truth for every FieldKind-to-wire mapping: the read and write
  ## generators only select a direction from this record instead of maintaining
  ## parallel kind cases.
  case kind
  of fkInt8:
    result = FieldKindSpec(
      nimType: "int8",
      typeShape: ftsIdent,
      readPrimitive: "readInt8",
      writePrimitive: "writeInt8",
      wireType: "int8",
      zeroKind: fzkInt8,
      taggable: true,
    )
  of fkInt16:
    result = FieldKindSpec(
      nimType: "int16",
      typeShape: ftsIdent,
      readPrimitive: "readInt16",
      writePrimitive: "writeInt16",
      wireType: "int16",
      zeroKind: fzkInt16,
      taggable: true,
    )
  of fkInt32:
    result = FieldKindSpec(
      nimType: "int32",
      typeShape: ftsIdent,
      readPrimitive: "readInt32",
      writePrimitive: "writeInt32",
      wireType: "int32",
      zeroKind: fzkInt32,
      taggable: true,
    )
  of fkInt64:
    result = FieldKindSpec(
      nimType: "int64",
      typeShape: ftsIdent,
      readPrimitive: "readInt64",
      writePrimitive: "writeInt64",
      wireType: "int64",
      zeroKind: fzkInt64,
      taggable: true,
    )
  of fkBool:
    result = FieldKindSpec(
      nimType: "bool",
      typeShape: ftsIdent,
      readPrimitive: "readBool",
      writePrimitive: "writeBool",
      zeroKind: fzkBool,
      taggable: true,
    )
  of fkString:
    result = FieldKindSpec(
      nimType: "string",
      typeShape: ftsString,
      readPrimitive: "readString",
      writePrimitive: "writeString",
      compactReadPrimitive: "readCompactString",
      compactWritePrimitive: "writeCompactString",
      zeroKind: fzkString,
      taggable: true,
    )
  of fkNullableString:
    result = FieldKindSpec(
      nimType: "Option[string]",
      typeShape: ftsNullableString,
      readPrimitive: "readNullableString",
      writePrimitive: "writeNullableString",
      compactReadPrimitive: "readCompactNullableString",
      compactWritePrimitive: "writeCompactNullableString",
      zeroKind: fzkNullableString,
      taggable: true,
    )
  of fkBytes:
    result = FieldKindSpec(
      nimType: "seq[byte]",
      typeShape: ftsBytes,
      readPrimitive: "readLengthPrefixedBytes",
      writePrimitive: "writeBytes",
      compactReadPrimitive: "readCompactBytes",
      compactWritePrimitive: "writeCompactBytes",
      zeroKind: fzkBytes,
      taggable: true,
    )
  of fkNullableBytes:
    result = FieldKindSpec(
      nimType: "Option[seq[byte]]",
      typeShape: ftsNullableBytes,
      readPrimitive: "readNullableBytes",
      writePrimitive: "writeNullableBytes",
      compactReadPrimitive: "readCompactNullableBytes",
      compactWritePrimitive: "writeCompactNullableBytes",
      zeroKind: fzkNullableBytes,
      taggable: true,
    )
  of fkUuid:
    result = FieldKindSpec(
      nimType: "array[16, byte]",
      typeShape: ftsUuid,
      readPrimitive: "readUuid",
      writePrimitive: "writeUuid",
      zeroKind: fzkUuid,
      taggable: true,
    )
  of fkVarInt:
    result = FieldKindSpec(
      nimType: "int64",
      typeShape: ftsVarInt,
      readPrimitive: "decodeVarint",
      writePrimitive: "encodeVarint",
      wireType: "int64",
      zeroKind: fzkVarInt,
      taggable: false,
      deltaCapable: true,
    )
  of fkNullableVarBytes:
    result = FieldKindSpec(
      nimType: "Option[seq[byte]]",
      typeShape: ftsNullableBytes,
      readPrimitive: "decodeVarBytes",
      writePrimitive: "encodeVarBytes",
      zeroKind: fzkNullableVarBytes,
      taggable: false,
    )
  of fkVarString:
    result = FieldKindSpec(
      nimType: "string",
      typeShape: ftsVarString,
      readPrimitive: "decodeVarString",
      writePrimitive: "encodeVarString",
      zeroKind: fzkVarString,
      taggable: false,
    )
  of fkArray:
    result = FieldKindSpec(
      nimType: "", typeShape: ftsArray, zeroKind: fzkNone, taggable: false
    )

proc primitiveTypeNode(kind: FieldKind): NimNode {.compileTime.} =
  ## Nim type node for a non-array, non-distinct field kind. Also used by the
  ## tag-resident payload encoder to materialize the inner type of `Option[<scalar>]`.
  let spec = fieldKindSpec(kind)
  case spec.typeShape
  of ftsIdent:
    ident(spec.nimType)
  of ftsString, ftsVarString:
    ident("string")
  of ftsNullableString:
    nnkBracketExpr.newTree(ident("Option"), ident("string"))
  of ftsBytes:
    nnkBracketExpr.newTree(ident("seq"), ident("byte"))
  of ftsNullableBytes:
    nnkBracketExpr.newTree(
      ident("Option"), nnkBracketExpr.newTree(ident("seq"), ident("byte"))
    )
  of ftsUuid:
    nnkBracketExpr.newTree(ident("array"), newLit(16), ident("byte"))
  of ftsVarInt:
    ident(spec.nimType)
  of ftsArray:
    doAssert false, "primitiveTypeNode: fkArray has no scalar form"
    newEmptyNode()

proc zeroExprForField(kind: FieldKind): NimNode {.compileTime.} =
  ## Canonical placeholder for a read-and-drop field; the kind-to-zero mapping
  ## comes from FieldKindSpec rather than being duplicated here.
  let spec = fieldKindSpec(kind)
  case spec.zeroKind
  of fzkNone:
    doAssert false, "zeroExprForField has no zero value for " & $kind
    newEmptyNode()
  of fzkInt8, fzkInt16, fzkInt32, fzkInt64, fzkVarInt:
    newCall(ident(spec.nimType), newLit(0))
  of fzkBool:
    ident("false")
  of fzkString, fzkVarString:
    newLit("")
  of fzkNullableString:
    newCall(nnkBracketExpr.newTree(ident("none"), ident("string")))
  of fzkBytes:
    nnkPrefix.newTree(ident("@"), nnkBracket.newTree())
  of fzkNullableBytes, fzkNullableVarBytes:
    newCall(
      nnkBracketExpr.newTree(
        ident("none"), nnkBracketExpr.newTree(ident("seq"), ident("byte"))
      )
    )
  of fzkUuid:
    newCall(
      ident("default"),
      nnkBracketExpr.newTree(ident("array"), newLit(16), ident("byte")),
    )

proc fieldTypeNode(f: FieldDef): NimNode {.compileTime.} =
  ## Nim type node for a field's declaration. Array fields become
  ## `seq[<element>]`, wrapped in `Option[...]` when nullable; tag-resident fields
  ## too, so a missing value suppresses the tag on the wire.
  var inner: NimNode
  if f.kind == fkArray:
    let elem =
      if f.nested.len > 0:
        ident(f.nestedTypeName)
      elif f.elementTypeName.len > 0:
        ident(f.elementTypeName)
      else:
        primitiveTypeNode(f.elementKind)
    let seqNode = nnkBracketExpr.newTree(ident("seq"), elem)
    inner =
      if f.nullable:
        nnkBracketExpr.newTree(ident("Option"), seqNode)
      else:
        seqNode
  elif f.nimType.len > 0:
    # Distinct alias like KafkaErrorCode: trust the caller's spelling; only valid
    # for single-ident types (UUID/Option go through primitiveTypeNode).
    inner = ident(f.nimType)
  else:
    inner = primitiveTypeNode(f.kind)
  if f.taggedId >= 0:
    nnkBracketExpr.newTree(ident("Option"), inner)
  else:
    inner

proc objectTypeDef(typeName: string, fields: seq[FieldDef]): NimNode {.compileTime.} =
  var recList = newNimNode(nnkRecList)
  for f in fields:
    if f.discardOnRead:
      # Read-and-drop fields exist only on the wire: no Nim field, so callers
      # cannot depend on a value the decoder threw away.
      continue
    recList.add newIdentDefs(postfix(ident(f.name), "*"), fieldTypeNode(f))
  nnkTypeDef.newTree(
    postfix(ident(typeName), "*"),
    newEmptyNode(),
    nnkObjectTy.newTree(newEmptyNode(), newEmptyNode(), recList),
  )

proc versionGateCondition(
    sinceVersion, untilVersion: int, versionSym: NimNode
): NimNode {.compileTime.} =
  ## Condition node for an `if` gating a field by version range; nil when the
  ## field is present in every version (no gate needed).
  let hasSince = sinceVersion >= 0
  let hasUntil = untilVersion >= 0
  if not hasSince and not hasUntil:
    return nil

  let sinceExpr =
    if hasSince:
      infix(versionSym, ">=", newLit(int16(sinceVersion)))
    else:
      nil
  let untilExpr =
    if hasUntil:
      infix(versionSym, "<=", newLit(int16(untilVersion)))
    else:
      nil
  if sinceExpr != nil and untilExpr != nil:
    infix(sinceExpr, "and", untilExpr)
  elif sinceExpr != nil:
    sinceExpr
  else:
    untilExpr

proc wrapInVersionGate(stmt, gate: NimNode): NimNode {.compileTime.} =
  if gate == nil:
    stmt
  else:
    nnkIfStmt.newTree(nnkElifBranch.newTree(gate, stmt))

proc protocolErrorRaise(message: NimNode): NimNode {.compileTime.} =
  ## AST for `raise newException(KafkaProtocolError, <message>)`: the shape every
  ## generated guard uses to reject a malformed response.
  nnkRaiseStmt.newTree(
    newCall(ident("newException"), ident("KafkaProtocolError"), message)
  )

proc readCallForField(
    f: FieldDef, readerSym: NimNode, flexFlag: NimNode
): NimNode {.compileTime.} =
  ## Call expression that reads one field of `f` from `readerSym`. A `nimType`
  ## override wraps the result in a converter call; a non-nil `flexFlag` with a
  ## compact variant emits `if flexFlag: <compact> else: <legacy>`; `deltaFrom`
  ## reconstructs the absolute value with a wrapping `+%` (see below).
  let spec = fieldKindSpec(f.kind)
  let primitive = spec.readPrimitive
  doAssert primitive.len > 0,
    "readCallForField: unsupported field kind " & $f.kind & " for " & f.name
  let legacyCall = newCall(ident(primitive), readerSym)
  let compactName = spec.compactReadPrimitive
  var call =
    if flexFlag != nil and compactName.len > 0:
      nnkIfStmt.newTree(
        nnkElifBranch.newTree(flexFlag, newCall(ident(compactName), readerSym)),
        nnkElse.newTree(legacyCall),
      )
    else:
      legacyCall
  if f.deltaFrom.len > 0:
    doAssert fieldKindSpec(f.kind).deltaCapable,
      "readCallForField: deltaFrom only supported on delta-capable field kinds (" &
        f.name & ")"
    # Wrapping add (`+%`): a hostile delta can overflow `base + delta`. Java wraps
    # silently; matching that keeps OverflowDefect out of the `raises` contract.
    call = infix(call, "+%", ident(f.deltaFrom))
  if f.nimTypeCtor.len > 0:
    newCall(ident(f.nimTypeCtor), call)
  elif f.nimType.len > 0:
    # Wrap in `Type(<wire value>)` for distinct aliases like KafkaErrorCode.
    newCall(ident(f.nimType), call)
  else:
    call

proc writeCallForField(
    f: FieldDef, writerSym, valueExpr: NimNode, flexFlag: NimNode
): NimNode {.compileTime.} =
  let spec = fieldKindSpec(f.kind)
  let primitive = spec.writePrimitive
  doAssert primitive.len > 0,
    "writeCallForField: unsupported field kind " & $f.kind & " for " & f.name
  var coerced =
    if f.nimType.len > 0:
      doAssert spec.wireType.len > 0,
        "writeCallForField: nimType override only valid for int wire kinds"
      newCall(ident(spec.wireType), valueExpr)
    else:
      valueExpr
  if f.deltaFrom.len > 0:
    doAssert fieldKindSpec(f.kind).deltaCapable,
      "writeCallForField: deltaFrom only supported on delta-capable field kinds (" &
        f.name & ")"
    # Wire value = absolute -% context base (wrapping inverse of the decoder's
    # `+%`), keeping round-trips exact far from the batch base.
    coerced = infix(coerced, "-%", ident(f.deltaFrom))
  let legacyCall = newCall(ident(primitive), writerSym, coerced)
  let compactName = spec.compactWritePrimitive
  if flexFlag != nil and compactName.len > 0:
    nnkIfStmt.newTree(
      nnkElifBranch.newTree(flexFlag, newCall(ident(compactName), writerSym, coerced)),
      nnkElse.newTree(legacyCall),
    )
  else:
    legacyCall

proc partitionTagFields(
    fields: seq[FieldDef]
): tuple[regular, tagged: seq[FieldDef]] {.compileTime.} =
  ## Splits fields into regular (positional) and tag-resident, sorting the
  ## latter by ascending taggedId as required by KIP-482.
  for f in fields:
    doAssert not (f.taggedId >= 0 and f.discardOnRead),
      "field '" & f.name &
        "': discardOnRead cannot combine with tagged fields (Java client " &
        "skips unknown tags via the tagged-fields trailer's length prefix " &
        "rather than a per-field discard mode)"
    doAssert not (f.kind == fkArray and f.discardOnRead),
      "field '" & f.name &
        "': discardOnRead does not support fkArray yet (only positional " &
        "scalars are read-and-dropped today)"
    if f.taggedId >= 0:
      result.tagged.add f
    else:
      result.regular.add f
  result.tagged.sort do(a, b: FieldDef) -> int:
    cmp(a.taggedId, b.taggedId)

proc readTagPayloadCall(f: FieldDef, readerSym: NimNode): NimNode {.compileTime.} =
  ## Reads a tag-resident field's payload; the compact/legacy choice and the
  ## supported-kind contract come from the same FieldKindSpec as ordinary fields.
  let spec = fieldKindSpec(f.kind)
  doAssert spec.taggable,
    "readTagPayloadCall: kind " & $f.kind & " is not supported as a tag (" & f.name & ")"
  let primitive =
    if spec.compactReadPrimitive.len > 0:
      spec.compactReadPrimitive
    else:
      spec.readPrimitive
  doAssert primitive.len > 0,
    "readTagPayloadCall: unsupported kind " & $f.kind & " for " & f.name
  let call = newCall(ident(primitive), readerSym)
  if f.nimTypeCtor.len > 0:
    newCall(ident(f.nimTypeCtor), call)
  elif f.nimType.len > 0:
    newCall(ident(f.nimType), call)
  else:
    call

proc writeTagPayloadCall(
    f: FieldDef, writerSym, valueExpr: NimNode
): NimNode {.compileTime.} =
  ## Writes a tag-resident field's payload using the same FieldKindSpec.
  let spec = fieldKindSpec(f.kind)
  doAssert spec.taggable,
    "writeTagPayloadCall: kind " & $f.kind & " is not supported as a tag (" & f.name &
      ")"
  let primitive =
    if spec.compactWritePrimitive.len > 0:
      spec.compactWritePrimitive
    else:
      spec.writePrimitive
  doAssert primitive.len > 0,
    "writeTagPayloadCall: unsupported kind " & $f.kind & " for " & f.name
  let coerced =
    if f.nimType.len > 0:
      doAssert spec.wireType.len > 0,
        "writeTagPayloadCall: nimType override only valid for int wire kinds"
      newCall(ident(spec.wireType), valueExpr)
    else:
      valueExpr
  newCall(ident(primitive), writerSym, coerced)

proc buildArrayLenReadStmt(
    lenSym, readerSym, flexFlag: NimNode,
    nullable: bool,
    countKind: ArrayCountKind = ackInt32,
): NimNode {.compileTime.} =
  ## Decodes an array length into `lenSym` (int32). Nullable arrays preserve the
  ## negative null marker for the caller to branch on; non-nullable clamp to 0
  ## (legacy `readArrayLen`). `ackVarint` overrides `flexFlag` with a zigzag read.
  if countKind == ackVarint:
    doAssert not nullable,
      "buildArrayLenReadStmt: ackVarint does not support nullable arrays"
    # Validate count fits in int32 and is non-negative before truncating: the
    # same overflow protection readArrayLen applies.
    let raw = genSym(nskLet, "varintLen")
    return newStmtList(
      newLetStmt(raw, newCall(ident("decodeVarint"), readerSym)),
      nnkIfStmt.newTree(
        nnkElifBranch.newTree(
          infix(
            infix(raw, "<", newLit(0'i64)),
            "or",
            infix(raw, ">", newLit(int64(maxWireArrayLen))),
          ),
          protocolErrorRaise(
            infix(
              newLit("invalid varint array length: "), "&", newCall(ident("$"), raw)
            )
          ),
        )
      ),
      newVarStmt(lenSym, newCall(ident("int32"), raw)),
    )
  let legacyReader = if nullable: "readNullableArrayLen" else: "readArrayLen"
  if flexFlag == nil:
    return newVarStmt(lenSym, newCall(ident(legacyReader), readerSym))
  let flexBranch =
    if nullable:
      newAssignment(lenSym, newCall(ident("readCompactArrayLen"), readerSym))
    else:
      newStmtList(
        newAssignment(lenSym, newCall(ident("readCompactArrayLen"), readerSym)),
        nnkIfStmt.newTree(
          nnkElifBranch.newTree(
            infix(lenSym, "<", newLit(0'i32)), newAssignment(lenSym, newLit(0'i32))
          )
        ),
      )
  newStmtList(
    nnkVarSection.newTree(newIdentDefs(lenSym, ident("int32"), newEmptyNode())),
    nnkIfStmt.newTree(
      nnkElifBranch.newTree(flexFlag, flexBranch),
      nnkElse.newTree(newAssignment(lenSym, newCall(ident(legacyReader), readerSym))),
    ),
  )

proc buildArrayLenWriteStmt(
    writerSym, sourceExpr, flexFlag: NimNode, countKind: ArrayCountKind = ackInt32
): NimNode {.compileTime.} =
  if countKind == ackVarint:
    return newCall(
      ident("encodeVarint"),
      writerSym,
      newCall(ident("int64"), newCall(ident("len"), sourceExpr)),
    )
  if flexFlag == nil:
    return newCall(
      ident("writeArrayLen"),
      writerSym,
      newCall(ident("int32"), newCall(ident("len"), sourceExpr)),
    )
  nnkIfStmt.newTree(
    nnkElifBranch.newTree(
      flexFlag,
      newCall(
        ident("writeCompactArrayLen"), writerSym, newCall(ident("len"), sourceExpr)
      ),
    ),
    nnkElse.newTree(
      newCall(
        ident("writeArrayLen"),
        writerSym,
        newCall(ident("int32"), newCall(ident("len"), sourceExpr)),
      )
    ),
  )

proc buildNullArrayLenWriteStmt(writerSym, flexFlag: NimNode): NimNode {.compileTime.} =
  ## Emits the null-array length marker (int32 -1 in legacy, uvarint(0) in
  ## compact). Used for the `isNone` branch of nullable arrays.
  if flexFlag == nil:
    return newCall(ident("writeArrayLen"), writerSym, newLit(-1'i32))
  nnkIfStmt.newTree(
    nnkElifBranch.newTree(
      flexFlag, newCall(ident("writeCompactArrayLen"), writerSym, newLit(-1))
    ),
    nnkElse.newTree(newCall(ident("writeArrayLen"), writerSym, newLit(-1'i32))),
  )

proc buildStructDecodeBlock(
  regularFields, taggedFields: seq[FieldDef],
  target, readerSym, versionSym, flexFlag: NimNode,
): NimNode {.compileTime.}

proc buildDiscardDecodeStmt(
    f: FieldDef, readerSym, flexFlag: NimNode
): NimNode {.compileTime.} =
  ## Reads and discards `f`: the reader must advance but the Nim type has no
  ## field. Scalars only; `partitionTagFields` enforces that at compile time.
  doAssert f.kind != fkArray,
    "buildDiscardDecodeStmt: fkArray not supported (field '" & f.name & "')"
  # Strip nimType so the discarded read skips the distinct-alias conversion.
  let plain = FieldDef(kind: f.kind, taggedId: -1)
  nnkDiscardStmt.newTree(readCallForField(plain, readerSym, flexFlag))

const arrayInitialCap = 1024
  ## Capacity cap for a generated array decode, in elements: the declared count is
  ## reserved only up to this many elements, and the sequence grows as the loop fills
  ## it, so a lying length prefix cannot turn a few bytes of frame into hundreds of
  ## megabytes of live allocation. The element count is already bounded by
  ## `checkArrayLenFitsBuffer`, and a long array still reaches its final size by
  ## amortised doubling. The same 1024 cap the RecordBatch record loop uses.

proc minWireBytesScalar(kind: FieldKind, flexible: bool): int {.compileTime.} =
  ## Fewest wire bytes a scalar of this kind can occupy; a safe lower bound, not an
  ## estimate. Flexible versions keep fixed-width ints, so the 1-byte figures below are
  ## a deliberate floor for them; strings/bytes bottom out at their unsigned varint
  ## length prefix, since zero length is legal, and a non-flexible schema has only
  ## fixed-width legacy encodings.
  ## `flexible` marks a schema flexible at SOME version whose codec also decodes
  ## legacy ones, so the varint width is the safe bound for both: it may under-count
  ## a legacy decode (int32 = 4 bytes), which is a missed tightening, never a false
  ## rejection.
  case kind
  of fkUuid:
    16
  # KIP-516: fixed 16 bytes in both encodings
  of fkVarInt, fkVarString, fkNullableVarBytes:
    1
  # varint framing, either encoding
  of fkNullableString, fkNullableBytes:
    1
  # These kinds are nullable by construction: the cheapest encoding is a single
  # null marker, and the 2-byte legacy length prefix of a non-null value would
  # over-estimate by one and reject a valid response whose buffer is short.
  of fkInt8, fkBool:
    1
  of fkInt16:
    (if flexible: 1 else: 2)
  of fkInt32:
    (if flexible: 1 else: 4)
  of fkInt64:
    (if flexible: 1 else: 8)
  of fkString, fkBytes:
    (if flexible: 1 else: 2)
  # length prefix, zero-length payload
  else:
    1 # a kind added later: assume the weakest, never reject valid data

proc isUnconditional(f: FieldDef): bool {.compileTime.} =
  ## True when the field is on the wire at every version (`sinceVersion` and
  ## `untilVersion` both -1). Only these may feed a minimum-size computation, since
  ## a version-gated field could be absent and inflate the divisor past valid data.
  f.sinceVersion < 0 and f.untilVersion < 0

proc minWireBytesField(f: FieldDef, flexible: bool): int {.compileTime.} =
  ## Fewest wire bytes one *unconditional* occurrence of `f` can occupy, null
  ## marker and inline tag included. A struct sums only its unconditional fields;
  ## a nested array contributes its length prefix plus the cheapest element. Every
  ## fallback returns the weakest possible value, so an underestimate costs a
  ## missed tightening and never a false rejection.
  result = 0
  if f.kind == fkArray:
    if f.nullable:
      # The null marker REPLACES the length prefix rather than being paid
      # alongside it; adding both would reject a short but valid response.
      return 1
    # Length prefix ONLY: an embedded array may be empty. Adding the cheapest
    # ELEMENT would over-estimate — a one-element `topics` array whose single
    # topic has no partitions costs 8 bytes on the wire, not 12 — and reject it.
    result += (if flexible: 1 else: 2)
  elif f.nested.len > 0:
    for sub in f.nested:
      if isUnconditional(sub):
        result += minWireBytesField(sub, flexible)
    if flexible:
      inc result # tagged-fields tag count, zero when there are none
  else:
    result = minWireBytesScalar(f.kind, flexible)
  if f.nullable:
    inc result # 0/1 null marker, both encodings
  if f.taggedId >= 0:
    inc result # inline tag on the wire, before the value

proc minWirePerArrayElement(f: FieldDef, flexible: bool): int {.compileTime.} =
  ## The guard divisor for ONE element of array field `f`: the reader consumed the
  ## array's length prefix before the guard runs, so it is not part of this, and
  ## dividing by the prefix instead of the payload would tighten far less than
  ## intended. A scalar element keeps the historical 1-byte floor — true for a compact
  ## int or a zero-length string's prefix, deliberately weaker than the legacy fixed
  ## width a non-flexible int32/int64 still divides by (4/8); a struct element gets its
  ## computed minimum, which is where the tightening lands — `FetchPartitionResp` is
  ## 88 bytes in memory, 27+ on the wire.
  if f.nested.len > 0:
    var total = 0
    for sub in f.nested:
      if isUnconditional(sub):
        total += minWireBytesField(sub, flexible)
    if flexible:
      inc total # tagged-fields tag count, zero when there are none
    max(1, total)
  else:
    max(1, minWireBytesScalar(f.elementKind, flexible))

proc buildFieldDecodeStmt(
    f: FieldDef, target, readerSym, versionSym, flexFlag: NimNode
): NimNode {.compileTime.} =
  ## Bare decode statement for one regular (non-tag-resident) field; the caller
  ## wraps it in version gates. `target` is the field lvalue, not the struct.
  if f.kind != fkArray:
    return newAssignment(target, readCallForField(f, readerSym, flexFlag))

  let lenSym = genSym(nskVar, "arrLen")
  let idxSym = genSym(nskForVar, "i")
  let isStructArray = f.nested.len > 0
  let elementTypeNode =
    if isStructArray:
      ident(f.nestedTypeName)
    else:
      primitiveTypeNode(f.elementKind)
  # Nullable arrays need a temporary because Option[seq[X]].get returns by value
  # and cannot be indexed; non-nullable populate the target directly.
  let seqLvalue =
    if f.nullable:
      genSym(nskVar, "arrTmp")
    else:
      target
  # Decode each element into a local, then append it: at iteration `i` the growing
  # sequence's length is `i`, so writing to `seqLvalue[i]` would be out of bounds.
  let elemVar = genSym(nskVar, "elem")

  var loopBody = newStmtList()
  loopBody.add newVarStmt(
    elemVar, newCall(ident("default"), copyNimTree(elementTypeNode))
  )
  if isStructArray:
    let (regs, tags) = partitionTagFields(f.nested)
    loopBody.add(
      buildStructDecodeBlock(regs, tags, elemVar, readerSym, versionSym, flexFlag)
    )
  else:
    # A minimal FieldDef drives readCallForField to the right primitive (and
    # flex-compact branch).
    let elemField = FieldDef(kind: f.elementKind)
    loopBody.add newAssignment(
      elemVar, readCallForField(elemField, readerSym, flexFlag)
    )
  # Append only once the element's bytes are read, so the allocation tracks the
  # decode rather than the length prefix: the half that stops a lying count from
  # reserving memory the buffer could never fill.
  loopBody.add newCall(
    nnkDotExpr.newTree(seqLvalue, ident("add")), copyNimTree(elemVar)
  )

  let lenInit =
    buildArrayLenReadStmt(lenSym, readerSym, flexFlag, f.nullable, f.arrayCountKind)
  # Reject an element count the remaining bytes cannot hold BEFORE anything is
  # reserved. The divisor is the element's compile-time minimum wire size: 1 for a
  # scalar (unchanged behaviour), the real minimum for a struct (`FetchPartitionResp`
  # guards 24 bytes, 88 in memory). The capped allocation (see arrayInitialCap)
  # and the append-as-you-go loop are the other half of the defence.
  let minWire = minWirePerArrayElement(f, flexFlag != nil)
  let lenGuard =
    newCall(ident("checkArrayLenFitsBuffer"), readerSym, lenSym, newLit(minWire))
  let capExpr = nnkCall.newTree(
    ident("min"), newCall(ident("int"), lenSym), newLit(arrayInitialCap)
  )
  let newSeqOfCapCall =
    newCall(nnkBracketExpr.newTree(ident("newSeqOfCap"), elementTypeNode), capExpr)
  let buildSeqStmt =
    if f.nullable:
      newStmtList(
        lenGuard,
        newVarStmt(seqLvalue, newSeqOfCapCall),
        nnkForStmt.newTree(idxSym, infix(newLit(0), "..<", lenSym), loopBody),
        newAssignment(target, newCall(ident("some"), seqLvalue)),
      )
    else:
      newStmtList(
        lenGuard,
        newAssignment(target, newSeqOfCapCall),
        nnkForStmt.newTree(idxSym, infix(newLit(0), "..<", lenSym), loopBody),
      )

  if f.nullable:
    let elemTypeForNone =
      if isStructArray:
        ident(f.nestedTypeName)
      else:
        primitiveTypeNode(f.elementKind)
    let noneBranch = newAssignment(
      target,
      newCall(
        nnkBracketExpr.newTree(
          ident("none"), nnkBracketExpr.newTree(ident("seq"), elemTypeForNone)
        )
      ),
    )
    newStmtList(
      lenInit,
      nnkIfStmt.newTree(
        nnkElifBranch.newTree(infix(lenSym, "<", newLit(0'i32)), noneBranch),
        nnkElse.newTree(buildSeqStmt),
      ),
    )
  else:
    newStmtList(lenInit, buildSeqStmt)

proc buildTaggedFieldsDecode(
    taggedFields: seq[FieldDef], target, readerSym, versionSym, flexFlag: NimNode
): NimNode {.compileTime.} =
  ## Flex-only block reading the tagged-fields trailer. Known tags populate the
  ## matching Option[...] field; unknown tags and tags received outside their
  ## declared version range are skipped.
  let countSym = genSym(nskLet, "tagCount")
  let iterSym = genSym(nskForVar, "_")
  let tagIdSym = genSym(nskLet, "tagId")
  let payloadLenSym = genSym(nskLet, "payloadLen")
  let endPosSym = genSym(nskLet, "endPos")

  # The discriminant must be `cast`, not `int64(tagId)`: a hostile tagId above
  # int64.high would raise RangeDefect under the checked conversion, while a cast
  # lands it on a negative int64 matching no branch → `else: skip`.
  var caseStmt = nnkCaseStmt.newTree(nnkCast.newTree(ident("int64"), tagIdSym))
  let skipCall = newCall(ident("skip"), readerSym, newCall(ident("int"), payloadLenSym))
  for f in taggedFields:
    let readPayload = readTagPayloadCall(f, readerSym)
    let assignSome = newAssignment(
      newDotExpr(target, ident(f.name)), newCall(ident("some"), readPayload)
    )
    # Window the read to the declared payload: a known tag with a shorter payload
    # would otherwise absorb the next tag's bytes (the clamp below restores the
    # position, but the value stays polluted). Reject the overrun as malformed.
    let overshootGuard = nnkIfStmt.newTree(
      nnkElifBranch.newTree(
        infix(newDotExpr(readerSym, ident("pos")), ">", endPosSym),
        protocolErrorRaise(
          infix(
            infix(
              newLit("tagged field " & f.name & " overran its declared payload: "),
              "&",
              newCall(ident("$"), newDotExpr(readerSym, ident("pos"))),
            ),
            "&",
            infix(newLit(" > "), "&", newCall(ident("$"), endPosSym)),
          )
        ),
      )
    )
    let guardedRead = nnkStmtList.newTree(assignSome, overshootGuard)
    let gate = versionGateCondition(f.sinceVersion, f.untilVersion, versionSym)
    let branchBody =
      if gate == nil:
        guardedRead
      else:
        nnkIfStmt.newTree(
          nnkElifBranch.newTree(gate, guardedRead), nnkElse.newTree(skipCall)
        )
    caseStmt.add nnkOfBranch.newTree(newLit(int64(f.taggedId)), branchBody)
  caseStmt.add nnkElse.newTree(skipCall)

  # Bound the payload length to the bytes remaining BEFORE computing endPos: an
  # unchecked huge length wraps `int(payloadLen)` negative and drives `r.pos` past
  # `checkRemaining` — a memory-unsafe read. Same bound as the standalone tag skippers.
  let payloadGuard = nnkIfStmt.newTree(
    nnkElifBranch.newTree(
      infix(
        payloadLenSym,
        ">",
        newCall(ident("uint64"), newDotExpr(readerSym, ident("remaining"))),
      ),
      protocolErrorRaise(
        infix(
          infix(
            newLit("tagged field payload length "),
            "&",
            newCall(ident("$"), payloadLenSym),
          ),
          "&",
          newLit(" exceeds remaining buffer"),
        )
      ),
    )
  )

  let loopBody = newStmtList(
    newLetStmt(tagIdSym, newCall(ident("decodeUvarint"), readerSym)),
    newLetStmt(payloadLenSym, newCall(ident("decodeUvarint"), readerSym)),
    payloadGuard,
    newLetStmt(
      endPosSym,
      infix(
        newDotExpr(readerSym, ident("pos")), "+", newCall(ident("int"), payloadLenSym)
      ),
    ),
    caseStmt,
    # Defensive: clamp to the declared payload end so a misread tag cannot desync the trailer.
    newAssignment(newDotExpr(readerSym, ident("pos")), endPosSym),
  )

  # Cap the entry count to the same sanity limit the standalone skippers use so
  # a hostile count cannot drive a pathological loop.
  let countGuard = nnkIfStmt.newTree(
    nnkElifBranch.newTree(
      infix(countSym, ">", newCall(ident("uint64"), newLit(10_000))),
      protocolErrorRaise(
        infix(
          infix(newLit("tagged-fields count "), "&", newCall(ident("$"), countSym)),
          "&",
          newLit(" exceeds sanity limit 10000"),
        )
      ),
    )
  )

  let trailerBlock = newStmtList(
    newLetStmt(countSym, newCall(ident("decodeUvarint"), readerSym)),
    countGuard,
    nnkForStmt.newTree(
      iterSym, infix(newLit(0), "..<", newCall(ident("int"), countSym)), loopBody
    ),
  )
  nnkIfStmt.newTree(nnkElifBranch.newTree(flexFlag, trailerBlock))

proc buildStructDecodeBlock(
    regularFields, taggedFields: seq[FieldDef],
    target, readerSym, versionSym, flexFlag: NimNode,
): NimNode {.compileTime.} =
  ## One struct's worth of decode statements into `target` (the struct lvalue).
  ## Used for the top-level proc body and each struct-array loop iteration.
  result = newStmtList()
  for f in regularFields:
    let stmt =
      if f.discardOnRead:
        buildDiscardDecodeStmt(f, readerSym, flexFlag)
      else:
        let fieldTarget = newDotExpr(target, ident(f.name))
        buildFieldDecodeStmt(f, fieldTarget, readerSym, versionSym, flexFlag)
    let gate = versionGateCondition(f.sinceVersion, f.untilVersion, versionSym)
    result.add wrapInVersionGate(stmt, gate)
  if flexFlag != nil:
    result.add buildTaggedFieldsDecode(
      taggedFields, target, readerSym, versionSym, flexFlag
    )

proc buildStructEncodeBlock(
  regularFields, taggedFields: seq[FieldDef],
  source, writerSym, versionSym, flexFlag: NimNode,
): NimNode {.compileTime.}

proc buildDiscardEncodeStmt(
    f: FieldDef, writerSym, flexFlag: NimNode
): NimNode {.compileTime.} =
  ## Writes a zero-value placeholder for `f` (the Nim type has no field, but the wire
  ## encoding must advance through this slot). Only for server-side payload synthesis
  ## (fake brokers, round-trip tests); real clients never encode response-shaped messages.
  doAssert f.kind != fkArray,
    "buildDiscardEncodeStmt: fkArray not supported (field '" & f.name & "')"
  let zeroExpr = zeroExprForField(f.kind)
  # Strip nimType: a raw zero must not take the distinct-alias conversion path.
  let plain = FieldDef(kind: f.kind, taggedId: -1)
  writeCallForField(plain, writerSym, zeroExpr, flexFlag)

proc buildFieldEncodeStmt(
    f: FieldDef, source, writerSym, versionSym, flexFlag: NimNode
): NimNode {.compileTime.} =
  if f.kind != fkArray:
    return writeCallForField(f, writerSym, source, flexFlag)

  let idxSym = genSym(nskForVar, "i")
  let isStructArray = f.nested.len > 0
  # Nullable arrays iterate `source.get` (inner seq); non-nullable iterate `source`.
  let seqExpr =
    if f.nullable:
      newCall(ident("get"), source)
    else:
      source
  let elemRef = nnkBracketExpr.newTree(seqExpr, idxSym)

  var loopBody = newStmtList()
  if isStructArray:
    let (regs, tags) = partitionTagFields(f.nested)
    loopBody =
      buildStructEncodeBlock(regs, tags, elemRef, writerSym, versionSym, flexFlag)
  else:
    let elemField = FieldDef(kind: f.elementKind)
    loopBody.add writeCallForField(elemField, writerSym, elemRef, flexFlag)

  let lenStmt = buildArrayLenWriteStmt(writerSym, seqExpr, flexFlag, f.arrayCountKind)
  let loopStmt = nnkForStmt.newTree(
    idxSym, infix(newLit(0), "..<", newCall(ident("len"), seqExpr)), loopBody
  )
  let writeBody = newStmtList(lenStmt, loopStmt)

  if f.nullable:
    let nullStmt = buildNullArrayLenWriteStmt(writerSym, flexFlag)
    nnkIfStmt.newTree(
      nnkElifBranch.newTree(newCall(ident("isSome"), source), writeBody),
      nnkElse.newTree(nullStmt),
    )
  else:
    writeBody

proc buildTaggedFieldsEncode(
    taggedFields: seq[FieldDef], source, writerSym, versionSym, flexFlag: NimNode
): NimNode {.compileTime.} =
  ## Flex-only block serializing the tagged-fields trailer. Entries are staged
  ## into a temp writer so the leading count only counts populated entries.
  let entriesSym = genSym(nskVar, "tagEntries")
  let countSym = genSym(nskVar, "tagCount")

  var body = newStmtList(
    newVarStmt(entriesSym, newCall(ident("initKafkaWriter"))),
    newVarStmt(countSym, newLit(0)),
  )

  for f in taggedFields:
    let fieldExpr = newDotExpr(source, ident(f.name))
    let payloadSym = genSym(nskVar, "tagPayload")
    let inner = newCall(ident("get"), fieldExpr)
    let writePayload = writeTagPayloadCall(f, payloadSym, inner)
    var emit = newStmtList(
      newCall(ident("encodeUvarint"), entriesSym, newLit(uint64(f.taggedId))),
      newVarStmt(payloadSym, newCall(ident("initKafkaWriter"))),
      writePayload,
      newCall(
        ident("encodeUvarint"),
        entriesSym,
        newCall(
          ident("uint64"),
          newDotExpr(newDotExpr(payloadSym, ident("buf")), ident("len")),
        ),
      ),
      newCall(
        ident("add"),
        newDotExpr(entriesSym, ident("buf")),
        newDotExpr(payloadSym, ident("buf")),
      ),
      newCall(ident("inc"), countSym),
    )
    let presence = newCall(ident("isSome"), fieldExpr)
    let gate = versionGateCondition(f.sinceVersion, f.untilVersion, versionSym)
    let cond =
      if gate == nil:
        presence
      else:
        infix(gate, "and", presence)
    body.add nnkIfStmt.newTree(nnkElifBranch.newTree(cond, emit))

  body.add newCall(
    ident("encodeUvarint"), writerSym, newCall(ident("uint64"), countSym)
  )
  body.add newCall(
    ident("add"),
    newDotExpr(writerSym, ident("buf")),
    newDotExpr(entriesSym, ident("buf")),
  )

  # Wrap in a block so the staging symbols are scoped, and gate on flex.
  nnkIfStmt.newTree(nnkElifBranch.newTree(flexFlag, newBlockStmt(body)))

proc buildStructEncodeBlock(
    regularFields, taggedFields: seq[FieldDef],
    source, writerSym, versionSym, flexFlag: NimNode,
): NimNode {.compileTime.} =
  result = newStmtList()
  for f in regularFields:
    let stmt =
      if f.discardOnRead:
        # No `source.<name>` — emit a placeholder so the payload still parses.
        buildDiscardEncodeStmt(f, writerSym, flexFlag)
      else:
        let fieldExpr = newDotExpr(source, ident(f.name))
        buildFieldEncodeStmt(f, fieldExpr, writerSym, versionSym, flexFlag)
    let gate = versionGateCondition(f.sinceVersion, f.untilVersion, versionSym)
    result.add wrapInVersionGate(stmt, gate)
  if flexFlag != nil:
    result.add buildTaggedFieldsEncode(
      taggedFields, source, writerSym, versionSym, flexFlag
    )

proc buildRecordBatchV2EncodeBody(
    valueSym, writerSym: NimNode
): NimNode {.compileTime.} =
  ## Body of `encode(w, batch: RecordBatch)` for the v2 record-batch format
  ## (KIP-98). Computed fields (`batchLength`, `magic`, `crc`, `lastOffsetDelta`,
  ## `maxTimestamp`, `numRecords`) are derived here, overriding caller values; the
  ## codec helpers (`crc32c`, `compressRecords`, ...) must be in scope at the call site.
  result = quote:
    if `valueSym`.records.len > maxWireArrayLen:
      raise newException(
        KafkaProtocolError,
        "RecordBatch record count " & $`valueSym`.records.len &
          " exceeds maxWireArrayLen " & $maxWireArrayLen,
      )
    # Per KIP-98 lastOffsetDelta is `max(record.offset) - baseOffset`, computed
    # from the actual record offsets so the header cannot disagree with the payload
    # (each record's offsetDelta is emitted from `record.offset - baseOffset`).
    var lastOffsetDelta = 0'i32
    for rec in `valueSym`.records:
      let d = int32(rec.offset - `valueSym`.baseOffset)
      if d > lastOffsetDelta:
        lastOffsetDelta = d
    var maxTs = `valueSym`.firstTimestamp
    for rec in `valueSym`.records:
      if rec.timestamp > maxTs:
        maxTs = rec.timestamp
    let codecKind = compressionFromAttributes(`valueSym`.attributes)
    # Emit the records section in place: uncompressed writes straight into
    # writerSym, compressed goes through a temp writer + splice, avoiding a copy.
    let batchStart = `writerSym`.buf.len
    `writerSym`.writeInt64(`valueSym`.baseOffset)
    let batchLenPos = `writerSym`.buf.len
    `writerSym`.writeInt32(0'i32) # batchLength placeholder
    let batchLenStart = `writerSym`.buf.len
    `writerSym`.writeInt32(`valueSym`.partitionLeaderEpoch)
    `writerSym`.writeInt8(2'i8) # magic
    let crcPos = `writerSym`.buf.len
    `writerSym`.writeInt32(0'i32) # crc placeholder
    let crcStart = `writerSym`.buf.len
    `writerSym`.writeInt16(`valueSym`.attributes)
    `writerSym`.writeInt32(lastOffsetDelta)
    `writerSym`.writeInt64(`valueSym`.firstTimestamp)
    `writerSym`.writeInt64(maxTs)
    `writerSym`.writeInt64(`valueSym`.producerId)
    `writerSym`.writeInt16(`valueSym`.producerEpoch)
    `writerSym`.writeInt32(`valueSym`.baseSequence)
    `writerSym`.writeInt32(int32(`valueSym`.records.len))
    let recordsStart = `writerSym`.buf.len
    # KIP-98 prefixes each record body with its zigzag-varint length; encode
    # the body in-place then splice the varint via moveMem.
    if codecKind == ccNone:
      for rec in `valueSym`.records:
        let bodyStart = `writerSym`.buf.len
        encode(`writerSym`, rec, `valueSym`.baseOffset, `valueSym`.firstTimestamp)
        let bodyLen = `writerSym`.buf.len - bodyStart
        var varintBuf {.noinit.}: array[10, byte]
        let varintLen = encodeVarintTo(varintBuf, int64(bodyLen))
        `writerSym`.buf.setLen(`writerSym`.buf.len + varintLen)
        if bodyLen > 0:
          moveMem(
            addr `writerSym`.buf[bodyStart + varintLen],
            addr `writerSym`.buf[bodyStart],
            bodyLen,
          )
        copyMem(addr `writerSym`.buf[bodyStart], addr varintBuf[0], varintLen)
    else:
      var recsW = initKafkaWriter()
      for rec in `valueSym`.records:
        let bodyStart = recsW.buf.len
        encode(recsW, rec, `valueSym`.baseOffset, `valueSym`.firstTimestamp)
        let bodyLen = recsW.buf.len - bodyStart
        var varintBuf {.noinit.}: array[10, byte]
        let varintLen = encodeVarintTo(varintBuf, int64(bodyLen))
        recsW.buf.setLen(recsW.buf.len + varintLen)
        if bodyLen > 0:
          moveMem(
            addr recsW.buf[bodyStart + varintLen], addr recsW.buf[bodyStart], bodyLen
          )
        copyMem(addr recsW.buf[bodyStart], addr varintBuf[0], varintLen)
      let compressed = compressRecords(recsW.buf, codecKind)
      `writerSym`.buf.add(compressed)
    # Encode-side mirror of the decoder's size guards: a pathological batch would
    # otherwise overflow the int32 batchLength into an uncatchable RangeDefect.
    let recordsBytesLen = `writerSym`.buf.len - recordsStart
    if recordsBytesLen > maxWireBytesLen:
      `writerSym`.buf.setLen(batchStart)
      raise newException(
        KafkaProtocolError,
        "RecordBatch records section " & $recordsBytesLen & " exceeds maxWireBytesLen " &
          $maxWireBytesLen,
      )
    let batchLen = int32(`writerSym`.buf.len - batchLenStart)
    let batchLenBE = toBytesBE(cast[uint32](batchLen))
    copyMem(addr `writerSym`.buf[batchLenPos], unsafeAddr batchLenBE[0], 4)
    let crcVal = crc32c(`writerSym`.buf.toOpenArray(crcStart, `writerSym`.buf.len - 1))
    let crcBE = toBytesBE(cast[uint32](crcVal))
    copyMem(addr `writerSym`.buf[crcPos], unsafeAddr crcBE[0], 4)

proc buildRecordBatchV2DecodeBody(
    typeIdent, readerSym, resultSym: NimNode
): NimNode {.compileTime.} =
  ## Body of `decode(r, typedesc[RecordBatch])` for the v2 wire format: validates
  ## `magic == 2`, verifies CRC32C over the post-CRC bytes, optionally decompresses
  ## the records section, and populates each Record inside a varint-framed window.
  result = quote:
    let startPos = `readerSym`.pos
    `resultSym`.baseOffset = `readerSym`.readInt64()
    `resultSym`.batchLength = `readerSym`.readInt32()
    # Need at least partitionLeaderEpoch(4) + magic(1) below to decide v2 vs
    # legacy; a shorter batchLength would let those reads over-run the batch.
    if `resultSym`.batchLength < 5:
      raise newException(
        KafkaProtocolError, "invalid batch length: " & $`resultSym`.batchLength
      )
    `resultSym`.partitionLeaderEpoch = `readerSym`.readInt32()
    `resultSym`.magic = `readerSym`.readInt8()
    if `resultSym`.magic != 2:
      # Skip non-v2 record batches (legacy MessageSet v0/v1) rather than failing;
      # the tolerance is deliberate.
      let totalSize = 12 + int(`resultSym`.batchLength)
      let consumed = `readerSym`.pos - startPos
      let leftover = totalSize - consumed
      if leftover > 0:
        `readerSym`.skip(leftover)
      return
    `resultSym`.crc = `readerSym`.readInt32()
    let crcStart = `readerSym`.pos
    let batchEnd = startPos + 12 + int(`resultSym`.batchLength)
    # `limit` (when set by an outer window, e.g. per-partition fetch bytes) is the
    # real end for this batch: `data.len` may extend across concatenated batches,
    # so gating on it lets a forged batchLength pull CRC input from the next batch.
    let effEnd =
      if `readerSym`.limit <= 0:
        `readerSym`.data.len
      else:
        min(`readerSym`.data.len, `readerSym`.limit)
    # The 40-byte fixed v2 header after the CRC must fit between crcStart and
    # batchEnd. CRC32C is not cryptographic, so a forged batchLength leaving
    # batchEnd inside that header would pass a mere `batchEnd > crcStart` check.
    if batchEnd > effEnd or batchEnd < crcStart + 40:
      raise newException(
        KafkaProtocolError, "invalid record batch length: " & $`resultSym`.batchLength
      )
    let expectedCrc = cast[uint32](`resultSym`.crc)
    let actualCrc = crc32c(`readerSym`.data.toOpenArray(crcStart, batchEnd - 1))
    if actualCrc != expectedCrc:
      raise newException(
        KafkaProtocolError,
        "CRC32C mismatch in record batch at baseOffset " & $`resultSym`.baseOffset &
          ": expected " & $expectedCrc & ", got " & $actualCrc,
      )
    `resultSym`.attributes = `readerSym`.readInt16()
    `resultSym`.lastOffsetDelta = `readerSym`.readInt32()
    `resultSym`.firstTimestamp = `readerSym`.readInt64()
    `resultSym`.maxTimestamp = `readerSym`.readInt64()
    `resultSym`.producerId = `readerSym`.readInt64()
    `resultSym`.producerEpoch = `readerSym`.readInt16()
    `resultSym`.baseSequence = `readerSym`.readInt32()
    let numRecords = `readerSym`.readInt32()
    if numRecords < 0 or numRecords > int32(maxWireArrayLen):
      raise
        newException(KafkaProtocolError, "invalid record batch count: " & $numRecords)
    let codecKind = compressionFromAttributes(`resultSym`.attributes)
    template loadRecords(rdr: var KafkaReader, recordsEnd: int) =
      # Allocate incrementally rather than `newSeq[Record](numRecords)`: a header
      # may pass the `numRecords <= maxWireArrayLen` bound yet describe a tiny body,
      # so pre-sizing could allocate millions of elements before the first cheap
      # per-record check rejects the batch. `recordsEnd` bounds the section (batchEnd
      # uncompressed, decompressed length compressed), so a record claiming bytes of a
      # following batch is rejected here.
      `resultSym`.records = newSeqOfCap[Record](min(int(numRecords), 1024))
      for i in 0 ..< int(numRecords):
        let recLen = rdr.decodeVarint()
        # recLen is a BYTE length: bound with the byte-size cap, not the
        # array-element cap (which would reject a large record under a raised
        # message.max.bytes); the recordsEnd window is the real constraint.
        if recLen < 0 or recLen > int64(maxWireBytesLen) or
            recLen > int64(recordsEnd - rdr.pos):
          raise newException(KafkaProtocolError, "invalid record length: " & $recLen)
        let recStart = rdr.pos
        let recordEnd = recStart + int(recLen)
        # Bound every read to the record window so a hostile key/value/header length
        # is rejected by checkRemaining before it can allocate against the whole
        # buffer. Restore the outer window on every exit, so a rejected record does
        # not leave the enclosing batch loop bounded to an abandoned window.
        let savedLimit = rdr.limit
        rdr.limit = recordEnd
        try:
          let rec =
            rdr.decode(Record, `resultSym`.baseOffset, `resultSym`.firstTimestamp)
          if rdr.pos > recordEnd:
            raise newException(
              KafkaProtocolError,
              "record over-read at index " & $i & ": pos " & $rdr.pos & " > recordEnd " &
                $recordEnd,
            )
          if rdr.pos < recordEnd:
            rdr.skip(recordEnd - rdr.pos)
          `resultSym`.records.add(rec)
        finally:
          rdr.limit = savedLimit

    if codecKind != ccNone:
      let headerConsumed = `readerSym`.pos - startPos
      let totalSize = 12 + int(`resultSym`.batchLength)
      # compressedLen stays >= 0 because the minimum-header guard forced batchEnd
      # to span the full 40-byte header; without it a negative value could slip
      # past checkRemaining and let toOpenArray return a malformed slice.
      let compressedLen = totalSize - headerConsumed
      `readerSym`.checkRemaining(compressedLen)
      let compressedStart = `readerSym`.pos
      `readerSym`.pos += compressedLen
      # The per-batch cap bounds one batch, but many small high-ratio batches can
      # still multiply one response's wire bytes into a huge decompressed total.
      # When the reader carries a `DecompressionBudget` (set by the fetch parser),
      # exceeding it raises KafkaDecompressionLimitError (terminal, never a skip),
      # and each batch decrements the budget by its decompressed length. The budget
      # also becomes the cap handed to the codec, so the batch is refused while it
      # decompresses instead of after it has allocated its whole expansion.
      let budgetCapped =
        `readerSym`.decompressionBudget != nil and
        `readerSym`.decompressionBudget.remaining < defaultMaxDecompressSize
      if `readerSym`.decompressionBudget != nil and
          `readerSym`.decompressionBudget.remaining <= 0:
        raise newException(
          KafkaDecompressionLimitError,
          "fetch decompression budget exhausted before record batch at baseOffset " &
            $`resultSym`.baseOffset,
        )
      var decompressed: seq[byte]
      try:
        decompressed = decompressRecords(
          `readerSym`.data.toOpenArray(
            compressedStart, compressedStart + compressedLen - 1
          ),
          codecKind,
          if budgetCapped:
            `readerSym`.decompressionBudget.remaining
          else:
            defaultMaxDecompressSize,
        )
      except KafkaError as e:
        # Only the budget makes this terminal; a batch over the per-batch cap alone
        # stays on the corrupt-batch path, which skips it and moves on.
        if budgetCapped:
          raise newException(
            KafkaDecompressionLimitError,
            "fetch decompression budget exhausted by record batch at baseOffset " &
              $`resultSym`.baseOffset & ": " & e.msg,
          )
        raise e
      if `readerSym`.decompressionBudget != nil:
        if decompressed.len > `readerSym`.decompressionBudget.remaining:
          raise newException(
            KafkaDecompressionLimitError,
            "fetch decompression budget exhausted by record batch at baseOffset " &
              $`resultSym`.baseOffset & ": need " & $decompressed.len & ", have " &
              $`readerSym`.decompressionBudget.remaining,
          )
        `readerSym`.decompressionBudget.remaining -= decompressed.len
      var dr = initKafkaReader(decompressed)
      loadRecords(dr, decompressed.len)
      # Mirror the non-compressed tail check: unread trailing bytes mean the codec
      # or the batch's `numRecords` disagreed with the compressed payload, and
      # silently tolerating that would slip garbage past the record count.
      if dr.pos != decompressed.len:
        raise newException(
          KafkaProtocolError,
          "decompressed record batch has " & $(decompressed.len - dr.pos) &
            " unread trailing byte(s) after loadRecords",
        )
    else:
      loadRecords(`readerSym`, batchEnd)
      # Framing check, mirroring the compressed branch: a well-formed batch leaves
      # the reader exactly at batchEnd. Tolerating an under-read by snapping forward
      # to batchEnd would accept a batch whose records section carries more bytes than
      # `numRecords` describes while `lastOffsetDelta` still claims the wider span, so
      # the consumer's cursor skips records no later fetch returns. Reject as corruption.
      if `readerSym`.pos > batchEnd:
        raise newException(
          KafkaProtocolError,
          "record batch over-read: pos " & $`readerSym`.pos & " > batchEnd " & $batchEnd,
        )
      if `readerSym`.pos < batchEnd:
        raise newException(
          KafkaProtocolError,
          "record batch has " & $(batchEnd - `readerSym`.pos) &
            " unread trailing byte(s) after loadRecords",
        )

proc validateContextParams(
    params: seq[ContextParam], schemaName: string
) {.compileTime.} =
  for i, param in params:
    doAssert param.name.len > 0,
      "schema '" & schemaName & "' contextParams[" & $i & "] has an empty name"
    doAssert param.typ.len > 0,
      "schema '" & schemaName & "' contextParam '" & param.name & "' has an empty type"
    for j in 0 ..< i:
      doAssert params[j].name != param.name,
        "schema '" & schemaName & "' has duplicate contextParam '" & param.name & "'"

proc validateFieldDefinitions(
    fields: seq[FieldDef],
    ctx: seq[ContextParam],
    maxVersion, flexibleSince: int,
    envelope: EnvelopeKind,
    schemaName, path: string,
) {.compileTime.} =
  ## Validate one struct level and all nested struct arrays. A single recursive
  ## entry point, so every MessageDef option is checked by the same contract —
  ## including options only meaningful for arrays or flexible versions.
  for i, f in fields:
    let fieldPath =
      if path.len == 0:
        f.name
      else:
        path & "." & f.name
    doAssert f.name.len > 0,
      "schema '" & schemaName & "' has a field with an empty name at '" & path & "'"
    for j in 0 ..< i:
      doAssert fields[j].name != f.name,
        "schema '" & schemaName & "' has duplicate field '" & fieldPath & "'"

    # These values are narrowed to int16 in generated version gates. Reject
    # out-of-range constants before that conversion can produce a bare defect.
    doAssert f.sinceVersion >= -1 and f.sinceVersion <= int16.high,
      "schema '" & schemaName & "' field '" & fieldPath &
        "' sinceVersion must be in -1 .. " & $int16.high & ", got " & $f.sinceVersion
    doAssert f.untilVersion >= -1 and f.untilVersion <= int16.high,
      "schema '" & schemaName & "' field '" & fieldPath &
        "' untilVersion must be in -1 .. " & $int16.high & ", got " & $f.untilVersion
    doAssert f.sinceVersion <= f.untilVersion or f.untilVersion == -1,
      "schema '" & schemaName & "' field '" & fieldPath & "' sinceVersion (" &
        $f.sinceVersion & ") must be <= untilVersion (" & $f.untilVersion & ")"
    if f.sinceVersion >= 0:
      doAssert f.sinceVersion <= maxVersion,
        "schema '" & schemaName & "' field '" & fieldPath & "' sinceVersion (" &
          $f.sinceVersion & ") exceeds schema maxVersion (" & $maxVersion & ")"
    # An untilVersion above maxVersion is harmless (every modeled version lies in
    # the interval); the real failures are inverted or unrepresentable ranges.

    doAssert f.taggedId >= -1,
      "schema '" & schemaName & "' field '" & fieldPath &
        "' taggedId must be -1 or non-negative, got " & $f.taggedId
    let spec = fieldKindSpec(f.kind)
    doAssert f.nimType.len == 0 or spec.wireType.len > 0,
      "schema '" & schemaName & "' field '" & fieldPath & "' nimType overrides are only " &
        "supported for integer wire kinds (got " & $f.kind & ")"
    doAssert f.nimTypeCtor.len == 0 or f.nimType.len > 0,
      "schema '" & schemaName & "' field '" & fieldPath &
        "' nimTypeCtor requires nimType"
    doAssert f.nimTypeCtor.len == 0 or spec.wireType.len > 0,
      "schema '" & schemaName & "' field '" & fieldPath &
        "' nimTypeCtor is only supported for integer wire kinds (got " & $f.kind & ")"

    if f.deltaFrom.len > 0:
      doAssert spec.deltaCapable,
        "schema '" & schemaName & "' field '" & fieldPath &
          "' deltaFrom is only supported on delta-capable field kinds (got " & $f.kind &
          ")"
      var found = false
      for param in ctx:
        if param.name == f.deltaFrom:
          found = true
          break
      doAssert found,
        "schema '" & schemaName & "' field '" & fieldPath & "' declares deltaFrom '" &
          f.deltaFrom & "' but no contextParams entry matches"

    if f.kind != fkArray:
      doAssert f.nested.len == 0 and f.nestedTypeName.len == 0,
        "schema '" & schemaName & "' field '" & fieldPath &
          "' uses nested array metadata on a non-array kind"
      doAssert f.elementTypeName.len == 0,
        "schema '" & schemaName & "' field '" & fieldPath &
          "' uses elementTypeName on a non-array kind"
      doAssert f.elementKind == fkInt8,
        "schema '" & schemaName & "' field '" & fieldPath &
          "' sets elementKind on a non-array kind"
      doAssert not f.nullable,
        "schema '" & schemaName & "' field '" & fieldPath &
          "' sets nullable on a non-array kind"
      doAssert f.arrayCountKind == ackInt32,
        "schema '" & schemaName & "' field '" & fieldPath &
          "' sets arrayCountKind on a non-array kind"
      doAssert not f.discardOnRead or f.taggedId < 0,
        "schema '" & schemaName & "' field '" & fieldPath &
          "' cannot combine discardOnRead with taggedId"
    else:
      doAssert f.nimType.len == 0 and f.nimTypeCtor.len == 0,
        "schema '" & schemaName & "' fkArray field '" & fieldPath &
          "' cannot use nimType/nimTypeCtor"
      doAssert not f.discardOnRead,
        "schema '" & schemaName & "' fkArray field '" & fieldPath &
          "' cannot set discardOnRead"
      doAssert not (f.arrayCountKind == ackVarint and f.nullable),
        "schema '" & schemaName & "' fkArray field '" & fieldPath &
          "' cannot combine ackVarint with nullable"
      if f.nested.len > 0:
        doAssert f.nestedTypeName.len > 0,
          "schema '" & schemaName & "' fkArray field '" & fieldPath &
            "' with nested fields requires nestedTypeName"
        doAssert f.elementTypeName.len == 0,
          "schema '" & schemaName & "' fkArray field '" & fieldPath &
            "' cannot combine nested fields with elementTypeName"
        doAssert f.elementKind == fkInt8,
          "schema '" & schemaName & "' fkArray field '" & fieldPath &
            "' sets elementKind on a nested struct array; it is ignored"
        validateFieldDefinitions(
          f.nested, ctx, maxVersion, flexibleSince, envelope, schemaName, fieldPath
        )
      elif f.elementTypeName.len > 0:
        doAssert envelope == ekRecordBatchV2,
          "schema '" & schemaName & "' fkArray field '" & fieldPath &
            "' has elementTypeName '" & f.elementTypeName &
            "' but only the ekRecordBatchV2 envelope supports external element types"
        doAssert f.elementTypeName == "Record",
          "schema '" & schemaName & "' fkArray field '" & fieldPath &
            "' uses unsupported external element type '" & f.elementTypeName &
            "'; ekRecordBatchV2 only supports Record"
        doAssert f.nestedTypeName.len == 0 and not f.nullable,
          "schema '" & schemaName & "' fkArray field '" & fieldPath &
            "' external elementTypeName cannot use nestedTypeName or nullable"
        doAssert f.elementKind == fkInt8,
          "schema '" & schemaName & "' fkArray field '" & fieldPath &
            "' sets elementKind on an external element array; it is ignored"
      else:
        doAssert f.nestedTypeName.len == 0,
          "schema '" & schemaName & "' fkArray field '" & fieldPath &
            "' has nestedTypeName without nested fields"
        if f.elementKind == fkArray:
          doAssert false,
            "schema '" & schemaName & "' fkArray field '" & fieldPath &
              "' cannot use another array as its scalar elementKind"
        else:
          doAssert fieldKindSpec(f.elementKind).readPrimitive.len > 0,
            "schema '" & schemaName & "' fkArray field '" & fieldPath &
              "' has unsupported elementKind " & $f.elementKind

    if f.taggedId >= 0:
      doAssert flexibleSince >= 0,
        "schema '" & schemaName & "' field '" & fieldPath &
          "' is tag-resident but flexibleSince is -1; tagged fields require a flexible schema"
      doAssert f.untilVersion == -1 or f.untilVersion >= flexibleSince,
        "schema '" & schemaName & "' field '" & fieldPath &
          "' ends before flexibleSince (" & $flexibleSince &
          "); a tag-resident field is never encoded"
      doAssert spec.taggable,
        "schema '" & schemaName & "' field '" & fieldPath & "' kind " & $f.kind &
          " is not supported as a tag-resident field"
      for j in 0 ..< i:
        if fields[j].taggedId == f.taggedId:
          doAssert false,
            "schema '" & schemaName & "' has duplicate taggedId " & $f.taggedId &
              " at fields '" & fields[j].name & "' and '" & f.name & "'"

proc validateRecordBatchEnvelope(
    fields: seq[FieldDef], schemaName: string
) {.compileTime.} =
  ## The RecordBatch envelope is a hand-written codec rather than a generic
  ## field walker. Pin the field shape it consumes so a malformed envelope is
  ## rejected before generated code refers to a missing object member.
  const required: array[13, tuple[name: string, kind: FieldKind]] = [
    ("baseOffset", fkInt64),
    ("batchLength", fkInt32),
    ("partitionLeaderEpoch", fkInt32),
    ("magic", fkInt8),
    ("crc", fkInt32),
    ("attributes", fkInt16),
    ("lastOffsetDelta", fkInt32),
    ("firstTimestamp", fkInt64),
    ("maxTimestamp", fkInt64),
    ("producerId", fkInt64),
    ("producerEpoch", fkInt16),
    ("baseSequence", fkInt32),
    ("records", fkArray),
  ]
  for requiredField in required:
    var found = false
    for f in fields:
      if f.name == requiredField.name:
        found = true
        doAssert f.kind == requiredField.kind,
          "schema '" & schemaName & "' RecordBatch envelope field '" & f.name &
            "' must use " & $requiredField.kind & ", got " & $f.kind
        doAssert f.taggedId == -1 and not f.discardOnRead,
          "schema '" & schemaName & "' RecordBatch envelope field '" & f.name &
            "' cannot be tagged or discarded"
        doAssert f.sinceVersion == -1 and f.untilVersion == -1,
          "schema '" & schemaName & "' RecordBatch envelope field '" & f.name &
            "' must be present in every version"
        doAssert f.nimType.len == 0 and f.nimTypeCtor.len == 0,
          "schema '" & schemaName & "' RecordBatch envelope field '" & f.name &
            "' cannot use nimType/nimTypeCtor"
        break
    doAssert found,
      "schema '" & schemaName & "' RecordBatch envelope requires field '" &
        requiredField.name & "'"
  var recordsField: FieldDef
  for f in fields:
    if f.name == "records":
      recordsField = f
      break
  doAssert recordsField.elementTypeName == "Record",
    "schema '" & schemaName &
      "' RecordBatch envelope records must use elementTypeName 'Record'"

proc validateMessageDef*(def: MessageDef) {.compileTime.} =
  ## Compile-time contract for every MessageDef passed to defineKafkaMessage, kept
  ## separate from AST emission so tests reuse it and nested definitions are covered.
  doAssert def.name.len > 0, "schema name must not be empty"
  doAssert def.apiKey == -1 or def.apiKey >= 0,
    "schema '" & def.name & "' apiKey must be -1 or non-negative"
  doAssert def.maxVersion >= 0 and def.maxVersion <= int16.high,
    "schema '" & def.name & "' maxVersion must be in 0 .. " & $int16.high & ", got " &
      $def.maxVersion
  doAssert def.flexibleSince == -1 or
    (def.flexibleSince >= 0 and def.flexibleSince <= def.maxVersion),
    "schema '" & def.name & "' flexibleSince (" & $def.flexibleSince &
      ") must be -1 or in 0 .. maxVersion (" & $def.maxVersion & ")"
  validateContextParams(def.contextParams, def.name)
  validateFieldDefinitions(
    def.fields, def.contextParams, def.maxVersion, def.flexibleSince, def.envelope,
    def.name, "",
  )
  if def.envelope == ekRecordBatchV2:
    validateRecordBatchEnvelope(def.fields, def.name)

proc invalidArrayElementKindError(
    fields: seq[FieldDef], schemaName: string
): string {.compileTime.} =
  ## Detects the invalid array shape that would otherwise reach
  ## primitiveTypeNode(fkArray) during macro recovery, so the macro can stop before
  ## constructing a partial type tree.
  for f in fields:
    if f.kind == fkArray and f.nested.len == 0 and f.elementTypeName.len == 0 and
        f.elementKind == fkArray:
      return
        "schema '" & schemaName & "' fkArray field '" & f.name &
        "' cannot use another array as its scalar elementKind"
    if f.kind == fkArray and f.nested.len > 0:
      let nestedError = invalidArrayElementKindError(f.nested, schemaName)
      if nestedError.len > 0:
        return nestedError

proc validateFieldKindCoverage() {.compileTime.} =
  ## Force every enum value through the central codec table at module compile time,
  ## so a FieldKind added without a read/write/type/zero contract fails here instead
  ## of waiting for the first schema that happens to use it.
  for i in 0 .. ord(high(FieldKind)):
    let spec = fieldKindSpec(FieldKind(i))
    if FieldKind(i) == fkArray:
      doAssert spec.readPrimitive.len == 0 and spec.writePrimitive.len == 0,
        "fkArray must not expose a scalar primitive in FieldKindSpec"
    else:
      doAssert spec.readPrimitive.len > 0 and spec.writePrimitive.len > 0,
        "FieldKind " & $FieldKind(i) & " has an incomplete codec contract"
      doAssert spec.nimType.len > 0 and spec.zeroKind != fzkNone,
        "FieldKind " & $FieldKind(i) & " has no type or zero-value contract"
      discard zeroExprForField(FieldKind(i))
      discard primitiveTypeNode(FieldKind(i))

static:
  validateFieldKindCoverage()

proc nimTypeCtorChecks(
    fields: seq[FieldDef], schemaName: string
): NimNode {.compileTime.} =
  ## Emits call-site `when <nimType> is enum: {.error.}` for fields overriding
  ## `nimType` without a `nimTypeCtor`: raw `EnumType(wire)` raises uncatchable
  ## RangeDefect and breaks the KafkaError contract, while distinct aliases such as
  ## KafkaErrorCode stay allowed. The type only resolves at the expansion site.
  result = newStmtList()
  for f in fields:
    if f.nimType.len > 0 and f.nimTypeCtor.len == 0:
      let msg =
        "schema '" & schemaName & "' field '" & f.name & "' nimType '" & f.nimType &
        "' is an enum; set nimTypeCtor to a proc that raises " & "KafkaError (raw " &
        f.nimType & "(wire) raises RangeDefect)"
      result.add nnkWhenStmt.newTree(
        nnkElifBranch.newTree(
          infix(ident(f.nimType), "is", newNimNode(nnkEnumTy)),
          newStmtList(
            nnkPragma.newTree(nnkExprColonExpr.newTree(ident("error"), newLit(msg)))
          ),
        )
      )
    if f.kind == fkArray and f.nested.len > 0:
      result.add nimTypeCtorChecks(f.nested, schemaName)

proc buildVersionGuard(
    defName: string, maxVersion: int, versionSym: NimNode, direction: string
): NimNode {.compileTime.} =
  ## Single owner for the version ceiling every generated codec enforces: reject
  ## versions above the modeled maximum (negotiation normally prevents this)
  ## instead of silently using the highest known shape.
  newTree(
    nnkIfStmt,
    newTree(
      nnkElifBranch,
      infix(versionSym, ">", newLit(maxVersion)),
      newStmtList(
        protocolErrorRaise(
          newCall(
            ident("&"),
            newCall(
              ident("&"),
              newLit("schema " & defName & " cannot " & direction & " version "),
              newCall(ident("$"), versionSym),
            ),
            newLit(" (max " & $maxVersion & ")"),
          )
        )
      ),
    ),
  )

macro defineKafkaMessage*(def: static[MessageDef]): untyped =
  ## Emits the Nim type, decode proc, and encode proc for `def`:
  ##   proc decode*(r: var KafkaReader, T: typedesc[<def.name>],
  ##                <contextParams...>, version: int16 = 0): T
  ##   proc encode*(w: var KafkaWriter, value: <def.name>,
  ##                <contextParams...>, version: int16 = 0)
  ##
  ## `contextParams` are appended in declaration order before the trailing
  ## `version`, threading per-batch values (baseOffset, firstTimestamp) into nested
  ## codecs that delta-encode via `FieldDef.deltaFrom`.
  ##
  ## When `def.flexibleSince >= 0`, the procs bind `isFlexible = version >=
  ## flexibleSince` to switch string/bytes/array-length primitives between legacy
  ## and compact (KIP-482) forms and to emit/skip a tagged-fields trailer per
  ## struct; versions below `flexibleSince` use the legacy format.
  result = newStmtList()
  let arrayKindError = invalidArrayElementKindError(def.fields, def.name)
  if arrayKindError.len > 0:
    result = nnkStmtList.newTree(
      nnkPragma.newTree(
        nnkExprColonExpr.newTree(ident("error"), newLit(arrayKindError))
      )
    )
    return
  validateMessageDef(def)
  result.add nimTypeCtorChecks(def.fields, def.name)
  var typeSection = newNimNode(nnkTypeSection)

  # Nested struct types first, depth-first so inner structs are defined before any
  # outer type referencing them; scalar arrays need no nested type.
  proc collectNested(fields: seq[FieldDef], section: NimNode) {.compileTime.} =
    for f in fields:
      if f.kind == fkArray and f.nested.len > 0:
        doAssert f.nestedTypeName.len > 0,
          "fkArray field '" & f.name & "' with nested fields requires nestedTypeName"
        collectNested(f.nested, section)
        section.add objectTypeDef(f.nestedTypeName, f.nested)

  collectNested(def.fields, typeSection)

  typeSection.add objectTypeDef(def.name, def.fields)
  result.add typeSection

  let typeIdent = ident(def.name)
  let readerSym = ident("r")
  let writerSym = ident("w")
  let versionSym = ident("version")
  let resultSym = ident("result")
  let valueSym = ident("value")
  let flexFlag: NimNode =
    if def.flexibleSince >= 0:
      ident("isFlexible")
    else:
      nil
  let flexLit = newLit(int16(def.flexibleSince))

  let (topRegs, topTags) = partitionTagFields(def.fields)

  proc ctxIdentDefs(params: seq[ContextParam]): seq[NimNode] {.compileTime.} =
    for p in params:
      result.add newIdentDefs(ident(p.name), ident(p.typ), newEmptyNode())

  var decodeBody = newStmtList()
  case def.envelope
  of ekRecordBatchV2:
    decodeBody.add buildVersionGuard(def.name, def.maxVersion, versionSym, "decode")
    decodeBody.add buildRecordBatchV2DecodeBody(typeIdent, readerSym, resultSym)
  of ekNone:
    decodeBody.add buildVersionGuard(def.name, def.maxVersion, versionSym, "decode")
    let decodeInner = buildStructDecodeBlock(
      topRegs, topTags, resultSym, readerSym, versionSym, flexFlag
    )
    if flexFlag != nil:
      decodeBody.add newLetStmt(flexFlag, infix(versionSym, ">=", flexLit))
    decodeBody.add decodeInner

  var decodeParams = nnkFormalParams.newTree(typeIdent)
  decodeParams.add newIdentDefs(
    readerSym, nnkVarTy.newTree(ident("KafkaReader")), newEmptyNode()
  )
  decodeParams.add newIdentDefs(
    ident("T"), nnkBracketExpr.newTree(ident("typedesc"), typeIdent), newEmptyNode()
  )
  for d in ctxIdentDefs(def.contextParams):
    decodeParams.add d
  decodeParams.add newIdentDefs(versionSym, ident("int16"), newLit(int16(0)))
  result.add nnkProcDef.newTree(
    postfix(ident("decode"), "*"),
    newEmptyNode(),
    newEmptyNode(),
    decodeParams,
    newEmptyNode(),
    newEmptyNode(),
    decodeBody,
  )

  var encodeBody = newStmtList()
  case def.envelope
  of ekRecordBatchV2:
    encodeBody.add buildVersionGuard(def.name, def.maxVersion, versionSym, "encode")
    encodeBody.add buildRecordBatchV2EncodeBody(valueSym, writerSym)
  of ekNone:
    encodeBody.add buildVersionGuard(def.name, def.maxVersion, versionSym, "encode")
    let encodeInner = buildStructEncodeBlock(
      topRegs, topTags, valueSym, writerSym, versionSym, flexFlag
    )
    if flexFlag != nil:
      encodeBody.add newLetStmt(flexFlag, infix(versionSym, ">=", flexLit))
    encodeBody.add encodeInner

  var encodeParams = nnkFormalParams.newTree(newEmptyNode())
  encodeParams.add newIdentDefs(
    writerSym, nnkVarTy.newTree(ident("KafkaWriter")), newEmptyNode()
  )
  encodeParams.add newIdentDefs(valueSym, typeIdent, newEmptyNode())
  for d in ctxIdentDefs(def.contextParams):
    encodeParams.add d
  encodeParams.add newIdentDefs(versionSym, ident("int16"), newLit(int16(0)))
  result.add nnkProcDef.newTree(
    postfix(ident("encode"), "*"),
    newEmptyNode(),
    newEmptyNode(),
    encodeParams,
    newEmptyNode(),
    newEmptyNode(),
    encodeBody,
  )
