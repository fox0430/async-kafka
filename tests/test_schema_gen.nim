## Verifies the schema-driven message generator. The "wire decode" suites are
## fixed-payload regression tests for the schema-gen decoders.

import std/[options, strutils, unittest]

import pkg/stew/endians2

import async_kafka/errors
import async_kafka/protocol/[primitives, schema, messages, crc32c]

template expectKafkaProtocolError(body: untyped) =
  ## Shared helper for the error-path tests throughout this file; the unittest
  ## module ships `expect` but it requires a single-statement block.
  var raised = false
  try:
    body
  except KafkaProtocolError:
    raised = true
  check raised

const dummySchema = MessageDef(
  name: "DummyMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "alpha", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "beta", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
  ],
)

defineKafkaMessage(dummySchema)

suite "schema_gen: macro mechanism (dummy message)":
  test "round-trip a flat int16/int32 dummy message":
    var w = initKafkaWriter()
    w.encode(DummyMsg(alpha: 0x1234'i16, beta: 0x12345678'i32))
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyMsg)
    check decoded.alpha == 0x1234'i16
    check decoded.beta == 0x12345678'i32

  test "wire format matches manual layout":
    var w = initKafkaWriter()
    w.encode(DummyMsg(alpha: 0x1234'i16, beta: 0x12345678'i32))
    check w.buf ==
      @[
        0x12'u8,
        0x34, # alpha (BE int16)
        0x12,
        0x34,
        0x56,
        0x78, # beta  (BE int32)
      ]

const dummyUntilSchema = MessageDef(
  name: "DummyUntilMsg",
  apiKey: -1,
  maxVersion: 2,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "always", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "onlyV0", kind: fkInt16, sinceVersion: -1, untilVersion: 0),
    FieldDef(name: "v1Plus", kind: fkInt16, sinceVersion: 1, untilVersion: -1),
    FieldDef(name: "v1Only", kind: fkInt16, sinceVersion: 1, untilVersion: 1),
  ],
)

defineKafkaMessage(dummyUntilSchema)

suite "schema_gen: version range gating":
  ## Version range gating: DummyUntilMsg exercises every shape of gate.
  ## Field presence per version:
  ##
  ##   field    | sinceV | untilV | v0 | v1 | v2
  ##   ---------+--------+--------+----+----+----
  ##   always   |   -1   |   -1   |  ✓ |  ✓ |  ✓
  ##   onlyV0   |   -1   |    0   |  ✓ |    |
  ##   v1Plus   |    1   |   -1   |    |  ✓ |  ✓
  ##   v1Only   |    1   |    1   |    |  ✓ |
  ##
  ## All four fields exist in the Nim object (the type is invariant); the
  ## encoder/decoder skip any field outside its gate.

  test "v0 encode emits only fields present at v0":
    var w = initKafkaWriter()
    let msg = DummyUntilMsg(
      always: 0x0001'i16, onlyV0: 0x0002'i16, v1Plus: 0x0003'i16, v1Only: 0x0004'i16
    )
    w.encode(msg, version = 0'i16)
    check w.buf ==
      @[
        0x00'u8,
        0x01, # always
        0x00,
        0x02, # onlyV0 (untilVersion=0 → present at v0)
      ]

  test "v1 encode emits always + v1Plus + v1Only":
    var w = initKafkaWriter()
    let msg = DummyUntilMsg(
      always: 0x0001'i16, onlyV0: 0x0002'i16, v1Plus: 0x0003'i16, v1Only: 0x0004'i16
    )
    w.encode(msg, version = 1'i16)
    check w.buf ==
      @[
        0x00'u8,
        0x01, # always
        0x00,
        0x03, # v1Plus (sinceVersion=1)
        0x00,
        0x04, # v1Only (sinceVersion=1, untilVersion=1)
      ]

  test "v2 encode emits always + v1Plus only":
    var w = initKafkaWriter()
    let msg = DummyUntilMsg(
      always: 0x0001'i16, onlyV0: 0x0002'i16, v1Plus: 0x0003'i16, v1Only: 0x0004'i16
    )
    w.encode(msg, version = 2'i16)
    check w.buf ==
      @[
        0x00'u8,
        0x01, # always
        0x00,
        0x03, # v1Plus (sinceVersion=1, no untilVersion cap)
      ] # v1Only is excluded by untilVersion=1; onlyV0 by untilVersion=0.

  test "v0 round-trip preserves always + onlyV0, leaves v1 fields zero":
    var w = initKafkaWriter()
    w.encode(
      DummyUntilMsg(
        always: 0x1111'i16, onlyV0: 0x2222'i16, v1Plus: 0x3333'i16, v1Only: 0x4444'i16
      ),
      version = 0'i16,
    )
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyUntilMsg, version = 0'i16)
    check decoded.always == 0x1111'i16
    check decoded.onlyV0 == 0x2222'i16
    check decoded.v1Plus == 0'i16 # not present in v0 wire payload
    check decoded.v1Only == 0'i16
    check r.remaining == 0

  test "v2 round-trip drops untilVersion=1 field even when populated":
    var w = initKafkaWriter()
    w.encode(
      DummyUntilMsg(
        always: 0x1111'i16,
        onlyV0: 0x2222'i16,
        v1Plus: 0x3333'i16,
        v1Only: 0x4444'i16, # populated, but untilVersion=1 must drop it at v2
      ),
      version = 2'i16,
    )
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyUntilMsg, version = 2'i16)
    check decoded.always == 0x1111'i16
    check decoded.onlyV0 == 0'i16 # untilVersion=0 → not in v2 wire
    check decoded.v1Plus == 0x3333'i16
    check decoded.v1Only == 0'i16 # untilVersion=1 → not in v2 wire
    check r.remaining == 0

  test "decode above the modeled maxVersion raises KafkaProtocolError":
    # The schema stops modeling at v2; a decode at v3 used to silently fall
    # back to the highest known shape. Reject it loudly instead.
    var w = initKafkaWriter()
    w.encode(DummyUntilMsg(always: 1'i16), version = 2'i16)
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.decode(DummyUntilMsg, version = 3'i16)

  test "encode above the modeled maxVersion raises KafkaProtocolError":
    # Encode must reject a version above the modeled maximum symmetrically
    # with decode; otherwise the raw wire is silently written with the highest
    # known shape and the encode/decode contract diverges.
    var w = initKafkaWriter()
    expect KafkaProtocolError:
      w.encode(DummyUntilMsg(always: 1'i16), version = 3'i16)

const dummyBytesSchema = MessageDef(
  name: "DummyBytesMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "header", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "payload", kind: fkBytes, sinceVersion: -1, untilVersion: -1),
  ],
)

defineKafkaMessage(dummyBytesSchema)

suite "schema_gen: non-nullable bytes":
  ## Non-nullable BYTES (fkBytes) read/write symmetry: round-trips the
  ## int32-length-prefixed payload and rejects a negative (null) length
  ## with KafkaProtocolError.

  test "round-trip preserves payload":
    var w = initKafkaWriter()
    let original =
      DummyBytesMsg(header: 0x0001'i16, payload: @[0xDE'u8, 0xAD, 0xBE, 0xEF])
    w.encode(original)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyBytesMsg)
    check decoded.header == 0x0001'i16
    check decoded.payload == @[0xDE'u8, 0xAD, 0xBE, 0xEF]
    check r.remaining == 0

  test "wire layout pin: header + int32 length + bytes":
    var w = initKafkaWriter()
    w.encode(DummyBytesMsg(header: 0x0001'i16, payload: @[0xDE'u8, 0xAD, 0xBE, 0xEF]))
    check w.buf ==
      @[
        0x00'u8,
        0x01, # header (BE int16)
        0x00,
        0x00,
        0x00,
        0x04, # payload length = 4 (BE int32)
        0xDE,
        0xAD,
        0xBE,
        0xEF, # payload bytes
      ]

  test "empty payload encodes as length=0 and decodes as empty seq":
    var w = initKafkaWriter()
    w.encode(DummyBytesMsg(header: 0x0042'i16, payload: @[]))
    check w.buf ==
      @[
        0x00'u8,
        0x42, # header
        0x00,
        0x00,
        0x00,
        0x00, # payload length = 0
      ]
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyBytesMsg)
    check decoded.header == 0x0042'i16
    check decoded.payload.len == 0
    check r.remaining == 0

  test "negative length payload raises KafkaProtocolError":
    let bytes = @[
      0x00'u8,
      0x01, # header
      0xFF,
      0xFF,
      0xFF,
      0xFF, # payload length = -1 (NULL marker — invalid here)
    ]
    var r = initKafkaReader(bytes)
    expectKafkaProtocolError:
      discard r.decode(DummyBytesMsg)

const dummyFlexSchema = MessageDef(
  name: "DummyFlexMsg",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: 1,
  fields: @[
    FieldDef(name: "tag", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "name", kind: fkString, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "items",
      kind: fkArray,
      sinceVersion: -1,
      untilVersion: -1,
      nestedTypeName: "DummyFlexItem",
      nested: @[
        FieldDef(name: "value", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
        FieldDef(name: "label", kind: fkString, sinceVersion: -1, untilVersion: -1),
      ],
    ),
  ],
)

defineKafkaMessage(dummyFlexSchema)

suite "schema_gen: flexible auto-switch":
  ## Flexible (compact + tagged-fields) auto-switching. DummyFlexMsg uses
  ## flexibleSince=1, so one schema serves both:
  ##   v0 → legacy (int16/int32 lengths, no tagged-fields trailer)
  ##   v1 → compact (uvarint length+1, tagged-fields trailer per struct)

  test "v0 (pre-flexible) wire layout uses legacy primitives":
    var w = initKafkaWriter()
    w.encode(
      DummyFlexMsg(
        tag: 0x0123'i16,
        name: "ab",
        items: @[DummyFlexItem(value: 0x42'i32, label: "x")],
      ),
      version = 0'i16,
    )
    check w.buf ==
      @[
        0x01'u8,
        0x23, # tag
        0x00,
        0x02,
        0x61,
        0x62, # name "ab" (int16 length + bytes)
        0x00,
        0x00,
        0x00,
        0x01, # items.len = 1 (int32)
        0x00,
        0x00,
        0x00,
        0x42, # items[0].value = 0x42 (int32)
        0x00,
        0x01,
        0x78, # items[0].label "x" (int16 length + bytes)
      ]

  test "v1 (flexible) wire layout uses compact primitives + tagged trailers":
    var w = initKafkaWriter()
    w.encode(
      DummyFlexMsg(
        tag: 0x0123'i16,
        name: "ab",
        items: @[DummyFlexItem(value: 0x42'i32, label: "x")],
      ),
      version = 1'i16,
    )
    check w.buf ==
      @[
        0x01'u8,
        0x23, # tag
        0x03,
        0x61,
        0x62, # name "ab" (compact: uvarint(len+1=3) + bytes)
        0x02, # items count (compact: uvarint(count+1=2))
        0x00,
        0x00,
        0x00,
        0x42, # items[0].value = 0x42 (int32 — fixed-width, not compacted)
        0x02,
        0x78, # items[0].label "x" (compact: uvarint(2) + bytes)
        0x00, # items[0] tagged-fields trailer (count = 0)
        0x00, # top-level tagged-fields trailer (count = 0)
      ]

  test "v0 round-trip":
    let original = DummyFlexMsg(
      tag: 0x1234'i16,
      name: "hello",
      items: @[
        DummyFlexItem(value: 1'i32, label: "first"),
        DummyFlexItem(value: 2'i32, label: ""),
      ],
    )
    var w = initKafkaWriter()
    w.encode(original, version = 0'i16)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyFlexMsg, version = 0'i16)
    check decoded.tag == original.tag
    check decoded.name == original.name
    check decoded.items.len == original.items.len
    check decoded.items[0].value == 1'i32
    check decoded.items[0].label == "first"
    check decoded.items[1].value == 2'i32
    check decoded.items[1].label == ""
    check r.remaining == 0

  test "v1 round-trip":
    let original = DummyFlexMsg(
      tag: 0x1234'i16,
      name: "hello",
      items: @[
        DummyFlexItem(value: 1'i32, label: "first"),
        DummyFlexItem(value: 2'i32, label: ""),
      ],
    )
    var w = initKafkaWriter()
    w.encode(original, version = 1'i16)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyFlexMsg, version = 1'i16)
    check decoded.tag == original.tag
    check decoded.name == original.name
    check decoded.items.len == original.items.len
    check decoded.items[0].value == 1'i32
    check decoded.items[0].label == "first"
    check decoded.items[1].value == 2'i32
    check decoded.items[1].label == ""
    check r.remaining == 0

  test "v1 with empty items still emits both tagged-fields trailers":
    var w = initKafkaWriter()
    w.encode(DummyFlexMsg(tag: 0x0001'i16, name: "", items: @[]), version = 1'i16)
    check w.buf ==
      @[
        0x00'u8,
        0x01, # tag
        0x01, # name "" (compact: uvarint(len+1=1), no bytes)
        0x01, # items count = 0 (compact: uvarint(count+1=1))
        0x00, # top-level tagged-fields trailer
      ]
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyFlexMsg, version = 1'i16)
    check decoded.tag == 0x0001'i16
    check decoded.name == ""
    check decoded.items.len == 0
    check r.remaining == 0

  test "v1 decode tolerates non-empty tagged-fields trailers":
    # Top-level trailer carries one unknown tag (id=7, payload=2 bytes).
    let bytes = @[
      0x00'u8,
      0x01, # tag
      0x01, # name ""
      0x01, # items.len = 0
      0x01, # tagged-fields count = 1
      0x07, # tag id = 7
      0x02, # payload length = 2
      0xAB,
      0xCD, # opaque payload (must be skipped)
    ]
    var r = initKafkaReader(bytes)
    let decoded = r.decode(DummyFlexMsg, version = 1'i16)
    check decoded.tag == 0x0001'i16
    check decoded.name == ""
    check decoded.items.len == 0
    check r.remaining == 0

  test "v1 decode rejects a tagged-field payload length that overflows int":
    # Regression for the macro tagged-fields decoder: a payload length of 2^63
    # (a valid 10-byte uvarint) makes int(payloadLen) wrap negative. Without
    # the `payloadLen <= remaining` bound the decoder walks r.pos out of
    # bounds (past checkRemaining, which sees a large positive remaining for a
    # negative pos) — a memory-unsafe read. The guard must reject it instead.
    let bytes = @[
      0x00'u8,
      0x01, # tag
      0x01, # name ""
      0x01, # items.len = 0
      0x01, # tagged-fields count = 1
      0x07,
        # tag id = 7
        # payload length = 2^63 (uvarint: nine 0x80 continuation bytes, then 0x01)
      0x80,
      0x80,
      0x80,
      0x80,
      0x80,
      0x80,
      0x80,
      0x80,
      0x80,
      0x01,
    ]
    var r = initKafkaReader(bytes)
    expectKafkaProtocolError:
      discard r.decode(DummyFlexMsg, version = 1'i16)

  test "v1 decode rejects a tagged-field payload length past the buffer":
    # A payload length of 200 with no bytes left must raise, not silently run
    # the skip path off the end of the buffer.
    let bytes = @[
      0x00'u8,
      0x01, # tag
      0x01, # name ""
      0x01, # items.len = 0
      0x01, # tagged-fields count = 1
      0x07, # tag id = 7
      0xC8,
      0x01, # payload length = 200 (uvarint), but 0 bytes remain
    ]
    var r = initKafkaReader(bytes)
    expectKafkaProtocolError:
      discard r.decode(DummyFlexMsg, version = 1'i16)

  test "v1 decode rejects an absurd tagged-fields count":
    # tagged-fields count of 2^32 exceeds the 10000 sanity cap; without the
    # guard the decoder would attempt a multi-billion-iteration loop.
    let bytes = @[
      0x00'u8,
      0x01, # tag
      0x01, # name ""
      0x01,
        # items.len = 0
        # tagged-fields count = 2^32 (uvarint: 0x80 0x80 0x80 0x80 0x10)
      0x80,
      0x80,
      0x80,
      0x80,
      0x10,
    ]
    var r = initKafkaReader(bytes)
    expectKafkaProtocolError:
      discard r.decode(DummyFlexMsg, version = 1'i16)

  test "decode rejects a struct-array count larger than the remaining buffer":
    # Allocation-amplification guard: a huge items.len with no element bytes
    # must raise before newSeq[DummyFlexItem](len) pre-allocates ~1M structs.
    let bytes = @[
      0x00'u8,
      0x01, # tag
      0x00,
      0x00, # name "" (legacy int16 length 0)
      0x00,
      0x0F,
      0x42,
      0x40, # items.len = 1_000_000 (int32), but no element bytes follow
    ]
    var r = initKafkaReader(bytes)
    expectKafkaProtocolError:
      discard r.decode(DummyFlexMsg, version = 0'i16)

const dummyUuidSchema = MessageDef(
  name: "DummyUuidMsg",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: 1,
  fields: @[FieldDef(name: "id", kind: fkUuid, sinceVersion: -1, untilVersion: -1)],
)

defineKafkaMessage(dummyUuidSchema)

const sampleUuid: array[16, byte] = [
  0x11'u8, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
  0xFF, 0x00,
]

suite "schema_gen: fkUuid":
  test "v0 (legacy) wire is 16 raw bytes":
    var w = initKafkaWriter()
    w.encode(DummyUuidMsg(id: sampleUuid), version = 0'i16)
    check w.buf == @sampleUuid

  test "v1 (flex) wire is 16 raw bytes plus empty trailer":
    var w = initKafkaWriter()
    w.encode(DummyUuidMsg(id: sampleUuid), version = 1'i16)
    check w.buf == @sampleUuid & @[0x00'u8]

  test "round-trip preserves all 16 bytes":
    for ver in [0'i16, 1'i16]:
      var w = initKafkaWriter()
      w.encode(DummyUuidMsg(id: sampleUuid), version = ver)
      var r = initKafkaReader(w.buf)
      let decoded = r.decode(DummyUuidMsg, version = ver)
      check decoded.id == sampleUuid
      check r.remaining == 0

const dummyScalarArrSchema = MessageDef(
  name: "DummyScalarArrMsg",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: 1,
  fields: @[
    FieldDef(name: "header", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "values",
      kind: fkArray,
      elementKind: fkInt32,
      sinceVersion: -1,
      untilVersion: -1,
    ),
  ],
)

defineKafkaMessage(dummyScalarArrSchema)

suite "schema_gen: scalar arrays":
  test "v0 wire: int32 length prefix + raw int32 elements":
    var w = initKafkaWriter()
    w.encode(
      DummyScalarArrMsg(header: 0x0001'i16, values: @[1'i32, 2'i32, 3'i32]),
      version = 0'i16,
    )
    check w.buf ==
      @[
        0x00'u8,
        0x01, # header
        0x00,
        0x00,
        0x00,
        0x03, # values.len (int32)
        0x00,
        0x00,
        0x00,
        0x01, # values[0]
        0x00,
        0x00,
        0x00,
        0x02, # values[1]
        0x00,
        0x00,
        0x00,
        0x03, # values[2]
      ]

  test "v1 wire: compact length (uvarint count+1) + int32 elements + trailer":
    var w = initKafkaWriter()
    w.encode(
      DummyScalarArrMsg(header: 0x0001'i16, values: @[1'i32, 2'i32, 3'i32]),
      version = 1'i16,
    )
    check w.buf ==
      @[
        0x00'u8,
        0x01, # header
        0x04, # compact length: 3 + 1
        0x00,
        0x00,
        0x00,
        0x01, # values[0]
        0x00,
        0x00,
        0x00,
        0x02, # values[1]
        0x00,
        0x00,
        0x00,
        0x03, # values[2]
        0x00, # empty top-level tagged-fields trailer
      ]

  test "round-trip empty scalar array at v0 and v1":
    for ver in [0'i16, 1'i16]:
      var w = initKafkaWriter()
      w.encode(DummyScalarArrMsg(header: 0x0042'i16, values: @[]), version = ver)
      var r = initKafkaReader(w.buf)
      let decoded = r.decode(DummyScalarArrMsg, version = ver)
      check decoded.header == 0x0042'i16
      check decoded.values.len == 0
      check r.remaining == 0

const dummyNestedArrSchema = MessageDef(
  name: "DummyNestedArrMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[
    FieldDef(
      name: "groups",
      kind: fkArray,
      nestedTypeName: "DummyNestedGroup",
      sinceVersion: -1,
      untilVersion: -1,
      nested: @[
        FieldDef(name: "groupId", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
        FieldDef(
          name: "items",
          kind: fkArray,
          elementKind: fkInt16,
          sinceVersion: -1,
          untilVersion: -1,
        ),
      ],
    )
  ],
)

defineKafkaMessage(dummyNestedArrSchema)

suite "schema_gen: nested arrays":
  test "wire layout: outer struct array → inner scalar array":
    var w = initKafkaWriter()
    w.encode(
      DummyNestedArrMsg(
        groups: @[
          DummyNestedGroup(groupId: 10'i32, items: @[1'i16, 2'i16]),
          DummyNestedGroup(groupId: 20'i32, items: @[]),
        ]
      )
    )
    check w.buf ==
      @[
        0x00'u8,
        0x00,
        0x00,
        0x02,
          # groups.len = 2
          # group 0
        0x00,
        0x00,
        0x00,
        0x0A, # groupId = 10
        0x00,
        0x00,
        0x00,
        0x02, # items.len = 2
        0x00,
        0x01, # items[0] = 1
        0x00,
        0x02,
          # items[1] = 2
          # group 1
        0x00,
        0x00,
        0x00,
        0x14, # groupId = 20
        0x00,
        0x00,
        0x00,
        0x00, # items.len = 0
      ]

  test "round-trip preserves all groups and items":
    let original = DummyNestedArrMsg(
      groups: @[
        DummyNestedGroup(groupId: 1'i32, items: @[10'i16, 20'i16, 30'i16]),
        DummyNestedGroup(groupId: 2'i32, items: @[]),
        DummyNestedGroup(groupId: 3'i32, items: @[42'i16]),
      ]
    )
    var w = initKafkaWriter()
    w.encode(original)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyNestedArrMsg)
    check decoded.groups.len == 3
    check decoded.groups[0].groupId == 1'i32
    check decoded.groups[0].items == @[10'i16, 20'i16, 30'i16]
    check decoded.groups[1].groupId == 2'i32
    check decoded.groups[1].items.len == 0
    check decoded.groups[2].groupId == 3'i32
    check decoded.groups[2].items == @[42'i16]
    check r.remaining == 0

const dummyTaggedSchema = MessageDef(
  name: "DummyTaggedMsg",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: 1,
  fields: @[
    FieldDef(name: "a", kind: fkInt16, sinceVersion: -1, untilVersion: -1),
    FieldDef(name: "b", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
    FieldDef(
      name: "tagAlpha", kind: fkInt32, sinceVersion: -1, untilVersion: -1, taggedId: 0
    ),
    FieldDef(
      name: "tagBeta", kind: fkInt64, sinceVersion: -1, untilVersion: -1, taggedId: 5
    ),
  ],
)

defineKafkaMessage(dummyTaggedSchema)

suite "schema_gen: tag-resident scalars":
  test "v1 wire with no tags populated emits empty trailer":
    var w = initKafkaWriter()
    w.encode(DummyTaggedMsg(a: 0x0001'i16, b: 0x00000002'i32), version = 1'i16)
    check w.buf ==
      @[
        0x00'u8,
        0x01, # a
        0x00,
        0x00,
        0x00,
        0x02, # b
        0x00, # tagged-fields count = 0
      ]

  test "v1 wire with only tagAlpha populated":
    var w = initKafkaWriter()
    w.encode(
      DummyTaggedMsg(a: 0x0001'i16, b: 0x00000002'i32, tagAlpha: some(0x42'i32)),
      version = 1'i16,
    )
    check w.buf ==
      @[
        0x00'u8,
        0x01, # a
        0x00,
        0x00,
        0x00,
        0x02, # b
        0x01, # tagged-fields count = 1
        0x00, # tagId = 0 (alpha)
        0x04, # payload length = 4
        0x00,
        0x00,
        0x00,
        0x42, # payload (int32 = 0x42)
      ]

  test "v1 wire with both tags populated emits ascending tagId order":
    var w = initKafkaWriter()
    w.encode(
      DummyTaggedMsg(
        a: 0'i16,
        b: 0'i32,
        tagAlpha: some(0x11223344'i32),
        tagBeta: some(int64(0x1122334455667788'i64)),
      ),
      version = 1'i16,
    )
    check w.buf ==
      @[
        0x00'u8,
        0x00, # a
        0x00,
        0x00,
        0x00,
        0x00, # b
        0x02, # tagged-fields count = 2
        0x00, # tagId = 0 (alpha)
        0x04, # payload length = 4
        0x11,
        0x22,
        0x33,
        0x44, # payload (int32)
        0x05, # tagId = 5 (beta)
        0x08, # payload length = 8
        0x11,
        0x22,
        0x33,
        0x44,
        0x55,
        0x66,
        0x77,
        0x88, # payload (int64)
      ]

  test "v1 round-trip populates Option fields":
    let original = DummyTaggedMsg(
      a: 0x1234'i16,
      b: 0x56789ABC'i32,
      tagAlpha: some(0x42'i32),
      tagBeta: some(0x100'i64),
    )
    var w = initKafkaWriter()
    w.encode(original, version = 1'i16)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyTaggedMsg, version = 1'i16)
    check decoded.a == original.a
    check decoded.b == original.b
    check decoded.tagAlpha.isSome and decoded.tagAlpha.get == 0x42'i32
    check decoded.tagBeta.isSome and decoded.tagBeta.get == 0x100'i64
    check r.remaining == 0

  test "v1 round-trip leaves missing tags as none":
    var w = initKafkaWriter()
    w.encode(
      DummyTaggedMsg(a: 1'i16, b: 2'i32, tagAlpha: some(99'i32)), version = 1'i16
    )
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyTaggedMsg, version = 1'i16)
    check decoded.a == 1'i16
    check decoded.b == 2'i32
    check decoded.tagAlpha == some(99'i32)
    check decoded.tagBeta.isNone
    check r.remaining == 0

  test "v1 decoder skips unknown tags interleaved with known ones":
    # Bytes: a, b, count=3, tagId=0 (known), tagId=2 (unknown), tagId=5 (known)
    let bytes = @[
      0x00'u8,
      0x01, # a
      0x00,
      0x00,
      0x00,
      0x02, # b
      0x03,
        # tagged-fields count = 3
        # known tag 0
      0x00,
      0x04, # tagId=0, len=4
      0x00,
      0x00,
      0x00,
      0xAA,
      # unknown tag 2 (skipped)
      0x02,
      0x02, # tagId=2, len=2
      0xCA,
      0xFE,
        # opaque payload
        # known tag 5
      0x05,
      0x08, # tagId=5, len=8
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0xBB,
    ]
    var r = initKafkaReader(bytes)
    let decoded = r.decode(DummyTaggedMsg, version = 1'i16)
    check decoded.a == 0x0001'i16
    check decoded.b == 0x00000002'i32
    check decoded.tagAlpha == some(0xAA'i32)
    check decoded.tagBeta == some(0xBB'i64)
    check r.remaining == 0

  test "v1 decode rejects a known tag whose payload overruns its declared length":
    # A known tag (tagAlpha, int32) declares a 1-byte payload but the int32
    # read absorbs the next tag's bytes. The clamp restores the position (a
    # well-formed tagBeta starts 3 bytes later, after the absorbed padding), but
    # without the window guard the field value would stay polluted and the
    # decode would succeed — reject it.
    let bytes = @[
      0x00'u8,
      0x00, # a
      0x00,
      0x00,
      0x00,
      0x00, # b
      0x02, # tagged-fields count = 2
      0x00, # tagId = 0 (alpha)
      0x01, # payload length = 1 (declared), but the int32 read needs 4 bytes
      0xAA,
      0x00,
      0x00,
      0x00, # the 4 bytes the int32 read absorbs (1 declared + 3 of the next region)
      0x05,
      0x08, # tagId = 5 (beta), len = 8, positioned exactly at endPos
      0x11,
      0x22,
      0x33,
      0x44,
      0x55,
      0x66,
      0x77,
      0x88, # beta payload
    ]
    var r = initKafkaReader(bytes)
    expectKafkaProtocolError:
      discard r.decode(DummyTaggedMsg, version = 1'i16)

  test "v1 decode tolerates a known tag with trailing padding in its payload":
    # A declared payload longer than the field's wire size is clamped back to
    # endPos (trailing padding discarded), mirroring the standalone skippers'
    # window semantics. Only overruns are malformed.
    let bytes = @[
      0x00'u8,
      0x00, # a
      0x00,
      0x00,
      0x00,
      0x00, # b
      0x01, # tagged-fields count = 1
      0x00, # tagId = 0 (alpha)
      0x05, # payload length = 5 (int32 reads 4, 1 trailing byte)
      0x00,
      0x00,
      0x00,
      0x2A, # payload (int32 = 42, big-endian)
      0x2A, # trailing padding byte
    ]
    var r = initKafkaReader(bytes)
    let decoded = r.decode(DummyTaggedMsg, version = 1'i16)
    check decoded.tagAlpha == some(0x2A'i32)
    check r.remaining == 0

  test "v0 (pre-flexible) ignores Option fields entirely":
    # taggedId fields require flex, but the schema declares flex starting
    # at v1. At v0 the encoder writes only the positional fields and the
    # decoder leaves Option fields as none.
    var w = initKafkaWriter()
    w.encode(
      DummyTaggedMsg(
        a: 0x0011'i16,
        b: 0x00000022'i32,
        tagAlpha: some(99'i32), # populated but must not be encoded at v0
        tagBeta: some(100'i64),
      ),
      version = 0'i16,
    )
    check w.buf ==
      @[
        0x00'u8,
        0x11, # a
        0x00,
        0x00,
        0x00,
        0x22,
          # b
          # no trailer at v0
      ]
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyTaggedMsg, version = 0'i16)
    check decoded.a == 0x0011'i16
    check decoded.b == 0x00000022'i32
    check decoded.tagAlpha.isNone
    check decoded.tagBeta.isNone
    check r.remaining == 0

const taggedKindsSchema = MessageDef(
  name: "TaggedKindsMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: 0,
  fields: @[
    FieldDef(name: "i8", kind: fkInt8, sinceVersion: 0, untilVersion: -1, taggedId: 0),
    FieldDef(
      name: "str", kind: fkString, sinceVersion: 0, untilVersion: -1, taggedId: 1
    ),
    FieldDef(
      name: "nullableStr",
      kind: fkNullableString,
      sinceVersion: 0,
      untilVersion: -1,
      taggedId: 2,
    ),
    FieldDef(
      name: "bytes", kind: fkBytes, sinceVersion: 0, untilVersion: -1, taggedId: 3
    ),
    FieldDef(
      name: "nullableBytes",
      kind: fkNullableBytes,
      sinceVersion: 0,
      untilVersion: -1,
      taggedId: 4,
    ),
    FieldDef(name: "uuid", kind: fkUuid, sinceVersion: 0, untilVersion: -1, taggedId: 5),
  ],
)

defineKafkaMessage(taggedKindsSchema)

suite "schema_gen: tag-capable FieldKind coverage":
  test "encodes and decodes every tag-capable scalar kind":
    let value = TaggedKindsMsg(
      i8: some(1'i8),
      str: some("s"),
      nullableStr: some(some("n")),
      bytes: some(@[0xAA'u8]),
      nullableBytes: some(some(newSeq[byte]())),
      uuid: some(sampleUuid),
    )
    var w = initKafkaWriter()
    w.encode(value)
    let expectedTaggedKinds: seq[byte] = @[
      0x06'u8,
      0x00,
      0x01,
      0x01,
      0x01,
      0x02,
      0x02,
      byte('s'),
      0x02,
      0x02,
      0x02,
      byte('n'),
      0x03,
      0x02,
      0x02,
      0xAA,
      0x04,
      0x01,
      0x01,
      0x05,
      0x10,
      0x11,
      0x22,
      0x33,
      0x44,
      0x55,
      0x66,
      0x77,
      0x88,
      0x99,
      0xAA,
      0xBB,
      0xCC,
      0xDD,
      0xEE,
      0xFF,
      0x00,
    ]
    check w.buf == expectedTaggedKinds
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(TaggedKindsMsg)
    check decoded.i8 == some(1'i8)
    check decoded.str == some("s")
    check decoded.nullableStr == some(some("n"))
    check decoded.bytes == some(@[0xAA'u8])
    check decoded.nullableBytes == some(some(newSeq[byte]()))
    check decoded.uuid == some(sampleUuid)
    check r.remaining == 0

const dummyNullableArrSchema = MessageDef(
  name: "DummyNullableArrMsg",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: 1,
  fields: @[
    FieldDef(
      name: "topics",
      kind: fkArray,
      nullable: true,
      nestedTypeName: "DummyNullableTopic",
      sinceVersion: -1,
      untilVersion: -1,
      nested:
        @[FieldDef(name: "name", kind: fkString, sinceVersion: -1, untilVersion: -1)],
    )
  ],
)

defineKafkaMessage(dummyNullableArrSchema)

suite "schema_gen: nullable struct arrays":
  test "v0 wire: none encodes as int32 -1":
    var w = initKafkaWriter()
    w.encode(
      DummyNullableArrMsg(topics: none(seq[DummyNullableTopic])), version = 0'i16
    )
    check w.buf == @[0xFF'u8, 0xFF, 0xFF, 0xFF]

  test "v1 wire: none encodes as compact uvarint 0":
    var w = initKafkaWriter()
    w.encode(
      DummyNullableArrMsg(topics: none(seq[DummyNullableTopic])), version = 1'i16
    )
    # null array (0x00) + empty top-level tagged-fields trailer (0x00)
    check w.buf == @[0x00'u8, 0x00]

  test "v0 wire: some(@[]) encodes as int32 0":
    var w = initKafkaWriter()
    w.encode(
      DummyNullableArrMsg(topics: some(newSeq[DummyNullableTopic]())), version = 0'i16
    )
    check w.buf == @[0x00'u8, 0x00, 0x00, 0x00]

  test "v1 wire: some(@[]) encodes as compact uvarint 1":
    var w = initKafkaWriter()
    w.encode(
      DummyNullableArrMsg(topics: some(newSeq[DummyNullableTopic]())), version = 1'i16
    )
    # empty array (0x01 = 0+1) + tagged-fields trailer (0x00)
    check w.buf == @[0x01'u8, 0x00]

  test "v0 wire: some(@[one, two]) encodes normally":
    var w = initKafkaWriter()
    w.encode(
      DummyNullableArrMsg(
        topics: some(@[DummyNullableTopic(name: "a"), DummyNullableTopic(name: "bb")])
      ),
      version = 0'i16,
    )
    check w.buf ==
      @[
        0x00'u8,
        0x00,
        0x00,
        0x02, # topics.len = 2
        0x00,
        0x01,
        0x61, # name "a"
        0x00,
        0x02,
        0x62,
        0x62, # name "bb"
      ]

  test "v0 round-trip none":
    var w = initKafkaWriter()
    w.encode(
      DummyNullableArrMsg(topics: none(seq[DummyNullableTopic])), version = 0'i16
    )
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyNullableArrMsg, version = 0'i16)
    check decoded.topics.isNone
    check r.remaining == 0

  test "v1 round-trip none":
    var w = initKafkaWriter()
    w.encode(
      DummyNullableArrMsg(topics: none(seq[DummyNullableTopic])), version = 1'i16
    )
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyNullableArrMsg, version = 1'i16)
    check decoded.topics.isNone
    check r.remaining == 0

  test "v1 round-trip some with entries":
    let original = DummyNullableArrMsg(
      topics: some(@[DummyNullableTopic(name: "x"), DummyNullableTopic(name: "yy")])
    )
    var w = initKafkaWriter()
    w.encode(original, version = 1'i16)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyNullableArrMsg, version = 1'i16)
    check decoded.topics.isSome
    let ts = decoded.topics.get
    check ts.len == 2
    check ts[0].name == "x"
    check ts[1].name == "yy"
    check r.remaining == 0

const apiVersionsV0Bytes = @[
  # errorCode = 0 (NONE)
  0x00'u8,
  0x00,
  # apiKeys.len = 2
  0x00,
  0x00,
  0x00,
  0x02,
  # entry 1: apiKey=0 (Produce), minVersion=0, maxVersion=3
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0x03,
  # entry 2: apiKey=18 (ApiVersions), minVersion=0, maxVersion=4
  0x00,
  0x12,
  0x00,
  0x00,
  0x00,
  0x04,
]

suite "ApiVersionsResponse wire decode":
  test "decode v0 wire payload":
    var r = initKafkaReader(apiVersionsV0Bytes)
    let resp = r.decode(ApiVersionsResponse)
    check resp.errorCode == keNone
    check resp.apiKeys.len == 2
    check resp.apiKeys[0].apiKey == 0'i16
    check resp.apiKeys[0].minVersion == 0'i16
    check resp.apiKeys[0].maxVersion == 3'i16
    check resp.apiKeys[1].apiKey == 18'i16
    check resp.apiKeys[1].minVersion == 0'i16
    check resp.apiKeys[1].maxVersion == 4'i16
    check r.remaining == 0

  test "decode v0 with empty apiKeys array":
    let bytes = @[
      0x00'u8,
      0x00, # errorCode = 0
      0x00,
      0x00,
      0x00,
      0x00, # apiKeys.len = 0
    ]
    var r = initKafkaReader(bytes)
    let resp = r.decode(ApiVersionsResponse)
    check resp.errorCode == keNone
    check resp.apiKeys.len == 0
    check r.remaining == 0

  test "decode v0 with non-zero error code":
    let bytes = @[
      0x00'u8,
      0x23, # errorCode = 35 (UNSUPPORTED_VERSION)
      0x00,
      0x00,
      0x00,
      0x00, # apiKeys.len = 0
    ]
    var r = initKafkaReader(bytes)
    let resp = r.decode(ApiVersionsResponse)
    check resp.errorCode == keUnsupportedVersion
    check resp.apiKeys.len == 0

suite "ApiVersionsResponse v1 versioned field":
  test "decode v1 wire payload reads throttleTimeMs":
    let bytes = @[
      0x00'u8,
      0x00, # errorCode = 0
      0x00,
      0x00,
      0x00,
      0x01, # apiKeys.len = 1
      0x00,
      0x12,
      0x00,
      0x00,
      0x00,
      0x04, # one entry
      0x00,
      0x00,
      0x03,
      0xE8, # throttleTimeMs = 1000
    ]
    var r = initKafkaReader(bytes)
    let resp = r.decode(ApiVersionsResponse, version = 1'i16)
    check resp.errorCode == keNone
    check resp.apiKeys.len == 1
    check resp.throttleTimeMs == 1000'i32
    check r.remaining == 0

  test "decode v0 of the same schema does NOT read throttleTimeMs":
    var r = initKafkaReader(apiVersionsV0Bytes)
    let resp = r.decode(ApiVersionsResponse, version = 0'i16)
    check resp.errorCode == keNone
    check resp.apiKeys.len == 2
    check resp.throttleTimeMs == 0'i32 # default — not present in v0 payload
    check r.remaining == 0

  test "encode v0 then decode v0 round-trip":
    var w = initKafkaWriter()
    var resp = ApiVersionsResponse(
      errorCode: keNone,
      apiKeys: @[
        ApiVersionsResponseKey(apiKey: 0, minVersion: 0, maxVersion: 3),
        ApiVersionsResponseKey(apiKey: 18, minVersion: 0, maxVersion: 4),
      ],
      throttleTimeMs: 999'i32, # set but should NOT be encoded at v0
    )
    w.encode(resp, version = 0'i16)
    check w.buf == apiVersionsV0Bytes
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(ApiVersionsResponse, version = 0'i16)
    check decoded.throttleTimeMs == 0'i32 # v0 has no slot for it

  test "encode v1 then decode v1 round-trip":
    var w = initKafkaWriter()
    let resp = ApiVersionsResponse(
      errorCode: keNone,
      apiKeys: @[ApiVersionsResponseKey(apiKey: 18, minVersion: 0, maxVersion: 4)],
      throttleTimeMs: 1000'i32,
    )
    w.encode(resp, version = 1'i16)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(ApiVersionsResponse, version = 1'i16)
    check decoded.errorCode == keNone
    check decoded.apiKeys.len == 1
    check decoded.throttleTimeMs == 1000'i32

const initProducerIdV0Bytes = @[
  # throttleTimeMs = 100 (int32 BE)
  0x00'u8,
  0x00,
  0x00,
  0x64,
  # errorCode = 0 (NONE)
  0x00,
  0x00,
  # producerId = 99999 (int64 BE)
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0x01,
  0x86,
  0x9F,
  # producerEpoch = 3 (int16 BE)
  0x00,
  0x03,
]

suite "InitProducerIdResponse wire decode":
  ## InitProducerIdResponse wire decode. v0 layout:
  ##   throttleTimeMs (int32) → errorCode (int16) → producerId (int64) →
  ##   producerEpoch (int16). v2+ adds a flexible tagged-fields trailer.

  test "decode v0 wire payload":
    var r = initKafkaReader(initProducerIdV0Bytes)
    let resp = r.decode(InitProducerIdResponse)
    check resp.throttleTimeMs == 100'i32
    check resp.errorCode == keNone
    check resp.producerId == 99999'i64
    check resp.producerEpoch == 3'i16
    check r.remaining == 0

  test "decode v0 with non-zero error code":
    let bytes = @[
      0x00'u8,
      0x00,
      0x00,
      0x00, # throttleTimeMs = 0
      0x00,
      0x2F, # errorCode = 47 (INVALID_PRODUCER_EPOCH)
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF, # producerId = -1
      0xFF,
      0xFF, # producerEpoch = -1
    ]
    var r = initKafkaReader(bytes)
    let resp = r.decode(InitProducerIdResponse)
    check resp.errorCode == keInvalidProducerEpoch
    check resp.producerId == -1'i64
    check resp.producerEpoch == -1'i16

suite "HeartbeatResponse wire decode":
  test "decode v0 wire payload":
    let bytes = @[0x00'u8, 0x00] # errorCode = 0
    var r = initKafkaReader(bytes)
    let resp = r.decode(HeartbeatResponse)
    check resp.errorCode == keNone
    check r.remaining == 0

  test "decode v0 with rebalance-in-progress":
    let bytes = @[0x00'u8, 0x1B] # errorCode = 27 (REBALANCE_IN_PROGRESS)
    var r = initKafkaReader(bytes)
    let resp = r.decode(HeartbeatResponse)
    check resp.errorCode == keRebalanceInProgress

suite "LeaveGroup wire decode":
  test "request v0 encode is byte-for-byte stable":
    var w = initKafkaWriter()
    w.encode(LeaveGroupRequest(groupId: "g1", memberId: "m1"))
    check w.buf ==
      @[
        0x00'u8,
        0x02,
        byte('g'),
        byte('1'), # groupId
        0x00,
        0x02,
        byte('m'),
        byte('1'), # memberId
      ]

  test "response v0 round-trip":
    let original = LeaveGroupResponse(throttleTimeMs: 0, errorCode: keNone)
    var w = initKafkaWriter()
    w.encode(original)
    check w.buf == @[0x00'u8, 0x00] # errorCode only at v0
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(LeaveGroupResponse)
    check decoded.errorCode == keNone
    check r.remaining == 0

  test "response v1 prepends throttleTimeMs":
    let original =
      LeaveGroupResponse(throttleTimeMs: 100'i32, errorCode: keRebalanceInProgress)
    var w = initKafkaWriter()
    w.encode(original, version = 1'i16)
    check w.buf ==
      @[
        0x00'u8,
        0x00,
        0x00,
        0x64, # throttleTimeMs = 100
        0x00,
        0x1B, # errorCode = 27 (REBALANCE_IN_PROGRESS)
      ]
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(LeaveGroupResponse, version = 1'i16)
    check decoded.throttleTimeMs == 100'i32
    check decoded.errorCode == keRebalanceInProgress
    check r.remaining == 0

suite "AddPartitionsToTxn wire decode":
  ## AddPartitionsToTxn / AddOffsetsToTxn / TxnOffsetCommit wire decode (all v0).
  ## These three transactional APIs share a similar shape — a small set of
  ## scalar request fields plus a topic-keyed response array — and pin one
  ## round-trip per direction.

  test "request v0 byte pin":
    var w = initKafkaWriter()
    w.encode(
      AddPartitionsToTxnRequest(
        transactionalId: "tx",
        producerId: 42'i64,
        producerEpoch: 1'i16,
        topics: @[AddPartitionsToTxnTopicReq(topic: "t", partitions: @[0'i32])],
      )
    )
    check w.buf ==
      @[
        0x00'u8,
        0x02,
        byte('t'),
        byte('x'), # transactionalId
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x2A, # producerId = 42
        0x00,
        0x01, # producerEpoch
        0x00,
        0x00,
        0x00,
        0x01, # topics.len
        0x00,
        0x01,
        byte('t'),
        0x00,
        0x00,
        0x00,
        0x01, # partitions.len
        0x00,
        0x00,
        0x00,
        0x00, # partition
      ]

  test "response v0 round-trip":
    let original = AddPartitionsToTxnResponse(
      throttleTimeMs: 75'i32,
      errors: @[
        AddPartitionsToTxnTopicResp(
          topic: "t",
          partitions:
            @[AddPartitionsToTxnPartitionResp(partition: 0'i32, errorCode: keNone)],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(original)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(AddPartitionsToTxnResponse)
    check decoded.throttleTimeMs == 75'i32
    check decoded.errors[0].partitions[0].errorCode == keNone
    check r.remaining == 0

suite "AddOffsetsToTxn wire decode":
  test "request v0 byte pin":
    var w = initKafkaWriter()
    w.encode(
      AddOffsetsToTxnRequest(
        transactionalId: "tx", producerId: 1'i64, producerEpoch: 0'i16, groupId: "g"
      )
    )
    check w.buf ==
      @[
        0x00'u8,
        0x02,
        byte('t'),
        byte('x'), # transactionalId
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x01, # producerId
        0x00,
        0x00, # producerEpoch
        0x00,
        0x01,
        byte('g'), # groupId
      ]

  test "response v0 round-trip":
    let original = AddOffsetsToTxnResponse(throttleTimeMs: 0'i32, errorCode: keNone)
    var w = initKafkaWriter()
    w.encode(original)
    check w.buf == @[0x00'u8, 0x00, 0x00, 0x00, 0x00, 0x00]
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(AddOffsetsToTxnResponse)
    check decoded.errorCode == keNone
    check r.remaining == 0

suite "TxnOffsetCommit wire decode":
  test "response v0 round-trip":
    let original = TxnOffsetCommitResponse(
      throttleTimeMs: 0'i32,
      topics: @[
        TxnOffsetCommitTopicResp(
          topic: "t",
          partitions:
            @[TxnOffsetCommitPartitionResp(partition: 0'i32, errorCode: keNone)],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(original)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(TxnOffsetCommitResponse)
    check decoded.topics[0].partitions[0].errorCode == keNone
    check r.remaining == 0

suite "Produce wire decode":
  ## Produce request/response wire decode (v3). Unlike most Kafka responses,
  ## ProduceResponse carries throttle_time_ms at the END of the payload — the
  ## schema field order matches: topics → throttleTimeMs.

  test "request v3 encodes a single record set":
    var w = initKafkaWriter()
    w.encode(
      ProduceRequest(
        transactionalId: none(string),
        acks: 1'i16,
        timeoutMs: 30000'i32,
        topics: @[
          ProduceTopicReq(
            topic: "t",
            partitions:
              @[ProducePartitionReq(partition: 0'i32, recordSet: @[0xDE'u8, 0xAD])],
          )
        ],
      )
    )
    check w.buf ==
      @[
        0xFF'u8,
        0xFF, # transactionalId = null
        0x00,
        0x01, # acks
        0x00,
        0x00,
        0x75,
        0x30, # timeoutMs = 30000
        0x00,
        0x00,
        0x00,
        0x01, # topics.len
        0x00,
        0x01,
        byte('t'),
        0x00,
        0x00,
        0x00,
        0x01, # partitions.len
        0x00,
        0x00,
        0x00,
        0x00, # partition
        0x00,
        0x00,
        0x00,
        0x02,
        0xDE,
        0xAD, # recordSet
      ]

  test "response v3 places throttleTimeMs at the tail":
    let original = ProduceResponse(
      topics: @[
        ProduceTopicResp(
          topic: "t",
          partitions: @[
            ProducePartitionResp(
              partition: 0'i32,
              errorCode: keNone,
              baseOffset: 42'i64,
              timestampMs: -1'i64,
            )
          ],
        )
      ],
      throttleTimeMs: 100'i32,
    )
    var w = initKafkaWriter()
    w.encode(original)
    # Trailing 4 bytes must be the throttleTimeMs.
    check w.buf[^4 .. ^1] == @[0x00'u8, 0x00, 0x00, 0x64]
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(ProduceResponse)
    check decoded.throttleTimeMs == 100'i32
    check decoded.topics[0].partitions[0].baseOffset == 42'i64
    check r.remaining == 0

suite "OffsetFetch wire decode":
  ## OffsetFetch request/response wire decode (v1-v3). Response field order
  ## matches the wire: throttleTimeMs (v3+ prefix) → topics → errorCode
  ## (v2+ trailer). Pins all three version layouts.

  test "request v1 encodes a single topic+partition":
    var w = initKafkaWriter()
    w.encode(
      OffsetFetchRequest(
        groupId: "g",
        topics: @[OffsetFetchTopicReq(topic: "t", partitions: @[0'i32, 1'i32])],
      )
    )
    check w.buf ==
      @[
        0x00'u8,
        0x01,
        byte('g'), # groupId
        0x00,
        0x00,
        0x00,
        0x01, # topics.len
        0x00,
        0x01,
        byte('t'),
        0x00,
        0x00,
        0x00,
        0x02, # partitions.len
        0x00,
        0x00,
        0x00,
        0x00, # partition[0]
        0x00,
        0x00,
        0x00,
        0x01, # partition[1]
      ]

  test "response v1 layout: topics only":
    var w = initKafkaWriter()
    w.encode(
      OffsetFetchResponse(
        throttleTimeMs: 0,
        topics: @[
          OffsetFetchTopicResp(
            topic: "t",
            partitions: @[
              OffsetFetchPartitionResp(
                partition: 0'i32,
                offset: 100'i64,
                metadata: none(string),
                errorCode: keNone,
              )
            ],
          )
        ],
        errorCode: keNone,
      ),
      version = 1'i16,
    )
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(OffsetFetchResponse, version = 1'i16)
    check decoded.throttleTimeMs == 0'i32
    check decoded.errorCode == keNone
    check decoded.topics[0].partitions[0].offset == 100'i64
    check r.remaining == 0

  test "response v2 appends top-level errorCode":
    var w = initKafkaWriter()
    w.encode(
      OffsetFetchResponse(
        throttleTimeMs: 0, topics: @[], errorCode: keGroupCoordinatorNotAvailable
      ),
      version = 2'i16,
    )
    check w.buf ==
      @[
        0x00'u8,
        0x00,
        0x00,
        0x00, # topics.len = 0
        0x00,
        0x0F, # errorCode = 15
      ]
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(OffsetFetchResponse, version = 2'i16)
    check decoded.errorCode == keGroupCoordinatorNotAvailable
    check r.remaining == 0

  test "response v3 prefixes throttleTimeMs and keeps trailing errorCode":
    var w = initKafkaWriter()
    w.encode(
      OffsetFetchResponse(throttleTimeMs: 250'i32, topics: @[], errorCode: keNone),
      version = 3'i16,
    )
    check w.buf ==
      @[
        0x00'u8,
        0x00,
        0x00,
        0xFA, # throttleTimeMs = 250
        0x00,
        0x00,
        0x00,
        0x00, # topics.len = 0
        0x00,
        0x00, # errorCode = 0
      ]
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(OffsetFetchResponse, version = 3'i16)
    check decoded.throttleTimeMs == 250'i32
    check decoded.errorCode == keNone
    check r.remaining == 0

suite "ListOffsets wire decode":
  ## ListOffsets request/response wire decode (v1-v2). v2 adds isolation_level
  ## (int8) to the request and throttle_time_ms to the response. The
  ## isolationLevel field uses nimType="IsolationLevel" so the enum value
  ## encodes as its underlying int8.

  test "request v1 omits isolationLevel":
    var w = initKafkaWriter()
    w.encode(
      ListOffsetsRequest(
        replicaId: -1'i32,
        isolationLevel: ilReadUncommitted,
        topics: @[
          ListOffsetsTopicReq(
            topic: "t",
            partitions: @[ListOffsetsPartitionReq(partition: 0'i32, timestamp: -1'i64)],
          )
        ],
      ),
      version = 1'i16,
    )
    check w.buf ==
      @[
        0xFF'u8,
        0xFF,
        0xFF,
        0xFF, # replicaId = -1
        0x00,
        0x00,
        0x00,
        0x01, # topics.len
        0x00,
        0x01,
        byte('t'),
        0x00,
        0x00,
        0x00,
        0x01, # partitions.len
        0x00,
        0x00,
        0x00,
        0x00, # partition = 0
        0xFF'u8,
        0xFF,
        0xFF,
        0xFF,
        0xFF,
        0xFF,
        0xFF,
        0xFF, # timestamp = -1
      ]

  test "request v2 emits isolationLevel":
    var w = initKafkaWriter()
    w.encode(
      ListOffsetsRequest(
        replicaId: -1'i32, isolationLevel: ilReadCommitted, topics: @[]
      ),
      version = 2'i16,
    )
    check w.buf ==
      @[
        0xFF'u8,
        0xFF,
        0xFF,
        0xFF, # replicaId
        0x01, # isolationLevel = 1
        0x00,
        0x00,
        0x00,
        0x00, # topics.len = 0
      ]

  test "response v2 round-trip":
    let original = ListOffsetsResponse(
      throttleTimeMs: 50'i32,
      topics: @[
        ListOffsetsTopicResp(
          topic: "t",
          partitions: @[
            ListOffsetsPartitionResp(
              partition: 0'i32, errorCode: keNone, timestamp: 1234'i64, offset: 5678'i64
            )
          ],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(original, version = 2'i16)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(ListOffsetsResponse, version = 2'i16)
    check decoded.throttleTimeMs == 50'i32
    check decoded.topics[0].partitions[0].timestamp == 1234'i64
    check decoded.topics[0].partitions[0].offset == 5678'i64
    check r.remaining == 0

  test "request v2 rejects unknown isolationLevel with KafkaProtocolError":
    # Wire byte outside {0, 1} would raise an uncatchable RangeDefect if the
    # decoder used a raw `IsolationLevel(int8)` conversion.
    let payload = @[
      0xFF'u8,
      0xFF,
      0xFF,
      0xFF, # replicaId
      0x02'u8, # isolationLevel = 2 (invalid)
      0x00,
      0x00,
      0x00,
      0x00, # topics.len = 0
    ]
    var r = initKafkaReader(payload)
    expectKafkaProtocolError:
      discard r.decode(ListOffsetsRequest, version = 2'i16)

  test "FetchRequest rejects unknown isolationLevel with KafkaProtocolError":
    let payload = @[
      0xFF'u8,
      0xFF,
      0xFF,
      0xFF, # replicaId
      0x00'u8,
      0x00,
      0x00,
      0x00, # maxWaitMs
      0x00'u8,
      0x00,
      0x00,
      0x00, # minBytes
      0x00'u8,
      0x00,
      0x00,
      0x00, # maxBytes
      0x7F'u8, # isolationLevel = 127 (invalid)
      0x00'u8,
      0x00,
      0x00,
      0x00, # topics.len = 0
    ]
    var r = initKafkaReader(payload)
    expectKafkaProtocolError:
      discard r.decode(FetchRequest, version = 4'i16)

suite "OffsetForLeaderEpoch wire decode":
  ## OffsetForLeaderEpoch request/response wire decode (v2-v3, KIP-320). v3
  ## prepends replicaId (consumer sends -2) to the request. Response partition
  ## fields are errorCode-then-partition, the reverse of ListOffsets.

  test "request v2 omits replicaId, carries currentLeaderEpoch":
    var w = initKafkaWriter()
    w.encode(
      OffsetForLeaderEpochRequest(
        replicaId: -2'i32,
        topics: @[
          OffsetForLeaderTopicReq(
            topic: "t",
            partitions: @[
              OffsetForLeaderPartitionReq(
                partition: 0'i32, currentLeaderEpoch: 5'i32, leaderEpoch: 3'i32
              )
            ],
          )
        ],
      ),
      version = 2'i16,
    )
    check w.buf ==
      @[
        0x00'u8,
        0x00,
        0x00,
        0x01, # topics.len = 1 (no replicaId at v2)
        0x00,
        0x01,
        byte('t'), # topic "t"
        0x00,
        0x00,
        0x00,
        0x01, # partitions.len = 1
        0x00,
        0x00,
        0x00,
        0x00, # partition = 0
        0x00,
        0x00,
        0x00,
        0x05, # currentLeaderEpoch = 5
        0x00,
        0x00,
        0x00,
        0x03, # leaderEpoch = 3
      ]

  test "request v3 prepends replicaId = -2":
    var w = initKafkaWriter()
    w.encode(
      OffsetForLeaderEpochRequest(
        replicaId: -2'i32,
        topics: @[
          OffsetForLeaderTopicReq(
            topic: "t",
            partitions: @[
              OffsetForLeaderPartitionReq(
                partition: 0'i32, currentLeaderEpoch: 5'i32, leaderEpoch: 3'i32
              )
            ],
          )
        ],
      ),
      version = 3'i16,
    )
    check w.buf ==
      @[
        0xFF'u8,
        0xFF,
        0xFF,
        0xFE, # replicaId = -2
        0x00,
        0x00,
        0x00,
        0x01, # topics.len = 1
        0x00,
        0x01,
        byte('t'), # topic "t"
        0x00,
        0x00,
        0x00,
        0x01, # partitions.len = 1
        0x00,
        0x00,
        0x00,
        0x00, # partition = 0
        0x00,
        0x00,
        0x00,
        0x05, # currentLeaderEpoch = 5
        0x00,
        0x00,
        0x00,
        0x03, # leaderEpoch = 3
      ]

  test "response v2 decode pins errorCode-before-partition order":
    # Hand-built bytes; field order is the wire gotcha this suite guards.
    let bytes = @[
      0x00'u8,
      0x00,
      0x00,
      0x00, # throttle_time_ms = 0
      0x00,
      0x00,
      0x00,
      0x01, # topics.len = 1
      0x00,
      0x01,
      byte('t'), # topic "t"
      0x00,
      0x00,
      0x00,
      0x01, # partitions.len = 1
      0x00,
      0x00, # error_code = 0 (keNone)
      0x00,
      0x00,
      0x00,
      0x07, # partition = 7
      0x00,
      0x00,
      0x00,
      0x05, # leader_epoch = 5
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x64, # end_offset = 100
    ]
    var r = initKafkaReader(bytes)
    let decoded = r.decode(OffsetForLeaderEpochResponse, version = 2'i16)
    check decoded.throttleTimeMs == 0'i32
    check decoded.topics[0].partitions[0].errorCode == keNone
    check decoded.topics[0].partitions[0].partition == 7'i32
    check decoded.topics[0].partitions[0].leaderEpoch == 5'i32
    check decoded.topics[0].partitions[0].endOffset == 100'i64
    check r.remaining == 0

  test "response v3 round-trip":
    let original = OffsetForLeaderEpochResponse(
      throttleTimeMs: 25'i32,
      topics: @[
        OffsetForLeaderTopicResp(
          topic: "t",
          partitions: @[
            OffsetForLeaderPartitionResp(
              errorCode: keNone, partition: 0'i32, leaderEpoch: 4'i32, endOffset: 42'i64
            )
          ],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(original, version = 3'i16)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(OffsetForLeaderEpochResponse, version = 3'i16)
    check decoded.throttleTimeMs == 25'i32
    check decoded.topics[0].partitions[0].leaderEpoch == 4'i32
    check decoded.topics[0].partitions[0].endOffset == 42'i64
    check r.remaining == 0

suite "FindCoordinator wire decode":
  ## FindCoordinator request/response wire decode (v0-v1). v1 adds
  ## coordinatorType (request) and throttle_time_ms + error_message (response).
  ## error_message uses discardOnRead so the generated type doesn't surface a
  ## duplicate of the errorCode signal.

  test "request v0 omits coordinatorType":
    var w = initKafkaWriter()
    w.encode(FindCoordinatorRequest(key: "g1", coordinatorType: 0'i8))
    check w.buf == @[0x00'u8, 0x02, byte('g'), byte('1')]

  test "request v1 emits coordinatorType":
    var w = initKafkaWriter()
    w.encode(FindCoordinatorRequest(key: "tx", coordinatorType: 1'i8), version = 1'i16)
    check w.buf == @[0x00'u8, 0x02, byte('t'), byte('x'), 0x01]

  test "response v0 round-trip":
    var w = initKafkaWriter()
    w.encode(
      FindCoordinatorResponse(
        throttleTimeMs: 0, errorCode: keNone, nodeId: 1'i32, host: "h", port: 9092'i32
      )
    )
    check w.buf ==
      @[
        0x00'u8,
        0x00, # errorCode
        0x00,
        0x00,
        0x00,
        0x01, # nodeId
        0x00,
        0x01,
        byte('h'), # host
        0x00,
        0x00,
        0x23,
        0x84, # port = 9092
      ]
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(FindCoordinatorResponse)
    check decoded.errorCode == keNone
    check decoded.nodeId == 1'i32
    check decoded.host == "h"
    check decoded.port == 9092'i32
    check r.remaining == 0

  test "response v1 writes a null error_message placeholder":
    var w = initKafkaWriter()
    w.encode(
      FindCoordinatorResponse(
        throttleTimeMs: 100'i32,
        errorCode: keGroupCoordinatorNotAvailable,
        nodeId: -1'i32,
        host: "",
        port: -1'i32,
      ),
      version = 1'i16,
    )
    check w.buf ==
      @[
        0x00'u8,
        0x00,
        0x00,
        0x64, # throttleTimeMs = 100
        0x00,
        0x0F, # errorCode = 15
        0xFF'u8,
        0xFF, # error_message null placeholder
        0xFF,
        0xFF,
        0xFF,
        0xFF, # nodeId = -1
        0x00,
        0x00, # host = ""
        0xFF,
        0xFF,
        0xFF,
        0xFF, # port = -1
      ]
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(FindCoordinatorResponse, version = 1'i16)
    check decoded.throttleTimeMs == 100'i32
    check decoded.errorCode == keGroupCoordinatorNotAvailable
    check decoded.nodeId == -1'i32
    check r.remaining == 0

  test "response v1 advances past a non-null error_message":
    var w = initKafkaWriter()
    w.writeInt32(0'i32) # throttleTimeMs
    w.writeInt16(15'i16) # errorCode
    w.writeNullableString(some("not available")) # error_message
    w.writeInt32(-1'i32) # nodeId
    w.writeString("") # host
    w.writeInt32(-1'i32) # port
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(FindCoordinatorResponse, version = 1'i16)
    check decoded.errorCode == keGroupCoordinatorNotAvailable
    check decoded.host == ""
    check r.remaining == 0

suite "JoinGroup wire decode":
  ## JoinGroup request/response wire decode (v1-v2). v2 prepends
  ## throttle_time_ms to the response. Both directions carry a nested array of
  ## (id, metadata:bytes) tuples.

  test "request v1 encode pins wire layout":
    var w = initKafkaWriter()
    w.encode(
      JoinGroupRequest(
        groupId: "g",
        sessionTimeoutMs: 30000'i32,
        rebalanceTimeoutMs: 60000'i32,
        memberId: "",
        protocolType: "consumer",
        protocols: @[JoinGroupProtocol(name: "range", metadata: @[0x00'u8, 0x01])],
      ),
      version = 1'i16,
    )
    check w.buf ==
      @[
        0x00'u8,
        0x01,
        byte('g'), # groupId
        0x00,
        0x00,
        0x75,
        0x30, # sessionTimeoutMs = 30000
        0x00,
        0x00,
        0xEA,
        0x60, # rebalanceTimeoutMs = 60000
        0x00,
        0x00, # memberId = ""
        0x00,
        0x08,
        byte('c'),
        byte('o'),
        byte('n'),
        byte('s'),
        byte('u'),
        byte('m'),
        byte('e'),
        byte('r'), # protocolType
        0x00,
        0x00,
        0x00,
        0x01, # protocols.len
        0x00,
        0x05,
        byte('r'),
        byte('a'),
        byte('n'),
        byte('g'),
        byte('e'),
        0x00,
        0x00,
        0x00,
        0x02,
        0x00,
        0x01, # metadata bytes
      ]

  test "response v1 round-trip with one member":
    let original = JoinGroupResponse(
      throttleTimeMs: 0,
      errorCode: keNone,
      generationId: 7'i32,
      protocolName: "range",
      leader: "L",
      memberId: "M",
      members: @[JoinGroupMember(memberId: "M", metadata: @[0xAA'u8])],
    )
    var w = initKafkaWriter()
    w.encode(original, version = 1'i16)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(JoinGroupResponse, version = 1'i16)
    check decoded.errorCode == keNone
    check decoded.generationId == 7'i32
    check decoded.protocolName == "range"
    check decoded.members.len == 1
    check decoded.members[0].memberId == "M"
    check decoded.members[0].metadata == @[0xAA'u8]
    check r.remaining == 0

  test "response v2 prepends throttleTimeMs":
    let original = JoinGroupResponse(
      throttleTimeMs: 250'i32,
      errorCode: keNone,
      generationId: 1'i32,
      protocolName: "range",
      leader: "L",
      memberId: "M",
      members: @[],
    )
    var w = initKafkaWriter()
    w.encode(original, version = 2'i16)
    var r = initKafkaReader(w.buf)
    check r.readInt32() == 250'i32
    r = initKafkaReader(w.buf)
    let decoded = r.decode(JoinGroupResponse, version = 2'i16)
    check decoded.throttleTimeMs == 250'i32
    check decoded.members.len == 0
    check r.remaining == 0

suite "SyncGroup wire decode":
  ## SyncGroup request/response wire decode (v0-v1). The request carries an
  ## array of (memberId, assignment) tuples. The response's assignment is
  ## modelled as nullable bytes because brokers send -1 length alongside
  ## rebalance-in-progress errors.

  test "request v0 with one assignment is byte-stable":
    var w = initKafkaWriter()
    w.encode(
      SyncGroupRequest(
        groupId: "g",
        generationId: 5'i32,
        memberId: "m",
        assignments: @[SyncGroupAssignment(memberId: "m", assignment: @[0xAA'u8, 0xBB])],
      )
    )
    check w.buf ==
      @[
        0x00'u8,
        0x01,
        byte('g'), # groupId
        0x00,
        0x00,
        0x00,
        0x05, # generationId
        0x00,
        0x01,
        byte('m'), # memberId
        0x00,
        0x00,
        0x00,
        0x01, # assignments.len
        0x00,
        0x01,
        byte('m'), # nested memberId
        0x00,
        0x00,
        0x00,
        0x02,
        0xAA,
        0xBB, # assignment bytes
      ]

  test "response v0 round-trip with assignment":
    let original = SyncGroupResponse(
      throttleTimeMs: 0, errorCode: keNone, assignment: some(@[0x01'u8, 0x02])
    )
    var w = initKafkaWriter()
    w.encode(original)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(SyncGroupResponse)
    check decoded.errorCode == keNone
    check decoded.assignment == some(@[0x01'u8, 0x02])
    check r.remaining == 0

  test "response v1 with null assignment (rebalance-in-progress)":
    let original = SyncGroupResponse(
      throttleTimeMs: 0, errorCode: keRebalanceInProgress, assignment: none(seq[byte])
    )
    var w = initKafkaWriter()
    w.encode(original, version = 1'i16)
    check w.buf ==
      @[
        0x00'u8,
        0x00,
        0x00,
        0x00, # throttleTimeMs
        0x00,
        0x1B, # errorCode = 27
        0xFF'u8,
        0xFF,
        0xFF,
        0xFF, # assignment = null (-1)
      ]
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(SyncGroupResponse, version = 1'i16)
    check decoded.errorCode == keRebalanceInProgress
    check decoded.assignment.isNone
    check r.remaining == 0

suite "EndTxnResponse wire decode":
  ## EndTxnResponse wire decode. Two-field response (throttleTimeMs
  ## + errorCode); the tests here decode v0 payloads only.

  test "decode v0 wire payload":
    let bytes = @[
      0x00'u8,
      0x00,
      0x00,
      0x32, # throttleTimeMs = 50
      0x00,
      0x00, # errorCode = 0
    ]
    var r = initKafkaReader(bytes)
    let resp = r.decode(EndTxnResponse)
    check resp.throttleTimeMs == 50'i32
    check resp.errorCode == keNone
    check r.remaining == 0

  test "decode v0 with INVALID_TXN_STATE":
    let bytes = @[
      0x00'u8,
      0x00,
      0x00,
      0x00, # throttleTimeMs = 0
      0x00,
      0x30, # errorCode = 48 (INVALID_TXN_STATE)
    ]
    var r = initKafkaReader(bytes)
    let resp = r.decode(EndTxnResponse)
    check resp.errorCode == keInvalidTxnState

const dummyDiscardSchema = MessageDef(
  name: "DummyDiscardMsg",
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
      name: "errorMessage",
      kind: fkNullableString,
      sinceVersion: 1,
      untilVersion: -1,
      discardOnRead: true,
    ),
    FieldDef(name: "nodeId", kind: fkInt32, sinceVersion: -1, untilVersion: -1),
  ],
)

defineKafkaMessage(dummyDiscardSchema)

suite "schema_gen: discardOnRead":
  ## discardOnRead: a scalar read off the wire (to keep the next field
  ## aligned) but never surfaced on the generated type; encoding writes a
  ## null/zero placeholder. Models FindCoordinatorResponse v1's errorMessage.

  test "generated type omits the discarded field":
    let msg = DummyDiscardMsg(errorCode: keNone, nodeId: 7'i32)
    check msg.errorCode == keNone
    check msg.nodeId == 7'i32
    # The discarded field must not exist on the generated type; if it did,
    # `compiles` would return true and this regression test would fire.
    check not compiles(msg.errorMessage)

  test "decode v0 skips the gated discard field entirely":
    # v0 has no errorMessage on the wire, matching the source-level absence.
    let bytes = @[
      0x00'u8,
      0x00, # errorCode (int16)
      0x00,
      0x00,
      0x00,
      0x05, # nodeId (int32) = 5
    ]
    var r = initKafkaReader(bytes)
    let decoded = r.decode(DummyDiscardMsg, version = 0'i16)
    check decoded.errorCode == keNone
    check decoded.nodeId == 5'i32
    check r.remaining == 0

  test "decode v1 consumes errorMessage bytes and lands on nodeId":
    # v1 wire: errorCode(int16) + errorMessage(int16 length + bytes) + nodeId(int32).
    let bytes = @[
      0x00'u8,
      0x06, # errorCode = 6 (keNotLeaderOrFollower)
      0x00,
      0x05, # errorMessage length = 5
      byte('h'),
      byte('e'),
      byte('l'),
      byte('l'),
      byte('o'), # errorMessage payload
      0x00,
      0x00,
      0x00,
      0x2A, # nodeId = 42
    ]
    var r = initKafkaReader(bytes)
    let decoded = r.decode(DummyDiscardMsg, version = 1'i16)
    check decoded.errorCode == keNotLeaderOrFollower
    check decoded.nodeId == 42'i32
    check r.remaining == 0

  test "decode v1 with null errorMessage still advances correctly":
    let bytes = @[
      0x00'u8,
      0x00, # errorCode = 0
      0xFF'u8,
      0xFF, # errorMessage length = -1 (null)
      0x00,
      0x00,
      0x00,
      0x01, # nodeId = 1
    ]
    var r = initKafkaReader(bytes)
    let decoded = r.decode(DummyDiscardMsg, version = 1'i16)
    check decoded.errorCode == keNone
    check decoded.nodeId == 1'i32
    check r.remaining == 0

  test "encode v0 omits the discarded field (gated by version)":
    var w = initKafkaWriter()
    w.encode(DummyDiscardMsg(errorCode: keNone, nodeId: 9'i32), version = 0'i16)
    check w.buf ==
      @[
        0x00'u8,
        0x00, # errorCode
        0x00,
        0x00,
        0x00,
        0x09, # nodeId
      ]

  test "encode v1 writes a null placeholder for the discarded field":
    var w = initKafkaWriter()
    w.encode(DummyDiscardMsg(errorCode: keNone, nodeId: 9'i32), version = 1'i16)
    check w.buf ==
      @[
        0x00'u8,
        0x00, # errorCode
        0xFF'u8,
        0xFF, # placeholder errorMessage = null
        0x00,
        0x00,
        0x00,
        0x09, # nodeId
      ]

  test "v1 round-trip works through the placeholder":
    let original = DummyDiscardMsg(errorCode: keNotCoordinator, nodeId: 12345'i32)
    var w = initKafkaWriter()
    w.encode(original, version = 1'i16)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(DummyDiscardMsg, version = 1'i16)
    check decoded.errorCode == original.errorCode
    check decoded.nodeId == original.nodeId
    check r.remaining == 0

const allKindsDiscardSchema = MessageDef(
  name: "AllKindsDiscardMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[
    FieldDef(name: "dI8", kind: fkInt8, discardOnRead: true),
    FieldDef(name: "dI16", kind: fkInt16, discardOnRead: true),
    FieldDef(name: "dI32", kind: fkInt32, discardOnRead: true),
    FieldDef(name: "dI64", kind: fkInt64, discardOnRead: true),
    FieldDef(name: "dBool", kind: fkBool, discardOnRead: true),
    FieldDef(name: "dStr", kind: fkString, discardOnRead: true),
    FieldDef(name: "dBytes", kind: fkBytes, discardOnRead: true),
    FieldDef(name: "dNBytes", kind: fkNullableBytes, discardOnRead: true),
    FieldDef(name: "dNullableString", kind: fkNullableString, discardOnRead: true),
    FieldDef(name: "dUuid", kind: fkUuid, discardOnRead: true),
    FieldDef(name: "dVarInt", kind: fkVarInt, discardOnRead: true),
    FieldDef(name: "dVarBytes", kind: fkNullableVarBytes, discardOnRead: true),
    FieldDef(name: "dVarString", kind: fkVarString, discardOnRead: true),
    FieldDef(name: "marker", kind: fkInt32),
  ],
)

defineKafkaMessage(allKindsDiscardSchema)

suite "schema_gen: discardOnRead exhaustive kinds":
  ## discardOnRead: exhaustive FieldKind placeholder coverage. Every scalar
  ## kind is a discarded field, pinning its zero-value placeholder bytes; the
  ## trailing `marker` confirms the reader advanced through all of them.

  test "generated type only contains the non-discarded marker":
    let msg = AllKindsDiscardMsg(marker: 99'i32)
    check msg.marker == 99'i32
    check not compiles(msg.dI8)
    check not compiles(msg.dI16)
    check not compiles(msg.dI32)
    check not compiles(msg.dI64)
    check not compiles(msg.dBool)
    check not compiles(msg.dStr)
    check not compiles(msg.dBytes)
    check not compiles(msg.dNBytes)
    check not compiles(msg.dNullableString)
    check not compiles(msg.dUuid)
    check not compiles(msg.dVarInt)
    check not compiles(msg.dVarBytes)
    check not compiles(msg.dVarString)

  test "encode emits zero/null placeholder for every kind":
    var w = initKafkaWriter()
    w.encode(AllKindsDiscardMsg(marker: 0x01020304'i32))
    var expected: seq[byte] = @[]
    expected.add 0x00'u8 # int8 = 0
    expected.add @[0x00'u8, 0x00] # int16 = 0
    expected.add @[0x00'u8, 0x00, 0x00, 0x00] # int32 = 0
    expected.add @[0x00'u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00] # int64 = 0
    expected.add 0x00'u8 # bool = false
    expected.add @[0x00'u8, 0x00] # string len = 0, no bytes
    expected.add @[0x00'u8, 0x00, 0x00, 0x00] # bytes len = 0, no bytes
    expected.add @[0xFF'u8, 0xFF, 0xFF, 0xFF] # nullable bytes len = -1 (null)
    expected.add @[0xFF'u8, 0xFF] # nullable string len = -1 (null)
    for _ in 0 ..< 16: # UUID = 16 zero bytes
      expected.add 0x00'u8
    expected.add 0x00'u8 # varint = 0
    expected.add 0x01'u8 # nullable varbytes = null
    expected.add 0x00'u8 # varstring length = 0
    expected.add @[0x01'u8, 0x02, 0x03, 0x04] # marker = 0x01020304
    check w.buf == expected

  test "decode reads each placeholder and lands on marker":
    var w = initKafkaWriter()
    # Write non-zero values for each discarded field so we can prove the
    # decoder consumed them (rather than the test passing because everything
    # happened to be zero already).
    w.writeInt8(0x7F'i8)
    w.writeInt16(0x1234'i16)
    w.writeInt32(0x11223344'i32)
    w.writeInt64(0x1122334455667788'i64)
    w.writeBool(true)
    w.writeString("hello")
    w.writeBytes(@[0xAA'u8, 0xBB])
    w.writeNullableBytes(some(@[0xCC'u8, 0xDD, 0xEE]))
    w.writeNullableString(some("dropped"))
    var uuidBytes: array[16, byte]
    for i in 0 ..< 16:
      uuidBytes[i] = byte(i + 1)
    w.writeUuid(uuidBytes)
    w.encodeVarint(0x7F'i64)
    w.encodeVarBytes(some(@[0x99'u8, 0xAA]))
    w.encodeVarString("var")
    w.writeInt32(42'i32) # marker
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(AllKindsDiscardMsg)
    check decoded.marker == 42'i32
    check r.remaining == 0

  test "round-trip preserves marker through every placeholder":
    let original = AllKindsDiscardMsg(marker: 0x7EADBEEF'i32)
    var w = initKafkaWriter()
    w.encode(original)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(AllKindsDiscardMsg)
    check decoded.marker == original.marker
    check r.remaining == 0

const nestedDiscardSchema = MessageDef(
  name: "NestedDiscardMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[
    FieldDef(
      name: "items",
      kind: fkArray,
      nestedTypeName: "NestedDiscardItem",
      nested: @[
        FieldDef(name: "id", kind: fkInt32),
        FieldDef(name: "skipped", kind: fkNullableString, discardOnRead: true),
        FieldDef(name: "tag", kind: fkInt16),
      ],
    )
  ],
)

defineKafkaMessage(nestedDiscardSchema)

suite "schema_gen: discardOnRead inside nested struct array":
  ## discardOnRead inside a nested struct array element (a discarded field
  ## between two regular ones) — exercises the recursive decode path so the
  ## feature is not silently top-level-only.

  test "nested element type omits the discarded field":
    let item = NestedDiscardItem(id: 1'i32, tag: 2'i16)
    check item.id == 1'i32
    check item.tag == 2'i16
    check not compiles(item.skipped)

  test "decode consumes the nested placeholder and lands on tag":
    # items array = 2 elements, each with int32 id + nullable-string skipped + int16 tag.
    var w = initKafkaWriter()
    w.writeArrayLen(2'i32)
    w.writeInt32(10'i32)
    w.writeNullableString(some("dropped-1"))
    w.writeInt16(100'i16)
    w.writeInt32(20'i32)
    w.writeNullableString(none(string))
    w.writeInt16(200'i16)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(NestedDiscardMsg)
    check decoded.items.len == 2
    check decoded.items[0].id == 10'i32
    check decoded.items[0].tag == 100'i16
    check decoded.items[1].id == 20'i32
    check decoded.items[1].tag == 200'i16
    check r.remaining == 0

  test "encode writes a null placeholder for the nested discarded field":
    let msg = NestedDiscardMsg(
      items: @[
        NestedDiscardItem(id: 7'i32, tag: 70'i16),
        NestedDiscardItem(id: 8'i32, tag: 80'i16),
      ]
    )
    var w = initKafkaWriter()
    w.encode(msg)
    check w.buf ==
      @[
        0x00'u8,
        0x00,
        0x00,
        0x02, # array len = 2
        0x00,
        0x00,
        0x00,
        0x07, # id = 7
        0xFF'u8,
        0xFF, # skipped placeholder = null
        0x00,
        0x46, # tag = 70
        0x00,
        0x00,
        0x00,
        0x08, # id = 8
        0xFF'u8,
        0xFF, # skipped placeholder = null
        0x00,
        0x50, # tag = 80
      ]

  test "nested round-trip preserves id and tag through the placeholder":
    let original = NestedDiscardMsg(
      items: @[
        NestedDiscardItem(id: 111'i32, tag: 11'i16),
        NestedDiscardItem(id: 222'i32, tag: 22'i16),
        NestedDiscardItem(id: 333'i32, tag: 33'i16),
      ]
    )
    var w = initKafkaWriter()
    w.encode(original)
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(NestedDiscardMsg)
    check decoded.items.len == original.items.len
    for i in 0 ..< original.items.len:
      check decoded.items[i].id == original.items[i].id
      check decoded.items[i].tag == original.items[i].tag
    check r.remaining == 0

const badDiscardArraySchema = MessageDef(
  name: "BadDiscardArrayMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields:
    @[FieldDef(name: "arr", kind: fkArray, elementKind: fkInt32, discardOnRead: true)],
)

const badDiscardTaggedSchema = MessageDef(
  name: "BadDiscardTaggedMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: 0,
  fields: @[FieldDef(name: "tagged", kind: fkInt32, taggedId: 0, discardOnRead: true)],
)

const badElementTypeNameNoEnvelopeSchema = MessageDef(
  name: "BadElementTypeNameMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[FieldDef(name: "records", kind: fkArray, elementTypeName: "Record")],
    # envelope defaults to ekNone → standard codegen would silently emit an
    # fkInt8 codec (elementKind default); the compile-time guard must fire.
)

suite "schema_gen: discardOnRead compile-time rejections":
  ## discardOnRead cannot combine with fkArray or taggedId >= 0. Negative
  ## tests: the schemas must NOT compile (`compiles` is false when the macro
  ## doAsserts during expansion), so a true result means the guard regressed.

  test "discardOnRead + fkArray fails to compile":
    check not compiles(
      (
        block:
          defineKafkaMessage(badDiscardArraySchema)
      )
    )

  test "discardOnRead + taggedId fails to compile":
    check not compiles(
      (
        block:
          defineKafkaMessage(badDiscardTaggedSchema)
      )
    )

  test "elementTypeName without an envelope fails to compile":
    check not compiles(
      (
        block:
          defineKafkaMessage(badElementTypeNameNoEnvelopeSchema)
      )
    )

const badSinceAfterUntilSchema = MessageDef(
  name: "BadSinceAfterUntilMsg",
  apiKey: -1,
  maxVersion: 2,
  flexibleSince: -1,
  fields: @[FieldDef(name: "x", kind: fkInt16, sinceVersion: 2, untilVersion: 1)],
)

const badNestedSincePastMaxSchema = MessageDef(
  name: "BadNestedSincePastMaxMsg",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: -1,
  fields: @[
    FieldDef(
      name: "items",
      kind: fkArray,
      nestedTypeName: "BadNestedSincePastMaxItem",
      nested: @[FieldDef(name: "x", kind: fkInt32, sinceVersion: 2, untilVersion: -1)],
    )
  ],
)

const badFlexibleSincePastMaxSchema = MessageDef(
  name: "BadFlexibleSincePastMaxMsg",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: 2,
  fields: @[FieldDef(name: "x", kind: fkInt16, sinceVersion: -1, untilVersion: -1)],
)

suite "schema_gen: version range compile-time rejections":
  ## Inverted or unreachable version gates make encode and decode omit the same
  ## field, so round-trip cannot catch them. `compiles` is false when the macro
  ## doAsserts during expansion.

  test "sinceVersion after untilVersion fails to compile":
    check not compiles(
      (
        block:
          defineKafkaMessage(badSinceAfterUntilSchema)
      )
    )

  test "nested sinceVersion past maxVersion fails to compile":
    check not compiles(
      (
        block:
          defineKafkaMessage(badNestedSincePastMaxSchema)
      )
    )

  test "flexibleSince past maxVersion fails to compile":
    check not compiles(
      (
        block:
          defineKafkaMessage(badFlexibleSincePastMaxSchema)
      )
    )

const badEnumNoCtorSchema = MessageDef(
  name: "BadEnumNoCtorMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[FieldDef(name: "x", kind: fkInt8, nimType: "IsolationLevel")],
)

const badNestedEnumNoCtorSchema = MessageDef(
  name: "BadNestedEnumNoCtorMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[
    FieldDef(
      name: "items",
      kind: fkArray,
      nestedTypeName: "BadNestedEnumNoCtorItem",
      nested: @[FieldDef(name: "x", kind: fkInt8, nimType: "IsolationLevel")],
    )
  ],
)

suite "schema_gen: nimTypeCtor compile-time rejections":
  ## A raw EnumType(wire) conversion raises an uncatchable RangeDefect, so the
  ## KafkaError contract does not hold. Distinct aliases without a ctor
  ## (KafkaErrorCode, used by the dummyDiscardSchema message) must still
  ## compile. `compiles` is false when the expansion emits `{.error.}` for
  ## `when <nimType> is enum`.

  test "enum nimType without nimTypeCtor fails to compile":
    check not compiles(
      (
        block:
          defineKafkaMessage(badEnumNoCtorSchema)
      )
    )

  test "nested enum nimType without nimTypeCtor fails to compile":
    check not compiles(
      (
        block:
          defineKafkaMessage(badNestedEnumNoCtorSchema)
      )
    )

const badTaggedNoFlexSchema = MessageDef(
  name: "BadTaggedNoFlexMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[FieldDef(name: "tagged", kind: fkInt32, taggedId: 0)],
)

const badNestedTaggedNoFlexSchema = MessageDef(
  name: "BadNestedTaggedNoFlexMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[
    FieldDef(
      name: "items",
      kind: fkArray,
      nestedTypeName: "BadNestedTaggedNoFlexItem",
      nested: @[FieldDef(name: "tagged", kind: fkInt32, taggedId: 0)],
    )
  ],
)

suite "schema_gen: tagged fields require flexible":
  ## Tagged fields without flexibleSince make encode and decode omit the same
  ## tag, so round-trip cannot catch them. Top-level tags are already rejected
  ## by defineKafkaMessage; nested taggedId was not. `compiles` is false when
  ## the macro doAsserts during expansion.

  test "top-level taggedId without flexibleSince fails to compile":
    check not compiles(
      (
        block:
          defineKafkaMessage(badTaggedNoFlexSchema)
      )
    )

  test "nested taggedId without flexibleSince fails to compile":
    check not compiles(
      (
        block:
          defineKafkaMessage(badNestedTaggedNoFlexSchema)
      )
    )

const badUntilOutOfRangeSchema = MessageDef(
  name: "BadUntilOutOfRangeMsg",
  apiKey: -1,
  maxVersion: 1,
  flexibleSince: -1,
  fields: @[
    FieldDef(
      name: "x", kind: fkInt16, sinceVersion: 0, untilVersion: int16.high.int + 1
    )
  ],
)

const badTagBeforeFlexSchema = MessageDef(
  name: "BadTagBeforeFlexMsg",
  apiKey: -1,
  maxVersion: 5,
  flexibleSince: 5,
  fields:
    @[FieldDef(name: "x", kind: fkInt32, sinceVersion: 0, untilVersion: 4, taggedId: 0)],
)

const badOutOfRangeVersionSchema = MessageDef(
  name: "BadOutOfRangeVersionMsg",
  apiKey: -1,
  maxVersion: int16.high.int + 1,
  flexibleSince: -1,
  fields: @[FieldDef(name: "x", kind: fkInt16, sinceVersion: -1, untilVersion: -1)],
)

const badArrayNestedNameSchema = MessageDef(
  name: "BadArrayNestedNameMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[FieldDef(name: "items", kind: fkArray, nestedTypeName: "MissingNestedType")],
)

const badArrayElementArraySchema = MessageDef(
  name: "BadArrayElementArrayMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[FieldDef(name: "items", kind: fkArray, elementKind: fkArray)],
)

const badExternalElementSchema = MessageDef(
  name: "BadExternalElementMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  envelope: ekRecordBatchV2,
  fields: @[FieldDef(name: "records", kind: fkArray, elementTypeName: "Other")],
)

const badTaggedArraySchema = MessageDef(
  name: "BadTaggedArrayMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: 0,
  fields: @[FieldDef(name: "items", kind: fkArray, taggedId: 0)],
)

const badDuplicateTagSchema = MessageDef(
  name: "BadDuplicateTagMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: 0,
  fields: @[
    FieldDef(name: "left", kind: fkInt32, taggedId: 3),
    FieldDef(name: "right", kind: fkInt32, taggedId: 3),
  ],
)

const badCtorWithoutTypeSchema = MessageDef(
  name: "BadCtorWithoutTypeMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[FieldDef(name: "x", kind: fkInt16, nimTypeCtor: "toIsolationLevel")],
)

const badNegativeTagIdSchema = MessageDef(
  name: "BadNegativeTagIdMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: 0,
  fields: @[FieldDef(name: "x", kind: fkInt32, taggedId: -2)],
)

const badNullableScalarSchema = MessageDef(
  name: "BadNullableScalarMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[FieldDef(name: "x", kind: fkString, nullable: true)],
)

const badArrayCountScalarSchema = MessageDef(
  name: "BadArrayCountScalarMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[FieldDef(name: "x", kind: fkString, arrayCountKind: ackVarint)],
)

const badAckVarintNullableArraySchema = MessageDef(
  name: "BadAckVarintNullableArrayMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields:
    @[FieldDef(name: "items", kind: fkArray, nullable: true, arrayCountKind: ackVarint)],
)

const badDeltaKindSchema = MessageDef(
  name: "BadDeltaKindMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  contextParams: @[ContextParam(name: "base", typ: "int64")],
  fields: @[FieldDef(name: "x", kind: fkInt32, deltaFrom: "base")],
)

const badNonIntOverrideSchema = MessageDef(
  name: "BadNonIntOverrideMsg",
  apiKey: -1,
  maxVersion: 0,
  flexibleSince: -1,
  fields: @[FieldDef(name: "x", kind: fkString, nimType: "CustomString")],
)

suite "schema_gen: MessageDef contract validation":
  test "untilVersion outside int16 fails to compile":
    check not compiles(
      (
        block:
          defineKafkaMessage(badUntilOutOfRangeSchema)
      )
    )

  test "version constants outside int16 fail to compile":
    check not compiles(
      (
        block:
          defineKafkaMessage(badOutOfRangeVersionSchema)
      )
    )

  test "tagged fields must overlap the flexible version range":
    check not compiles(
      (
        block:
          defineKafkaMessage(badTagBeforeFlexSchema)
      )
    )

  test "array metadata must describe a real element shape":
    check not compiles(
      (
        block:
          defineKafkaMessage(badArrayNestedNameSchema)
      )
    )
    check not compiles(
      (
        block:
          defineKafkaMessage(badArrayElementArraySchema)
      )
    )
    check not compiles(
      (
        block:
          defineKafkaMessage(badExternalElementSchema)
      )
    )

  test "tagged fields require a supported scalar kind":
    check not compiles(
      (
        block:
          defineKafkaMessage(badTaggedArraySchema)
      )
    )

  test "duplicate tagged ids fail to compile":
    check not compiles(
      (
        block:
          defineKafkaMessage(badDuplicateTagSchema)
      )
    )

  test "nimTypeCtor requires a matching nimType":
    check not compiles(
      (
        block:
          defineKafkaMessage(badCtorWithoutTypeSchema)
      )
    )

  test "invalid FieldDef metadata is rejected":
    check not compiles(
      (
        block:
          defineKafkaMessage(badNegativeTagIdSchema)
      )
    )
    check not compiles(
      (
        block:
          defineKafkaMessage(badNullableScalarSchema)
      )
    )
    check not compiles(
      (
        block:
          defineKafkaMessage(badArrayCountScalarSchema)
      )
    )
    check not compiles(
      (
        block:
          defineKafkaMessage(badAckVarintNullableArraySchema)
      )
    )

  test "deltaFrom and nimType overrides require compatible kinds":
    check not compiles(
      (
        block:
          defineKafkaMessage(badDeltaKindSchema)
      )
    )
    check not compiles(
      (
        block:
          defineKafkaMessage(badNonIntOverrideSchema)
      )
    )

suite "Record wire decode":
  ## Record / RecordBatch wire decode (v2). Pins the zigzag varint field
  ## kinds, delta encoding via contextParams + deltaFrom, the varint headers
  ## count, and the ekRecordBatchV2 envelope (fixed header + CRC32C + optional
  ## records-section compression).

  test "encode emits attributes + varint deltas + varbytes key/value + headers count":
    # Build a single Record at offset=10, ts=1500 against baseOffset=10,
    # firstTimestamp=1000. Deltas are 0 and 500 respectively, which zigzag
    # to 0x00 and (500 << 1) = 1000 = 0xE8,0x07 in varint LE.
    let rec = Record(
      offset: 10'i64,
      timestamp: 1500'i64,
      key: some(@[byte('k')]),
      value: some(@[byte('v')]),
      headers: @[],
    )
    var w = initKafkaWriter()
    w.encode(rec, baseOffset = 10'i64, firstTimestamp = 1000'i64)
    check w.buf ==
      @[
        0x00'u8, # attributes (always 0 on v2 records)
        0xE8,
        0x07, # timestampDelta = 500 → zigzag 1000 → varint 0xE8 0x07
        0x00, # offsetDelta = 0
        0x02, # key length: zigzag 1 = 2 → varint 0x02
        byte('k'),
        0x02, # value length
        byte('v'),
        0x00, # headers count (varint 0)
      ]

  test "decode reapplies baseOffset and firstTimestamp to the deltas":
    # Wire payload: deltas 3 and 250 → absolute offset = base + 3, ts =
    # firstTs + 250. Key null, value "vv", one header with null value.
    let body = @[
      0x00'u8, # attributes
      0xF4,
      0x03, # timestampDelta = 250 → zigzag 500 → varint 0xF4 0x03
      0x06, # offsetDelta = 3 → zigzag 6
      0x01, # key length: zigzag -1 → varint 0x01 (null)
      0x04, # value length: zigzag 2 = 4
      byte('v'),
      byte('v'),
      0x02, # headers count = 1
      0x02, # header key length: zigzag 1 = 2
      byte('h'),
      0x01, # header value length: zigzag -1 = null
    ]
    var r = initKafkaReader(body)
    let decoded = r.decode(Record, baseOffset = 100'i64, firstTimestamp = 2000'i64)
    check decoded.offset == 103'i64
    check decoded.timestamp == 2250'i64
    check decoded.key.isNone
    check decoded.value.isSome and decoded.value.get == @[byte('v'), byte('v')]
    check decoded.headers.len == 1
    check decoded.headers[0].key == "h"
    check decoded.headers[0].value.isNone
    check r.remaining == 0

  test "round-trip preserves absolute offset/timestamp through deltas":
    let rec = Record(
      offset: 1_234_567'i64,
      timestamp: 9_999_999_999'i64,
      key: some(@[byte('K'), byte('1')]),
      value: some(@[byte('V'), byte('V')]),
      headers: @[RecordHeader(key: "trace", value: some(@[byte('t'), byte('t')]))],
    )
    var w = initKafkaWriter()
    w.encode(rec, baseOffset = 1_234_000'i64, firstTimestamp = 9_999_999_000'i64)
    var r = initKafkaReader(w.buf)
    let back =
      r.decode(Record, baseOffset = 1_234_000'i64, firstTimestamp = 9_999_999_000'i64)
    check back == rec
    check r.remaining == 0

const recordBatchV2KafkaLongValue =
  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef" &
  "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

proc stringBytes(s: string): seq[byte] =
  cast[seq[byte]](s)

proc recordBatchV2KafkaValue(): RecordBatch =
  RecordBatch(
    baseOffset: 1_234_567'i64,
    partitionLeaderEpoch: 11'i32,
    attributes: 0'i16,
    firstTimestamp: 1_700_000_000_000'i64,
    producerId: 12_345'i64,
    producerEpoch: 7'i16,
    baseSequence: 9'i32,
    records: @[
      Record(
        offset: 1_234_567'i64,
        timestamp: 1_700_000_000_000'i64,
        key: some(stringBytes("key-1")),
        value: some(stringBytes(recordBatchV2KafkaLongValue)),
        headers: @[
          RecordHeader(key: "trace", value: some(stringBytes("abc"))),
          RecordHeader(key: "empty", value: some(newSeq[byte]())),
          RecordHeader(key: "null", value: none(seq[byte])),
        ],
      ),
      Record(
        offset: 1_234_569'i64,
        timestamp: 1_700_000_000_123'i64,
        key: none(seq[byte]),
        value: some(stringBytes("value-2")),
        headers: @[],
      ),
    ],
  )

## Fixed v2 wire vector generated by kafka-clients 3.7.0, an independent
## Kafka client. It contains two records, headers, a non-dense offset delta,
## and a 128-byte value so the record and value lengths exercise multi-byte
## varints. Keep this literal independent from the encoder. The derived batch
## fields are intentionally left unset in the source value so the envelope
## must calculate them.
const recordBatchV2KafkaGolden: seq[byte] = @[
  # header (61 bytes)
  0x00'u8,
  0x00,
  0x00,
  0x00,
  0x00,
  0x12,
  0xD6,
  0x87,
  0x00,
  0x00,
  0x00,
  0xE5,
  0x00,
  0x00,
  0x00,
  0x0B,
  0x02,
  0x72,
  0xF9,
  0xD5,
  0x49,
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0x02,
  0x00,
  0x00,
  0x01,
  0x8B,
  0xCF,
  0xE5,
  0x68,
  0x00,
  0x00,
  0x00,
  0x01,
  0x8B,
  0xCF,
  0xE5,
  0x68,
  0x7B,
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0x30,
  0x39,
  0x00,
  0x07,
  0x00,
  0x00,
  0x00,
  0x09,
  0x00,
  0x00,
  0x00,
  0x02,
  # first record (165 bytes)
  0xC6,
  0x02,
  0x00,
  0x00,
  0x00,
  0x0A,
  0x6B,
  0x65,
  0x79,
  0x2D,
  0x31,
  0x80,
  0x02,
  0x30,
  0x31,
  0x32,
  0x33,
  0x34,
  0x35,
  0x36,
  0x37,
  0x38,
  0x39,
  0x61,
  0x62,
  0x63,
  0x64,
  0x65,
  0x66,
  0x30,
  0x31,
  0x32,
  0x33,
  0x34,
  0x35,
  0x36,
  0x37,
  0x38,
  0x39,
  0x61,
  0x62,
  0x63,
  0x64,
  0x65,
  0x66,
  0x30,
  0x31,
  0x32,
  0x33,
  0x34,
  0x35,
  0x36,
  0x37,
  0x38,
  0x39,
  0x61,
  0x62,
  0x63,
  0x64,
  0x65,
  0x66,
  0x30,
  0x31,
  0x32,
  0x33,
  0x34,
  0x35,
  0x36,
  0x37,
  0x38,
  0x39,
  0x61,
  0x62,
  0x63,
  0x64,
  0x65,
  0x66,
  0x30,
  0x31,
  0x32,
  0x33,
  0x34,
  0x35,
  0x36,
  0x37,
  0x38,
  0x39,
  0x61,
  0x62,
  0x63,
  0x64,
  0x65,
  0x66,
  0x30,
  0x31,
  0x32,
  0x33,
  0x34,
  0x35,
  0x36,
  0x37,
  0x38,
  0x39,
  0x61,
  0x62,
  0x63,
  0x64,
  0x65,
  0x66,
  0x30,
  0x31,
  0x32,
  0x33,
  0x34,
  0x35,
  0x36,
  0x37,
  0x38,
  0x39,
  0x61,
  0x62,
  0x63,
  0x64,
  0x65,
  0x66,
  0x30,
  0x31,
  0x32,
  0x33,
  0x34,
  0x35,
  0x36,
  0x37,
  0x38,
  0x39,
  0x61,
  0x62,
  0x63,
  0x64,
  0x65,
  0x66,
  0x06,
  0x0A,
  0x74,
  0x72,
  0x61,
  0x63,
  0x65,
  0x06,
  0x61,
  0x62,
  0x63,
  0x0A,
  0x65,
  0x6D,
  0x70,
  0x74,
  0x79,
  0x00,
  0x08,
  0x6E,
  0x75,
  0x6C,
  0x6C,
  0x01,
  # second record (15 bytes)
  0x1C,
  0x00,
  0xF6,
  0x01,
  0x04,
  0x01,
  0x0E,
  0x76,
  0x61,
  0x6C,
  0x75,
  0x65,
  0x2D,
  0x32,
  0x00,
]

## Kafka 3.7.0 MemoryRecordsBuilder fixture using CompressionType.GZIP. The
## compressed bytes are intentionally fixed independently of this encoder.
proc recordBatchV2KafkaGzipValue(): RecordBatch =
  RecordBatch(
    baseOffset: 1_000'i64,
    partitionLeaderEpoch: 7'i32,
    attributes: 1'i16,
    firstTimestamp: 1_700_000_000_000'i64,
    producerId: 42'i64,
    producerEpoch: 3'i16,
    baseSequence: 5'i32,
    records: @[
      Record(
        offset: 1_000'i64,
        timestamp: 1_700_000_000_000'i64,
        key: some(stringBytes("key")),
        value: some(stringBytes("value-" & "0123456789abcdef".repeat(8))),
        headers: @[RecordHeader(key: "h", value: some(stringBytes("v")))],
      ),
      Record(
        offset: 1_001'i64,
        timestamp: 1_700_000_000_123'i64,
        key: none(seq[byte]),
        value: some(stringBytes("second")),
        headers: @[],
      ),
    ],
  )

const recordBatchV2KafkaGzipGolden: seq[byte] = @[
  # header (61 bytes)
  0x00'u8,
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0x03,
  0xE8,
  0x00,
  0x00,
  0x00,
  0x7B,
  0x00,
  0x00,
  0x00,
  0x07,
  0x02,
  0xE0,
  0xAD,
  0xA3,
  0xF2,
  0x00,
  0x01,
  0x00,
  0x00,
  0x00,
  0x01,
  0x00,
  0x00,
  0x01,
  0x8B,
  0xCF,
  0xE5,
  0x68,
  0x00,
  0x00,
  0x00,
  0x01,
  0x8B,
  0xCF,
  0xE5,
  0x68,
  0x7B,
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0x2A,
  0x00,
  0x03,
  0x00,
  0x00,
  0x00,
  0x05,
  0x00,
  0x00,
  0x00,
  0x02,
  # gzip-compressed records section
  0x1F,
  0x8B,
  0x08,
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0x00,
  0xFF,
  0x5B,
  0xC1,
  0xC4,
  0xC0,
  0xC0,
  0xC0,
  0x96,
  0x9D,
  0x5A,
  0xD9,
  0xC3,
  0x54,
  0x96,
  0x98,
  0x53,
  0x9A,
  0xAA,
  0x6B,
  0x60,
  0x68,
  0x64,
  0x6C,
  0x62,
  0x6A,
  0x66,
  0x6E,
  0x61,
  0x99,
  0x98,
  0x94,
  0x9C,
  0x92,
  0x9A,
  0x46,
  0x6F,
  0x3E,
  0x13,
  0x53,
  0x06,
  0x53,
  0x99,
  0x14,
  0xC3,
  0x37,
  0x46,
  0x26,
  0x46,
  0x9E,
  0xE2,
  0xD4,
  0xE4,
  0xFC,
  0xBC,
  0x14,
  0x06,
  0x00,
  0x19,
  0x4C,
  0xA5,
  0x83,
  0xA4,
  0x00,
  0x00,
  0x00,
]

const recordBatchV2KafkaSnappyGolden: seq[byte] = @[
  0x00'u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8, 0x00, 0x00, 0x00, 0x83, 0x00, 0x00,
  0x00, 0x07, 0x02, 0xB5, 0x0D, 0xB6, 0xE9, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00,
  0x00, 0x01, 0x8B, 0xCF, 0xE5, 0x68, 0x00, 0x00, 0x00, 0x01, 0x8B, 0xCF, 0xE5, 0x68,
  0x7B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x03, 0x00, 0x00, 0x00,
  0x05, 0x00, 0x00, 0x00, 0x02, 0x82, 0x53, 0x4E, 0x41, 0x50, 0x50, 0x59, 0x00, 0x00,
  0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x3E, 0xA4, 0x01, 0x80,
  0xA8, 0x02, 0x00, 0x00, 0x00, 0x06, 0x6B, 0x65, 0x79, 0x8C, 0x02, 0x76, 0x61, 0x6C,
  0x75, 0x65, 0x2D, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61,
  0x62, 0x63, 0x64, 0x65, 0x66, 0xFE, 0x10, 0x00, 0xBE, 0x10, 0x00, 0x48, 0x02, 0x02,
  0x68, 0x02, 0x76, 0x1A, 0x00, 0xF6, 0x01, 0x02, 0x01, 0x0C, 0x73, 0x65, 0x63, 0x6F,
  0x6E, 0x64, 0x00,
]

const recordBatchV2KafkaLz4Golden: seq[byte] = @[
  0x00'u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8, 0x00, 0x00, 0x00, 0x7B, 0x00, 0x00,
  0x00, 0x07, 0x02, 0xC2, 0xCB, 0x84, 0x50, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00,
  0x00, 0x01, 0x8B, 0xCF, 0xE5, 0x68, 0x00, 0x00, 0x00, 0x01, 0x8B, 0xCF, 0xE5, 0x68,
  0x7B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x03, 0x00, 0x00, 0x00,
  0x05, 0x00, 0x00, 0x00, 0x02, 0x04, 0x22, 0x4D, 0x18, 0x60, 0x40, 0x82, 0x3B, 0x00,
  0x00, 0x00, 0xFF, 0x12, 0xA8, 0x02, 0x00, 0x00, 0x00, 0x06, 0x6B, 0x65, 0x79, 0x8C,
  0x02, 0x76, 0x61, 0x6C, 0x75, 0x65, 0x2D, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36,
  0x37, 0x38, 0x39, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x10, 0x00, 0x5D, 0xF0, 0x04,
  0x02, 0x02, 0x68, 0x02, 0x76, 0x1A, 0x00, 0xF6, 0x01, 0x02, 0x01, 0x0C, 0x73, 0x65,
  0x63, 0x6F, 0x6E, 0x64, 0x00, 0x00, 0x00, 0x00, 0x00,
]

## The uncompressed record section comes from the Kafka 3.7.0 client and the
## content-sized zstd frame is produced independently with zstd-jni. This keeps
## the fixture valid for the streaming zstd decoder (ZSTD_decompressStream)
## while avoiding a dependency on a particular streaming-frame policy.
const recordBatchV2KafkaZstdGolden: seq[byte] = @[
  0x00'u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8, 0x00, 0x00, 0x00, 0x76, 0x00, 0x00,
  0x00, 0x07, 0x02, 0xEC, 0x42, 0x6E, 0xD2, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00,
  0x00, 0x01, 0x8B, 0xCF, 0xE5, 0x68, 0x00, 0x00, 0x00, 0x01, 0x8B, 0xCF, 0xE5, 0x68,
  0x7B, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A, 0x00, 0x03, 0x00, 0x00, 0x00,
  0x05, 0x00, 0x00, 0x00, 0x02, 0x28, 0xB5, 0x2F, 0xFD, 0x20, 0xA4, 0xE5, 0x01, 0x00,
  0x44, 0x03, 0xA8, 0x02, 0x00, 0x00, 0x00, 0x06, 0x6B, 0x65, 0x79, 0x8C, 0x02, 0x76,
  0x61, 0x6C, 0x75, 0x65, 0x2D, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38,
  0x39, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66, 0x02, 0x02, 0x68, 0x02, 0x76, 0x1A, 0x00,
  0xF6, 0x01, 0x02, 0x01, 0x0C, 0x73, 0x65, 0x63, 0x6F, 0x6E, 0x64, 0x00, 0x01, 0x00,
  0x69, 0x93, 0xCE, 0x27,
]

template checkRecordBatchV2KafkaCompressedFixture(data: untyped, codec: int16) =
  var reader = initKafkaReader(data)
  let batch = reader.decode(RecordBatch)
  check reader.remaining == 0
  check batch.baseOffset == 1_000'i64
  check batch.partitionLeaderEpoch == 7'i32
  check batch.attributes == codec
  check batch.records.len == 2
  check batch.records[0].offset == 1_000'i64
  check batch.records[0].timestamp == 1_700_000_000_000'i64
  check batch.records[0].key == some(stringBytes("key"))
  check batch.records[0].value ==
    some(stringBytes("value-" & "0123456789abcdef".repeat(8)))
  check batch.records[0].headers ==
    @[RecordHeader(key: "h", value: some(stringBytes("v")))]
  check batch.records[1].offset == 1_001'i64
  check batch.records[1].timestamp == 1_700_000_000_123'i64
  check batch.records[1].key.isNone
  check batch.records[1].value == some(stringBytes("second"))
  check batch.records[1].headers.len == 0

suite "RecordBatch wire decode (v2 envelope)":
  test "matches the independent v2 wire fixture byte-for-byte":
    var w = initKafkaWriter()
    w.encode(recordBatchV2KafkaValue())
    check w.buf == recordBatchV2KafkaGolden

  test "decodes the independent v2 wire fixture":
    check recordBatchV2KafkaGolden.len == 241
    var r = initKafkaReader(recordBatchV2KafkaGolden)
    let batch = r.decode(RecordBatch)
    check batch.baseOffset == 1_234_567'i64
    check batch.batchLength == 229'i32
    check batch.partitionLeaderEpoch == 11'i32
    check batch.magic == 2'i8
    check batch.crc == cast[int32](0x72F9D549'u32)
    check batch.attributes == 0'i16
    check batch.lastOffsetDelta == 2'i32
    check batch.firstTimestamp == 1_700_000_000_000'i64
    check batch.maxTimestamp == 1_700_000_000_123'i64
    check batch.producerId == 12_345'i64
    check batch.producerEpoch == 7'i16
    check batch.baseSequence == 9'i32
    check batch.records.len == 2
    check batch.records[0].offset == 1_234_567'i64
    check batch.records[0].timestamp == 1_700_000_000_000'i64
    check batch.records[0].key == some(stringBytes("key-1"))
    check batch.records[0].value == some(stringBytes(recordBatchV2KafkaLongValue))
    check batch.records[0].headers.len == 3
    check batch.records[0].headers[0] ==
      RecordHeader(key: "trace", value: some(stringBytes("abc")))
    check batch.records[0].headers[1].key == "empty"
    check batch.records[0].headers[1].value == some(newSeq[byte]())
    check batch.records[0].headers[2].key == "null"
    check batch.records[0].headers[2].value.isNone
    check batch.records[1].offset == 1_234_569'i64
    check batch.records[1].timestamp == 1_700_000_000_123'i64
    check batch.records[1].key.isNone
    check batch.records[1].value == some(stringBytes("value-2"))
    check batch.records[1].headers.len == 0
    check r.remaining == 0

  test "matches the independent gzip v2 wire fixture byte-for-byte":
    var w = initKafkaWriter()
    w.encode(recordBatchV2KafkaGzipValue())
    check w.buf == recordBatchV2KafkaGzipGolden

  test "decodes the independent gzip v2 wire fixture":
    check recordBatchV2KafkaGzipGolden.len == 135
    var r = initKafkaReader(recordBatchV2KafkaGzipGolden)
    let batch = r.decode(RecordBatch)
    check batch.baseOffset == 1_000'i64
    check batch.attributes == 1'i16
    check batch.records.len == 2
    check batch.records[0].offset == 1_000'i64
    check batch.records[0].timestamp == 1_700_000_000_000'i64
    check batch.records[0].key == some(stringBytes("key"))
    check batch.records[0].value ==
      some(stringBytes("value-" & "0123456789abcdef".repeat(8)))
    check batch.records[0].headers ==
      @[RecordHeader(key: "h", value: some(stringBytes("v")))]
    check batch.records[1].offset == 1_001'i64
    check batch.records[1].timestamp == 1_700_000_000_123'i64
    check batch.records[1].key.isNone
    check batch.records[1].value == some(stringBytes("second"))
    check batch.records[1].headers.len == 0
    check r.remaining == 0

  test "decodes the independent snappy v2 wire fixture":
    check recordBatchV2KafkaSnappyGolden.len == 143
    checkRecordBatchV2KafkaCompressedFixture(recordBatchV2KafkaSnappyGolden, 2'i16)

  test "decodes the independent lz4 v2 wire fixture":
    check recordBatchV2KafkaLz4Golden.len == 135
    checkRecordBatchV2KafkaCompressedFixture(recordBatchV2KafkaLz4Golden, 3'i16)

  test "decodes the independent zstd v2 wire fixture":
    check recordBatchV2KafkaZstdGolden.len == 130
    checkRecordBatchV2KafkaCompressedFixture(recordBatchV2KafkaZstdGolden, 4'i16)

  test "uncompressed batch encodes the fixed header + CRC + one record":
    # Single-record batch, attributes=0 (no compression), no transactional
    # bit. baseOffset=0, firstTimestamp=0 means the record's deltas equal
    # its absolute values (both zero here).
    let batch = RecordBatch(
      baseOffset: 0'i64,
      partitionLeaderEpoch: 0'i32,
      attributes: 0'i16,
      firstTimestamp: 0'i64,
      producerId: -1'i64,
      producerEpoch: -1'i16,
      baseSequence: -1'i32,
      records: @[
        Record(
          offset: 0'i64,
          timestamp: 0'i64,
          key: none(seq[byte]),
          value: some(@[byte('a')]),
          headers: @[],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(batch)
    # Round-trip first — the cheapest correctness check.
    var r = initKafkaReader(w.buf)
    let back = r.decode(RecordBatch)
    check back.baseOffset == 0'i64
    check back.magic == 2'i8
    check back.attributes == 0'i16
    check back.lastOffsetDelta == 0'i32
    check back.firstTimestamp == 0'i64
    check back.maxTimestamp == 0'i64
    check back.producerId == -1'i64
    check back.producerEpoch == -1'i16
    check back.baseSequence == -1'i32
    check back.records.len == 1
    check back.records[0].offset == 0'i64
    check back.records[0].timestamp == 0'i64
    check back.records[0].key.isNone
    check back.records[0].value.isSome and back.records[0].value.get == @[byte('a')]
    # Pin the fixed-header slots checked below so a wire-format drift on them
    # fails the test; CRC and the varint-framed fields are covered elsewhere.
    # batchLength = total - 12 (baseOffset(8) + batchLength(4)).
    check w.buf[0 .. 7] == @[0x00'u8, 0, 0, 0, 0, 0, 0, 0] # baseOffset
    check w.buf[12 .. 15] == @[0x00'u8, 0, 0, 0] # partitionLeaderEpoch
    check w.buf[16] == 0x02'u8 # magic
    check w.buf[21 .. 22] == @[0x00'u8, 0x00] # attributes
    check w.buf[23 .. 26] == @[0x00'u8, 0, 0, 0] # lastOffsetDelta
    check w.buf[51 .. 52] == @[0xFF'u8, 0xFF] # producerEpoch (-1 i16)
    check w.buf[53 .. 56] == @[0xFF'u8, 0xFF, 0xFF, 0xFF] # baseSequence (-1)
    check w.buf[57 .. 60] == @[0x00'u8, 0, 0, 1] # numRecords = 1

  test "lastOffsetDelta and maxTimestamp are derived from the records":
    # The encoder MUST overwrite user-supplied values for these computed
    # fields — passing nonsense to the constructor proves the envelope
    # ignores them.
    let batch = RecordBatch(
      baseOffset: 100'i64,
      lastOffsetDelta: 9999'i32, # garbage — should be replaced
      firstTimestamp: 1000'i64,
      maxTimestamp: 7777'i64, # garbage — should be replaced
      producerId: -1'i64,
      producerEpoch: -1'i16,
      baseSequence: -1'i32,
      records: @[
        Record(
          offset: 100'i64,
          timestamp: 1000'i64,
          key: none(seq[byte]),
          value: none(seq[byte]),
          headers: @[],
        ),
        Record(
          offset: 101'i64,
          timestamp: 1500'i64, # out-of-order timestamp still contributes to maxTs
          key: none(seq[byte]),
          value: none(seq[byte]),
          headers: @[],
        ),
        Record(
          offset: 102'i64,
          timestamp: 1200'i64,
          key: none(seq[byte]),
          value: none(seq[byte]),
          headers: @[],
        ),
      ],
    )
    var w = initKafkaWriter()
    w.encode(batch)
    var r = initKafkaReader(w.buf)
    let back = r.decode(RecordBatch)
    check back.lastOffsetDelta == 2'i32
    check back.maxTimestamp == 1500'i64
    check back.records.len == 3
    check back.records[0].offset == 100'i64
    check back.records[1].offset == 101'i64
    check back.records[2].offset == 102'i64

  test "lastOffsetDelta matches the max per-record offset delta":
    # Regression: lastOffsetDelta was hard-coded to `records.len - 1`; a
    # non-dense caller (e.g. offsets 100, 102 with baseOffset 100) sent a
    # header contradicting the schema-driven per-record deltas (0, 2).
    # Now derived from max(record.offset) - baseOffset.
    let batch = RecordBatch(
      baseOffset: 100'i64,
      firstTimestamp: 1000'i64,
      producerId: -1'i64,
      producerEpoch: -1'i16,
      baseSequence: -1'i32,
      records: @[
        Record(
          offset: 100'i64,
          timestamp: 1000'i64,
          key: none(seq[byte]),
          value: none(seq[byte]),
          headers: @[],
        ),
        Record(
          offset: 102'i64,
          timestamp: 1000'i64,
          key: none(seq[byte]),
          value: none(seq[byte]),
          headers: @[],
        ),
      ],
    )
    var w = initKafkaWriter()
    w.encode(batch)
    var r = initKafkaReader(w.buf)
    let back = r.decode(RecordBatch)
    check back.lastOffsetDelta == 2'i32
    check back.records.len == 2
    check back.records[0].offset == 100'i64
    check back.records[1].offset == 102'i64

  test "decode skips a non-magic-2 batch without raising":
    # Synthesize a magic=1 (legacy MessageSet) header so the decoder
    # walks past it instead of trying to verify CRC. The remaining bytes
    # are arbitrary filler — the decoder must skip exactly
    # `12 + batchLength` bytes from startPos.
    var w = initKafkaWriter()
    w.writeInt64(42'i64) # baseOffset
    w.writeInt32(20'i32) # batchLength: 20 more bytes follow
    w.writeInt32(0'i32) # partitionLeaderEpoch
    w.writeInt8(1'i8) # magic = 1 (legacy, not v2)
    # 15 bytes of filler (20 batchLength - 4 partLeaderEpoch - 1 magic).
    for _ in 0 ..< 15:
      w.buf.add(0xAB'u8)
    var r = initKafkaReader(w.buf)
    let back = r.decode(RecordBatch)
    check back.baseOffset == 42'i64
    check back.batchLength == 20'i32
    check back.magic == 1'i8
    check back.records.len == 0
    check r.remaining == 0

  test "decode rejects a CRC mismatch":
    let batch = RecordBatch(
      baseOffset: 0'i64,
      producerId: -1'i64,
      producerEpoch: -1'i16,
      baseSequence: -1'i32,
      records: @[
        Record(
          offset: 0'i64,
          timestamp: 0'i64,
          key: none(seq[byte]),
          value: some(@[byte('z')]),
          headers: @[],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(batch)
    # Flip a single bit inside the CRC-covered payload (attributes onward
    # — byte 21 is the first attributes byte) so CRC verification fails.
    w.buf[21] = w.buf[21] xor 0x01'u8
    var r = initKafkaReader(w.buf)
    expectKafkaProtocolError:
      discard r.decode(RecordBatch)

  test "decode rejects a records section that outgrows numRecords":
    # Regression: the uncompressed path snapped the reader forward over the
    # leftover bytes ("so the outer fetch loop frames the next batch") instead
    # of rejecting the batch, so a CRC-valid batch whose records section holds
    # more records than its `numRecords` was accepted as clean — while the
    # header's `lastOffsetDelta` still described the wider offset span, and the
    # consumer advances its cursor by that span. The compressed path has always
    # raised on the same condition; this pins the uncompressed mirror.
    let batch = RecordBatch(
      baseOffset: 0'i64,
      firstTimestamp: 0'i64,
      producerId: -1'i64,
      producerEpoch: -1'i16,
      baseSequence: -1'i32,
      records: @[
        Record(
          offset: 0'i64,
          timestamp: 0'i64,
          key: none(seq[byte]),
          value: some(stringBytes("aa")),
          headers: @[],
        ),
        Record(
          offset: 1'i64,
          timestamp: 0'i64,
          key: none(seq[byte]),
          value: some(stringBytes("bb")),
          headers: @[],
        ),
      ],
    )
    var w = initKafkaWriter()
    w.encode(batch)
    # numRecords lives at bytes 57..60 (baseOffset 8 + batchLength 4 +
    # partitionLeaderEpoch 4 + magic 1 + crc 4 + the 36-byte post-CRC v2
    # header). Claim 1 so the second record's bytes are
    # left as trailing data, then re-stamp the CRC so the batch is otherwise
    # perfectly valid.
    let one: uint32 = 1
    let oneBE = toBytesBE(one)
    for i in 0 ..< 4:
      w.buf[57 + i] = oneBE[i]
    let crcBE = toBytesBE(crc32c(w.buf.toOpenArray(21, w.buf.len - 1)))
    for i in 0 ..< 4:
      w.buf[17 + i] = crcBE[i]
    var r = initKafkaReader(w.buf)
    var raised = false
    try:
      discard r.decode(RecordBatch)
    except KafkaProtocolError as e:
      raised = true
      check "unread trailing byte" in e.msg
    check raised

  test "decode accepts a records section that exactly fills numRecords":
    # The counter-check must not cost the well-formed batch: same fixture as
    # the test above, left unpatched, decodes both records and consumes the
    # batch exactly (no trailing byte, no over-read).
    let batch = RecordBatch(
      baseOffset: 0'i64,
      firstTimestamp: 0'i64,
      producerId: -1'i64,
      producerEpoch: -1'i16,
      baseSequence: -1'i32,
      records: @[
        Record(
          offset: 0'i64,
          timestamp: 0'i64,
          key: none(seq[byte]),
          value: some(stringBytes("aa")),
          headers: @[],
        ),
        Record(
          offset: 1'i64,
          timestamp: 0'i64,
          key: none(seq[byte]),
          value: some(stringBytes("bb")),
          headers: @[],
        ),
      ],
    )
    var w = initKafkaWriter()
    w.encode(batch)
    var r = initKafkaReader(w.buf)
    let back = r.decode(RecordBatch)
    check back.records.len == 2
    check back.lastOffsetDelta == 1'i32
    check r.remaining == 0

  test "round-trip preserves a multi-record batch with headers":
    let batch = RecordBatch(
      baseOffset: 1000'i64,
      partitionLeaderEpoch: 7'i32,
      attributes: 0'i16,
      firstTimestamp: 50_000'i64,
      producerId: 42'i64,
      producerEpoch: 3'i16,
      baseSequence: 5'i32,
      records: @[
        Record(
          offset: 1000'i64,
          timestamp: 50_000'i64,
          key: some(@[byte('a')]),
          value: some(@[byte('A')]),
          headers: @[],
        ),
        Record(
          offset: 1001'i64,
          timestamp: 50_010'i64,
          key: none(seq[byte]),
          value: some(@[byte('B'), byte('B')]),
          headers: @[
            RecordHeader(key: "h1", value: some(@[byte('x')])),
            RecordHeader(key: "h2", value: none(seq[byte])),
          ],
        ),
      ],
    )
    var w = initKafkaWriter()
    w.encode(batch)
    var r = initKafkaReader(w.buf)
    let back = r.decode(RecordBatch)
    check back.records.len == 2
    check back.records[0].offset == 1000'i64
    check back.records[0].timestamp == 50_000'i64
    check back.records[1].offset == 1001'i64
    check back.records[1].timestamp == 50_010'i64
    check back.records[1].headers.len == 2
    check back.records[1].headers[0].key == "h1"
    check back.records[1].headers[0].value.get == @[byte('x')]
    check back.records[1].headers[1].key == "h2"
    check back.records[1].headers[1].value.isNone
    check r.remaining == 0

  test "encode rejects an oversized records section with KafkaProtocolError":
    # Encode-side mirror of the decoder's maxWireBytesLen guard. A value past
    # the bound would otherwise overflow its int32 length field and raise an
    # uncatchable RangeDefect; the encoder must surface a catchable
    # KafkaProtocolError instead. This allocates ~100MB for one value just over
    # maxWireBytesLen — the minimal input that exercises the bound. (The sibling
    # maxWireArrayLen guard on records.len would need >10M records to trigger,
    # which is not feasible to allocate in a unit test, so only the byte-size
    # bound is exercised here.)
    let batch = RecordBatch(
      baseOffset: 0'i64,
      partitionLeaderEpoch: 0'i32,
      attributes: 0'i16, # ccNone — no compression, so recordsBytes stays large
      firstTimestamp: 0'i64,
      producerId: -1'i64,
      producerEpoch: -1'i16,
      baseSequence: -1'i32,
      records: @[
        Record(
          offset: 0'i64,
          timestamp: 0'i64,
          key: none(seq[byte]),
          value: some(newSeq[byte](maxWireBytesLen + 1)),
          headers: @[],
        )
      ],
    )
    var w = initKafkaWriter()
    expectKafkaProtocolError:
      w.encode(batch)

  test "decode above the modeled maxVersion raises KafkaProtocolError":
    var w = initKafkaWriter()
    w.encode(recordBatchV2KafkaValue())
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.decode(RecordBatch, version = 1'i16)

  test "encode above the modeled maxVersion raises KafkaProtocolError":
    var w = initKafkaWriter()
    expect KafkaProtocolError:
      w.encode(recordBatchV2KafkaValue(), version = 1'i16)

  test "decode at the modeled maxVersion accepts version 0":
    var w = initKafkaWriter()
    w.encode(recordBatchV2KafkaValue(), version = 0'i16)
    var r = initKafkaReader(w.buf)
    let back = r.decode(RecordBatch, version = 0'i16)
    check back.records.len == 2
    check r.remaining == 0

suite "MetadataResponse wire decode":
  ## The topic name is a required string here although upstream allows a null
  ## Name at v12+ (answers to topic-id requests). This client requests metadata
  ## by name, so it should never see one; these two tests pin both sides of that
  ## boundary — the compact framing decodes, and a null marker fails loudly
  ## instead of producing a name the metadata cache would key on.

  test "v12 topic names decode from the compact framing":
    var w = initKafkaWriter()
    w.encode(
      MetadataResponse(
        throttleTimeMs: 0'i32, topics: @[TopicMeta(errorCode: keNone, name: "t")]
      ),
      version = 12'i16,
    )
    var r = initKafkaReader(w.buf)
    let decoded = r.decode(MetadataResponse, version = 12'i16)
    check decoded.topics.len == 1
    check decoded.topics[0].name == "t"
    check decoded.topics[0].topicId == default(array[16, byte])
    check r.remaining == 0

  test "a null v12 topic name is rejected, not decoded as empty":
    const name = "topic-null-name"
    var w = initKafkaWriter()
    w.encode(
      MetadataResponse(topics: @[TopicMeta(errorCode: keNone, name: name)]),
      version = 12'i16,
    )
    let at = cast[string](w.buf).find(name)
    check at > 0
    check w.buf[at - 1] == byte(name.len + 1) # compact length prefix
    w.buf[at - 1] = 0x00 # compact null marker (upstream-legal at v12)
    var r = initKafkaReader(w.buf)
    expectKafkaProtocolError:
      discard r.decode(MetadataResponse, version = 12'i16)
