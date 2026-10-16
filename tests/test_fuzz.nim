## Property-based fuzz tests for the wire-format decoders. Contract: on any
## untrusted input every `protocol/` decoder either succeeds or raises a
## `KafkaError` — never a foreign exception, a `Defect` (IndexDefect /
## RangeDefect / overflow), an unbounded allocation, or a crash. The RNG uses a
## fixed seed and each recorded failure carries the offending input as hex, so
## failures reproduce directly; inputs and decompression are bounded to avoid
## an OOM taking down the suite.

import std/[unittest, options, random]

import async_kafka/errors
import async_kafka/protocol/[primitives, messages, compression]

const
  FuzzSeed = 0x5EED_1234
  MaxInputLen = 256
  Versions = [0'i16, 1, 3, 7, 12]
  RespIters = 120 ## random/structured inputs per response type per version
  PrimIters = 1500 ## inputs per primitive-reader sweep
  MutIters = 300 ## mutations per valid corpus entry
  DecompIters = 300 ## inputs per compression codec
  DecompMaxSize = 1_048_576 ## bound decompression memory during fuzzing

proc toHexStr(bytes: openArray[byte]): string =
  const hexChars = "0123456789abcdef"
  result = newStringOfCap(bytes.len * 2)
  for b in bytes:
    result.add(hexChars[int(b shr 4)])
    result.add(hexChars[int(b and 0x0F)])

proc randomBytes(rng: var Rand, maxLen: int): seq[byte] =
  let n = rng.rand(maxLen)
  result = newSeq[byte](n)
  for i in 0 ..< n:
    result[i] = byte(rng.rand(255))

proc structuredBytes(rng: var Rand, maxLen: int): seq[byte] =
  ## Emit chunks that look like Kafka length-prefixed fields so the fuzzer
  ## reaches *past* the first bounds check into nested array/string/record
  ## decoding instead of bouncing off the outermost length validation.
  var w = initKafkaWriter()
  let chunks = rng.rand(10)
  for _ in 0 ..< chunks:
    case rng.rand(7)
    of 0:
      w.writeInt16(int16(rng.rand(int(int16.high)))) # random up to int16.high
    of 1:
      w.writeInt32(int32(rng.rand(2000)) - 1000) # maybe-negative length
    of 2:
      w.writeInt32(int32(rng.rand(int(int32.high)))) # hostile huge length
    of 3:
      w.writeInt8(cast[int8](byte(rng.rand(255))))
    of 4:
      w.writeInt64(rng.rand(int64.high))
    of 5:
      w.encodeVarint(rng.rand(int64.high) - int64(rng.rand(int.high)))
    else:
      for _ in 0 ..< rng.rand(8):
        w.buf.add(byte(rng.rand(255)))
  if w.buf.len > maxLen:
    w.buf.setLen(maxLen)
  w.buf

proc mutate(rng: var Rand, data: seq[byte]): seq[byte] =
  ## Apply a few random edits to a copy: byte overwrites, truncation, growth,
  ## and byte zeroing. Mutating valid frames probes the deep code paths that
  ## pure random bytes rarely reach.
  result = data
  let ops = 1 + rng.rand(5)
  for _ in 0 ..< ops:
    case rng.rand(4)
    of 0:
      if result.len > 0:
        result[rng.rand(result.len - 1)] = byte(rng.rand(255))
    of 1:
      if result.len > 0:
        result.setLen(rng.rand(result.len))
    of 2:
      result.add(byte(rng.rand(255)))
    else:
      if result.len > 0:
        result[rng.rand(result.len - 1)] = 0'u8

proc pickInput(rng: var Rand): seq[byte] =
  case rng.rand(3)
  of 0:
    randomBytes(rng, MaxInputLen)
  of 1:
    structuredBytes(rng, MaxInputLen)
  else:
    mutate(rng, structuredBytes(rng, MaxInputLen div 2))

template expectClean(
    fails: var seq[string], label: string, inp: seq[byte], body: untyped
) =
  ## Run `body`; a `KafkaError` is the contract-satisfying rejection. Anything
  ## else — a foreign CatchableError or a Defect — is a bug, recorded with the
  ## reproducing input rather than aborting the sweep.
  try:
    body
  except KafkaError:
    discard
  except CatchableError as e:
    fails.add(label & ": " & $e.name & ": " & e.msg & " | input=" & toHexStr(inp))
  except Defect as e:
    fails.add(
      label & ": DEFECT " & $e.name & ": " & e.msg & " | input=" & toHexStr(inp)
    )

proc reportFailures(fails: seq[string]) =
  for i, f in fails:
    if i >= 12:
      checkpoint("... and " & $(fails.len - 12) & " more")
      break
    checkpoint(f)
  check fails.len == 0

template fuzzResponses(fails: var seq[string], rng: var Rand, T: typedesc) =
  for ver in Versions:
    for _ in 0 ..< RespIters:
      let inp = pickInput(rng)
      expectClean(fails, $T & " v" & $ver, inp):
        var r = initKafkaReader(inp)
        discard r.decode(T, ver)

template fuzzMutate(
    fails: var seq[string], rng: var Rand, T: typedesc, ver: int16, valid: seq[byte]
) =
  block:
    # Sanity: the unmutated corpus entry must decode, else the corpus is wrong.
    var r = initKafkaReader(valid)
    discard r.decode(T, ver)
  for _ in 0 ..< MutIters:
    let inp = mutate(rng, valid)
    expectClean(fails, $T & " mutate", inp):
      var r = initKafkaReader(inp)
      discard r.decode(T, ver)

proc validApiVersions(): seq[byte] =
  var w = initKafkaWriter()
  w.writeInt16(0)
  w.writeArrayLen(2)
  w.writeInt16(0)
  w.writeInt16(0)
  w.writeInt16(7)
  w.writeInt16(1)
  w.writeInt16(0)
  w.writeInt16(11)
  w.buf

proc validMetadata(): seq[byte] =
  var w = initKafkaWriter()
  w.writeArrayLen(1)
  w.writeInt32(0)
  w.writeString("localhost")
  w.writeInt32(9092)
  w.writeNullableString(none(string))
  w.writeInt32(0)
  w.writeArrayLen(1)
  w.writeInt16(0)
  w.writeString("test-topic")
  w.writeInt8(0)
  w.writeArrayLen(1)
  w.writeInt16(0)
  w.writeInt32(0)
  w.writeInt32(0)
  w.writeArrayLen(1)
  w.writeInt32(0)
  w.writeArrayLen(1)
  w.writeInt32(0)
  w.buf

proc validHeartbeat(): seq[byte] =
  var w = initKafkaWriter()
  w.writeInt16(0)
  w.buf

proc validJoinGroup(): seq[byte] =
  var w = initKafkaWriter()
  w.writeInt16(0)
  w.writeInt32(5)
  w.writeString("range")
  w.writeString("leader-1")
  w.writeString("member-1")
  w.writeArrayLen(1)
  w.writeString("member-1")
  w.writeInt32(3)
  w.buf.add(@[0xAA'u8, 0xBB, 0xCC])
  w.buf

proc validSyncGroup(): seq[byte] =
  var w = initKafkaWriter()
  w.writeInt16(0)
  w.writeInt32(2)
  w.buf.add(@[0x01'u8, 0x02])
  w.buf

proc validListOffsets(): seq[byte] =
  var w = initKafkaWriter()
  w.writeArrayLen(1)
  w.writeString("t1")
  w.writeArrayLen(1)
  w.writeInt32(0)
  w.writeInt16(0)
  w.writeInt64(1000)
  w.writeInt64(42)
  w.buf

proc validRecordBatch(): seq[byte] =
  let records = @[
    Record(
      offset: 0,
      timestamp: 1000,
      key: none(seq[byte]),
      value: some(cast[seq[byte]]("hello")),
      headers: @[],
    )
  ]
  var w = initKafkaWriter()
  w.encode(
    RecordBatch(
      baseOffset: 0'i64,
      firstTimestamp: 1000'i64,
      producerId: -1'i64,
      producerEpoch: -1'i16,
      baseSequence: -1'i32,
      records: records,
    )
  )
  w.buf

suite "Fuzz: primitive readers":
  test "readers reject malformed input without crashing":
    var rng = initRand(FuzzSeed)
    var fails: seq[string]
    for _ in 0 ..< PrimIters:
      let inp = pickInput(rng)

      template prim(label: string, call: untyped) =
        expectClean(fails, label, inp):
          var r {.inject.} = initKafkaReader(inp)
          discard call

      prim("readString", r.readString())
      prim("readNullableString", r.readNullableString())
      prim("readNullableBytes", r.readNullableBytes())
      prim("readLengthPrefixedBytes", r.readLengthPrefixedBytes())
      prim("readArrayLen", r.readArrayLen())
      prim("readNullableArrayLen", r.readNullableArrayLen())
      prim("decodeUvarint", r.decodeUvarint())
      prim("decodeVarint", r.decodeVarint())
      prim("decodeVarBytes", r.decodeVarBytes())
      prim("decodeVarString", r.decodeVarString())
      prim("readCompactString", r.readCompactString())
      prim("readCompactNullableString", r.readCompactNullableString())
      prim("readCompactBytes", r.readCompactBytes())
      prim("readCompactNullableBytes", r.readCompactNullableBytes())
      prim("readCompactArrayLen", r.readCompactArrayLen())

      expectClean(fails, "readBytes", inp):
        var r = initKafkaReader(inp)
        let n =
          if inp.len > 0:
            int(inp[0])
          else:
            0
        discard r.readBytes(n)
    reportFailures(fails)

suite "Fuzz: response decoders":
  test "every response decoder survives arbitrary bytes":
    var rng = initRand(FuzzSeed xor 0x1111)
    var fails: seq[string]
    fuzzResponses(fails, rng, ApiVersionsResponse)
    fuzzResponses(fails, rng, MetadataResponse)
    fuzzResponses(fails, rng, FindCoordinatorResponse)
    fuzzResponses(fails, rng, ListOffsetsResponse)
    fuzzResponses(fails, rng, OffsetForLeaderEpochResponse)
    fuzzResponses(fails, rng, LeaveGroupResponse)
    fuzzResponses(fails, rng, FetchResponse)
    fuzzResponses(fails, rng, JoinGroupResponse)
    fuzzResponses(fails, rng, SyncGroupResponse)
    fuzzResponses(fails, rng, HeartbeatResponse)
    fuzzResponses(fails, rng, OffsetFetchResponse)
    fuzzResponses(fails, rng, OffsetCommitResponse)
    fuzzResponses(fails, rng, ProduceResponse)
    fuzzResponses(fails, rng, InitProducerIdResponse)
    fuzzResponses(fails, rng, AddPartitionsToTxnResponse)
    fuzzResponses(fails, rng, AddOffsetsToTxnResponse)
    fuzzResponses(fails, rng, EndTxnResponse)
    fuzzResponses(fails, rng, TxnOffsetCommitResponse)
    fuzzResponses(fails, rng, SaslHandshakeResponse)
    fuzzResponses(fails, rng, SaslAuthenticateResponse)
    reportFailures(fails)

  test "RecordBatch decoder survives arbitrary bytes":
    var rng = initRand(FuzzSeed xor 0x2222)
    var fails: seq[string]
    for _ in 0 ..< RespIters * 4:
      let inp = pickInput(rng)
      expectClean(fails, "RecordBatch", inp):
        var r = initKafkaReader(inp)
        discard r.decode(RecordBatch)
    reportFailures(fails)

suite "Fuzz: mutated valid frames":
  test "mutating well-formed frames never crashes a decoder":
    var rng = initRand(FuzzSeed xor 0x3333)
    var fails: seq[string]
    fuzzMutate(fails, rng, ApiVersionsResponse, 0, validApiVersions())
    fuzzMutate(fails, rng, MetadataResponse, 0, validMetadata())
    fuzzMutate(fails, rng, HeartbeatResponse, 0, validHeartbeat())
    fuzzMutate(fails, rng, JoinGroupResponse, 0, validJoinGroup())
    fuzzMutate(fails, rng, SyncGroupResponse, 0, validSyncGroup())
    fuzzMutate(fails, rng, ListOffsetsResponse, 0, validListOffsets())
    fuzzMutate(fails, rng, RecordBatch, 0, validRecordBatch())
    reportFailures(fails)

suite "Fuzz: decompression":
  test "decompressRecords never crashes on hostile input":
    var rng = initRand(FuzzSeed xor 0x4444)
    var fails: seq[string]
    for codec in [ccGzip, ccSnappy, ccLz4, ccZstd]:
      for _ in 0 ..< DecompIters:
        let inp = pickInput(rng)
        expectClean(fails, "decompress " & $codec, inp):
          discard decompressRecords(inp, codec, DecompMaxSize)
    reportFailures(fails)
