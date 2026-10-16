import std/[unittest, options, strutils, hashes, tables]

import pkg/supersnappy
import pkg/stew/endians2
# Raw zstd bindings: the zero-content-size regression test builds a frame our
# own zstdCompress (which short-circuits empty input) cannot produce.
import pkg/zstd/compress as rawzstd
import pkg/zstd/decompress as rawzstdd
import pkg/zstd/common as rawzstdcommon

import async_kafka/errors
import async_kafka/protocol/[primitives, api_keys, messages, crc32c, compression, zstd]
import async_kafka/protocol/lz4 as lz4mod

proc toBytes(s: string): seq[byte] =
  cast[seq[byte]](s)

proc repeatByte(c: byte, n: int): seq[byte] =
  result = newSeq[byte](n)
  for i in 0 ..< n:
    result[i] = c

proc peakRssKb(): int =
  ## VmHWM, the high-water mark of resident set size. A plain allocation
  ## reading cannot see this: an allocation made and then released — which is
  ## exactly what a decode that reserves a declared size and immediately fails
  ## does — leaves nothing behind for getOccupiedMem to report, but the pages
  ## were still faulted in. The high-water mark is monotonic for the process, so
  ## one measurement before and one after brackets the peak.
  for line in lines("/proc/self/status"):
    if line.startsWith("VmHWM:"):
      return parseInt(line.splitWhitespace()[1])
  0

proc zstdCompressStreaming(data: openArray[byte], level: cint = 1): seq[byte] =
  ## Compress through the streaming API, which cannot know the output size when
  ## it writes the frame header and therefore omits the content-size field.
  ## zstdCompress uses the one-shot call, which knows the size up front and
  ## always writes it — so this is the only way to produce the frame shape the
  ## streaming decompressor is meant to accept.
  var cstream = ZSTD_createCStream()
  if cstream.isNil:
    raise newException(KafkaError, "zstd compression failed: cannot create stream")
  defer:
    discard ZSTD_freeCStream(cstream)
  let initRes = ZSTD_initCStream(cstream, level)
  if ZSTD_isError(initRes):
    raise newException(
      KafkaError, "zstd compression failed: " & $ZSTD_getErrorName(initRes)
    )

  var scratch = newSeq[byte](int(ZSTD_CStreamOutSize()))
  var inBuf = ZSTD_inBuffer(src: unsafeAddr data[0], size: csize_t(data.len), pos: 0)
  # Phase 1: feed all input. ZSTD_compressStream returns a hint for the next
  # input size and consumes input until `inBuf.pos` reaches `inBuf.size`.
  while inBuf.pos < inBuf.size:
    var outBuf =
      ZSTD_outBuffer(dst: addr scratch[0], size: csize_t(scratch.len), pos: 0)
    let hint = ZSTD_compressStream(cstream, addr outBuf, addr inBuf)
    if ZSTD_isError(hint):
      raise
        newException(KafkaError, "zstd compression failed: " & $ZSTD_getErrorName(hint))
    if outBuf.pos > 0:
      let start = result.len
      result.setLen(start + int(outBuf.pos))
      copyMem(addr result[start], addr scratch[0], int(outBuf.pos))
  # Phase 2: drain. ZSTD_endStream returns 0 once the frame is complete, and
  # must be called again after each call that wrote output.
  var pending = true
  while pending:
    var outBuf =
      ZSTD_outBuffer(dst: addr scratch[0], size: csize_t(scratch.len), pos: 0)
    let remaining = ZSTD_endStream(cstream, addr outBuf)
    if ZSTD_isError(remaining):
      raise newException(
        KafkaError, "zstd compression failed: " & $ZSTD_getErrorName(remaining)
      )
    if outBuf.pos > 0:
      let start = result.len
      result.setLen(start + int(outBuf.pos))
      copyMem(addr result[start], addr scratch[0], int(outBuf.pos))
    pending = remaining > 0

proc batchesFromPartition(p: FetchPartitionResp): seq[RecordBatch] =
  ## Helper for the Fetch suite: parse the raw NULLABLE_BYTES records blob
  ## into RecordBatch objects via the same path the consumer uses.
  if p.recordsRaw.isSome:
    let raw = p.recordsRaw.get
    if raw.len > 0:
      var rdr = initKafkaReader(raw)
      while rdr.remaining >= 12:
        result.add(rdr.decode(RecordBatch))

suite "KafkaWriter/KafkaReader primitives":
  test "int8 round-trip":
    var w = initKafkaWriter()
    w.writeInt8(42'i8)
    w.writeInt8(-1'i8)
    var r = initKafkaReader(w.buf)
    check r.readInt8() == 42'i8
    check r.readInt8() == -1'i8
    check r.remaining == 0

  test "int16 round-trip":
    var w = initKafkaWriter()
    w.writeInt16(256'i16)
    w.writeInt16(-1'i16)
    var r = initKafkaReader(w.buf)
    check r.readInt16() == 256'i16
    check r.readInt16() == -1'i16

  test "int32 round-trip":
    var w = initKafkaWriter()
    w.writeInt32(70000'i32)
    w.writeInt32(-42'i32)
    var r = initKafkaReader(w.buf)
    check r.readInt32() == 70000'i32
    check r.readInt32() == -42'i32

  test "int64 round-trip":
    var w = initKafkaWriter()
    w.writeInt64(int64.high)
    w.writeInt64(-1'i64)
    var r = initKafkaReader(w.buf)
    check r.readInt64() == int64.high
    check r.readInt64() == -1'i64

  test "string round-trip":
    var w = initKafkaWriter()
    w.writeString("hello")
    w.writeString("")
    var r = initKafkaReader(w.buf)
    check r.readString() == "hello"
    check r.readString() == ""

  test "nullable string round-trip":
    var w = initKafkaWriter()
    w.writeNullableString(some("test"))
    w.writeNullableString(none(string))
    var r = initKafkaReader(w.buf)
    check r.readNullableString() == some("test")
    check r.readNullableString() == none(string)

  test "readString preserves high bytes":
    var w = initKafkaWriter()
    let s = "\xFF\x00\x80"
    w.writeString(s)
    var r = initKafkaReader(w.buf)
    let got = r.readString()
    check got.len == 3
    check byte(got[0]) == 0xFF'u8
    check byte(got[1]) == 0x00'u8
    check byte(got[2]) == 0x80'u8

  test "bytes round-trip":
    var w = initKafkaWriter()
    w.writeBytes(@[1'u8, 2, 3])
    var r = initKafkaReader(w.buf)
    let b = r.readNullableBytes()
    check b.isSome
    check b.get == @[1'u8, 2, 3]

  test "nullable bytes null":
    var w = initKafkaWriter()
    w.writeNullableBytes(none(seq[byte]))
    var r = initKafkaReader(w.buf)
    check r.readNullableBytes() == none(seq[byte])

  test "big-endian byte order":
    var w = initKafkaWriter()
    w.writeInt32(0x01020304'i32)
    check w.buf == @[0x01'u8, 0x02, 0x03, 0x04]

  test "buffer underflow raises KafkaProtocolError":
    var r = initKafkaReader(@[0x00'u8])
    expect KafkaProtocolError:
      discard r.readInt32()

  test "bool round-trip":
    var w = initKafkaWriter()
    w.writeBool(true)
    w.writeBool(false)
    var r = initKafkaReader(w.buf)
    check r.readBool() == true
    check r.readBool() == false

  test "readBytes and skip":
    var w = initKafkaWriter()
    w.buf.add(@[1'u8, 2, 3, 4, 5, 6])
    var r = initKafkaReader(w.buf)
    check r.readBytes(3) == @[1'u8, 2, 3]
    r.skip(2)
    check r.readBytes(1) == @[6'u8]
    check r.remaining == 0

  test "skip underflow raises KafkaProtocolError":
    var r = initKafkaReader(@[0x00'u8])
    expect KafkaProtocolError:
      r.skip(5)

  test "negative skip length raises KafkaProtocolError":
    # A negative `n` would rewind the cursor and mask malformed framing as a
    # parse of earlier bytes; it must be rejected rather than honoured.
    var r = initKafkaReader(@[0x01'u8, 0x02, 0x03])
    expect KafkaProtocolError:
      r.skip(-1)

  test "readArrayLen round-trip":
    var w = initKafkaWriter()
    w.writeArrayLen(42)
    var r = initKafkaReader(w.buf)
    check r.readArrayLen() == 42'i32

  test "readArrayLen clamps -1 (null array) to 0":
    var w = initKafkaWriter()
    w.writeArrayLen(-1)
    var r = initKafkaReader(w.buf)
    check r.readArrayLen() == 0'i32

  test "readArrayLen rejects -2":
    var w = initKafkaWriter()
    w.writeInt32(-2'i32)
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.readArrayLen()

  test "checkArrayLenFitsBuffer rejects a count larger than the remaining buffer":
    # Allocation-amplification guard the schema decoder applies before
    # allocating: an element count the remaining bytes cannot hold is malformed
    # and must raise rather than reserve memory from a tiny frame.
    var r = initKafkaReader(@[0x00'u8, 0x01, 0x02, 0x03]) # 4 bytes remaining
    expect KafkaProtocolError:
      r.checkArrayLenFitsBuffer(1_000_000'i32, 1)

  test "checkArrayLenFitsBuffer accepts a count that fits and ignores nulls":
    var r = initKafkaReader(@[0x00'u8, 0x01, 0x02, 0x03]) # 4 bytes remaining
    r.checkArrayLenFitsBuffer(4'i32, 1) # exactly fits — 1-byte elements
    r.checkArrayLenFitsBuffer(0'i32, 1) # empty
    r.checkArrayLenFitsBuffer(-1'i32, 1) # null sentinel — no-op

  test "checkArrayLenFitsBuffer scales the bound by the element's minimum size":
    # The scalar form bounds the COUNT at one byte per element, which is right
    # for a scalar and wrong for a struct: `FetchPartitionResp` is 88 bytes in
    # memory against a 27-byte minimum on the wire, so the count has to be
    # divided by what one element really costs. Without this the divisor is 1
    # and a 10 MB frame can name `maxWireArrayLen` of them.
    var r = initKafkaReader(newSeq[byte](100))
    # 100 bytes at 1 byte/element: 100 fits, 101 does not.
    r.checkArrayLenFitsBuffer(100'i32, 1)
    expect KafkaProtocolError:
      r.checkArrayLenFitsBuffer(101'i32, 1)
    # At 27 bytes/element only 3 fit.
    r.checkArrayLenFitsBuffer(3'i32, 27)
    expect KafkaProtocolError:
      r.checkArrayLenFitsBuffer(4'i32, 27)
    # A non-positive divisor falls back to 1 rather than dividing by zero.
    r.checkArrayLenFitsBuffer(4'i32, 0)

  test "readNullableBytes negative length means null":
    var w = initKafkaWriter()
    w.writeInt32(-2'i32)
    var r = initKafkaReader(w.buf)
    check r.readNullableBytes() == none(seq[byte])

  test "writer len":
    var w = initKafkaWriter()
    check w.len == 0
    w.writeInt32(1)
    check w.len == 4

  test "initKafkaReader from openArray":
    let arr: array[3, byte] = [1'u8, 2, 3]
    var r = initKafkaReader(arr)
    check r.readBytes(3) == @[1'u8, 2, 3]

  test "readBytes rejects a negative length":
    let arr: array[3, byte] = [1'u8, 2, 3]
    var r = initKafkaReader(arr)
    var raised = false
    try:
      discard r.readBytes(-1)
    except KafkaProtocolError as e:
      raised = true
      check "negative read length" in e.msg
    check raised

  test "response-header uvarint rejects an overflowing 10th byte":
    # decodeUvarintAt (via skipResponseHeaderTaggedFields) must apply the same
    # final-byte strictness as decodeUvarint: 0xFF*9 then 0x02 sets a bit above
    # bit 63, which would silently truncate without the shift==63 guard.
    let bad = @[0xFF'u8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02]
    var raised = false
    try:
      discard skipResponseHeaderTaggedFields(bad, 0)
    except KafkaProtocolError as e:
      raised = true
      check "overflow" in e.msg
    check raised

  test "readString null raises KafkaProtocolError":
    var w = initKafkaWriter()
    w.writeInt16(-1'i16)
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.readString()

  test "nullable bytes empty":
    var w = initKafkaWriter()
    w.writeBytes(@[]) # length 0
    var r = initKafkaReader(w.buf)
    let b = r.readNullableBytes()
    check b.isSome
    check b.get.len == 0

  test "peekInt32At reads a length prefix without advancing the cursor":
    var w = initKafkaWriter()
    w.writeInt32(0x01020304'i32)
    w.writeInt32(0x0A0B0C0D'i32)
    var r = initKafkaReader(w.buf)
    check r.peekInt32At(0) == 0x01020304'i32
    check r.peekInt32At(4) == 0x0A0B0C0D'i32
    check r.pos == 0 # peek must not advance the cursor

  test "peekInt32At honours the reader limit (no window escape)":
    # 8 bytes physically present, but `limit` caps the logical window at 4.
    # A peek at offset 4 would read bytes that exist yet sit beyond the
    # window, so it must raise — consistent with remaining/checkRemaining.
    var w = initKafkaWriter()
    w.writeInt32(0x01020304'i32)
    w.writeInt32(0x0A0B0C0D'i32)
    var r = initKafkaReader(w.buf)
    r.limit = 4
    check r.peekInt32At(0) == 0x01020304'i32 # within the window
    expect KafkaProtocolError:
      discard r.peekInt32At(4) # past limit, before data.len

  test "peekInt32At rejects an out-of-bounds offset":
    var w = initKafkaWriter()
    w.writeInt32(0x01020304'i32)
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.peekInt32At(1) # only 4 bytes, 4 needed from offset 1
    expect KafkaProtocolError:
      discard r.peekInt32At(-1)

suite "ApiVersions negotiation":
  test "negotiate overlapping ranges":
    let client = ApiVersionRange(minVersion: 0, maxVersion: 3)
    let broker = ApiVersionRange(minVersion: 1, maxVersion: 5)
    check negotiateVersion(client, broker) == 3

  test "negotiate no overlap":
    let client = ApiVersionRange(minVersion: 4, maxVersion: 5)
    let broker = ApiVersionRange(minVersion: 0, maxVersion: 2)
    check negotiateVersion(client, broker) == -1

  test "negotiate exact match":
    let client = ApiVersionRange(minVersion: 2, maxVersion: 2)
    let broker = ApiVersionRange(minVersion: 2, maxVersion: 2)
    check negotiateVersion(client, broker) == 2

  test "supportedVersion all API keys":
    check supportedVersion(akFetch).minVersion == 4
    check supportedVersion(akFetch).maxVersion == 11
    check supportedVersion(akMetadata).minVersion == 1
    check supportedVersion(akMetadata).maxVersion == 12
    check supportedVersion(akApiVersions).minVersion == 0
    check supportedVersion(akJoinGroup).minVersion == 1
    check supportedVersion(akOffsetCommit).minVersion == 2
    check supportedVersion(akOffsetCommit).maxVersion == 8
    check supportedVersion(akHeartbeat).maxVersion == 1
    check supportedVersion(akSyncGroup).maxVersion == 1
    check supportedVersion(akFindCoordinator).maxVersion == 1
    check supportedVersion(akOffsetFetch).maxVersion == 5
    check supportedVersion(akProduce).minVersion == 3
    check supportedVersion(akProduce).maxVersion == 7
    check supportedVersion(akListOffsets).minVersion == 1
    check supportedVersion(akListOffsets).maxVersion == 2
    check supportedVersion(akLeaveGroup).maxVersion == 1
    check supportedVersion(akJoinGroup).maxVersion == 2
    # Transaction APIs raised to v3 (KIP-482 flexible) for forward compat.
    check supportedVersion(akEndTxn).maxVersion == 3
    check supportedVersion(akAddOffsetsToTxn).maxVersion == 3
    check supportedVersion(akAddPartitionsToTxn).maxVersion == 3
    check supportedVersion(akTxnOffsetCommit).maxVersion == 3

suite "Deserializer":
  test "ApiVersions response decode":
    var w = initKafkaWriter()
    w.writeInt16(0)
    w.writeArrayLen(2)
    w.writeInt16(0)
    w.writeInt16(0)
    w.writeInt16(7)
    w.writeInt16(1)
    w.writeInt16(0)
    w.writeInt16(11)
    var r = initKafkaReader(w.buf)
    let resp = r.decode(ApiVersionsResponse)
    check resp.errorCode == keNone
    check resp.apiKeys.len == 2
    check resp.apiKeys[0].apiKey == 0
    check resp.apiKeys[0].maxVersion == 7
    check resp.apiKeys[1].apiKey == 1
    check resp.apiKeys[1].maxVersion == 11

  test "Metadata response decode":
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
    var r = initKafkaReader(w.buf)
    let resp = r.decode(MetadataResponse)
    check resp.brokers.len == 1
    check resp.brokers[0].host == "localhost"
    check resp.brokers[0].port == 9092
    check resp.topics.len == 1
    check resp.topics[0].name == "test-topic"
    check resp.topics[0].partitions.len == 1

  test "Heartbeat response decode":
    var w = initKafkaWriter()
    w.writeInt16(0)
    var r = initKafkaReader(w.buf)
    let resp = r.decode(HeartbeatResponse)
    check resp.errorCode == keNone

  test "FindCoordinator response decode":
    var w = initKafkaWriter()
    w.writeInt16(0)
    w.writeInt32(1)
    w.writeString("broker1")
    w.writeInt32(9092)
    var r = initKafkaReader(w.buf)
    let resp = r.decode(FindCoordinatorResponse, 0)
    check resp.errorCode == keNone
    check resp.nodeId == 1
    check resp.host == "broker1"

  test "JoinGroup response decode":
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
    var r = initKafkaReader(w.buf)
    let resp = r.decode(JoinGroupResponse)
    check resp.errorCode == keNone
    check resp.generationId == 5
    check resp.protocolName == "range"
    check resp.leader == "leader-1"
    check resp.memberId == "member-1"
    check resp.members.len == 1
    check resp.members[0].memberId == "member-1"
    check resp.members[0].metadata == @[0xAA'u8, 0xBB, 0xCC]

  test "SyncGroup response decode":
    var w = initKafkaWriter()
    w.writeInt16(0)
    w.writeInt32(2)
    w.buf.add(@[0x01'u8, 0x02])
    var r = initKafkaReader(w.buf)
    let resp = r.decode(SyncGroupResponse)
    check resp.errorCode == keNone
    check resp.assignment == some(@[0x01'u8, 0x02])

  test "SyncGroup response decode empty assignment":
    var w = initKafkaWriter()
    w.writeInt16(keRebalanceInProgress.toInt16)
    w.writeInt32(-1)
    var r = initKafkaReader(w.buf)
    let resp = r.decode(SyncGroupResponse)
    check resp.errorCode == keRebalanceInProgress
    check resp.assignment.isNone

  test "ListOffsets response decode":
    var w = initKafkaWriter()
    w.writeArrayLen(1)
    w.writeString("t1")
    w.writeArrayLen(2)
    w.writeInt32(0)
    w.writeInt16(0)
    w.writeInt64(1000)
    w.writeInt64(42)
    w.writeInt32(1)
    w.writeInt16(0)
    w.writeInt64(-1)
    w.writeInt64(100)
    var r = initKafkaReader(w.buf)
    let resp = r.decode(ListOffsetsResponse)
    check resp.topics.len == 1
    check resp.topics[0].topic == "t1"
    check resp.topics[0].partitions.len == 2
    check resp.topics[0].partitions[0].partition == 0
    check resp.topics[0].partitions[0].errorCode == keNone
    check resp.topics[0].partitions[0].timestamp == 1000
    check resp.topics[0].partitions[0].offset == 42
    check resp.topics[0].partitions[1].partition == 1
    check resp.topics[0].partitions[1].offset == 100

  test "LeaveGroup response decode":
    var w = initKafkaWriter()
    w.writeInt16(0)
    var r = initKafkaReader(w.buf)
    let resp = r.decode(LeaveGroupResponse)
    check resp.errorCode == keNone

  test "OffsetFetch response decode":
    var w = initKafkaWriter()
    w.writeArrayLen(1)
    w.writeString("topic-1")
    w.writeArrayLen(2)
    w.writeInt32(0)
    w.writeInt64(100)
    w.writeNullableString(some(""))
    w.writeInt16(0)
    w.writeInt32(1)
    w.writeInt64(-1)
    w.writeNullableString(none(string))
    w.writeInt16(0)
    var r = initKafkaReader(w.buf)
    let resp = r.decode(OffsetFetchResponse)
    check resp.topics.len == 1
    check resp.topics[0].topic == "topic-1"
    check resp.topics[0].partitions.len == 2
    check resp.topics[0].partitions[0].partition == 0
    check resp.topics[0].partitions[0].offset == 100
    check resp.topics[0].partitions[1].offset == -1
    check resp.topics[0].partitions[1].metadata == none(string)

  test "OffsetCommit response decode":
    var w = initKafkaWriter()
    w.writeArrayLen(1)
    w.writeString("t1")
    w.writeArrayLen(2)
    w.writeInt32(0)
    w.writeInt16(0)
    w.writeInt32(1)
    w.writeInt16(keRebalanceInProgress.toInt16)
    var r = initKafkaReader(w.buf)
    let resp = r.decode(OffsetCommitResponse)
    check resp.topics.len == 1
    check resp.topics[0].topic == "t1"
    check resp.topics[0].partitions.len == 2
    check resp.topics[0].partitions[0].errorCode == keNone
    check resp.topics[0].partitions[1].errorCode == keRebalanceInProgress

  test "varint zigzag decode":
    var w = initKafkaWriter()
    w.buf.add(0x00'u8) # 0
    w.buf.add(0x01'u8) # -1
    w.buf.add(0x02'u8) # 1
    w.buf.add(0x03'u8) # -2
    var r = initKafkaReader(w.buf)
    check r.decodeVarint() == 0
    check r.decodeVarint() == -1
    check r.decodeVarint() == 1
    check r.decodeVarint() == -2

  test "varint multi-byte decode":
    var w = initKafkaWriter()
    w.buf.add(0xD8'u8)
    w.buf.add(0x04'u8)
    var r = initKafkaReader(w.buf)
    check r.decodeVarint() == 300

  test "decodeVarBytes null and present":
    var w = initKafkaWriter()
    w.buf.add(0x01'u8) # null
    w.buf.add(0x00'u8) # empty
    w.buf.add(0x04'u8) # 2 bytes
    w.buf.add(0xAA'u8)
    w.buf.add(0xBB'u8)
    var r = initKafkaReader(w.buf)
    check r.decodeVarBytes() == none(seq[byte])
    check r.decodeVarBytes() == some(newSeq[byte]())
    check r.decodeVarBytes() == some(@[0xAA'u8, 0xBB])

  test "decodeVarString":
    var w = initKafkaWriter()
    w.buf.add(0x04'u8) # "hi"
    w.buf.add(byte('h'))
    w.buf.add(byte('i'))
    w.buf.add(0x00'u8) # empty
    var r = initKafkaReader(w.buf)
    check r.decodeVarString() == "hi"
    check r.decodeVarString() == ""

  test "decodeVarString rejects a negative (null) length":
    # fkVarString (RecordHeader.key) is non-nullable on the wire, so a
    # zigzag-negative length is a malformed frame and must raise rather than
    # silently coerce to "".
    var w = initKafkaWriter()
    w.buf.add(0x01'u8) # zigzag -1 (null)
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.decodeVarString()

  test "Response header decode":
    var w = initKafkaWriter()
    w.writeInt32(42)
    var r = initKafkaReader(w.buf)
    let hdr = r.readResponseHeader()
    check hdr.correlationId == 42

  test "Fetch response decode with record batch":
    var w = initKafkaWriter()
    w.writeInt32(0) # throttleTimeMs
    w.writeArrayLen(1)
    w.writeString("t1")
    w.writeArrayLen(1)
    w.writeInt32(0)
    w.writeInt16(0)
    w.writeInt64(10)
    w.writeInt64(10)
    w.writeArrayLen(-1)

    # Build payload (attributes through records) for CRC calculation
    var payload = initKafkaWriter()
    payload.writeInt16(0) # attributes
    payload.writeInt32(0) # lastOffsetDelta
    payload.writeInt64(1000) # firstTimestamp
    payload.writeInt64(1000) # maxTimestamp
    payload.writeInt64(-1) # producerId
    payload.writeInt16(-1) # producerEpoch
    payload.writeInt32(-1) # baseSequence
    payload.writeInt32(1) # numRecords

    var recBody = initKafkaWriter()
    recBody.buf.add(0x00'u8) # attributes
    recBody.buf.add(0x00'u8) # timestampDelta
    recBody.buf.add(0x00'u8) # offsetDelta
    recBody.buf.add(0x01'u8) # key null
    recBody.buf.add(0x0A'u8) # value "hello" len=5
    for c in "hello":
      recBody.buf.add(byte(c))
    recBody.buf.add(0x00'u8) # numHeaders = 0

    let recLen = recBody.buf.len
    payload.buf.add(byte(uint32(recLen) shl 1))
    payload.buf.add(recBody.buf)

    let crc = crc32c(payload.buf)
    let batchLength = int32(4 + 1 + 4 + payload.buf.len)
      # partitionLeaderEpoch + magic + crc + payload

    var batch = initKafkaWriter()
    batch.writeInt64(5) # baseOffset
    batch.writeInt32(batchLength)
    batch.writeInt32(0) # partitionLeaderEpoch
    batch.writeInt8(2) # magic = v2
    batch.writeInt32(cast[int32](crc))
    batch.buf.add(payload.buf)

    w.writeInt32(int32(batch.buf.len))
    w.buf.add(batch.buf)

    var r = initKafkaReader(w.buf)
    let resp = r.decode(FetchResponse)
    check resp.throttleTimeMs == 0
    check resp.topics.len == 1
    check resp.topics[0].partitions.len == 1
    let p = resp.topics[0].partitions[0]
    check p.errorCode == keNone
    check p.highWatermark == 10
    let batches = batchesFromPartition(p)
    check batches.len == 1
    check batches[0].baseOffset == 5
    check batches[0].records.len == 1
    check batches[0].records[0].offset == 5
    check batches[0].records[0].timestamp == 1000
    check batches[0].records[0].key == none(seq[byte])
    check batches[0].records[0].value == some("hello".toBytes)

  test "Fetch response decode with record headers":
    var w = initKafkaWriter()
    w.writeInt32(0)
    w.writeArrayLen(1)
    w.writeString("t1")
    w.writeArrayLen(1)
    w.writeInt32(0)
    w.writeInt16(0)
    w.writeInt64(10)
    w.writeInt64(10)
    w.writeArrayLen(-1)

    # Build payload for CRC calculation
    var payload = initKafkaWriter()
    payload.writeInt16(0) # attributes
    payload.writeInt32(0) # lastOffsetDelta
    payload.writeInt64(2000) # firstTimestamp
    payload.writeInt64(2000) # maxTimestamp
    payload.writeInt64(-1) # producerId
    payload.writeInt16(-1) # producerEpoch
    payload.writeInt32(-1) # baseSequence
    payload.writeInt32(1) # numRecords

    var recBody = initKafkaWriter()
    recBody.buf.add(0x00'u8) # attributes
    recBody.buf.add(0x00'u8) # timestampDelta
    recBody.buf.add(0x00'u8) # offsetDelta
    recBody.buf.add(0x01'u8) # key null
    recBody.buf.add(0x01'u8) # value null
    recBody.buf.add(0x02'u8) # numHeaders = 1
    recBody.buf.add(0x02'u8) # header key "k" len=1
    recBody.buf.add(byte('k'))
    recBody.buf.add(0x02'u8) # header value "v" len=1
    recBody.buf.add(byte('v'))

    let recLen = recBody.buf.len
    payload.buf.add(byte(uint32(recLen) shl 1))
    payload.buf.add(recBody.buf)

    let crc = crc32c(payload.buf)
    let batchLength = int32(4 + 1 + 4 + payload.buf.len)

    var batch = initKafkaWriter()
    batch.writeInt64(0) # baseOffset
    batch.writeInt32(batchLength)
    batch.writeInt32(0) # partitionLeaderEpoch
    batch.writeInt8(2) # magic = v2
    batch.writeInt32(cast[int32](crc))
    batch.buf.add(payload.buf)

    w.writeInt32(int32(batch.buf.len))
    w.buf.add(batch.buf)

    var r = initKafkaReader(w.buf)
    let resp = r.decode(FetchResponse)
    let batches = batchesFromPartition(resp.topics[0].partitions[0])
    let rec = batches[0].records[0]
    check rec.headers.len == 1
    check rec.headers[0].key == "k"
    check rec.headers[0].value == some(@[byte('v')])

  test "Fetch response empty records":
    var w = initKafkaWriter()
    w.writeInt32(0)
    w.writeArrayLen(1)
    w.writeString("t1")
    w.writeArrayLen(1)
    w.writeInt32(0)
    w.writeInt16(0)
    w.writeInt64(0)
    w.writeInt64(0)
    w.writeArrayLen(-1)
    w.writeInt32(0)
    var r = initKafkaReader(w.buf)
    let resp = r.decode(FetchResponse)
    check batchesFromPartition(resp.topics[0].partitions[0]).len == 0

  test "Produce response decode (v3)":
    var w = initKafkaWriter()
    w.writeArrayLen(1)
    w.writeString("test-topic")
    w.writeArrayLen(2)
    w.writeInt32(0)
    w.writeInt16(0) # keNone
    w.writeInt64(42)
    w.writeInt64(1000) # timestampMs
    w.writeInt32(1)
    w.writeInt16(keNotLeaderOrFollower.toInt16)
    w.writeInt64(-1)
    w.writeInt64(-1) # timestampMs
    w.writeInt32(100) # throttleTimeMs (at end for Produce v3)
    var r = initKafkaReader(w.buf)
    let resp = r.decode(ProduceResponse)
    check resp.throttleTimeMs == 100
    check resp.topics.len == 1
    check resp.topics[0].topic == "test-topic"
    check resp.topics[0].partitions.len == 2
    check resp.topics[0].partitions[0].partition == 0
    check resp.topics[0].partitions[0].errorCode == keNone
    check resp.topics[0].partitions[0].baseOffset == 42
    check resp.topics[0].partitions[0].timestampMs == 1000
    check resp.topics[0].partitions[1].partition == 1
    check resp.topics[0].partitions[1].errorCode == keNotLeaderOrFollower
    check resp.topics[0].partitions[1].timestampMs == -1

suite "CRC32C":
  test "empty input":
    check crc32c(newSeq[byte]()) == 0x00000000'u32

  test "known vector 123456789":
    let data = "123456789".toBytes
    check crc32c(data) == 0xE3069283'u32

suite "Varint encode/decode round-trip":
  test "basic values":
    for v in [0'i64, -1, 1, -2, 2, 127, -128, 300, -300]:
      var w = initKafkaWriter()
      w.encodeVarint(v)
      var r = initKafkaReader(w.buf)
      check r.decodeVarint() == v

  test "large values":
    for v in [int64.high, int64.low, 1_000_000'i64, -1_000_000]:
      var w = initKafkaWriter()
      w.encodeVarint(v)
      var r = initKafkaReader(w.buf)
      check r.decodeVarint() == v

  test "encodeVarBytes round-trip":
    var w = initKafkaWriter()
    w.encodeVarBytes(none(seq[byte]))
    w.encodeVarBytes(some(newSeq[byte]()))
    w.encodeVarBytes(some(@[0xDE'u8, 0xAD]))
    var r = initKafkaReader(w.buf)
    check r.decodeVarBytes() == none(seq[byte])
    check r.decodeVarBytes() == some(newSeq[byte]())
    check r.decodeVarBytes() == some(@[0xDE'u8, 0xAD])

  test "encodeVarString round-trip":
    var w = initKafkaWriter()
    w.encodeVarString("hello")
    w.encodeVarString("")
    var r = initKafkaReader(w.buf)
    check r.decodeVarString() == "hello"
    check r.decodeVarString() == ""

  test "encodeVarBytes rejects a payload past maxWireBytesLen":
    # Symmetry with decodeVarBytes: a caller bug that would otherwise only
    # surface as a broker-side disconnect fails fast at encode time.
    var w = initKafkaWriter()
    let oversized = newSeq[byte](maxWireBytesLen + 1)
    expect KafkaProtocolError:
      w.encodeVarBytes(some(oversized))

  test "encodeVarString rejects a payload past maxWireBytesLen":
    var w = initKafkaWriter()
    let oversized = newString(maxWireBytesLen + 1)
    expect KafkaProtocolError:
      w.encodeVarString(oversized)

suite "RecordBatch encode/decode":
  test "single record round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some("hello".toBytes),
        headers: @[],
      )
    ]
    let encoded = block:
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
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.magic == 2
    check batch.baseOffset == 0
    check batch.firstTimestamp == 1000
    check batch.records.len == 1
    check batch.records[0].offset == 0
    check batch.records[0].timestamp == 1000
    check batch.records[0].key == none(seq[byte])
    check batch.records[0].value == some("hello".toBytes)

  test "multiple records round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 1000,
        key: some("k1".toBytes),
        value: some("v1".toBytes),
        headers: @[],
      ),
      Record(
        offset: 1,
        timestamp: 1001,
        key: some("k2".toBytes),
        value: some("v2".toBytes),
        headers: @[],
      ),
      Record(
        offset: 2,
        timestamp: 1002,
        key: none(seq[byte]),
        value: some("v3".toBytes),
        headers: @[],
      ),
    ]
    let encoded = block:
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
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records.len == 3
    check batch.lastOffsetDelta == 2
    check batch.maxTimestamp == 1002
    check batch.records[0].key == some("k1".toBytes)
    check batch.records[1].offset == 1
    check batch.records[2].key == none(seq[byte])
    check batch.records[2].value == some("v3".toBytes)

  test "hostile timestamp/offset delta does not crash decode (wraps like Java)":
    # A malicious broker can set timestampDelta = int64.high with
    # firstTimestamp = 1 so the absolute reconstruction (firstTimestamp +
    # delta) overflows int64. Java's long arithmetic wraps silently; our
    # decode must do the same instead of raising an uncatchable OverflowDefect
    # that escapes the `{.raises: [KafkaError].}` contract and crashes the
    # consumer.
    var payload = initKafkaWriter()
    payload.writeInt16(0) # attributes
    payload.writeInt32(0) # lastOffsetDelta
    payload.writeInt64(1) # firstTimestamp = 1
    payload.writeInt64(1) # maxTimestamp
    payload.writeInt64(-1) # producerId
    payload.writeInt16(-1) # producerEpoch
    payload.writeInt32(-1) # baseSequence
    payload.writeInt32(1) # numRecords

    var recBody = initKafkaWriter()
    recBody.buf.add(0x00'u8) # record attributes
    recBody.encodeVarint(int64.high) # timestampDelta = int64.high (hostile)
    recBody.encodeVarint(int64.high) # offsetDelta = int64.high (hostile)
    recBody.buf.add(0x01'u8) # key null
    recBody.buf.add(0x01'u8) # value null
    recBody.buf.add(0x00'u8) # numHeaders = 0

    let recLen = recBody.buf.len
    payload.buf.add(byte(uint32(recLen) shl 1)) # recLen zigzag varint (single byte)
    payload.buf.add(recBody.buf)

    let crc = crc32c(payload.buf)
    let batchLength = int32(4 + 1 + 4 + payload.buf.len)

    var batch = initKafkaWriter()
    batch.writeInt64(7) # baseOffset = 7
    batch.writeInt32(batchLength)
    batch.writeInt32(0) # partitionLeaderEpoch
    batch.writeInt8(2) # magic = v2
    batch.writeInt32(cast[int32](crc))
    batch.buf.add(payload.buf)

    var r = initKafkaReader(batch.buf)
    let decoded = r.decode(RecordBatch)
    check decoded.records.len == 1
    # 1 +% int64.high wraps to int64.low; 7 +% int64.high wraps to int64.low + 6.
    check decoded.records[0].timestamp == 1'i64 +% int64.high
    check decoded.records[0].offset == 7'i64 +% int64.high

  test "records with headers round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: some("val".toBytes),
        headers: @[
          RecordHeader(key: "h1", value: some("v1".toBytes)),
          RecordHeader(key: "h2", value: some("v2".toBytes)),
        ],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records[0].headers.len == 2
    check batch.records[0].headers[0].key == "h1"
    check batch.records[0].headers[0].value == some("v1".toBytes)
    check batch.records[0].headers[1].key == "h2"
    check batch.records[0].headers[1].value == some("v2".toBytes)

  test "CRC verification":
    let records = @[
      Record(
        offset: 0,
        timestamp: 5000,
        key: none(seq[byte]),
        value: some("test".toBytes),
        headers: @[],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          firstTimestamp: 5000'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    # Extract CRC from encoded bytes (offset 17-20: after baseOffset(8) + batchLength(4) + partLeaderEpoch(4) + magic(1))
    var cr = initKafkaReader(encoded)
    discard cr.readInt64() # baseOffset
    discard cr.readInt32() # batchLength
    discard cr.readInt32() # partitionLeaderEpoch
    discard cr.readInt8() # magic
    let storedCrc = cast[uint32](cr.readInt32())
    # Compute CRC over remaining bytes (attributes through end)
    let payload = encoded[cr.pos .. ^1]
    check crc32c(payload) == storedCrc

  test "record with many headers round-trip":
    var headers: seq[RecordHeader]
    for i in 0 ..< 50:
      headers.add(RecordHeader(key: "h" & $i, value: some(("v" & $i).toBytes)))
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: some("val".toBytes),
        headers: headers,
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records[0].headers.len == 50
    check batch.records[0].headers[49].key == "h49"
    check batch.records[0].headers[49].value == some("v49".toBytes)

  test "record with empty key and empty value":
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: some(newSeq[byte]()),
        value: some(newSeq[byte]()),
        headers: @[],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records[0].key == some(newSeq[byte]())
    check batch.records[0].value == some(newSeq[byte]())

  test "record with null key and null value":
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: none(seq[byte]),
        headers: @[],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records[0].key == none(seq[byte])
    check batch.records[0].value == none(seq[byte])

  test "decodeRecords rejects per-record over-read":
    ## A malformed record whose inner content claims more bytes than its
    ## declared length must be rejected before allocating the inner seq, so
    ## corruption cannot spill into the next record. The per-record `limit`
    ## narrows the window so checkRemaining catches the over-read pre-alloc.
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: some("hello".toBytes),
        headers: @[],
      )
    ]
    var encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    # Records start at byte 61: header (21) + payload prefix (40).
    # The first byte at offset 61 is the zigzag varint for recLength. With
    # the layout above, recLength = 11 → varint byte 0x16. Patching it to
    # 5 (varint 0x0A) keeps the wire width identical and makes the value
    # bytes (`"hello"`, 5 bytes) over-read past the declared recordEnd.
    check encoded[61] == 0x16'u8
    encoded[61] = 0x0A'u8
    let newCrc = crc32c(encoded.toOpenArray(21, encoded.len - 1))
    let crcBE = toBytesBE(newCrc)
    for i in 0 ..< 4:
      encoded[17 + i] = crcBE[i]
    var r = initKafkaReader(encoded)
    var raised = false
    var msg = ""
    try:
      discard r.decode(RecordBatch)
    except KafkaProtocolError as e:
      raised = true
      msg = e.msg
    check raised
    # Accept any of the rejection paths: the bounded reader ("underflow" from
    # checkRemaining, or "window" from a maxLen-aware varbytes read) or the
    # defense-in-depth post-decode "over-read" assertion. All indicate the
    # malformed record was caught before corruption spilled into the next one.
    check ("underflow" in msg) or ("window" in msg) or ("over-read" in msg)

  test "decodeRecordBatch rejects adversarial numRecords":
    # Guard against OOM from a broker response that claims billions of records
    # in a small batch. The bound must trigger *before* newSeq[Record] runs.
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: some("x".toBytes),
        headers: @[],
      )
    ]
    var encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    # numRecords sits at bytes 57..60; payload covered by CRC starts at byte 21.
    # Patch numRecords to 20_000_000 (> maxWireArrayLen = 10_000_000) and
    # recompute CRC so parsing reaches our bounds check.
    let bogusCount: uint32 = 20_000_000
    let countBE = toBytesBE(bogusCount)
    for i in 0 ..< 4:
      encoded[57 + i] = countBE[i]
    let newCrc = crc32c(encoded.toOpenArray(21, encoded.len - 1))
    let crcBE = toBytesBE(newCrc)
    for i in 0 ..< 4:
      encoded[17 + i] = crcBE[i]
    var r = initKafkaReader(encoded)
    var raised = false
    try:
      discard r.decode(RecordBatch)
    except KafkaProtocolError as e:
      raised = true
      check "record batch count" in e.msg
    check raised

  test "decodeRecordBatch fails fast on under-cap numRecords with truncated body":
    # numRecords below maxWireArrayLen passes the count bound, so the decoder
    # reaches the per-record loop. With records allocated incrementally (not
    # newSeq[Record](numRecords)), a header claiming millions of records but
    # carrying only one must fail on the missing record's length/varint check
    # — without first pre-allocating millions of Record objects.
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: some("x".toBytes),
        headers: @[],
      )
    ]
    var encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    # Patch numRecords (bytes 57..60) to 5_000_000 (< maxWireArrayLen) and
    # recompute the CRC (covers bytes 21..end) so parsing reaches the loop.
    let bogusCount: uint32 = 5_000_000
    let countBE = toBytesBE(bogusCount)
    for i in 0 ..< 4:
      encoded[57 + i] = countBE[i]
    let newCrc = crc32c(encoded.toOpenArray(21, encoded.len - 1))
    let crcBE = toBytesBE(newCrc)
    for i in 0 ..< 4:
      encoded[17 + i] = crcBE[i]
    var r = initKafkaReader(encoded)
    var raised = false
    try:
      discard r.decode(RecordBatch)
    except KafkaProtocolError:
      raised = true
    check raised

  test "decodeRecordBatch rejects a batchLength too small for the fixed header":
    # A crafted batchLength can leave batchEnd *before* the end of the 40-byte
    # fixed header it follows (attributes..record count). The decoder must
    # reject this up front: otherwise the header reads drift past batchEnd into
    # the next batch, and on the compressed branch `compressedLen = batchEnd -
    # pos` goes negative (readBytes rejects negatives, but this branch slices
    # with toOpenArray instead, feeding a malformed window to the decompressor
    # — see the gzip OOB hazard). The guard fires before CRC verification, so
    # the forged CRC below is never consulted.
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: some("x".toBytes),
        headers: @[],
      )
    ]
    var encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    # Mark the batch gzip-compressed (low 3 bits of attributes = 1), then shrink
    # batchLength so batchEnd (= 12 + batchLength = 40) lands inside the fixed
    # header (which ends at crcStart(21) + 40 = 61). A valid v2 batch needs
    # batchLength >= 49; 28 is below the minimum.
    encoded[21] = 0x00
    encoded[22] = 0x01
    let bogusLen: uint32 = 28 # batchEnd = 12 + 28 = 40, below crcStart(21) + 40
    let lenBE = toBytesBE(bogusLen)
    for i in 0 ..< 4:
      encoded[8 + i] = lenBE[i]
    # CRC covers [crcStart=21, batchEnd-1=39] — forged so the (unreached) CRC
    # check could not be what rejects the batch.
    let newCrc = crc32c(encoded.toOpenArray(21, 39))
    let crcBE = toBytesBE(newCrc)
    for i in 0 ..< 4:
      encoded[17 + i] = crcBE[i]
    var r = initKafkaReader(encoded)
    var raised = false
    try:
      discard r.decode(RecordBatch)
    except KafkaProtocolError as e:
      raised = true
      check "invalid record batch length" in e.msg
    check raised

  test "decodeRecordBatch bounds a record length to the batch window":
    # A record whose length points past batchEnd but still inside the larger
    # fetch buffer (the next batch's bytes) must be rejected at the length
    # check, not decoded into the following batch and caught only afterward.
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: some("hello".toBytes),
        headers: @[],
      )
    ]
    var encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    let batchEnd = encoded.len
      # the single batch fills the buffer
      # The single record's length varint sits at byte 61 (just after the 4-byte
      # numRecords at 57..60); recStart is the byte after it.
    let recStart = 62
    let origRecLen = batchEnd - recStart
    check encoded[61] == byte(origRecLen * 2) # 1-byte zigzag of a small +int
    let patchedRecLen = origRecLen + 8 # claim 8 bytes past batchEnd
    check patchedRecLen < 64 # keep it a single-byte zigzag varint
    encoded[61] = byte(patchedRecLen * 2)
    # Recompute the batch CRC (covers [21, batchEnd-1]); the patched byte is in range.
    let newCrc = crc32c(encoded.toOpenArray(21, batchEnd - 1))
    let crcBE = toBytesBE(newCrc)
    for i in 0 ..< 4:
      encoded[17 + i] = crcBE[i]
    # Append a fake "next batch" region so the bogus length stays inside the
    # buffer — with the old whole-buffer bound it would be accepted here.
    encoded.add(newSeq[byte](16))
    var r = initKafkaReader(encoded)
    var raised = false
    try:
      discard r.decode(RecordBatch)
    except KafkaProtocolError as e:
      raised = true
      check "invalid record length" in e.msg
    check raised

suite "Compression codec range":
  test "decompressRecords rejects gzip data below the 18-byte frame floor":
    # gzipDecompress reads from data[10] and computes the deflate length as
    # `data.len - 18`; on input shorter than an 18-byte gzip frame it reads out
    # of bounds and the unsigned subtraction underflows. Guard that a too-short
    # section is a clean KafkaError, not a crash, for every length the
    # underflow window covers.
    for n in 0 .. 17:
      var raised = false
      try:
        discard decompressRecords(newSeq[byte](n), ccGzip)
      except KafkaError as e:
        raised = true
        check "gzip data too short" in e.msg
      check raised

  test "compressionFromAttributes maps the four valid ids":
    check compressionFromAttributes(0) == ccNone
    check compressionFromAttributes(1) == ccGzip
    check compressionFromAttributes(2) == ccSnappy
    check compressionFromAttributes(3) == ccLz4
    check compressionFromAttributes(4) == ccZstd

  test "compressionFromAttributes ignores high bits above codec":
    # Only the low 3 bits of `attributes` describe the codec; the rest
    # (timestampType, isTransactional, isControlBatch, hasDeleteHorizonMs)
    # must not corrupt codec extraction.
    check compressionFromAttributes(0x00F8'i16) == ccNone
    check compressionFromAttributes(int16(0x00F8 or 0x0001)) == ccGzip

  test "compressionFromAttributes rejects unknown codec ids 5..7":
    # Regression: a previous implementation cast `attributes and 0x07` to
    # CompressionCodec unconditionally. For ids 5..7 that produced an
    # out-of-range enum value — undefined behavior with range checks off.
    for badId in 5'i16 .. 7'i16:
      var raised = false
      try:
        discard compressionFromAttributes(badId)
      except KafkaProtocolError as e:
        raised = true
        check "unsupported compression codec" in e.msg
      check raised

  test "decodeRecordBatch surfaces unknown codec as KafkaProtocolError":
    # End-to-end: corrupt the attributes field of an otherwise valid v2
    # record batch so the low 3 bits encode codec id 7, fix the CRC, then
    # confirm the deserializer fails cleanly rather than constructing an
    # out-of-range CompressionCodec.
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: some("x".toBytes),
        headers: @[],
      )
    ]
    var encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    # attributes is a 2-byte int16 BE at bytes 21..22; set the low 3 bits to 7.
    encoded[21] = 0x00
    encoded[22] = 0x07
    let newCrc = crc32c(encoded.toOpenArray(21, encoded.len - 1))
    let crcBE = toBytesBE(newCrc)
    for i in 0 ..< 4:
      encoded[17 + i] = crcBE[i]
    var r = initKafkaReader(encoded)
    var raised = false
    try:
      discard r.decode(RecordBatch)
    except KafkaProtocolError as e:
      raised = true
      check "unsupported compression codec" in e.msg
    check raised

suite "Gzip compression":
  test "single record gzip round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 1000,
        key: some("key".toBytes),
        value: some("hello gzip".toBytes),
        headers: @[],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccGzip),
          firstTimestamp: 1000'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.magic == 2
    check (batch.attributes and 0x07) == 1
    check batch.records.len == 1
    check batch.records[0].offset == 0
    check batch.records[0].timestamp == 1000
    check batch.records[0].key == some("key".toBytes)
    check batch.records[0].value == some("hello gzip".toBytes)

  test "multiple records gzip round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 1000,
        key: some("k1".toBytes),
        value: some("v1".toBytes),
        headers: @[],
      ),
      Record(
        offset: 1,
        timestamp: 1001,
        key: some("k2".toBytes),
        value: some("v2".toBytes),
        headers: @[],
      ),
      Record(
        offset: 2,
        timestamp: 1002,
        key: none(seq[byte]),
        value: some("v3".toBytes),
        headers: @[],
      ),
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccGzip),
          firstTimestamp: 1000'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records.len == 3
    check batch.lastOffsetDelta == 2
    check batch.records[0].key == some("k1".toBytes)
    check batch.records[1].offset == 1
    check batch.records[2].key == none(seq[byte])
    check batch.records[2].value == some("v3".toBytes)

  test "gzip records with headers round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: some("val".toBytes),
        headers: @[
          RecordHeader(key: "h1", value: some("v1".toBytes)),
          RecordHeader(key: "h2", value: some("v2".toBytes)),
        ],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccGzip),
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records[0].headers.len == 2
    check batch.records[0].headers[0].key == "h1"
    check batch.records[0].headers[0].value == some("v1".toBytes)
    check batch.records[0].headers[1].key == "h2"
    check batch.records[0].headers[1].value == some("v2".toBytes)

  test "gzip CRC verification":
    let records = @[
      Record(
        offset: 0,
        timestamp: 5000,
        key: none(seq[byte]),
        value: some("test".toBytes),
        headers: @[],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccGzip),
          firstTimestamp: 5000'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var cr = initKafkaReader(encoded)
    discard cr.readInt64() # baseOffset
    discard cr.readInt32() # batchLength
    discard cr.readInt32() # partitionLeaderEpoch
    discard cr.readInt8() # magic
    let storedCrc = cast[uint32](cr.readInt32())
    let payload = encoded[cr.pos .. ^1]
    check crc32c(payload) == storedCrc

suite "Snappy compression":
  test "single record snappy round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 1000,
        key: some("key".toBytes),
        value: some("hello snappy".toBytes),
        headers: @[],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccSnappy),
          firstTimestamp: 1000'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.magic == 2
    check (batch.attributes and 0x07) == 2
    check batch.records.len == 1
    check batch.records[0].offset == 0
    check batch.records[0].timestamp == 1000
    check batch.records[0].key == some("key".toBytes)
    check batch.records[0].value == some("hello snappy".toBytes)

  test "multiple records snappy round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 1000,
        key: some("k1".toBytes),
        value: some("v1".toBytes),
        headers: @[],
      ),
      Record(
        offset: 1,
        timestamp: 1001,
        key: some("k2".toBytes),
        value: some("v2".toBytes),
        headers: @[],
      ),
      Record(
        offset: 2,
        timestamp: 1002,
        key: none(seq[byte]),
        value: some("v3".toBytes),
        headers: @[],
      ),
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccSnappy),
          firstTimestamp: 1000'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records.len == 3
    check batch.lastOffsetDelta == 2
    check batch.records[0].key == some("k1".toBytes)
    check batch.records[1].offset == 1
    check batch.records[2].key == none(seq[byte])
    check batch.records[2].value == some("v3".toBytes)

  test "snappy records with headers round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: some("val".toBytes),
        headers: @[
          RecordHeader(key: "h1", value: some("v1".toBytes)),
          RecordHeader(key: "h2", value: some("v2".toBytes)),
        ],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccSnappy),
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records[0].headers.len == 2
    check batch.records[0].headers[0].key == "h1"
    check batch.records[0].headers[0].value == some("v1".toBytes)
    check batch.records[0].headers[1].key == "h2"
    check batch.records[0].headers[1].value == some("v2".toBytes)

  test "snappy CRC verification":
    let records = @[
      Record(
        offset: 0,
        timestamp: 5000,
        key: none(seq[byte]),
        value: some("test".toBytes),
        headers: @[],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccSnappy),
          firstTimestamp: 5000'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var cr = initKafkaReader(encoded)
    discard cr.readInt64() # baseOffset
    discard cr.readInt32() # batchLength
    discard cr.readInt32() # partitionLeaderEpoch
    discard cr.readInt8() # magic
    let storedCrc = cast[uint32](cr.readInt32())
    let payload = encoded[cr.pos .. ^1]
    check crc32c(payload) == storedCrc

suite "Snappy edge cases":
  test "raw snappy decompress (no xerial header)":
    let original = "hello raw snappy".toBytes
    let compressed = compress(original)
    let decompressed = decompressRecords(compressed, ccSnappy)
    check decompressed == "hello raw snappy".toBytes

  test "snappy compress/decompress empty data":
    let empty: seq[byte] = @[]
    let compressed = compressRecords(empty, ccSnappy)
    let decompressed = decompressRecords(compressed, ccSnappy)
    check decompressed == empty

  test "snappy large data multi-block round-trip":
    var largeData = newSeq[byte](65536)
    for i in 0 ..< largeData.len:
      largeData[i] = byte(ord('A') + (i mod 26))
    let compressed = compressRecords(largeData, ccSnappy)
    let decompressed = decompressRecords(compressed, ccSnappy)
    check decompressed == largeData

  test "snappy decompression keeps accepting every valid stream":
    # The read-slack buffer must never disturb a well-formed stream: exercise
    # literals and copy tags across a range of payload shapes, including
    # copy-heavy repeated data where the last tag sits near the buffer end.
    let payloads = @[
      @[],
      @[0x41'u8],
      "hello raw snappy".toBytes,
      newSeq[byte](60), # literal exactly at the length-extension boundary
      block:
        var d = newSeq[byte](64)
        for i in 0 ..< d.len:
          d[i] = byte(ord('A') + (i mod 26))
        d,
      block:
        var d = newSeq[byte](65536)
        for i in 0 ..< d.len:
          d[i] = byte(0xAB + (i mod 3))
        d,
    ]
    for payload in payloads:
      let compressed = compress(payload)
      check decompressRecords(compressed, ccSnappy) == payload

  test "truncated COPY tag fails cleanly instead of reading out of bounds":
    # The dependency's 4-byte COPY trailer read is guarded only by
    # `ip + 1 < srcLen`, so a trailer cut short by 1..3 bytes would read past
    # the buffer without the read slack. Each stream below is a zero-length
    # prefix followed by a COPY tag with an incomplete trailer.
    let hostile = @[
      @[0x00'u8, 0x01, 0x00], # 1-byte-offset COPY, 1 trailer byte
      @[0x00'u8, 0x02, 0x00, 0x00], # 2-byte-offset COPY, 2 trailer bytes
      @[0x00'u8, 0x03, 0x00], # 4-byte-offset COPY, 1 trailer byte
      @[0x00'u8, 0x03, 0x00, 0x00], # 4-byte-offset COPY, 2 trailer bytes
      @[0x00'u8, 0x03, 0x00, 0x00, 0x00], # 4-byte-offset COPY, 3 trailer bytes
    ]
    for payload in hostile:
      expect KafkaError:
        discard decompressRecords(payload, ccSnappy)

  test "truncated LITERAL length extension fails cleanly":
    # 0xF0..0xFC are literals of 61..64 bytes: the decoder's length read at
    # `ip` is unchecked, and each stream below leaves fewer than 4 bytes
    # after the tag.
    let hostile = @[
      @[0x00'u8, 0xFC, 0x00], # needs 4 extension bytes, 1 present
      @[0x00'u8, 0xFC, 0x00, 0x00], # needs 4, 2 present
      @[0x00'u8, 0xFC, 0x00, 0x00, 0x00], # needs 4, 3 present
      @[0x00'u8, 0xF8, 0x00, 0x00], # needs 3, 2 present
      @[0x00'u8, 0xF4, 0x00, 0x00], # needs 2, 2 present but the read is 4 bytes
      @[0x00'u8, 0xF0, 0x00], # needs 1, 1 present but the read is 4 bytes
    ]
    for payload in hostile:
      expect KafkaError:
        discard decompressRecords(payload, ccSnappy)

  test "xerial block with truncated COPY tag is rejected":
    # The per-block read slack must cover the xerial framing path too.
    let header = @[
      0x82'u8, 0x53, 0x4E, 0x41, 0x50, 0x50, 0x59, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
      0x00, 0x00, 0x01,
    ]
    let blockBody = @[0x00'u8, 0x01, 0x00]
    var frame = header
    frame.add(byte((blockBody.len shr 24) and 0xFF))
    frame.add(byte((blockBody.len shr 16) and 0xFF))
    frame.add(byte((blockBody.len shr 8) and 0xFF))
    frame.add(byte(blockBody.len and 0xFF))
    frame.add(blockBody)
    expect KafkaError:
      discard decompressRecords(frame, ccSnappy)

suite "Gzip edge cases":
  test "gzip compress/decompress empty data":
    let empty: seq[byte] = @[]
    let compressed = compressRecords(empty, ccGzip)
    let decompressed = decompressRecords(compressed, ccGzip)
    check decompressed == empty

  test "gzip large data round-trip":
    var largeData = newSeq[byte](65536)
    for i in 0 ..< largeData.len:
      largeData[i] = byte(ord('A') + (i mod 26))
    let compressed = compressRecords(largeData, ccGzip)
    let decompressed = decompressRecords(compressed, ccGzip)
    check decompressed == largeData

  ## gzipDecompress used to treat Z_OK + exhausted input
  ## as success and never read the RFC 1952 trailer, so a truncated frame or a
  ## flipped CRC/ISIZE could yield a prefix of the payload. Require
  ## Z_STREAM_END and a matching ISO 3309 CRC-32 / ISIZE before returning.

  test "gzip truncated frame is rejected":
    let compressed = compressRecords("hello gzip trailer".toBytes, ccGzip)
    check compressed.len > 18
    var truncated = compressed
    truncated.setLen(compressed.len - 1)
    expect KafkaError:
      discard decompressRecords(truncated, ccGzip)

  test "gzip CRC mismatch is rejected":
    var compressed = compressRecords("hello gzip trailer".toBytes, ccGzip)
    compressed[^8] = compressed[^8] xor 0x01
    var raised = false
    var msg = ""
    try:
      discard decompressRecords(compressed, ccGzip)
    except KafkaError as e:
      raised = true
      msg = e.msg
    check raised
    check "gzip CRC mismatch" in msg

  test "gzip ISIZE mismatch is rejected":
    var compressed = compressRecords("hello gzip trailer".toBytes, ccGzip)
    compressed[^1] = compressed[^1] xor 0x01
    var raised = false
    var msg = ""
    try:
      discard decompressRecords(compressed, ccGzip)
    except KafkaError as e:
      raised = true
      msg = e.msg
    check raised
    check "gzip ISIZE mismatch" in msg

suite "Decompression size limits":
  test "snappy xerial decompression exceeds size limit":
    var data = newSeq[byte](1024)
    for i in 0 ..< data.len:
      data[i] = byte(ord('A') + (i mod 26))
    let compressed = compressRecords(data, ccSnappy)
    expect KafkaError:
      discard decompressRecords(compressed, ccSnappy, maxDecompressSize = 100)

  test "snappy raw decompression exceeds size limit":
    let original = "hello raw snappy test data for size limit".toBytes
    let compressed = compress(original)
    expect KafkaError:
      discard decompressRecords(compressed, ccSnappy, maxDecompressSize = 10)

  test "snappy raw oversized length prefix is rejected before decompress":
    # A crafted raw-snappy stream whose self-declared uncompressed length
    # (little-endian base-128 varint for 1_000_000) far exceeds maxDecompressSize.
    # The pre-check must raise before `uncompress` allocates for the claim, even
    # though the trailing bytes are not a real snappy payload.
    let blob = @[0xC0'u8, 0x84, 0x3D, 0x00, 0x00, 0x00]
    var raised = false
    var msg = ""
    try:
      discard decompressRecords(blob, ccSnappy, maxDecompressSize = 100)
    except KafkaError as e:
      raised = true
      msg = e.msg
    check raised
    check "exceeds size limit" in msg

  test "snappy raw length prefix rejects a final byte that would overflow uint64":
    # On the 10th prefix byte (shift 63) only the lowest bit fits a uint64;
    # higher bits used to be silently truncated by the shl wrap, leaving the
    # size check to catch the (possibly truncated) claim. Reject at the prefix.
    let blob = @[0xFF'u8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]
    var raised = false
    var msg = ""
    try:
      discard decompressRecords(blob, ccSnappy, maxDecompressSize = 100)
    except KafkaError as e:
      raised = true
      msg = e.msg
    check raised
    check "overflow" in msg

  test "snappy xerial block oversized length prefix is rejected before decompress":
    # A xerial frame with one tiny block whose internal snappy length prefix
    # claims 1_000_000 uncompressed bytes. supersnappy would setLen that claim
    # before validating the body; the per-block pre-check must reject it against
    # maxDecompressSize *before* uncompress allocates, just like the raw path.
    let header = @[
      0x82'u8, 0x53, 0x4E, 0x41, 0x50, 0x50, 0x59, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
      0x00, 0x00, 0x01,
    ]
    # base-128 varint for 1_000_000 followed by filler the pre-check never reads.
    let blockBody = @[0xC0'u8, 0x84, 0x3D, 0x00, 0x00, 0x00]
    let blockLen = blockBody.len
    var frame = header
    frame.add(byte((blockLen shr 24) and 0xFF))
    frame.add(byte((blockLen shr 16) and 0xFF))
    frame.add(byte((blockLen shr 8) and 0xFF))
    frame.add(byte(blockLen and 0xFF))
    frame.add(blockBody)
    var raised = false
    var msg = ""
    try:
      discard decompressRecords(frame, ccSnappy, maxDecompressSize = 100)
    except KafkaError as e:
      raised = true
      msg = e.msg
    check raised
    check "exceeds remaining decompression budget" in msg

  test "snappy xerial accepts a terminal block whose compressed size exceeds the remaining budget":
    # Regression: the per-block pre-check must gate on the *claimed uncompressed*
    # length, not the compressed blockLen. Build a two-block xerial stream where
    # the second block's compressed size is larger than `maxDecompressSize -
    # first_block_uncompressed`, but its uncompressed size still fits in the
    # remaining budget. Comparing blockLen to the remaining budget would falsely
    # reject an otherwise valid terminal block.
    var block1 = newSeq[byte](64)
    for i in 0 ..< block1.len:
      block1[i] = byte(ord('A') + (i mod 26))
    var block2 = newSeq[byte](8)
    for i in 0 ..< block2.len:
      block2[i] = byte(ord('a') + i)
    let c1 = compress(block1)
    let c2 = compress(block2)
    # Pick maxSize so uncompressed total (72) fits, but the compressed second
    # block is bigger than the leftover budget (maxSize - block1.len).
    let maxSize = block1.len + block2.len
    doAssert c2.len > maxSize - block1.len,
      "test premise: c2 compressed size must exceed the post-block1 leftover budget"
    let header = @[
      0x82'u8, 0x53, 0x4E, 0x41, 0x50, 0x50, 0x59, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
      0x00, 0x00, 0x01,
    ]
    var frame = header
    for c in [c1, c2]:
      let n = c.len
      frame.add(byte((n shr 24) and 0xFF))
      frame.add(byte((n shr 16) and 0xFF))
      frame.add(byte((n shr 8) and 0xFF))
      frame.add(byte(n and 0xFF))
      frame.add(c)
    let decompressed = decompressRecords(frame, ccSnappy, maxDecompressSize = maxSize)
    check decompressed == block1 & block2

  test "gzip decompression exceeds size limit":
    var data = newSeq[byte](1024)
    for i in 0 ..< data.len:
      data[i] = byte(ord('A') + (i mod 26))
    let compressed = compressRecords(data, ccGzip)
    expect KafkaError:
      discard decompressRecords(compressed, ccGzip, maxDecompressSize = 100)

  test "gzip decompression error paths stay deterministic under repetition":
    # Regression: the dependency's `ungzip` returns on limit/error without
    # calling inflateEnd, leaking ~32KB of inflate state per failed frame. The
    # local decompressor guarantees inflateEnd on every path; hammer both
    # failure paths so a reintroduced leak (or an exception leak) fails here.
    var data = newSeq[byte](1024)
    for i in 0 ..< data.len:
      data[i] = byte(ord('A') + (i mod 26))
    let compressed = compressRecords(data, ccGzip)
    for _ in 0 ..< 1000:
      expect KafkaError:
        discard decompressRecords(compressed, ccGzip, maxDecompressSize = 100)
    let corrupted = @[
      0x1F'u8,
      0x8B,
      0x08,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x03,
      # deflate block header 0xFF is a reserved BTYPE -> inflate data error
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00, # trailer
    ]
    for _ in 0 ..< 1000:
      expect KafkaError:
        discard decompressRecords(corrupted, ccGzip)

suite "RecordBatch total decompression budget":
  proc compressedBatch(payload: seq[byte]): seq[byte] =
    ## Encode a snappy-compressed single-record v2 batch to wire bytes.
    let batch = RecordBatch(
      baseOffset: 0'i64,
      attributes: int16(ccSnappy),
      firstTimestamp: 0'i64,
      producerId: -1'i64,
      producerEpoch: -1'i16,
      baseSequence: -1'i32,
      records: @[
        Record(
          offset: 0'i64,
          timestamp: 0'i64,
          key: none(seq[byte]),
          value: some(payload),
          headers: @[],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(batch)
    w.buf

  test "without a budget, decompression is unlimited per batch":
    # Backwards compatibility: readers constructed by callers that never attach
    # a budget (e.g. the producer side) behave exactly as before.
    let encoded = compressedBatch("payload".toBytes)
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records[0].value.get == "payload".toBytes

  test "a batch within the budget decodes and the budget is decremented":
    let encoded = compressedBatch("payload".toBytes)
    var r = initKafkaReader(encoded)
    let budget = DecompressionBudget(remaining: 4096)
    r.decompressionBudget = budget
    let batch = r.decode(RecordBatch)
    check batch.records.len == 1
    check budget.remaining < 4096
    check budget.remaining >= 0

  test "a batch exceeding the remaining budget raises KafkaDecompressionLimitError":
    # The budget check is cumulative: a batch whose decompressed size would
    # take the total past the remaining budget must raise the limit error, not
    # a generic decompression failure (which the fetcher would mistake for
    # corruption and skip).
    let encoded = compressedBatch("payload".toBytes)
    var r = initKafkaReader(encoded)
    r.decompressionBudget = DecompressionBudget(remaining: 1)
    var raised = false
    var msg = ""
    try:
      discard r.decode(RecordBatch)
    except KafkaDecompressionLimitError as e:
      raised = true
      msg = e.msg
    check raised
    check "budget exhausted" in msg

  test "an exhausted budget rejects the next batch before decompression":
    # A fully spent budget must fail fast (no decompression attempt) and keep
    # failing until the caller allocates a fresh budget for the next response.
    let encoded = compressedBatch("payload".toBytes)
    var r = initKafkaReader(encoded)
    let budget = DecompressionBudget(remaining: 0)
    r.decompressionBudget = budget
    expect KafkaDecompressionLimitError:
      discard r.decode(RecordBatch)

  test "a shared budget accumulates across partitions":
    # One response-scoped budget handed to several partition readers: the sum
    # of their decompressed sizes is capped, so a broker cannot bypass the cap
    # by fanning batches across partitions. The first partition consumes the
    # budget (measured by the decrement), leaving the second with one byte
    # short of what its batch needs.
    let encoded = compressedBatch("payload".toBytes)
    var r1 = initKafkaReader(encoded)
    let budget = DecompressionBudget(remaining: high(int) div 4)
    r1.decompressionBudget = budget
    let b1 = r1.decode(RecordBatch)
    let consumed = (high(int) div 4) - budget.remaining
    check b1.records.len == 1
    check consumed > 0
    budget.remaining = consumed - 1
    var r2 = initKafkaReader(encoded)
    r2.decompressionBudget = budget
    expect KafkaDecompressionLimitError:
      discard r2.decode(RecordBatch)

suite "LZ4 compression":
  test "lz4 compress/decompress round-trip":
    let original = "hello lz4 compression".toBytes
    let compressed = compressRecords(original, ccLz4)
    check compressed != original
    let decompressed = decompressRecords(compressed, ccLz4)
    check decompressed == original

  test "lz4 compress/decompress empty data":
    let empty: seq[byte] = @[]
    let compressed = compressRecords(empty, ccLz4)
    let decompressed = decompressRecords(compressed, ccLz4)
    check decompressed == empty

  test "lz4 decompress rejects a non-positive size limit":
    # Non-empty input with maxSize <= 0 must raise rather than size dstBuf to
    # an empty buffer and index addr dstBuf[0]. Empty input short-circuits to
    # @[] before the guard, so it stays unaffected.
    let compressed = compressRecords("payload".toBytes, ccLz4)
    expect KafkaError:
      discard lz4mod.lz4Decompress(compressed, maxSize = 0)
    check lz4mod.lz4Decompress(newSeq[byte](0), maxSize = 0).len == 0

  test "lz4 large data round-trip":
    var largeData = newSeq[byte](65536)
    for i in 0 ..< largeData.len:
      largeData[i] = byte(ord('A') + (i mod 26))
    let compressed = compressRecords(largeData, ccLz4)
    let decompressed = decompressRecords(compressed, ccLz4)
    check decompressed == largeData

  test "compressed batch with trailing garbage after last record is rejected":
    # Regression: the compressed path did not verify that loadRecords consumed
    # every decompressed byte, so a numRecords count that undershot the actual
    # payload silently discarded the tail — asymmetric with the uncompressed
    # path, which already raised via the batchEnd defensive framing.
    let records = @[
      Record(
        offset: 0'i64,
        timestamp: 0'i64,
        key: none(seq[byte]),
        value: some("a".toBytes),
        headers: @[],
      ),
      Record(
        offset: 1'i64,
        timestamp: 0'i64,
        key: none(seq[byte]),
        value: some("b".toBytes),
        headers: @[],
      ),
    ]
    var encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          attributes: 0x2'i16, # snappy (ccSnappy)
          records: records,
        )
      )
      w.buf

    # numRecords is at bytes 57..60. Patch from 2 to 1 so loadRecords stops
    # after the first decompressed record, leaving the second one as trailing
    # bytes in the decompressed buffer.
    let patched: uint32 = 1
    let patchedBE = toBytesBE(patched)
    for i in 0 ..< 4:
      encoded[57 + i] = patchedBE[i]
    let newCrc = crc32c(encoded.toOpenArray(21, encoded.len - 1))
    let crcBE = toBytesBE(newCrc)
    for i in 0 ..< 4:
      encoded[17 + i] = crcBE[i]

    var r = initKafkaReader(encoded)
    var raised = false
    try:
      discard r.decode(RecordBatch)
    except KafkaProtocolError as e:
      raised = true
      check "unread trailing byte" in e.msg
    check raised

  test "lz4 decompress rejects a truncated frame missing the endmark":
    # Regression: dropping the tail (endmark + trailing block) used to return
    # a partial payload with no error — silent data loss on a torn stream.
    let payload = repeat("A", 4096).toBytes
    let compressed = lz4mod.lz4Compress(payload)
    check compressed.len > 32
    # Chop the endmark and part of the last block.
    let truncated = compressed[0 ..< compressed.len - 16]
    expect KafkaError:
      discard lz4mod.lz4Decompress(truncated)

  test "lz4 output between half of maxSize and maxSize is accepted":
    # Regression: the growth loop used to raise as soon as doubling the
    # buffer overshot maxSize, rejecting legitimate outputs in
    # (initial buffer, maxSize]. 100 KB of zeros compresses to a few hundred
    # bytes, so the initial buffer (compressed.len * 4 + 256) is far below
    # the output and the loop must double-and-clamp its way up to exactly
    # the output size without tripping the limit.
    let original = newSeq[byte](100_000) # zeros: highly compressible
    let compressed = compressRecords(original, ccLz4)
    check compressed.len < 10_000 # sanity: growth loop genuinely exercised
    let decompressed = lz4mod.lz4Decompress(compressed, maxSize = 110_000)
    check decompressed == original
    # A limit just below the real output still rejects.
    expect KafkaError:
      discard lz4mod.lz4Decompress(compressed, maxSize = 99_999)

suite "LZ4 RecordBatch":
  test "single record lz4 round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 1000,
        key: some("key".toBytes),
        value: some("hello lz4".toBytes),
        headers: @[],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccLz4),
          firstTimestamp: 1000'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.magic == 2
    check (batch.attributes and 0x07) == 3
    check batch.records.len == 1
    check batch.records[0].offset == 0
    check batch.records[0].timestamp == 1000
    check batch.records[0].key == some("key".toBytes)
    check batch.records[0].value == some("hello lz4".toBytes)

  test "multiple records lz4 round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 1000,
        key: some("k1".toBytes),
        value: some("v1".toBytes),
        headers: @[],
      ),
      Record(
        offset: 1,
        timestamp: 1001,
        key: some("k2".toBytes),
        value: some("v2".toBytes),
        headers: @[],
      ),
      Record(
        offset: 2,
        timestamp: 1002,
        key: none(seq[byte]),
        value: some("v3".toBytes),
        headers: @[],
      ),
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccLz4),
          firstTimestamp: 1000'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records.len == 3
    check batch.lastOffsetDelta == 2
    check batch.records[0].key == some("k1".toBytes)
    check batch.records[1].offset == 1
    check batch.records[2].key == none(seq[byte])
    check batch.records[2].value == some("v3".toBytes)

  test "lz4 records with headers round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: some("val".toBytes),
        headers: @[
          RecordHeader(key: "h1", value: some("v1".toBytes)),
          RecordHeader(key: "h2", value: some("v2".toBytes)),
        ],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccLz4),
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records[0].headers.len == 2
    check batch.records[0].headers[0].key == "h1"
    check batch.records[0].headers[0].value == some("v1".toBytes)
    check batch.records[0].headers[1].key == "h2"
    check batch.records[0].headers[1].value == some("v2".toBytes)

  test "lz4 CRC verification":
    let records = @[
      Record(
        offset: 0,
        timestamp: 5000,
        key: none(seq[byte]),
        value: some("test".toBytes),
        headers: @[],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccLz4),
          firstTimestamp: 5000'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var cr = initKafkaReader(encoded)
    discard cr.readInt64() # baseOffset
    discard cr.readInt32() # batchLength
    discard cr.readInt32() # partitionLeaderEpoch
    discard cr.readInt8() # magic
    let storedCrc = cast[uint32](cr.readInt32())
    let payload = encoded[cr.pos .. ^1]
    check crc32c(payload) == storedCrc

suite "Zstd compression":
  test "zstd compress/decompress round-trip":
    let original = "hello zstd compression".toBytes
    let compressed = compressRecords(original, ccZstd)
    check compressed != original
    let decompressed = decompressRecords(compressed, ccZstd)
    check decompressed == original

  test "zstd compress/decompress empty data":
    let empty: seq[byte] = @[]
    let compressed = compressRecords(empty, ccZstd)
    let decompressed = decompressRecords(compressed, ccZstd)
    check decompressed == empty

  test "zstd large data round-trip":
    var largeData = newSeq[byte](65536)
    for i in 0 ..< largeData.len:
      largeData[i] = byte(ord('A') + (i mod 26))
    let compressed = compressRecords(largeData, ccZstd)
    let decompressed = decompressRecords(compressed, ccZstd)
    check decompressed == largeData

  test "zstd custom compression level":
    let original = "hello zstd custom level".toBytes
    let compressed1 = zstdCompress(original, level = 1)
    let compressed19 = zstdCompress(original, level = 19)
    check zstdDecompress(compressed1) == original
    check zstdDecompress(compressed19) == original

  test "zstd zero-content-size frame decodes to empty without crashing":
    # Regression: a frame whose header declares content size 0 (valid — zstd
    # of empty input; any peer can send one) made zstdDecompress index an
    # empty seq (`addr result[0]`) and die with an IndexDefect instead of
    # honouring the catchable-KafkaError contract. Our own zstdCompress
    # short-circuits empty input to @[], so build the frame via the raw
    # binding.
    var frame = newSeq[byte](64)
    let sz = ZSTD_compress(addr frame[0], csize_t(frame.len), nil, 0, 3)
    check not ZSTD_isError(sz)
    frame.setLen(int(sz))
    check frame.len > 0 # a real frame, not the @[] short-circuit
    check zstdDecompress(frame) == newSeq[byte]()

  test "zstd frame declaring a >= 2^63 content size raises KafkaError":
    # Regression: a hostile frame header can declare an 8-byte content size
    # >= 2^63, which ZSTD_getFrameContentSize returns as a negative clonglong
    # — not CONTENTSIZE_UNKNOWN/-ERROR and not > maxSize — so it sailed past
    # every guard into newSeq[byte](int(frameSize)): a RangeDefect escaping
    # the {.raises: [KafkaError].} contract. Hand-craft the 13-byte header:
    # magic, FHD 0xE0 (single-segment, 8-byte FCS field), FCS = 2^63 (LE).
    let frame = @[
      0x28'u8,
      0xB5,
      0x2F,
      0xFD, # zstd magic number
      0xE0, # frame header descriptor: FCS_flag=3, Single_Segment=1
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x80, # FCS = 2^63
    ]
    expect KafkaError:
      discard zstdDecompress(frame)

  test "zstd frame lying about its content size does not reserve that much":
    # Regression: the one-shot path took ZSTD_getFrameContentSize — a field the
    # sender writes — as the allocation size for newSeq[byte]. A frame of a few
    # dozen bytes could therefore declare 128 MB, force that reservation, and
    # only then fail with "Src size is incorrect". OutOfMemDefect is not
    # catchable, so the ceiling had to be a real limit, not a hope.
    #
    # Streaming decompression makes the reservation track the bytes actually
    # produced, so a frame claiming 100 MB but carrying 4 KB costs 4 KB.
    let real = zstdCompress(repeatByte(byte('a'), 4096), level = 19)
    check real.len < 64 # the whole attack is a few dozen bytes

    var forged = real
    # Frame_Content_Size_flag lives in the top 2 bits of the frame header
    # descriptor; flag 2 selects a 4-byte field, the smallest that can express
    # a size near the cap at all.
    forged[4] = (forged[4] and 0x3F'u8) or 0x80'u8
    let cap = 900 * 1024 * 1024
    let declared = uint32(cap) - 1024 * 1024
    forged[5] = byte(declared and 0xFF)
    forged[6] = byte((declared shr 8) and 0xFF)
    forged[7] = byte((declared shr 16) and 0xFF)
    forged[8] = byte((declared shr 24) and 0xFF)

    # The cap is raised to 900 MB rather than using the 128 MB default so the
    # reservation is far larger than the process's own high-water mark by the
    # time this test runs. VmHWM is a high-water mark: it only ever rises, and
    # the suites above it have already peaked in the hundreds of MB, so a
    # 128 MB reservation inside them would be invisible. The frame is a few
    # dozen bytes either way — the attack size does not grow with the claim.
    let before = peakRssKb()
    expect KafkaError:
      discard zstdDecompress(forged, cap)
    let grewKb = peakRssKb() - before
    # A pre-allocation would be ~899 MB. The threshold is set well above the
    # ~128 KB of streaming scratch a real 4 KB decode needs, and far below
    # anything a working implementation would touch.
    check grewKb < 64 * 1024

  test "zstd decompression is bounded by the maxSize argument":
    # The limit is a real ceiling, not a hint: a frame that decompresses to more
    # than the caller allows must raise rather than return the bytes.
    let big = zstdCompress(repeatByte(byte('a'), 200_000))
    check zstdDecompress(big, 1_000_000).len == 200_000
    expect KafkaError:
      discard zstdDecompress(big, 1024)

  test "zstd stream frames without a declared content size decode":
    # ZSTD_decompressStream accepts what the one-shot path rejected: a frame
    # whose content size the sender left unknown. Built through the streaming
    # COMPRESSOR rather than by hand-editing a header, because the field layout
    # is version-dependent (FCS_flag selects 0/1/2/4/8 bytes, and whether a
    # Window_Descriptor is present depends on Single_Segment) — a hand-built
    # "unknown size" frame is a coin flip that either decodes by luck or fails
    # as corrupt. A streaming compressor emits the real thing: a frame that
    # genuinely carries no content size, because it did not know one yet.
    #
    # Real brokers always write the field, so this is not a compatibility fix.
    # It is what shows the switch to streaming costs nothing in coverage.
    let original = repeatByte(byte('z'), 300_000)
    let streaming = zstdCompressStreaming(original)
    check streaming.len > 0
    # Confirm the fixture really is the case under test: a frame with no
    # declared content size is one the one-shot API cannot size up front.
    check rawzstdd.ZSTD_getFrameContentSize(
      unsafeAddr streaming[0], csize_t(streaming.len)
    ) == rawzstdcommon.ZSTD_CONTENTSIZE_UNKNOWN
    check zstdDecompress(streaming) == original

  test "single record zstd RecordBatch round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 1000,
        key: some("key".toBytes),
        value: some("hello zstd".toBytes),
        headers: @[],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccZstd),
          firstTimestamp: 1000'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.magic == 2
    check (batch.attributes and 0x07) == 4
    check batch.records.len == 1
    check batch.records[0].offset == 0
    check batch.records[0].timestamp == 1000
    check batch.records[0].key == some("key".toBytes)
    check batch.records[0].value == some("hello zstd".toBytes)

  test "multiple records zstd RecordBatch round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 1000,
        key: some("k1".toBytes),
        value: some("v1".toBytes),
        headers: @[],
      ),
      Record(
        offset: 1,
        timestamp: 1001,
        key: some("k2".toBytes),
        value: some("v2".toBytes),
        headers: @[],
      ),
      Record(
        offset: 2,
        timestamp: 1002,
        key: none(seq[byte]),
        value: some("v3".toBytes),
        headers: @[],
      ),
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccZstd),
          firstTimestamp: 1000'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records.len == 3
    check batch.lastOffsetDelta == 2
    check batch.records[0].key == some("k1".toBytes)
    check batch.records[1].offset == 1
    check batch.records[2].key == none(seq[byte])
    check batch.records[2].value == some("v3".toBytes)

  test "zstd records with headers round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: some("val".toBytes),
        headers: @[
          RecordHeader(key: "h1", value: some("v1".toBytes)),
          RecordHeader(key: "h2", value: some("v2".toBytes)),
        ],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: int16(ccZstd),
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records[0].headers.len == 2
    check batch.records[0].headers[0].key == "h1"
    check batch.records[0].headers[0].value == some("v1".toBytes)
    check batch.records[0].headers[1].key == "h2"
    check batch.records[0].headers[1].value == some("v2".toBytes)

suite "Error paths":
  test "buffer underflow on readInt32":
    var r = initKafkaReader(@[0x00'u8, 0x01])
    expect KafkaProtocolError:
      discard r.readInt32()

  test "buffer underflow on readInt64":
    var r = initKafkaReader(@[0x00'u8, 0x01, 0x02, 0x03])
    expect KafkaProtocolError:
      discard r.readInt64()

  test "null string raises on readString":
    var w = initKafkaWriter()
    w.writeInt16(-1'i16) # null string marker
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.readString()

  test "varint overflow":
    # 10 bytes of 0x80 (continuation) should overflow
    var data: seq[byte]
    for i in 0 ..< 10:
      data.add(0x80'u8)
    data.add(0x01'u8)
    var r = initKafkaReader(data)
    expect KafkaProtocolError:
      discard r.decodeVarint()

  test "readString truncated data raises KafkaProtocolError":
    var w = initKafkaWriter()
    w.writeInt16(100'i16) # claims 100 bytes
    w.buf.add(@[0x41'u8, 0x42]) # but only 2 bytes of data
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.readString()

  test "readArrayLen rejects very large value":
    var w = initKafkaWriter()
    w.writeInt32(int32.high)
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.readArrayLen()

  test "empty RecordBatch decode (magic != 2)":
    var w = initKafkaWriter()
    w.writeInt64(0'i64) # baseOffset
    let batchLength = int32(4 + 1 + 4) # partitionLeaderEpoch + magic + padding
    w.writeInt32(batchLength)
    w.writeInt32(0'i32) # partitionLeaderEpoch
    w.writeInt8(1'i8) # magic = 1 (not v2)
    # Padding to fill batchLength
    w.writeInt32(0'i32)
    var r = initKafkaReader(w.buf)
    let batch = r.decode(RecordBatch)
    check batch.records.len == 0
    check batch.magic == 1

  test "invalid gzip data raises":
    # The 12-byte payload is under the 18-byte gzip frame floor, so the length
    # guard raises "gzip data too short" before any header/body parsing.
    expect KafkaError:
      discard decompressRecords(
        @[0x1F'u8, 0x8B, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xFF, 0xFF],
        ccGzip,
      )

  test "invalid snappy data raises":
    expect KafkaError:
      discard decompressRecords(@[0x01'u8, 0x02], ccSnappy)

  test "invalid lz4 data raises":
    # LZ4 frame magic header followed by corrupted data
    expect KafkaError:
      discard
        decompressRecords(@[0x04'u8, 0x22, 0x4D, 0x18, 0xFF, 0xFF, 0xFF, 0xFF], ccLz4)

  test "invalid zstd data raises":
    expect KafkaError:
      discard decompressRecords(@[0x01'u8, 0x02], ccZstd)

  test "encodeRecordBatch with idempotent fields":
    let records = @[
      Record(
        offset: 0,
        timestamp: 2000,
        key: none(seq[byte]),
        value: some("idempotent".toBytes),
        headers: @[],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          firstTimestamp: 2000'i64,
          producerId: 12345'i64,
          producerEpoch: 5'i16,
          baseSequence: 42'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.magic == 2
    check batch.producerId == 12345'i64
    check batch.producerEpoch == 5'i16
    check batch.baseSequence == 42'i32
    check batch.records.len == 1
    check batch.records[0].value == some("idempotent".toBytes)

  test "encodeRecordBatch default non-idempotent fields":
    let records = @[
      Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some("test".toBytes),
        headers: @[],
      )
    ]
    let encoded = block:
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
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.producerId == -1'i64
    check batch.producerEpoch == -1'i16
    check batch.baseSequence == -1'i32

  test "RecordHeader null vs empty value round-trip":
    let records = @[
      Record(
        offset: 0,
        timestamp: 0,
        key: none(seq[byte]),
        value: some("test".toBytes),
        headers: @[
          RecordHeader(key: "null-val", value: none(seq[byte])),
          RecordHeader(key: "empty-val", value: some(newSeq[byte]())),
          RecordHeader(key: "has-val", value: some("data".toBytes)),
        ],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check batch.records.len == 1
    check batch.records[0].headers.len == 3
    check batch.records[0].headers[0].key == "null-val"
    check batch.records[0].headers[0].value == none(seq[byte])
    check batch.records[0].headers[1].key == "empty-val"
    check batch.records[0].headers[1].value == some(newSeq[byte]())
    check batch.records[0].headers[2].key == "has-val"
    check batch.records[0].headers[2].value == some("data".toBytes)

  test "InitProducerIdResponse decode":
    var w = initKafkaWriter()
    w.writeInt32(100'i32) # throttleTimeMs
    w.writeInt16(0'i16) # errorCode (NONE)
    w.writeInt64(99999'i64) # producerId
    w.writeInt16(3'i16) # producerEpoch
    var r = initKafkaReader(w.buf)
    let resp = r.decode(InitProducerIdResponse)
    check resp.throttleTimeMs == 100
    check resp.errorCode == keNone
    check resp.producerId == 99999'i64
    check resp.producerEpoch == 3'i16

  test "encodeRecordBatch with transactional bit":
    let records = @[
      Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some("txn-test".toBytes),
        headers: @[],
      )
    ]
    let encoded = block:
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: 0'i64,
          attributes: 0x10'i16,
          firstTimestamp: 1000'i64,
          producerId: 100'i64,
          producerEpoch: 0'i16,
          baseSequence: 0'i32,
          records: records,
        )
      )
      w.buf
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check (batch.attributes and 0x10'i16) != 0
    check batch.producerId == 100'i64
    check batch.records.len == 1

  test "encodeRecordBatch without transactional bit":
    let records = @[
      Record(
        offset: 0,
        timestamp: 1000,
        key: none(seq[byte]),
        value: some("no-txn".toBytes),
        headers: @[],
      )
    ]
    let encoded = block:
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
    var r = initKafkaReader(encoded)
    let batch = r.decode(RecordBatch)
    check (batch.attributes and 0x10'i16) == 0

  test "AddPartitionsToTxnResponse decode":
    var w = initKafkaWriter()
    w.writeInt32(50'i32) # throttleTimeMs
    w.writeArrayLen(1'i32)
    w.writeString("test-topic")
    w.writeArrayLen(2'i32)
    w.writeInt32(0'i32) # partition
    w.writeInt16(0'i16) # errorCode NONE
    w.writeInt32(1'i32) # partition
    w.writeInt16(0'i16) # errorCode NONE
    var r = initKafkaReader(w.buf)
    let resp = r.decode(AddPartitionsToTxnResponse)
    check resp.throttleTimeMs == 50
    check resp.errors.len == 1
    check resp.errors[0].topic == "test-topic"
    check resp.errors[0].partitions.len == 2
    check resp.errors[0].partitions[0].partition == 0
    check resp.errors[0].partitions[0].errorCode == keNone
    check resp.errors[0].partitions[1].partition == 1

  test "AddPartitionsToTxnRequest encode v3 (flexible)":
    let req = AddPartitionsToTxnRequest(
      transactionalId: "txn-1",
      producerId: 7,
      producerEpoch: 3,
      topics: @[AddPartitionsToTxnTopicReq(topic: "t", partitions: @[0'i32, 1'i32])],
    )
    var w = initKafkaWriter()
    w.encode(req, 3'i16)
    var r = initKafkaReader(w.buf)
    check r.readCompactString() == "txn-1"
    check r.readInt64() == 7'i64
    check r.readInt16() == 3'i16
    check r.readCompactArrayLen() == 1'i32 # topics
    check r.readCompactString() == "t"
    check r.readCompactArrayLen() == 2'i32 # partitions
    check r.readInt32() == 0'i32
    check r.readInt32() == 1'i32
    check r.decodeUvarint() == 0'u64 # topics[0] tagged trailer
    check r.decodeUvarint() == 0'u64 # top-level tagged trailer
    check r.remaining == 0

  test "AddPartitionsToTxnResponse decode v3 (flexible)":
    var w = initKafkaWriter()
    w.writeInt32(50'i32) # throttleTimeMs
    w.writeCompactArrayLen(1) # errors array
    w.writeCompactString("test-topic")
    w.writeCompactArrayLen(2) # partitions array
    w.writeInt32(0'i32) # partition 0
    w.writeInt16(0'i16) # errorCode NONE
    w.encodeUvarint(0) # partitions[0] tagged trailer
    w.writeInt32(1'i32) # partition 1
    w.writeInt16(0'i16) # errorCode NONE
    w.encodeUvarint(0) # partitions[1] tagged trailer
    w.encodeUvarint(0) # errors[0] tagged trailer
    w.encodeUvarint(0) # top-level tagged trailer
    var r = initKafkaReader(w.buf)
    let resp = r.decode(AddPartitionsToTxnResponse, 3'i16)
    check resp.throttleTimeMs == 50
    check resp.errors.len == 1
    check resp.errors[0].topic == "test-topic"
    check resp.errors[0].partitions.len == 2
    check resp.errors[0].partitions[1].partition == 1
    check resp.errors[0].partitions[1].errorCode == keNone
    check r.remaining == 0

  test "AddOffsetsToTxnResponse decode":
    var w = initKafkaWriter()
    w.writeInt32(10'i32) # throttleTimeMs
    w.writeInt16(0'i16) # errorCode NONE
    var r = initKafkaReader(w.buf)
    let resp = r.decode(AddOffsetsToTxnResponse)
    check resp.throttleTimeMs == 10
    check resp.errorCode == keNone

  test "AddOffsetsToTxnRequest encode v3 (flexible)":
    let req = AddOffsetsToTxnRequest(
      transactionalId: "txn-1", producerId: 42, producerEpoch: 1, groupId: "grp"
    )
    var w = initKafkaWriter()
    w.encode(req, 3'i16)
    var r = initKafkaReader(w.buf)
    check r.readCompactString() == "txn-1"
    check r.readInt64() == 42'i64
    check r.readInt16() == 1'i16
    check r.readCompactString() == "grp"
    check r.decodeUvarint() == 0'u64 # top-level tagged-fields trailer
    check r.remaining == 0

  test "AddOffsetsToTxnResponse decode v3 (flexible)":
    var w = initKafkaWriter()
    w.writeInt32(10'i32) # throttleTimeMs
    w.writeInt16(0'i16) # errorCode NONE
    w.encodeUvarint(0) # tagged-fields trailer
    var r = initKafkaReader(w.buf)
    let resp = r.decode(AddOffsetsToTxnResponse, 3'i16)
    check resp.throttleTimeMs == 10
    check resp.errorCode == keNone
    check r.remaining == 0

  test "EndTxnResponse decode":
    var w = initKafkaWriter()
    w.writeInt32(20'i32) # throttleTimeMs
    w.writeInt16(0'i16) # errorCode NONE
    var r = initKafkaReader(w.buf)
    let resp = r.decode(EndTxnResponse)
    check resp.throttleTimeMs == 20
    check resp.errorCode == keNone

  test "EndTxnResponse decode with error":
    var w = initKafkaWriter()
    w.writeInt32(0'i32) # throttleTimeMs
    w.writeInt16(int16(keInvalidTxnState)) # errorCode
    var r = initKafkaReader(w.buf)
    let resp = r.decode(EndTxnResponse)
    check resp.errorCode == keInvalidTxnState

  test "EndTxnResponse decode v3 (flexible: trailing tagged-fields)":
    var w = initKafkaWriter()
    w.writeInt32(20'i32) # throttleTimeMs
    w.writeInt16(0'i16) # errorCode NONE
    w.encodeUvarint(0) # top-level tagged-fields trailer (count = 0)
    var r = initKafkaReader(w.buf)
    let resp = r.decode(EndTxnResponse, 3'i16)
    check resp.throttleTimeMs == 20
    check resp.errorCode == keNone
    check r.remaining == 0

  test "TxnOffsetCommitResponse decode":
    var w = initKafkaWriter()
    w.writeInt32(30'i32) # throttleTimeMs
    w.writeArrayLen(1'i32)
    w.writeString("topic-1")
    w.writeArrayLen(1'i32)
    w.writeInt32(0'i32) # partition
    w.writeInt16(0'i16) # errorCode NONE
    var r = initKafkaReader(w.buf)
    let resp = r.decode(TxnOffsetCommitResponse)
    check resp.throttleTimeMs == 30
    check resp.topics.len == 1
    check resp.topics[0].topic == "topic-1"
    check resp.topics[0].partitions.len == 1
    check resp.topics[0].partitions[0].partition == 0
    check resp.topics[0].partitions[0].errorCode == keNone

  test "TxnOffsetCommitRequest encode v3 (flexible + KIP-447 fields)":
    let req = TxnOffsetCommitRequest(
      transactionalId: "txn-1",
      groupId: "grp",
      producerId: 100,
      producerEpoch: 5,
      generationId: 7,
      memberId: "member-1",
      groupInstanceId: none(string),
      topics: @[
        TxnOffsetCommitTopicReq(
          topic: "t",
          partitions: @[
            TxnOffsetCommitPartitionReq(
              partition: 0, offset: 42, committedLeaderEpoch: 3, metadata: none(string)
            )
          ],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(req, 3'i16)
    var r = initKafkaReader(w.buf)
    check r.readCompactString() == "txn-1"
    check r.readCompactString() == "grp"
    check r.readInt64() == 100'i64
    check r.readInt16() == 5'i16
    check r.readInt32() == 7'i32 # generationId (v3)
    check r.readCompactString() == "member-1" # memberId (v3)
    check r.readCompactNullableString() == none(string) # groupInstanceId (v3)
    check r.readCompactArrayLen() == 1'i32 # topics
    check r.readCompactString() == "t"
    check r.readCompactArrayLen() == 1'i32 # partitions
    check r.readInt32() == 0'i32 # partition
    check r.readInt64() == 42'i64 # offset
    check r.readInt32() == 3'i32 # committedLeaderEpoch (v2+)
    check r.readCompactNullableString() == none(string) # metadata
    check r.decodeUvarint() == 0'u64 # partition struct trailer
    check r.decodeUvarint() == 0'u64 # topic struct trailer
    check r.decodeUvarint() == 0'u64 # top-level trailer
    check r.remaining == 0

  test "TxnOffsetCommitResponse decode v3 (flexible)":
    var w = initKafkaWriter()
    w.writeInt32(30'i32) # throttleTimeMs
    w.writeCompactArrayLen(1) # topics
    w.writeCompactString("topic-1")
    w.writeCompactArrayLen(1) # partitions
    w.writeInt32(0'i32) # partition
    w.writeInt16(0'i16) # errorCode NONE
    w.encodeUvarint(0) # partition struct trailer
    w.encodeUvarint(0) # topic struct trailer
    w.encodeUvarint(0) # top-level trailer
    var r = initKafkaReader(w.buf)
    let resp = r.decode(TxnOffsetCommitResponse, 3'i16)
    check resp.throttleTimeMs == 30
    check resp.topics.len == 1
    check resp.topics[0].topic == "topic-1"
    check resp.topics[0].partitions[0].errorCode == keNone
    check r.remaining == 0

suite "Serializer encode roundtrip":
  test "MetadataRequest encode with topics (v1, legacy wire)":
    let req = MetadataRequest(
      topics: some(
        @[MetadataRequestTopic(name: "topic-a"), MetadataRequestTopic(name: "topic-b")]
      )
    )
    var w = initKafkaWriter()
    w.encode(req, 1'i16)
    var r = initKafkaReader(w.buf)
    check r.readArrayLen() == 2
    check r.readString() == "topic-a"
    check r.readString() == "topic-b"
    check r.remaining == 0

  test "MetadataRequest encode with null topics (v1, legacy wire)":
    let req = MetadataRequest(topics: none(seq[MetadataRequestTopic]))
    var w = initKafkaWriter()
    w.encode(req, 1'i16)
    var r = initKafkaReader(w.buf)
    # Wire value is -1 (null array); readArrayLen clamps to 0,
    # so check the raw int32 to verify serialization correctness.
    check r.readInt32() == -1'i32
    check r.remaining == 0

  test "ListOffsetsRequest encode":
    let req = ListOffsetsRequest(
      replicaId: -1,
      topics: @[
        ListOffsetsTopicReq(
          topic: "test-topic",
          partitions: @[ListOffsetsPartitionReq(partition: 0, timestamp: -1)],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(req)
    var r = initKafkaReader(w.buf)
    check r.readInt32() == -1'i32
    check r.readArrayLen() == 1
    check r.readString() == "test-topic"
    check r.readArrayLen() == 1
    check r.readInt32() == 0'i32
    check r.readInt64() == -1'i64
    check r.remaining == 0

  test "FetchRequest encode":
    let req = FetchRequest(
      replicaId: -1,
      maxWaitMs: 500,
      minBytes: 1,
      maxBytes: 1048576,
      isolationLevel: ilReadUncommitted,
      topics: @[
        FetchTopicReq(
          topic: "fetch-topic",
          partitions:
            @[FetchPartitionReq(partition: 3, fetchOffset: 100, maxBytes: 65536)],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(req)
    var r = initKafkaReader(w.buf)
    check r.readInt32() == -1'i32
    check r.readInt32() == 500'i32
    check r.readInt32() == 1'i32
    check r.readInt32() == 1048576'i32
    check r.readInt8() == 0'i8
    check r.readArrayLen() == 1
    check r.readString() == "fetch-topic"
    check r.readArrayLen() == 1
    check r.readInt32() == 3'i32
    check r.readInt64() == 100'i64
    check r.readInt32() == 65536'i32
    check r.remaining == 0

  test "JoinGroupRequest encode":
    let req = JoinGroupRequest(
      groupId: "group-1",
      sessionTimeoutMs: 30000,
      rebalanceTimeoutMs: 60000,
      memberId: "",
      protocolType: "consumer",
      protocols: @[JoinGroupProtocol(name: "range", metadata: @[0x00'u8, 0x01])],
    )
    var w = initKafkaWriter()
    w.encode(req)
    var r = initKafkaReader(w.buf)
    check r.readString() == "group-1"
    check r.readInt32() == 30000'i32
    check r.readInt32() == 60000'i32
    check r.readString() == ""
    check r.readString() == "consumer"
    check r.readArrayLen() == 1
    check r.readString() == "range"
    let metaLen = r.readInt32()
    check metaLen == 2'i32
    let metaBytes = r.readBytes(int(metaLen))
    check metaBytes == @[0x00'u8, 0x01]
    check r.remaining == 0

  test "OffsetCommitRequest encode v2":
    # v2 wire layout: groupId, generationId, memberId, retentionTimeMs,
    # topics[topic, partitions[partition, offset, metadata]].
    # committedLeaderEpoch (v6+) and groupInstanceId (v7+) are not emitted.
    let req = OffsetCommitRequest(
      groupId: "g1",
      generationId: 5,
      memberId: "member-abc",
      groupInstanceId: none(string),
      retentionTimeMs: -1,
      topics: @[
        OffsetCommitTopicReq(
          topic: "t1",
          partitions: @[
            OffsetCommitPartitionReq(
              partition: 0, offset: 42, committedLeaderEpoch: -1, metadata: some("meta")
            )
          ],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(req, 2'i16)
    var r = initKafkaReader(w.buf)
    check r.readString() == "g1"
    check r.readInt32() == 5'i32
    check r.readString() == "member-abc"
    check r.readInt64() == -1'i64
    check r.readArrayLen() == 1
    check r.readString() == "t1"
    check r.readArrayLen() == 1
    check r.readInt32() == 0'i32
    check r.readInt64() == 42'i64
    check r.readNullableString() == some("meta")
    check r.remaining == 0

  test "OffsetCommitRequest encode v6 carries committedLeaderEpoch (KIP-320)":
    # v6 adds committedLeaderEpoch per partition after offset, before metadata.
    # retentionTimeMs is gone (dropped at v5), groupInstanceId not yet present.
    let req = OffsetCommitRequest(
      groupId: "g1",
      generationId: 5,
      memberId: "member-abc",
      groupInstanceId: none(string),
      retentionTimeMs: 0,
      topics: @[
        OffsetCommitTopicReq(
          topic: "t1",
          partitions: @[
            OffsetCommitPartitionReq(
              partition: 0, offset: 42, committedLeaderEpoch: 17, metadata: some("meta")
            )
          ],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(req, 6'i16)
    var r = initKafkaReader(w.buf)
    check r.readString() == "g1"
    check r.readInt32() == 5'i32
    check r.readString() == "member-abc"
    # No retentionTimeMs at v6.
    check r.readArrayLen() == 1
    check r.readString() == "t1"
    check r.readArrayLen() == 1
    check r.readInt32() == 0'i32
    check r.readInt64() == 42'i64
    check r.readInt32() == 17'i32 # committedLeaderEpoch
    check r.readNullableString() == some("meta")
    check r.remaining == 0

  test "ProduceRequest encode":
    let req = ProduceRequest(
      transactionalId: none(string),
      acks: -1,
      timeoutMs: 30000,
      topics: @[
        ProduceTopicReq(
          topic: "prod-topic",
          partitions: @[ProducePartitionReq(partition: 0, recordSet: @[0xAA'u8, 0xBB])],
        )
      ],
    )
    var w = initKafkaWriter()
    w.encode(req)
    var r = initKafkaReader(w.buf)
    check r.readNullableString() == none(string) # transactionalId
    check r.readInt16() == -1'i16 # acks
    check r.readInt32() == 30000'i32 # timeoutMs
    check r.readArrayLen() == 1
    check r.readString() == "prod-topic"
    check r.readArrayLen() == 1
    check r.readInt32() == 0'i32 # partition
    let rsLen = r.readInt32()
    check rsLen == 2'i32
    let rs = r.readBytes(int(rsLen))
    check rs == @[0xAA'u8, 0xBB]
    check r.remaining == 0

  test "HeartbeatRequest encode":
    let req = HeartbeatRequest(groupId: "hb-group", generationId: 3, memberId: "m-1")
    var w = initKafkaWriter()
    w.encode(req)
    var r = initKafkaReader(w.buf)
    check r.readString() == "hb-group"
    check r.readInt32() == 3'i32
    check r.readString() == "m-1"
    check r.remaining == 0

  test "EndTxnRequest encode":
    let req = EndTxnRequest(
      transactionalId: "txn-1", producerId: 12345, producerEpoch: 2, committed: true
    )
    var w = initKafkaWriter()
    w.encode(req)
    var r = initKafkaReader(w.buf)
    check r.readString() == "txn-1"
    check r.readInt64() == 12345'i64
    check r.readInt16() == 2'i16
    check r.readBool() == true
    check r.remaining == 0

  test "EndTxnRequest encode v3 (flexible: compact string + tagged trailer)":
    let req = EndTxnRequest(
      transactionalId: "txn-1", producerId: 12345, producerEpoch: 2, committed: true
    )
    var w = initKafkaWriter()
    w.encode(req, 3'i16)
    var r = initKafkaReader(w.buf)
    check r.readCompactString() == "txn-1"
    check r.readInt64() == 12345'i64
    check r.readInt16() == 2'i16
    check r.readBool() == true
    # top-level tagged-fields trailer (count = 0)
    check r.decodeUvarint() == 0'u64
    check r.remaining == 0

  test "FindCoordinatorRequest encode v0":
    var w = initKafkaWriter()
    w.encode(FindCoordinatorRequest(key: "my-group", coordinatorType: 0), 0'i16)
    var r = initKafkaReader(w.buf)
    check r.readString() == "my-group"
    check r.remaining == 0 # v0: no coordinatorType field

  test "FindCoordinatorRequest encode v1":
    var w = initKafkaWriter()
    w.encode(FindCoordinatorRequest(key: "my-txn", coordinatorType: 1), 1'i16)
    var r = initKafkaReader(w.buf)
    check r.readString() == "my-txn"
    check r.readInt8() == 1'i8
    check r.remaining == 0

  test "InitProducerIdRequest encode v0":
    let req = InitProducerIdRequest(
      transactionalId: some("txn-1"),
      transactionTimeoutMs: 60000,
      producerId: -1,
      producerEpoch: -1,
    )
    var w = initKafkaWriter()
    w.encode(req, 0'i16)
    var r = initKafkaReader(w.buf)
    check r.readNullableString() == some("txn-1")
    check r.readInt32() == 60000'i32
    check r.remaining == 0 # v0: no producerId/epoch

  test "InitProducerIdRequest encode v2 (flexible, no producerId/epoch)":
    # Per Kafka spec, producer_id and producer_epoch are v3+ only. The
    # schema gates them with `sinceVersion: 3` so v2 wire is just
    # transactionalId (compact nullable) + transactionTimeoutMs +
    # tagged_fields trailer.
    let req = InitProducerIdRequest(
      transactionalId: some("txn-2"),
      transactionTimeoutMs: 30000,
      producerId: 100, # ignored at v2
      producerEpoch: 5, # ignored at v2
    )
    var w = initKafkaWriter()
    w.encode(req, 2'i16)
    var r = initKafkaReader(w.buf)
    let strLenPlusOne = r.decodeUvarint()
    check strLenPlusOne == 6'u64
    let strBytes = r.readBytes(5)
    check strBytes == @[byte('t'), byte('x'), byte('n'), byte('-'), byte('2')]
    check r.readInt32() == 30000'i32
    check r.data[r.pos] == 0x00'u8 # tagged_fields
    check r.remaining == 1

  test "InitProducerIdRequest encode v3 (flexible, with producerId/epoch)":
    let req = InitProducerIdRequest(
      transactionalId: some("txn-3"),
      transactionTimeoutMs: 30000,
      producerId: 100,
      producerEpoch: 5,
    )
    var w = initKafkaWriter()
    w.encode(req, 3'i16)
    var r = initKafkaReader(w.buf)
    let strLenPlusOne = r.decodeUvarint()
    check strLenPlusOne == 6'u64
    let strBytes = r.readBytes(5)
    check strBytes == @[byte('t'), byte('x'), byte('n'), byte('-'), byte('3')]
    check r.readInt32() == 30000'i32
    check r.readInt64() == 100'i64
    check r.readInt16() == 5'i16
    check r.data[r.pos] == 0x00'u8 # tagged_fields

suite "Null array handling in deserializer":
  test "ApiVersionsResponse with null apiKeys array decodes to empty seq":
    var w = initKafkaWriter()
    w.writeInt16(0) # errorCode = NONE
    w.writeArrayLen(-1) # null array on wire
    var r = initKafkaReader(w.buf)
    let resp = r.decode(ApiVersionsResponse)
    check resp.errorCode == keNone
    check resp.apiKeys.len == 0

  test "MetadataResponse with null brokers array decodes to empty seq":
    var w = initKafkaWriter()
    w.writeArrayLen(-1) # null brokers
    w.writeInt32(0) # controllerId
    w.writeArrayLen(-1) # null topics
    var r = initKafkaReader(w.buf)
    let resp = r.decode(MetadataResponse)
    check resp.brokers.len == 0
    check resp.topics.len == 0

  test "ListOffsetsResponse with null topics array decodes to empty seq":
    var w = initKafkaWriter()
    w.writeArrayLen(-1) # null topics
    var r = initKafkaReader(w.buf)
    let resp = r.decode(ListOffsetsResponse)
    check resp.topics.len == 0

  test "ProduceResponse with null topics array decodes to empty seq":
    var w = initKafkaWriter()
    w.writeArrayLen(-1) # null topics
    w.writeInt32(0) # throttleTimeMs
    var r = initKafkaReader(w.buf)
    let resp = r.decode(ProduceResponse)
    check resp.topics.len == 0
    check resp.throttleTimeMs == 0

suite "Protocol edge cases":
  test "readNullableBytes rejects length exceeding 100MB limit":
    var w = initKafkaWriter()
    w.writeInt32(104_857_601'i32) # 100MB + 1
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.readNullableBytes()

  test "readArrayLen rejects values above maxWireArrayLen":
    var w = initKafkaWriter()
    w.writeInt32(10_000_001'i32)
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.readArrayLen()

  test "readArrayLen rejects values below -1":
    var w = initKafkaWriter()
    w.writeInt32(-2'i32)
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.readArrayLen()

  test "decodeUvarint rejects overflow (shift >= 64)":
    # 10 bytes of 0xFF followed by a terminator — would shift past 64 bits
    var data: seq[byte]
    for i in 0 ..< 10:
      data.add(0xFF'u8)
    data.add(0x00'u8)
    var r = initKafkaReader(data)
    expect KafkaProtocolError:
      discard r.decodeUvarint()

suite "readNullableArrayLen":
  test "null array returns -1":
    var w = initKafkaWriter()
    w.writeInt32(-1'i32)
    var r = initKafkaReader(w.buf)
    check r.readNullableArrayLen() == -1'i32

  test "empty array returns 0":
    var w = initKafkaWriter()
    w.writeInt32(0'i32)
    var r = initKafkaReader(w.buf)
    check r.readNullableArrayLen() == 0'i32

  test "positive array length returned as-is":
    var w = initKafkaWriter()
    w.writeInt32(42'i32)
    var r = initKafkaReader(w.buf)
    check r.readNullableArrayLen() == 42'i32

  test "out of range value raises":
    var w = initKafkaWriter()
    w.writeInt32(-2'i32)
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.readNullableArrayLen()

suite "Protocol overflow protection":
  test "writeString rejects strings exceeding int16 max":
    var w = initKafkaWriter()
    let bigStr = newString(32768)
    expect KafkaProtocolError:
      w.writeString(bigStr)

  test "writeString accepts strings at int16 max":
    var w = initKafkaWriter()
    let maxStr = newString(32767)
    w.writeString(maxStr) # should not raise

  test "encodeVarintTo handles int64 max (10-byte varint)":
    var buf: array[10, byte]
    let n = encodeVarintTo(buf, int64.high)
    check n == 10

  test "encodeVarintTo raises on undersized buffer":
    var buf: array[1, byte]
    expect KafkaProtocolError:
      discard encodeVarintTo(buf, int64.high)

  test "decodeRecordBatch rejects negative batchLength":
    var w = initKafkaWriter()
    w.writeInt64(0'i64) # baseOffset
    w.writeInt32(-1'i32) # batchLength (invalid)
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.decode(RecordBatch)

  test "decodeRecordBatch rejects zero batchLength":
    var w = initKafkaWriter()
    w.writeInt64(0'i64) # baseOffset
    w.writeInt32(0'i32) # batchLength (invalid)
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.decode(RecordBatch)

  test "decodeRecordBatch rejects batchLength below the format-discriminator size":
    # batchLength 1..4 is too short for partitionLeaderEpoch(4) + magic(1); the
    # legacy-skip branch would previously over-read into the next batch and
    # desync framing for every following batch in the fetch response.
    for badLen in 1'i32 .. 4'i32:
      var w = initKafkaWriter()
      w.writeInt64(0'i64) # baseOffset
      w.writeInt32(badLen) # batchLength (too small to read magic)
      for _ in 0 ..< int(badLen):
        w.buf.add(0'u8)
      var r = initKafkaReader(w.buf)
      expect KafkaProtocolError:
        discard r.decode(RecordBatch)

  test "decodeRecordBatch honours reader.limit against a forged batchLength":
    # Two concatenated batches. Corrupt batch1's batchLength so batchEnd would
    # land inside batch2, then set reader.limit to end exactly at batch1's true
    # boundary. Without a limit-aware guard the decoder would pull CRC input
    # (and drift header reads) from batch2.
    let mkBatch = proc(baseOffset: int64): seq[byte] =
      var w = initKafkaWriter()
      w.encode(
        RecordBatch(
          baseOffset: baseOffset,
          firstTimestamp: 0'i64,
          producerId: -1'i64,
          producerEpoch: -1'i16,
          baseSequence: -1'i32,
          records: @[
            Record(
              offset: baseOffset,
              timestamp: 0'i64,
              key: none(seq[byte]),
              value: some("x".toBytes),
              headers: @[],
            )
          ],
        )
      )
      w.buf

    let batch1 = mkBatch(0'i64)
    let batch2 = mkBatch(1'i64)
    var combined = batch1 & batch2
    let forgedLen = int32(batch1.len - 12 + 4)
    let lenBE = toBytesBE(cast[uint32](forgedLen))
    for i in 0 ..< 4:
      combined[8 + i] = lenBE[i]
    var r = initKafkaReader(combined)
    r.limit = batch1.len
    expect KafkaProtocolError:
      discard r.decode(RecordBatch)

  test "decompressRecords rejects output exceeding maxDecompressSize":
    let data = newSeq[byte](1000)
    let compressed = compressRecords(data, ccSnappy)
    expect KafkaError:
      discard decompressRecords(compressed, ccSnappy, maxDecompressSize = 100)

suite "skipResponseHeaderTaggedFields":
  test "single-byte varint (0x00)":
    let data = [0x00'u8]
    check skipResponseHeaderTaggedFields(data, 0) == 1

  test "single-byte varint at offset":
    let data = [0xFF'u8, 0xFF'u8, 0xFF'u8, 0xFF'u8, 0x00'u8]
    check skipResponseHeaderTaggedFields(data, 4) == 5

  test "non-zero count without entries is truncated":
    # count = 128 (2-byte varint) but no tagged-field entries follow. Under
    # KIP-482 the trailer is count + entries, so a bare count with nothing
    # after it is malformed/truncated (the old impl wrongly skipped just the
    # count varint and returned 2).
    let data = [0x80'u8, 0x01'u8]
    expect KafkaProtocolError:
      discard skipResponseHeaderTaggedFields(data, 0)

  test "implausibly large count is rejected":
    # count = 16384 (3-byte varint) exceeds the tagged-fields sanity limit.
    let data = [0x80'u8, 0x80'u8, 0x01'u8]
    expect KafkaProtocolError:
      discard skipResponseHeaderTaggedFields(data, 0)

  test "malformed varint (all high bits set)":
    # The varint is well-formed: five continuation bytes then 0x01 decode to
    # 2^35, a count past the tagged-fields sanity limit (10_000).
    let data = [0x80'u8, 0x80, 0x80, 0x80, 0x80, 0x01]
    expect KafkaProtocolError:
      discard skipResponseHeaderTaggedFields(data, 0)

  test "truncated varint (buffer ends mid-varint)":
    let data = [0x80'u8, 0x80'u8]
    expect KafkaProtocolError:
      discard skipResponseHeaderTaggedFields(data, 0)

  test "empty buffer at offset":
    let data: seq[byte] = @[]
    expect KafkaProtocolError:
      discard skipResponseHeaderTaggedFields(data, 0)

suite "Compact primitives round-trip":
  test "compact string round-trip":
    var w = initKafkaWriter()
    w.writeCompactString("hello")
    var r = initKafkaReader(w.buf)
    check r.readCompactString() == "hello"

  test "compact string at length boundaries":
    for length in [0, 1, 126, 127, 128, 16383, 16384]:
      var w = initKafkaWriter()
      let s = newString(length)
      w.writeCompactString(s)
      var r = initKafkaReader(w.buf)
      check r.readCompactString().len == length

  test "compact nullable string round-trip none":
    var w = initKafkaWriter()
    w.writeCompactNullableString(none(string))
    var r = initKafkaReader(w.buf)
    check r.readCompactNullableString().isNone

  test "compact nullable string round-trip some empty":
    var w = initKafkaWriter()
    w.writeCompactNullableString(some(""))
    var r = initKafkaReader(w.buf)
    let v = r.readCompactNullableString()
    check v.isSome
    check v.get == ""

  test "compact nullable string round-trip some value":
    var w = initKafkaWriter()
    w.writeCompactNullableString(some("kafka"))
    var r = initKafkaReader(w.buf)
    let v = r.readCompactNullableString()
    check v.isSome
    check v.get == "kafka"

  test "compact bytes round-trip":
    var w = initKafkaWriter()
    w.writeCompactBytes(@[0xDE'u8, 0xAD, 0xBE, 0xEF])
    var r = initKafkaReader(w.buf)
    check r.readCompactBytes() == @[0xDE'u8, 0xAD, 0xBE, 0xEF]

  test "compact bytes empty round-trip":
    var w = initKafkaWriter()
    w.writeCompactBytes(@[])
    var r = initKafkaReader(w.buf)
    check r.readCompactBytes().len == 0

  test "compact nullable bytes round-trip none":
    var w = initKafkaWriter()
    w.writeCompactNullableBytes(none(seq[byte]))
    var r = initKafkaReader(w.buf)
    check r.readCompactNullableBytes().isNone

  test "compact nullable bytes round-trip some":
    var w = initKafkaWriter()
    w.writeCompactNullableBytes(some(@[0x01'u8, 0x02, 0x03]))
    var r = initKafkaReader(w.buf)
    let v = r.readCompactNullableBytes()
    check v.isSome
    check v.get == @[0x01'u8, 0x02, 0x03]

  test "readCompactString rejects null length":
    var w = initKafkaWriter()
    w.encodeUvarint(0) # length+1 = 0 → null
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.readCompactString()

  test "compact array length round-trip":
    var w = initKafkaWriter()
    w.writeCompactArrayLen(0)
    w.writeCompactArrayLen(1)
    w.writeCompactArrayLen(42)
    w.writeCompactArrayLen(-1) # null
    var r = initKafkaReader(w.buf)
    check r.readCompactArrayLen() == 0'i32
    check r.readCompactArrayLen() == 1'i32
    check r.readCompactArrayLen() == 42'i32
    check r.readCompactArrayLen() == -1'i32

suite "Tagged fields parsing":
  test "empty tagged fields trailer":
    var w = initKafkaWriter()
    w.writeTaggedFieldsEmpty()
    check w.buf == @[0x00'u8]
    var r = initKafkaReader(w.buf)
    r.skipTaggedFields()
    check r.remaining == 0

  test "skipTaggedFields walks past unknown entries":
    # Build a buffer with two tagged fields manually:
    #   count = 2
    #   entry 1: tagId=5, length=3, payload=[0xAA, 0xBB, 0xCC]
    #   entry 2: tagId=99, length=1, payload=[0x42]
    var w = initKafkaWriter()
    w.encodeUvarint(2)
    w.encodeUvarint(5) # tagId
    w.encodeUvarint(3) # length
    w.buf.add(@[0xAA'u8, 0xBB, 0xCC])
    w.encodeUvarint(99)
    w.encodeUvarint(1)
    w.buf.add(0x42'u8)
    # Append a sentinel byte after the trailer so we can verify the cursor
    # landed exactly at the end of the tagged-fields region.
    w.writeInt8(0x77'i8)
    var r = initKafkaReader(w.buf)
    r.skipTaggedFields()
    check r.readInt8() == 0x77'i8

  test "skipTaggedFields rejects truncated payload":
    var w = initKafkaWriter()
    w.encodeUvarint(1) # one entry
    w.encodeUvarint(0) # tagId
    w.encodeUvarint(10) # length=10, but no bytes follow
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      r.skipTaggedFields()

  test "skipTaggedFields rejects wildly oversized count":
    var w = initKafkaWriter()
    w.encodeUvarint(1_000_000) # well above sanity limit
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      r.skipTaggedFields()

suite "decodeUvarint overflow tightening":
  test "rejects 10th-byte high bits that would overflow uint64":
    # 9 bytes of 0xFF (continuation) + a 10th byte 0x7F (no continuation,
    # but seven bits set — only one bit fits at shift=63).
    var data: seq[byte] = @[]
    for _ in 0 ..< 9:
      data.add(0xFF'u8)
    data.add(0x7F'u8)
    var r = initKafkaReader(data)
    expect KafkaProtocolError:
      discard r.decodeUvarint()

  test "accepts 10-byte encoding of uint64.high":
    # Maximum legal uint64 varint: 9 bytes of 0xFF + 0x01.
    var data: seq[byte] = @[]
    for _ in 0 ..< 9:
      data.add(0xFF'u8)
    data.add(0x01'u8)
    var r = initKafkaReader(data)
    check r.decodeUvarint() == uint64.high

suite "decodeVarint overflow tightening (record-batch varint)":
  ## decodeVarint is the signed/zigzag variant used by record batches.
  ## Without the final-byte high-bit check, a crafted record could feed
  ## values that wrap into a small int64 instead of being rejected as
  ## malformed.

  test "rejects 10th byte with high data bits (overflow)":
    var data: seq[byte] = @[]
    for _ in 0 ..< 9:
      data.add(0xFF'u8)
    # 10th byte: no continuation but high bits set — must be rejected.
    data.add(0x7F'u8)
    var r = initKafkaReader(data)
    expect KafkaProtocolError:
      discard r.decodeVarint()

  test "rejects 11+ byte varints":
    var data: seq[byte] = @[]
    for _ in 0 ..< 11:
      data.add(0xFF'u8) # all continuation bits set forces overrun
    var r = initKafkaReader(data)
    expect KafkaProtocolError:
      discard r.decodeVarint()

  test "accepts a normal signed value":
    var w = initKafkaWriter()
    w.encodeVarint(-1234567890'i64)
    var r = initKafkaReader(w.buf)
    check r.decodeVarint() == -1234567890'i64

suite "compact reader uint64 length truncation":
  # The compact readers previously cast `lenPlusOne: uint64` to `int` before
  # comparing against maxWireBytesLen. A crafted near-uint64.high length
  # would wrap to a small / negative int and bypass the size guard, allowing
  # an attacker-controlled read to corrupt memory. These tests pin the
  # uint64-first bounds check.
  proc uint64MaxVarint(): seq[byte] =
    var w = initKafkaWriter()
    w.encodeUvarint(uint64.high)
    w.buf

  test "readCompactBytes rejects uint64.high length":
    var r = initKafkaReader(uint64MaxVarint())
    expect KafkaProtocolError:
      discard r.readCompactBytes()

  test "readCompactNullableBytes rejects uint64.high length":
    var r = initKafkaReader(uint64MaxVarint())
    expect KafkaProtocolError:
      discard r.readCompactNullableBytes()

  test "readCompactString rejects uint64.high length":
    var r = initKafkaReader(uint64MaxVarint())
    expect KafkaProtocolError:
      discard r.readCompactString()

  test "readCompactNullableString rejects uint64.high length":
    var r = initKafkaReader(uint64MaxVarint())
    expect KafkaProtocolError:
      discard r.readCompactNullableString()

  test "readCompactArrayLen rejects uint64.high length":
    var r = initKafkaReader(uint64MaxVarint())
    expect KafkaProtocolError:
      discard r.readCompactArrayLen()

  test "readCompactBytes still accepts a legal short payload":
    var w = initKafkaWriter()
    w.encodeUvarint(uint64(4)) # length+1 == 4 -> length == 3
    w.buf.add(@[0xDE'u8, 0xAD, 0xBE])
    var r = initKafkaReader(w.buf)
    check r.readCompactBytes() == @[0xDE'u8, 0xAD, 0xBE]

suite "decodeVarBytes/String maxLen window":
  test "decodeVarBytes raises before reading when length exceeds maxLen":
    # Encode varint length 100; pass maxLen=10. Must raise without touching
    # the (intentionally absent) payload bytes.
    var w = initKafkaWriter()
    w.encodeVarint(100)
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.decodeVarBytes(maxLen = 10)

  test "decodeVarString raises before reading when length exceeds maxLen":
    var w = initKafkaWriter()
    w.encodeVarint(100)
    var r = initKafkaReader(w.buf)
    expect KafkaProtocolError:
      discard r.decodeVarString(maxLen = 10)

  test "decodeVarBytes accepts exact maxLen boundary":
    var w = initKafkaWriter()
    w.encodeVarint(3)
    w.buf.add(@[1'u8, 2, 3])
    var r = initKafkaReader(w.buf)
    let v = r.decodeVarBytes(maxLen = 3)
    check v.isSome and v.get == @[1'u8, 2, 3]

  test "decodeVarBytes negative length still means null regardless of maxLen":
    var w = initKafkaWriter()
    w.encodeVarint(-1)
    var r = initKafkaReader(w.buf)
    check r.decodeVarBytes(maxLen = 0).isNone

suite "decodeRecords per-record window rejection":
  # Build a v2 RecordBatch where a single record's encoded key length
  # exceeds the record's own window. The per-record `rdr.limit` that
  # loadRecords pins to recordEnd makes readBytes/checkRemaining reject the
  # oversized key before the (much larger) seq is allocated, instead of
  # decoding it and only catching the over-read after the fact.
  proc oversizedKeyBatch(): seq[byte] =
    # Build the record body first to know its size.
    var body = initKafkaWriter()
    body.writeInt8(0'i8) # attributes
    body.encodeVarint(0) # timestampDelta
    body.encodeVarint(0) # offsetDelta
    # Key length claims 1000 bytes — far larger than the actual record.
    body.encodeVarint(1000)
    # Don't write the 1000 bytes; the decoder must reject before reading.
    let recordBody = body.buf

    var batch = initKafkaWriter()
    # RecordBatch header (v2). batchLength == bytes after batchLength.
    batch.writeInt64(0'i64) # baseOffset
    # We'll patch batchLength after we know the rest.
    let lenPos = batch.buf.len
    batch.writeInt32(0'i32) # placeholder
    batch.writeInt32(0'i32) # partitionLeaderEpoch
    batch.writeInt8(2'i8) # magic v2
    let crcPos = batch.buf.len
    batch.writeInt32(0'i32) # crc (patched with the real crc32c below)
    batch.writeInt16(0'i16) # attributes (no compression)
    batch.writeInt32(0'i32) # lastOffsetDelta
    batch.writeInt64(0'i64) # firstTimestamp
    batch.writeInt64(0'i64) # maxTimestamp
    batch.writeInt64(-1'i64) # producerId
    batch.writeInt16(-1'i16) # producerEpoch
    batch.writeInt32(-1'i32) # baseSequence
    batch.writeInt32(1'i32) # numRecords
    # Record entry: varint(recordLength) + body
    batch.encodeVarint(int64(recordBody.len))
    batch.buf.add(recordBody)

    # Patch batchLength = total - (baseOffset + batchLength prefix = 12)
    let total = batch.buf.len
    let computedLen = int32(total - 12)
    let be = toBytesBE(cast[uint32](computedLen))
    copyMem(addr batch.buf[lenPos], unsafeAddr be[0], 4)

    # Compute and write CRC32C so decodeRecordBatch doesn't reject early.
    let crcStart = crcPos + 4
    let crc = crc32c(batch.buf.toOpenArray(crcStart, batch.buf.len - 1))
    let crcBE = toBytesBE(crc)
    copyMem(addr batch.buf[crcPos], unsafeAddr crcBE[0], 4)

    batch.buf

  test "decodeRecordBatch rejects oversized record key length":
    let data = oversizedKeyBatch()
    var r = initKafkaReader(data)
    var raised = false
    try:
      discard r.decode(RecordBatch)
    except KafkaProtocolError:
      raised = true
    check raised
    # The schema-driven decoder calls `readBytes(1000)` which checks the
    # remaining buffer before allocating, so the rejection happens before
    # any large allocation regardless of whether the message says
    # "buffer underflow" or "window". The semantic guarantee — early
    # rejection of an attacker-controlled length — is what matters.

  test "a rejected record restores the reader limit":
    # The per-record window is temporary. Before the fix, `rdr.limit =
    # recordEnd` survived the failed decode, so a caller that caught the error
    # and kept walking the blob saw a negative `remaining` and silently
    # dropped every following batch.
    let data = oversizedKeyBatch()
    var r = initKafkaReader(data)
    var raised = false
    try:
      discard r.decode(RecordBatch)
    except KafkaProtocolError:
      raised = true
    check raised
    check r.limit == 0

suite "initKafkaReader sink overload":
  ## The seq[byte] overload takes `sink seq[byte]`, so Nim moves the buffer
  ## instead of deep-copying it — critical on MB-scale fetch payloads.

  test "round-trip equivalence: data read back matches what was written":
    var w = initKafkaWriter()
    w.writeInt32(1234567'i32)
    w.writeString("hello")
    w.writeInt64(987654321'i64)
    var r = initKafkaReader(w.buf)
    check r.readInt32() == 1234567'i32
    check r.readString() == "hello"
    check r.readInt64() == 987654321'i64

  test "sink overload still works when the source is moved":
    var src = newSeq[byte](16)
    for i in 0 ..< 16:
      src[i] = byte(i)
    var r = initKafkaReader(move src)
    # The reader should contain the original 16 bytes.
    check r.data.len == 16
    for i in 0 ..< 16:
      check r.data[i] == byte(i)

  test "openArray overload still copies (no source-ownership transfer)":
    let src = @[0x01'u8, 0x02'u8, 0x03'u8]
    var r = initKafkaReader(src.toOpenArray(0, 2))
    check r.data == @[0x01'u8, 0x02'u8, 0x03'u8]
    # The openArray overload copies into the reader, which owns an independent
    # buffer; no ownership of `src` is transferred.
    check src.len == 3

suite "Produce v3 wire fixture":
  ## Pins the broker-side wire layout for a Produce v3 response from an
  ## external source, so a one-sided encoder/decoder drift is caught (most
  ## pin tests round-trip our own writer+reader and miss it). Layout mirrors
  ## librdkafka's Produce response parsing: the topic array is read before the
  ## trailing throttle_time_ms.

  test "ProduceResponse v3 decode pins external wire layout":
    # Produce v3 response body for topic "test", partition 0, no error,
    # base_offset = 42, log_append_time = -1, throttle_time_ms = 250.
    # Layout: [topics] throttle_time_ms (note: throttle is at end of
    # body for Produce, unlike OffsetCommit/Fetch which put it first).
    let fixture: seq[byte] = @[
      # numTopics = 1
      0x00'u8,
      0x00,
      0x00,
      0x01,
      # topic name length = 4
      0x00'u8,
      0x04,
      # topic = "test"
      0x74'u8,
      0x65,
      0x73,
      0x74,
      # numPartitions = 1
      0x00'u8,
      0x00,
      0x00,
      0x01,
      # partition = 0
      0x00'u8,
      0x00,
      0x00,
      0x00,
      # error_code = 0
      0x00'u8,
      0x00,
      # base_offset = 42 (int64)
      0x00'u8,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x00,
      0x2A,
      # log_append_time = -1 (int64, sentinel for CREATE_TIME)
      0xFF'u8,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      0xFF,
      # throttle_time_ms = 250 (int32, trailing)
      0x00'u8,
      0x00,
      0x00,
      0xFA,
    ]
    var r = initKafkaReader(fixture)
    let resp = r.decode(ProduceResponse)
    check resp.topics.len == 1
    check resp.topics[0].topic == "test"
    check resp.topics[0].partitions.len == 1
    check resp.topics[0].partitions[0].partition == 0'i32
    check resp.topics[0].partitions[0].errorCode == keNone
    check resp.topics[0].partitions[0].baseOffset == 42'i64
    check resp.topics[0].partitions[0].timestampMs == -1'i64
    check resp.throttleTimeMs == 250'i32
    check r.remaining == 0

  test "ProduceResponse v7 decode reads logStartOffset (KIP-31)":
    # v5+ inserts logStartOffset (int64) after logAppendTime in the partition
    # response. The whole buffer must be consumed — a decoder that skipped the
    # field (decoding at < v5) would leave 8 bytes unread or misread throttle.
    var w = initKafkaWriter()
    w.writeArrayLen(1'i32) # numTopics
    w.writeString("test")
    w.writeArrayLen(1'i32) # numPartitions
    w.writeInt32(0'i32) # partition
    w.writeInt16(0'i16) # errorCode NONE
    w.writeInt64(42'i64) # baseOffset
    w.writeInt64(-1'i64) # logAppendTime
    w.writeInt64(7'i64) # logStartOffset (v5+)
    w.writeInt32(250'i32) # throttleTimeMs (trailing)
    var r = initKafkaReader(w.buf)
    let resp = r.decode(ProduceResponse, 7'i16)
    check resp.topics[0].partitions[0].baseOffset == 42'i64
    check resp.topics[0].partitions[0].timestampMs == -1'i64
    check resp.topics[0].partitions[0].logStartOffset == 7'i64
    check resp.throttleTimeMs == 250'i32
    check r.remaining == 0

suite "Flexible response header tagged fields":
  test "skipResponseHeaderTaggedFields skips an empty tag buffer":
    # [correlationId(4)][tagged_fields = 0x00][body...]
    let frame = @[0'u8, 0, 0, 1, 0x00, 0xAA, 0xBB]
    let bodyStart = skipResponseHeaderTaggedFields(frame, 4)
    check bodyStart == 5
    check frame[bodyStart ..< frame.len] == @[0xAA'u8, 0xBB]

  test "skipResponseHeaderTaggedFields skips a populated tag buffer":
    # tagged_fields: count=1, then (tagId=0, len=3, payload=3 bytes), then body.
    # offset 4: 0x01 0x00 0x03 p p p | body
    let frame = @[0'u8, 0, 0, 1, 0x01, 0x00, 0x03, 0x11, 0x22, 0x33, 0xAA, 0xBB]
    let bodyStart = skipResponseHeaderTaggedFields(frame, 4)
    check bodyStart == 10
    check frame[bodyStart ..< frame.len] == @[0xAA'u8, 0xBB]

  test "skipResponseHeaderTaggedFields rejects a payload length past the buffer":
    # count=1, tagId=0, len=200 but only a few bytes remain.
    let frame = @[0'u8, 0, 0, 1, 0x01, 0x00, 200'u8, 0x01, 0x02]
    var raised = false
    try:
      discard skipResponseHeaderTaggedFields(frame, 4)
    except KafkaProtocolError:
      raised = true
    check raised

suite "LZ4 frame codec":
  test "lz4 round-trips a single frame":
    let payload = cast[seq[byte]]("hello lz4 round trip payload")
    let compressed = lz4mod.lz4Compress(payload)
    check lz4mod.lz4Decompress(compressed) == payload

  test "lz4Decompress consumes concatenated multi-frame input":
    # Defensive: a stream of two back-to-back LZ4 frames must decode to the
    # concatenation of both payloads, not just the first frame.
    let a = cast[seq[byte]]("first frame contents AAAAAAAAAA")
    let b = cast[seq[byte]]("second frame contents BBBBBBBBBB")
    var concatenated = lz4mod.lz4Compress(a)
    concatenated.add(lz4mod.lz4Compress(b))
    check lz4mod.lz4Decompress(concatenated) == a & b

  test "lz4Decompress of empty input is empty":
    check lz4mod.lz4Decompress(@[]) == newSeq[byte](0)

  test "lz4 missing dynlib symbol is raised as KafkaError":
    # liblz4 itself is present in CI; a symbol it does not export still
    # produces a nil address that the production wrapper normalizes to
    # LibraryError. The outer wrap turns that into KafkaError.
    var raised = false
    var msg = ""
    try:
      lz4mod.lz4CallMissingSymbol()
    except KafkaError as e:
      raised = true
      msg = e.msg
    check raised
    check "lz4 native library error" in msg
    check "could not find symbol" in msg

  test "ensureCodecAvailable passes the linked codecs and lz4 on this host":
    # gzip / snappy / zstd are linked into the binary, so the probe is a no-op
    # for them; ccLz4 is present here because the suite above compresses with it.
    for codec in [ccNone, ccGzip, ccSnappy, ccZstd]:
      ensureCodecAvailable(codec)
    ensureCodecAvailable(ccLz4)

  test "ensureCodecAvailable reports a host without liblz4 as KafkaError":
    # The probe runs at producer construction, so this is the failure a user
    # sees there instead of a failed first send.
    lz4mod.lz4BlockLoadForTest(true)
    defer:
      lz4mod.lz4BlockLoadForTest(false)
    var raised = false
    var msg = ""
    try:
      ensureCodecAvailable(ccLz4)
    except KafkaError as e:
      raised = true
      msg = e.msg
    check raised
    check "ccLz4 requires the system liblz4" in msg
    # The injection covers the lz4 load only; an unrelated codec still passes.
    ensureCodecAvailable(ccZstd)

  test "lz4 load resumes after a blocked probe":
    lz4mod.lz4BlockLoadForTest(true)
    var blocked = false
    try:
      lz4mod.ensureLz4Loaded()
    except KafkaError:
      blocked = true
    check blocked
    lz4mod.lz4BlockLoadForTest(false)
    # A failed probe must not poison the cached handle: the next call loads.
    ensureCodecAvailable(ccLz4)
    check lz4mod.lz4Decompress(lz4mod.lz4Compress(@[1'u8, 2, 3])) == @[1'u8, 2, 3]

suite "toIsolationLevel":
  test "valid wire values map to enum":
    check toIsolationLevel(0'i8) == ilReadUncommitted
    check toIsolationLevel(1'i8) == ilReadCommitted

  test "out-of-range wire value raises KafkaProtocolError":
    var raised = false
    try:
      discard toIsolationLevel(2'i8)
    except KafkaProtocolError:
      raised = true
    check raised

  test "negative wire value raises KafkaProtocolError":
    var raised = false
    try:
      discard toIsolationLevel(-1'i8)
    except KafkaProtocolError:
      raised = true
    check raised

suite "TopicPartition hash":
  test "equal TopicPartitions hash equal":
    let a = TopicPartition(topic: "orders", partition: 3)
    let b = TopicPartition(topic: "orders", partition: 3)
    check hash(a) == hash(b)

  test "hash is deterministic across calls":
    let tp = TopicPartition(topic: "orders", partition: 3)
    check hash(tp) == hash(tp)

  test "different partitions hash differently":
    let a = TopicPartition(topic: "orders", partition: 0)
    let b = TopicPartition(topic: "orders", partition: 1)
    check hash(a) != hash(b)

  test "different topics hash differently":
    let a = TopicPartition(topic: "orders", partition: 0)
    let b = TopicPartition(topic: "payments", partition: 0)
    check hash(a) != hash(b)

  test "usable as a Table key":
    var t: Table[TopicPartition, int]
    t[TopicPartition(topic: "orders", partition: 0)] = 10
    t[TopicPartition(topic: "orders", partition: 1)] = 20
    check t[TopicPartition(topic: "orders", partition: 0)] == 10
    check t[TopicPartition(topic: "orders", partition: 1)] == 20
    check t.len == 2
