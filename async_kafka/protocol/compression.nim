import pkg/results
import pkg/zlib/gzip
import pkg/zlib/zlib_api
import pkg/supersnappy

import ../errors
import lz4, zstd

type CompressionCodec* = enum
  ## Record-batch compression codec. Every codec is linked into the binary.
  ccNone = 0
  ccGzip = 1
  ccSnappy = 2
  ccLz4 = 3
  ccZstd = 4

const
  defaultMaxDecompressSize* = lz4.defaultMaxDecompressSize
    ## Ceiling for one RecordBatch's decompressed records section. The generated
    ## RecordBatch decoder reads it so a response-wide budget can narrow the cap it
    ## hands to the codec.

  xerialHeader = [
    0x82'u8,
    0x53,
    0x4E,
    0x41,
    0x50,
    0x50,
    0x59,
    0x00, # magic: \x82SNAPPY\x00
    0x00,
    0x00,
    0x00,
    0x01, # version: 1
    0x00,
    0x00,
    0x00,
    0x01, # compat: 1
  ]
  xerialHeaderLen = 16
  snappyBlockSize = 32 * 1024

proc compressionFromAttributes*(
    attributes: int16
): CompressionCodec {.raises: [KafkaProtocolError].} =
  ## Extract the codec from a RecordBatch `attributes` field (low 3 bits). A
  ## value of 5..7 is unspecified by the wire format and would be an out-of-range
  ## enum conversion, so it fails as a protocol error instead.
  let v = attributes and 0x07
  if v > int16(ord(ccZstd)):
    raise newException(KafkaProtocolError, "unsupported compression codec id: " & $v)
  CompressionCodec(v)

proc readBE32(data: openArray[byte], offset: int): int32 =
  (int32(data[offset]) shl 24) or (int32(data[offset + 1]) shl 16) or
    (int32(data[offset + 2]) shl 8) or int32(data[offset + 3])

proc readLE32(data: openArray[byte], offset: int): uint32 =
  uint32(data[offset]) or (uint32(data[offset + 1]) shl 8) or
    (uint32(data[offset + 2]) shl 16) or (uint32(data[offset + 3]) shl 24)

proc writeBE32(buf: var seq[byte], value: int32) =
  buf.add(byte((value shr 24) and 0xFF))
  buf.add(byte((value shr 16) and 0xFF))
  buf.add(byte((value shr 8) and 0xFF))
  buf.add(byte(value and 0xFF))

proc snappyCompress(data: openArray[byte]): seq[byte] {.raises: [KafkaError].} =
  ## Compress data using Kafka's xerial snappy framing format.
  result = @xerialHeader
  var offset = 0
  while offset < data.len:
    let chunkLen = min(snappyBlockSize, data.len - offset)
    try:
      let chunk = @(data.toOpenArray(offset, offset + chunkLen - 1))
      let compressed = compress(chunk)
      result.writeBE32(int32(compressed.len))
      result.add(compressed)
    except SnappyError as e:
      raise newException(KafkaError, "snappy compression failed: " & e.msg)
    offset += chunkLen

proc rawSnappyUncompressedLen(data: openArray[byte]): uint64 {.raises: [KafkaError].} =
  ## Decode the varint prefixing a raw snappy stream with its uncompressed
  ## length, so an oversized claim is rejected before `uncompress` allocates.
  var shift = 0
  var i = 0
  while true:
    if i >= data.len:
      raise newException(KafkaError, "snappy: truncated uncompressed-length prefix")
    let b = data[i]
    inc i
    # Unsigned `shl` wraps, so on the 63rd shift only the low bit fits a uint64;
    # this mirrors the uvarint final-byte rule.
    if shift == 63 and (b and 0xFE'u8) != 0:
      raise newException(KafkaError, "snappy: uncompressed-length prefix overflow")
    result = result or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      break
    shift += 7
    if shift >= 64:
      raise newException(KafkaError, "snappy: uncompressed-length prefix overflow")

const snappyReadSlack = 4
  ## Zero padding past the logical end of the buffer handed to the dependency's
  ## `uncompress`. Its COPY trailer read is a raw `copyMem` guarded only by
  ## `ip + 1 < srcLen`, so a tag ending in the last 3 bytes of a stream
  ## would read past it. The slack keeps those reads inside the allocation and is
  ## never walked as tags, since the logical length is unchanged.

proc rawSnappyUncompress(
    data: openArray[byte], maxSize: int
): seq[byte] {.raises: [KafkaError].} =
  ## Decompress a raw (non-xerial-framed) snappy stream, rejecting a self-declared
  ## length above `maxSize` before the decompressor allocates. The post-decompress
  ## check stays as defence-in-depth in case the prefix understates the output.
  if data.len == 0:
    return @[]
  let claimed = rawSnappyUncompressedLen(data)
  if claimed > uint64(maxSize):
    raise newException(
      KafkaError,
      "snappy raw uncompressed length " & $claimed & " exceeds size limit " & $maxSize,
    )
  try:
    # Copy into a buffer with trailing slack (see snappyReadSlack); newSeq
    # zero-fills it, keeping hostile-stream behaviour deterministic.
    var dataCopy = newSeq[byte](data.len + snappyReadSlack)
    dataCopy.setLen(data.len)
    copyMem(addr dataCopy[0], unsafeAddr data[0], data.len)
    result = uncompress(dataCopy)
  except SnappyError as e:
    raise newException(KafkaError, "snappy decompression failed: " & e.msg)
  except Defect as e:
    # A hostile stream can make the dependency raise a Defect instead of a
    # catchable SnappyError; contain it so a crafted payload surfaces as a
    # KafkaError, not a crash. (Under --panics:on a Defect aborts before it can be
    # caught — a Nim-level limit outside this layer.)
    raise newException(KafkaError, "snappy decompression failed: " & e.msg)
  if result.len > maxSize:
    raise newException(KafkaError, "snappy decompression exceeded size limit")

proc snappyDecompress(
    data: openArray[byte], maxSize: int
): seq[byte] {.raises: [KafkaError].} =
  ## Decompress data using Kafka's xerial snappy framing format.
  if data.len < xerialHeaderLen:
    return rawSnappyUncompress(data, maxSize)

  var hasXerialHeader = true
  for i in 0 ..< xerialHeaderLen:
    if data[i] != xerialHeader[i]:
      hasXerialHeader = false
      break

  if not hasXerialHeader:
    return rawSnappyUncompress(data, maxSize)

  # Xerial framed snappy. `uncompress` allocates for a block's self-declared
  # length before validating the body, so a tiny block can claim a multi-GB
  # output: decode the prefix and reject it against the remaining budget before
  # calling it. The post-decompress check is defence-in-depth on the logical
  # length; `result.add` reallocation can lift the peak above 2*maxSize.
  result = newSeq[byte]()
  var offset = xerialHeaderLen
  var blk = newSeq[byte](snappyBlockSize + snappyReadSlack)
  while offset < data.len:
    if offset + 4 > data.len:
      raise newException(KafkaError, "snappy: truncated block length")
    let blockLen = int(readBE32(data, offset))
    offset += 4
    if blockLen <= 0 or offset + blockLen > data.len:
      raise newException(KafkaError, "snappy: invalid block length")
    try:
      # The buffer is reused, so grow it with read slack (see snappyReadSlack)
      # whenever a block needs more.
      if blockLen > blk.capacity - snappyReadSlack:
        blk = newSeq[byte](blockLen + snappyReadSlack)
      blk.setLen(blockLen)
      # Re-zero the slack so over-reading tag reads stay deterministic.
      zeroMem(cast[pointer](cast[uint](addr blk[0]) + uint(blockLen)), snappyReadSlack)
      copyMem(addr blk[0], unsafeAddr data[offset], blockLen)
      let claimed = rawSnappyUncompressedLen(blk)
      if claimed > uint64(maxSize - result.len):
        raise newException(
          KafkaError,
          "snappy: block uncompressed length " & $claimed &
            " exceeds remaining decompression budget",
        )
      let decompressed = uncompress(blk)
      if decompressed.len > maxSize - result.len:
        raise newException(KafkaError, "snappy decompression exceeded size limit")
      result.add(decompressed)
    except SnappyError as e:
      raise newException(KafkaError, "snappy decompression failed: " & e.msg)
    except Defect as e:
      # See rawSnappyUncompress: a malformed body can still raise a Defect.
      raise newException(KafkaError, "snappy decompression failed: " & e.msg)
    offset += blockLen

proc gzipDecompress(
    data: openArray[byte], maxSize: int
): seq[byte] {.raises: [KafkaError].} =
  ## Stream the deflate payload of a gzip frame through zlib's raw inflate path,
  ## with `inflateEnd` on every exit path (unlike the dependency's `ungzip`), so a
  ## hostile stream of failing frames cannot leak the ~32KB inflate state.
  ##
  ## The last 8 bytes are the RFC 1952 trailer (CRC-32 + ISIZE, little-endian),
  ## not fed to inflate. `Z_STREAM_END` is required so a truncated payload cannot
  ## be returned as a prefix, and the trailer is checked with zlib's ISO 3309
  ## CRC-32 (what the compressor writes, not Kafka's CRC32C).
  if data.len < 18:
    raise newException(KafkaError, "gzip data too short: " & $data.len & " bytes")
  var mz = ZStream(
    next_in: cast[ptr uint8](unsafeAddr data[10]), avail_in: (data.len - 18).cuint
  )
  if mz.inflateInit2(Z_RAW_DEFLATE) != Z_OK:
    raise newException(KafkaError, "gzip decompression failed: inflate init")
  defer:
    discard mz.inflateEnd()
  result = newSeq[byte]()
  var buf: array[0xFFFF, byte]
  while true:
    mz.next_out = cast[ptr uint8](addr buf[0])
    mz.avail_out = buf.len.cuint
    let r = mz.inflate(Z_SYNC_FLUSH)
    let outSize = buf.len - mz.avail_out.int
    if outSize > 0:
      result.add toOpenArray(buf, 0, outSize - 1)
    if result.len > maxSize:
      raise newException(KafkaError, "gzip decompression exceeded size limit")
    if r == Z_STREAM_END:
      break
    elif r == Z_OK:
      if mz.avail_in > 0 or mz.avail_out == 0:
        continue
      else:
        raise newException(KafkaError, "gzip decompression failed: truncated")
    else:
      raise newException(KafkaError, "gzip decompression failed: " & $r)
  let storedCrc = readLE32(data, data.len - 8)
  let storedIsize = readLE32(data, data.len - 4)
  if uint32(crc32(result)) != storedCrc:
    raise newException(KafkaError, "gzip CRC mismatch")
  if uint32(result.len) != storedIsize:
    raise newException(KafkaError, "gzip ISIZE mismatch")

proc compressRecords*(
    data: openArray[byte], codec: CompressionCodec
): seq[byte] {.raises: [KafkaError].} =
  case codec
  of ccNone:
    return @data
  of ccGzip:
    let res = gzip(seq[byte], data)
    if res.isErr:
      raise newException(KafkaError, "gzip compression failed: " & res.error)
    return res.get
  of ccSnappy:
    return snappyCompress(data)
  of ccLz4:
    return lz4Compress(data)
  of ccZstd:
    return zstdCompress(data)

proc decompressRecords*(
    data: openArray[byte],
    codec: CompressionCodec,
    maxDecompressSize: int = defaultMaxDecompressSize,
): seq[byte] {.raises: [KafkaError].} =
  case codec
  of ccNone:
    return @data
  of ccGzip:
    # 10-byte header + deflate payload + 8-byte trailer; reject anything shorter
    # before the decompressor indexes into it.
    if data.len < 18:
      raise newException(KafkaError, "gzip data too short: " & $data.len & " bytes")
    return gzipDecompress(data, maxDecompressSize)
  of ccSnappy:
    return snappyDecompress(data, maxDecompressSize)
  of ccLz4:
    return lz4Decompress(data, maxDecompressSize)
  of ccZstd:
    return zstdDecompress(data, maxDecompressSize)
