import std/options

import pkg/stew/endians2

import ../errors

type
  DecompressionBudget* = ref object
    ## Shared counter of decompressed-record bytes one fetch response may still
    ## produce; one budget spans every partition, so a malicious broker cannot
    ## bypass the total cap by spreading high-ratio batches. RecordBatch-v2 raises
    ## KafkaDecompressionLimitError past `remaining` and decrements it by each
    ## batch's decompressed length.
    remaining*: int

  KafkaWriter* = object
    buf*: seq[byte]

  KafkaReader* = object
    data*: seq[byte]
    pos*: int
    limit*: int
      ## Optional soft end-of-window, exclusive; 0 (default) means reads are
      ## bounded only by `data.len`, else by `min(data.len, limit)` for `remaining`
      ## and every guarded read. The RecordBatch-v2 record loop narrows it to each
      ## record so a hostile key/value length cannot allocate beyond that window.
    decompressionBudget*: DecompressionBudget
      ## Optional response-scoped budget attached by the fetch parser; nil keeps
      ## the per-batch cap only. See `DecompressionBudget` for the accounting.

const
  maxWireBytesLen* = 104_857_600 ## 100MB, matches connection frame limit
  maxWireArrayLen* = 10_000_000

proc initKafkaWriter*(initialCap: int = 256): KafkaWriter =
  result.buf = newSeqOfCap[byte](initialCap)

proc writeInt8*(w: var KafkaWriter, v: int8) =
  w.buf.add(cast[byte](v))

proc writeInt16*(w: var KafkaWriter, v: int16) =
  let be = toBytesBE(cast[uint16](v))
  let pos = w.buf.len
  w.buf.setLen(pos + 2)
  copyMem(addr w.buf[pos], unsafeAddr be[0], 2)

proc writeInt32*(w: var KafkaWriter, v: int32) =
  let be = toBytesBE(cast[uint32](v))
  let pos = w.buf.len
  w.buf.setLen(pos + 4)
  copyMem(addr w.buf[pos], unsafeAddr be[0], 4)

proc writeInt64*(w: var KafkaWriter, v: int64) =
  let be = toBytesBE(cast[uint64](v))
  let pos = w.buf.len
  w.buf.setLen(pos + 8)
  copyMem(addr w.buf[pos], unsafeAddr be[0], 8)

proc writeBool*(w: var KafkaWriter, v: bool) =
  w.writeInt8(if v: 1'i8 else: 0'i8)

proc writeString*(w: var KafkaWriter, v: string) =
  if v.len > int16.high.int:
    raise newException(
      KafkaProtocolError, "string length " & $v.len & " exceeds maximum of 32767"
    )
  w.writeInt16(int16(v.len))
  if v.len > 0:
    let pos = w.buf.len
    w.buf.setLen(pos + v.len)
    copyMem(addr w.buf[pos], unsafeAddr v[0], v.len)

proc writeNullableString*(w: var KafkaWriter, v: Option[string]) =
  if v.isNone:
    w.writeInt16(-1'i16)
  else:
    w.writeString(v.get)

proc writeBytes*(w: var KafkaWriter, v: seq[byte]) =
  if v.len > int32.high.int:
    raise newException(
      KafkaProtocolError, "bytes length " & $v.len & " exceeds int32 maximum"
    )
  w.writeInt32(int32(v.len))
  w.buf.add(v)

proc writeNullableBytes*(w: var KafkaWriter, v: Option[seq[byte]]) =
  if v.isNone:
    w.writeInt32(-1'i32)
  else:
    w.writeBytes(v.get)

proc writeArrayLen*(w: var KafkaWriter, n: int32) =
  w.writeInt32(n)

proc encodeVarint*(w: var KafkaWriter, v: int64) =
  var zigzag = cast[uint64]((v shl 1) xor (v shr 63))
  while zigzag > 0x7F'u64:
    w.buf.add(byte((zigzag and 0x7F) or 0x80))
    zigzag = zigzag shr 7
  w.buf.add(byte(zigzag))

proc encodeUvarint*(w: var KafkaWriter, v: uint64) =
  var val = v
  while val > 0x7F'u64:
    w.buf.add(byte((val and 0x7F) or 0x80))
    val = val shr 7
  w.buf.add(byte(val))

proc writeCompactString*(w: var KafkaWriter, v: string) =
  ## COMPACT_STRING (KIP-482): unsigned varint length+1, then the bytes.
  ## Use this for non-nullable string fields in flexible message versions.
  if v.len > maxWireBytesLen:
    raise newException(
      KafkaProtocolError,
      "compact string length " & $v.len & " exceeds maximum " & $maxWireBytesLen,
    )
  w.encodeUvarint(uint64(v.len + 1))
  if v.len > 0:
    let pos = w.buf.len
    w.buf.setLen(pos + v.len)
    copyMem(addr w.buf[pos], unsafeAddr v[0], v.len)

proc writeCompactNullableString*(w: var KafkaWriter, v: Option[string]) =
  ## COMPACT_NULLABLE_STRING: unsigned varint length+1, or 0 for null.
  if v.isNone:
    w.encodeUvarint(0)
  else:
    w.writeCompactString(v.get)

proc writeCompactBytes*(w: var KafkaWriter, v: seq[byte]) =
  ## COMPACT_BYTES (KIP-482): unsigned varint length+1, then the bytes.
  if v.len > maxWireBytesLen:
    raise newException(
      KafkaProtocolError,
      "compact bytes length " & $v.len & " exceeds maximum " & $maxWireBytesLen,
    )
  w.encodeUvarint(uint64(v.len + 1))
  if v.len > 0:
    w.buf.add(v)

proc writeCompactNullableBytes*(w: var KafkaWriter, v: Option[seq[byte]]) =
  ## COMPACT_NULLABLE_BYTES: unsigned varint length+1, or 0 for null.
  if v.isNone:
    w.encodeUvarint(0)
  else:
    w.writeCompactBytes(v.get)

proc writeCompactArrayLen*(w: var KafkaWriter, n: int) =
  ## COMPACT_ARRAY length prefix: unsigned varint count+1. Pass -1 for null
  ## arrays — those become 0 on the wire (the same encoding as length+1=0).
  if n == -1:
    w.encodeUvarint(0)
  elif n < 0:
    raise newException(
      KafkaProtocolError,
      "compact array length " & $n & " is negative; only -1 (null) is allowed",
    )
  else:
    w.encodeUvarint(uint64(n + 1))

proc writeUuid*(w: var KafkaWriter, v: array[16, byte]) =
  ## KIP-516 UUID: 16 bytes, big-endian, no length prefix. Same layout in legacy
  ## and flexible (KIP-482) versions — no compact form since the length is fixed.
  let pos = w.buf.len
  w.buf.setLen(pos + 16)
  copyMem(addr w.buf[pos], unsafeAddr v[0], 16)

proc writeTaggedFieldsEmpty*(w: var KafkaWriter) =
  ## Writes an empty tagged-fields trailer (count = 0). Every flexible request and
  ## response struct ends with at least an empty tag buffer.
  w.encodeUvarint(0)

proc encodeVarintTo*(dst: var openArray[byte], v: int64): int =
  ## Encode a varint into a fixed-size buffer. Returns the number of bytes written.
  ## The caller must ensure dst is at least 10 bytes for arbitrary int64 values.
  var zigzag = cast[uint64]((v shl 1) xor (v shr 63))
  result = 0
  while zigzag > 0x7F'u64:
    if result >= dst.len:
      raise newException(KafkaProtocolError, "varint buffer overflow")
    dst[result] = byte((zigzag and 0x7F) or 0x80)
    zigzag = zigzag shr 7
    inc result
  if result >= dst.len:
    raise newException(KafkaProtocolError, "varint buffer overflow")
  dst[result] = byte(zigzag)
  inc result

proc encodeVarBytes*(w: var KafkaWriter, v: Option[seq[byte]]) =
  if v.isNone:
    w.encodeVarint(-1)
  else:
    let data = v.get
    # Mirror decodeVarBytes: refuse to emit a frame the peer would reject with
    # KafkaProtocolError anyway; the bug would otherwise look like a broker disconnect.
    if data.len > maxWireBytesLen:
      raise newException(
        KafkaProtocolError,
        "varbytes length " & $data.len & " exceeds maximum " & $maxWireBytesLen,
      )
    w.encodeVarint(int64(data.len))
    w.buf.add(data)

proc encodeVarString*(w: var KafkaWriter, v: string) =
  if v.len > maxWireBytesLen:
    raise newException(
      KafkaProtocolError,
      "varstring length " & $v.len & " exceeds maximum " & $maxWireBytesLen,
    )
  w.encodeVarint(int64(v.len))
  for c in v:
    w.buf.add(byte(c))

proc len*(w: KafkaWriter): int =
  w.buf.len

# KafkaReader

proc initKafkaReader*(data: sink seq[byte]): KafkaReader =
  ## Initialise a reader from a fresh `seq[byte]`. `sink` lets Nim move the buffer
  ## in instead of deep-copying it, eliding a copy of MB-scale fetch responses.
  result.data = data
  result.pos = 0

proc initKafkaReader*(data: openArray[byte]): KafkaReader =
  ## Initialise a reader from a non-owning view: copies into a fresh `seq[byte]`
  ## so the reader outlives `data`. Use the seq overload to hand over ownership.
  result.data = @data
  result.pos = 0

proc remaining*(r: KafkaReader): int =
  (if r.limit <= 0: r.data.len else: min(r.data.len, r.limit)) - r.pos

proc checkRemaining*(r: KafkaReader, n: int) =
  if r.remaining < n:
    raise newException(
      KafkaProtocolError,
      "buffer underflow: need " & $n & " bytes, have " & $r.remaining,
    )

proc readInt8*(r: var KafkaReader): int8 =
  r.checkRemaining(1)
  result = cast[int8](r.data[r.pos])
  r.pos += 1

proc readInt16*(r: var KafkaReader): int16 =
  r.checkRemaining(2)
  var buf: array[2, byte]
  buf[0] = r.data[r.pos]
  buf[1] = r.data[r.pos + 1]
  result = cast[int16](fromBytesBE(uint16, buf))
  r.pos += 2

proc readInt32*(r: var KafkaReader): int32 =
  r.checkRemaining(4)
  var buf: array[4, byte]
  for i in 0 ..< 4:
    buf[i] = r.data[r.pos + i]
  result = cast[int32](fromBytesBE(uint32, buf))
  r.pos += 4

proc readInt64*(r: var KafkaReader): int64 =
  r.checkRemaining(8)
  var buf: array[8, byte]
  for i in 0 ..< 8:
    buf[i] = r.data[r.pos + i]
  result = cast[int64](fromBytesBE(uint64, buf))
  r.pos += 8

proc peekInt32At*(r: KafkaReader, offset: int): int32 =
  ## Read a big-endian int32 at an absolute offset WITHOUT advancing the cursor,
  ## used to inspect a length prefix (e.g. a RecordBatch's batchLength) before
  ## committing to decode, so a truncated trailing element is left for the next
  ## fetch. Raises KafkaProtocolError if the four bytes are absent; honours `limit`.
  let effEnd =
    if r.limit <= 0:
      r.data.len
    else:
      min(r.data.len, r.limit)
  if offset < 0 or offset > effEnd - 4:
    raise
      newException(KafkaProtocolError, "peekInt32At out of bounds at offset " & $offset)
  var buf: array[4, byte]
  for i in 0 ..< 4:
    buf[i] = r.data[offset + i]
  cast[int32](fromBytesBE(uint32, buf))

proc readBool*(r: var KafkaReader): bool =
  r.readInt8() != 0

proc readString*(r: var KafkaReader): string =
  let length = r.readInt16()
  if length < 0:
    raise newException(KafkaProtocolError, "unexpected null string")
  if length == 0:
    return ""
  r.checkRemaining(int(length))
  result = newString(length)
  copyMem(addr result[0], addr r.data[r.pos], int(length))
  r.pos += int(length)

proc readNullableString*(r: var KafkaReader): Option[string] =
  let length = r.readInt16()
  if length < 0:
    return none(string)
  if length == 0:
    return some("")
  r.checkRemaining(int(length))
  var s = newString(length)
  copyMem(addr s[0], addr r.data[r.pos], int(length))
  r.pos += int(length)
  result = some(s)

proc readBytes*(r: var KafkaReader, length: int): seq[byte] =
  # A negative length slips past checkRemaining (remaining < negative is false) and
  # yields a malformed slice; reject it here so every caller is safe.
  if length < 0:
    raise newException(KafkaProtocolError, "negative read length: " & $length)
  r.checkRemaining(length)
  result = r.data[r.pos ..< r.pos + length]
  r.pos += length

proc readNullableBytes*(r: var KafkaReader): Option[seq[byte]] =
  let length = r.readInt32()
  if length < 0:
    return none(seq[byte])
  if length == 0:
    return some(newSeq[byte]())
  if length > int32(maxWireBytesLen):
    raise newException(
      KafkaProtocolError,
      "wire bytes length " & $length & " exceeds maximum " & $maxWireBytesLen,
    )
  r.checkRemaining(int(length))
  result = some(r.data[r.pos ..< r.pos + int(length)])
  r.pos += int(length)

proc readLengthPrefixedBytes*(r: var KafkaReader): seq[byte] =
  ## Reads a Kafka non-nullable BYTES field: int32 length prefix then that many
  ## bytes. Raises if the length is negative (a NULLABLE_BYTES null in a
  ## non-nullable slot) or exceeds the 100MB cap `readNullableBytes` uses.
  let length = r.readInt32()
  if length < 0:
    raise newException(
      KafkaProtocolError, "non-nullable BYTES field has negative length: " & $length
    )
  if length > int32(maxWireBytesLen):
    raise newException(
      KafkaProtocolError,
      "wire bytes length " & $length & " exceeds maximum " & $maxWireBytesLen,
    )
  r.readBytes(int(length))

proc checkArrayLenFitsBuffer*(
    r: KafkaReader, len: int32, minWirePerElem: int
) {.inline, raises: [KafkaProtocolError].} =
  ## Reject an element count the remaining bytes cannot possibly hold: the invariant
  ## is `len * minWirePerElem <= remaining`, where `minWirePerElem` is the element's
  ## compile-time minimum wire size from the schema generator. A negative `len`, the
  ## null sentinel, is a no-op.
  ##
  ## Passing 1 keeps the weaker historical form, enough for a scalar element. A
  ## struct element needs its real minimum: `MetadataResponse`'s `topics` ->
  ## `partitions` -> `replicaNodes` nests three deep, and one `FetchPartitionResp`
  ## is 88 bytes in memory, and the guard's compile-time minimum for it is 24 bytes.
  ## At 1 byte/element a 10MB frame
  ## could demand `maxWireArrayLen` x 88 bytes, which `newSeq[Elem](len)` allocated
  ## before the first element was read — an uncatchable OutOfMemDefect. The generated
  ## decode now grows the sequence as it fills, so memory tracks what was decoded.
  if int(len) > r.remaining div max(1, minWirePerElem):
    raise newException(
      KafkaProtocolError,
      "array length " & $len & " exceeds the " & $r.remaining & " remaining bytes at " &
        $minWirePerElem & " bytes per element",
    )

proc readArrayLen*(r: var KafkaReader): int32 =
  result = r.readInt32()
  if result < -1 or result > int32(maxWireArrayLen):
    raise newException(KafkaProtocolError, "invalid array length: " & $result)
  if result < 0:
    result = 0

proc readNullableArrayLen*(r: var KafkaReader): int32 =
  ## Returns raw array length including -1 for null arrays.
  ## Unlike readArrayLen, does NOT convert -1 to 0.
  result = r.readInt32()
  if result < -1 or result > int32(maxWireArrayLen):
    raise newException(KafkaProtocolError, "invalid array length: " & $result)

proc skip*(r: var KafkaReader, n: int) =
  ## Skip `n` bytes. A negative `n` would rewind the cursor, so reject it.
  if n < 0:
    raise newException(KafkaProtocolError, "negative skip length: " & $n)
  r.checkRemaining(n)
  r.pos += n

proc decodeUvarint*(r: var KafkaReader): uint64 =
  ## Decodes an unsigned varint. A uint64 spans at most 10 bytes (shift 0..63) and
  ## the 10th carries only one extra bit; anything larger is overflow, not truncation.
  var shift = 0
  var bytesRead = 0
  while true:
    r.checkRemaining(1)
    let b = r.data[r.pos]
    r.pos += 1
    inc bytesRead
    if shift == 63 and (b and 0xFE'u8) != 0:
      # On the 10th byte only the lowest bit fits a uint64; any other bit would
      # overflow (and the continuation bit must be clear).
      raise newException(KafkaProtocolError, "uvarint overflow on final byte")
    result = result or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      break
    shift += 7
    if shift >= 64 or bytesRead > 10:
      raise newException(KafkaProtocolError, "uvarint overflow")

proc decodeVarint*(r: var KafkaReader): int64 =
  ## Decode a signed (zigzag) varint used in Kafka record batches. Same strictness
  ## as `decodeUvarint` (at most 10 bytes, only the low bit on the 10th); without it
  ## a crafted batch (`0xFF*9 0xFF`) could wrap to a bad int64.
  var shift = 0
  var raw: uint64 = 0
  var bytesRead = 0
  while true:
    r.checkRemaining(1)
    let b = r.data[r.pos]
    r.pos += 1
    bytesRead.inc
    if shift == 63 and (b and 0xFE'u8) != 0:
      raise newException(KafkaProtocolError, "varint overflow on final byte")
    raw = raw or (uint64(b and 0x7F) shl shift)
    if (b and 0x80) == 0:
      break
    shift += 7
    if shift >= 64 or bytesRead > 10:
      raise newException(KafkaProtocolError, "varint overflow")
  # Zigzag decode: (raw >>> 1) ^ -(raw & 1)
  let sign = 0'u64 - (raw and 1'u64)
  result = cast[int64]((raw shr 1) xor sign)

proc decodeVarBytes*(r: var KafkaReader, maxLen: int = -1): Option[seq[byte]] =
  ## VARINT-length-prefixed bytes (Record key/value); a negative length is null and
  ## returns `none`. Two independent bounds reject a hostile length before the
  ## payload is allocated: `maxWireBytesLen`, the absolute cap every length-prefixed
  ## reader enforces regardless of `limit`, and the reader's `limit`, which
  ## `loadRecords` pins to `recordEnd` around each Record decode and
  ## `readBytes`/`checkRemaining` honour, so it caps the allocation too. `maxLen >= 0`
  ## is an optional tighter window used by tests; production decoding leaves it -1.
  let length = r.decodeVarint()
  if length < 0:
    return none(seq[byte])
  if maxLen >= 0 and length > int64(maxLen):
    raise newException(
      KafkaProtocolError, "varbytes length " & $length & " exceeds window " & $maxLen
    )
  if length > int64(maxWireBytesLen):
    raise newException(
      KafkaProtocolError,
      "varbytes length " & $length & " exceeds maximum " & $maxWireBytesLen,
    )
  if length == 0:
    return some(newSeq[byte]())
  result = some(r.readBytes(int(length)))

proc decodeVarString*(r: var KafkaReader, maxLen: int = -1): string =
  ## See `decodeVarBytes` for the `maxWireBytesLen` cap, `maxLen` window, and
  ## reader-`limit` window that bound the read. `fkVarString` is non-nullable (its
  ## only use is RecordHeader.key), so a negative length is malformed, not null.
  let length = r.decodeVarint()
  if length < 0:
    raise newException(
      KafkaProtocolError,
      "negative varstring length " & $length & " (non-nullable field)",
    )
  if maxLen >= 0 and length > int64(maxLen):
    raise newException(
      KafkaProtocolError, "varstring length " & $length & " exceeds window " & $maxLen
    )
  if length > int64(maxWireBytesLen):
    raise newException(
      KafkaProtocolError,
      "varstring length " & $length & " exceeds maximum " & $maxWireBytesLen,
    )
  if length == 0:
    return ""
  r.checkRemaining(int(length))
  result = newString(length)
  copyMem(addr result[0], addr r.data[r.pos], int(length))
  r.pos += int(length)

proc readCompactString*(r: var KafkaReader): string =
  ## COMPACT_STRING: unsigned varint length+1, then the bytes. Raises on null
  ## (length+1 == 0) since the type is non-nullable.
  let lenPlusOne = r.decodeUvarint()
  if lenPlusOne == 0:
    raise newException(KafkaProtocolError, "unexpected null COMPACT_STRING")
  # Bound-check in uint64 BEFORE narrowing to int: a length near uint64.high would
  # otherwise wrap to a small/negative int and bypass the limit check.
  if lenPlusOne - 1 > uint64(maxWireBytesLen):
    raise newException(
      KafkaProtocolError,
      "COMPACT_STRING length " & $(lenPlusOne - 1) & " exceeds maximum " &
        $maxWireBytesLen,
    )
  let length = int(lenPlusOne - 1)
  if length == 0:
    return ""
  r.checkRemaining(length)
  result = newString(length)
  copyMem(addr result[0], addr r.data[r.pos], length)
  r.pos += length

proc readCompactNullableString*(r: var KafkaReader): Option[string] =
  let lenPlusOne = r.decodeUvarint()
  if lenPlusOne == 0:
    return none(string)
  if lenPlusOne - 1 > uint64(maxWireBytesLen):
    raise newException(
      KafkaProtocolError,
      "COMPACT_NULLABLE_STRING length " & $(lenPlusOne - 1) & " exceeds maximum " &
        $maxWireBytesLen,
    )
  let length = int(lenPlusOne - 1)
  if length == 0:
    return some("")
  r.checkRemaining(length)
  var s = newString(length)
  copyMem(addr s[0], addr r.data[r.pos], length)
  r.pos += length
  result = some(s)

proc readCompactBytes*(r: var KafkaReader): seq[byte] =
  ## COMPACT_BYTES: unsigned varint length+1, then the bytes.
  let lenPlusOne = r.decodeUvarint()
  if lenPlusOne == 0:
    raise newException(KafkaProtocolError, "unexpected null COMPACT_BYTES")
  if lenPlusOne - 1 > uint64(maxWireBytesLen):
    raise newException(
      KafkaProtocolError,
      "COMPACT_BYTES length " & $(lenPlusOne - 1) & " exceeds maximum " &
        $maxWireBytesLen,
    )
  let length = int(lenPlusOne - 1)
  if length == 0:
    return @[]
  r.checkRemaining(length)
  result = r.data[r.pos ..< r.pos + length]
  r.pos += length

proc readCompactNullableBytes*(r: var KafkaReader): Option[seq[byte]] =
  let lenPlusOne = r.decodeUvarint()
  if lenPlusOne == 0:
    return none(seq[byte])
  if lenPlusOne - 1 > uint64(maxWireBytesLen):
    raise newException(
      KafkaProtocolError,
      "COMPACT_NULLABLE_BYTES length " & $(lenPlusOne - 1) & " exceeds maximum " &
        $maxWireBytesLen,
    )
  let length = int(lenPlusOne - 1)
  if length == 0:
    return some(newSeq[byte]())
  r.checkRemaining(length)
  result = some(r.data[r.pos ..< r.pos + length])
  r.pos += length

proc readCompactArrayLen*(r: var KafkaReader): int32 =
  ## COMPACT_ARRAY length prefix. Returns -1 for null arrays (length+1 == 0)
  ## and the unwrapped element count otherwise.
  let lenPlusOne = r.decodeUvarint()
  if lenPlusOne == 0:
    return -1
  if lenPlusOne - 1 > uint64(maxWireArrayLen):
    raise newException(
      KafkaProtocolError,
      "COMPACT_ARRAY length " & $(lenPlusOne - 1) & " exceeds maximum " &
        $maxWireArrayLen,
    )
  result = int32(lenPlusOne - 1)

proc readUuid*(r: var KafkaReader): array[16, byte] =
  ## KIP-516 UUID: 16 bytes, big-endian, no length prefix. Same layout in legacy
  ## and flexible message versions.
  r.checkRemaining(16)
  copyMem(addr result[0], addr r.data[r.pos], 16)
  r.pos += 16

proc skipTaggedFields*(r: var KafkaReader) =
  ## Reads and discards a tagged-fields trailer (KIP-482): entries of
  ## (tagId: uvarint, length: uvarint, payload). Unknown tags are ignored by design.
  let count = r.decodeUvarint()
  const maxFields = 10_000
  if count > uint64(maxFields):
    raise newException(
      KafkaProtocolError,
      "tagged-fields count " & $count & " exceeds sanity limit " & $maxFields,
    )
  for _ in 0 ..< int(count):
    discard r.decodeUvarint() # tagId
    let payloadLen = r.decodeUvarint()
    if payloadLen > uint64(r.remaining):
      raise newException(
        KafkaProtocolError,
        "tagged field payload length " & $payloadLen & " exceeds remaining buffer",
      )
    r.skip(int(payloadLen))

proc decodeUvarintAt(
    data: openArray[byte], offset: int
): tuple[value: uint64, nextPos: int] {.raises: [KafkaProtocolError].} =
  ## Decode a KIP-482 unsigned varint from `data` at `offset`, returning the value
  ## and the position past it; works on the openArray so the hot response-dispatch
  ## path does not copy the frame.
  var pos = offset
  var shift = 0
  var value: uint64 = 0
  var bytesRead = 0
  while true:
    if pos >= data.len:
      raise newException(KafkaProtocolError, "truncated varint in response header")
    let b = data[pos]
    inc pos
    inc bytesRead
    # Match decodeUvarint's strictness: on the 10th byte (shift == 63) only the low
    # bit fits a uint64; any higher data bit or continuation bit is an overflow.
    if shift == 63 and (b and 0xFE'u8) != 0:
      raise newException(KafkaProtocolError, "varint overflow in response header")
    value = value or (uint64(b and 0x7F) shl shift)
    if (b and 0x80'u8) == 0:
      break
    shift += 7
    if shift >= 64 or bytesRead > 10:
      raise newException(KafkaProtocolError, "varint overflow in response header")
  (value, pos)

proc skipResponseHeaderTaggedFields*(
    data: openArray[byte], offset: int
): int {.raises: [KafkaProtocolError].} =
  ## Skip the *entire* tagged_fields section of a flexible response header
  ## (KIP-482): a uvarint count then `count` entries of (tagId, length, payload).
  ## Returns the body offset. Almost always 0 entries, but skipping only the count
  ## varint would start the body mid-tag.
  let (count, afterCount) = decodeUvarintAt(data, offset)
  const maxFields = 10_000
  if count > uint64(maxFields):
    raise newException(
      KafkaProtocolError,
      "response header tagged-fields count " & $count & " exceeds sanity limit " &
        $maxFields,
    )
  var pos = afterCount
  for _ in 0 ..< int(count):
    let (_, afterTag) = decodeUvarintAt(data, pos) # tagId (discarded)
    let (payloadLen, afterLen) = decodeUvarintAt(data, afterTag)
    pos = afterLen
    if payloadLen > uint64(data.len - pos):
      raise newException(
        KafkaProtocolError,
        "response header tagged field payload length " & $payloadLen &
          " exceeds remaining buffer",
      )
    pos += int(payloadLen)
  return pos
