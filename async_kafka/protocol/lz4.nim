## LZ4 frame compression wrapper using the pure Nim lz4 package with KafkaError
## handling.
##
## The Kafka 0.8.2–0.9.0.1 frame header-checksum bug (KAFKA-3160, fixed in
## 0.10.0.0) is intentionally not supported: the decoder expects a correct header
## checksum, so only pre-2016 magic v0/v1 data fails to decode, never the
## RecordBatch v2 format used here.

import pkg/lz4 as lz4_lib

import ../errors

const
  defaultMaxDecompressSize* = 128 * 1024 * 1024
  # Kafka Java's LZ4 stream rejects linked blocks, and neither it nor librdkafka
  # writes a content checksum.
  kafkaFrameOptions = FrameOptions(blockIndependent: true, contentChecksum: false)

proc lz4Compress*(data: openArray[byte]): seq[byte] {.raises: [KafkaError].} =
  try:
    lz4_lib.compressFrame(data, kafkaFrameOptions)
  except Lz4Error as e:
    raise newException(KafkaError, "lz4 compression failed: " & e.msg)

proc lz4Decompress*(
    data: openArray[byte], maxSize: int = defaultMaxDecompressSize
): seq[byte] {.raises: [KafkaError].} =
  ## Decodes every concatenated frame in `data`. A truncated frame, a checksum
  ## mismatch, a legacy frame, or output beyond `maxSize` raises.
  if data.len == 0:
    return @[]
  if maxSize <= 0:
    raise newException(
      KafkaError, "lz4 decompression size limit must be positive, got " & $maxSize
    )
  try:
    var dec = lz4_lib.initFrameDecoder(maxSize)
    var info: FrameInfo
    var pos = 0
    while pos < data.len:
      pos += dec.feed(data.toOpenArray(pos, data.high), result)
      # feed stops after each frame header. A legacy frame has no checksum or
      # end mark, so a cut one would decode short; Kafka Java and LZ4F reject it.
      if dec.frameInfo(info) and info.kind == fkLegacy:
        raise newException(
          KafkaError, "lz4 decompression failed: legacy frames are not supported"
        )
    dec.finish()
  except Lz4Error as e:
    raise newException(KafkaError, "lz4 decompression failed: " & e.msg)
