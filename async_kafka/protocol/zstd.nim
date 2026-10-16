## Zstd compression wrapper using nim_zstd bindings with KafkaError handling.

import pkg/zstd/compress as zstd_compress
import pkg/zstd/decompress as zstd_decompress
import pkg/zstd/common

import ../errors

const
  defaultMaxDecompressSize* = 128 * 1024 * 1024
  defaultCompressionLevel*: cint = 3
  decompressChunkSize = 128 * 1024
    ## Output window for the streaming decode; bounds the copy-out buffer, not
    ## zstd's own internal buffers, which follow the frame's declared window.

proc zstdCompress*(
    data: openArray[byte], level: cint = defaultCompressionLevel
): seq[byte] {.raises: [KafkaError].} =
  if data.len == 0:
    return @[]
  let srcPtr = unsafeAddr data[0]
  let srcSize = csize_t(data.len)
  let bound = ZSTD_compressBound(srcSize)
  result = newSeq[byte](bound)
  let sz = ZSTD_compress(addr result[0], csize_t(result.len), srcPtr, srcSize, level)
  if ZSTD_isError(sz):
    raise newException(KafkaError, "zstd compression failed: " & $ZSTD_getErrorName(sz))
  result.setLen(sz)

proc zstdDecompress*(
    data: openArray[byte], maxSize: int = defaultMaxDecompressSize
): seq[byte] {.raises: [KafkaError].} =
  if data.len == 0:
    return @[]
  if maxSize <= 0:
    raise newException(KafkaError, "zstd decompression size limit must be positive")

  let srcPtr = unsafeAddr data[0]
  let srcSize = csize_t(data.len)

  # Streaming decode rather than one-shot ZSTD_decompress: the one-shot call
  # needs the whole output buffer up front, sized by ZSTD_getFrameContentSize —
  # a number the sender writes into the frame header. A tiny frame declaring
  # 128 MB would allocate that much before failing, and OutOfMemDefect is not
  # catchable. ZSTD_decompressStream grows the result by what the frame really
  # yields, and the declared size is consulted only as a rejection check.
  let declared = ZSTD_getFrameContentSize(srcPtr, srcSize)
  if declared == ZSTD_CONTENTSIZE_ERROR:
    raise newException(KafkaError, "zstd decompression failed: content size error")
  if declared != ZSTD_CONTENTSIZE_UNKNOWN and declared > clonglong(maxSize):
    # A declared size of 2^63 or more arrives as a negative clonglong and passes
    # this check; zstd rejects those frames on its window check.
    raise newException(KafkaError, "zstd decompression exceeded size limit")

  var dstream = ZSTD_createDStream()
  if dstream.isNil:
    raise newException(KafkaError, "zstd decompression failed: cannot create stream")
  defer:
    discard ZSTD_freeDStream(dstream)

  let initRes = ZSTD_initDStream(dstream)
  if ZSTD_isError(initRes):
    raise newException(
      KafkaError, "zstd decompression failed: " & $ZSTD_getErrorName(initRes)
    )

  var chunk = newSeq[byte](decompressChunkSize)
  var inBuf = ZSTD_inBuffer(src: srcPtr, size: srcSize, pos: 0)
  # Start small: a short frame need not reserve the full limit.
  result = newSeqOfCap[byte](min(decompressChunkSize, maxSize))

  while inBuf.pos < inBuf.size:
    var outBuf = ZSTD_outBuffer(dst: addr chunk[0], size: csize_t(chunk.len), pos: 0)
    let hint = ZSTD_decompressStream(dstream, addr outBuf, addr inBuf)
    if ZSTD_isError(hint):
      raise newException(
        KafkaError, "zstd decompression failed: " & $ZSTD_getErrorName(hint)
      )
    # Append only what this call produced: `pos` is where zstd stopped, not the
    # window size.
    if outBuf.pos > 0:
      let produced = int(outBuf.pos)
      if result.len + produced > maxSize:
        raise newException(KafkaError, "zstd decompression exceeded size limit")
      let start = result.len
      result.setLen(start + produced)
      copyMem(addr result[start], addr chunk[0], produced)
    # zstd returns 0 only at a frame boundary, where `inBuf.pos` has advanced; a
    # truncated frame just ends the loop above with partial output. Guard on no
    # progress anyway so a malformed frame cannot hang the event loop.
    if outBuf.pos == 0 and inBuf.pos == 0:
      raise newException(KafkaError, "zstd decompression failed: truncated frame")
