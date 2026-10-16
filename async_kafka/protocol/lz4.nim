## Minimal LZ4 frame compression wrapper using system liblz4.
##
## The Kafka 0.8.2–0.9.0.1 frame header-checksum bug (KAFKA-3160, fixed in
## 0.10.0.0) is intentionally not
## supported: liblz4's decoder expects a correct header checksum, so only
## pre-2016 magic v0/v1 data fails to decode, never the RecordBatch v2 format
## used here.
##
## Symbols are resolved with `std/dynlib` on first use rather than `{.importc,
## dynlib.}`, whose module-init `nimGetProcAddr` `rawQuit`s on a missing library
## or symbol. `LibraryError` becomes `KafkaError` in `callNativeLz4`.

import std/dynlib

import ../errors

type LZ4F_dctx = pointer

const lz4lib =
  when defined(linux):
    "liblz4.so(|.1)"
  elif defined(macosx):
    "liblz4(|.1).dylib"
  elif defined(windows):
    "liblz4.dll"
  else:
    "liblz4.so(|.1)"

const
  LZ4F_VERSION: cuint = 100
  defaultMaxDecompressSize* = 128 * 1024 * 1024

type
  Lz4CompressFrameBound =
    proc(srcSize: csize_t, prefsPtr: pointer): csize_t {.cdecl, gcsafe, raises: [].}
  Lz4CompressFrame = proc(
    dstBuffer: pointer,
    dstCapacity: csize_t,
    srcBuffer: pointer,
    srcSize: csize_t,
    prefsPtr: pointer,
  ): csize_t {.cdecl, gcsafe, raises: [].}
  Lz4IsError = proc(code: csize_t): cuint {.cdecl, gcsafe, raises: [].}
  Lz4GetErrorName = proc(code: csize_t): cstring {.cdecl, gcsafe, raises: [].}
  Lz4CreateDecompressionContext =
    proc(dctxPtr: ptr LZ4F_dctx, version: cuint): csize_t {.cdecl, gcsafe, raises: [].}
  Lz4FreeDecompressionContext =
    proc(dctx: LZ4F_dctx): csize_t {.cdecl, gcsafe, raises: [].}
  Lz4Decompress = proc(
    dctx: LZ4F_dctx,
    dstBuffer: pointer,
    dstSizePtr: ptr csize_t,
    srcBuffer: pointer,
    srcSizePtr: ptr csize_t,
    optionsPtr: pointer,
  ): csize_t {.cdecl, gcsafe, raises: [].}

var
  lz4Handle: LibHandle
  LZ4F_compressFrameBound: Lz4CompressFrameBound
  LZ4F_compressFrame: Lz4CompressFrame
  LZ4F_isError: Lz4IsError
  LZ4F_getErrorName: Lz4GetErrorName
  LZ4F_createDecompressionContext: Lz4CreateDecompressionContext
  LZ4F_freeDecompressionContext: Lz4FreeDecompressionContext
  LZ4F_decompress: Lz4Decompress

when defined(kafkaLz4TestHooks):
  var lz4LoadBlockedForTest = false

proc bindLz4Proc[T](lib: LibHandle, name: cstring): T {.raises: [LibraryError].} =
  # `symAddr` raises nothing by contract, so normalise its nil result here rather
  # than via `checkedSymAddr`, whose inferred effect is broader.
  let sym = symAddr(lib, name)
  if sym.isNil:
    raise newException(LibraryError, "could not find symbol: " & $name)
  result = cast[T](sym)

proc loadLz4Lib() {.raises: [LibraryError].} =
  when defined(kafkaLz4TestHooks):
    # Ahead of the cached-handle shortcut: the hook must fail even after an
    # earlier load in this process.
    if lz4LoadBlockedForTest:
      raise newException(LibraryError, "could not load: " & lz4lib & " (test hook)")
  if lz4Handle != nil:
    return
  # Normalise a nil handle and a loader exception into the LibraryError contract.
  var lib: LibHandle
  try:
    lib = loadLibPattern(lz4lib)
  except:
    raise newException(LibraryError, "could not load: " & lz4lib)
  if lib.isNil:
    raise newException(LibraryError, "could not load: " & lz4lib)
  let compressFrameBound =
    bindLz4Proc[Lz4CompressFrameBound](lib, "LZ4F_compressFrameBound")
  let compressFrame = bindLz4Proc[Lz4CompressFrame](lib, "LZ4F_compressFrame")
  let isError = bindLz4Proc[Lz4IsError](lib, "LZ4F_isError")
  let getErrorName = bindLz4Proc[Lz4GetErrorName](lib, "LZ4F_getErrorName")
  let createDecompressionContext =
    bindLz4Proc[Lz4CreateDecompressionContext](lib, "LZ4F_createDecompressionContext")
  let freeDecompressionContext =
    bindLz4Proc[Lz4FreeDecompressionContext](lib, "LZ4F_freeDecompressionContext")
  let decompress = bindLz4Proc[Lz4Decompress](lib, "LZ4F_decompress")
  LZ4F_compressFrameBound = compressFrameBound
  LZ4F_compressFrame = compressFrame
  LZ4F_isError = isError
  LZ4F_getErrorName = getErrorName
  LZ4F_createDecompressionContext = createDecompressionContext
  LZ4F_freeDecompressionContext = freeDecompressionContext
  LZ4F_decompress = decompress
  lz4Handle = lib

template callNativeLz4(body: untyped): untyped =
  # Shared by compress, decompress, and the missing-symbol probe: `LibraryError`
  # becomes `KafkaError`, an existing `KafkaError` is re-raised, and Defect is not
  # caught (`--panics:on`).
  try:
    loadLz4Lib()
    body
  except KafkaError as e:
    raise e
  except LibraryError as e:
    raise newException(KafkaError, "lz4 native library error: " & e.msg)

proc ensureLz4Loaded*() {.raises: [KafkaError].} =
  ## Force the load and symbol binding the first compress or decompress would do
  ## lazily, so a host without liblz4 reports a configuration error instead of a
  ## failed send. Repeated calls cost one nil check.
  callNativeLz4:
    discard

proc lz4Compress*(data: openArray[byte]): seq[byte] {.raises: [KafkaError].} =
  callNativeLz4:
    let srcPtr =
      if data.len == 0:
        nil
      else:
        unsafeAddr data[0]
    let bound = LZ4F_compressFrameBound(csize_t(data.len), nil)
    result = newSeq[byte](bound)
    let sz = LZ4F_compressFrame(
      addr result[0], csize_t(result.len), srcPtr, csize_t(data.len), nil
    )
    if LZ4F_isError(sz) != 0:
      raise
        newException(KafkaError, "lz4 compression failed: " & $LZ4F_getErrorName(sz))
    result.setLen(sz)

proc lz4Decompress*(
    data: openArray[byte], maxSize: int = defaultMaxDecompressSize
): seq[byte] {.raises: [KafkaError].} =
  if data.len == 0:
    return @[]
  # A negative budget would raise RangeDefect in the initial `newSeq`, and zero
  # would leave the first `addr dstBuf[0]` out of bounds. Checked before any dynlib
  # call.
  if maxSize <= 0:
    raise newException(
      KafkaError, "lz4 decompression size limit must be positive, got " & $maxSize
    )

  callNativeLz4:
    var dctx: LZ4F_dctx
    let ctxResult = LZ4F_createDecompressionContext(addr dctx, LZ4F_VERSION)
    if LZ4F_isError(ctxResult) != 0:
      raise newException(
        KafkaError, "lz4 decompression context failed: " & $LZ4F_getErrorName(ctxResult)
      )
    defer:
      discard LZ4F_freeDecompressionContext(dctx)

    # Size the initial buffer from the compressed input, capped at maxSize, so a
    # ~100MB frame does not force a ~400MB up-front allocation.
    var dstBuf = newSeq[byte](min(data.len * 4 + 256, maxSize))
    var srcOffset: csize_t = 0
    let srcTotal = csize_t(data.len)
    var totalOut: int = 0

    # Backstop alongside the no-progress guard and maxSize cap: a legitimate
    # stream stays well under this bound, a spinning decoder does not.
    var iterations = 0
    let maxIterations = data.len * 2 + 64

    # `0` = current frame fully decoded; `> 0` = liblz4 wants more input. A
    # non-zero value at loop exit means the frame was truncated.
    var lastRet: csize_t = 0
    var brokeOnNoProgress = false

    while srcOffset < srcTotal:
      inc iterations
      if iterations > maxIterations:
        raise newException(KafkaError, "lz4 decompression exceeded iteration bound")
      if totalOut >= dstBuf.len:
        # Clamp the doubled size rather than rejecting on overshoot: an 0.8*maxSize
        # output is legitimate even though 2*(0.6*maxSize) > maxSize. Only a buffer
        # already at the cap that still needs room overflows.
        if dstBuf.len >= maxSize:
          raise newException(KafkaError, "lz4 decompression exceeded size limit")
        dstBuf.setLen(min(dstBuf.len * 2, maxSize))

      var dstSize = csize_t(dstBuf.len - totalOut)
      var srcSize = csize_t(srcTotal - srcOffset)

      let ret = LZ4F_decompress(
        dctx,
        addr dstBuf[totalOut],
        addr dstSize,
        unsafeAddr data[srcOffset],
        addr srcSize,
        nil,
      )

      if LZ4F_isError(ret) != 0:
        raise newException(
          KafkaError, "lz4 decompression failed: " & $LZ4F_getErrorName(ret)
        )

      totalOut += int(dstSize)
      srcOffset += srcSize
      lastRet = ret

      # `ret == 0` means only the *current* frame is decoded; the loop continues so
      # concatenated frames keep decoding (the shared dctx resets at the boundary).
      # Termination comes from `srcOffset < srcTotal` plus the no-progress guard.
      if dstSize == 0 and srcSize == 0:
        brokeOnNoProgress = true
        break

    # `lastRet > 0` means liblz4 still wanted input when `data` ran out (missing
    # endmark); a no-progress break with unconsumed input loses the same bytes.
    if lastRet != 0:
      raise newException(
        KafkaError,
        "lz4 decompression: truncated frame (endmark missing, decoder hint=" & $lastRet &
          ")",
      )
    if brokeOnNoProgress and srcOffset < srcTotal:
      raise newException(
        KafkaError,
        "lz4 decompression: stalled with " & $(int(srcTotal - srcOffset)) &
          " unconsumed byte(s)",
      )

    dstBuf.setLen(totalOut)
    return dstBuf

when defined(kafkaLz4TestHooks):
  # Probe that resolves a symbol liblz4 does not export, through the same wrapper
  # production compress/decompress use. Absent without the -d flag.
  proc lz4CallMissingSymbol*() {.raises: [KafkaError].} =
    callNativeLz4:
      discard bindLz4Proc[pointer](lz4Handle, "LZ4F_asyncKafkaMissingSymbol")

  proc lz4BlockLoadForTest*(blocked: bool) =
    ## Drives the "host without liblz4" path on a host that has it: while blocked
    ## every load attempt fails, even one with a cached handle.
    lz4LoadBlockedForTest = blocked
