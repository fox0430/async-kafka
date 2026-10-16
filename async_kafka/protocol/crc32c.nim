# CRC-32C (Castagnoli), the v2 RecordBatch checksum (KIP-98). 0x82F63B78 is the
# bit-reversed Castagnoli polynomial (0x1EDC6F41), for the reflected table below.
const crc32cTable: array[256, uint32] = block:
  var t: array[256, uint32]
  for i in 0'u32 ..< 256:
    var crc = i
    for _ in 0 ..< 8:
      if (crc and 1) != 0:
        crc = (crc shr 1) xor 0x82F63B78'u32
      else:
        crc = crc shr 1
    t[i] = crc
  t

proc crc32c*(data: openArray[byte]): uint32 =
  result = 0xFFFFFFFF'u32
  for b in data:
    result = crc32cTable[(result xor uint32(b)) and 0xFF] xor (result shr 8)
  result = result xor 0xFFFFFFFF'u32
