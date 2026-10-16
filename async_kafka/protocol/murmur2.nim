## Kafka-compatible murmur2 hash, bit-identical to the Java client's
## ``org.apache.kafka.common.utils.Utils.murmur2``.

proc murmur2*(data: openArray[byte]): int32 =
  const
    seed = 0x9747b28c'u32
    m = 0x5bd1e995'u32
    r = 24

  let length = data.len
  # Java seeds with `seed ^ length` (signed int32); uint32(length) matches for
  # every key below 2 GiB, which is all the partitioner ever sees.
  var h = seed xor uint32(length)

  var i = 0
  while length - i >= 4:
    var k =
      uint32(data[i]) or (uint32(data[i + 1]) shl 8) or (uint32(data[i + 2]) shl 16) or
      (uint32(data[i + 3]) shl 24)
    k = k * m
    k = k xor (k shr r)
    k = k * m
    h = h * m
    h = h xor k
    i += 4

  let remaining = length - i
  if remaining >= 3:
    h = h xor (uint32(data[i + 2]) shl 16)
  if remaining >= 2:
    h = h xor (uint32(data[i + 1]) shl 8)
  if remaining >= 1:
    h = h xor uint32(data[i])
    h = h * m

  h = h xor (h shr 13)
  h = h * m
  h = h xor (h shr 15)

  return cast[int32](h)
