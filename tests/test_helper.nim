proc toBytes*(s: string): seq[byte] =
  cast[seq[byte]](s)

proc toString*(b: seq[byte]): string =
  cast[string](b)
