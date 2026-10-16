import std/[unittest, os, tables, algorithm, strutils]

## Regression gate for verbatim copy-paste across the package sources.
##
## Duplication here is not a style preference. A `fix` commit once rewrote one
## of three identical O(T*P) lookups and left the other two untouched, and
## another `fix` commit re-created the very block it was removing. Neither was
## visible to the build, the tests, or review — the second one was a fix that
## reintroduced the duplication it meant to remove. Two budgets bound that:
## the total duplicated lines, and the fan-out of the single most repeated
## block. Both are ratchets: pay debt down by lowering them, never raise one
## without deciding the new copy is worth having.
##
## Method: every window of `windowLen` normalized lines (indentation stripped;
## blank and comment-only lines dropped) is looked up across all sources. A
## window found at two or more places is one clone. Windows made up entirely of
## parameter/argument lines are skipped — five helpers taking the same
## arguments share a signature, which is not duplicated logic.
##
## The protocol declaration table is excluded. It is consumed by the schema
## generator, where uniformity is the shape of the data rather than a copy of
## someone else's fix, and it would otherwise account for the largest share of
## the total.

const
  projectRoot = parentDir(parentDir(currentSourcePath))

  windowLen = 5
  excludedSource = "async_kafka/protocol/messages.nim"

  # Measured on this tree: 1680 duplicated lines and a fan-out of 3. Lower them
  # as debt is paid; raise one only after deciding the new copy is worth having.
  # The line budget carries a deliberate margin over the measurement: a
  # format-only pass rewrites line breaks without adding logic and has moved
  # this measurement by ~30 lines on its own, so re-measure after one and
  # re-baseline the budget instead of reading its delta as new duplication.
  maxDuplicatedLoc = 1720
  maxBlockFanout = 3

type
  Occurrence = tuple[file: string, line: int]
  SourceLine = tuple[code: string, line: int]
  CloneRow = tuple[fanout: int, text: string]

  ScanResult = object
    clones: int
    sites: int
    duplicatedLoc: int
    maxFanout: int
    worst: seq[CloneRow]

proc isArgumentWindow(window: seq[string]): bool =
  ## True when every line ends in `,` or `)`, so the scan skips it: usually a
  ## repeated parameter or argument list, but logic lines ending in `)` match.
  for line in window:
    if not (
      line.endsWith(",") or line.endsWith(")") or line.endsWith("],") or
      line.endsWith("),")
    ):
      return false
  true

proc scannedSources(): seq[string] =
  let prefix = projectRoot & DirSep
  for path in walkDirRec(projectRoot / "async_kafka"):
    if not path.endsWith(".nim"):
      continue
    let rel = path[prefix.len .. ^1]
    if rel == excludedSource:
      continue
    result.add(rel)
  result.sort()

proc normalizedSource(rel: string): seq[SourceLine] =
  ## Comment and blank lines carry no logic and are dropped, which also makes a
  ## block's key independent of how it happens to be commented.
  let src = readFile(projectRoot / rel).splitLines()
  for i, raw in src:
    let s = raw.strip()
    if s.len == 0 or s.startsWith("#"):
      continue
    result.add((code: s, line: i + 1))

proc describeOccurrences(occ: seq[Occurrence]): string =
  for i, o in occ:
    if i > 0:
      result.add(" ")
    result.add(o.file & ":" & $o.line)

proc computeScan(): ScanResult =
  var index = initTable[string, seq[Occurrence]]()
  for rel in scannedSources():
    let src = normalizedSource(rel)
    if src.len < windowLen:
      continue
    for j in 0 .. src.len - windowLen:
      var window: seq[string] = @[]
      for k in 0 ..< windowLen:
        window.add(src[j + k].code)
      if isArgumentWindow(window):
        continue
      let key = window.join("\n")
      if key notin index:
        index[key] = @[]
      index[key].add((rel, src[j].line))

  var rows: seq[CloneRow] = @[]
  for _, occ in index.pairs:
    if occ.len < 2:
      continue
    inc result.clones
    result.sites += occ.len
    result.duplicatedLoc += occ.len * windowLen
    if occ.len > result.maxFanout:
      result.maxFanout = occ.len
    rows.add(
      (occ.len, "x" & $occ.len & " " & $windowLen & "L  " & describeOccurrences(occ))
    )
  rows.sort(
    proc(a, b: CloneRow): int =
      cmp(b.fanout, a.fanout)
  )
  for i in 0 ..< min(rows.len, 6):
    result.worst.add(rows[i])

var cached: ScanResult
var cachedReady = false

proc scan(): ScanResult =
  ## Memoized: both tests below assert on the same measurement.
  if not cachedReady:
    cached = computeScan()
    cachedReady = true
  cached

suite "Duplication ratchet":
  test "duplicated lines stay within the recorded budget":
    let s = scan()
    if s.duplicatedLoc > maxDuplicatedLoc:
      echo "duplicated lines ",
        s.duplicatedLoc, " exceeds budget ", maxDuplicatedLoc, " (", s.clones,
        " clones over ", s.sites, " sites)"
      for row in s.worst:
        echo "  ", row.text
    check s.duplicatedLoc <= maxDuplicatedLoc

  test "no single block is repeated beyond the fan-out budget":
    let s = scan()
    if s.maxFanout > maxBlockFanout:
      echo "most repeated block appears ",
        s.maxFanout, " times, budget is ", maxBlockFanout
      for row in s.worst:
        echo "  ", row.text
    check s.maxFanout <= maxBlockFanout
