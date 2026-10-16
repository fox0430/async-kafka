## Gate for swallowed teardown failures.
##
## `CancelledError` reaching a shutdown step is expected, so broad handlers that
## absorb it are correct — but they used to be indistinguishable from a silent
## failure, and `consumer.close()` had eight of them that logged nothing. Every
## absorbed step now names itself through `absorbDuringTeardown`, which requires
## a compile-time site and reason and emits a debug line.
##
## This gate keeps that the only way to drop one: a `CancelledError` or
## `CatchableError` handler that discards without recording fails, with the file
## and line. Comment-only lines inside a handler body do not count as recording,
## so a site that explains itself in prose only still fails.
##
## The matcher is text, so it is itself under test: the first revision compared
## each statement for equality against the recording list, which no real call
## satisfies (`recording` held bare names, the sites write arguments), and it
## accepted only the `as e` spelling. Both made the gate pass without checking
## anything; the suite below drives the shapes so that cannot silently return.
##
## Scope: only the two broad types above. A narrower handler such as
## `except KafkaApiError` on a metrics-only path is a typed decision and not this
## gate's business.

import std/[algorithm, os, sequtils, strutils, unittest]

const
  projectRoot = parentDir(parentDir(currentSourcePath))
  scannedDir = projectRoot / "async_kafka"
  broadTypes = ["CancelledError", "CatchableError"]
  recording =
    ["absorbDuringTeardown", "warn", "error", "info", "debug", "trace", "emitMetric"]

type Swallow = tuple[site: string, body: seq[string]]

proc scannedSources(): seq[string] =
  let prefix = projectRoot & DirSep
  for path in walkDirRec(scannedDir):
    if not path.endsWith(".nim"):
      continue
    result.add(path[prefix.len .. ^1])
  result.sort()

proc indentOf(line: string): int =
  var index = 0
  while index < line.len and line[index] in {' ', '\t'}:
    inc index
  index

proc isBroadHandler(line: string): bool =
  ## True for a handler catching one of the broad types, in every spelling the
  ## code uses: `except X:`, `except X as e:`, `except X as exc:  # comment`.
  ## Anything else (`except KafkaError as e:`) is a narrower decision.
  let stripped = line.strip
  if not stripped.startsWith("except "):
    return false
  let rest = stripped["except ".len .. ^1]
  for broadType in broadTypes:
    if not rest.startsWith(broadType):
      continue
    var tail = rest[broadType.len .. ^1].strip()
    let comment = tail.find('#')
    if comment >= 0:
      tail = tail[0 ..< comment].strip()
    if tail == ":":
      return true
    if tail.startsWith("as ") and tail.endsWith(":"):
      return true
  false

proc handlerBody(lines: seq[string], handlerLine: int): seq[string] =
  ## Statements belonging to the handler starting at `handlerLine`, by
  ## indentation.
  let indent = indentOf(lines[handlerLine])
  for lineIndex in handlerLine + 1 ..< lines.len:
    let line = lines[lineIndex]
    if line.strip.len == 0:
      continue
    if indentOf(line) <= indent:
      break
    if line.strip.startsWith("#"):
      continue
    result.add(line.strip)

proc records(body: seq[string]): bool =
  ## True when a statement in the handler body records the absorbed failure.
  ## Matched by prefix, not equality: the real statements carry arguments
  ## (`absorbDuringTeardown("site", "reason")`, `warn "..."`), so an equality
  ## test against bare names never fires and the gate passes vacuously.
  for statement in body:
    for name in recording:
      if statement == name or statement.startsWith(name & " ") or
          statement.startsWith(name & "("):
        return true
  false

proc unrecordedSwallowsIn(rel: string, text: string): seq[Swallow] =
  ## Sites in one source text that absorb a broad exception and discard it
  ## without recording. Split out from the tree walk so the matcher can be
  ## driven with written-out sources.
  let lines = text.splitLines()
  for lineIndex, line in lines:
    if not isBroadHandler(line):
      continue
    let body = handlerBody(lines, lineIndex)
    let discards = body.filterIt(it == "discard" or it.startsWith("discard "))
    if discards.len == 0:
      continue
    # Re-raising reports the failure to the caller, so the `discard` beside it
    # is cleanup (a released slot handle, a cleared flag), not a swallowed
    # exception. A body that raises anywhere is out of scope, not only one that
    # ends by raising.
    if body.anyIt(it == "raise" or it.startsWith("raise ")):
      continue
    if records(body):
      continue
    result.add((site: rel & ":" & $(lineIndex + 1), body: body))

proc unrecordedSwallows(): seq[Swallow] =
  for rel in scannedSources():
    result.add unrecordedSwallowsIn(rel, readFile(projectRoot / rel))

suite "Teardown failure policy":
  test "no broad handler discards without recording":
    let swallows = unrecordedSwallows()
    for swallow in swallows:
      # Not a count check: each site has to be fixed on its own, so name them
      # all rather than reporting "N sites".
      echo swallow.site, " discards without recording: ", swallow.body.join(" | ")
    check swallows.len == 0

  test "the matcher flags and clears each shape it claims to read":
    ## Both halves matter: a matcher that recognizes nothing passes the test
    ## above on a tree full of unrecorded absorbs, and one that accepts every
    ## recording spelling hides a real swallow behind a stray token.
    proc flags(source: string): int =
      unrecordedSwallowsIn("synthetic.nim", source).len

    # Flagged: the other `as` spelling, a trailing comment, and CatchableError.
    check flags(
      """
proc f() =
  try:
    discard
  except CancelledError as ce:
    discard
"""
    ) == 1
    check flags(
      """
proc f() =
  try:
    discard
  except CatchableError as exc: # teardown
    discard
"""
    ) == 1
    # Cleared: the recording calls as they are written at real sites.
    check flags(
      """
proc f() =
  try:
    discard
  except CancelledError as ce:
    absorbDuringTeardown("producer::close", "cancel")
    discard
"""
    ) == 0
    check flags(
      """
proc f() =
  try:
    discard
  except CatchableError as exc:
    warn "absorbing ", exc.msg
    discard
"""
    ) == 0
    # Cleared: a narrow type, and a handler that re-raises.
    check flags(
      """
proc f() =
  try:
    discard
  except KafkaError as e:
    discard
"""
    ) == 0
    check flags(
      """
proc f() =
  try:
    discard
  except CancelledError:
    discard
    raise
"""
    ) == 0

  test "absorbDuringTeardown is the live entry point":
    # The test above passes vacuously on a library that swallows nothing, and
    # removing `absorbDuringTeardown` would silently reopen the hole. Prove the
    # entry point is wired in: the count covers call sites only, because the
    # definition writes the name as `absorbDuringTeardown*` and does not match.
    var callSites = 0
    for rel in scannedSources():
      callSites += readFile(projectRoot / rel).count("absorbDuringTeardown(")
    check callSites >= 2
