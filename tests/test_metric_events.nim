import std/[unittest, os, sets, tables, algorithm, strutils, sequtils]

## Vocabulary gate for the `MetricsHook` event contract (the metrics contract).
##
## Four rules are enforced by scanning text, so no runtime hook is needed:
##
##  1. **Coverage**: every event name emitted from `async_kafka/` is named by at
##     least one test, or listed in `knownUnverified`. Both directions of drift
##     were real: 26 of 62 events had no assertion anywhere, including every
##     consumer-side failure event.
##  2. **Documentation**: the metrics contract and the emit sites list exactly
##     the same events. An event that is emitted but undocumented fails, and so
##     does a documented row whose event no longer exists.
##  3. **Tag shape**: for every event, the tag keys at its emit site(s) match the
##     alternatives its row declares. Adding a key at one site of a multi-site
##     event therefore fails until the doc says so — that is what makes an
##     inconsistent tag set reviewable instead of invisible.
##  4. **Vocabulary**: each emit site names a member of the `metricEventNames`
##     table, and every member of that table is emitted somewhere. The compiler
##     enforces the first half (the argument is a `MetricEvent`); this reads the
##     mapping so rules 1-3 can still work on names, and catches the second half,
##     which no other rule sees: an enum value with no emit site is dead
##     vocabulary, and a name reachable only through the table would otherwise
##     look documented and covered.
##
## Known limits, stated so nobody over-reads it:
##   - Coverage proves a test *names* the event, not that the assertion is
##     meaningful. A name in a dead code path would satisfy it.
##   - Some assertions compare against names held in `string` variables; the scan
##     only sees literals.
##   - Two tag builders are resolved by name: `tpTags(...)` contributes
##     `topic` + `partition`, and `txnErrorTags(...)` contributes `error_code` +
##     `error_class`. A new helper would have to be taught here; a site that
##     builds its tags inline needs no entry.
##   - Tag *values* are not checked, only the keys a site passes.
##   - This file is excluded from the test-side scan; otherwise its own
##     allowlist would satisfy the requirement it exists to enforce.
##
## History: the first revision scanned every dotted literal in a namespace list,
## which counted three non-metrics as events — the `producer.send` cancellation
## scope name and two chronos future debug names (`producer.close.flush.race`,
## `producer.close.dlq.race`). A later revision read the name from the first
## string literal of each `emitMetric` call; now that the argument is a
## `MetricEvent`, the name is resolved through the vocabulary table instead.

const
  projectRoot = parentDir(parentDir(currentSourcePath))

  # Events with no test naming them yet. Empty on this tree: every emitted event
  # now has a runtime assertion somewhere under tests/. Add a name here only when
  # a path genuinely cannot be driven without infrastructure the suite lacks, and
  # say why; the staleness check below fails while a covered name is still listed.
  knownUnverified: seq[string] = @[]

type
  EventSite = object
    file: string
    line: int
    tagKeys: string
      ## Canonical, sorted `+`-joined tag-key list, so one site's tag set is one
      ## comparable string (`""` = no tags).

  EmitCall = object
    spelling: string
      ## The event argument as written, i.e. the `MetricEvent` member name.
    name: string
      ## Event name that member maps to; empty when the spelling is not in the
      ## vocabulary table (reported by the vocabulary test).
    file: string
    line: int
    tagKeys: string

proc canonical(keys: HashSet[string]): string =
  var sorted = toSeq(keys)
  sorted.sort()
  sorted.join("+")

proc vocabulary(): Table[string, string] =
  ## The `metricEventNames` table of the metrics module: enum member -> event
  ## name. Emit sites name an event through that enum, so this mapping is what
  ## turns a call site back into the name rules 1-3 check. Whitespace is
  ## collapsed first: the formatter wraps a long row between the member and its
  ## `cstring` literal.
  let text = readFile(projectRoot / "async_kafka" / "internal" / "metrics.nim")
  let blockStart = text.find("const metricEventNames*")
  if blockStart < 0:
    return
  let blockStop = text.find("\n]", blockStart)
  if blockStop < 0:
    return
  var tableText: string
  var lastWasSpace = false
  for c in text[blockStart ..< blockStop]:
    if c in {' ', '\t', '\n', '\r'}:
      if not lastWasSpace:
        tableText.add ' '
      lastWasSpace = true
    else:
      tableText.add c
      lastWasSpace = false
  var i = 0
  while true:
    let separator = tableText.find(": cstring\"", i)
    if separator < 0:
      break
    var memberStart = separator
    while memberStart > 0 and
        tableText[memberStart - 1] in {'a' .. 'z', 'A' .. 'Z', '0' .. '9', '_'}:
      dec memberStart
    let nameStart = separator + len(": cstring\"")
    let nameStop = tableText.find('"', nameStart)
    if nameStop < 0:
      break
    result[tableText[memberStart ..< separator]] = tableText[nameStart ..< nameStop]
    i = nameStop + 1

proc splitArgs(call: string): seq[string] =
  ## Top-level arguments of one call, as written. `call` includes the outer
  ## parentheses; nesting, brackets, and string literals are respected so a
  ## `tpTags(tp, (...))` argument stays one argument.
  var depth = 0
  var inString = false
  var start = 1
  var i = 1
  while i < call.len - 1:
    let c = call[i]
    if inString:
      if c == '"':
        inString = false
    elif c == '"':
      inString = true
    elif c in {'(', '[', '{'}:
      inc depth
    elif c in {')', ']', '}'}:
      dec depth
    elif c == ',' and depth == 0:
      result.add call[start ..< i]
      start = i + 1
    inc i
  result.add call[start ..< call.len - 1]

proc stringLiterals(lines: seq[string]): seq[string] =
  ## Every double-quoted run in the line, comment-only lines skipped so a name
  ## mentioned in prose is not mistaken for a call site.
  for line in lines:
    if line.strip(leading = true, trailing = false).startsWith("#"):
      continue
    var i = 0
    while i < line.len:
      if line[i] == '"':
        var j = i + 1
        while j < line.len and line[j] != '"':
          inc j
        if j < line.len:
          result.add(line[i + 1 ..< j])
          i = j + 1
        else:
          i = line.len
      else:
        inc i

proc sourceLines(path: string): seq[string] =
  ## Source split into lines, with comment-only lines blanked so a commented
  ## example is not read as a call site. A `#` inside a string literal on an
  ## otherwise-live line is left alone (no event name contains one).
  result = readFile(path).splitLines()
  for i, line in result:
    if line.strip(leading = true, trailing = false).startsWith("#"):
      result[i] = ""

proc tagKeysIn(call: string): HashSet[string] =
  ## Tag keys passed by one `emitMetric` call: the first element of every
  ## `("key", ...)` / `(cstring"key", ...)` tuple, plus the pair `tpTags`
  ## contributes implicitly.
  var i = 0
  while i < call.len:
    if call[i] == '(':
      # A `(` directly after an identifier closes over a call (`txnErrorTags(`,
      # `tpTags(`), not a tag tuple; its arguments only look like keys. Tag
      # tuples start after a separator, a bracket, or a line start.
      var prev = i - 1
      while prev >= 0 and call[prev] in {' ', '\n', '\t'}:
        dec prev
      if prev >= 0 and (call[prev] in {'a' .. 'z', 'A' .. 'Z', '0' .. '9', '_', ')'}):
        inc i
        continue
      var j = i + 1
      while j < call.len and call[j] in {' ', '\n', '\t'}:
        inc j
      if j + 7 <= call.len and call[j ..< j + 7] == "cstring":
        j += 7
        while j < call.len and call[j] in {' ', '\n', '\t'}:
          inc j
      if j < call.len and call[j] == '"':
        var k = j + 1
        while k < call.len and call[k] != '"':
          inc k
        let key = call[j + 1 ..< k]
        if key.len > 0 and key[0] in {'a' .. 'z', 'A' .. 'Z', '_'}:
          var identifier = true
          for ch in key:
            if ch notin {'a' .. 'z', 'A' .. 'Z', '0' .. '9', '_'}:
              identifier = false
          if identifier:
            result.incl key
    inc i
  if "tpTags(" in call:
    result.incl "topic"
    result.incl "partition"
  if "txnErrorTags(" in call:
    result.incl "error_code"
    result.incl "error_class"

proc emitCalls(lines: seq[string], rel: string): seq[EmitCall] =
  ## Every `emitMetric(...)` call in one file: the `MetricEvent` member it names
  ## and the tag keys it passes. The call is delimited by balanced parentheses so
  ## a nested `tpTags(tp, (...))` argument stays inside the same call.
  let vocabulary = vocabulary()
  let text = lines.join("\n")
  var searchFrom = 0
  while true:
    let at = text.find("emitMetric(", searchFrom)
    if at < 0:
      break
    searchFrom = at + 1
    let openParen = at + "emitMetric".len
    var depth = 0
    var i = openParen
    var closeParen = -1
    while i < text.len:
      if text[i] == '(':
        inc depth
      elif text[i] == ')':
        dec depth
        if depth == 0:
          closeParen = i
          break
      inc i
    if closeParen < 0:
      continue
    let call = text[openParen .. closeParen]
    let args = splitArgs(call)
    if args.len < 2:
      continue
    let spelling = args[1].strip()
    result.add(
      EmitCall(
        spelling: spelling,
        name: vocabulary.getOrDefault(spelling, ""),
        file: rel,
        line: text[0 ..< at].count('\n') + 1,
        tagKeys: canonical(tagKeysIn(call)),
      )
    )

proc emittedEvents(): tuple[events: Table[string, seq[EventSite]], unknown: seq[string]] =
  ## Emit sites by event name, plus the sites whose event argument is not a
  ## member of the vocabulary table (the compiler rejects those, so a hit here
  ## means the scan read something that is not a real call).
  let prefix = projectRoot & DirSep
  for path in walkDirRec(projectRoot / "async_kafka"):
    if not path.endsWith(".nim"):
      continue
    let rel = path[prefix.len .. ^1]
    for call in emitCalls(sourceLines(path), rel):
      if call.name.len == 0:
        result.unknown.add(
          call.file & ":" & $call.line & " passes `" & call.spelling & "`"
        )
        continue
      result.events.mgetOrPut(call.name, @[]).add(
        EventSite(file: call.file, line: call.line, tagKeys: call.tagKeys)
      )

proc documentedEvents(): Table[string, HashSet[string]] =
  ## Rows of the vocabulary tables: event name -> the set of canonical tag-key
  ## alternatives the row declares. A tagless row maps to a one-element set
  ## holding the empty string, never the empty set.
  for line in readFile(projectRoot / "docs" / "metrics.md").splitLines():
    let stripped = line.strip()
    if not stripped.startsWith("| `"):
      continue
    let cells = stripped.split('|')
    if cells.len < 5:
      continue
    let name = cells[1].strip().strip(chars = {'`'})
    var alternatives: HashSet[string]
    for alternative in cells[3].split(';'):
      var keys: HashSet[string]
      for token in alternative.split('+'):
        let key = token.strip().strip(chars = {'`'})
        if key.len > 0 and key != "—":
          keys.incl key
      alternatives.incl canonical(keys)
    result[name] = alternatives

proc testLiterals(): HashSet[string] =
  ## String literals appearing anywhere under tests/, this file excluded.
  # `rel` keeps the leading "tests/", so match on the full relative path.
  let prefix = projectRoot & DirSep
  for path in walkDirRec(projectRoot / "tests"):
    if not path.endsWith(".nim"):
      continue
    let rel = path[prefix.len .. ^1]
    if rel == "tests/test_metric_events.nim":
      continue
    for literal in stringLiterals(readFile(path).splitLines()):
      result.incl literal

proc sortedItems(names: seq[string]): string =
  var copy = names
  copy.sort()
  copy.join(", ")

suite "Metrics event vocabulary":
  test "every emitted event is named by a test or listed as unverified":
    let (events, unknown) = emittedEvents()
    check events.len > 0 # guards against a scan that silently matches nothing
    if unknown.len > 0:
      echo "emit sites whose event argument is not a vocabulary member:"
      for site in unknown:
        echo "  ", site
    check unknown.len == 0

    let named = testLiterals()
    var unverified: seq[string]
    for name in events.keys:
      if name notin named:
        unverified.add(name)
    unverified.sort()

    let listed = knownUnverified.toHashSet()
    var uncovered, stale: seq[string]
    for name in unverified:
      if name notin listed:
        uncovered.add(name)
    for name in listed:
      if name in named:
        stale.add(name)

    if uncovered.len > 0:
      echo "events with no test naming them (add a test, or list them in knownUnverified):"
      for name in uncovered:
        echo "  ",
          name, "  emitted from ", events[name][0].file, ":", events[name][0].line
    if stale.len > 0:
      echo "knownUnverified entries that a test now names — delete them:"
      for name in stale:
        echo "  ", name
    if stale.len > 0 or uncovered.len > 0:
      echo "events: ",
        events.len,
        "  covered: ",
        events.len - unverified.len,
        "  listed unverified: ",
        unverified.len
    check uncovered.len == 0
    check stale.len == 0

  test "the vocabulary table and the emit sites cover each other":
    ## The compiler already rejects an emit site naming a value outside the
    ## enum. What it cannot see is a value nobody emits: dead vocabulary that
    ## rules 1-3 would happily call documented and covered.
    let (events, unknown) = emittedEvents()
    check unknown.len == 0
    var dead: seq[string]
    for name in vocabulary().values:
      if name notin events:
        dead.add name
    if dead.len > 0:
      echo "vocabulary entries with no emit site: ", sortedItems(dead)
    check dead.len == 0

  test "docs/metrics.md lists exactly the emitted events":
    let (events, unknown) = emittedEvents()
    check unknown.len == 0
    let documented = documentedEvents()
    var undocumented, staleRows: seq[string]
    for name in events.keys:
      if name notin documented:
        undocumented.add name
    for name in documented.keys:
      if name notin events:
        staleRows.add name
    if undocumented.len > 0 or staleRows.len > 0:
      echo "emitted but not in docs/metrics.md: ", sortedItems(undocumented)
      echo "documented but no longer emitted: ", sortedItems(staleRows)
      echo "emitted: ", events.len, "  documented: ", documented.len
    check undocumented.len == 0
    check staleRows.len == 0

  test "the documented tag shape matches every emit site":
    let (events, unknown) = emittedEvents()
    check unknown.len == 0
    let documented = documentedEvents()
    var mismatches: seq[string]
    for name, sites in events:
      if name notin documented:
        continue # the previous test reports the missing row
      var observed: HashSet[string]
      for site in sites:
        observed.incl site.tagKeys
      if observed != documented[name]:
        var obs = toSeq(observed)
        obs.sort()
        var doc = toSeq(documented[name])
        doc.sort()
        mismatches.add(
          name & ": doc [" & doc.join(" | ") & "] sites [" & obs.join(" | ") & "]"
        )
    if mismatches.len > 0:
      mismatches.sort()
      echo "tag keys differ between docs/metrics.md and the emit sites:"
      for line in mismatches:
        echo "  ", line
    check mismatches.len == 0
