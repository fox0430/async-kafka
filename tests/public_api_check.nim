## Verifies the stable package-root API contract. The source manifest pins
## qualified export intent; the semantic manifest pins actual symbols,
## overloads, constants, proc defaults, public fields, and enum values.

import std/[algorithm, os, strutils, tables]

import ast, options, pathutils, idents, renderer
import modulegraphs, passes, commands, cmdlinehelper, types, sem, semdata

const
  expectedQualifiedApiSize = 205
  expectedSemanticApiSize = 208
  qualifiedManifestPath = "tests/public_api.txt"
  semanticManifestPath = "tests/public_api.signatures.txt"

type ContractCapture = object
  qualifiedEntries: seq[string]
  semanticEntries: seq[string]
  qualifiedExportStatements: int
  semanticExportStatements: int
  forbiddenSyntaxNodes: int
  mainModuleProcessed: bool
  semanticItemsValid: bool

var capture = ContractCapture(semanticItemsValid: true)

proc hasDuplicate(entries: seq[string]): bool =
  var seen = initTable[string, bool]()
  for entry in entries:
    if entry in seen:
      return true
    seen[entry] = true

proc normalizedText(value: string): string =
  var pendingSpace = false
  for character in value:
    if character in {' ', '\t', '\r', '\n'}:
      pendingSpace = result.len > 0
    else:
      if pendingSpace:
        result.add(' ')
      result.add(character)
      pendingSpace = false

  result = result.replace("system.", "")
  result = result.replace("messages.", "")
  result = result.replace("fetcher.", "")
  result = result.replace("types.", "")
  result = result.replace("InternalRaisesFuture[", "Future[")

proc publicTypeShape(typ: PType): string =
  case typ.kind
  of tyRef:
    result = "ref"
    if typ.len > 0 and not typ[0].isNil:
      result.add(" " & publicTypeShape(typ[0]))
  of tyPtr:
    result = "ptr"
    if typ.len > 0 and not typ[0].isNil:
      result.add(" " & publicTypeShape(typ[0]))
  of tyDistinct:
    result = "distinct"
    if typ.len > 0 and not typ[0].isNil:
      result.add(" " & publicTypeShape(typ[0]))
  of tyEnum:
    result = "enum"
    if typ.len > 0 and not typ[0].isNil and typ[0].kind != tyEmpty:
      result.add(" of " & typeToString(typ[0]))
    if not typ.n.isNil:
      for itemIndex in 0 ..< typ.n.len:
        let item = typ.n[itemIndex]
        if item.kind == nkSym and sfExported in item.sym.flags:
          result.add("|" & item.sym.name.s & "=" & $item.sym.position)
  of tyObject:
    result = "object"
    if typ.len > 0 and not typ[0].isNil and typ[0].kind != tyEmpty:
      result.add(" of " & typeToString(typ[0]))
    if not typ.n.isNil and typ.n.kind == nkRecList:
      for partIndex in 0 ..< typ.n.len:
        let part = typ.n[partIndex]
        if part.kind == nkSym and sfExported in part.sym.flags:
          result.add("|" & part.sym.name.s & ":" & typeToString(part.sym.typ))
        elif part.kind == nkIdentDefs:
          for identIndex in 0 ..< part.len:
            let ident = part[identIndex]
            if ident.kind == nkSym and sfExported in ident.sym.flags:
              result.add("|" & ident.sym.name.s & ":" & typeToString(ident.sym.typ))
  else:
    result = typeToString(typ)

  result = normalizedText(result)

proc formalParamsText(symbol: PSym): string =
  if symbol.ast.isNil:
    return ""
  for childIndex in 0 ..< safeLen(symbol.ast):
    let child = symbol.ast[childIndex]
    if child.kind == nkFormalParams:
      return normalizedText(renderTree(child, {renderNoComments}))

proc serializeSymbol(symbol: PSym): string =
  var details: string
  case symbol.kind
  of skType:
    if symbol.typ.kind == tyProc and not symbol.ast.isNil:
      details = normalizedText(renderTree(symbol.ast, {renderNoComments}))
    else:
      details = publicTypeShape(symbol.typ)
  of skProc, skFunc, skIterator, skTemplate, skMacro:
    details = typeToString(symbol.typ)
    let formals = formalParamsText(symbol)
    if formals.len > 0:
      details.add(" formals=")
      details.add(formals)
  of skConst:
    if not symbol.ast.isNil:
      details = normalizedText(renderTree(symbol.ast, {renderNoComments}))
  else:
    details = typeToString(symbol.typ)

  normalizedText(symbol.name.s & "\t" & $symbol.kind & "\t" & details)

proc inspectSourceSyntax(node: PNode, topLevel: bool) =
  if node.kind == nkExportStmt:
    inc capture.qualifiedExportStatements
    if not topLevel:
      inc capture.forbiddenSyntaxNodes
    for itemIndex in 0 ..< safeLen(node):
      let item = node[itemIndex]
      if item.kind != nkDotExpr or safeLen(item) != 2 or item[0].kind != nkIdent or
          item[1].kind notin {nkIdent, nkAccQuoted}:
        inc capture.forbiddenSyntaxNodes
      else:
        let symbolName =
          if item[1].kind == nkAccQuoted:
            renderTree(item[1], {renderNoComments})
          else:
            item[1].ident.s
        capture.qualifiedEntries.add(item[0].ident.s & "." & symbolName)
  elif node.kind in {nkExportExceptStmt, nkIncludeStmt}:
    inc capture.forbiddenSyntaxNodes

  for childIndex in 0 ..< safeLen(node):
    inspectSourceSyntax(node[childIndex], topLevel and node.kind == nkStmtList)

proc inspectSemanticExports(node: PNode) =
  if node.kind == nkExportStmt:
    inc capture.semanticExportStatements
    for itemIndex in 0 ..< safeLen(node):
      let item = node[itemIndex]
      if item.kind != nkSym:
        capture.semanticItemsValid = false
      else:
        capture.semanticEntries.add(serializeSymbol(item.sym))
  else:
    for childIndex in 0 ..< safeLen(node):
      inspectSemanticExports(node[childIndex])

proc contractOpen(graph: ModuleGraph, module: PSym, idgen: IdGenerator): PPassContext =
  preparePContext(graph, module, idgen)

proc contractProcess(context: PPassContext, node: PNode): PNode =
  let module = PContext(context).module
  let isMain = sfMainModule in module.flags
  if isMain:
    capture.mainModuleProcessed = true
    inspectSourceSyntax(node, true)

  result = semWithPContext(PContext(context), node)

  if isMain:
    inspectSemanticExports(node)

proc contractClose(graph: ModuleGraph, context: PPassContext, node: PNode): PNode =
  closePContext(graph, PContext(context), node)

const contractPass =
  makePass(contractOpen, contractProcess, contractClose, isFrontend = true)

proc readContract(path: string): seq[string] =
  for line in lines(path):
    let entry = line.strip()
    if entry.len > 0 and not entry.startsWith("#"):
      result.add(entry)

proc firstDifference(expected, actual: seq[string]): string =
  let commonLength = min(expected.len, actual.len)
  for index in 0 ..< commonLength:
    if expected[index] != actual[index]:
      return
        "line " & $(index + 1) & ": expected <" & expected[index] & "> but got <" &
        actual[index] & ">"
  if expected.len != actual.len:
    return "length: expected " & $expected.len & " but got " & $actual.len
  ""

proc failContract(message: string) {.noreturn.} =
  stderr.writeLine("public API contract failed: " & message)
  quit(1)

proc main() =
  let projectRoot = currentSourcePath.parentDir.parentDir
  let projectFile = absolutePath(projectRoot / "async_kafka.nim")
  let qualifiedManifest = absolutePath(projectRoot / qualifiedManifestPath)
  let semanticManifest = absolutePath(projectRoot / semanticManifestPath)
  let updateSemanticManifest = "--update" in commandLineParams()

  var config = newConfigRef()
  config.projectPath = AbsoluteDir(projectRoot)
  config.projectName = splitFile(projectFile).name
  config.projectFull = AbsoluteFile(projectFile)
  config.setCmd cmdCheck

  var cache = newIdentCache()
  var graph = newModuleGraph(cache, config)
  let driver = NimProg(
    processCmdLine: proc(pass: TCmdLinePass, command: string, target: ConfigRef) =
      discard
  )
  driver.initDefinesProg(config, "async_kafka_api_contract")
  if not driver.loadConfigsAndProcessCmdLine(cache, config, graph):
    failContract("compiler configuration failed")

  registerPass(graph, contractPass)
  graph.compileProject()
  if config.errorCounter != 0:
    failContract("semantic analysis failed")
  if not capture.mainModuleProcessed:
    failContract("root module was not processed")

  if capture.forbiddenSyntaxNodes != 0:
    failContract("root contains a non-qualified, nested, except, or include export")
  if capture.qualifiedExportStatements != 1:
    failContract("root must contain exactly one top-level export statement")
  if capture.semanticExportStatements != capture.qualifiedExportStatements:
    failContract("source and semantic export statement counts differ")
  if not capture.semanticItemsValid:
    failContract("an export statement did not resolve to explicit exported symbols")
  if capture.qualifiedEntries.len != expectedQualifiedApiSize:
    failContract("qualified API size changed")
  if hasDuplicate(capture.qualifiedEntries) or hasDuplicate(capture.semanticEntries):
    failContract("duplicate API entries")

  let expectedQualified = readContract(qualifiedManifest)
  if capture.qualifiedEntries != expectedQualified:
    failContract(firstDifference(expectedQualified, capture.qualifiedEntries))

  capture.semanticEntries.sort()
  if updateSemanticManifest:
    let content =
      "# Stable semantic root API snapshot.\n" &
      "# Generated by the public API checker (--update).\n" &
      capture.semanticEntries.join("\n") & "\n"
    writeFile(semanticManifest, content)
    echo "updated ", semanticManifestPath
    return

  if capture.semanticEntries.len != expectedSemanticApiSize:
    failContract("semantic API size changed")
  let expectedSemantic = readContract(semanticManifest)
  if capture.semanticEntries != expectedSemantic:
    failContract(firstDifference(expectedSemantic, capture.semanticEntries))

  echo "public API contract verified: ",
    capture.qualifiedEntries.len, " qualified exports, ", capture.semanticEntries.len,
    " semantic symbols"

main()
