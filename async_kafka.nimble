# Package

version = "0.1.0"
author = "fox0430"
description = "Async Kafka client in Nim"
license = "MIT"

skipDirs = @["tests", "docker"]

# Dependencies

requires "nim >= 2.2.8"
requires "chronos >= 4.2.2"
requires "results >= 0.5.0"
requires "stew >= 0.1.0"
requires "zlib >= 0.1.0"
requires "supersnappy >= 2.0.0"
requires "nimcrypto >= 0.7.0"
requires "bearssl >= 0.2.13"
requires "https://github.com/fox0430/nim_zstd == 0.2.1"

# Test hook defines come from tests/config.nims.
const backendFlag = "-d:asyncBackend=chronos"

import std/strutils

proc runTest(file: string) =
  exec "nim c " & backendFlag & " -r " & file

task verify, "type-check the package against the locked dependency set":
  ## Unlike `nimble check` (which only parses this file, and shadows a `check`
  ## task), this resolves through nimble.lock, so an out-of-lock bound fails
  ## setup, and type-checks the facade; `--compileOnly` skips the C compile.
  exec "nimble setup"
  exec "nim c " & backendFlag & " --compileOnly async_kafka.nim"

task check_api, "verify the stable root API contract":
  exec "nimble setup"
  runTest "tests/public_api_check.nim"

task test, "run unit tests only (no Kafka broker required)":
  checkApiTask()
  runTest "tests/unit_tests.nim"

task test_unit, "alias for `nimble test` (unit tests, no broker required)":
  testTask()

task check_examples, "type-check the example programs":
  # `listFiles` rather than a glob: nimscript's `walkFiles` is an {.error.}, and
  # a hand-kept list would let a new example go unchecked.
  exec "nimble setup"
  for file in listFiles("examples"):
    if file.endsWith(".nim"):
      exec "nim c " & backendFlag & " --compileOnly " & file

task test_integration, "run the full suite (requires a live Kafka broker)":
  checkApiTask()
  runTest "tests/all_tests.nim"
