switch("path", "$projectDir/..")

# The public API checker uses the compiler AST for a semantic signature snapshot.
let nimExe = selfExe()
let binarySuffix = when defined(windows): "bin\\nim.exe" else: "bin/nim"
doAssert nimExe.len > binarySuffix.len
doAssert nimExe[nimExe.len - binarySuffix.len .. ^1] == binarySuffix
switch("path", nimExe[0 ..< nimExe.len - binarySuffix.len] & "compiler")

# Enable SCRAM test hooks (deterministic client nonce) for KAT / integration
# tests. Compile-time gated so production builds cannot pin the nonce.
switch("define", "kafkaScramTestHooks")
# Enable connection acquire-park hooks so tests can cancel after a synchronous
# writeLock / inflight take. Compile-time gated so production builds cannot
# stall acquires.
switch("define", "kafkaConnTestHooks")
# Enable close-teardown park hooks so tests can cancel a producer/consumer
# close() while it is inside a teardown stage that must absorb the cancel.
# Compile-time gated so production builds cannot park a close.
switch("define", "kafkaCloseTestHooks")
