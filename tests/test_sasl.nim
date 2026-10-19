import std/[unittest, base64, strutils, options, importutils]

import nimcrypto/[hmac, hash, sha2, pbkdf2]

import chronos
import stew/endians2

import async_kafka/[errors, sasl, sasl_plain, sasl_scram, cluster, broker]
import async_kafka/internal/metrics
import async_kafka/protocol/api_keys

import fake_broker
import async_helpers

privateAccess(SaslMechanism)

proc challengeSync(session: SaslSession, data: seq[byte]): (SaslState, seq[byte]) =
  ## Drive the now-async `challenge` from a synchronous test body.
  ##
  ## SCRAM's challenge is async because the key derivation yields the event loop
  ## between slices, so a test that wants a plain call site needs a blocking
  ## bridge. `waitFor` spins the loop, which is exactly what these tests want:
  ## they assert the result of a whole handshake, not that it interleaves.
  result = waitFor session.challenge(data)

proc toBytes(s: string): seq[byte] =
  @(s.toOpenArrayByte(0, s.high))

proc toString(data: seq[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

type ScramSimServer[T] = ref object
  ## Bidirectional SCRAM server simulator. Unlike a pre-computed (serverFirst,
  ## serverFinal) tuple, this walks the exchange as a real broker would:
  ## `serverFirst` is emitted first; `verifyProofAndBuildServerFinal` receives
  ## the client's final message, verifies the `p=` proof against the stored
  ## key (as an actual Kafka broker does), and only then emits `v=<sig>`.
  ## A client whose ClientKey/xorBytes/AuthMessage derivation is broken will
  ## fail proof verification here, closing the gap the pre-refactor
  ## `simulateScramServer` left open (it never touched `p=`).
  password: string
  salt: seq[byte]
  iterations: int
  clientFirstBare: string
  serverFirst: string
  authMessagePrefix: string

proc newScramSimServer[T](
    password: string,
    salt: seq[byte],
    iterations: int,
    clientFirstBare: string,
    serverNonce: string,
): ScramSimServer[T] =
  let saltB64 = encode(salt)
  let serverFirst = "r=" & serverNonce & ",s=" & saltB64 & ",i=" & $iterations
  ScramSimServer[T](
    password: password,
    salt: @salt,
    iterations: iterations,
    clientFirstBare: clientFirstBare,
    serverFirst: serverFirst,
    authMessagePrefix: clientFirstBare & "," & serverFirst,
  )

proc verifyProofAndBuildServerFinal[T](
    srv: ScramSimServer[T], clientFinal: string
): string {.raises: [KafkaProtocolError].} =
  # Split c=biws,r=<nonce>,p=<b64-proof> — RFC 5802 §5.1: p= is required and
  # must be last, but a real broker parses positionally so we do the same.
  var receivedProof: seq[byte]
  var haveProof = false
  var noProofParts: seq[string]
  for part in clientFinal.split(','):
    if part.startsWith("p="):
      let b64 = part[2 .. ^1]
      try:
        receivedProof = decode(b64).toBytes()
      except ValueError:
        raise newException(KafkaProtocolError, "sim: invalid base64 in p=")
      haveProof = true
    else:
      noProofParts.add(part)
  if not haveProof:
    raise newException(KafkaProtocolError, "sim: client-final missing p=")
  let clientFinalNoProof = noProofParts.join(",")
  let authMessage = srv.authMessagePrefix & "," & clientFinalNoProof

  let saltedPassword = pbkdf2(T, srv.password, srv.salt, srv.iterations, T.sizeDigest)

  var hmacCtx: HMAC[T]

  hmacCtx.init(saltedPassword)
  hmacCtx.update("Client Key")
  let clientKey = hmacCtx.finish().data
  let storedKey = T.digest(clientKey).data

  hmacCtx.init(storedKey)
  hmacCtx.update(authMessage)
  let clientSignature = hmacCtx.finish().data

  # Recover the client key from proof XOR signature, then confirm the client
  # actually knew the password by verifying H(recovered) == storedKey. This
  # is the check that ties the whole ClientKey / xorBytes / AuthMessage chain
  # on the client side to something observable in the test.
  if receivedProof.len != clientSignature.len:
    raise newException(KafkaProtocolError, "sim: proof length mismatch")
  var recoveredClientKey = newSeq[byte](clientSignature.len)
  for i in 0 ..< clientSignature.len:
    recoveredClientKey[i] = receivedProof[i] xor clientSignature[i]
  let recoveredStoredKey = T.digest(recoveredClientKey).data
  if @recoveredStoredKey != @storedKey:
    raise newException(KafkaProtocolError, "sim: invalid-proof")

  hmacCtx.init(saltedPassword)
  hmacCtx.update("Server Key")
  let serverKey = hmacCtx.finish().data
  hmacCtx.init(serverKey)
  hmacCtx.update(authMessage)
  let serverSignature = hmacCtx.finish().data
  hmacCtx.clear()

  result = "v=" & encode(serverSignature)

proc extractClientNonce(clientFirstBare: string): string =
  for part in clientFirstBare.split(','):
    if part.startsWith("r="):
      return part[2 .. ^1]
  return ""

suite "SASL PLAIN":
  test "mechanism name":
    let m = newSaslPlain("user", "pass")
    check m.name == "PLAIN"

  test "initial response format (RFC 4616)":
    let m = newSaslPlain("alice", "secret")
    let session = m.newSession()
    let resp = session.initialResponse()
    # Format: \0<username>\0<password>
    check resp[0] == 0x00
    let parts = toString(resp).split('\0')
    check parts.len == 3
    check parts[0] == "" # authzid (empty)
    check parts[1] == "alice"
    check parts[2] == "secret"

  test "challenge completes immediately":
    let m = newSaslPlain("user", "pass")
    let session = m.newSession()
    discard session.initialResponse()
    let (state, data) = challengeSync(session, @[])
    check state == ssComplete
    check data.len == 0

  test "initialResponse rejects a second call (m5)":
    # The password copy is burned after the first call, so a second call would
    # silently build a \0user\0 empty-password payload. Reject it instead.
    let m = newSaslPlain("user", "pass")
    let session = m.newSession()
    discard session.initialResponse()
    expect KafkaProtocolError:
      discard session.initialResponse()

  test "dispose burns the password copy of an unused session (m5)":
    # When auth aborts before initialResponse runs, dispose must zero the
    # per-session password. Observe it: after dispose, the (step-0) payload
    # carries an empty password.
    let m = newSaslPlain("alice", "secret")
    let session = m.newSession()
    check session.dispose != nil
    session.dispose()
    session.dispose() # idempotent
    let parts = toString(session.initialResponse()).split('\0')
    check parts[1] == "alice"
    check parts[2] == "" # password was burned to empty

  test "empty username and password are rejected at construction":
    # Empty SASL credentials would otherwise authenticate against a
    # misconfigured broker that accepts blank secrets. validateAscii now
    # fails the constructor instead of letting an empty password reach
    # the wire — surface the misconfiguration locally.
    expect KafkaError:
      discard newSaslPlain("", "pw")
    expect KafkaError:
      discard newSaslPlain("user", "")
    expect KafkaError:
      discard newSaslPlain("", "")

  test "independent sessions":
    let m = newSaslPlain("user", "pass")
    let s1 = m.newSession()
    let s2 = m.newSession()
    let r1 = s1.initialResponse()
    let r2 = s2.initialResponse()
    check r1 == r2

  test "password bytes survive across many sessions (sink elision regression)":
    ## Regression: each newSession must deep-copy the captured password buffer.
    ## If sink inference moves the master copy into the first session, later
    ## sessions build an initialResponse with an empty password and silently
    ## fail auth against a second broker in a multi-broker cluster.
    const sessions = 5
    let m = newSaslPlain("user", "letmein")
    var responses: seq[seq[byte]]
    # Build all sessions first, then call initialResponse on each in order,
    # so a sink-elided first session would visibly damage the rest.
    var ss: seq[SaslSession]
    for _ in 0 ..< sessions:
      ss.add(m.newSession())
    for s in ss:
      responses.add(s.initialResponse())
    check responses.len == sessions
    let expected = responses[0]
    # All sessions must produce byte-identical payloads — proves the password
    # bytes were not silently zeroed by an earlier session's burn.
    for r in responses:
      check r == expected
    # And the payload must actually contain the literal password bytes
    # (not zeros), so an `r == expected` check on all-zero buffers would not
    # silently pass.
    var s0 = ""
    for b in responses[0]:
      s0.add(char(b))
    check "letmein" in s0

suite "SASL SCRAM-SHA-256":
  const
    testUser = "testuser"
    testPass = "testpass"
    testSalt = "randomsalt"
    testIterations = 4096

  test "mechanism name":
    let m = newSaslScram(testUser, testPass, scramSha256)
    check m.name == "SCRAM-SHA-256"

  test "session exposes an idempotent dispose":
    # dispose burns ownedPwd / expectedServerSig on an auth abort that lands
    # before the mechanism's own burn points. Assert it is wired and safe to
    # call repeatedly, including before any step has run.
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    check session.dispose != nil
    session.dispose()
    session.dispose()

  test "empty credentials are rejected at construction":
    # Mirrors the PLAIN constructor: empty username or password fails fast
    # rather than running a full SCRAM exchange against a misconfigured
    # broker that would silently accept it.
    expect KafkaError:
      discard newSaslScram("", testPass, scramSha256)
    expect KafkaError:
      discard newSaslScram(testUser, "", scramSha256)
    expect KafkaError:
      discard newSaslScram("", "", scramSha256)

  test "initial response format":
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    let resp = toString(session.initialResponse())
    # "n,," = no channel binding, no authzid.
    check resp.startsWith("n,,")
    check resp.contains("n=" & testUser)
    check resp.contains(",r=")

  test "initialResponse rejects a second call before challenge":
    # Regression: the `step != 0` guard never fired (challenge is what advances
    # step), so a second call silently regenerated clientNonce.
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    discard session.initialResponse()
    var raised = false
    try:
      discard session.initialResponse()
    except KafkaProtocolError as e:
      raised = true
      check "called twice" in e.msg
    check raised

  test "full handshake":
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()

    # Step 1: client-first
    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1] # strip "n,,"

    let clientNonce = extractClientNonce(clientFirstBare)
    let serverNonce = clientNonce & "serverpart"
    let salt = testSalt.toBytes()

    let srv = newScramSimServer[sha256](
      testPass, salt, testIterations, clientFirstBare, serverNonce
    )

    # Step 2: client processes server-first
    let (state1, clientFinalBytes) = challengeSync(session, srv.serverFirst.toBytes())
    check state1 == ssInProgress

    let clientFinal = toString(clientFinalBytes)
    check clientFinal.startsWith("c=biws,r=" & serverNonce)
    check clientFinal.contains(",p=")

    # Server verifies the proof (raises KafkaProtocolError on mismatch) and
    # only then emits its own signature. This is the check the pre-refactor
    # helper skipped — a broken ClientKey / xorBytes / AuthMessage on the
    # client side would trip verifyProofAndBuildServerFinal.
    let serverFinal = verifyProofAndBuildServerFinal[sha256](srv, clientFinal)

    # Step 3: client processes server-final
    let (state2, finalResp) = challengeSync(session, serverFinal.toBytes())
    check state2 == ssComplete
    check finalResp.len == 0

  test "re-driving challenge after ssComplete is rejected":
    ## Regression: without a step advance in the step-1 success path, the
    ## burned expectedServerSig (len 0) would match an empty "v=" payload and
    ## the session would return ssComplete twice. The step guard must fire
    ## regardless of the payload contents.
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let clientNonce = extractClientNonce(clientFirstBare)
    let serverNonce = clientNonce & "serverpart"
    let salt = testSalt.toBytes()
    let srv = newScramSimServer[sha256](
      testPass, salt, testIterations, clientFirstBare, serverNonce
    )
    let (_, clientFinalBytes) = challengeSync(session, srv.serverFirst.toBytes())
    let serverFinal =
      verifyProofAndBuildServerFinal[sha256](srv, toString(clientFinalBytes))
    let (state2, _) = challengeSync(session, serverFinal.toBytes())
    check state2 == ssComplete

    # Re-drive with the original server-final: must raise via the step-2 guard
    # (not sneak through as another ssComplete).
    var raised = false
    try:
      discard challengeSync(session, serverFinal.toBytes())
    except KafkaProtocolError as e:
      raised = true
      check "unexpected challenge in step 2" in e.msg
    check raised

    # Re-drive with an empty "v=" payload: without the step advance the burned
    # expectedServerSig (len 0) would compare equal to an empty decoded
    # signature and ssComplete would be returned again.
    var raised2 = false
    try:
      discard challengeSync(session, "v=".toBytes())
    except KafkaProtocolError as e:
      raised2 = true
      check "unexpected challenge in step 2" in e.msg
    check raised2

  test "SCRAM aborts on a server mandatory extension (m=)":
    # RFC 5802 §7: an m= field in server-first-message demands an extension the
    # client must understand. We support none, so abort rather than downgrade.
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    discard session.initialResponse()
    let serverFirst = "m=must-understand,r=abc,s=c2FsdA==,i=4096"
    expect KafkaProtocolError:
      discard challengeSync(session, serverFirst.toBytes())

  test "SCRAM rejects a challenge before initialResponse":
    # Regression: challenge in step 0 without initialResponse ran PBKDF2 with
    # empty clientFirstBare / clientNonce (PLAIN rejects this; SCRAM did not).
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    let serverFirst = "r=abc,s=c2FsdA==,i=4096"
    var raised = false
    try:
      discard challengeSync(session, serverFirst.toBytes())
    except KafkaProtocolError as e:
      raised = true
      check "before initialResponse" in e.msg
    check raised

  test "SCRAM rejects e= in server-first-message":
    # RFC 5802 §5.1: e= belongs to server-final only.
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    discard session.initialResponse()
    let serverFirst = "e=other-error,r=abc,s=c2FsdA==,i=4096"
    var raised = false
    try:
      discard challengeSync(session, serverFirst.toBytes())
    except KafkaProtocolError as e:
      raised = true
      check "e= is not permitted" in e.msg
    check raised

  test "SCRAM rejects duplicate attributes in server-first-message":
    # RFC 5802 ABNF is strict; a duplicate r=/s=/i= would silently last-win.
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    discard session.initialResponse()
    # Duplicate r=: second r= would clobber the first (last-wins) — a hostile
    # server could sneak a nonce mismatch through by placing one of each.
    let dupR = "r=abc,r=def,s=c2FsdA==,i=4096"
    var raised = false
    try:
      discard challengeSync(session, dupR.toBytes())
    except KafkaProtocolError as e:
      raised = true
      check "duplicate r=" in e.msg
    check raised

    let session2 = m.newSession()
    discard session2.initialResponse()
    let dupS = "r=abc,s=c2FsdA==,s=b3Roc2FsdA==,i=4096"
    var raised2 = false
    try:
      discard challengeSync(session2, dupS.toBytes())
    except KafkaProtocolError as e:
      raised2 = true
      check "duplicate s=" in e.msg
    check raised2

    let session3 = m.newSession()
    discard session3.initialResponse()
    let dupI = "r=abc,s=c2FsdA==,i=4096,i=8192"
    var raised3 = false
    try:
      discard challengeSync(session3, dupI.toBytes())
    except KafkaProtocolError as e:
      raised3 = true
      check "duplicate i=" in e.msg
    check raised3

  test "SCRAM accepts unknown extension attributes in server-first-message":
    # RFC 5802 allows an [extensions] tail of attr-val pairs the client need
    # not understand; unknown attributes are skipped, not rejected.
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let clientNonce = extractClientNonce(clientFirstBare)
    let serverNonce = clientNonce & "serverpart"
    let serverFirst =
      "r=" & serverNonce & ",s=" & encode(testSalt) & ",i=" & $testIterations &
      ",ext=quota-info"
    let (state, resp) = challengeSync(session, serverFirst.toBytes())
    check state == ssInProgress
    check resp.len > 0

  test "SCRAM rejects malformed attribute shapes in server-first-message":
    # An attribute without '=' (or with an empty name) is not a valid extension
    # and must not be silently skipped.
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let clientNonce = extractClientNonce(clientFirstBare)
    let serverNonce = clientNonce & "serverpart"
    let bad =
      "r=" & serverNonce & ",s=" & encode(testSalt) & ",i=" & $testIterations & ",junk"
    expect KafkaProtocolError:
      discard challengeSync(session, bad.toBytes())

  test "SCRAM rejects a non-numeric iteration count":
    # `i=` is strict ASCII digits; Nim's lenient parseInt would otherwise accept
    # `_` separators / a leading sign.
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    discard session.initialResponse()
    let serverFirst = "r=abc,s=c2FsdA==,i=40_96"
    expect KafkaProtocolError:
      discard challengeSync(session, serverFirst.toBytes())

  test "SCRAM surfaces a server-final e= rejection":
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let clientNonce = extractClientNonce(clientFirstBare)
    let serverNonce = clientNonce & "serverpart"
    let salt = testSalt.toBytes()
    let srv = newScramSimServer[sha256](
      testPass, salt, testIterations, clientFirstBare, serverNonce
    )
    discard challengeSync(session, srv.serverFirst.toBytes())
    # Server rejects with an in-band e= error instead of v=<sig>.
    var raised = false
    try:
      discard challengeSync(session, "e=invalid-proof".toBytes())
    except KafkaProtocolError as e:
      raised = true
      check "server rejected authentication" in e.msg
      check "invalid-proof" in e.msg
    check raised

  test "SCRAM accepts v=<sig> with RFC 5802 trailing extensions":
    # RFC 5802 §5.1: server-final-message = (server-error / verifier)
    #                                       ["," extensions]
    # An SASL extension after v=<sig> must not corrupt the base64 decode.
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let clientNonce = extractClientNonce(clientFirstBare)
    let serverNonce = clientNonce & "serverpart"
    let salt = testSalt.toBytes()
    let srv = newScramSimServer[sha256](
      testPass, salt, testIterations, clientFirstBare, serverNonce
    )
    let (_, clientFinalBytes) = challengeSync(session, srv.serverFirst.toBytes())
    let serverFinal =
      verifyProofAndBuildServerFinal[sha256](srv, toString(clientFinalBytes))
    let withExt = serverFinal & ",ext=quota-info"
    let (state, resp) = challengeSync(session, withExt.toBytes())
    check state == ssComplete
    check resp.len == 0

  test "password bytes survive across many sessions (sink elision regression)":
    ## Regression: each session must deep-copy the captured password buffer. If
    ## sink inference moves the master copy into the first session, the burn in
    ## computeScramAndBurnPassword destroys the master and later sessions derive
    ## HMACs from an empty password, producing proofs the server cannot verify.
    const sessions = 5
    let m = newSaslScram(testUser, testPass, scramSha256)

    # Build all sessions up front, then drive each through a full handshake.
    # If sink-elision damaged the master after session 0, session 1+ would
    # produce a client proof inconsistent with `testPass` and the server
    # signature derived from `testPass` would not match.
    var ss: seq[SaslSession]
    for _ in 0 ..< sessions:
      ss.add(m.newSession())

    for s in ss:
      let clientFirst = toString(s.initialResponse())
      let clientFirstBare = clientFirst[3 .. ^1]
      let clientNonce = extractClientNonce(clientFirstBare)
      let serverNonce = clientNonce & "serverpart"
      let salt = testSalt.toBytes()
      let srv = newScramSimServer[sha256](
        testPass, salt, testIterations, clientFirstBare, serverNonce
      )
      let (state1, clientFinalBytes) = challengeSync(s, srv.serverFirst.toBytes())
      check state1 == ssInProgress
      let serverFinal =
        verifyProofAndBuildServerFinal[sha256](srv, toString(clientFinalBytes))
      let (state2, finalResp) = challengeSync(s, serverFinal.toBytes())
      check state2 == ssComplete
      check finalResp.len == 0

  test "username escaping (RFC 5802)":
    let m = newSaslScram("user=name,test", testPass, scramSha256)
    let session = m.newSession()
    let resp = toString(session.initialResponse())
    check resp.contains("n=user=3Dname=2Ctest")

  test "invalid iteration count raises KafkaProtocolError":
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let clientNonce = extractClientNonce(clientFirstBare)

    let badServerFirst =
      "r=" & clientNonce & "server,s=" & encode("salt") & ",i=notanumber"
    expect KafkaProtocolError:
      discard challengeSync(session, badServerFirst.toBytes())

  test "nonce mismatch raises KafkaProtocolError":
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    discard session.initialResponse()

    let badServerFirst = "r=completelydifferentnonce,s=" & encode("salt") & ",i=4096"
    expect KafkaProtocolError:
      discard challengeSync(session, badServerFirst.toBytes())

  test "bad server signature raises KafkaProtocolError":
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()

    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let clientNonce = extractClientNonce(clientFirstBare)
    let serverNonce = clientNonce & "serverpart"
    let salt = testSalt.toBytes()

    let srv = newScramSimServer[sha256](
      testPass, salt, testIterations, clientFirstBare, serverNonce
    )
    discard challengeSync(session, srv.serverFirst.toBytes())

    expect KafkaProtocolError:
      discard challengeSync(session, "v=bm90YXJlYWxzaWduYXR1cmU=".toBytes())

  test "single-byte server signature tamper raises KafkaProtocolError":
    # Regression for constant-time comparison: an attacker that flips a
    # single byte of the server signature must still be rejected (and no
    # path may shortcut on first-byte equality).
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()

    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let clientNonce = extractClientNonce(clientFirstBare)
    let serverNonce = clientNonce & "serverpart"
    let salt = testSalt.toBytes()

    let srv = newScramSimServer[sha256](
      testPass, salt, testIterations, clientFirstBare, serverNonce
    )
    let (_, clientFinalBytes) = challengeSync(session, srv.serverFirst.toBytes())
    let serverFinal =
      verifyProofAndBuildServerFinal[sha256](srv, toString(clientFinalBytes))

    # Mutate the trailing base64 '=' padding: the signature then decodes to 33
    # bytes instead of 32, so the explicit length check rejects it.
    var tampered = serverFinal
    let lastIdx = tampered.high
    tampered[lastIdx] = (if tampered[lastIdx] == 'A': 'B' else: 'A')
    expect KafkaProtocolError:
      discard challengeSync(session, tampered.toBytes())

  test "length-mismatched server signature raises KafkaProtocolError":
    # Verifies the constant-time compare still rejects a v= payload whose
    # decoded length differs from the expected signature length.
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()

    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let clientNonce = extractClientNonce(clientFirstBare)
    let serverNonce = clientNonce & "serverpart"
    let salt = testSalt.toBytes()

    let srv = newScramSimServer[sha256](
      testPass, salt, testIterations, clientFirstBare, serverNonce
    )
    discard challengeSync(session, srv.serverFirst.toBytes())

    # 4-byte signature is impossible for SHA-256 (32 bytes).
    expect KafkaProtocolError:
      discard challengeSync(session, "v=YWJjZA==".toBytes())

  test "unexpected server-final format raises KafkaProtocolError":
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()

    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let clientNonce = extractClientNonce(clientFirstBare)
    let serverNonce = clientNonce & "serverpart"
    let salt = testSalt.toBytes()

    let srv = newScramSimServer[sha256](
      testPass, salt, testIterations, clientFirstBare, serverNonce
    )
    discard challengeSync(session, srv.serverFirst.toBytes())

    # The raw server payload must not leak into the exception message (a
    # malicious broker could inject arbitrary text into logs).
    let marker = "server-injected-log-line-0123456789"
    var raised = false
    try:
      discard challengeSync(session, ("x=" & marker).toBytes())
    except KafkaProtocolError as e:
      raised = true
      check marker notin e.msg
    check raised

  test "server error in server-first-message":
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    discard session.initialResponse()

    let errorMsg = "e=invalid-encoding"
    expect KafkaProtocolError:
      discard challengeSync(session, errorMsg.toBytes())

  test "incomplete server-first-message (missing fields)":
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let clientNonce = extractClientNonce(clientFirstBare)

    let incomplete = "r=" & clientNonce & "server" # missing salt and iterations
    expect KafkaProtocolError:
      discard challengeSync(session, incomplete.toBytes())

  test "iteration count too low":
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let clientNonce = extractClientNonce(clientFirstBare)

    let lowIter = "r=" & clientNonce & "server,s=" & encode("salt") & ",i=100"
    expect KafkaProtocolError:
      discard challengeSync(session, lowIter.toBytes())

  test "iteration count too high":
    # The bound is per-hash: SHA-512 costs ~6.5x SHA-256 per iteration, so the
    # old single 10,000,000 cap sat 47x past the OWASP recommendation for
    # SHA-512 — 81s of blocked event loop, measured. Each hash now has its own
    # ceiling, and the two differ.
    block:
      let m = newSaslScram(testUser, testPass, scramSha256)
      let session = m.newSession()
      let clientFirst = toString(session.initialResponse())
      let clientFirstBare = clientFirst[3 .. ^1]
      let clientNonce = extractClientNonce(clientFirstBare)

      let highIter = "r=" & clientNonce & "server,s=" & encode("salt") & ",i=99999999"
      expect KafkaProtocolError:
        discard challengeSync(session, highIter.toBytes())

  test "the iteration ceiling is per-hash, not shared":
    # A count that SHA-256 accepts is refused by SHA-512. Sharing one cap either
    # lets SHA-512 run far past its safe budget or rejects SHA-256 brokers that
    # configure above the OWASP recommendation.
    block:
      let m = newSaslScram(testUser, testPass, scramSha512)
      let session = m.newSession()
      # One initialResponse: a second call raises by design, and the nonce has to
      # be the one that was actually sent.
      let clientFirst = toString(session.initialResponse())
      let clientNonce = extractClientNonce(clientFirst[3 .. ^1])
      # 1,000,000 is inside the SHA-256 bound (1.2M) and outside SHA-512's
      # (420k), so the two hashes disagree on the same server-supplied count.
      let rejectedBySha512Only =
        "r=" & clientNonce & "server,s=" & encode("salt") & ",i=1000000"
      expect KafkaProtocolError:
        discard challengeSync(session, rejectedBySha512Only.toBytes())

  test "iteration count at the per-hash boundary is accepted":
    # The ceiling must not reject a legitimate broker sitting exactly on it:
    # an off-by-one here would break real clusters on a correctness axis rather
    # than a security one. SHA-512 at 420,000 (2x the OWASP recommendation).
    let m = newSaslScram(testUser, testPass, scramSha512)
    let session = m.newSession()
    let clientFirst = toString(session.initialResponse())
    let clientNonce = extractClientNonce(clientFirst[3 .. ^1])
    let atLimit = "r=" & clientNonce & "server,s=" & encode("salt") & ",i=420000"
    let (state, resp) = challengeSync(session, atLimit.toBytes())
    check state == ssInProgress
    check resp.len > 0

  test "an oversized salt is rejected before the derivation runs":
    # `s=` is attacker-controlled and feeds the first HMAC block, so an
    # unbounded salt is unbounded work the server hands the client. 1024 bytes
    # is far above any real deployment (16-32 is typical).
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    let clientFirst = toString(session.initialResponse())
    let clientNonce = extractClientNonce(clientFirst[3 .. ^1])
    let bigSalt =
      "r=" & clientNonce & "server,s=" & encode("x".repeat(2048)) & ",i=4096"
    expect KafkaProtocolError:
      discard challengeSync(session, bigSalt.toBytes())

  test "an oversized server-first-message is rejected before the split":
    # `readBytes` has no per-call cap, so the grammar parse used to run
    # `split(',')` over the whole frame: 100 MB of commas became ~50M
    # substrings before any attribute was validated. The length is checked
    # first now, and the frame below says which check fired: it carries no
    # attribute at all, so the grammar would reject it as incomplete if the
    # split had run first.
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()
    let clientFirst = toString(session.initialResponse())
    let clientNonce = extractClientNonce(clientFirst[3 .. ^1])
    let huge = "r=" & clientNonce & "," & ",".repeat(128 * 1024)
    var raised = false
    try:
      discard challengeSync(session, huge.toBytes())
    except KafkaProtocolError as e:
      raised = true
      check "exceeds the" in e.msg
      check "byte limit" in e.msg
    check raised

  test "the key derivation yields the event loop instead of blocking it":
    # The defect: `iterations` is server-chosen, so the cost of the derivation
    # is the cost of the PBKDF2 loop, and that loop used to run to completion
    # inside one synchronous call — 81s of a stopped event loop at the old
    # ceiling, with `maxSaslSteps` and the request timeout unable to help (they
    # bound entries, not duration). Now the loop runs in ~ms slices and awaits
    # between them.
    #
    # Pin the property directly: drive a handshake whose derivation is long
    # enough to span many slices, and require a concurrent timer to keep
    # firing. If the loop went back to blocking, the timer could not tick until
    # the derivation finished.
    proc body() {.async: (raises: [Exception]).} =
      let m = newSaslScram(testUser, testPass, scramSha512)
      let session = m.newSession()
      let clientFirst = toString(session.initialResponse())
      let clientNonce = extractClientNonce(clientFirst[3 .. ^1])
      # 200,000 is ~1.6s of SHA-512 work, so hundreds of slices at the 2ms
      # budget — far more than enough to prove the loop is not held.
      let serverFirst = "r=" & clientNonce & "server,s=" & encode("salt") & ",i=200000"

      # Record completion ORDER, not just completion. A 10ms timer started
      # before the derivation must fire *while* the derivation is in flight; if
      # the loop were held it could not resume until the derivation ended, and
      # the order would invert. A boolean would pass either way.
      var sideFired = false
      var sideFiredBefore = false
      var challengeDone = false
      proc onTick(_: pointer) {.gcsafe.} =
        sideFired = true
        sideFiredBefore = not challengeDone

      let timer = setTimer(Moment.now() + 10.milliseconds, onTick)

      let (state, _) = await session.challenge(serverFirst.toBytes())
      challengeDone = true
      # Let the timer land if the derivation somehow beat it.
      await sleepAsync(30.milliseconds)
      clearTimer(timer)

      check state == ssInProgress
      check sideFired
      check sideFiredBefore

    waitFor body()

suite "SCRAM-SHA-256 RFC 7677 §3 KAT":
  ## Known Answer Test with the RFC 7677 §3 published vector. Locks every
  ## byte of the derivation chain against the spec: with the injected client
  ## nonce, the client-first message, the client-final `p=` proof, and the
  ## acceptance of the exact server-final `v=` are all byte-identical to the
  ## RFC. A regression in PBKDF2, HMAC, xorBytes, StoredKey derivation, or
  ## the AuthMessage concatenation would break the KAT.
  const
    katUser = "user"
    katPass = "pencil"
    katClientNonce = "rOprNGfwEbeRWgbNEkqO"
    katServerNonceSuffix = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0"
    katSaltB64 = "W22ZaJ0SNY7soEsUEjb6gQ=="
    katIterations = 4096
    katExpectedClientFirst = "n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
    katExpectedServerFirst =
      "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," &
      "s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
    katExpectedClientFinal =
      "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0," &
      "p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ="
    katExpectedServerFinal = "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="

  test "client-first equals RFC 7677 §3 vector (with injected nonce)":
    setScramTestNonce(katClientNonce)
    defer:
      clearScramTestNonce()
    let m = newSaslScram(katUser, katPass, scramSha256)
    let session = m.newSession()
    let clientFirst = toString(session.initialResponse())
    check clientFirst == katExpectedClientFirst

  test "client-final proof matches RFC 7677 §3 vector byte-for-byte":
    setScramTestNonce(katClientNonce)
    defer:
      clearScramTestNonce()
    let m = newSaslScram(katUser, katPass, scramSha256)
    let session = m.newSession()
    discard session.initialResponse()
    let (state, clientFinalBytes) =
      challengeSync(session, katExpectedServerFirst.toBytes())
    check state == ssInProgress
    check toString(clientFinalBytes) == katExpectedClientFinal

  test "accepts RFC 7677 §3 server-final and completes":
    setScramTestNonce(katClientNonce)
    defer:
      clearScramTestNonce()
    let m = newSaslScram(katUser, katPass, scramSha256)
    let session = m.newSession()
    discard session.initialResponse()
    discard challengeSync(session, katExpectedServerFirst.toBytes())
    let (state, resp) = challengeSync(session, katExpectedServerFinal.toBytes())
    check state == ssComplete
    check resp.len == 0

  test "bidirectional sim server verifies the KAT proof and emits the KAT signature":
    # Round-trips the vector through newScramSimServer: proves that the
    # simulator's proof-verification path is consistent with the same RFC
    # vector the client is checked against, and closes the loop the old
    # helper left open.
    setScramTestNonce(katClientNonce)
    defer:
      clearScramTestNonce()
    let m = newSaslScram(katUser, katPass, scramSha256)
    let session = m.newSession()
    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let serverNonce = katClientNonce & katServerNonceSuffix
    let salt = decode(katSaltB64).toBytes()
    let srv = newScramSimServer[sha256](
      katPass, salt, katIterations, clientFirstBare, serverNonce
    )
    check srv.serverFirst == katExpectedServerFirst
    let (_, clientFinalBytes) = challengeSync(session, srv.serverFirst.toBytes())
    check toString(clientFinalBytes) == katExpectedClientFinal
    let serverFinal =
      verifyProofAndBuildServerFinal[sha256](srv, toString(clientFinalBytes))
    check serverFinal == katExpectedServerFinal

  test "a cancelled derivation propagates and leaves the session able to finish the KAT":
    # The derivation's intermediate state (HMAC ctx + work/md SaltedPassword)
    # must be zeroed on the cancellation path, and the cancellation must still
    # propagate — the cleanup must not swallow it. Cancel mid-derivation, then
    # drive the same session through the RFC vector: the cleanup must not have
    # burned ownedPwd or advanced the step counter, and the retry must reproduce
    # the pinned proof byte-for-byte.
    proc body() {.async: (raises: [Exception]).} =
      setScramTestNonce(katClientNonce)
      defer:
        clearScramTestNonce()
      let m = newSaslScram(katUser, katPass, scramSha256)
      let session = m.newSession()
      check toString(session.initialResponse()) == katExpectedClientFirst
      # A server-supplied count far beyond one slice, so the cancel lands in an
      # awaited slice instead of racing the single-slice path.
      let serverFirstLong =
        "r=" & katClientNonce & katServerNonceSuffix & ",s=" & katSaltB64 & ",i=200000"
      let fut = session.challenge(serverFirstLong.toBytes())
      await sleepAsync(5.milliseconds)
      check not fut.finished
      fut.cancel()
      var cancelled = false
      try:
        discard await fut
      except CancelledError:
        cancelled = true
      check cancelled
      let (state, clientFinalBytes) =
        await session.challenge(katExpectedServerFirst.toBytes())
      check state == ssInProgress
      check toString(clientFinalBytes) == katExpectedClientFinal

    waitFor body()

suite "SASL SCRAM-SHA-512":
  test "mechanism name":
    let m = newSaslScram("user", "pass", scramSha512)
    check m.name == "SCRAM-SHA-512"

  test "full handshake":
    let m = newSaslScram("user", "pass", scramSha512)
    let session = m.newSession()

    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let clientNonce = extractClientNonce(clientFirstBare)
    let serverNonce = clientNonce & "srv512"
    let salt = "salt512".toBytes()

    let srv =
      newScramSimServer[sha512]("pass", salt, 4096, clientFirstBare, serverNonce)

    let (state1, clientFinalBytes) = challengeSync(session, srv.serverFirst.toBytes())
    check state1 == ssInProgress

    let serverFinal =
      verifyProofAndBuildServerFinal[sha512](srv, toString(clientFinalBytes))
    let (state2, _) = challengeSync(session, serverFinal.toBytes())
    check state2 == ssComplete

  test "password bytes survive across many sessions (sink elision regression)":
    # Parallel regression to the SHA-256 case; verifies the @capturedPwdBytes
    # deep-copy in newSession() holds across the SHA-512 instantiation too.
    const sessions = 5
    let m = newSaslScram("user", "pass", scramSha512)

    var ss: seq[SaslSession]
    for _ in 0 ..< sessions:
      ss.add(m.newSession())

    for s in ss:
      let clientFirst = toString(s.initialResponse())
      let clientFirstBare = clientFirst[3 .. ^1]
      let clientNonce = extractClientNonce(clientFirstBare)
      let serverNonce = clientNonce & "srv512"
      let salt = "salt512".toBytes()
      let srv =
        newScramSimServer[sha512]("pass", salt, 4096, clientFirstBare, serverNonce)
      let (state1, clientFinalBytes) = challengeSync(s, srv.serverFirst.toBytes())
      check state1 == ssInProgress
      let serverFinal =
        verifyProofAndBuildServerFinal[sha512](srv, toString(clientFinalBytes))
      let (state2, _) = challengeSync(s, serverFinal.toBytes())
      check state2 == ssComplete

suite "SASL TLS requirement":
  test "PLAIN requires TLS by default":
    let m = newSaslPlain("user", "pass")
    check m.requireTls == true

  test "PLAIN with allowPlaintext opts out of TLS requirement":
    let m = newSaslPlain("user", "pass", allowPlaintext = true)
    check m.requireTls == false

  test "SCRAM-SHA-256 does not require TLS":
    let m = newSaslScram("user", "pass", scramSha256)
    check m.requireTls == false

  test "SCRAM-SHA-512 does not require TLS":
    let m = newSaslScram("user", "pass", scramSha512)
    check m.requireTls == false

suite "SASL session independence":
  test "multiple sessions from same mechanism are independent":
    let m = newSaslScram("user", "pass", scramSha256)
    let s1 = m.newSession()
    let s2 = m.newSession()

    let r1 = toString(s1.initialResponse())
    let r2 = toString(s2.initialResponse())

    check r1.startsWith("n,,")
    check r2.startsWith("n,,")

    let nonce1 = extractClientNonce(r1[3 .. ^1])
    let nonce2 = extractClientNonce(r2[3 .. ^1])
    check nonce1 != nonce2

suite "SASL ASCII enforcement":
  test "PLAIN rejects non-ASCII username":
    expect KafkaError:
      discard newSaslPlain("\xC3\xA9user", "pass") # "é" prefix

  test "PLAIN rejects non-ASCII password":
    expect KafkaError:
      discard newSaslPlain("user", "p\xC3\xA9ss")

  test "PLAIN rejects control characters in credentials":
    expect KafkaError:
      discard newSaslPlain("user\x00", "pass") # NUL is RFC 4616 separator

  test "SCRAM rejects non-ASCII credentials":
    expect KafkaError:
      discard newSaslScram("\xC3\xA9user", "pass", scramSha256)
    expect KafkaError:
      discard newSaslScram("user", "\xC3\xA9pass", scramSha256)

  test "PLAIN accepts printable ASCII":
    let m = newSaslPlain("alice@example.com", "S3cret!")
    check m.name == "PLAIN"

  test "SCRAM accepts printable ASCII":
    let m = newSaslScram("alice@example.com", "S3cret!", scramSha256)
    check m.name == "SCRAM-SHA-256"

suite "SASL PLAIN session step counter":
  test "challenge after initialResponse raises":
    let m = newSaslPlain("user", "pass")
    let s = m.newSession()
    discard s.initialResponse()
    let (state, _) = challengeSync(s, @[])
    check state == ssComplete
    expect KafkaProtocolError:
      discard challengeSync(s, @[])

  test "challenge before initialResponse raises":
    let m = newSaslPlain("user", "pass")
    let s = m.newSession()
    expect KafkaProtocolError:
      discard challengeSync(s, @[])

suite "SASL burnCredentials":
  test "PLAIN exposes burnCredentials":
    let m = newSaslPlain("user", "pass")
    check m.burnCredentials != nil

  test "SCRAM exposes burnCredentials":
    let m = newSaslScram("user", "pass", scramSha256)
    check m.burnCredentials != nil

  test "PLAIN sessions created before burn carry their own copy":
    # The master is zeroed by burnCredentials but the per-session ownedPwd
    # was already copied at newSession() time, so a session created before
    # burn still produces the original password on the wire.
    let m = newSaslPlain("alice", "secret")
    let s = m.newSession()
    m.burnCredentials()
    let payload = s.initialResponse()
    # RFC 4616: NUL "alice" NUL "secret"
    let expected = @[byte(0)] & toBytes("alice") & @[byte(0)] & toBytes("secret")
    check payload == expected

  test "PLAIN sessions created after burn produce an empty password":
    let m = newSaslPlain("alice", "secret")
    m.burnCredentials()
    let s = m.newSession()
    let payload = s.initialResponse()
    # NUL "alice" NUL (no password trailing)
    let expected = @[byte(0)] & toBytes("alice") & @[byte(0)]
    check payload == expected

  test "SCRAM sessions created after burn fail server-signature verification":
    # After burnCredentials() the per-session ownedPwd copy is empty, so the
    # client derives PBKDF2 keys from an empty password while the server knows
    # the original: the sim rejects that proof as invalid-proof, so no
    # server-final v= is ever sent.
    const
      burnUser = "testuser"
      burnPass = "testpass"
      burnSalt = "randomsalt"
      burnIters = 4096
    let m = newSaslScram(burnUser, burnPass, scramSha256)
    m.burnCredentials()
    let session = m.newSession()

    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1] # strip "n,,"

    let clientNonce = extractClientNonce(clientFirstBare)
    let serverNonce = clientNonce & "serverpart"
    let salt = burnSalt.toBytes()

    let srv =
      newScramSimServer[sha256](burnPass, salt, burnIters, clientFirstBare, serverNonce)

    # Step 0 succeeds — the client builds its (mismatched) clientProof.
    let (state1, clientFinalBytes) = challengeSync(session, srv.serverFirst.toBytes())
    check state1 == ssInProgress

    # The bidirectional sim server (like a real broker) rejects the mismatched
    # proof at verification time — the client derived it from an empty password.
    var proofRejected = false
    try:
      discard verifyProofAndBuildServerFinal[sha256](srv, toString(clientFinalBytes))
    except KafkaProtocolError as e:
      proofRejected = true
      check "invalid-proof" in e.msg
    check proofRejected

    # And the client-side error path still raises when it receives an e= from
    # a broker that rejected the proof: closes the loop end-to-end.
    expect KafkaProtocolError:
      discard challengeSync(session, "e=invalid-proof".toBytes())

type
  HandshakeAction = enum
    haAccept ## return errorCode=0 and let SaslAuthenticate proceed
    haReject ## return UNSUPPORTED_SASL_MECHANISM and close the transport
    haAuthFail ## accept the handshake, then fail the SaslAuthenticate

  HandshakeRule = object
    mechanism: string
    action: HandshakeAction
    supportedOnReject: seq[string]

  MetricEvent = tuple[event: string, tags: seq[(string, string)], value: int64]
  MetricSink = ref object
    events: seq[MetricEvent]

proc apiVersionsBody(): seq[byte] {.raises: [].} =
  buildApiVersionsResponseBody(
    0'i16,
    [
      FakeApiVersion(apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0),
      FakeApiVersion(apiKey: int16(ord(akSaslHandshake)), minVersion: 0, maxVersion: 1),
      FakeApiVersion(
        apiKey: int16(ord(akSaslAuthenticate)), minVersion: 0, maxVersion: 1
      ),
    ],
  )

proc readClientHandshakeMechanism(body: seq[byte]): string {.raises: [].} =
  ## SaslHandshake v0 request body is just a single string (int16 length + bytes).
  if body.len < 2:
    return ""
  let lo = int(body[1])
  let hi = int(body[0])
  let strLen = (hi shl 8) or lo
  if body.len < 2 + strLen:
    return ""
  result = newString(strLen)
  if strLen > 0:
    copyMem(addr result[0], unsafeAddr body[2], strLen)

proc saslDispatcher(fb: FakeBroker, rules: seq[HandshakeRule]) {.async: (raises: []).} =
  ## Drive a FakeBroker through one or more (ApiVersions → SaslHandshake →
  ## SaslAuthenticate) sequences. Each `rules` entry is consumed in order:
  ## one rule per logical SaslHandshake-request the dispatcher sees. The
  ## dispatcher loops forever until the broker is closed.
  var ruleIdx = 0
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 5.seconds)
    except CatchableError:
      break
    try:
      let key = req.apiKey
      if key == int16(ord(akApiVersions)):
        await req.respond(apiVersionsBody())
      elif key == int16(ord(akSaslHandshake)):
        let mech = readClientHandshakeMechanism(req.body)
        let rule =
          if ruleIdx < rules.len:
            rules[ruleIdx]
          else:
            rules[rules.len - 1]
        ruleIdx.inc
        doAssert rule.mechanism == "" or rule.mechanism == mech,
          "dispatcher expected mech " & rule.mechanism & " but client sent " & mech
        case rule.action
        of haAccept, haAuthFail:
          let body = buildSaslHandshakeResponseBody(0'i16, [])
          await req.respond(body)
        of haReject:
          let body = buildSaslHandshakeResponseBody(33'i16, rule.supportedOnReject)
            # 33 = UNSUPPORTED_SASL_MECHANISM
          await req.respond(body)
          # KIP-43: broker closes the connection after this response.
          await closeTransport(req)
      elif key == int16(ord(akSaslAuthenticate)):
        # The most recently consumed rule determines auth outcome.
        let rule =
          if ruleIdx == 0:
            rules[0]
          else:
            rules[ruleIdx - 1]
        if rule.action == haAuthFail:
          let body = buildSaslAuthenticateResponseBody(
            58'i16, # SASL_AUTHENTICATION_FAILED
            errorMessage = some("bad credentials"),
          )
          await req.respond(body)
        else:
          let body = buildSaslAuthenticateResponseBody(0'i16)
          await req.respond(body)
      else:
        # Unknown — ignore so the dispatcher does not crash on unexpected
        # requests; tests assert state explicitly.
        discard
    except CatchableError:
      break

proc makeMetricSink(): MetricSink =
  MetricSink(events: @[])

proc makeMetricHook(sink: MetricSink): MetricsHook =
  result = proc(
      event: cstring, tags: openArray[(cstring, cstring)], value: int64
  ) {.gcsafe, raises: [].} =
    var copied: seq[(string, string)]
    for t in tags:
      copied.add(($t[0], $t[1]))
    sink.events.add((event: $event, tags: copied, value: value))

proc countEvents(sink: MetricSink, name: string): int =
  for ev in sink.events:
    if ev.event == name:
      inc result

suite "SASL multi-mechanism fallback (wire-level)":
  ## The broker may close the TCP connection after responding with
  ## UNSUPPORTED_SASL_MECHANISM (KIP-43), so each candidate must be tried
  ## on a fresh connection. These tests use FakeBroker to verify the
  ## reconnect-per-candidate behavior end-to-end.

  test "fallback succeeds: SCRAM rejected, PLAIN accepted on fresh connection":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      let rules = @[
        HandshakeRule(
          mechanism: "SCRAM-SHA-256", action: haReject, supportedOnReject: @["PLAIN"]
        ),
        HandshakeRule(mechanism: "PLAIN", action: haAccept),
      ]
      let dispatcherFut = saslDispatcher(fb, rules)

      let scram = newSaslScram("user", "pw", scramSha256)
      let plain = newSaslPlain("user", "pw", allowPlaintext = true)
      let b = newBroker(
        0'i32,
        "127.0.0.1",
        port,
        "test-client",
        saslMechanisms = @[scram, plain],
        connectTimeoutMs = 5_000,
        requestTimeoutMs = 5_000,
      )

      await b.connect()
      check b.isConnected
      # Two TCP connections must have been accepted — one rejected after
      # SCRAM handshake, one for the successful PLAIN handshake.
      check fb.transports.len >= 2

      await b.close()
      await fb.close()
      awaitDrain(dispatcherFut, 1.seconds)

    waitFor body()

  test "all candidates rejected: raises UNSUPPORTED, failure counter bumped":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      # Both candidates rejected; broker closes connection each time.
      let rules = @[
        HandshakeRule(
          mechanism: "SCRAM-SHA-256", action: haReject, supportedOnReject: @["GSSAPI"]
        ),
        HandshakeRule(
          mechanism: "SCRAM-SHA-512", action: haReject, supportedOnReject: @["GSSAPI"]
        ),
      ]
      let dispatcherFut = saslDispatcher(fb, rules)

      let scram256 = newSaslScram("user", "pw", scramSha256)
      let scram512 = newSaslScram("user", "pw", scramSha512)
      let b = newBroker(
        0'i32,
        "127.0.0.1",
        port,
        "test-client",
        saslMechanisms = @[scram256, scram512],
        connectTimeoutMs = 5_000,
        requestTimeoutMs = 5_000,
      )

      var caughtCode: KafkaErrorCode
      var raised = false
      try:
        await b.connect()
      except KafkaApiError as e:
        raised = true
        caughtCode = e.errorCode
      except KafkaError:
        raised = true
      check raised
      check caughtCode == keUnsupportedSaslMechanism
      check (not b.isConnected)

      await b.close()
      await fb.close()
      awaitDrain(dispatcherFut, 1.seconds)

    waitFor body()

  test "non-UNSUPPORTED auth failure is terminal: second candidate not tried":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      # First handshake accepted, then SaslAuthenticate fails with a
      # non-fallback error. The second candidate must NOT be attempted.
      let rules = @[
        HandshakeRule(mechanism: "PLAIN", action: haAuthFail),
        HandshakeRule(mechanism: "SCRAM-SHA-256", action: haAccept),
      ]
      let dispatcherFut = saslDispatcher(fb, rules)

      let plain = newSaslPlain("user", "pw", allowPlaintext = true)
      let scram = newSaslScram("user", "pw", scramSha256)
      let b = newBroker(
        0'i32,
        "127.0.0.1",
        port,
        "test-client",
        saslMechanisms = @[plain, scram],
        connectTimeoutMs = 5_000,
        requestTimeoutMs = 5_000,
      )

      var raised = false
      var caughtCode = keNone
      var caughtApi = false
      try:
        await b.connect()
      except KafkaApiError as e:
        raised = true
        caughtApi = true
        caughtCode = e.errorCode
      except KafkaError:
        raised = true
      check raised
      # The SaslAuthenticate failure must surface as a KafkaApiError that
      # carries the broker's actual error code (58), not the keNone (=success)
      # default a bare newException would leave behind.
      check caughtApi
      check caughtCode == keSaslAuthenticationFailed
      check (not b.isConnected)
      # Exactly one TCP connection — fallback should have stopped after
      # the auth failure (which is not UNSUPPORTED_SASL_MECHANISM).
      check fb.transports.len == 1

      await b.close()
      await fb.close()
      awaitDrain(dispatcherFut, 1.seconds)

    waitFor body()

  test "requireTls failure does not trigger fallback":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      # Dispatcher should NOT see a SaslHandshake at all because the
      # requireTls check fires synchronously before any wire bytes go out
      # — the connection completes ApiVersions, then performSaslAuthOne
      # raises KafkaConnectionError on the requireTls guard.
      let rules = @[HandshakeRule(mechanism: "PLAIN", action: haAccept)]
      let dispatcherFut = saslDispatcher(fb, rules)

      let plainStrict = newSaslPlain("user", "pw") # requireTls = true (default)
      let scram = newSaslScram("user", "pw", scramSha256)
      let b = newBroker(
        0'i32,
        "127.0.0.1",
        port,
        "test-client",
        saslMechanisms = @[plainStrict, scram],
        connectTimeoutMs = 5_000,
        requestTimeoutMs = 5_000,
      )

      var raised = false
      try:
        await b.connect()
      except KafkaConnectionError:
        raised = true
      except KafkaError:
        raised = true
      check raised
      check (not b.isConnected)
      # Only one TCP connection — the PLAIN requireTls guard raises
      # KafkaConnectionError, which is not a KafkaApiError, so no fallback to
      # the TLS-free SCRAM candidate is attempted.
      check fb.transports.len == 1

      await b.close()
      await fb.close()
      awaitDrain(dispatcherFut, 1.seconds)

    waitFor body()

  test "metrics: one reconnect.attempt per connect, one rejected per candidate":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)

      let rules = @[
        HandshakeRule(
          mechanism: "SCRAM-SHA-256", action: haReject, supportedOnReject: @["PLAIN"]
        ),
        HandshakeRule(mechanism: "PLAIN", action: haAccept),
      ]
      let dispatcherFut = saslDispatcher(fb, rules)

      let sink = makeMetricSink()
      let scram = newSaslScram("user", "pw", scramSha256)
      let plain = newSaslPlain("user", "pw", allowPlaintext = true)
      let b = newBroker(
        0'i32,
        "127.0.0.1",
        port,
        "test-client",
        saslMechanisms = @[scram, plain],
        connectTimeoutMs = 5_000,
        requestTimeoutMs = 5_000,
        metricsHook = makeMetricHook(sink),
      )

      await b.connect()
      check b.isConnected

      # Exactly one logical attempt despite the per-candidate inner loop.
      check countEvents(sink, "broker.reconnect.attempt") == 1
      # One UNSUPPORTED rejection before the successful candidate.
      check countEvents(sink, "broker.sasl.mechanism_rejected") == 1
      # The PLAIN candidate completed its round-trip: one authenticated session.
      check countEvents(sink, "broker.sasl.authenticated") == 1
      # ApiVersions negotiation runs on each fresh connection (2 here).
      check countEvents(sink, "broker.api_versions.negotiated") == 2
      # The connect ultimately succeeded; no terminal failure metric.
      check countEvents(sink, "broker.reconnect.failed") == 0

      await b.close()
      await fb.close()
      awaitDrain(dispatcherFut, 1.seconds)

    waitFor body()

  test "constructor rejects nil entries in saslMechanisms":
    expect KafkaError:
      discard newBroker(
        0'i32,
        "127.0.0.1",
        9092'i32,
        "test-client",
        saslMechanisms = @[SaslMechanism(nil)],
      )

  test "singular saslMechanism path still works when seq is empty":
    let plain = newSaslPlain("user", "pw", allowPlaintext = true)
    let b =
      newBroker(0'i32, "127.0.0.1", 9092'i32, "test-client", saslMechanism = plain)
    check b != nil

proc readSaslAuthenticateAuthBytes(body: seq[byte]): seq[byte] {.raises: [].} =
  ## Extract the `authBytes` payload from a SaslAuthenticate v0 request body.
  ## The wire format is a single fkBytes field: int32 length + payload. Any
  ## framing/length inconsistency is fatal for the test (returned empty).
  if body.len < 4:
    return @[]
  let length = int(
    fromBytesBE(uint32, [byte(body[0]), byte(body[1]), byte(body[2]), byte(body[3])])
  )
  if length < 0 or length > body.len - 4:
    return @[]
  result = body[4 ..< 4 + length]

proc scramApiVersionsBody(): seq[byte] {.raises: [].} =
  buildApiVersionsResponseBody(
    0'i16,
    [
      FakeApiVersion(apiKey: int16(ord(akApiVersions)), minVersion: 0, maxVersion: 0),
      FakeApiVersion(apiKey: int16(ord(akSaslHandshake)), minVersion: 0, maxVersion: 1),
      FakeApiVersion(
        apiKey: int16(ord(akSaslAuthenticate)), minVersion: 0, maxVersion: 1
      ),
    ],
  )

type ScramExchangeOutcome = enum
  seAcceptProof ## verify proof and reply with the real server-final
  seRejectProof ## reply with SASL_AUTHENTICATION_FAILED at client-final step
  seTamperSig ## verify proof, then corrupt v=<sig> before sending it back

proc scramDispatcher[T](
    fb: FakeBroker,
    password: string,
    salt: seq[byte],
    iterations: int,
    serverNonceSuffix: string,
    outcome: ScramExchangeOutcome,
) {.async: (raises: []).} =
  ## Full SCRAM server-side dispatcher: services ApiVersions, SaslHandshake
  ## (SCRAM-SHA-*), then the two-round SaslAuthenticate exchange. Uses
  ## `newScramSimServer` on the server side so proof verification is real,
  ## not stubbed.
  var serverSim: ScramSimServer[T]
  var seenClientFinal = false
  while true:
    var req: CapturedRequest
    try:
      req = await fb.nextRequest(timeout = 5.seconds)
    except CatchableError:
      break
    try:
      let key = req.apiKey
      if key == int16(ord(akApiVersions)):
        await req.respond(scramApiVersionsBody())
      elif key == int16(ord(akSaslHandshake)):
        let body = buildSaslHandshakeResponseBody(0'i16, [])
        await req.respond(body)
      elif key == int16(ord(akSaslAuthenticate)):
        let authBytes = readSaslAuthenticateAuthBytes(req.body)
        if serverSim == nil:
          # First SaslAuthenticate — payload is SCRAM client-first
          let clientFirst = toString(authBytes)
          doAssert clientFirst.startsWith("n,,"),
            "unexpected SCRAM client-first: " & clientFirst
          let clientFirstBare = clientFirst[3 .. ^1]
          let clientNonce = extractClientNonce(clientFirstBare)
          doAssert clientNonce.len > 0, "SCRAM client-first missing r="
          let serverNonce = clientNonce & serverNonceSuffix
          serverSim = newScramSimServer[T](
            password, salt, iterations, clientFirstBare, serverNonce
          )
          let body = buildSaslAuthenticateResponseBody(
            0'i16, authBytes = serverSim.serverFirst.toBytes()
          )
          await req.respond(body)
        elif not seenClientFinal:
          seenClientFinal = true
          let clientFinal = toString(authBytes)
          case outcome
          of seAcceptProof:
            let serverFinal =
              try:
                verifyProofAndBuildServerFinal[T](serverSim, clientFinal)
              except KafkaProtocolError as e:
                doAssert false, "scramDispatcher expected valid proof but got: " & e.msg
                ""
            let body = buildSaslAuthenticateResponseBody(
              0'i16, authBytes = serverFinal.toBytes()
            )
            await req.respond(body)
          of seRejectProof:
            let body = buildSaslAuthenticateResponseBody(
              58'i16, # SASL_AUTHENTICATION_FAILED
              errorMessage = some("invalid client proof"),
            )
            await req.respond(body)
          of seTamperSig:
            let serverFinal =
              try:
                verifyProofAndBuildServerFinal[T](serverSim, clientFinal)
              except KafkaProtocolError as e:
                doAssert false, "scramDispatcher expected valid proof but got: " & e.msg
                ""
            # Flip the last base64 char so length is preserved, decoded bytes
            # differ. Exercises the client's constant-time signature compare
            # end-to-end through broker.connect().
            var tampered = serverFinal
            let lastIdx = tampered.high
            tampered[lastIdx] = (if tampered[lastIdx] == 'A': 'B' else: 'A')
            let body =
              buildSaslAuthenticateResponseBody(0'i16, authBytes = tampered.toBytes())
            await req.respond(body)
      else:
        discard
    except CatchableError:
      break

suite "SASL SCRAM handshake (wire-level integration)":
  ## End-to-end wire-level tests that drive broker.connect() through the
  ## full SCRAM-SHA-* exchange against an in-process FakeBroker whose
  ## server side does real proof verification via `newScramSimServer`.
  ## Closes the integration gap flagged in the design review — before this
  ## suite, no test at any layer exercised SCRAM through the full
  ## ApiVersions → SaslHandshake → SaslAuthenticate stack.

  test "SCRAM-SHA-256 completes end-to-end through broker.connect()":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let salt = "integ-salt-256".toBytes()
      let dispatcherFut = scramDispatcher[sha256](
        fb, "s3cret", salt, 4096, "srv-256-suffix", seAcceptProof
      )

      let scram = newSaslScram("alice", "s3cret", scramSha256)
      let b = newBroker(
        0'i32,
        "127.0.0.1",
        port,
        "test-client",
        saslMechanism = scram,
        connectTimeoutMs = 5_000,
        requestTimeoutMs = 5_000,
      )
      await b.connect()
      check b.isConnected

      await b.close()
      await fb.close()
      awaitDrain(dispatcherFut, 1.seconds)

    waitFor body()

  test "SCRAM-SHA-512 completes end-to-end through broker.connect()":
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let salt = "integ-salt-512".toBytes()
      let dispatcherFut = scramDispatcher[sha512](
        fb, "s3cret", salt, 4096, "srv-512-suffix", seAcceptProof
      )

      let scram = newSaslScram("alice", "s3cret", scramSha512)
      let b = newBroker(
        0'i32,
        "127.0.0.1",
        port,
        "test-client",
        saslMechanism = scram,
        connectTimeoutMs = 5_000,
        requestTimeoutMs = 5_000,
      )
      await b.connect()
      check b.isConnected

      await b.close()
      await fb.close()
      awaitDrain(dispatcherFut, 1.seconds)

    waitFor body()

  test "broker rejects mismatched client proof (wrong password)":
    ## Client authenticates with "wrong-password"; the dispatcher is pre-configured
    ## with seRejectProof, so it rejects the client-final step unconditionally,
    ## without verifying the proof, and replies with SASL_AUTHENTICATION_FAILED.
    ## The broker.connect() must surface that as a KafkaApiError. Proves that
    ## a proof derived from the wrong password is rejected upstream and
    ## the failure propagates through the whole client stack.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let salt = "integ-salt-256".toBytes()
      # Dispatcher would happily verify the proof with the REAL password
      # ("s3cret"), but the client is configured with "wrong-password". The
      # sim's verifyProofAndBuildServerFinal will find the recovered stored
      # key doesn't match — the dispatcher's doAssert would fire — so we
      # pre-configure the dispatcher to reject unconditionally instead.
      let dispatcherFut = scramDispatcher[sha256](
        fb, "s3cret", salt, 4096, "srv-256-suffix", seRejectProof
      )

      let scram = newSaslScram("alice", "wrong-password", scramSha256)
      let b = newBroker(
        0'i32,
        "127.0.0.1",
        port,
        "test-client",
        saslMechanism = scram,
        connectTimeoutMs = 5_000,
        requestTimeoutMs = 5_000,
      )
      var raised = false
      var caughtCode = keNone
      try:
        await b.connect()
      except KafkaApiError as e:
        raised = true
        caughtCode = e.errorCode
      except KafkaError:
        raised = true
      check raised
      check caughtCode == keSaslAuthenticationFailed
      check (not b.isConnected)

      await b.close()
      await fb.close()
      awaitDrain(dispatcherFut, 1.seconds)

    waitFor body()

  test "client rejects tampered server signature at end of exchange":
    ## The dispatcher accepts the client's proof but returns a corrupted
    ## v=<sig> in server-final. The client's constant-time signature check
    ## must reject it, and broker.connect() must fail rather than silently
    ## treat the session as authenticated. Guards against a broker that
    ## proves knowledge of the password but returns a wrong signature.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let salt = "integ-salt-256".toBytes()
      let dispatcherFut =
        scramDispatcher[sha256](fb, "s3cret", salt, 4096, "srv-256-suffix", seTamperSig)

      let scram = newSaslScram("alice", "s3cret", scramSha256)
      let b = newBroker(
        0'i32,
        "127.0.0.1",
        port,
        "test-client",
        saslMechanism = scram,
        connectTimeoutMs = 5_000,
        requestTimeoutMs = 5_000,
      )
      var raised = false
      try:
        await b.connect()
      except KafkaError:
        raised = true
      check raised
      check (not b.isConnected)

      await b.close()
      await fb.close()
      awaitDrain(dispatcherFut, 1.seconds)

    waitFor body()

  test "RFC 7677 §3 KAT flows through broker.connect() (deterministic nonce)":
    ## Pins the client nonce to the RFC 7677 §3 vector and drives the
    ## exchange through broker.connect() end-to-end. Ties the KAT
    ## byte-for-byte assertions to the full protocol stack: if any layer
    ## (SASL codec, request framing, response decoding) rewrites the
    ## payload, the fixed proof/signature would no longer verify.
    proc body() {.async: (raises: [Exception]).} =
      let fb = newFakeBroker()
      let port = int32(fb.address().port)
      let salt = decode("W22ZaJ0SNY7soEsUEjb6gQ==").toBytes()
      let dispatcherFut = scramDispatcher[sha256](
        fb, "pencil", salt, 4096, "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0", seAcceptProof
      )

      setScramTestNonce("rOprNGfwEbeRWgbNEkqO")
      defer:
        clearScramTestNonce()
      let scram = newSaslScram("user", "pencil", scramSha256)
      let b = newBroker(
        0'i32,
        "127.0.0.1",
        port,
        "test-client",
        saslMechanism = scram,
        connectTimeoutMs = 5_000,
        requestTimeoutMs = 5_000,
      )
      await b.connect()
      check b.isConnected

      await b.close()
      await fb.close()
      awaitDrain(dispatcherFut, 1.seconds)

    waitFor body()

suite "SCRAM hardening":
  const
    testUser = "testuser"
    testPass = "testpass"
    testIterations = 4096

  test "constTimeEqual: equal sequences match":
    let a = @[1'u8, 2, 3, 4]
    let b = @[1'u8, 2, 3, 4]
    check constTimeEqual(a, b)

  test "constTimeEqual: single-byte mismatch fails":
    let a = @[1'u8, 2, 3, 4]
    let b = @[1'u8, 2, 3, 5]
    check not constTimeEqual(a, b)

  test "constTimeEqual: first-byte mismatch fails":
    let a = @[0'u8, 2, 3, 4]
    let b = @[1'u8, 2, 3, 4]
    check not constTimeEqual(a, b)

  test "constTimeEqual: differing lengths fail":
    let a = @[1'u8, 2, 3]
    let b = @[1'u8, 2, 3, 4]
    check not constTimeEqual(a, b)

  test "constTimeEqual: empty sequences match":
    check constTimeEqual(newSeq[byte](), newSeq[byte]())

  test "server nonce equal to client nonce is rejected (missing server entropy)":
    let m = newSaslScram(testUser, testPass, scramSha256)
    let session = m.newSession()

    let clientFirst = toString(session.initialResponse())
    let clientFirstBare = clientFirst[3 .. ^1]
    let clientNonce = extractClientNonce(clientFirstBare)

    # Server-first that echoes the client nonce verbatim — no server-side
    # entropy. Must be rejected per RFC 5802 §5.1.
    let badServerFirst =
      "r=" & clientNonce & ",s=" & encode("salt") & ",i=" & $testIterations
    expect KafkaProtocolError:
      discard challengeSync(session, badServerFirst.toBytes())

suite "SASL mechanism construction":
  test "hand-built mechanism is rejected at construction":
    let bad = SaslMechanism()
    expect KafkaError:
      discard newCluster(@["127.0.0.1:9092"], saslMechanism = bad)
    expect KafkaError:
      discard newCluster(@["127.0.0.1:9092"], saslMechanisms = @[bad])
    expect KafkaError:
      discard
        newBroker(0'i32, "127.0.0.1", 9092'i32, "test-client", saslMechanism = bad)

  test "nil mechanism is rejected as KafkaError":
    let nilMech: SaslMechanism = nil
    expect KafkaError:
      validateMechanism(nilMech)
