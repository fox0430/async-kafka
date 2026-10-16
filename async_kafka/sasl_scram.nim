import std/[base64, strutils, sysrand, importutils]

# Selective import: the whole module would shadow chronos's `Duration` /
# `seconds` / `nanosecond` with std/times' incompatible `TimeInterval` versions.
from std/times import epochTime

# `pbkdf2` is deliberately not imported: `deriveSaltedPassword` below resumes the
# same loop in slices, pinned against the RFC 7677 KAT.
import nimcrypto/[hmac, hash, sha2, utils]

import chronos

import sasl, errors

privateAccess(SaslMechanism)

type ScramHash* = enum
  ## SCRAM hash function: the wire mechanism name and the PBKDF2 hash. Server
  ## iteration bounds are enforced per hash.
  scramSha256 = "SCRAM-SHA-256"
  scramSha512 = "SCRAM-SHA-512"

const
  minScramIterations = 4096
    ## RFC 5802's floor, and the Kafka broker default; lower is a config error.

  maxScramIterationsSha256 = 1_200_000
  maxScramIterationsSha512 = 420_000
    ## Per-hash upper bounds on the server-supplied `i=` value, at twice the
    ## OWASP-recommended PBKDF2 counts (SHA-256 600k, SHA-512 210k). One shared
    ## cap is wrong: SHA-512 costs ~6.5x SHA-256 per iteration, so these bound the
    ## worst case at ~8s / ~1s, yielded in slices (nimcrypto 0.7.3, one core).

  scramSaltMaxLen = 1024
    ## RFC 5802 §5.1 does not bound the salt (real deployments use 16-32 bytes);
    ## a cap keeps a hostile server from forcing a large first HMAC block.

  scramServerFirstMaxLen = 64 * 1024
    ## Upper bound on the server-first-message, orders of magnitude above any real
    ## `r=`/`s=`/`i=` value. Checked before parsing, so it also bounds the
    ## `split(',')` amplification.

  pbkdf2SliceBudgetMs = 2
    ## Wall-clock budget for one uninterrupted run of PBKDF2 iterations; the
    ## derivation awaits between slices. A few ms keeps the stall unobservable
    ## while the `await` overhead stays negligible against the work.

proc maxIterationsFor(hash: ScramHash): int {.raises: [].} =
  ## The per-hash ceiling on the server-supplied `i=` value.
  case hash
  of scramSha256: maxScramIterationsSha256
  of scramSha512: maxScramIterationsSha512

func constTimeEqual*(a, b: openArray[byte]): bool {.inline.} =
  equalMemFull(a, b)

proc toBytes(s: string): seq[byte] =
  @(s.toOpenArrayByte(0, s.high))

proc toString(data: seq[byte]): string =
  result = newString(data.len)
  if data.len > 0:
    copyMem(addr result[0], unsafeAddr data[0], data.len)

proc scramEscapeUsername(username: string): string =
  ## RFC 5802 Section 5.1: '=' -> '=3D', ',' -> '=2C'
  result = username.replace("=", "=3D").replace(",", "=2C")

proc generateNonce(): string {.raises: [KafkaConnectionError].} =
  var buf: array[24, byte]
  if not urandom(buf):
    raise newException(KafkaConnectionError, "failed to generate random nonce")
  result = encode(buf)

when defined(kafkaScramTestHooks):
  # Compile-time-gated deterministic client nonce for KAT tests (RFC 5802 §5 /
  # RFC 7677 §3); absent without the -d flag, so production cannot pin it.
  var testNonceOverride {.threadvar.}: string

  proc setScramTestNonce*(nonce: string) =
    ## Pin the client nonce for the next `initialResponse()` call; "" resets it.
    testNonceOverride = nonce

  proc clearScramTestNonce*() =
    testNonceOverride = ""

proc xorBytes(a, b: seq[byte]): seq[byte] {.raises: [KafkaProtocolError].} =
  if a.len != b.len:
    raise newException(
      KafkaProtocolError, "SCRAM: xorBytes length mismatch: " & $a.len & " vs " & $b.len
    )
  result = newSeq[byte](a.len)
  for i in 0 ..< a.len:
    result[i] = a[i] xor b[i]

proc deriveSaltedPassword[T](
    password, salt: seq[byte], iterations: int, outLen: int
): Future[seq[byte]] {.async: (raises: [CancelledError]).} =
  ## PBKDF2-HMAC (RFC 8018) for the SCRAM case, yielding the event loop between
  ## slices of the iteration loop.
  ##
  ## `iterations` is server-supplied, so the peer chooses the cost, and
  ## `maxSaslSteps` / the request timeout do not help — they bound how often this
  ## is entered, not how long one entry runs. Each iteration depends only on the
  ## previous digest, so the loop runs in slices of a few milliseconds and awaits
  ## between them; a thread would spend the same wall-clock on one derivation.
  ##
  ## The output matches nimcrypto's `pbkdf2` byte for byte (pinned by the RFC 7677
  ## KAT). One output block suffices: SCRAM asks for the
  ## digest length, which never exceeds the block size.
  var
    ctx: HMAC[T]
    work: array[int(ctx.sizeDigest), byte]
    md: array[int(ctx.sizeDigest), byte]
    counter: array[4, byte]
  beStore32(counter, 0, 1'u32)
  ctx.init(password)
  ctx.update(salt)
  ctx.update(counter)
  discard ctx.finish(md)
  work = md

  try:
    # U1 is done. The rest runs in slices, reading the clock once per slice.
    var completed = 1
    while completed < iterations:
      let sliceStart = epochTime()
      while completed < iterations and
          (epochTime() - sliceStart) * 1000.0 < pbkdf2SliceBudgetMs.float:
        ctx.init(password)
        ctx.update(md)
        discard ctx.finish(md)
        for k in 0 ..< work.len:
          work[k] = work[k] xor md[k]
        inc completed
      if completed < iterations:
        # Hand the loop back (other connections, timers, shutdown) and provide the
        # cancellation point: a cancel here aborts the remaining slices.
        await sleepAsync(0.seconds)

    # Password-equivalent material; the caller burns it once the proof is derived.
    result = @work[0 ..< outLen]
  finally:
    # Zero every intermediate on every exit, including the cancellation above:
    # `ctx` holds the password-keyed ipad/opad and work/md the SaltedPassword.
    # The returned seq is a copy taken before this runs.
    ctx.clear()
    burnMem(addr work[0], work.len)
    burnMem(addr md[0], md.len)

proc computeScramAndBurnPassword[T](
    password: var seq[byte],
    saltedPassword: seq[byte],
    clientFirstBare, serverFirst, clientFinalNoProof: string,
): (seq[byte], seq[byte]) {.raises: [KafkaError].} =
  ## Returns (clientProof, serverSignature), burning `password` and the derived
  ## saltedPassword so credentials do not linger. The derivation is passed in
  ## rather than done here: it suspends, and a `var` parameter cannot be captured
  ## across an await.
  burnBytes(password)

  var hmacCtx: HMAC[T]

  hmacCtx.init(saltedPassword)
  hmacCtx.update("Client Key")
  var clientKey = hmacCtx.finish().data
  var storedKey = T.digest(clientKey).data

  var authMessage = clientFirstBare & "," & serverFirst & "," & clientFinalNoProof

  hmacCtx.init(storedKey)
  hmacCtx.update(authMessage)
  var clientSignature = hmacCtx.finish().data
  let clientProof = xorBytes(@clientKey, @clientSignature)

  hmacCtx.init(saltedPassword)
  hmacCtx.update("Server Key")
  var serverKey = hmacCtx.finish().data

  hmacCtx.init(serverKey)
  hmacCtx.update(authMessage)
  let serverSignature = hmacCtx.finish().data

  # Wipe every password-equivalent derivation so a later memory dump cannot
  # recover them. clientProof and serverSignature stay: the first goes on the
  # wire, the second is checked against server-final.
  hmacCtx.clear()
  if saltedPassword.len > 0:
    burnMem(addr saltedPassword[0], saltedPassword.len)
  burnMem(addr clientKey[0], clientKey.len)
  burnMem(addr storedKey[0], storedKey.len)
  burnMem(addr serverKey[0], serverKey.len)
  burnMem(addr clientSignature[0], clientSignature.len)
  if authMessage.len > 0:
    burnMem(addr authMessage[0], authMessage.len)
  result = (clientProof, @serverSignature)

proc newSaslScram*(
    username, password: string, hash: ScramHash
): SaslMechanism {.raises: [KafkaError].} =
  ## SASL/SCRAM-SHA-256 or -512. Credentials must be printable ASCII (SASLprep is
  ## not implemented); each session zeroes its private password copy after PBKDF2,
  ## and callers should wipe their own copy too.
  ##
  ## The mechanism holds a master password copy so `newSession()` can run once per
  ## broker connection; `burnCredentials()` zeros it (disabling future auth).
  ##
  ## RFC 5929 channel binding (`tls-server-end-point`) is not implemented, so the
  ## exchange cannot detect a TLS-layer MITM on its own: run it over verified TLS.
  validateAscii(username, "username")
  validateAscii(password, "password")
  let capturedUser = username
  # Bytes, so per-session copies can be zeroed in place regardless of string
  # buffer ownership.
  var capturedPwdBytes = newSeq[byte](password.len)
  for i in 0 ..< password.len:
    capturedPwdBytes[i] = byte(password[i])
  SaslMechanism(
    mechanismName: $hash,
    # SCRAM hashes the credentials on the wire, so TLS is not required (though
    # still recommended for the rest of the traffic).
    tlsRequired: false,
    burnCredentials: proc() {.raises: [].} =
      burnBytes(capturedPwdBytes),
    newSession: proc(): SaslSession {.raises: [].} =
      var clientNonce: string
      var clientFirstBare: string
      # Per-session password copy, zeroed by computeScramAndBurnPassword. The `@`
      # forces a fresh seq: ORC sink-elision could otherwise move the master copy
      # in, and the later burn would destroy credentials for future sessions.
      var ownedPwd = @capturedPwdBytes
      # Expected server signature (raw HMAC bytes) computed in step 0 — NOT the
      # RFC 5802 AuthMessage (the signed string). The decoded step-1 "v=" payload
      # is compared against it constant-time.
      var expectedServerSig: seq[byte]
      var step = 0
      # Separate from `step`, which only `challenge` advances.
      var initialResponseSent = false

      SaslSession(
        initialResponse: proc(): seq[byte] {.raises: [KafkaError].} =
          # Regenerating the nonce would invalidate the AuthMessage already sent.
          if initialResponseSent:
            raise
              newException(KafkaProtocolError, "SCRAM: initialResponse called twice")
          when defined(kafkaScramTestHooks):
            clientNonce =
              if testNonceOverride.len > 0:
                testNonceOverride
              else:
                generateNonce()
          else:
            clientNonce = generateNonce()
          let escaped = scramEscapeUsername(capturedUser)
          clientFirstBare = "n=" & escaped & ",r=" & clientNonce
          let clientFirstMessage = "n,," & clientFirstBare
          result = clientFirstMessage.toBytes()
          initialResponseSent = true,
        dispose: proc() {.raises: [].} =
          # Burn what auth aborted before its normal burn points: ownedPwd (step 0
          # never ran) and expectedServerSig (failure between step 0 and 1).
          burnBytes(ownedPwd)
          burnBytes(expectedServerSig),
        challenge: proc(
            data: seq[byte]
        ): Future[(SaslState, seq[byte])] {.
            gcsafe, async: (raises: [KafkaError, CancelledError])
        .} =
          let msg = data.toString()

          if step == 0:
            # clientNonce / clientFirstBare are still empty here, so PBKDF2 would
            # sign a garbage AuthMessage.
            if not initialResponseSent:
              raise newException(
                KafkaProtocolError, "SCRAM: challenge received before initialResponse"
              )
            # Bound before parsing: `split` allocates one substring per comma, so
            # a frame the connection accepts becomes that many substrings before
            # any grammar check could reject it.
            if msg.len > scramServerFirstMaxLen:
              raise newException(
                KafkaProtocolError,
                "SCRAM: server-first-message " & $msg.len & " bytes exceeds the " &
                  $scramServerFirstMaxLen & " byte limit",
              )
            # Parse server-first-message: [reserved-mext ","] r=<nonce>,s=<salt>,
            # i=<iterations> [,extensions].
            let serverFirst = msg
            var serverNonce, saltB64: string
            var iterations: int
            var seenR, seenS, seenI: bool

            for part in serverFirst.split(','):
              if part.startsWith("m="):
                # RFC 5802 §5.1 / §7: `m=` carries a mandatory extension this
                # client does not support; abort rather than downgrade.
                raise newException(
                  KafkaProtocolError,
                  "SCRAM: server requires an unsupported mandatory extension",
                )
              elif part.startsWith("e="):
                # RFC 5802 §5.1: `e=` belongs to server-final-message, not
                # server-first.
                raise newException(
                  KafkaProtocolError,
                  "SCRAM: e= is not permitted in server-first-message",
                )
              elif part.startsWith("r="):
                if seenR:
                  raise newException(
                    KafkaProtocolError, "SCRAM: duplicate r= in server-first-message"
                  )
                seenR = true
                serverNonce = part[2 .. ^1]
              elif part.startsWith("s="):
                if seenS:
                  raise newException(
                    KafkaProtocolError, "SCRAM: duplicate s= in server-first-message"
                  )
                seenS = true
                saltB64 = part[2 .. ^1]
              elif part.startsWith("i="):
                if seenI:
                  raise newException(
                    KafkaProtocolError, "SCRAM: duplicate i= in server-first-message"
                  )
                seenI = true
                # RFC 5802 `i=` is strict ASCII digits; parseInt is lenient (signs,
                # `_` separators), so validate the charset first.
                let iterStr = part[2 .. ^1]
                if iterStr.len == 0 or not iterStr.allCharsInSet({'0' .. '9'}):
                  raise newException(
                    KafkaProtocolError,
                    "SCRAM: non-numeric iteration count in server-first-message",
                  )
                try:
                  iterations = parseInt(iterStr)
                except ValueError:
                  raise newException(
                    KafkaProtocolError,
                    "SCRAM: invalid iteration count in server-first-message",
                  )
              else:
                # The extensions tail is ignored, but its attr-val shape is still
                # validated so garbage is not silently skipped.
                let eqIdx = part.find('=')
                if eqIdx <= 0:
                  raise newException(
                    KafkaProtocolError,
                    "SCRAM: malformed attribute in server-first-message",
                  )

            if serverNonce.len == 0 or saltB64.len == 0 or iterations == 0:
              raise newException(
                KafkaProtocolError, "SCRAM: incomplete server-first-message"
              )
            if iterations < minScramIterations:
              raise newException(
                KafkaProtocolError, "SCRAM: iteration count too low: " & $iterations
              )
            # Per-hash cap: SHA-512 costs ~6.5x SHA-256 per iteration (see the
            # constants above).
            let maxIterations = maxIterationsFor(hash)
            if iterations > maxIterations:
              raise newException(
                KafkaProtocolError,
                "SCRAM: iteration count " & $iterations & " exceeds the " & $hash &
                  " limit of " & $maxIterations,
              )

            if not serverNonce.startsWith(clientNonce):
              raise newException(
                KafkaProtocolError,
                "SCRAM: server nonce does not start with client nonce",
              )

            if serverNonce.len <= clientNonce.len:
              # RFC 5802 §5.1 requires the server to append its own entropy; an
              # echo of the client nonce removes server-side randomness.
              raise newException(
                KafkaProtocolError, "SCRAM: server nonce missing server-side entropy"
              )

            let salt =
              try:
                decode(saltB64).toBytes()
              except ValueError:
                raise newException(KafkaProtocolError, "SCRAM: invalid base64 salt")
            # The salt feeds the first HMAC block, so it is work the server hands
            # us; checked decoded, since a base64 field inflates by ~4/3.
            if salt.len > scramSaltMaxLen:
              raise newException(
                KafkaProtocolError,
                "SCRAM: salt " & $salt.len & " bytes exceeds the " & $scramSaltMaxLen &
                  " byte limit",
              )
            let clientFinalNoProof = "c=biws,r=" & serverNonce

            # The derivation is awaited in slices; the proof work after it is plain
            # CPU and stays here.
            var clientProof, serverSignature: seq[byte]
            case hash
            of scramSha256:
              let saltedPassword = await deriveSaltedPassword[sha256](
                ownedPwd, salt, iterations, sha256.sizeDigest
              )
              (clientProof, serverSignature) = computeScramAndBurnPassword[sha256](
                ownedPwd, saltedPassword, clientFirstBare, serverFirst,
                clientFinalNoProof,
              )
            of scramSha512:
              let saltedPassword = await deriveSaltedPassword[sha512](
                ownedPwd, salt, iterations, sha512.sizeDigest
              )
              (clientProof, serverSignature) = computeScramAndBurnPassword[sha512](
                ownedPwd, saltedPassword, clientFirstBare, serverFirst,
                clientFinalNoProof,
              )

            expectedServerSig = serverSignature # for step-1 verification
            step = 1

            let clientFinal = clientFinalNoProof & ",p=" & encode(clientProof)
            result = (ssInProgress, clientFinal.toBytes())
          elif step == 1:
            # Burn the sensitive expectedServerSig on every exit path.
            try:
              # server-final-message is v=<signature> or e=<error> (RFC 5802
              # §5.1); a trailing ",extensions" is allowed, so split at the first
              # ',' before decoding.
              let commaIdx = msg.find(',')
              let firstAttr =
                if commaIdx < 0:
                  msg
                else:
                  msg[0 ..< commaIdx]

              if firstAttr.startsWith("e="):
                raise newException(
                  KafkaProtocolError,
                  "SCRAM: server rejected authentication: " & firstAttr[2 .. ^1],
                )
              if not firstAttr.startsWith("v="):
                # Never embed the raw server payload (log-injection vector).
                raise newException(
                  KafkaProtocolError, "SCRAM: unexpected server-final-message"
                )

              let serverSigB64 = firstAttr[2 .. ^1]
              # Decode before comparing so base64 representation differences do not
              # cause spurious mismatches; the compare is constant-time.
              let serverSigBytes =
                try:
                  decode(serverSigB64).toBytes()
                except ValueError:
                  raise newException(
                    KafkaProtocolError, "SCRAM: server signature is not valid base64"
                  )
              # Explicit length check for the clearer error; the compare would also
              # report a mismatch.
              if serverSigBytes.len != expectedServerSig.len:
                raise newException(
                  KafkaProtocolError, "SCRAM: server signature length mismatch"
                )
              if not constTimeEqual(serverSigBytes, expectedServerSig):
                raise newException(
                  KafkaProtocolError, "SCRAM: server signature verification failed"
                )

              # Advance before returning, or a re-drive after ssComplete would match
              # the burned (empty) expectedServerSig against an empty "v=".
              step = 2
              result = (ssComplete, @[])
            finally:
              burnBytes(expectedServerSig)
          else:
            # Post-ssComplete re-entry; mirrors the PLAIN mechanism's step guard.
            raise newException(
              KafkaProtocolError, "SCRAM: unexpected challenge in step " & $step
            ),
      ),
  )
