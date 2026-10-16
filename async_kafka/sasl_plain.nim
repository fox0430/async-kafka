import std/importutils

import chronos

import sasl, errors

privateAccess(SaslMechanism)

proc newSaslPlain*(
    username, password: string, allowPlaintext: bool = false
): SaslMechanism {.raises: [KafkaError].} =
  ## SASL PLAIN (RFC 4616), which sends the credentials unhashed: the broker layer
  ## refuses it unless the connection is TLS with skipVerify off, or
  ## `allowPlaintext = true` opts out (local testing / trusted network).
  ## Credentials must be printable ASCII (SASLprep is not implemented). The
  ## mechanism holds a master password copy so `newSession()` works per
  ## connection; each session burns its own copy after use, and
  ## `mech.burnCredentials()` zeros the master (disabling future auth).
  validateAscii(username, "username")
  validateAscii(password, "password")

  let capturedUser = username
  # Bytes, so the per-session copy can be zeroed in place regardless of string
  # buffer ownership.
  var capturedPwdBytes = newSeq[byte](password.len)
  for i in 0 ..< password.len:
    capturedPwdBytes[i] = byte(password[i])

  SaslMechanism(
    mechanismName: "PLAIN",
    tlsRequired: not allowPlaintext,
    burnCredentials: proc() {.raises: [].} =
      burnBytes(capturedPwdBytes),
    newSession: proc(): SaslSession {.raises: [].} =
      # `@capturedPwdBytes` forces a fresh seq: ORC's sink inference can move the
      # captured buffer instead of copying it, and burnBytes below would then zero
      # the master copy, leaving later connections with an empty password.
      var ownedPwd = @capturedPwdBytes
      var step = 0

      SaslSession(
        initialResponse: proc(): seq[byte] {.raises: [KafkaError].} =
          # A second call would build an empty-password payload (ownedPwd is burned).
          if step != 0:
            raise newException(
              KafkaProtocolError, "PLAIN: initialResponse called in step " & $step
            )
          # RFC 4616: [authzid] NUL authcid NUL passwd
          var buf: seq[byte]
          buf.add(0x00) # authzid
          for c in capturedUser:
            buf.add(byte(c))
          buf.add(0x00)
          buf.add(ownedPwd)
          burnBytes(ownedPwd)
          step = 1
          result = buf,
        dispose: proc() {.raises: [].} =
          # Burns ownedPwd if initialResponse never ran; idempotent.
          burnBytes(ownedPwd),
        challenge: proc(
            data: seq[byte]
        ): Future[(SaslState, seq[byte])] {.
            gcsafe, async: (raises: [KafkaError, CancelledError])
        .} =
          # PLAIN completes in one round-trip; reject a re-drive of the session.
          if step != 1:
            raise newException(
              KafkaProtocolError, "PLAIN: unexpected challenge in step " & $step
            )
          step = 2
          return (ssComplete, @[]),
      ),
  )
