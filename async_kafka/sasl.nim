import std/strutils

import pkg/nimcrypto/utils

import chronos

import errors

type
  SaslState* = enum
    ssInitial
    ssInProgress
    ssComplete

  SaslSession* = ref object
    initialResponse*: proc(): seq[byte] {.gcsafe, raises: [KafkaError].}
    challenge*: proc(data: seq[byte]): Future[(SaslState, seq[byte])] {.
      gcsafe, async: (raises: [KafkaError, CancelledError])
    .}
      ## Async because a server-chosen value drives the cost: SCRAM's `i=`
      ## iteration count decides the key-derivation CPU, so the implementation
      ## yields between slices and a cancellation aborts it promptly.
    dispose*: proc() {.gcsafe, raises: [].}
      ## Optional best-effort zero of per-session secrets, invoked on every auth
      ## exit path so a credential is not left behind when the handshake fails
      ## early. Idempotent; `nil` means nothing to dispose.

  SaslMechanism* = ref object
    ## SASL mechanism: name, session factory, and credential hygiene. Build with
    ## `newSaslPlain` / `newSaslScram`.
    mechanismName: string
      ## Private and fixed at construction; read it through `name`. A writable name
      ## would let the value announced in the handshake drift from the mechanism the
      ## session implements, and from what `requireTls` reports.
    newSession: proc(): SaslSession {.gcsafe, raises: [].}
      ## Private. Built-in mechanisms and the broker layer are trusted siblings;
      ## applications get mechanisms from the public constructors.
    tlsRequired: bool
      ## Private, read-only through `requireTls`, and fixed at construction;
      ## PLAIN's opt-out is `allowPlaintext = true` on `newSaslPlain`.
    burnCredentials*: proc() {.gcsafe, raises: [].}
      ## Optional. Zeros the master credential copy, after which `newSession()`
      ## authenticates with an empty password and fails — use only when no
      ## further sessions are needed. Sessions created before the call keep their
      ## own copy. `nil` means burning is unsupported.

proc name*(m: SaslMechanism): string {.inline, raises: [].} =
  ## The mechanism announced in the SASL handshake, fixed at construction.
  m.mechanismName

proc requireTls*(m: SaslMechanism): bool {.raises: [].} =
  ## When true, the broker layer refuses this mechanism over a non-TLS connection
  ## (or TLS with skipVerify=true) to avoid leaking credentials. PLAIN defaults to
  ## true, SCRAM to false; the value is fixed at construction.
  m.tlsRequired

proc validateAscii*(s: string, fieldName: string) {.raises: [KafkaError].} =
  ## Reject credentials that are empty or contain anything outside printable ASCII
  ## (0x20..0x7E); SASLprep (RFC 4013) is not implemented, so ambiguous Unicode
  ## normalisation cannot change the wire bytes. Empty fails locally because some
  ## misconfigured brokers accept an empty PLAIN password. The failure index is a
  ## byte offset, not a code point.
  if s.len == 0:
    raise newException(KafkaError, "SASL: " & fieldName & " must not be empty")

  for i, c in s:
    let b = byte(c)
    if b < 0x20 or b > 0x7E:
      raise newException(
        KafkaError,
        "SASL: " & fieldName & " contains non-ASCII or control byte 0x" & toHex(b, 2) &
          " at byte index " & $i &
          " (SASLprep is not implemented; only printable ASCII is accepted)",
      )

proc burnString*(s: var string) {.raises: [].} =
  ## Best-effort secure erase of a SASL credential. Nim strings are GC-managed,
  ## so a copy may already exist; callers should wipe their own copies too.
  if s.len > 0:
    burnMem(addr s[0], s.len)
  s.setLen(0)

proc burnBytes*(b: var seq[byte]) {.raises: [].} =
  ## Zero a byte sequence in place; same caveats as `burnString`.
  if b.len > 0:
    burnMem(addr b[0], b.len)
  b.setLen(0)
