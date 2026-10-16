## Trust anchors for `TlsConfig`, built from PEM-encoded CA certificates.
##
## BearSSL reads an anchor's DN and public key through raw pointers that
## `TrustAnchorStore.new` only shallow-copies, so the buffers behind them are copied
## into module-level storage that is intentionally never released: a store built here
## stays valid for the rest of the process, which a start-up CA bundle needs.

import std/[base64, locks, strutils]

import bearssl/[x509, rsa, ec]
import chronos/streams/tlsstream

import errors

export tlsstream.TrustAnchorStore

var
  anchorStorage: seq[seq[byte]]
    ## Backing for every pointer inside a returned anchor. Entries are never freed:
    ## the TLS engine reads through them for as long as any store built here is in
    ## use, and `TrustAnchorStore` has nowhere to own them itself.
  anchorLock: Lock
    ## Guards `anchorStorage`; the `{.cast(gcsafe).}` in `retain` is what makes the
    ## module's procs GC-safe to call from an async context.

initLock(anchorLock)

proc retain(buf: openArray[byte]): ptr byte {.raises: [].} =
  ## Copy `buf` into process-lifetime storage and return a stable address.
  if buf.len == 0:
    return nil
  var copy = newSeq[byte](buf.len)
  copyMem(addr copy[0], unsafeAddr buf[0], buf.len)
  # The cast asserts that the access below is safe; the lock serializes it, and the
  # analysis cannot see through the global seq it guards.
  {.cast(gcsafe).}:
    withLock anchorLock:
      anchorStorage.add(copy)
      result = addr anchorStorage[^1][0]

proc decoderInit(
  ctx: var X509DecoderContext,
  appendDn: proc(ctx: pointer, buf: ConstPointer, len: csize_t) {.
    cdecl, gcsafe, noSideEffect, raises: []
  .},
  appendDnCtx: pointer,
) {.
  importc: "br_x509_decoder_init", header: "bearssl_x509.h", cdecl, gcsafe, raises: []
.}
  ## Bound directly: nim-bearssl 0.2.13 declares this callback's buffer as `void *`
  ## while the C header says `const void *`, and gcc 14 rejects the mismatch; going
  ## through the header keeps the `const`.

proc appendDn(
    ctx: pointer, buf: ConstPointer, len: csize_t
) {.cdecl, gcsafe, noSideEffect, raises: [].} =
  ## BearSSL hands the subject DN over in chunks; accumulate them.
  if len == 0 or len > csize_t(high(int)):
    return
  let dn = cast[ptr seq[byte]](ctx)
  let src = cast[ptr UncheckedArray[byte]](buf)
  let base = dn[].len
  dn[].setLen(base + int(len))
  copyMem(addr dn[][base], unsafeAddr src[0], int(len))

proc anchorFromDer(der: openArray[byte]): X509TrustAnchor {.raises: [KafkaError].} =
  if der.len == 0:
    raise newException(KafkaError, "TLS: empty certificate in PEM data")

  var dn: seq[byte]
  var decoder: X509DecoderContext
  decoderInit(decoder, appendDn, addr dn)
  x509DecoderPush(decoder, unsafeAddr der[0], csize_t(der.len))

  let err = x509DecoderLastError(decoder)
  if err != 0:
    raise newException(
      KafkaError,
      "TLS: malformed certificate in PEM data (x509 error " & $int(err) & ")",
    )

  let pkey = x509DecoderGetPkey(decoder)
  if pkey.isNil:
    raise newException(KafkaError, "TLS: certificate in PEM data has no public key")
  if dn.len == 0:
    raise newException(KafkaError, "TLS: certificate in PEM data has an empty subject")

  case pkey.keyType
  of byte(KEYTYPE_RSA):
    let rsa = pkey.key.rsa
    if rsa.nlen == 0 or rsa.elen == 0:
      raise
        newException(KafkaError, "TLS: certificate in PEM data has an empty RSA key")
    var n = newSeq[byte](int(rsa.nlen))
    copyMem(addr n[0], rsa.n, n.len)
    var e = newSeq[byte](int(rsa.elen))
    copyMem(addr e[0], rsa.e, e.len)
    result.pkey.key.rsa = RsaPublicKey(
      n: retain(n), nlen: csize_t(n.len), e: retain(e), elen: csize_t(e.len)
    )
  of byte(KEYTYPE_EC):
    let ec = pkey.key.ec
    if ec.qlen == 0:
      raise newException(KafkaError, "TLS: certificate in PEM data has an empty EC key")
    var q = newSeq[byte](int(ec.qlen))
    copyMem(addr q[0], ec.q, q.len)
    result.pkey.key.ec =
      EcPublicKey(curve: ec.curve, q: retain(q), qlen: csize_t(q.len))
  else:
    raise newException(
      KafkaError,
      "TLS: certificate in PEM data uses unsupported key type " & $pkey.keyType,
    )

  # Retained only once this certificate's checks have passed: process-lifetime
  # storage must not collect the bytes of a certificate that was rejected.
  result.dn = X500Name(data: retain(dn), len: csize_t(dn.len))
  result.flags =
    if x509DecoderIsCA(decoder) != 0:
      cuint(X509_TA_CA)
    else:
      0'u32
  result.pkey.keyType = pkey.keyType

const
  CertBegin = "-----BEGIN CERTIFICATE-----"
  CertEnd = "-----END CERTIFICATE-----"

proc certificateDers(pem: string): seq[seq[byte]] {.raises: [KafkaError].} =
  ## DER of every `CERTIFICATE` block. `TRUSTED CERTIFICATE` blocks (OpenSSL
  ## `-trustout`) carry trust settings BearSSL cannot honour, so they are not read
  ## as if the settings were absent.
  var pos = 0
  while true:
    let start = pem.find(CertBegin, pos)
    if start < 0:
      break
    let stop = pem.find(CertEnd, start + CertBegin.len)
    if stop < 0:
      raise newException(KafkaError, "TLS: unterminated CERTIFICATE block in PEM data")
    var body = ""
    for c in pem[start + CertBegin.len ..< stop]:
      if c notin {' ', '\t', '\r', '\n'}:
        body.add(c)
    try:
      let der = base64.decode(body)
      var bytes = newSeq[byte](der.len)
      if der.len > 0:
        copyMem(addr bytes[0], unsafeAddr der[0], der.len)
      result.add(bytes)
    except ValueError as e:
      raise newException(
        KafkaError, "TLS: invalid base64 in PEM CERTIFICATE block: " & e.msg
      )
    pos = stop + CertEnd.len

  if result.len == 0:
    raise newException(KafkaError, "TLS: PEM data contains no CERTIFICATE block")

proc newTrustAnchorStoreFromPem*(
    pem: string
): TrustAnchorStore {.raises: [KafkaError].} =
  ## Build a `TrustAnchorStore` from PEM-encoded CA certificates, for
  ## `newTlsConfig(trustAnchors = ...)`.
  ##
  ## Every `CERTIFICATE` block is read; one that is not a decodable certificate, or
  ## that carries an unsupported key type, raises `KafkaError` rather than being
  ## skipped, so a truncated bundle cannot silently weaken verification. The bytes
  ## are retained for the process lifetime (see the module doc), so build the store
  ## once at start-up and share it across connections and TLS sessions. A `CA:FALSE`
  ## certificate is used as a direct trust anchor, so pinning a leaf is legitimate;
  ## a `CA:TRUE` one becomes a CA anchor.
  var anchors: seq[X509TrustAnchor]
  for der in certificateDers(pem):
    anchors.add(anchorFromDer(der))
  TrustAnchorStore.new(anchors)
