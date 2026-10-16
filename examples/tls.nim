## Connect to a TLS listener, verifying the broker against a private CA.
##
## `newTlsConfig()` with no arguments verifies against the CA bundle compiled
## into the TLS backend, which covers the public CAs only; a private or
## self-signed CA is supplied as PEM-encoded certificates instead, and the trust
## anchors are built from them here. `skipVerify = true` turns verification off
## for a dev or throwaway setup; never in production.
##
## BearSSL matches the broker's advertised `host` against the certificate's
## `dNSName` SANs only, so a listener advertising an IP literal cannot complete
## verification even when the certificate carries an `iPAddress` SAN for it.
##
## Prerequisites: a TLS listener, the PEM of the CA its certificate chains to,
## and a certificate valid for the advertised host name. The defaults target the
## local development broker, whose TLS listener is SASL_SSL and offers PLAIN
## only, so a mechanism is configured as well; a TLS-only listener needs none.
##
## Run from the repository root, once the local broker has generated its CA:
##
##   nim c -r examples/tls.nim

import std/[os, sequtils, strutils]

import pkg/chronos
import pkg/async_kafka

proc main() {.async.} =
  let brokers = getEnv("KAFKA_BROKERS", "localhost:9095")
    .split(',')
    .mapIt(it.strip())
    .filterIt(it.len > 0)
  let caPath = getEnv("KAFKA_CA_CERT", "docker/certs/ca.crt")
  let user = getEnv("KAFKA_SASL_USERNAME", "client")
  let password = getEnv("KAFKA_SASL_PASSWORD", "client-secret")

  # Check first: `readFile` on a missing path raises IOError, which is not a
  # KafkaError and would escape the handler below as a traceback.
  if not fileExists(caPath):
    raise newException(
      KafkaError,
      "missing " & caPath & "; point KAFKA_CA_CERT at the CA the listener chains to",
    )
  let anchors = newTrustAnchorStoreFromPem(readFile(caPath))
  let cluster = newCluster(
    brokers,
    tlsConfig = newTlsConfig(trustAnchors = anchors),
    saslMechanism = newSaslPlain(user, password),
  )

  # A completed metadata exchange means the handshake, the certificate
  # verification, and the SASL exchange all passed.
  await cluster.refreshMetadata()
  echo "verified TLS connection to ", brokers.join(", ")

  await cluster.close()

try:
  waitFor main()
except KafkaError as e:
  echo "kafka error: ", e.msg
  quit(QuitFailure)
