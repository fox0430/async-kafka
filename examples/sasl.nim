## Authenticate with SASL.
##
## SCRAM-SHA-512 is used here over a SASL_PLAINTEXT listener: SCRAM never puts
## the password on the wire, only a proof of it, so a passive listener learns
## nothing. SCRAM has no channel binding, so an active man in the middle can
## still relay the exchange; verified TLS is what rules that out.
##
## PLAIN sends the password in the clear and is therefore refused unless the
## connection is TLS with verification on; `newSaslPlain(user, password,
## allowPlaintext = true)` opts out for a local or trusted network, and a private
## CA is supplied by building the trust anchors from its PEM.
##
## `newCluster(saslMechanisms = @[...])` offers several mechanisms in order,
## retrying the next candidate on a fresh connection only when the broker answers
## UNSUPPORTED_SASL_MECHANISM; any other rejection fails the connect. The order
## matters: with its default `tlsRequired`, PLAIN is refused before any auth
## bytes reach a plaintext listener, and that refusal is not a switch-candidate
## case, so PLAIN listed first would never reach SCRAM here.
##
## Prerequisites: a SASL listener and a user on it. The defaults match the SCRAM
## user the local broker is given before the integration suites run.
##
## Run from the repository root:
##
##   nim c -r examples/sasl.nim

import std/[os, sequtils, strutils]

import pkg/chronos
import pkg/async_kafka

proc main() {.async.} =
  let brokers = getEnv("KAFKA_BROKERS", "localhost:9094")
    .split(',')
    .mapIt(it.strip())
    .filterIt(it.len > 0)
  let user = getEnv("KAFKA_SASL_USERNAME", "scram-user")
  let password = getEnv("KAFKA_SASL_PASSWORD", "scram-secret")

  let mechanism = newSaslScram(user, password, scramSha512)
  let cluster = newCluster(brokers, saslMechanism = mechanism)

  # The SASL exchange runs on connect, so a completed metadata exchange means
  # the broker accepted the credentials.
  await cluster.refreshMetadata()
  echo "authenticated as ", user, " with ", mechanism.name

  await cluster.close()

try:
  waitFor main()
except KafkaError as e:
  echo "kafka error: ", e.msg
  quit(QuitFailure)
