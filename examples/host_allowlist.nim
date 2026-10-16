## Bound which broker addresses the client will dial.
##
## A metadata response can redirect the client to any address the peer can
## reach, and under TLS that peer can present a certificate valid *for the
## address it chose*, so TLS cannot bound that on its own. The allowlist is a
## cluster-level policy, checked wherever a broker is dialed, before the socket
## is opened.
##
## Rules come from `allowHost` (one literal address), `allowCidr` (a block), and
## `allowPrivateHosts` (RFC1918 + loopback, deliberately excluding link-local
## and cloud metadata addresses). A rule always names an address, never a name:
## an advertised host name is resolved first and every answer is checked, while
## the `hostAllowed` predicate takes a literal and denies anything else.
##
## Prerequisites: a broker at `KAFKA_BROKERS` (default `localhost:9092`) whose
## advertised address is one the rules below permit, and the topic named by
## `KAFKA_TOPIC` (default `orders`).
##
## Run from the repository root:
##
##   nim c -r examples/host_allowlist.nim

import std/[os, sequtils, strutils]

import pkg/chronos
import pkg/async_kafka

proc main() {.async.} =
  let brokers = getEnv("KAFKA_BROKERS", "localhost:9092")
    .split(',')
    .mapIt(it.strip())
    .filterIt(it.len > 0)
  let topic = getEnv("KAFKA_TOPIC", "orders")

  let allowlist = newHostAllowlist(@[allowHost("127.0.0.1"), allowCidr("10.0.0.0/8")])

  # An empty list is refused by `newHostAllowlist` rather than read as "deny
  # everything", while omitting the parameter (nil) permits every advertised
  # address.
  echo "10.1.2.3 permitted: ", hostAllowed(allowlist, "10.1.2.3")
  echo "192.0.2.10 permitted: ", hostAllowed(allowlist, "192.0.2.10")

  # The check runs at the dial, so a request that must reach the partition
  # leader of the metadata response is what exercises it; refreshing metadata
  # alone would only cache whatever addresses the response advertised.
  let cluster = newCluster(brokers, hostAllowlist = allowlist)
  let producer = newKafkaProducer(cluster)
  let produced = await producer.send(topic = topic, value = toBytes("hello kafka"))
  echo "reached the advertised leader of ", topic, "[", produced.partition, "]"
  await producer.close()
  await cluster.close()

  # A rule the configured broker cannot satisfy fails closed, before a socket
  # is opened, instead of falling back to an unchecked dial.
  let denied =
    newCluster(brokers, hostAllowlist = newHostAllowlist(@[allowHost("192.0.2.1")]))
  try:
    await denied.refreshMetadata()
    echo "unexpected: ", brokers.join(", "), " was not refused"
  except KafkaError as e:
    echo "refused before dialing: ", e.msg
  await denied.close()

try:
  waitFor main()
except KafkaError as e:
  echo "kafka error: ", e.msg
  quit(QuitFailure)
