## Divert records the producer cannot deliver permanently to a dead-letter
## topic, sent through a second producer.
##
## `deadLetterOnPermanent()` diverts only non-retriable broker errors;
## `alwaysDeadLetter()` and `alwaysDrop()` are the other built-in handlers, and
## any `ErrorHandler` proc can choose per `FailedRecord`. A diverted `send`
## returns a `ProduceResult` whose `status` says what happened instead of
## raising, and a transactional producer never diverts: the failure surfaces so
## that the transaction cannot commit without the record.
##
## Whether a divert happens is the broker's call: a broker that accepts the
## record delivers it, and this program reports `pstSuccess`. The shortest way to
## see the other path is a source topic that caps the record size below what is
## sent, since MESSAGE_TOO_LARGE is permanent, so for example:
##
##   kafka-topics.sh --bootstrap-server localhost:9092 --alter \
##     --topic orders --config max.message.bytes=8
##
## A later run then reports `pstDeadLettered` and the copy lands on the
## dead-letter topic. `pstDropped` needs an `ErrorHandler` that returns `eaDrop`,
## which `deadLetterOnPermanent()` never does.
##
## Prerequisites: a broker at `KAFKA_BROKERS` (default `localhost:9092`) with
## the source and dead-letter topics (default `orders` / `orders.dlq`) already
## created, since the client asks for no auto-creation.
##
## Run from the repository root:
##
##   nim c -r examples/dead_letter_queue.nim

import std/[os, sequtils, strutils]

import pkg/chronos
import pkg/async_kafka

proc main() {.async.} =
  let brokers = getEnv("KAFKA_BROKERS", "localhost:9092")
    .split(',')
    .mapIt(it.strip())
    .filterIt(it.len > 0)
  let topic = getEnv("KAFKA_TOPIC", "orders")
  let dlqTopic = getEnv("KAFKA_DLQ_TOPIC", "orders.dlq")

  let cluster = newCluster(brokers)
  let producer = newKafkaProducer(cluster, errorHandler = deadLetterOnPermanent())
  let dlq = newKafkaProducer(cluster)
  # `ownsProducer = true` hands the sink to the producer's own close().
  await producer.setDeadLetterQueue(dlqTopic, dlq, ownsProducer = true)

  # This record is delivered, so it reports `pstSuccess`. A record the broker
  # rejects for good takes the same path and reports `pstDeadLettered` (or
  # `pstDeadLetterFailed` when the sink itself could not take it) instead of
  # raising; the header names a broker configuration that rejects it.
  let produced = await producer.send(topic = topic, value = toBytes("hello kafka"))
  case produced.status
  of pstSuccess:
    echo "delivered to ",
      topic, "[", produced.partition, "] at offset ", produced.offset
    echo "the broker accepted the record; see the header for the divert path"
  of pstDeadLettered:
    echo "diverted to ", dlqTopic
  of pstDeadLetterFailed:
    echo "diverting to ", dlqTopic, " failed; the record is lost"
  of pstDropped:
    echo "dropped without a dead-letter copy"

  await producer.close()
  await cluster.close()

try:
  waitFor main()
except KafkaError as e:
  echo "kafka error: ", e.msg
  quit(QuitFailure)
