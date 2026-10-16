## Producer module — DLQ policy and ErrorHandler factories. The DLQ send and
## `handleSendFailure` dispatcher live with the batch pipeline (they call `send`,
## which would otherwise be a circular import); `setDeadLetterQueue` sits beside
## `close`.

import ../errors
import ../protocol/messages

import types

proc makeFailedRecord*(
    tp: TopicPartition, rec: Record, error: ref KafkaError, attempts: int32
): FailedRecord =
  FailedRecord(
    topic: tp.topic,
    partition: tp.partition,
    key: rec.key,
    value: rec.value,
    headers: rec.headers,
    error: error,
    attempts: attempts,
    timestamp: rec.timestamp,
  )

proc alwaysDeadLetter*(): ErrorHandler =
  ## Routes every failure to the dead-letter topic.
  proc handler(rec: FailedRecord): ErrorAction {.closure, gcsafe, raises: [].} =
    eaDeadLetter

  handler

proc alwaysDrop*(): ErrorHandler =
  ## Drops every failure silently.
  proc handler(rec: FailedRecord): ErrorAction {.closure, gcsafe, raises: [].} =
    eaDrop

  handler

proc deadLetterOnPermanent*(): ErrorHandler =
  ## Permanent (non-retriable) API failures go to the dead-letter topic; everything
  ## else fails back to the caller.
  proc handler(rec: FailedRecord): ErrorAction {.closure, gcsafe, raises: [].} =
    if rec.error of KafkaApiError:
      if isPermanent((ref KafkaApiError)(rec.error).errorCode):
        return eaDeadLetter
    eaFail

  handler
