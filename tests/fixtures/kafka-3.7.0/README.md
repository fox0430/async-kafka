# Apache Kafka 3.7.0 message definitions

`message/*.json` are verbatim copies of

    https://github.com/apache/kafka/blob/3.7.0/clients/src/main/resources/common/message/

fetched from the `3.7.0` tag (the release CI's broker runs, see
`docker-compose.yml`). They are the upstream definitions the hand-written
`MessageDef` declarations in `async_kafka/protocol/messages.nim` were
transcribed from.

They are vendored so `tests/test_wire_spec_drift.nim` can check the local
declarations against them offline, in CI, on every run. The files are
unchanged; the test strips the `//` comments (Kafka's own tooling accepts
them, `std/json` does not) before parsing.

Record and RecordBatch have no message JSON upstream — their wire format is
the log format, not an API message — so they are checked by the hand-written
RecordBatch fixtures in `tests/test_schema_gen.nim` instead.

Licensed to the Apache Software Foundation (ASF) under the Apache License,
Version 2.0; each file carries its own header.
