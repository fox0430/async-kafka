## tests/fake_broker.nim
##
## Shared in-process fake Kafka broker for the unit tests: drives the connection
## layer (and higher layers) without a real broker. Frames are read off the wire,
## the request header is parsed, and captured requests are pushed onto an
## AsyncQueue for tests to answer via the response-builder helpers.
##
## Intentionally a minimal protocol-aware loopback: it never interprets request
## bodies or generates responses on its own — every response is test-driven, so
## tests fully control reorderings, timeouts, duplicates, partial writes, and
## disconnects. The per-helper self-tests live in the test suite.

import std/options

import chronos
import stew/endians2

import async_kafka/errors
import async_kafka/protocol/primitives

type
  CapturedRequest* = object
    apiKey*: int16
    apiVersion*: int16
    correlationId*: int32
    clientId*: string
    body*: seq[byte]
      ## Bytes after the clientId field. For flexible-version requests this
      ## still includes the request-header tagged_fields byte (typically 0x00);
      ## tests that care can use `flexibleBody` to strip it.
    transport*: StreamTransport

  FakeBroker* = ref object
    server: StreamServer
    requests: AsyncQueue[CapturedRequest]
    transports: seq[StreamTransport]
    accepted: AsyncEvent
    stopping: bool

  FakeApiVersion* = object
    apiKey*: int16
    minVersion*: int16
    maxVersion*: int16

  FakeBrokerMeta* = object
    nodeId*: int32
    host*: string
    port*: int32

  FakePartitionMeta* = object
    partitionIndex*: int32
    leaderId*: int32
    replicas*: seq[int32]
    isr*: seq[int32]

  FakeTopicMeta* = object
    name*: string
    errorCode*: int16
    isInternal*: bool
    partitions*: seq[FakePartitionMeta]

  FakeFetchPartition* = object
    partition*: int32
    errorCode*: int16
    highWatermark*: int64
    lastStableOffset*: int64
    logStartOffset*: int64
    recordsRaw*: Option[seq[byte]]

  FakeFetchTopic* = object
    topic*: string
    partitions*: seq[FakeFetchPartition]

const maxFakeFrameSize = 100_000_000

proc address*(fb: FakeBroker): TransportAddress =
  fb.server.localAddress()

proc pendingRequests*(fb: FakeBroker): int =
  fb.requests.len

proc transports*(fb: FakeBroker): seq[StreamTransport] =
  fb.transports

proc flexibleBody*(req: CapturedRequest): seq[byte] {.raises: [ValueError].} =
  ## Strips the leading request-header tagged_fields byte (must be 0x00).
  ## Raises ValueError instead of asserting so that a malformed test input
  ## surfaces as a unittest failure rather than a Defect that crashes the
  ## whole test process.
  if req.body.len == 0:
    raise newException(ValueError, "request body is empty")
  if req.body[0] != 0x00'u8:
    raise newException(
      ValueError, "non-empty request-header tagged_fields not supported by fake broker"
    )
  result = req.body[1 ..< req.body.len]

proc parseRequestHeader(
    frame: seq[byte], transport: StreamTransport
): CapturedRequest {.raises: [ValueError].} =
  if frame.len < 10:
    raise newException(ValueError, "frame too short for request header")
  result.apiKey = cast[int16](fromBytesBE(uint16, [frame[0], frame[1]]))
  result.apiVersion = cast[int16](fromBytesBE(uint16, [frame[2], frame[3]]))
  result.correlationId =
    cast[int32](fromBytesBE(uint32, [frame[4], frame[5], frame[6], frame[7]]))
  let clientIdLen = cast[int16](fromBytesBE(uint16, [frame[8], frame[9]]))
  var bodyStart = 10
  if clientIdLen > 0:
    if frame.len < 10 + int(clientIdLen):
      raise newException(ValueError, "frame missing clientId bytes")
    result.clientId = newString(int(clientIdLen))
    for i in 0 ..< int(clientIdLen):
      result.clientId[i] = char(frame[10 + i])
    bodyStart = 10 + int(clientIdLen)
  if bodyStart < frame.len:
    result.body = frame[bodyStart ..< frame.len]
  else:
    result.body = @[]
  result.transport = transport

proc readOneFrame(
    transport: StreamTransport
): Future[Option[seq[byte]]] {.async: (raises: [CatchableError]).} =
  var sizeBuf: array[4, byte]
  try:
    await transport.readExactly(addr sizeBuf[0], 4)
  except TransportIncompleteError:
    return none(seq[byte])
  except TransportError:
    return none(seq[byte])
  let size = int(fromBytesBE(uint32, sizeBuf))
  if size <= 0 or size > maxFakeFrameSize:
    return none(seq[byte])
  var body = newSeq[byte](size)
  try:
    await transport.readExactly(addr body[0], size)
  except TransportIncompleteError:
    return none(seq[byte])
  except TransportError:
    return none(seq[byte])
  return some(body)

proc clientLoop(fb: FakeBroker, transport: StreamTransport) {.async: (raises: []).} =
  try:
    while not transport.closed and not fb.stopping:
      let frameOpt = await readOneFrame(transport)
      if frameOpt.isNone:
        break
      let frame = frameOpt.get()
      try:
        let req = parseRequestHeader(frame, transport)
        await fb.requests.put(req)
      except ValueError:
        break
  except CancelledError:
    discard
  except CatchableError:
    discard
  if not transport.closed:
    try:
      await transport.closeWait()
    except CatchableError:
      discard

proc newFakeBroker*(address = initTAddress("127.0.0.1:0")): FakeBroker =
  ## `address` is overridable for suites that dial the fake by name: bind where
  ## the name resolves to, or the test depends on the resolver's IPv4/IPv6
  ## preference.
  let fb = FakeBroker(
    requests: newAsyncQueue[CapturedRequest](),
    transports: @[],
    accepted: newAsyncEvent(),
    stopping: false,
  )

  proc onAccept(
      server: StreamServer, transport: StreamTransport
  ) {.async: (raises: []).} =
    fb.transports.add(transport)
    fb.accepted.fire()
    await clientLoop(fb, transport)

  fb.server = createStreamServer(address, onAccept, flags = {ReuseAddr})
  fb.server.start()
  fb

proc waitAccepted*(
    fb: FakeBroker, timeout: Duration = 5.seconds
) {.async: (raises: [CatchableError]).} =
  if fb.transports.len > 0:
    return
  await fb.accepted.wait().wait(timeout)

proc nextRequest*(
    fb: FakeBroker, timeout: Duration = 5.seconds
): Future[CapturedRequest] {.async: (raises: [CatchableError]).} =
  return await fb.requests.get().wait(timeout)

proc tryNextRequest*(fb: FakeBroker): Option[CapturedRequest] =
  if fb.requests.empty:
    return none(CapturedRequest)
  return some(fb.requests.popFirstNoWait())

proc buildResponseFrame*(
    correlationId: int32, body: seq[byte], flexible: bool = false
): seq[byte] =
  ## Builds a Kafka response frame:
  ##   [size:int32] [correlationId:int32] [tagged_fields:0x00 if flexible] [body...]
  var inner = newSeqOfCap[byte](4 + (if flexible: 1 else: 0) + body.len)
  inner.add(toBytesBE(cast[uint32](correlationId)))
  if flexible:
    inner.add(0x00'u8)
  if body.len > 0:
    inner.add(body)
  result = newSeqOfCap[byte](4 + inner.len)
  result.add(toBytesBE(uint32(inner.len)))
  result.add(inner)

proc writeAll(
    transport: StreamTransport, data: seq[byte]
) {.async: (raises: [CatchableError]).} =
  if data.len == 0:
    return
  discard await transport.write(data)

proc respond*(
    req: CapturedRequest, body: seq[byte] = @[], flexible: bool = false
) {.async: (raises: [CatchableError]).} =
  ## Sends a Kafka-framed response on the request's transport, echoing the
  ## captured correlation id.
  let frame = buildResponseFrame(req.correlationId, body, flexible)
  await writeAll(req.transport, frame)

proc respondRaw*(
    req: CapturedRequest, frame: seq[byte]
) {.async: (raises: [CatchableError]).} =
  ## Sends arbitrary bytes on the request's transport. The caller is
  ## responsible for the 4-byte length prefix.
  await writeAll(req.transport, frame)

proc respondWithCorrelationId*(
    req: CapturedRequest,
    correlationId: int32,
    body: seq[byte] = @[],
    flexible: bool = false,
) {.async: (raises: [CatchableError]).} =
  ## Sends a response on the request's transport using a caller-supplied
  ## correlation id (e.g. for unknown-corrId or duplicate-corrId tests).
  let frame = buildResponseFrame(correlationId, body, flexible)
  await writeAll(req.transport, frame)

proc respondInChunks*(
    req: CapturedRequest,
    body: seq[byte],
    firstChunkBytes: int,
    delayBetween: Duration,
    flexible: bool = false,
) {.async: (raises: [CatchableError]).} =
  ## Sends the response in two writes with a delay between them, exercising
  ## partial-read paths in the client.
  let frame = buildResponseFrame(req.correlationId, body, flexible)
  let split = max(0, min(firstChunkBytes, frame.len))
  if split > 0:
    await writeAll(req.transport, frame[0 ..< split])
  await sleepAsync(delayBetween)
  if split < frame.len:
    await writeAll(req.transport, frame[split ..< frame.len])

proc closeTransport*(req: CapturedRequest) {.async: (raises: []).} =
  if not req.transport.closed:
    try:
      await req.transport.closeWait()
    except CatchableError:
      discard

proc closeAllTransports*(fb: FakeBroker) {.async: (raises: []).} =
  for t in fb.transports:
    if not t.closed:
      try:
        await t.closeWait()
      except CatchableError:
        discard

proc close*(fb: FakeBroker) {.async: (raises: []).} =
  if fb.stopping:
    return
  fb.stopping = true
  await fb.closeAllTransports()
  try:
    fb.server.stop()
  except CatchableError:
    discard
  try:
    await fb.server.closeWait()
  except CatchableError:
    discard

# Response body builders
#
# The negotiated version is min(client supportedVersion.max, what this fake
# broker advertises), which clamps the exercised code paths to the versions
# each test advertises. The legacy builders below pin the exercised paths to
# specific non-flexible versions (ApiVersions v0, Metadata v1, Produce v3,
# txn APIs v0, JoinGroup v2, ListOffsets v2, OffsetFetch/OffsetCommit v3). The
# flexible builders emit KIP-482 bodies — compact strings/arrays plus the
# per-struct tagged-fields trailers — and must be paired with
# `respond(..., flexible = true)` so the response-header tagged-fields byte is
# emitted as well.
# Each builder emits the response *body* (everything after the response header)
# so the caller can pass the result to `respond()` directly.

proc buildApiVersionsResponseBody*(
    errorCode: int16, supported: openArray[FakeApiVersion]
): seq[byte] {.raises: [].} =
  ## ApiVersions v0 response body:
  ##   error_code:int16,
  ##   api_keys:int32-len-array of (api_key:int16, min:int16, max:int16)
  var w = initKafkaWriter(2 + 4 + 6 * supported.len)
  w.writeInt16(errorCode)
  w.writeArrayLen(int32(supported.len))
  for k in supported:
    w.writeInt16(k.apiKey)
    w.writeInt16(k.minVersion)
    w.writeInt16(k.maxVersion)
  result = w.buf

proc buildMetadataResponseBody*(
    brokers: openArray[FakeBrokerMeta],
    controllerId: int32,
    topics: openArray[FakeTopicMeta],
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## Metadata v1 response body. Only the fields the client decoder reads are
  ## emitted; rack is always null.
  var w = initKafkaWriter(64)
  w.writeArrayLen(int32(brokers.len))
  for b in brokers:
    w.writeInt32(b.nodeId)
    w.writeString(b.host)
    w.writeInt32(b.port)
    w.writeNullableString(none(string)) # rack
  w.writeInt32(controllerId)
  w.writeArrayLen(int32(topics.len))
  for t in topics:
    w.writeInt16(t.errorCode)
    w.writeString(t.name)
    w.writeBool(t.isInternal)
    w.writeArrayLen(int32(t.partitions.len))
    for p in t.partitions:
      w.writeInt16(0'i16) # partition error_code
      w.writeInt32(p.partitionIndex)
      w.writeInt32(p.leaderId)
      w.writeArrayLen(int32(p.replicas.len))
      for nodeId in p.replicas:
        w.writeInt32(nodeId)
      w.writeArrayLen(int32(p.isr.len))
      for nodeId in p.isr:
        w.writeInt32(nodeId)
  result = w.buf

proc buildProduceResponseBody*(
    topic: string,
    partition: int32,
    errorCode: int16,
    baseOffset: int64,
    logAppendTimeMs: int64 = -1,
    throttleTimeMs: int32 = 0,
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## Produce v3 response body for a single topic / single partition. The
  ## throttle_time_ms trailer is required (the client decoder reads it
  ## unconditionally for v1+).
  var w = initKafkaWriter(48 + topic.len)
  w.writeArrayLen(1) # topics
  w.writeString(topic)
  w.writeArrayLen(1) # partitions
  w.writeInt32(partition)
  w.writeInt16(errorCode)
  w.writeInt64(baseOffset)
  w.writeInt64(logAppendTimeMs)
  w.writeInt32(throttleTimeMs)
  result = w.buf

proc buildFetchResponseBody*(
    throttleTimeMs: int32,
    errorCode: int16,
    sessionId: int32,
    topics: openArray[FakeFetchTopic],
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## Fetch v7 response body. Layout:
  ##   throttle_time_ms:int32, error_code:int16, session_id:int32,
  ##   topics:int32-len-array of (
  ##     topic:string,
  ##     partitions:int32-len-array of (
  ##       partition:int32, error_code:int16, high_watermark:int64,
  ##       last_stable_offset:int64, log_start_offset:int64 (v5+),
  ##       aborted_transactions:nullable-array (null here),
  ##       records:nullable-bytes))
  ## preferredReadReplica (v11+) is omitted; this builder targets v7-v10.
  var w = initKafkaWriter(64)
  w.writeInt32(throttleTimeMs)
  w.writeInt16(errorCode)
  w.writeInt32(sessionId)
  w.writeArrayLen(int32(topics.len))
  for t in topics:
    w.writeString(t.topic)
    w.writeArrayLen(int32(t.partitions.len))
    for p in t.partitions:
      w.writeInt32(p.partition)
      w.writeInt16(p.errorCode)
      w.writeInt64(p.highWatermark)
      w.writeInt64(p.lastStableOffset)
      w.writeInt64(p.logStartOffset)
      w.writeArrayLen(-1'i32) # aborted_transactions = null
      w.writeNullableBytes(p.recordsRaw) # records (nullable bytes)
  result = w.buf

proc buildOffsetFetchResponseBody*(
    topic: string, partition: int32, errorCode: int16, offset: int64
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## OffsetFetch v1 response body for a single topic / single partition:
  ##   topics:int32-len-array of (
  ##     topic:string,
  ##     partitions:int32-len-array of (
  ##       partition:int32, offset:int64,
  ##       metadata:nullable-string, error_code:int16))
  var w = initKafkaWriter(32 + topic.len)
  w.writeArrayLen(1) # topics
  w.writeString(topic)
  w.writeArrayLen(1) # partitions
  w.writeInt32(partition)
  w.writeInt64(offset)
  w.writeNullableString(none(string))
  w.writeInt16(errorCode)
  result = w.buf

proc buildOffsetFetchResponseBodyV3*(
    topic: string,
    partition: int32,
    errorCode: int16,
    offset: int64,
    throttleTimeMs: int32 = 0'i32,
    topLevelErrorCode: int16 = 0'i16,
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## OffsetFetch v3 response body:
  ##   throttle_time_ms:int32, topics:int32-len-array of (...),
  ##   top_level_error_code:int16
  var w = initKafkaWriter(40 + topic.len)
  w.writeInt32(throttleTimeMs)
  w.writeArrayLen(1)
  w.writeString(topic)
  w.writeArrayLen(1)
  w.writeInt32(partition)
  w.writeInt64(offset)
  w.writeNullableString(none(string))
  w.writeInt16(errorCode)
  w.writeInt16(topLevelErrorCode)
  result = w.buf

type FakeOffsetFetchPartition* = object
  partition*: int32
  errorCode*: int16
  offset*: int64

proc buildOffsetFetchResponseBodyV3Multi*(
    topic: string,
    partitions: openArray[FakeOffsetFetchPartition],
    throttleTimeMs: int32 = 0'i32,
    topLevelErrorCode: int16 = 0'i16,
): seq[byte] {.raises: [KafkaProtocolError].} =
  var w = initKafkaWriter(40 + topic.len + partitions.len * 20)
  w.writeInt32(throttleTimeMs)
  w.writeArrayLen(1)
  w.writeString(topic)
  w.writeArrayLen(int32(partitions.len))
  for p in partitions:
    w.writeInt32(p.partition)
    w.writeInt64(p.offset)
    w.writeNullableString(none(string))
    w.writeInt16(p.errorCode)
  w.writeInt16(topLevelErrorCode)
  result = w.buf

proc buildOffsetCommitResponseBodyV3*(
    topic: string, partition: int32, errorCode: int16, throttleTimeMs: int32 = 0'i32
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## OffsetCommit v3 response body:
  ##   throttle_time_ms:int32, topics:int32-len-array of (
  ##     topic:string,
  ##     partitions:int32-len-array of (partition:int32, error_code:int16))
  var w = initKafkaWriter(24 + topic.len)
  w.writeInt32(throttleTimeMs)
  w.writeArrayLen(1)
  w.writeString(topic)
  w.writeArrayLen(1)
  w.writeInt32(partition)
  w.writeInt16(errorCode)
  result = w.buf

proc buildHeartbeatResponseBodyV1*(
    errorCode: int16 = 0'i16, throttleTimeMs: int32 = 0'i32
): seq[byte] {.raises: [].} =
  ## Heartbeat v1 response body: throttle_time_ms:int32, error_code:int16.
  var w = initKafkaWriter(6)
  w.writeInt32(throttleTimeMs)
  w.writeInt16(errorCode)
  result = w.buf

proc buildSyncGroupResponseBodyV1*(
    errorCode: int16 = 0'i16, assignment: seq[byte] = @[], throttleTimeMs: int32 = 0'i32
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## SyncGroup v1 response body:
  ##   throttle_time_ms:int32, error_code:int16, assignment:int32-len-bytes.
  var w = initKafkaWriter(10 + assignment.len)
  w.writeInt32(throttleTimeMs)
  w.writeInt16(errorCode)
  w.writeBytes(assignment)
  result = w.buf

proc buildLeaveGroupResponseBodyV1*(
    errorCode: int16 = 0'i16, throttleTimeMs: int32 = 0'i32
): seq[byte] {.raises: [].} =
  ## LeaveGroup v1 response body: throttle_time_ms:int32, error_code:int16.
  var w = initKafkaWriter(6)
  w.writeInt32(throttleTimeMs)
  w.writeInt16(errorCode)
  result = w.buf

proc buildJoinGroupResponseBodyV2*(
    errorCode: int16 = 0'i16,
    generationId: int32 = 1'i32,
    protocolName: string = "range",
    leader: string = "",
    memberId: string = "",
    members: seq[tuple[memberId: string, metadata: seq[byte]]] = @[],
    throttleTimeMs: int32 = 0'i32,
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## JoinGroup v2 response body:
  ##   throttle_time_ms:int32, error_code:int16, generation_id:int32,
  ##   group_protocol:string, leader_id:string, member_id:string,
  ##   members:int32-len-array of (member_id:string, metadata:int32-len-bytes).
  var w = initKafkaWriter(32 + protocolName.len + leader.len + memberId.len)
  w.writeInt32(throttleTimeMs)
  w.writeInt16(errorCode)
  w.writeInt32(generationId)
  w.writeString(protocolName)
  w.writeString(leader)
  w.writeString(memberId)
  w.writeArrayLen(int32(members.len))
  for m in members:
    w.writeString(m.memberId)
    w.writeBytes(m.metadata)
  result = w.buf

proc buildListOffsetsResponseBodyV2*(
    topic: string,
    partition: int32,
    errorCode: int16,
    timestamp: int64,
    offset: int64,
    throttleTimeMs: int32 = 0'i32,
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## ListOffsets v2 response body:
  ##   throttle_time_ms:int32, topics:int32-len-array of (
  ##     topic:string,
  ##     partitions:int32-len-array of (
  ##       partition:int32, error_code:int16, timestamp:int64, offset:int64))
  var w = initKafkaWriter(40 + topic.len)
  w.writeInt32(throttleTimeMs)
  w.writeArrayLen(1)
  w.writeString(topic)
  w.writeArrayLen(1)
  w.writeInt32(partition)
  w.writeInt16(errorCode)
  w.writeInt64(timestamp)
  w.writeInt64(offset)
  result = w.buf

type FakeEpochEndOffset* = object
  partition*: int32
  errorCode*: int16
  leaderEpoch*: int32
  endOffset*: int64

proc buildOffsetForLeaderEpochResponseBody*(
    topic: string,
    partitions: openArray[FakeEpochEndOffset],
    throttleTimeMs: int32 = 0'i32,
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## OffsetForLeaderEpoch v2/v3 response body (wire-identical for our fields):
  ##   throttle_time_ms:int32, topics:int32-len-array of (
  ##     topic:string,
  ##     partitions:int32-len-array of (
  ##       error_code:int16, partition:int32, leader_epoch:int32,
  ##       end_offset:int64))
  ## Note the errorCode-then-partition order (reverse of ListOffsets).
  var w = initKafkaWriter(24 + topic.len + partitions.len * 18)
  w.writeInt32(throttleTimeMs)
  w.writeArrayLen(1)
  w.writeString(topic)
  w.writeArrayLen(int32(partitions.len))
  for p in partitions:
    w.writeInt16(p.errorCode)
    w.writeInt32(p.partition)
    w.writeInt32(p.leaderEpoch)
    w.writeInt64(p.endOffset)
  result = w.buf

proc buildFindCoordinatorResponseBodyV1*(
    errorCode: int16 = 0'i16,
    errorMessage: Option[string] = none(string),
    nodeId: int32 = 1'i32,
    host: string = "127.0.0.1",
    port: int32 = 9092'i32,
    throttleTimeMs: int32 = 0'i32,
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## FindCoordinator v1 response body:
  ##   throttle_time_ms:int32, error_code:int16,
  ##   error_message:nullable-string, node_id:int32, host:string,
  ##   port:int32.
  var w = initKafkaWriter(28 + host.len)
  w.writeInt32(throttleTimeMs)
  w.writeInt16(errorCode)
  w.writeNullableString(errorMessage)
  w.writeInt32(nodeId)
  w.writeString(host)
  w.writeInt32(port)
  result = w.buf

proc buildInitProducerIdResponseBody*(
    errorCode: int16 = 0'i16,
    producerId: int64 = 1'i64,
    producerEpoch: int16 = 0'i16,
    throttleTimeMs: int32 = 0'i32,
): seq[byte] {.raises: [].} =
  ## InitProducerId v0 response body:
  ##   throttle_time_ms:int32, error_code:int16, producer_id:int64,
  ##   producer_epoch:int16.
  var w = initKafkaWriter(16)
  w.writeInt32(throttleTimeMs)
  w.writeInt16(errorCode)
  w.writeInt64(producerId)
  w.writeInt16(producerEpoch)
  result = w.buf

proc buildEndTxnResponseBody*(
    errorCode: int16 = 0'i16, throttleTimeMs: int32 = 0'i32
): seq[byte] {.raises: [].} =
  ## EndTxn v0 response body:
  ##   throttle_time_ms:int32, error_code:int16.
  var w = initKafkaWriter(6)
  w.writeInt32(throttleTimeMs)
  w.writeInt16(errorCode)
  result = w.buf

proc buildAddOffsetsToTxnResponseBody*(
    errorCode: int16 = 0'i16, throttleTimeMs: int32 = 0'i32
): seq[byte] {.raises: [].} =
  ## AddOffsetsToTxn v0 response body:
  ##   throttle_time_ms:int32, error_code:int16.
  var w = initKafkaWriter(6)
  w.writeInt32(throttleTimeMs)
  w.writeInt16(errorCode)
  result = w.buf

proc buildAddPartitionsToTxnResponseBody*(
    topic: string, partitions: openArray[(int32, int16)], throttleTimeMs: int32 = 0'i32
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## AddPartitionsToTxn v0 response body (non-flexible):
  ##   throttle_time_ms:int32,
  ##   errors:array[ topic:string, partitions:array[ partition:int32,
  ##     error_code:int16 ] ].
  ## `partitions` pairs each partition index with its per-partition error code so
  ## a test can express a partial result (one partition `keNone`, a sibling
  ## rejected).
  var w = initKafkaWriter()
  w.writeInt32(throttleTimeMs)
  w.writeArrayLen(1)
  w.writeString(topic)
  w.writeArrayLen(int32(partitions.len))
  for (partition, errorCode) in partitions:
    w.writeInt32(partition)
    w.writeInt16(errorCode)
  result = w.buf

proc buildTxnOffsetCommitResponseBody*(
    topic: string,
    partition: int32 = 0'i32,
    errorCode: int16 = 0'i16,
    throttleTimeMs: int32 = 0'i32,
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## TxnOffsetCommit v0 response body (non-flexible):
  ##   throttle_time_ms:int32,
  ##   topics:array[ topic:string, partitions:array[ partition:int32,
  ##     error_code:int16 ] ].
  var w = initKafkaWriter()
  w.writeInt32(throttleTimeMs)
  w.writeArrayLen(1)
  w.writeString(topic)
  w.writeArrayLen(1)
  w.writeInt32(partition)
  w.writeInt16(errorCode)
  result = w.buf

proc buildInitProducerIdResponseBodyV2*(
    errorCode: int16 = 0'i16,
    producerId: int64 = 1'i64,
    producerEpoch: int16 = 0'i16,
    throttleTimeMs: int32 = 0'i32,
): seq[byte] {.raises: [].} =
  ## InitProducerId v2+ response body (flexible, KIP-482): every field is a
  ## fixed-size scalar, so the wire matches v0 except for the trailing empty
  ## tagged-fields trailer. Pair with `respond(..., flexible = true)`.
  var w = initKafkaWriter(17)
  w.writeInt32(throttleTimeMs)
  w.writeInt16(errorCode)
  w.writeInt64(producerId)
  w.writeInt16(producerEpoch)
  w.writeTaggedFieldsEmpty()
  result = w.buf

proc buildEndTxnResponseBodyV3*(
    errorCode: int16 = 0'i16, throttleTimeMs: int32 = 0'i32
): seq[byte] {.raises: [].} =
  ## EndTxn v3 response body (flexible, KIP-482):
  ##   throttle_time_ms:int32, error_code:int16, tagged_fields:0x00.
  ## Pair with `respond(..., flexible = true)`.
  var w = initKafkaWriter(7)
  w.writeInt32(throttleTimeMs)
  w.writeInt16(errorCode)
  w.writeTaggedFieldsEmpty()
  result = w.buf

proc buildAddOffsetsToTxnResponseBodyV3*(
    errorCode: int16 = 0'i16, throttleTimeMs: int32 = 0'i32
): seq[byte] {.raises: [].} =
  ## AddOffsetsToTxn v3 response body (flexible, KIP-482): wire-identical to
  ## EndTxn v3 — throttle_time_ms:int32, error_code:int16, tagged_fields:0x00.
  ## Pair with `respond(..., flexible = true)`.
  var w = initKafkaWriter(7)
  w.writeInt32(throttleTimeMs)
  w.writeInt16(errorCode)
  w.writeTaggedFieldsEmpty()
  result = w.buf

proc buildAddPartitionsToTxnResponseBodyV3*(
    topic: string, partitions: openArray[(int32, int16)], throttleTimeMs: int32 = 0'i32
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## AddPartitionsToTxn v3 response body (flexible, KIP-482):
  ##   throttle_time_ms:int32,
  ##   errors:compact-array[ topic:compact-string,
  ##     partitions:compact-array[ partition:int32, error_code:int16,
  ##       tagged_fields:0x00 ], tagged_fields:0x00 ], tagged_fields:0x00.
  ## `partitions` pairs each partition index with its per-partition error code so
  ## a test can express a partial result (one partition `keNone`, a sibling
  ## rejected). Pair with `respond(..., flexible = true)`.
  var w = initKafkaWriter()
  w.writeInt32(throttleTimeMs)
  w.writeCompactArrayLen(1)
  w.writeCompactString(topic)
  w.writeCompactArrayLen(partitions.len)
  for (partition, errorCode) in partitions:
    w.writeInt32(partition)
    w.writeInt16(errorCode)
    w.writeTaggedFieldsEmpty()
  w.writeTaggedFieldsEmpty() # errors[0] tagged-fields trailer
  w.writeTaggedFieldsEmpty() # top-level tagged-fields trailer
  result = w.buf

proc buildTxnOffsetCommitResponseBodyV3*(
    topic: string,
    partition: int32 = 0'i32,
    errorCode: int16 = 0'i16,
    throttleTimeMs: int32 = 0'i32,
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## TxnOffsetCommit v3 response body (flexible, KIP-482):
  ##   throttle_time_ms:int32,
  ##   topics:compact-array[ topic:compact-string,
  ##     partitions:compact-array[ partition:int32, error_code:int16,
  ##       tagged_fields:0x00 ], tagged_fields:0x00 ], tagged_fields:0x00.
  ## Pair with `respond(..., flexible = true)`.
  var w = initKafkaWriter()
  w.writeInt32(throttleTimeMs)
  w.writeCompactArrayLen(1)
  w.writeCompactString(topic)
  w.writeCompactArrayLen(1)
  w.writeInt32(partition)
  w.writeInt16(errorCode)
  w.writeTaggedFieldsEmpty() # partition tagged-fields trailer
  w.writeTaggedFieldsEmpty() # topic tagged-fields trailer
  w.writeTaggedFieldsEmpty() # top-level tagged-fields trailer
  result = w.buf

proc buildSaslHandshakeResponseBody*(
    errorCode: int16, supportedMechanisms: openArray[string]
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## SaslHandshake v1 response body:
  ##   error_code:int16,
  ##   mechanisms:int32-len-array of string
  ## Used by the broker to advertise which SASL mechanisms it accepts when
  ## the client's requested mechanism is not supported.
  var w = initKafkaWriter(2 + 4 + 32 * supportedMechanisms.len)
  w.writeInt16(errorCode)
  w.writeArrayLen(int32(supportedMechanisms.len))
  for m in supportedMechanisms:
    w.writeString(m)
  result = w.buf

proc buildSaslAuthenticateResponseBody*(
    errorCode: int16,
    errorMessage: Option[string] = none(string),
    authBytes: seq[byte] = @[],
    sessionLifetimeMs: int64 = 0,
): seq[byte] {.raises: [KafkaProtocolError].} =
  ## SaslAuthenticate v1 response body:
  ##   error_code:int16,
  ##   error_message:nullable-string,
  ##   auth_bytes:int32-len-bytes,
  ##   session_lifetime_ms:int64
  var w = initKafkaWriter(2 + 2 + 4 + authBytes.len + 8)
  w.writeInt16(errorCode)
  w.writeNullableString(errorMessage)
  w.writeBytes(authBytes)
  w.writeInt64(sessionLifetimeMs)
  result = w.buf
