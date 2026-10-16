## Pins representative public facade usage: what `import async_kafka` exposes.
## The standalone public API checker owns the exact name/signature contract.
## These tests cover the application allowlist, field visibility, and
## constructor/signature compatibility.

import std/[unittest, tables, options, os, sets, strutils, sequtils, algorithm]

import chronos

import async_kafka

const projectRoot = parentDir(parentDir(currentSourcePath))

proc exportedSymbols(path: string): seq[string] =
  ## The qualified symbols (`module.Symbol`) of the single `export` statement in
  ## `path`, in source order. Comment and blank lines inside the statement are
  ## skipped; it ends at the first line that starts at column 0.
  var inBlock = false
  for raw in readFile(path).splitLines():
    let line = raw.strip()
    if not inBlock:
      if not line.startsWith("export"):
        continue
      inBlock = true
    elif line.len > 0 and raw[0] notin {' ', '\t'}:
      break
    var body =
      if line.startsWith("export"):
        line["export".len .. ^1]
      else:
        line
    let comment = body.find('#')
    if comment >= 0:
      body = body[0 ..< comment]
    for item in body.split(','):
      let symbol = item.strip()
      if symbol.len > 0:
        result.add symbol

func namesOf(symbols: seq[string]): HashSet[string] =
  for symbol in symbols:
    result.incl symbol.rsplit('.', 1)[^1]

suite "Facade exports":
  test "the root exposes every symbol the producer facade re-exports":
    let root = namesOf(exportedSymbols(projectRoot / "async_kafka.nim"))
    let producer =
      namesOf(exportedSymbols(projectRoot / "async_kafka/producer/producer.nim"))
    var missing: seq[string]
    for name in producer:
      if name notin root:
        missing.add name
    check missing.len == 0

  test "the root reaches producer symbols through the producer facade":
    let root = exportedSymbols(projectRoot / "async_kafka.nim")
    let producer =
      namesOf(exportedSymbols(projectRoot / "async_kafka/producer/producer.nim"))
    var unknown: seq[string]
    for symbol in root:
      if symbol.startsWith("producer.") and symbol.rsplit('.', 1)[^1] notin producer:
        unknown.add symbol
    check unknown.len == 0

    # Producer symbols the root takes from modules it imports for other reasons.
    # Pinned, so moving one between import paths is a deliberate edit.
    const crossModule = [
      "ConsumerGroupMetadata", "MetricsHook", "initConsumerGroupMetadata",
      "initUnfencedConsumerGroupMetadata",
    ]
    let direct = namesOf(root.filterIt(it.startsWith("producer.")))
    var viaOtherModules: seq[string]
    for name in producer:
      if name notin direct:
        viaOtherModules.add name
    check viaOtherModules.sorted == crossModule.sorted

  test "root allowlist exposes shared application types":
    check declared(KafkaErrorCode)
    check declared(KafkaError)
    check declared(KafkaConnectionError)
    check declared(KafkaProtocolError)
    check declared(KafkaDecompressionLimitError)
    check declared(KafkaApiError)
    check declared(KafkaTimeoutError)
    check declared(KafkaClosedError)
    check declared(KafkaCommitError)
    check declared(KafkaCommitSkippedError)
    check declared(KafkaOffsetFetchError)
    check declared(KafkaRebalanceCallbackError)
    check declared(LifecycleState)
    check declared(ErrorClass)
    check declared(keNone)
    check declared(keUnsupportedVersion)
    check declared(keProducerFenced)
    check declared(Cluster)
    check declared(TopicPartition)
    check declared(RecordHeader)
    check declared(FetchedRecord)
    check declared(CompressionCodec)
    check declared(AssignmentStrategy)
    check declared(OffsetResetPolicy)
    check declared(IsolationLevel)
    check declared(SaslMechanism)
    check declared(ScramHash)
    check declared(burnString)
    check declared(TrustAnchorStore)
    check declared(MetricsHook)

    check compiles(
      block:
        discard keNone.toInt16()
        discard keUnsupportedVersion.errorClass()
        discard keProducerFenced.errorClass()
        discard keUnsupportedVersion.isRetriable()
        discard keUnsupportedVersion.isRebalanceRequired()
        discard keProducerFenced.isAbortable()
        discard keDuplicateSequenceNumber.isDuplicateSequence()
        discard keProducerFenced.isFatal()
        discard keProducerFenced.isPermanent()
        discard keProducerFenced.isInstanceFatal()
        discard keProducerFenced.isClassified()
        discard hash(TopicPartition(topic: "topic", partition: 0))
    )

  test "burnString clears an application credential copy":
    var credential = newString("secret".len)
    credential[0 ..< credential.len] = "secret"
    burnString(credential)
    check credential.len == 0

  test "root allowlist exposes Cluster and consumer application API":
    check declared(newCluster)
    check declared(refreshMetadata)
    check declared(initConsumerConfig)
    check declared(newKafkaConsumer)
    check declared(RebalanceCallback)
    check declared(ConsumerConfig)
    check declared(KafkaConsumer)
    check declared(CommitCallback)
    check declared(groupMetadata)
    check declared(start)
    check declared(poll)
    check declared(tryPoll)
    check declared(pollBatch)
    check declared(pollBatchStrict)
    check declared(commitSync)
    check declared(commitAsync)
    check declared(pause)
    check declared(resume)
    check declared(seek)
    check declared(isPaused)
    check declared(pausedPartitions)

    check compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        discard cluster.isClosed()
        discard cluster.state()
        discard cluster.refreshMetadata()
        discard cluster.close()

        var config = initConsumerConfig(
          bootstrapBrokers = @["127.0.0.1:1"], groupId = "group", topics = @["topic"]
        )
        config.offsetResetPolicy = orpEarliest
        config.isolationLevel = ilReadCommitted
        config.assignmentStrategy = asRange
        config.assignmentStrategy = asRoundRobin
        config.onPartitionsAssigned = proc(
            partitions: seq[TopicPartition]
        ) {.gcsafe, raises: [].} =
          discard
        let commitCallback: CommitCallback = proc(
            error: ref KafkaError
        ) {.gcsafe, raises: [].} =
          discard

        let tp = TopicPartition(topic: "topic", partition: 0)
        let record = FetchedRecord(
          topic: "topic",
          partition: 0,
          offset: 0,
          headers: @[RecordHeader(key: "key", value: some(@[byte(1)]))],
        )
        discard record.topic
        discard record.offset
        discard record.headers
        let shared = newKafkaConsumer(config, cluster)
        let owned = newKafkaConsumer(config)
        discard shared.isClosed()
        discard shared.state()
        let metadata: ConsumerGroupMetadata = shared.groupMetadata()
        discard metadata
        discard shared.start()
        discard shared.poll(1.seconds)
        discard shared.tryPoll(1.seconds)
        discard shared.pollBatch(maxRecords = 1, timeout = 1.seconds)
        discard shared.pollBatchStrict(maxRecords = 1, timeout = 1.seconds)
        discard shared.commitSync()
        shared.commitAsync(commitCallback)
        shared.pause(@[tp])
        shared.resume(@[tp])
        shared.seek(tp, 0)
        discard shared.isPaused(tp)
        discard shared.pausedPartitions()
        discard shared.close()
        discard owned.close()
    )

  test "root allowlist preserves constructor type identity":
    check compiles(
      block:
        let trustAnchors: TrustAnchorStore = nil
        let tlsConfig = newTlsConfig(trustAnchors = trustAnchors)
        let saslMechanism = newSaslPlain("user", "password", allowPlaintext = true)
        let scram256: SaslMechanism = newSaslScram("user", "password", scramSha256)
        let scram512: SaslMechanism = newSaslScram("user", "password", scramSha512)
        discard scram256
        discard scram512
        let metricsHook: MetricsHook = proc(
            event: cstring, tags: openArray[(cstring, cstring)], value: int64
        ) {.gcsafe, raises: [].} =
          discard

        let cluster = newCluster(
          @["127.0.0.1:1"],
          tlsConfig = tlsConfig,
          saslMechanism = saslMechanism,
          metricsHook = metricsHook,
        )
        let producer =
          newKafkaProducer(cluster, compression = ccGzip, metricsHook = metricsHook)
        let ownedProducer = newKafkaProducer(
          @["127.0.0.1:1"],
          tlsConfig = tlsConfig,
          saslMechanism = saslMechanism,
          metricsHook = metricsHook,
        )
        discard ownedProducer
        discard producer.send(
          "topic",
          @[byte(1)],
          headers = @[RecordHeader(key: "key", value: some(@[byte(2)]))],
        )

        var config = initConsumerConfig(
          bootstrapBrokers = @["127.0.0.1:1"], groupId = "group", topics = @["topic"]
        )
        config.tlsConfig = tlsConfig
        config.saslMechanism = saslMechanism
        config.metricsHook = metricsHook
        # Type identity only: this block is never run, and the injected-Cluster
        # overload rejects the two transport fields above at runtime.
        let sharedConsumer = newKafkaConsumer(config, cluster)
        let metadata: ConsumerGroupMetadata = sharedConsumer.groupMetadata()
        discard newKafkaProducer(cluster).sendOffsetsToTransaction(
            initTable[TopicPartition, int64](), metadata
          )
    )

  test "root allowlist hides protocol codecs and low-level helpers":
    check not declared(ApiKey)
    check not declared(ApiVersionRange)
    check not declared(supportedVersion)
    check not declared(ApiVersionsRequest)
    check not declared(ApiVersionsResponse)
    check not declared(MetadataRequest)
    check not declared(MetadataResponse)
    check not declared(FetchRequest)
    check not declared(FetchResponse)
    check not declared(Record)
    check not declared(RecordBatch)
    check not declared(PartitionMeta)
    check not declared(RequestHeader)
    check not declared(ResponseHeader)
    check not declared(readResponseHeader)
    check not declared(toIsolationLevel)
    check not declared(encode)
    check not declared(decode)
    check not declared(compressionFromAttributes)
    check not declared(compressRecords)
    check not declared(decompressRecords)

    check not declared(SaslState)
    check not declared(SaslSession)
    check not declared(validateAscii)
    check not declared(burnBytes)
    check not declared(constTimeEqual)
    # The session factory is an internal callback, not an application hook.
    check not compiles(
      block:
        let mechanism = newSaslPlain("user", "password", allowPlaintext = true)
        discard mechanism.newSession()
    )

    check not declared(BrokerConnection)
    check not declared(X509TrustAnchor)
    check not declared(newTrustAnchorStore)
    check not declared(InFlightWindow)
    check not declared(InFlightHandle)
    check not declared(ResponsePayload)
    check not declared(newInFlightWindow)
    check not declared(sendRequest)
    check not declared(writeRequest)
    check not declared(effectiveTimeoutMs)

    check not declared(emitMetric)
    check not declared(newKafkaApiError)
    check not declared(newKafkaCommitError)
    check not declared(newKafkaCommitSkippedError)
    check not declared(newKafkaOffsetFetchError)
    check not declared(newKafkaRebalanceCallbackError)
    check not declared(errorClassForContext)
    check not declared(errorClassToCstring)
    check not declared(requiresProducerIdReset)

  test "root allowlist hides Cluster, fetcher, and consumer internals":
    check not declared(CoordinatorKind)
    check not declared(PartitionLeader)
    check not declared(snapshotBrokers)
    check not declared(getBrokerById)
    check not declared(getPartitionLeader)
    check not declared(getPartitionLeaderId)
    check not declared(getCachedCoordinator)
    check not declared(getTopicPartitionsCopy)
    check not declared(pruneStaleBrokers)
    check not declared(maybeRefreshMetadata)
    check not declared(brokerForPartition)
    check not declared(isStaleLeaderCode)
    check not declared(invalidateLeader)
    check not declared(getTopicPartitionsRefreshing)
    check not declared(invalidateCoordinator)
    check not declared(invalidateTxnCoordinator)
    check not declared(coordinatorBroker)
    check not declared(txnCoordinatorBroker)
    check not declared(anyBroker)

    check not compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        discard cluster.snapshotBrokers()
    )
    check not compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        discard cluster.getBrokerById(0)
    )
    # These reads are deliberate negative pins: constructor-owned Cluster
    # settings must not be reachable through the application facade.
    check not compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        discard cluster.clientId
    )
    check not compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        discard cluster.allowAutoTopicCreation
    )
    check not compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        discard cluster.metadataDebounceInterval
    )
    check not compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        discard cluster.reconnectBackoffMs
    )
    check not compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        discard cluster.reconnectBackoffMaxMs
    )
    check not compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        discard cluster.connectTimeoutMs
    )
    check not compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        discard cluster.requestTimeoutMs
    )
    check not compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        discard cluster.metadataRefreshTimeoutMs
    )
    check not compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        discard cluster.metricsHook
    )

    check not declared(PartitionState)
    check not declared(RebalanceCheck)
    check not declared(Fetcher)
    check not declared(BatchParseStatus)
    check not declared(ParsedBatches)
    check not declared(defaultMaxTotalDecompressedBytes)
    check not declared(newFetcher)
    check not declared(bumpGeneration)
    check not declared(isRecordStaleGeneration)
    check not declared(currentPosEpoch)
    check not declared(snapshotPausedFlags)
    check not declared(setPartitions)
    check not declared(filterBatches)
    check not declared(computeNextFetchOffset)
    check not declared(parseRecordBatches)
    check not declared(countLegacyMagicBatches)
    check not declared(isLoopAlive)
    check not declared(forceRunningFalse)
    check not declared(advanceConsumedOffset)
    check not declared(fetchErrorCount)
    check not declared(recordQueueCapacity)
    check not declared(setLastError)
    check not declared(getFetchOffsets)
    check not declared(getConsumedOffsets)
    check not declared(getConsumedLeaderEpochs)
    check not declared(consumedOffsetsAfter)
    check not declared(consumedLeaderEpochsAfter)
    # Delivery stamps are owned by the fetcher and must not be constructible
    # by an application even though FetchedRecord itself is public.
    check not compiles(
      block:
        discard FetchedRecord(topic: "topic", partition: 0, offset: 0, generation: 1)
    )
    check not compiles(
      block:
        discard FetchedRecord(topic: "topic", partition: 0, offset: 0, posEpoch: 1)
    )
    check not compiles(
      block:
        discard FetchedRecord(topic: "topic", partition: 0, offset: 0, leaderEpoch: 1)
    )

    check not declared(OffsetFetchAction)
    check not declared(flushPendingAdvance)
    check not declared(armRebalanceFromCommitError)
    check not declared(classifyOffsetFetchError)
    check not declared(applyOffsetFetchRecovery)

  test "producer allowlist exposes application-facing symbols":
    # Positive compile-time pin: these names are the supported producer
    # contract and must remain available from the root facade alone. Calls are
    # used for procs so overload resolution is checked; only the calls inside
    # `compiles` blocks stay unexecuted (the toBytes checks below do run).
    check declared(KafkaProducer)
    check declared(PartitionStrategy)
    check declared(CustomPartitioner)
    check declared(ProduceStatus)
    check declared(ProduceResult)
    check declared(ProduceCallback)
    check declared(ErrorAction)
    check declared(FailedRecord)
    check declared(ErrorHandler)
    check declared(toBytes)
    check toBytes("hi") == @[byte('h'), byte('i')]
    check toBytes("").len == 0
    check declared(MetricsHook)
    check declared(ConsumerGroupMetadata)
    check compiles(initUnfencedConsumerGroupMetadata("group"))
    check compiles(initConsumerGroupMetadata("group", 1'i32, "member"))
    # The fenced constructor defaults only groupInstanceId; generationId and
    # memberId are required, so an unfenced commit must be asked for explicitly.
    check not compiles(initConsumerGroupMetadata("group"))
    check compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        let producer = newKafkaProducer(cluster)
        discard producer.isClosed()
        discard producer.state()
        discard producer.ownsCluster()
        discard producer.send("topic", @[byte(1)])
        discard producer.flush()
        discard producer.sendFireAndForget("topic", @[byte(1)])
        discard producer.close()
    )
    check compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        let producer = newKafkaProducer(cluster)
        discard producer.initTransactions()
    )
    check compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        let producer = newKafkaProducer(cluster)
        producer.beginTransaction()
    )
    check compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        let producer = newKafkaProducer(cluster)
        var offsets = initTable[TopicPartition, int64]()
        offsets[TopicPartition(topic: "topic", partition: 0)] = 1
        discard producer.sendOffsetsToTransaction(offsets, "group")
    )
    check compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        let producer = newKafkaProducer(cluster)
        let metadata = initUnfencedConsumerGroupMetadata("group")
        discard producer.sendOffsetsToTransaction(
          initTable[TopicPartition, int64](), metadata
        )
    )
    check compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        let producer = newKafkaProducer(cluster)
        discard producer.commitTransaction()
    )
    check compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        let producer = newKafkaProducer(cluster)
        discard producer.abortTransaction()
    )
    check compiles(
      block:
        let cluster = newCluster(@["127.0.0.1:1"])
        let producer = newKafkaProducer(cluster)
        discard producer.setDeadLetterQueue("dead-letter", producer)
        discard alwaysDeadLetter()
        discard alwaysDrop()
        discard deadLetterOnPermanent()
    )

  test "producer allowlist hides implementation mutators and state types":
    # The negative names are intentionally unresolved. `declared` checks the
    # facade's symbol table at compile time without executing test code.
    check not declared(failBatchFutures)
    check not declared(renewLingerTimer)
    check not declared(roundRobinIndex)
    check not declared(resolvePartition)
    check not declared(resetProducerId)
    check not declared(ensureIdempotenceInitialized)
    check not declared(nextSequence)
    check not declared(allocateInflight)
    check not declared(dequeueInflight)
    check not declared(reassignSequencesAfterReset)
    check not declared(canTransition)
    check not declared(tryTransition)
    check not declared(transitionToError)
    check not declared(refreshTxnCoordinator)
    check not declared(firstTxnErrorCode)
    check not declared(withTxnCoordinatorRetry)
    check not declared(markPartitionForTxn)
    check not declared(registerPendingTxnPartitions)
    check not declared(bumpEpoch)
    check not declared(ensureEpochBumped)
    check not declared(recordBatchV2HeaderBytes)
    check not declared(estimateRecordSize)
    check not declared(makeFailedRecord)
    check not declared(validateProducerConfig)
    check not declared(resolveTimestamp)
    check not declared(releasePendingRecordPermit)
    check not declared(requireRunning)
    check not declared(PendingMessage)
    check not declared(InflightBatch)
    check not declared(TransactionState)
    check not declared(producer)
    check not compiles(
      block:
        discard producer.failBatchFutures
    )
    check not compiles(
      block:
        discard producer.renewLingerTimer
    )

  test "TlsConfig is constructible via the facade alone":
    let cfg = newTlsConfig()
    check not cfg.skipVerify # verification must default ON
    check cfg.trustAnchors == nil

    let devCfg = newTlsConfig(skipVerify = true)
    check devCfg.skipVerify

  test "TlsConfig from the facade is accepted by the public constructors":
    # Compile-level pin: the facade's TlsConfig must be the same type the
    # constructors take (no duplicated/aliased declaration drift).
    let cluster = newCluster(@["127.0.0.1:1"], tlsConfig = newTlsConfig())
    check cluster != nil
    waitFor cluster.close()

  test "HostAllowlist is constructible and applied via the facade alone":
    let allow = newHostAllowlist(@[allowCidr("10.0.0.0/8")])
    check allow.hostAllowed("10.0.0.5")
    check not allow.hostAllowed("203.0.113.7")
    check hostAllowed(nil, "203.0.113.7")
    # A cluster-level option, independent of tlsConfig: it bounds plaintext
    # dials too.
    let cluster =
      newCluster(@["127.0.0.1:1"], tlsConfig = newTlsConfig(), hostAllowlist = allow)
    check cluster != nil
    waitFor cluster.close()

  test "requireTls is read-only on the facade":
    let m = newSaslPlain("user", "pw")
    check m.requireTls
    # Compile-level pin: the TLS requirement is fixed at construction and cannot
    # be relaxed or switched off afterwards (`requireTls = false` would drop
    # it); opt out of PLAIN's requirement with allowPlaintext at construction.
    check not compiles(m.requireTls = false)
