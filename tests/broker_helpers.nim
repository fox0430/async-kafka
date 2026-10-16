## Test-only accessors for `Broker` internals. Reaches private state via
## `std/importutils`' `privateAccess`.

import std/importutils

import chronos

import async_kafka/broker {.all.}
import async_kafka/connection {.all.}
import async_kafka/errors

proc connForTest*(b: Broker): BrokerConnection {.inline, raises: [].} =
  ## Test-only accessor for the broker's current connection, so a reconnect test
  ## can capture the pre-reconnect connection and assert `connectImpl` disposed it
  ## (not leaked) when reopening the socket.
  privateAccess(Broker)
  b.conn

proc acquireWriteLockForTest*(
    conn: BrokerConnection
) {.async: (raises: [CancelledError]).} =
  ## Test-only: hold the connection's per-write serialization lock so a test
  ## can stand in for an in-flight write and assert close() waits for it
  ## before tearing the writer down. Uses the raw lock, not the production
  ## cancel-safe acquire, so it does not participate in self-release.
  privateAccess(BrokerConnection)
  await conn.writeLock.acquire()

proc acquireWriteLockGuardedForTest*(
    conn: BrokerConnection
) {.async: (raises: [CancelledError]).} =
  ## Test-only: production `acquireWriteLock`, including the cancel-after-take
  ## self-release. Pair with `armConnAcquirePark` to cancel on that branch.
  await conn.acquireWriteLock()

proc acquireInflightForTest*(
    conn: BrokerConnection
) {.async: (raises: [CancelledError]).} =
  ## Test-only: production `acquireInflight`, including the cancel-after-take
  ## self-release. Pair with `armConnAcquirePark` to cancel on that branch.
  privateAccess(BrokerConnection)
  await conn.inFlight.acquireInflight()

proc writeLockHeldForTest*(conn: BrokerConnection): bool {.raises: [].} =
  ## Test-only: whether `writeLock` is currently held.
  privateAccess(BrokerConnection)
  conn.writeLock.locked()

proc inflightAvailableForTest*(conn: BrokerConnection): int {.raises: [].} =
  ## Test-only: free in-flight permits. A leaked cancel-after-take leaves this
  ## below `maxInFlight` with no matching slot.
  privateAccess(BrokerConnection)
  conn.inFlight.available.availableSlots

proc releaseWriteLockForTest*(conn: BrokerConnection) {.raises: [].} =
  ## Test-only: release the lock taken by acquireWriteLockForTest.
  privateAccess(BrokerConnection)
  try:
    conn.writeLock.release()
  except AsyncLockError:
    discard

proc setInFlightForTesting*(b: Broker, n: int) {.inline, raises: [].} =
  ## Test-only escape hatch. Production code must not call this; use
  ## sendTypedRequest, which inc/dec the counter under try/finally.
  privateAccess(Broker)
  b.inFlightCount = n

proc setFireAndForgetForTesting*(b: Broker, n: int) {.inline, raises: [].} =
  ## Test-only escape hatch standing in for acks=0 writes on the wire.
  ## Production code must not call this; the acks=0 branches of produce /
  ## produceWrite inc/dec the counter under try/finally.
  privateAccess(Broker)
  b.fireAndForgetCount = n

proc failuresForTest*(b: Broker): int {.inline, raises: [].} =
  ## Test-only: the reconnect-failure counter that drives the backoff schedule.
  ## A failure path that forgets to record leaves this at 0 and the caller
  ## retries without any delay.
  privateAccess(Broker)
  b.consecutiveFailures

proc setRetiredForTesting*(b: Broker) {.inline, raises: [].} =
  ## Test-only: mark the broker retired WITHOUT closing its live connection, so a
  ## test can pin the `not b.retired` fast-path guard directly. Production code
  ## retires via retire().
  privateAccess(Broker)
  b.retired = true

proc ensureConnectedForTest*(
    b: Broker
) {.async: (raises: [KafkaError, CancelledError]).} =
  ## Test-only wrapper around the private `ensureConnected` so a test can
  ## assert its fast path does not slip past an in-flight handshake.
  await b.ensureConnected()

proc acquireConnectLockForTest*(b: Broker) {.async: (raises: [CancelledError]).} =
  ## Test-only: hold the broker's connect lock to stand in for a connectImpl
  ## that is mid-handshake, so ensureConnected callers must park on it.
  privateAccess(Broker)
  await b.connectLock.acquire()

proc releaseConnectLockForTest*(b: Broker) {.raises: [].} =
  ## Test-only: release the lock taken by acquireConnectLockForTest.
  privateAccess(Broker)
  try:
    b.connectLock.release()
  except AsyncLockError:
    discard
