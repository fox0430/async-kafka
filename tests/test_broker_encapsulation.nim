## Gate for `Broker`'s reconnect-invariant fields.
##
## `apiVersions` and `addressDirty` used to be exported mutable fields: any
## module could re-target the schema version every later request resolves
## against, or flag/clear the address-change state the connect fast paths and
## `connectImpl` pair on. Both are now private: `addressDirty` is reachable only
## through `isAddressDirty` / `markAddressDirty` (its only production write site,
## `cluster.getOrCreateBroker`); `apiVersions` has no accessor and stays internal.
##
## The `static` block is the gate: it compiles only while the fields stay
## private from this module, and dies if either regains a `*`. The runtime test
## is the positive control, pinning that the accessors actually work.

import std/[tables, unittest]

import async_kafka/broker

static:
  var b: Broker
  # Negative: no external read or write of the invariant fields.
  doAssert not compiles(b.addressDirty)
  doAssert not compiles(b.addressDirty = true)
  doAssert not compiles(b.apiVersions)
  doAssert not compiles(b.apiVersions.clear())
  # Positive: the accessors are the external surface.
  doAssert compiles(b.markAddressDirty())
  doAssert compiles(b.isAddressDirty())

suite "Broker encapsulation":
  test "addressDirty is reachable only through its accessors":
    let b = newBroker(0'i32, "127.0.0.1", 9092'i32, "encapsulation-test")
    check not b.isAddressDirty()
    b.markAddressDirty()
    check b.isAddressDirty()
