import std/random

var
  rng {.threadvar.}: Rand
  seeded {.threadvar.}: bool

proc jitteredBackoff*(baseMs: int32, attempt: int, maxShift: int = 5): int32 =
  ## Exponential backoff with ±25% symmetric jitter (neither the AWS "full jitter"
  ## scheme, `random(0, exp)`, nor "equal jitter", which never exceeds the slot).
  ## `attempt` is 1-indexed. Only the exponential slot
  ## is capped at `baseMs * 2^maxShift`: jitter can push the returned delay to
  ## ~1.25x, then it is clamped to int32.high. Returns milliseconds, at least 1ms.
  if not seeded:
    rng = initRand()
    seeded = true

  let safeAttempt = max(attempt - 1, 0)
  let shift = min(safeAttempt, maxShift)

  # Compute in int64 and clamp before narrowing: overflow would raise RangeDefect
  # (checked build) or wrap to a garbage delay (-d:danger).
  let expVal = min(baseMs.int64 * (1'i64 shl shift), int32.high.int64)
  let jitterRange = max((expVal div 2).int, 1)
  let jitter = rng.rand(jitterRange) - jitterRange div 2
  return int32(max(1'i64, min(expVal + jitter.int64, int32.high.int64)))

proc retryDelay*(baseMs: int32, attempt: int, throttleMs: int32 = 0): int32 =
  ## Fold a broker-quota throttle into the retry backoff as a wall-clock *overlap*,
  ## not a second throttle-honour path: compliance is owned by the broker layer
  ## (writeTypedRequest.awaitThrottle gates every typed request before the wire), so
  ## this only affects timing. `max` (not sum) runs the backoff in parallel with the
  ## wait and leaves the un-throttled case identical to `jitteredBackoff`.
  max(jitteredBackoff(baseMs, attempt), throttleMs)
