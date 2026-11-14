# Device Actor host-local attempt rate

An enabled actor canary can optionally bound how quickly its local actors reserve
new command attempts. Configure both fields inside `actor_canary`:

```json
{
  "attempt_rate_per_second": 10,
  "attempt_rate_burst": 4
}
```

Both fields omitted or zero disables rate limiting. When enabled, the refill rate
must be 1–10000 tokens per second and the burst capacity 1–256. Partial settings,
negative values and nonzero settings without actor mode are rejected. Legacy
remains the default; example deployment configuration does not enable this budget.

Runtime shares one token bucket across its local actor inventory. Direct actor
users can construct `device.NewAttemptRateBudget(rate, burst, now)` and share it
through `Options.AttemptRateBudget`. The constructor rejects invalid settings and
a missing clock; actors reject a zero-value budget. Production uses `time.Now`,
including its monotonic clock. Backwards clock readings do not refill tokens or
move the refill boundary backwards; idle refill is capped at burst capacity.

## Dispatch boundary

Existing maintenance and a [bounded event batch](device-event-batches.md) run first. Per-device uncertainty,
readiness, pause, generated-command policy checks, completion capacity and the
optional concurrent-attempt permit still gate execution. After acquiring any
concurrency permit and rechecking the durable deadline, the actor tries to reserve
a rate token without waiting. Rate denial releases the concurrency permit and
leaves the command queued without a new message identity, attempt sequence,
`attempting` save or publication. Queued generated intent may already have been
persisted by planning. A later reminder can try again; ordinary deadlines still
expire while waiting. No second worker queue, timer or SQL claim is introduced.

One token is charged before encoding and durable begin. Tokens are never refunded,
including on encoding errors, failed/ambiguous begin commits, cancellation after
reservation, transport errors or panic. This conservatively limits reservations
without turning uncertain storage or delivery outcomes into retries. Publication
still requires a successful durable begin. A cancelled context is checked before
rate reservation. Responses, notifications, export and read-only operations do
not reserve tokens. Explicit commands, bootstrap reads, generated Sets and
verification reads share the same rate budget.

## Limits and visibility

This is an in-memory, per-host resource budget, independent of the concurrent
attempt budget. Both can be configured, either can be omitted. It limits attempt
reservations, not outstanding USP responses, MQTT packets, notification traffic
or exact transmission times: commits can delay publication after a reservation.
The bucket starts full and resets on process replacement. Multiple hosts each
have their own rate and burst. It is not a durable site-wide quota, fair scheduler,
stale-writer fence or delivery guarantee. Reminder cadence can make actual
throughput much lower than the configured refill rate; sustained contention can
starve or expire commands. Durable attempt history is never replayed after restart.

When configured, authenticated `/v2/metrics` exposes four label-free local series:

- `usp_device_attempt_rate_per_second`: configured refill rate, gauge.
- `usp_device_attempt_rate_burst`: configured burst capacity, gauge.
- `usp_device_attempt_rate_granted_total`: reservations, including failed begins.
- `usp_device_attempt_rate_denied_total`: eligible turns left queued by rate denial.

The counters reset with the process. Scrapes read a passive snapshot without
calling the clock, refilling tokens or invoking actors. A rate-denied Tick can
return `ok`; denial is visible separately from command and publication outcomes.

Validation: the race-enabled full Go suite and `go vet` passed. Focused checks
cover concurrent burst bounds, fractional/backwards/idle clock behavior, shared
actor enforcement before begin, cancellation, no refund after a failed begin,
permit release on denial, actual runtime wiring and passive metrics. Storage
protocols and snapshot schema 8 are unchanged; long infrastructure fault drills
were not repeated for this local scheduling change.
