# Device Actor host-local attempt budget

An enabled actor canary can bound simultaneous command-attempt sections across its
local actors by adding this field inside `actor_canary`:

```json
{
  "max_concurrent_attempts": 4
}
```

Omission or zero preserves existing unbounded-across-actors behavior. Nonzero
values require enabled actor mode and must be 1–256. This does not enable actors
in legacy mode. Runtime creates one shared budget for its local actor inventory;
direct actor users can construct `device.NewAttemptBudget(limit)` and share the
pointer through `Options.AttemptBudget`. An invalid zero-value budget is rejected
by the actor constructor.

## Execution boundary

Each Tick still handles deadline/attempt recovery and one event export before
considering new command publication. Existing per-device uncertainty, readiness,
policy, reconciliation and completion-capacity checks apply first. An eligible
actor then tries to acquire a local permit without waiting or creating another
worker queue. A full budget returns a successful maintenance turn while leaving
intent queued; no new message identity, durable `attempting` record or publication
is created. Cancelled context is returned before acquisition. A later reminder can
try again, or the unchanged durable deadline may expire the command.

A permit covers encoding and the begin/publish/result section, including backend
commits. It is released on normal return, failed or ambiguous commits, transport
errors, cancellation and panic unwind. Release is idempotent; a duplicate cleanup
cannot release another actor's permit. The existing durable begin fence remains
a prerequisite for sending, and failed/uncertain attempts retain their existing
no-replay behavior.

Once the attempt section ends, its permit is free even if that command is still
awaiting a USP response or OperationComplete. This is not a cap on outstanding
protocol operations. Generated reconciliation intent may already have been
persisted as queued before budget acquisition; the budget prevents beginning an
attempt, not bounded command/desired admission or intent planning.

Notifications, response/completion persistence, maintenance, event export and
read-only APIs do not acquire attempt permits. Pause and readiness continue to
prevent new execution; the budget does not resume a paused device, acknowledge an
unknown outcome, refresh observations or create another desired revision.

## Scope and visibility

The budget belongs to one controller Runtime/host instance. It is not persisted,
transferred with an actor or coordinated with other replicas. With R independently
configured hosts at limit N, aggregate concurrency can approach R×N. It is not a
site-wide quota, per-second rate limiter, stale-publisher fence, transport owner
lease or exactly-once guarantee. Actor serialization and snapshot fencing remain
responsible for command state. Process replacement resets local permits without
replaying durable attempts. Existing external-send uncertainty still applies.

No FIFO or starvation-free scheduling across devices is promised; reminders keep
their current bounded cadence and deterministic stagger. Sustained contention can
delay or expire queued work. HTTP admission/readiness is not coupled to a full
attempt budget: durable bounded intent can still be accepted while execution is
busy. No additional dispatch goroutine, timer or SQL claiming path is introduced.

When configured, authenticated `/v2/metrics` adds three label-free local series:

- `usp_device_attempt_slots_limit`: configured maximum, a gauge.
- `usp_device_attempt_slots_in_use`: currently held permits, a gauge.
- `usp_device_attempt_budget_denied_total`: eligible turns left queued because
  the local budget was full, a counter.

Scraping these reads only concrete local counters and cannot acquire permits or
invoke an actor. Values can change during a scrape and reset on process restart.
No device identity is exported. A denied turn can still be an `actor/Tick/ok`
operation because maintenance completed without publication; command states and
attempt metrics remain distinct. These are per-instance resource observations,
not authority to dispatch or retry a command.

## Validation

On 2026-11-10, `go test -race -timeout=2m ./...` and `go vet ./...` passed. Focused
checks cover two actors contending for one shared slot, no begin/save/publication
on denial, cancellation before acquisition, progress after release, idempotent
release protecting another holder, and cleanup/no replay after begin/result save
failure or publisher panic. Existing configuration and canary end-to-end checks
exercise bounds and actual runtime wiring. Storage/transport protocols and schema
5 are unchanged; the long infrastructure fault/compaction suite was not rerun.
Legacy remains the default.
