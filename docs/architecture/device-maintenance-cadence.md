# Device Actor maintenance cadence

An enabled actor cohort can configure its recurring maintenance period:

```json
{
  "actor_canary": {
    "maintenance_interval_seconds": 7
  }
}
```

This fragment belongs inside a valid, explicitly enabled actor configuration.
Omission or zero preserves the 10-second default. Nonzero values must be 1–300
seconds and require actor mode. Legacy remains the default runtime, and example
configuration does not enable a different cadence.

Runtime uses the same configured period for both single-host Dapr state and the
etcd-backed replica profile's Dapr reminders. Deterministic per-device initial
staggering remains within one configured period. Repeated activation/admission
checks an existing matching registration without resetting its next firing. A
wrong period, TTL or failed reminder lookup remains an error; configuration alone
does not overwrite the registration.

## Existing cohorts and scheduling limits

Changing configuration is not a reminder migration. If a durable reminder already
has another period, activation and admission paths that validate it fail rather
than silently rewriting it. The existing reminder is not deleted or stopped by
that error and may still fire. Do not rely on a conflict as a global execution
fence.

For an existing cohort, first coordinate drain/retirement of its serving hosts,
preserve snapshots and inspect the scheduler registration. Use a reviewed explicit
administrative reminder replacement for the intended cadence, then start hosts
with matching configuration, verify activation/placement and resume deliberately.
All replicas serving one cohort must agree. Do not clear actor state, rewrite
accepted deadlines or republish attempted commands as part of a cadence change.
This implementation does not perform that administrative replacement.

The setting controls maintenance Tick cadence, not the reminder-repair loop's
30-second delay after each sweep. A shorter period increases offered wakeups; it
does not parallelize turns for one actor or guarantee dispatch/expiry timing.
Each Tick still attempts at most one device command and its configured bounded
event-export batch. Host rate/concurrency budgets, pause, readiness, uncertainty,
policy, capacity and durable begin-before-publish remain unchanged.

Long periods can delay queue progress, export and deadline maintenance. Review
them against absolute command deadlines, managed observation freshness and twin
command timeouts. A slower cadence can leave work expired or observations unusable;
it never extends their persisted time bounds or authorizes a blind write.

Snapshot schema 9, reminder identity, event envelopes, storage fencing and delivery
semantics are unchanged. Direct backend users retain the existing explicit
`period` constructor argument.

Validation on 2026-11-15: the full race-enabled Go suite and `go vet` passed. Existing
checks validate no overwrite/reset on mismatched or matching reminders. The canary
integration check now exercises a configured 7-second period and bounded initial
staggering; configuration checks retain the 10-second default and reject out-of-range
settings. Long infrastructure fault drills were not repeated for this wiring change.
