# Observation freshness after mutating attempts

Device Actors now treat observations received at or before a known mutating
attempt as historical evidence. Set, Add, Delete and Operate may change device
state, including paths beyond the explicit target. The barrier therefore applies
to the whole reported twin. Legacy remains the default runtime; existing actor
allowlists, reconciliation policy and dispatch permissions are unchanged.

The initial boundary is the durable begin commit, not MQTT acceptance.
[Definitive outcomes](device-mutation-outcomes.md) now advance it again when
processed, invalidating observations collected during the attempt. A committed attempt may have reached the device even if its reply was
lost. A failed begin that did not commit leaves freshness unchanged. A committed
begin with an ambiguous reply invalidates earlier observations even though the
controller does not publish in that invocation. Failure, timeout, cancellation of
later work and operator unknown acknowledgement do not restore old freshness.

The barrier is rebuilt on each load from retained command attempt timestamps and
sequences. It uses the greatest known mutation timestamp, so a later attempt with
a backwards controller clock cannot lower it. Read-only command attempts do not
advance it. Begin times already exist in attempt provenance. Schema 9 now records
[mutation outcome times](device-mutation-outcomes.md) with their evidence.
No extra backend operation or retry is introduced; fencing protocols are unchanged.

## Twin and reconciliation behavior

An observation is eligible only if its ordinary freshness window is open and its
controller receive time is strictly later than the mutation barrier. Get projection
also requires its attempt start to be strictly later than that barrier. Equal times
are conservative uncertainty. Reconciliation planning, queued write validation,
bootstrap-read planning and convergence use the same rule. A new desired revision
or acknowledgement alone cannot authorize a write using pre-mutation observations.

Twin reads retain historical values, source and receive time, but expose a zero
`FreshUntil` for observations invalidated by the barrier. Stored evidence is not
rewritten, and a read does not save state or advance `ReportedRevision`. Like
ordinary time expiry, freshness/convergence is a derived view and must not be
cached based solely on the reported revision. A Set response can therefore leave
convergence `unknown` until suitable newer evidence arrives. Optional bounded
[bootstrap](device-observation-reads.md) and [verification](device-verification-reads.md)
reads continue to obey their existing once-per-revision limits.

A later ValueChange, trusted Report or qualifying correlated Get can refresh the
twin. Existing Get ordering, timestamp-tie, scope/type and freshness checks still
apply; this change does not relax them. Newer observations are evidence of what
was seen, not proof that a command caused the values or that the device is fully
settled. There is no claim of device-side causal ordering for delayed notifications
or asynchronous operations. Device timestamps are not trusted.

## Compatibility and limits

Attempt provenance was introduced in schema 6. Older attempts without recorded
times/sequences do not establish a barrier: their start time cannot be invented
from a deadline or current time. Review migrated state and acquire fresh evidence
when its relationship to historical mutations is unknown. Restoring snapshots
preserves the existing provenance; the barrier is reconstructed from that state.
It is not independent protection against an unauthorized rewrite of history.

Older binaries do not apply this freshness rule. Roll forward to retain the safer
behavior; no automatic history pruning or schema downgrade is added. The maximum
retained command count already bounds reconstruction work.

Validation on 2026-11-14: the race-enabled full Go suite and `go vet` passed.
Focused checks cover successful, uncertain, failed and ambiguous begin outcomes,
clock rollback, restart reconstruction, historical values without read-side saves,
equal-time rejection, bounded post-mutation Get refresh and read-only attempts.
Existing acknowledgement tests now require fresh evidence after a mutation. Long
infrastructure fault drills were not repeated for this actor-local derived view.

The [twin diff](device-twin-diff.md) now exposes observation eligibility without
values. Controller receive times later than the current controller time are also
unusable after clock rollback; they remain historical evidence until time/order
checks allow newer usable observations.
