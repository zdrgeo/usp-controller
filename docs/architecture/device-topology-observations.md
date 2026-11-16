# Observation invalidation after object changes

Device Actors now record an observation cutoff for concrete ObjectCreation and
ObjectDeletion notifications. They already retained these messages as durable
events, but older twin readings could remain apparently fresh after a deletion
or recreation. Legacy remains the default runtime.

## Atomic notice handling

A concrete object path is a `Device.` object/instance path with a trailing dot;
searches, wildcards and malformed selectors are not projected. Unsupported shapes
still retain their full notification receipt and outbox event under the existing
contract. Generic Event/OnBoard messages are not topology triggers in this slice.

For a concrete object notice, the actor atomically saves its notification receipt,
full outbox event and latest topology cutoff. The cutoff links to that receipt's
message ID/fingerprint, and survives removal of the exported event. A failed save
leaves the prior twin view intact. An ambiguous committed save retains the cutoff
on restart. Duplicate message IDs with identical content do not move it, recreate
the event or refresh observations, including after export; conflicting content
still fails. A different message ID is a new notice under the existing receipt
identity contract.

Receipt recovery precedes the current-clock eligibility check for arrival time.
An identical retained notice can therefore be acknowledged after controller clock
rollback, including after an ambiguous admission reply or event export. It still
requires valid device/message structure, a nonzero arrival time and successful
reminder repair. A new message ID with a future arrival time is rejected; changed
content under a retained ID conflicts. Recovery does not rewrite state or move
the cutoff, last contact, observation freshness or outbox sequence.

This initial policy invalidates the whole tracked twin conservatively, including
parameters outside the changed object's subtree that could depend on topology.
It neither deletes historical reported values nor relinquishes desired state.
Creation unique keys are not copied into reported values. There is no automatic
USP Add/Delete, object recreation or corrective write retry.

## Fresh observations and clock rollback

The combined observation boundary is the greatest known mutation or topology
cutoff. Reported values received at or before it remain historical. Get projection
also requires a read started strictly after it; a pre-change Get cannot refresh
the twin just because its reply arrives after the notice. Full command evidence
is still retained. Newer valid Get/Notify/Report observations can restore freshness
under the existing policy, type, timing and once-per-revision rules.

A new invalidation cutoff covers all already-known reported receive times and the
previous cutoff, as well as current processing/ingress time. This prevents a
backwards controller clock from letting an earlier reading become fresh again
when time recovers. Mutating begins and definitive outcomes now capture this
high-water cutoff too, while their actual attempt/outcome timestamps remain
separate. Cutoffs are not device event times or claims about physical completion.
They may remain in the controller's future after clock rollback; equal-time
observations are stale. No automatic clock correction or history rewrite is added.

The value-free twin diff exposes `topology_barrier_at` and state
`topology_invalidated`. Its `mutation_barrier_at` is the effective mutation cutoff,
which can be later than an actual attempt/outcome time after clock anomalies.
Convergence and queued reconciliation checks use the combined boundary. A notice
itself does not count as a new parameter observation or advance ReportedRevision.

## Schema and rollout

Writers use schema 10. Schemas 1–9 load and upgrade on the next save without
inventing historical topology or high-water cutoffs. Already exported notices and
old duplicate callbacks cannot establish a missing historical cutoff. Acquire
fresh evidence during a reviewed upgrade when prior event/observation ordering
is unknown.

One optional topology marker and optional per-command mutation cutoffs are stored
in the existing bounded snapshot. The topology marker must reference a retained
matching notification receipt and have a supported kind/path/time. Mutation
cutoffs require mutating attempt/outcome provenance and cannot precede known
attempt/outcome timestamps. Metadata hidden in an older schema is rejected before
load/save. This is application consistency validation within the existing trusted
snapshot, not a separate authority or protection against arbitrary history edits.

Older binaries reject schema 10. Drain senders and roll forward or use a reviewed
state migration; all new actor saves use the version even without topology events.
Snapshot byte limits/reserves continue to apply, so added metadata can reduce
headroom. No extra backend commit, component, queue or delivery guarantee is added.

Validation on 2026-11-16: the full race-enabled Go suite and `go vet` passed.
Focused checks cover creation/deletion, failed/ambiguous atomic commits, cutoff
retention after export, duplicate stability, earlier-Get rejection and later
refresh, clock rollback at topology/begin/outcome boundaries, receipt/schema
validation and legacy upgrade without invented metadata. Long infrastructure fault
drills were not repeated for this actor-local state/observation change.
