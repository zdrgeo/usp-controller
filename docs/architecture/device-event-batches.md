# Bounded Device Actor event-export batches

The optional actor configuration accepts `event_exports_per_turn`:

```json
{
  "actor_canary": {
    "event_exports_per_turn": 8
  }
}
```

This fragment belongs inside an otherwise valid, explicitly enabled actor canary.
Omission or zero retains the current default of one event per turn. Configured
values must be 1–32 and require actor mode. Direct actor users can set
`Options.EventExportsPerTurn`; constructor zero also means one. Legacy remains
the default, and example deployment configuration does not enable batching.

## FIFO handoff and commit boundary

Maintenance tries the oldest pending events sequentially, stopping at the batch
limit, the first handoff error or the shared publication deadline. All handoffs
in the turn use one five-second context budget, rather than five seconds per
event. Publishers must honor cancellation. Backend removal uses the existing
parent context and backend timeout; the publication budget is not a promise about
total actor-turn latency.

Successfully handed-off events form a contiguous FIFO prefix. The actor removes
that prefix with one snapshot save, including when a later event in the batch
fails handoff. The failed event and untouched tail remain pending. If no event
was handed off, no removal save is attempted. There is no concurrent handoff,
secondary worker queue, within-turn retry or consumer-completion guarantee.

A crash or failed/ambiguous removal commit can repeat successfully handed-off
events. Stored epoch/sequence identities and payloads are retained for those
retries; consumers still deduplicate. Notification receipts are not removed. A
confirmed prefix-removal commit prevents that prefix from being resent merely
because a later event failed.

## Command dispatch and errors

A failed or ambiguous removal save always stops command dispatch from the locally
modified snapshot. It is never retried in the same turn. If both publication and
removal fail, both errors are retained. With default scheduling, a publication
error also stops dispatch, even when prefix removal succeeded.

With [dispatch with event backlog](device-outbox-dispatch.md) enabled, command
work may proceed after a handoff error only if the parent context is active and
any successful prefix removal committed. The remaining snapshot is then trusted,
including the pending failed event. The handoff error is still returned alongside
any command failure. Existing deadline, pause, uncertainty, readiness, policy,
capacity and host-budget checks remain unchanged. Each turn still publishes at
most one device command. Export continues even while command dispatch is blocked.

More events can now drain during a healthy reminder turn, but bounded snapshot
capacity, event admission and completion reserves remain unchanged. The option
neither accepts unlimited notification bursts nor prunes retained receipts.
Metrics observe each individual event handoff and the single prefix removal save;
a failed Tick can include successful handoffs and command work.

## Compatibility and validation

Snapshot schema 9, stable event envelopes, backend fencing and delivery guarantees
are unchanged. All hosts serving the same cohort should use reviewed consistent
policy; the batch count is a local scheduling limit, not a distributed quota.

Validation on 2026-11-14: the full race-enabled Go suite and `go vet` passed.
Focused checks cover the default and configured batch bound, one removal commit,
shared publication deadline, partial-prefix failure/recovery, stable FIFO retry
identities, optional command progress and failed/ambiguous removal commits. The
existing canary integration check verifies actual configuration wiring by draining
two notifications in one turn. Long infrastructure fault drills were not repeated
for this local scheduling change.
