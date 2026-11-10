# Command progress during event handoff failures

An enabled actor canary can set `dispatch_with_event_backlog` to `true` to let
command dispatch proceed after an event publisher reports failure. It defaults
to false; legacy remains the default runtime. Direct actor users can set
`Options.DispatchWithEventBacklog`. This changes scheduling only, not event
retention, durability or acknowledgement requirements.

Each Tick still performs maintenance and tries to export the oldest event first.
If the publisher fails, the stored event and notification receipt remain intact.
With the option enabled and the parent context still active, the actor can then
consider command work from that unchanged snapshot. Existing pause, uncertainty,
readiness, policy, deadline, completion-capacity, concurrency and rate gates still
apply. There is at most one event handoff and one command publication per turn.
The option does not create a worker, skip older events, retry the handoff within
the turn or bypass durable begin-before-publish.

A publisher error can mean the destination accepted the event but lost its reply.
The event stays pending and later reminders retain its stable epoch/sequence.
Consumers must continue deduplicating these possible duplicate handoffs. If the
publisher reports success but removing the event fails or has an ambiguous commit
reply, the turn stops immediately. It must not dispatch from a snapshot whose
outbox has been changed locally without confirmed persistence. Decode, load and
other storage failures also stop the turn. No backend operation is retried to
recover optional command progress.

## Errors and bounded backpressure

Tick preserves the event handoff error even if a command is durably attempted.
If command work also fails, both errors are returned together. A failed Tick
therefore does not establish that nothing was published; inspect the durable
command receipt. Repeated reminders keep the usual no-replay behavior for
attempted commands. Existing event handoff and command transport metrics remain
separate, and Tick remains an error while event handoff is failing.

Export still runs synchronously and can consume up to its existing five-second
budget before dispatch. The command deadline is rechecked before begin. This
option provides continued progress during handoff errors, not independent event
and command latency or unlimited buffering. A growing durable outbox still counts
against hard snapshot/event capacity and the configured completion reserve;
capacity can prevent notification admission or further command work. No event
is dropped and no receipt is pruned.

The default false setting keeps the existing behavior: any export failure stops
command dispatch. Nonzero configuration requires enabled actor mode. The setting
does not enable actor dispatch, resume paused devices or alter the device allowlist.
Snapshot schema 8, backend fencing and delivery guarantees are unchanged. During
rollout, monitor pending-event age/count, handoff failures, command receipts and
capacity; eventual fleet projections can lag while the event destination is down.

Validation: the full race-enabled Go suite and `go vet` passed. Focused checks
cover default blocking versus opt-in progress, stable retained events across
restart/recovery, no command replay, failed/ambiguous removal commits, cancellation
and preservation of simultaneous handoff/command-save failures. Long infrastructure
fault tests were not repeated for this actor-local scheduling change.
