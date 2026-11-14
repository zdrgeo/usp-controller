# Observation freshness after definitive mutation outcomes

Device Actors now persist `mutation_outcome_at` with definitive Set/Add/Delete/
Operate evidence. It is the controller processing time, not a device clock or a
claim about when physical work finished. The observation barrier uses the greatest
known mutation begin or outcome time. Legacy remains the default runtime; no new
execution permissions or automatic retries are introduced.

A correlated success or failure response records the time atomically with the
command result and transition. Failure may still have partial device effects.
An asynchronous Operate acceptance awaiting OperationComplete is not a definitive
outcome. A matching OperationComplete records the time atomically with completion
evidence, its notification receipt and durable event. Completion-before-response
uses the completion time; a later Operate response retains evidence without
moving the barrier. Controller clock rollback cannot put the outcome before its
known attempt start or lower the derived device-wide barrier.

Duplicate responses, duplicate completions under the same message ID and matching
completion evidence under a different message ID do not refresh the timestamp.
Failed uncommitted saves leave the prior barrier intact. Ambiguous committed
saves retain the outcome boundary after restart; no retry can move it merely by
repeating the same evidence.

## Observations and reads

An observation made during a mutating attempt is insufficient to establish fresh
state after its definitive outcome. Twin reads retain the historical value but
expose zero `FreshUntil` when its receive time is at or before the barrier.
Convergence and reconciliation remain unknown/waiting until suitable newer
evidence arrives. The existing one-write-per-desired-revision rule is unchanged.

A correlated Get must now have started strictly after the known barrier as well
as satisfying its other projection checks. A read started before a late outcome
cannot refresh the twin merely because its reply arrived afterward. Its command
result still settles and retains full evidence. Timestamp ties remain conservative.
Later Get/Notify/Report observations do not prove device-side causality, completion
of background work or continuous convergence. This change does not suspend all
observations while an operation is running; it invalidates in-flight observations
when definitive evidence is processed.

Command summaries and single-command reads expose optional `mutation_outcome_at`.
It participates in the command view fingerprint. Zero/missing means no known
recorded outcome time; it is not proof that no result or device effect exists.

## Schema and rollout

Writers now use schema 9. Schemas 1–8 load and upgrade on the next save, preserving
all evidence without inventing historical outcome times. A newly processed late
outcome can record an actual processing time even for a legacy attempt whose
start time is unavailable. Already retained historical results/completions and
their duplicate callbacks do not acquire an invented new time.

The loader rejects outcome metadata hidden in an older schema, nonmutating or
nonterminal commands carrying it, missing evidence/identity, and times before a
known attempt start. The marker is checked before saves as well as on loads.
The device-wide barrier remains derived from retained command history; it is not
a second independently authoritative store.

Older binaries reject schema 9. Drain senders and roll forward or use a reviewed
state migration; do not roll an older writer over upgraded snapshots. All actor
saves use schema 9 even when reconciliation/projection are disabled. Storage
backend, fencing and delivery guarantees are unchanged. History remains bounded
and retained; no pruning or correction loop is added.

Validation on 2026-11-14: the race-enabled full Go suite and `go vet` passed.
Focused checks cover atomic outcomes across failed/ambiguous saves, post-outcome
freshness, duplicate stability, completion/response ordering, acceptance versus
completion, metadata inspection and schema downgrade rejection, plus an in-flight
Get crossing a late outcome. Long infrastructure fault drills were not repeated
for this actor-local state and eligibility change.
