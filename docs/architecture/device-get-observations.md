# Device Actor Get observations

Correlated Get responses can now populate the managed reported twin. Set
`actor_canary.twin.observe_get_responses` to `true` only for a reviewed actor
canary policy. It defaults to false; legacy remains the default runtime. This
adds no automatic reads, retries, command claiming or new dispatch permissions.
The existing explicit device allowlist and managed-path policy still apply.

## Evidence and freshness

Every new command attempt records `attempted_at` and a monotonically increasing
per-device `attempt_sequence` in the durable begin commit, before publication.
The deadline is checked again immediately before begin because export and other
maintenance may have consumed time. Unknown outcomes retain their provenance.

A Get response can update the twin only when it is correlated to the latest
attempt, has a controller receive time between attempt start and now, and is
processed before attempt start plus the configured freshness interval. Existing
observations received at or after that attempt started win. This conservatively
excludes delayed reads after a newer command, even if that newer command failed.
An observation uses source `usp-get-response`, the response receive time, and
`fresh_until` anchored to attempt start. Arrival does not extend the read window.
The [mutation barrier](device-observation-barrier.md) also requires observations
to be newer than known mutating attempts. It is an observation, not proof that a
later write succeeded.

Projection follows the Get response mapping described in
[TR-369 Get](https://usp.technology/specification/#sec:get). Each promoted value
must belong to an original requested path and a successful requested-path result.
Only concrete object or parameter requests, concrete resolved objects and bare
parameter names are supported. Object requests respect `max_depth`. Search and
wildcard paths are retained as command evidence but skipped for projection.
Each value must also match an exact reviewed managed path and its configured type.
Contradictory values for one path suppress that path. Successful paths in a
partially failed response may update reported state; the command retains its
failed outcome and full protocol response.

## Commit and capacity

Command evidence, outcome and optional observations share one atomic snapshot
save. Reported revision advances once if values are promoted. Valid response
contact time may advance last contact even when no values qualify. Duplicate
responses neither save again nor refresh observations.

If optional observations exceed parameter/revision capacity they are skipped.
If encoding the combined snapshot exceeds the byte limit, projection and its
contact update are discarded locally and the command evidence is encoded again.
There is exactly one backend save attempt. Backend failures, including ambiguous
commit replies, never trigger a fallback write or automatic retry. If evidence
alone exceeds capacity, the existing capacity error remains. A later duplicate
does not backfill skipped values; obtain a new read instead.

## Schema and rollout

Attempt provenance introduced schema 6; current writers use
[schema 8](device-verification-reads.md). Schemas 1–7 load and upgrade on the next save without
inventing timestamps or sequences for historical attempts. Those attempts can
still settle and retain evidence, but cannot refresh the twin. New attempt
provenance is validated for sequence uniqueness, complete retained history and
compatible command state. There is no automatic history pruning.

Older binaries reject schema 8. Drain senders and roll forward or use a reviewed
state migration; do not roll an older writer over upgraded state. The schema
change applies to actor saves even when projection is disabled. No storage
backend, fencing or infrastructure recovery protocol changes in this slice.

Validation: `go test -race -timeout=2m ./...` and `go vet ./...` passed. Focused
checks cover scope/type/depth filtering, stale and conflicting observations,
legacy provenance, partial responses, failed and ambiguous commits, capacity
fallback with one save, duplicate stability and deadline expiry after export.
The long infrastructure fault suite was not rerun for this actor-local change.

[Bounded automatic observation reads](device-observation-reads.md) are now a separate
opt-in opportunity to acquire missing desired-path observations before reconciliation.
