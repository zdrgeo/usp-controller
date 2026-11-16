# Configurable actor observation freshness

An enabled actor cohort can set `observation_freshness_seconds` inside its existing
`actor_canary` configuration, for example `"observation_freshness_seconds":75`.
Omitted or zero preserves the five-minute (300-second) default. Explicit values
must be 1–86,400 seconds and require actor mode. This does not enable actors,
managed twins, Get projection, reconciliation or periodic reads. Legacy remains
the default and the example deployment remains unchanged.

The runtime passes the selected duration to every allowlisted Device Actor:

- New ValueChange and trusted Report observations expire relative to their
  controller receive time.
- Eligible correlated Get observations expire relative to durable attempt start,
  and processing must still occur within that window. Arrival does not extend it.
- Twin reads, diagnostics, reconciliation and optional read planners continue to
  use the saved observation expiry along with type, receive-order, mutation and
  lifecycle checks. The fleet projection uses the earliest fresh expiry as before.

This setting changes observation eligibility, not accepted command deadlines,
transport budgets, reminder cadence, event retention or command retry permission.
A window shorter than maintenance/response latency can leave too little time to
project a read or authorize reconciliation. A longer window accepts older evidence;
choose it for the reviewed device model and parameter scope. It is one duration
for the cohort, not a per-parameter rule.

## Configuration changes and retained evidence

Restart serving hosts with consistent configuration. New Get attempts now
[pin their window at durable begin](device-get-observation-window.md), so later
configuration cannot extend or truncate them. Resolve legacy in-flight reads
before changing freshness if their compatibility fallback is unsuitable: attempts
without a pinned window still use the current duration anchored to their stored
start. Configuration never changes attempt identity or authorizes a resend.

Existing observations keep their persisted `fresh_until` across restart, config
change, duplicate receipts and snapshot migration. Shortening the configured
window does not retroactively shorten them; lengthening it does not refresh them.
For a shorter-window rollout, obtain fresh observations under the new configuration
before resuming generated writes that must use that tighter bound. No timestamp
rewrite, TTL migration or synthetic reported revision is performed.

Current writers use snapshot schema 12 for pinned Get windows; older binaries
reject upgraded state. State backend and delivery protocols are unchanged. Direct
actor users retain `Options.Freshness`; application configuration governs the
canary runtime's option. Validation covers default preservation, bounds and legacy
rejection, plus a configured notification through runtime/Dapr callbacks whose
saved expiry is exactly 75 seconds after receive time. Existing actor checks cover
Get attempt anchoring and duplicate stability; infrastructure fault drills are
not repeated for this configuration wiring.
