# Bounded observation reads before reconciliation

The optional managed-twin policy now accepts `read_before_reconcile` (default
false). It requires both `reconcile` and `observe_get_responses`. Legacy remains
the default runtime; the explicit actor device allowlist and reviewed path policy
still apply. No configuration example enables this feature by default.

When a desired revision lacks fresh, correctly typed observations, the actor can
queue one Get containing only its missing or stale desired parameter paths. Paths
are concrete and sorted. Fresh paths are omitted. A policy-blocked desired path
prevents automatic reads as well as writes. Explicit commands take precedence:
the planner runs only when the command queue is empty and no unresolved attempt
blocks dispatch. Pausing, transport readiness, local attempt budgets, ordinary
capacity limits and durable begin-before-publish remain unchanged.

The generated ID is `reconcile-observe-<desired revision>`. It has a durable
`observation_revision`, exposed by command inspection, separate from a generated
write's `desired_revision`. The existing `reconcile-` namespace remains reserved.
The planner records at most one bootstrap read per desired revision, including
reads later cancelled, expired, failed or acknowledged as unknown. There is no
polling or automatic read retry. Use an explicit reviewed Get or a new desired
revision when new evidence is needed. Unknown acknowledgement still requires an
operator decision and does not recreate the read.

Queued reads recheck the current desired revision, policy, requested paths and
remaining observation need during maintenance and immediately before attempting.
A newer desired revision or disabled/changed policy cancels obsolete work. A
pause leaves eligible queued intent intact. Observations arriving while a read is
queued can eliminate its need and cancel it. There is no additional publication
in one reminder turn.

A correlated response updates the twin under the conservative
[Get observation rules](device-get-observations.md). Only a subsequent reminder
may plan the managed Set, and only if every desired path now has eligible fresh
observations. Partial or unusable reads leave the actor waiting for evidence;
`observation_recorded` distinguishes an exhausted bootstrap opportunity from
`observation_eligible`. A Set response does not verify convergence. This feature
provides an initial observation opportunity, not continuous refresh or automatic
post-write verification.

## Storage and validation

Current writers use schema 7. Schemas 1–6 upgrade on the next save while preserving
their state; generated-read provenance is never invented. The loader checks its
schema, revision, reserved identity, Get payload and separation from write
provenance. Older binaries reject schema 7, preventing dispatch with older policy
semantics. Drain senders and roll forward or use a reviewed state migration;
clearing snapshots is not a rollback procedure. This applies to actor saves even
when automatic reads are disabled. Storage fencing and backend protocols are
unchanged.

Focused checks cover durable queue-before-publish, read-to-write progression,
no fabricated convergence, no replay after uncertainty or acknowledgement,
policy/revision/pause changes and rejection of downgraded provenance. The full
race-enabled Go suite and `go vet` validate integration. Long infrastructure fault
tests are not repeated for this actor-local change.
