# Bounded reads after managed writes

The managed-twin policy accepts `verify_after_reconcile`, default false. It
requires `reconcile` and `observe_get_responses`; `read_before_reconcile` remains
an independent option. Legacy remains the default runtime. Existing explicit
actor inventory, reviewed paths and dispatch permissions still apply.

After a generated Set has a successful correlated response, a reminder can queue
one Get for all current desired parameter paths. It does not run for explicit
operator Sets, pending/failed writes, or unknown writes even after operator
acknowledgement. Historical writes without durable attempt provenance are also
ineligible. If fresh observations already show convergence, no read is needed.
A verification read is an opportunity to observe the device, not proof the Set
caused those values.

The read uses ID `reconcile-verify-<desired revision>` and carries a separate
`verification_revision` visible in command summaries and single-command reads.
It shares ordinary admission capacity, the command FIFO, pause/readiness/unknown
barriers, local attempt budgets and durable begin-before-publish behavior.
Explicit queued work runs before a verification intent is generated. Each reminder
still publishes at most one command. Queueing does not dispatch from a response
callback or a status query.

Queued verification rechecks the current desired revision, enabled policy, exact
reviewed path set, successful managed write and remaining observation need. A
new desired revision, policy withdrawal or fresh convergence cancels obsolete
intent. A pause retains eligible queued intent. These checks run in maintenance
and immediately before begin, after event export.

The correlated Get response uses the existing conservative
[projection rules](device-get-observations.md): reviewed values, latest attempt,
valid receive time, freshness anchored to read start, and protection for newer
observations. The command evidence and optional twin update share one save.
Pre-mutation readings are historical under the
[observation barrier](device-observation-barrier.md); Set success alone can leave
convergence unknown. Matching fresh observations can establish convergence. A mismatch remains
pending; unusable or stale evidence can leave convergence unknown. Neither case
authorizes another Set for the same desired revision.

At most one verification intent is recorded per desired revision, including
cancelled, expired, failed and uncertain reads. There is no automatic verification
retry, periodic polling or corrective write retry. An operator can inspect evidence
and choose an explicit read or a new desired revision. Unknown acknowledgement
does not recreate the verification. Status exposes `verification_eligible` and
`verification_recorded`; convergence remains a separate observation-based field.
Read-disabled policies retain the existing status behavior.

## Storage and rollout

Current writers use schema 8; schemas 1–7 load and upgrade on the next save.
The loader validates verification provenance against its reserved identity,
revision, Get payload and successful originating write, and enforces attempt
ordering when a verification attempt exists. Provenance cannot coexist with
bootstrap or write provenance or be hidden in an older schema.

Older binaries reject schema 8. Drain senders and roll forward or use a reviewed
state migration rather than rolling an older writer over upgraded snapshots.
All actor saves use the new schema even with verification disabled. Storage
backend and fencing protocols are unchanged.

Validation focuses on read-to-convergence behavior, mismatches and uncertainty
without replay, durable queueing before publication, successful-write eligibility,
queued policy/revision/pause changes and schema downgrade rejection. The full
race-enabled Go suite and `go vet` passed. Long infrastructure fault drills were
not repeated for this actor-local change.
