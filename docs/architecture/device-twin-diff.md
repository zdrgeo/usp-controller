# Read-only managed-twin diff

An authenticated caller can inspect observation differences without retrieving
parameter values or triggering work:

```http
GET /v2/devices/{device}/twin/diff
Authorization: Bearer <USP_API_TOKEN>
X-USP-Site: <configured-site>
```

The route uses the existing twin-enabled actor inventory, bearer and site checks.
It accepts no query fields and returns `Cache-Control: no-store`. Legacy remains
the default; the route is absent when the twin API is not enabled. The actor
interface/private callback expose the matching `GetTwinDiff` read, including for
shadow actors, without enabling command execution.

## View and states

The result includes one `as_of` controller timestamp, desired/reported revisions,
optional `mutation_barrier_at`, observation-based `convergence`, a count of
`hidden_desired_paths`, and `paths` ordered lexically. Rows describe desired paths
only; reported-only parameters are omitted. Removed policy paths are hidden by
name while their count remains visible. They can still make overall convergence
unknown. Visible policy paths whose write permission/type no longer permits the
desired value report `policy_blocked`.

Each row contains `path`, `state`, `has_observation`, and optional `received_at`
and `fresh_until`. Values, types, command bodies/IDs, evidence payloads and
free-form reasons are excluded. State precedence is:

| State | Meaning |
| --- | --- |
| `policy_blocked` | Current policy does not permit this desired write. |
| `missing` | No reported observation exists. |
| `invalid` | The observation is unusable under the current managed type/value policy. |
| `future` | Its controller receive time is later than `as_of`. |
| `mutation_invalidated` | It was received at or before a known mutation barrier. |
| `stale` | Its ordinary freshness window has ended. |
| `different` | A usable fresh observation differs from the desired value. |
| `matching` | A usable fresh observation matches the desired value. |

Future/mutation-invalidated rows expose a zero `fresh_until`; other rows retain
the observation's recorded expiry for context. Use the state, not that timestamp
alone, to determine whether a row is usable. Empty desired state returns
`paths:[]` and converged, preserving existing semantics.

Convergence now shares this classifier: all matching paths are converged, usable
differences are pending, and any unusable or policy-blocked desired path makes it
unknown. It is not a dispatch plan, device-side causal guarantee or promise that
a command is settled. Pause, unknown command barriers, queue order, readiness,
capacity and once-per-revision limits still govern execution separately.

## Safety and limits

The read takes one actor snapshot and one clock reading for all rows. It does not
schedule refresh/reconciliation, repair reminders, expire queued commands, save
state or contact a device. The existing bounded desired map limits response size;
there is no fleet fanout or pagination session. Backend errors propagate normally.

Controller clock rollback also now makes future-dated observations unusable in
reconciliation planning and queued-write checks, not merely in this diagnostic.
Twin reads expose those observations as historical with zero `FreshUntil`.
Convergence and freshness are derived views that can change without a reported
revision increment; do not cache solely by that revision. Older stored timestamps
remain intact, so ordinary receive-order protections may keep observations unusable
until the controller clock recovers. No automatic overwrite or clock correction
is attempted.

Snapshot schema 9, backend fencing, delivery semantics and execution permissions
are unchanged. Metrics add one fixed `GetTwinDiff` operation without identity/path
labels. This change also makes convergence respect managed observation validity
consistently with reconciliation eligibility.

Validation on 2026-11-14: the full race-enabled Go suite and `go vet` passed.
Focused checks cover no read side effects despite failed write/reminder backends,
policy-scoped value-free output, timestamp/state consistency, clock rollback
preventing dispatch, mutation invalidation, and the full API/client/callback path
with access checks and default-route absence. Long infrastructure fault drills
were not repeated for this read/eligibility change.
