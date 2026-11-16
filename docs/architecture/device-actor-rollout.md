# Phase 6 foundation: managed twins and conservative reconciliation

Status: opt-in application foundation, **not production rollout approval**. The observed
packet-loss snapshot conflict has a [conditional-write repair](device-state-fencing.md)
with mandatory quiesced migration; broader replica write qualification remains open. Legacy
is still the default. Live phase-5 Dapr fencing/partition qualification, real-device
canaries and site disaster-recovery drills remain outstanding. Optional per-site
fleet status projections are described in the [projection guide](device-fleet-projections.md).
No device, broker, database or deployment is changed by adding this code.

## Enable a reviewed single-model cohort

Add `twin` inside the existing enabled `actor_canary` object. It applies to the
explicit allowlist and must be identical on every replica of that cohort:

```json
"twin": {
  "site_id": "site-001",
  "reconcile": false,
  "command_timeout_seconds": 30,
  "paths": {
    "Device.Test.Value": { "type": "string", "writable": true },
    "Device.DeviceInfo.SoftwareVersion": { "type": "string", "writable": false }
  }
}
```

Replace the illustrative writable path with a reviewed parameter supported by the
actual device model. Begin with `reconcile: false` to inspect twin behavior. Set it
to true only after reviewing the persisted desired state and command queue, creating
appropriate ValueChange subscriptions, and confirming permission to write those
paths. Changing policy requires restart; stop or drain old hosts before introducing
new policy. Do not mix policy versions across replicas.

The path list is exact concrete parameters, not wildcard, search, object or command
paths. Supported types are `string`, `boolean` (`true`/`false`), `int` (signed 32-bit)
and `unsignedInt` (unsigned 32-bit). Values are bounded at 4,096 bytes; there are at
most 1,024 policy entries. Comparisons use textual values and type, so configure a
consistent lexical representation. There is no device schema discovery or inference
of device permissions. An API token cannot grant permission at the Agent.

Only writable entries accept desired values. Only managed entries are returned by
the public twin read, and only valid managed ValueChange values update reported
state while policy is configured. Notification values take their type from reviewed
policy. Old unmanaged stored fields are hidden, not erased. Removing a desired path
relinquishes management and never emits USP Delete. Policy is copied at actor
construction to prevent accidental mutation through shared configuration maps.

**Use only nonsensitive parameters in this initial policy.** This filter governs
the twin; it is not an event-redaction or raw-command authorization mechanism.
Full notifications still enter the durable event outbox, and command results may
contain additional values. Apply separate access controls and retention to those
surfaces. The existing raw command API remains an operator interface.

## Site-aware API

These routes exist only when `actor_canary.twin` is configured. They use the existing
bearer token and device allowlist. The configured site is returned in `X-USP-Site`.
Clients can send that header to pin a request to the intended site; a mismatch is
rejected with 409 before actor invocation. Omission preserves existing command-client
compatibility. This is a local routing guard, not a global site directory or a
cross-site migration protocol. Site is not added to persistent actor identity.

| Route | Behavior |
|---|---|
| `GET /v2/devices/{device}/twin` | Authoritative actor twin, filtered to managed paths |
| `PATCH /v2/devices/{device}/desired` | Atomic compare-and-set desired mutation with durable receipt |
| `GET /v2/devices/{device}/events` | Pending outbox metadata in FIFO order; no export or mutation |
| `GET /v2/devices/{device}/twin/diff` | Value-free per-path observation differences; no dispatch or maintenance |
| `POST /v2/devices/{device}/twin/refresh` | One explicit queued Get of reviewed managed paths; requires Get projection |
| `GET /v2/devices/{device}/status` | Value-free actor diagnostics and reconciliation eligibility |

Example desired patch (the API accepts the existing ParameterValue DTO):

```json
{
  "request_id": "configuration-001",
  "expected_revision": 0,
  "values": {
    "Device.Test.Value": { "Value": "target", "Type": "string" }
  },
  "remove": []
}
```

HTTP 200 returns the desired receipt `{ "ID": "configuration-001", "Revision": 1 }`;
it does not mean a device write occurred. Identical retries return the original
receipt, while changed content or a stale expected revision conflicts. Receipt
validation also respects current policy; retrying after policy removal may be
rejected. Read the twin to obtain the current `DesiredRevision` before a new change.
The twin retains the existing Device DTO field names (`Desired`, `Reported`,
`LastContact`, `Convergence`, `Pending`, `Unknown`). Diagnostics use snake_case.
All management endpoints share the configured token; per-operator RBAC is not added.

The `reconcile-` request-ID prefix is now reserved for generated work. Status for a
generated command is available through the existing command GET route. New public
submissions cannot claim that namespace. Legacy receipts already using those names
are preserved and may block generation with a conflict; review them before enabling.

## Reconciliation semantics

A maintenance turn may generate one USP Set only when:

1. Reconciliation is explicitly enabled and the local dispatch guard/MQTT is ready.
2. There is no queued explicit work or awaiting/unknown command.
3. Every desired path remains writable under policy and has a fresh, typed reported
   observation received after any known mutating attempt. Missing, stale or
   pre-mutation observations cause waiting, not a blind write.
4. At least one desired value differs and no generated command exists for that
   desired revision.

The Set groups parameters by object, uses `allow_partial=false` and marks each
parameter required. The queued command and its target desired revision are committed
before the existing attempting/send boundary. It has the configured absolute
command deadline. Transport uncertainty retains the existing unknown/no-replay
behavior. An ambiguous queue save is reloaded on a later turn; no send follows an
error in that invocation.

Offline desired updates coalesce naturally: generation happens when dispatch can
proceed and uses the latest revision. A generated queued command is cancelled if
the desired revision, policy or complete current diff ceases to be eligible before
publication. Planning, maintenance and the final pre-attempt check now share the
same deterministic diff builder. Every desired path must still have a fresh, typed
observation, including paths omitted from the Set because they already matched.
New drift on an omitted path, convergence of an included path, or expiry of any
desired-path observation cancels the old plan. The actor does not expand or rewrite
its payload or automatically generate a replacement under the same revision.
Changes to observed values that leave the complete requested diff unchanged do
not invalidate an otherwise eligible plan. Each diff check uses one clock reading. A running command cannot be revoked retroactively. Later responses
cannot mark a newer desired revision converged.

Each desired revision authorizes **at most one generated Set**, including
commands later cancelled, expired or failed. There is no automatic retry after
Set success with missing verification, after drift, or after an unknown outcome.
A reviewed new desired mutation (even to the same value) creates a new revision,
but an unresolved unknown command still blocks dispatch. This is a deliberately
conservative first reconciliation policy, not continuous self-healing.

A Set response never updates reported values. Convergence requires a fresh matching
ValueChange observation, trusted internal Report import, or an opt-in
[correlated Get projection](device-get-observations.md). Initial and ongoing
ValueChange subscriptions must be provisioned separately. Optional bounded
[bootstrap](device-observation-reads.md) and [verification](device-verification-reads.md)
reads provide observation opportunities without periodic polling or write retries.
Without usable observations, reconciliation waits. The default freshness window
is five minutes; device time is not trusted.

## Diagnostics and schema migration

The status endpoint exposes site ID, desired/reported revisions, convergence,
reconciliation reason, counts by command state, inbox/mutation receipt counts,
pending events, oldest pending event receipt time, last contact and state capacity.
It returns no parameter values, full command bodies or event payloads. Reasons
include `disabled`, `idle`, `waiting_observation`, `policy_blocked`, `eligible`,
`attempt_recorded`, `blocked_unknown`, and `converged`. Eligibility is not a promise
of immediate sending; explicit commands, MQTT or export failures can delay it.

Use these bounded per-device reads to inspect canaries and monitor receipt/command
capacity, pending-event age and unknown outcomes. There is no all-device fanout on
a health request. An optional persistent [fleet status projector](device-fleet-projections.md) consumes
notification hints and refreshes these diagnostics. Bounded [host-local operation metrics](device-actor-metrics.md) are now available.
Global site routing and value redaction remain future work. A fleet projection
must be explicitly eventually consistent and must never claim to be the actor's
current authoritative state.

New saves use snapshot schema **12**, preserving schema 1/2/3/4/5/6/7/8/9/10/11 state on upgrade,
recording generated command provenance, audited operator acknowledgement of
unknown outcomes and durable per-device execution control. See the [operator workflow](device-actor-canary.md#operator-acknowledgement-of-an-unknown-outcome). See [Get observations](device-get-observations.md) for durable attempt provenance and opt-in reported-twin updates. [Bounded observation reads](device-observation-reads.md) are separately opt-in. [Post-write verification reads](device-verification-reads.md) are also separately opt-in. [Mutation outcome times](device-mutation-outcomes.md) extend observation ordering. [Topology cutoffs](device-topology-observations.md) extend freshness invalidation. Older binaries reject schema 12; binary
rollback over upgraded state is unsupported. Drain senders and use a reviewed
state migration or roll forward. Merely clearing state can lose attempts and
receipts and is not a rollback procedure. Existing bounded retention limitations
remain: records are not automatically pruned, and capacity can halt admission.

## Rollout exit gates still required

Before enabling writes on real devices, record a cohort/model/site inventory,
managed-path review, broker subscriptions, desired baseline and rollback owner.
Complete the phase-5 live placement and stale-writer drills first. Then qualify:

- A small read-only twin cohort, followed by one reviewed writable path with
  independent verification of the applied value.
- Device disconnect/reconnect storms: bounded ingress, stale twin state and no
  unapproved repeats when observations recover.
- WAN isolation: local actor/state/broker operation, durable outbox retention and
  controlled recovery, within measured capacity budgets.
- State/broker restoration and node loss: retire old publishers, preserve event
  identities, and prevent replay of potentially executed restored queued work.
- Capacity exhaustion and unavailable subscriptions: visible waiting/unknown
  states, no acknowledgment before durability, and a tested retention procedure.

Automated tests cover policy validation/filtering, site/auth/allowlist/CAS rules,
offline coalescing, observation freshness, superseded queued work, old responses,
ambiguous attempts across restart, and default-disabled behavior. They do not
replace the real-device, WAN, restore and node-failure gates above.

## Live runtime integration coverage

The opt-in [live Dapr suite](device-dapr-integration.md) now covers two-host
receipt recovery, Scheduler reminders, Redis outage recovery and notification
export into fleet projections against real services. Owner-only Redis and
Placement connection cuts now check failed admission, a delayed stale snapshot
and state stability after healing. Packet-filter scenarios also cover one-way
loss and established connections with a black-holed path, with bounded recovery
checks. This local test does not
close the asymmetric-partition, stale-writer, real-device or disaster-recovery
gates above. Legacy remains the default.


### Operator recovery continuation

Unknown outcomes now support explicit, revision-checked acknowledgement through
the command API. The durable decision releases the execution barrier while
preserving the unknown outcome, attempt identity and late evidence correlation.
It never repeats a generated command for the same desired revision. A new desired
revision is still required to authorize another attempt. Status `blocking_unknown`
and twin `BlockingUnknown` distinguish unresolved barriers from acknowledged but
still uncertain outcomes. No real-device rollout or automatic outcome inference
is enabled by this feature; legacy remains the default.


### Per-device execution control continuation

The [execution-control API](device-actor-canary.md#durable-per-device-execution-control)
now supplies durable revision-checked pause/resume for an individual actor. This
supports staged maintenance without draining every device on a host. Paused actors
continue ingestion, event export and deadline maintenance; their existing bounded
command/desired admission remains available. Resuming preserves unknown-outcome
and observation/policy guards. It cannot replay old attempts or revoke a send
already begun. Twin/status/fleet rows expose the control state, and stale retries
cannot overwrite newer decisions. Legacy remains the default; production rollout,
retention budgets and site-level scheduling remain separate work.


### Host-local operation metrics continuation

Canary hosts now expose authenticated `GET /v2/metrics` for bounded operation
counters and latency histograms. Actor callbacks, snapshot/reminder calls, device
publication attempts and event handoffs use fixed method/outcome labels without
device identities or payloads. Scraping never reads actor state or invokes Dapr,
and does not hold the recording lock while writing to the client. Typed actor
conflicts/capacity failures are counted as domain errors even when Dapr's callback
envelope uses HTTP 200. See [metric semantics and limits](device-actor-metrics.md).
This adds passive instrumentation; legacy remains the default.


### Completion capacity continuation

An optional [completion reserve](device-actor-capacity.md) now protects bounded
byte/inbox/outbox headroom from ordinary admission and telemetry. The actor checks
capacity before beginning new attempts, while correlated responses and unresolved
operation completions can commit within the hard limits. Current reserve values
are visible through status and fleet rows. This does not prune receipts, replace
retention planning, or guarantee acceptance of arbitrarily large evidence. Legacy
remains the default and snapshot schema 5 is unchanged.


### Command inspection continuation

The [per-device command listing](device-command-inspection.md) now provides bounded
metadata pages for discovering accepted work and uncertainty barriers during
maintenance and rollout. It exposes persisted FIFO positions separately from
lexical pagination order, and rejects continuations when command/control state
changes. Operator mutations still need their existing expected revisions; reads
never alter deadlines or trigger execution. This is neither a global command
index nor a retention mechanism. Legacy remains the default.


### Local attempt budget continuation

An optional [host-local attempt concurrency budget](device-actor-attempt-budget.md)
now limits simultaneous begin/publish/result sections across a runtime's actors.
It is acquired before durable attempting, so contention leaves intent queued
without publication or a synthetic unknown outcome. Per-device guards and
maintenance remain intact; local gauges/denial counts expose contention. This is
one resource bound for reconnect bursts, not a global site rate or outstanding
response budget. Zero/omission preserves existing actor behavior. No schema change
is required and legacy remains the default.


## Optional local attempt rate

The actor canary can now configure a [host-local rate budget](device-actor-attempt-rate.md)
independently of concurrent-attempt limits. Both rate and burst must be supplied;
zero/omission leaves rate limiting disabled. Denied turns retain queued intent
without beginning a command, and metrics expose local reservations and denials.
This is not a distributed site quota. Legacy remains the default; schema 8 and
storage fencing protocols are unchanged.


## Optional command progress during event handoff failures

The actor canary can enable [dispatch with an event backlog](device-outbox-dispatch.md)
to avoid coupling command availability to the event destination. Pending events
remain durable and errors remain visible. Storage/ambiguous removal errors still
stop the turn, and hard capacity remains a backpressure boundary. The option is
disabled by default; legacy and snapshot schema 8 are unchanged.


## Complete queued reconciliation validation

The complete-diff checks do not change snapshot schema 8 or enable reconciliation
in any deployment. Legacy remains the default. The race-enabled full Go suite
and `go vet` passed; focused checks cover expiry of an unchanged desired path
both before and during export, expansion/shrinkage of a queued diff and continued
eligibility when observation values change without altering the requested diff.
Storage fault drills were not repeated for this actor-local eligibility change.


## Mutation observation barrier

The actor now derives an [observation barrier](device-observation-barrier.md) from
durable Set/Add/Delete/Operate attempt provenance. Older readings remain visible
as history but cannot establish convergence or authorize reconciliation. Suitable
newer evidence is required even after a Set success or unknown acknowledgement.
This uses existing retained timestamps without a schema change; pre-schema-6
attempts lacking provenance remain an explicit migration limitation. Legacy is
still the default.


## Explicit asynchronous twin refresh

The authenticated [refresh API](device-twin-refresh.md) now queues one Get over
all reviewed managed paths without requiring callers to construct USP messages.
It uses ordinary command IDs, deadlines, receipts and dispatch gates; it does not
enable reconciliation or retry attempted reads. Response callbacks now assign
controller receive time, allowing their correlated Get evidence to reach the twin
under the existing projection checks. Legacy and schema 8 remain unchanged.


## Definitive mutation outcome barrier

New definitive mutation results and OperationComplete evidence now retain a
[controller outcome time](device-mutation-outcomes.md). Observations made during
an attempt cannot establish fresh state after that outcome, and Gets started
before it remain ineligible even if their response arrives later. Schema 9
preserves older snapshots without inventing historical outcome times; older
binaries reject it. Legacy remains the default.


## Optional bounded event-export batches

The actor canary can configure [event_exports_per_turn](device-event-batches.md)
from 1–32, default one. Sequential FIFO handoffs share one publication deadline
and commit removal of a successful prefix once. Partial failures retain the tail;
failed/ambiguous removal commits still stop dispatch. This improves healthy
outbox drain opportunities without another worker or state schema change. Legacy
remains the default and schema 9 is unchanged.


## Optional bounded reminder repair workers

The runtime now owns a [joined reminder-repair loop](device-reminder-repair.md).
`reminder_repair_workers` configures 1–16 workers, default one, without overlapping
sweeps or a goroutine per device. Shutdown cancels and joins local repair work;
remote activation remains idempotent and non-dispatching. Legacy remains the
default and schema 9 is unchanged.


## Read-only twin difference diagnostics

The [twin diff](device-twin-diff.md) API now explains missing, stale, invalid,
future-dated and mutation-invalidated observations without exposing values or
triggering work. Current policy controls path visibility. Convergence uses the
same observation classification, and future-dated evidence cannot authorize
reconciliation after clock rollback. Legacy and schema 9 are unchanged.


## Optional maintenance cadence

The actor canary can set [maintenance_interval_seconds](device-maintenance-cadence.md)
to 1–300 seconds, default 10. Runtime uses it consistently across snapshot profiles
and preserves deterministic reminder staggering. Existing mismatched registrations
are not overwritten; coordinate drain and explicit administrative replacement
before changing a live cohort. Persisted deadlines, schema 9 and legacy defaults
are unchanged.


## Actor API read errors and caching

Actor command, execution, twin/diff and status reads now return ordinary read
errors without suggesting that admission committed or asking callers to replay a
mutation ID. Only conflicting command/event-list cursors receive the instruction to
restart pagination; unrelated read conflicts remain ordinary conflicts. Status
codes and backend retry behavior are unchanged. Mutating calls retain the existing
uncertain-admission guidance.

The shared actor API access wrapper sets `Cache-Control: no-store` before access,
input validation or invocation. This covers successful evidence reads and handled
access, inventory, validation and backend errors. It also covers mutation receipts.
Twin freshness, execution state and late command evidence can change independently
of a caller's cached view. Legacy fallback routes are unchanged.

Validation on 2026-11-15: the race-enabled full Go suite and `go vet` passed. A
focused regression check covers all actor read routes, error guidance, cursor-only
conflicts, no retries, access/inventory failures and preserved mutation uncertainty.
Storage, execution permissions and snapshot schema 9 are unchanged.


## Pending-event inspection

The [event listing](device-event-inspection.md) now exposes stable outbox identity,
receive time and notification kind without payloads. Bounded cursors detect changes
to pending metadata and require a fresh page after append/export. Reads never
export or mutate state. Legacy remains the default and schema 9 is unchanged.

## Receipt recovery across managed-policy changes

Desired-state updates and trusted Report imports now check an existing immutable
receipt before applying current managed-path admission policy. An identical retry
can recover its original revision after a path is removed, made read-only or given
a different type. It does not reapply old values, undo a later revision, refresh
observations, allocate another receipt or queue a command. Changed content under
an existing ID still conflicts, even if current policy would also reject it.

New IDs still require current policy validation. Desired updates still require
the current desired revision on first admission. New Report timestamps still
cannot be in the controller's future; an identical already-admitted Report can
recover its receipt after clock rollback without promoting its historical values.
Existing future-observation and mutation barriers continue to govern freshness.

Current route/access/identity checks and structural validation remain in force.
Retries also retain the existing reminder-repair requirement: storage or scheduler
unavailability can still prevent a successful response. This change does not relax
RefreshTwin's execution/projection gates or its underlying scoped-Get fingerprint.
An old receipt is evidence of historical admission, not current path authorization
or the current desired/reported revision.

Validation on 2026-11-16: the race-enabled full Go suite and `go vet` passed.
Focused checks cover ambiguous admission, later revisions, withdrawn/read-only/
retyped paths, full receipt budgets, immutable-body conflicts, report-clock rollback
and rejection of new disallowed/future input without state changes. Legacy remains
the default; schema 9, receipt fingerprints and dispatch semantics are unchanged.

## Cancellation at command attempt boundaries

Tick now checks cancellation at entry, after event export/readiness, immediately
before durable begin and after a confirmed begin before transport handoff. These
checks apply even when optional host attempt budgets are absent. Work already
completed during maintenance or confirmed event removal is not rolled back.

Cancelling a Tick context is not queued-command cancellation. If begin has not
committed, accepted intent remains queued and can be considered on a later active
turn under its existing deadline/policy. If begin committed before cancellation
was observed, the actor skips a new handoff but retains durable `attempting` for
conservative unknown recovery on the next turn. It does not reset the command to
queued or replay it, even if transport was not called in that invocation. Existing
operator unknown review remains the recovery workflow.

These checks do not revoke already-entered backend/transport operations or promise
a distributed abort; dependencies still must honor the supplied context. No
detached write is added to persist a cancellation outcome. Acquired local permits
are released on return, while consumed rate reservations remain nonrefundable.
Snapshot schema 9, delivery semantics and legacy defaults are unchanged.

Validation on 2026-11-16: the full race-enabled Go suite and `go vet` passed.
Focused checks cover cancellation before a turn, after readiness, after confirmed
event export and after a confirmed begin, without optional budgets. They verify
unchanged queued intent before begin, no new handoff afterward, preserved attempt
identity and unknown recovery without replay. Long infrastructure fault drills
were not repeated for this local boundary check.


## Topology-aware observation cutoffs

Concrete object creation/deletion notices now atomically retain a
[topology cutoff](device-topology-observations.md) with their receipt and outbox
event. Older readings and earlier-started Gets cannot establish fresh post-change
state. Cutoffs also cover known readings across clock rollback at mutation begins
and outcomes. Schema 10 preserves older snapshots without inventing historical
cutoffs; older binaries reject it. Legacy remains the default.

### Notification receipt recovery after clock rollback

Identical retained notifications now recover their receipt before applying the
current-clock arrival-time eligibility check. This preserves acknowledgement
recovery after a lost commit reply, restart or event export even if the controller
clock has moved backwards. Device/message validation, nonzero arrival time and
reminder repair still apply. Changed payloads conflict; new future-dated notices
remain invalid. Recovery adds no save, event or device handoff and does not refresh
observations, topology cutoffs, completion evidence or last contact.

A focused check covers value-change and topology receipts after ambiguous saves,
with both pending and exported events, plus immutable payloads, reminder failures
and new-message time validation. Legacy remains the default; schema 10 and storage
protocols are unchanged.

### Onboarding-aware twin freshness

OnBoardRequest now reuses the atomic receipt/outbox observation cutoff to invalidate
older twin readings and earlier-started Gets. It is a conservative lifecycle hint,
not proof of reboot/reset. No desired values, command deadlines, accepted scope or
retry authorization change. A later allowed observation can refresh the twin;
onboarding identity attributes remain event evidence rather than parameter facts.

Writers now use schema 11. Schema-10 object and mutation cutoffs are preserved;
older onboarding receipts do not acquire invented cutoffs on migration or retry.
Older actor binaries reject upgraded state, so drain/roll forward or use a reviewed
migration. Legacy remains the default; no backend or delivery protocol changes.

### Deadline check after durable begin

Tick now checks the accepted absolute command deadline again after a confirmed
begin save and before entering transport. A slow save can consume the remaining
lifetime; a transport implementation may queue before inspecting its context.
When the controller clock is at or beyond the deadline, Tick returns a deadline
error without calling Publish. No accepted deadline is extended.

The durable attempt remains `attempting` and becomes `unknown` on a subsequent
turn. It is never reset to queued, even if this invocation skipped transport or
the clock later moves backwards. This preserves the single non-replayable attempt
boundary and the existing operator review workflow. The check cannot revoke I/O
already entered; transport dependencies must still enforce their contexts.

Focused checks cover equality/overrun during the begin commit, retained attempt
identity and restart recovery without publication after clock rollback. Legacy
remains the default; schema 11, backend protocols and delivery guarantees are
unchanged.

### Scoped explicit twin refresh

RefreshTwin now accepts optional `paths` to select a bounded subset of reviewed
managed parameters, including read-only paths. Omitted/empty selections preserve
the all-policy default. The actor rejects unknown paths, selectors and duplicates,
copies/sorts the selection and queues the same ordinary asynchronous Get as before.
No immediate publication, polling or write retry is added.

Effective sorted scope and deadline determine command identity: equivalent path
orders recover the retained receipt, while changed scope conflicts. Response
projection remains limited to requested and currently managed parameters. See
[explicit refresh](device-twin-refresh.md) for policy-evolution and host-upgrade
constraints. Legacy remains the default; schema 11 and backend protocols are
unchanged.

### Observation-only twin diagnostics

The value-free twin diff now includes `observation_paths` for managed parameters
without desired intent, including read-only and missing paths. It shares freshness,
type, mutation and lifecycle eligibility checks with desired rows; usable evidence
is reported as `fresh`. Withdrawn reported path names remain hidden behind an
aggregate count. Desired convergence and reconciliation planning are unchanged.

The read still performs one bounded snapshot load and one clock reading with no
writes or maintenance. Existing API access controls and default-route absence
apply. See [twin diff](device-twin-diff.md) for response compatibility and limits.
Legacy remains the default; snapshot schema 11 and backend protocols are unchanged.

### Observation coverage in fleet status

Actor status now samples value-free counts for every managed path, including
read-only paths, plus the earliest expiry among fresh observations. Optional
[observation coverage](device-fleet-projections.md#sampled-observation-coverage)
is validated and retained by fleet projections. Cached status becomes stale when
that expiry passes or its sample lies in the reader's future, and normal bounded
background refresh can replace it. Old status without the field remains readable;
desired convergence and execution policy are unchanged. Legacy remains the
default; snapshot schema 11 and backend protocols are unchanged.

### Fleet observation-health aggregation

The fleet summary now totals managed observation states from fresh cached rows,
with explicit covered/unmeasured device counts. Expired evidence and invalidated
or unhealthy projections do not contribute. Missing optional diagnostics remain
unmeasured instead of looking healthy. The additive response contains no path names
or values and requires no actor fanout. See [fleet totals](device-fleet-projections.md#fleet-observation-health-totals)
for denominators and compatibility. Legacy remains the default; schema 11,
projection row storage and command delivery are unchanged.

### Ambiguous Get candidates under type validation

Get projection now detects contradictory candidates for requested managed paths
before type/value filtering. A valid boolean beside an invalid contradictory
boolean no longer becomes a fresh twin observation merely because validation
filtered the other candidate first. Both candidate orders suppress that path;
identical valid candidates remain usable and unrequested results cannot poison
reviewed evidence.

Prior observations retain their original timestamps and values. Independent usable
paths still project, and the command retains its protocol outcome and full response.
Duplicate responses still perform no projection or save. Focused checks cover both
candidate orders, scope isolation, identical values and duplicate stability. Legacy
remains the default; schema 11, request scope and storage protocols are unchanged.

### Configurable observation freshness

An enabled actor cohort can now configure `observation_freshness_seconds` for new
observations and Get projection windows. Omitted/zero preserves five minutes;
explicit values require 1–86,400 seconds. Persisted observation expiry times are
not rewritten. See [freshness configuration](device-observation-freshness.md) for
in-flight read behavior and coordinated rollout. Legacy remains the default;
schema 11, accepted command deadlines and delivery protocols are unchanged.

### Durable Get observation windows

New Get attempts now persist their observation expiry with durable begin, so a
restart with longer or shorter freshness configuration cannot alter the begun
read's projection window. Expiry equality remains stale; command outcomes and
full evidence remain independent of projection. See [pinned windows](device-get-observation-window.md)
for schema-12 migration and the historical-attempt fallback. Schemas 1–11 preserve
existing state without invented windows; older binaries reject upgraded state.
Legacy remains the default, with no backend or delivery protocol changes.

### Inspectable Get observation windows

Command details and listings now expose optional `observation_fresh_until` from
retained Get attempt metadata, preserving the original timestamp across restart
and configuration changes. Queued, non-Get and legacy commands without a pinned
window omit it. The additive field is read-only and participates in command-list
view consistency; see [command inspection](device-command-inspection.md#pinned-get-observation-expiry).
Legacy remains the default; snapshot schema 12 and command execution are unchanged.

### Consistent effective freshness in twin reads

Full twin reads and value-free diff rows now use one observation-expiry view.
Retained observations invalid under current type/value policy expose zero effective
freshness, as future-dated and mutation/lifecycle-invalidated observations already
do. Their historical value, type, receive time and persisted expiry remain intact;
no read-side save or reported revision is introduced.

Desired-write permission stays independent: a policy-blocked desired row can still
have usable read evidence, but its precedence cannot hide an invalid observation's
zero freshness. Existing policy-change and read-only diagnostic checks cover the
views and unchanged stored snapshot. Legacy remains the default; schema 12 and
command execution are unchanged.
