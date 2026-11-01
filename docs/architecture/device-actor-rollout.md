# Phase 6 foundation: managed twins and conservative reconciliation

Status: opt-in application foundation, **not production rollout approval**. Legacy
is still the default. Live phase-5 Dapr fencing/partition qualification, real-device
canaries, fleet projections and site disaster-recovery drills remain outstanding.
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
   observation. Missing/stale observations cause waiting, not a blind write.
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
the desired revision, policy or relevant observation ceases to be eligible before
publication. A running command cannot be revoked retroactively. Later responses
cannot mark a newer desired revision converged.

Each desired revision authorizes **at most one generated command**, including
commands later cancelled, expired or failed. There is no automatic retry after
Set success with missing verification, after drift, or after an unknown outcome.
A reviewed new desired mutation (even to the same value) creates a new revision,
but an unresolved unknown command still blocks dispatch. This is a deliberately
conservative first reconciliation policy, not continuous self-healing.

A Set response never updates reported values. Convergence requires a fresh matching
ValueChange observation or trusted internal Report import. Initial and ongoing
ValueChange subscriptions must be provisioned separately. Public Get responses are
not automatically imported into the twin and there is no automatic refresh polling
in this foundation. Without observations, reconciliation waits. Freshness remains
the existing five-minute window; device time and causal ordering are not inferred.

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
a health request. Prometheus metrics, a persistent fleet projection consumer,
global site routing and value redaction remain future work. A fleet projection
must be explicitly eventually consistent and must never claim to be the actor's
current authoritative state.

New saves use snapshot schema **3**, preserving schema 1/2 state on upgrade and
recording generated command provenance. Old binaries reject schema 3; binary
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
