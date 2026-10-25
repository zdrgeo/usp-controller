# Phase 3: single-host Device command canary

Status: opt-in implementation. Legacy remains the default. This is not a
multi-replica deployment profile. Live Dapr/component qualification remains a
rollout gate; the automated HTTP integration harness is not a placement test.

## Enable only an explicit cohort

The existing configuration works unchanged. Omit `actor_canary`, or set
`enabled` to false, to retain the legacy runtime without a Dapr dependency or
new API routes. To opt in, add the following object to a valid site configuration:

```json
{
  "actor_canary": {
    "enabled": true,
    "tenant": "example",
    "devices": ["os::example:device-001"],
    "dapr_url": "http://127.0.0.1:3500",
    "callback_listen": "127.0.0.1:8081",
    "lock_path": "data/device-canary.lock"
  }
}
```

Every device must already exist in `agents`; duplicate or unknown entries fail
startup. The list is exact endpoint IDs, not patterns. Existing endpoint IDs and
`agent_id` wire fields are not renamed. Configure a distinct loopback callback
port and a loopback HTTP Dapr origin. All mode changes require a controlled restart.

Run one Dapr-enabled Controller host for this cohort. Configure the sidecar app
channel for the callback listener (8081 above), **not** the public HTTP API port.
Use Dapr 1.19.x with a qualified transactional actor-state component and Scheduler.
Keep all dependencies local if offline admission/execution is required. No SDK or
new Go dependencies are introduced: the adapter uses the documented HTTP API.

Required credentials:

- `USP_API_TOKEN`: existing management API bearer token, at least 32 characters.
- `DAPR_APP_TOKEN`: at least 32 characters, shared by the app callback listener and
  Dapr's app-token configuration. All callback routes require it.
- `DAPR_API_TOKEN`: supplied to outbound sidecar calls when sidecar API token
  authentication is configured. It is a separate credential from the app token.

The existing K3s manifests remain single-replica/Recreate and are not modified to
install Dapr or enable canaries. Adapt deployment configuration deliberately; do
not simply increase replicas. Only this host should register these actor identities
for the selected app/namespace and use the associated MQTT connection identity.

An exclusive advisory lock on `lock_path` rejects a second local canary using the
same path. Keep that path stable; do not delete or replace the live lock file.
This is not cross-node fencing and does not fence a legacy process that ignores
it. Stop the old sender before enabling the cohort. Actor placement does not revoke
an old MQTT connection. Do not relocate this profile automatically to another host.

## Asynchronous API

All v2 routes use the existing bearer authentication and enforce the configured
allowlist. The single token still grants access to all configured devices; this
phase does not add per-operator authorization.

| Route | Result |
|---|---|
| `POST /v2/devices/{device}/commands` | 202 with current command receipt and Location |
| `GET /v2/devices/{device}/commands/{request_id}` | 200 with status, revision, deadline, protocol message ID and optional result |
| `POST /v2/devices/{device}/commands/{request_id}/cancel` | 200 for queued-only revision-checked cancellation |

Encode device endpoint IDs as URL path components. Request IDs contain 1–128 ASCII
letters, digits, dots, underscores or hyphens, excluding `.` and `..` alone.
Submission body:

```json
{
  "request_id": "read-device-info-001",
  "deadline": "2026-12-01T12:00:00Z",
  "request": {
    "get": { "paramPaths": ["Device.DeviceInfo."] }
  }
}
```

Use an actual future absolute deadline. It is persisted, including across restart;
retry the same body and request ID after any ambiguous HTTP result. The request
uses protobuf JSON, matching the existing API's request encoding. Unknown JSON
fields and trailing objects are rejected. Caller disconnect or HTTP timeout does
not cancel admitted work. A duplicate submission returns the current receipt and
never appends a new command, even after completion or cancellation.

A typical receipt is:

```json
{"request_id":"read-device-info-001","revision":1,"state":"queued"}
```

Cancellation body is `{"expected_revision":1}`. Cancellation succeeds only before
an attempt; a matching retry after a lost cancellation response returns the prior
result. Status results use protobuf JSON, not Go's encoding of protobuf oneofs.

202 means durable admission, **not execution**. Broker availability is not required
for admission; state and reminder availability are. The command's deadline may
expire while the broker or device is offline. The existing `/readyz` retains its
legacy transport/journal meaning and does not prove actor-state readiness. A failed
v2 call is the authoritative indication that actor admission could not be confirmed.

Status mapping: 400 invalid input/unsupported operation; 401 unauthorized; 404 device
outside the cohort or missing command; 409 identity/revision/state conflict; 429
bounded actor capacity; 503 unconfirmed actor invocation/backend outcome. On an
ambiguous failure, read or retry the same identity rather than create another.

`/v1/requests` returns 409 for cohort devices instead of publishing via legacy.
Non-cohort devices retain legacy synchronous behavior. There is no fallback when
Dapr is unavailable. With legacy mode selected, v2 routes do not exist.

## Dispatch and response policy

Recurring maintenance reminders dispatch at most one command per turn. The
single-host runtime also repairs cohort reminders through Dapr Activate calls,
repeating the inventory pass approximately every 30 seconds (backend failures and
cohort size can delay a pass). Reminder period is 10 seconds with a deterministic
initial stagger. Admission does not synchronously send a command or force a wake-up.

The actor checks transport readiness before beginning, persists a random protocol
message/attempt identity and `attempting`, and only then calls the local MQTT
adapter. Publication has a five-second upper bound and never waits for a USP
response inside the turn. There is one in-flight command per device. Responses
are validated with the existing protocol boundary, then routed through Dapr to
`HandleResponse`, including identity and expected-operation checks.

Get, Set, Add, Delete, GetInstances, GetSupportedDM and GetSupportedProtocol are
supported on this path. **Operate is rejected in phase 3**, because completion
notification ingestion is phase 4. This applies to synchronous Operate too; do not
infer safety from a device-specific operation name. Pre-existing shadow Operate
entries must be cancelled before dispatch is enabled. A canary is not a migration
of all existing API capabilities at once.

A valid matching response with USP Error, a nested nonzero error code or an explicit
failure variant marks the command failed, including partial Set/Add/Delete errors.
The complete response remains available for inspection. Otherwise the command is
marked succeeded based on that response; this is not independent verification of
reported device state. Duplicate identical responses are harmless, conflicting
response content is rejected, and a response can resolve an unknown outcome.
Unmatched late responses are ignored by runtime ingress.

| Failure | Behavior |
|---|---|
| Begin save fails before commit | Still queued; a later turn may attempt |
| Begin save commits but reply is lost | No send in that invocation; next turn marks unknown |
| Crash after attempting, before/after send | Unknown on next maintenance turn; never replayed |
| Send returns an error | Unknown, regardless of whether the device executed |
| Publish-result save fails | Read durable state later; no replay of attempting |
| Response deadline passes | Unknown, not failed or requeued |
| Response arrives after unknown | May resolve the command using the original identity |
| Unknown operation remains | Further command dispatch for this device is blocked |

There is deliberately no automatic retry of a command or desired-state write.
QoS 1 PUBACK means broker acceptance only; QoS 0 completion does not claim PUBACK.
MQTT retransmissions remain possible. Desired-state reconciliation and event export
are not enabled by this phase. There is no operator unknown-resolution endpoint yet:
use a reviewed reconciliation procedure before continuing an affected canary.

Notifications still pass through the existing JSONL Journal and requested NotifyResp
path, including notifications from cohort devices. They are not passed to the actor
inbox/outbox. OperationComplete therefore does not complete actor commands in this
phase; this is why Operate admission is disabled.

## Cutover, rollback and capacity

Stop admitting legacy mutations for the cohort, drain pending requests to a bounded
deadline, record unresolved outcomes, then stop the old execution owner before
starting this mode. Review all pre-existing actor queues before opt-in: enabling
execution can send previously queued shadow work. Do not run both execution paths.

State keeps the bounded phase-2 snapshot format plus optional attempt and result
fields. Existing phase-2 queued entries can be loaded. Phase-2 binaries reject the
new attempted/terminal execution states, so a binary rollback after execution is
not sufficient. Quiesce callbacks, drain/cancel queued work and preserve snapshots;
reconcile unknown operations before reverting the device to legacy. Never copy
attempted commands back into a legacy submission queue.

Snapshot, receipt and retained-command limits remain; there is no automatic pruning.
Results consume the same snapshot budget. If a result cannot be committed, the
operation remains uncertain until storage/capacity is resolved; it is not resent.
Plan enough headroom for result messages. No new schema promises node-loss survival:
the configured actor backend, broker and restore procedure determine durability.

## Validation

Automated tests cover explicit/default mode validation, lock exclusion, allowlist
and authentication, legacy bypass prevention, 202/status/cancel protobuf-JSON flows,
offline admission, duplicate/conflicting requests, one in-flight command, partial
USP failure, wrong identity/type, late evidence, cancelled/expired commands, and
crashes represented by failed/ambiguous begin/end saves. An HTTP sidecar emulator
exercises API -> Dapr invocation -> callback -> state/reminder calls -> MQTT adapter
and response routing, plus continued Journal/NotifyResp behavior.

A live pinned Dapr/Scheduler/component smoke test and actual device interoperability
are required before site rollout. HTTP contract tests do not establish distributed
ownership or component replication guarantees. See the [architecture proposal](device-actors.md)
for the later notification, replication and rollout phases.
