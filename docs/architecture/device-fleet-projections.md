# Phase 6: optional per-site fleet status projections

The Controller can maintain a persistent, eventually consistent status view for
its configured actor cohort. Fleet queries read this local view without invoking
actors. The view contains status and capacity diagnostics, **not parameter values**.
Actors remain authoritative for commands, desired state and twins. This projection
never submits commands, reconciles desired state, or writes actor state.

Legacy remains the default. This feature is separate from enabling reconciliation
and does not complete the outstanding live Dapr, device or disaster-recovery gates.
It adds a per-site fleet view, not a global cross-site directory or cloud service.

## Opt in

Add `fleet` inside an existing enabled `actor_canary` configuration with a valid
`twin` policy. Reconciliation can remain disabled:

```json
"fleet": {
  "directory": "data/fleet-projection",
  "refresh_seconds": 60,
  "stale_seconds": 180,
  "workers": 4
}
```

Only `directory` is required. The numeric defaults are 60 seconds, 180 seconds and
4 workers. Refresh must be 5–3,600 seconds; stale must be at least refresh and at
most 86,400 seconds; workers must be 1–16. This profile supports up to 4,096 devices,
all from the configured actor allowlist, tenant and logical Controller. Site ID
comes from the twin policy. Invalid combinations fail configuration validation.

Use a dedicated directory on a local filesystem with atomic rename, fsync and
advisory file locking. It must be writable by the Controller. An exclusive lock
rejects a second process using the same directory. Each replica needs its own
volume/directory; this is not a shared filesystem database or a cross-node lock.
The existing legacy K3s manifest and example configuration remain unchanged.

The directory is bound to its site, tenant and Controller. Startup rejects a
mismatched scope, unsupported/corrupt rows, or oversized rows rather than silently
resetting them. Device filenames are hashes of actor identity. A row is limited
to 64 KiB and is replaced via a temporary file, file fsync, atomic rename and
directory fsync. New files use restrictive permissions. Disk capacity and actual
power-loss guarantees depend on the filesystem; this is rebuildable projection
data, never the authoritative actor store.

Removing a device from inventory hides its row immediately on restart. Old device
files and interrupted temporary files are not automatically deleted; clean those
up only while the projector is stopped. Never point this directory at actor state
or the legacy Journal. A controlled cache rebuild is safe: stop the projector,
retain/move its directory for diagnosis, and restart against a new empty directory.
Do not use that procedure for actor state, command receipts or the event broker.

## Refresh and event ingestion

Workers invoke only `GetStatus` through Dapr. There is at most one in-flight read
per device and at most the configured number across the local projector. Reads
are bounded to ten seconds, inventory service is round-robin, and attempts for
one device are spaced by at least five seconds. Refresh intervals are scheduling
targets, not latency guarantees: a large or unhealthy inventory can take longer
than one interval. Measure sidecar and disk load before expanding a cohort.

The projector registers a programmatic Dapr subscription on the existing
`event_pubsub`/`event_topic`, with `rawPayload: "true"`, through the private callback
listener. `/dapr/subscribe` and `/fleet/events` require `DAPR_APP_TOKEN`, just like
actor callbacks. Configure the same callback app port used by actors. No additional
public ingestion endpoint or broker component is provisioned.

Dapr wraps raw subscriber payloads in a CloudEvent with `data_base64`, as described
in the [Dapr raw-payload documentation](https://docs.dapr.io/developing-applications/building-blocks/pubsub/pubsub-raw/).
The consumer verifies the pubsub/topic, envelope schema 1, actor/Device identity,
notification shape, epoch and sequence before accepting a hint. It accepts only
inventory devices. The callback body is limited to 3 MiB; the decoded envelope is
limited to 2 MiB. Concurrent callback parsing is limited to the configured worker
count; excess callbacks return `RETRY` before decoding. It does not retain message
bodies, parameter paths or values.

An accepted event atomically persists a pending-refresh flag and a fingerprinted
receipt, then returns `SUCCESS`. It does **not** use event payloads as status data.
The latest 32 event identities per device are retained; identical redelivery is
harmless. After eviction a replay merely schedules another bounded refresh.
A conflicting payload under a retained identity is rejected. Malformed, unrelated,
unsupported, oversized or conflicting hints return `DROP`; persistence failures
return `RETRY`. Configure broker access controls so only trusted event publishers
can produce these messages. There is no audit/dead-letter archive in this cache.

Events coalesce into one pending flag. An event arriving during a read keeps that
flag set after the read completes, ensuring another refresh. Epochs are random per
event and restored actor counters can regress, so the projector does not try to
order snapshots using event epochs or sequences. It replaces a row only with a
validated actor-status read, including lower revisions returned after a restore.
Such a read reflects the restored actor; it does not establish that restoration
was safe for device-command execution.

Periodic refresh is essential: desired changes, command transitions and outbox
removal do not all emit Notify events. It also repairs dropped, expired or missing
hints. Replicas sharing a Dapr consumer group may receive different hints, but each
polls its own full configured cohort. Their cached views can temporarily differ.
This implementation does not require broadcast delivery or global cache coherence.

## Read API

All fleet routes require the existing management bearer token. `X-USP-Site` is
returned; supplying a different site header rejects the request with 409. Only
configured inventory appears in the view. Responses use `Cache-Control: no-store`.
These routes exist only when the fleet option is configured.

| Route | Result |
|---|---|
| `GET /v2/fleet/devices?limit=100&after=<device-id>` | Sorted page of device snapshots, total inventory and optional `next_after` |
| `GET /v2/fleet/devices/{device}` | One cached device snapshot; 404 outside inventory |
| `GET /v2/fleet/summary` | Fresh/stale/unavailable counts and aggregates from fresh snapshots only |

Page size defaults to 100 and is limited to 200. URL-encode the returned device ID
when using `next_after` as the next request's `after` cursor. Pagination order is
lexical device ID; pages are separate reads and do not form a transactionally
consistent fleet snapshot. Every device row includes:

- `identity`: tenant, logical Controller and Agent identity.
- `snapshot`: the actor status DTO, or null before any successful read.
- `captured_at`: read-start time, used conservatively for freshness; the actual
  actor turn completes no later than `refreshed_at`.
- `freshness`: `fresh`, `stale` or `unavailable`.
- `refresh_pending` and a bounded error code (`refresh_failed`) when applicable.

**Fresh means recently sampled status, not that the device is online or the actor
is still in that state.** Even a fresh snapshot can have an old `last_contact` or
unknown twin convergence. Per-device command and twin endpoints remain authoritative.
List/summary responses explicitly state `eventually_consistent`. Summary fields
`fresh_convergence` and `fresh_command_states` exclude stale/unavailable rows, so a
partial fleet is never silently reported as wholly healthy or converged.

A row is stale after the age threshold, when invalidated, during a refresh, after a
failed read, when its capture time is in the future, or while persistence is faulty.
Startup always marks loaded snapshots stale and refreshes them before claiming
freshness. Query requests never trigger actor fan-out, even for missing snapshots.

## Failure handling and operational limits

State is partitioned into one local file per device. On any failed or ambiguous
write, the projector stops refreshing and rejects new hints with `RETRY` until a
restart reloads disk state. Queries retain the last view, mark snapshots stale and
report `persistence_healthy: false` on list/summary responses. Core Controller
readiness and command handling are independent of this optional cache; monitor
its own health and stale/unavailable counts. Refresh errors retain the last good
snapshot and expose no raw backend errors or credentials.

No actor schema migration is introduced. Projection rows have their own schema 1
and are disposable. This cache is not a command queue, exactly-once event ledger,
parameter-value projection or primary event archive. The bounded receipt window
is sufficient because event effects are only invalidations. Original event exports
and command/actor durability remain governed by the earlier phases.

Tests cover the actual publisher envelope and Dapr callback contract, duplicate
and conflicting identities, out-of-order hints, bounded receipt eviction, restart,
ambiguous persistence, corrupt files, inventory removal, status/site validation,
refresh/event races, periodic fallback, worker limits, pagination, authentication
and no query fan-out. Live Dapr/broker delivery qualification, site-scale load,
WAN isolation and power-loss/restore drills remain deployment gates.
