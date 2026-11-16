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

## Current capacity and reconciliation compatibility

Projected status now preserves optional [capacity headroom](device-actor-capacity.md#remaining-capacity-diagnostics).
Older cached rows omit this object rather than reporting false zero headroom. New
objects must agree with their root counters before a refresh replaces the cache.
Bootstrap and verification eligibility/recorded reason codes are now accepted;
previously these valid actor responses could leave the projection in refresh failure.
Unknown reasons and inconsistent headroom still fail validation and preserve the
last valid snapshot as stale. No projection schema migration or actor mutation is
required.

## Sampled observation coverage

Current actor status optionally includes `observation_coverage`, containing one
`as_of` time and counts across all configured managed paths (including read-only
paths and paths without desired intent). `managed_paths` is partitioned into
`fresh`, `missing`, `invalid`, `future`, `mutation_invalidated`,
`topology_invalidated` and `stale`. The summary exposes no path names, values,
command bodies or device-provided timestamps. No managed policy means the field
is absent; absence from an older host/cache means unavailable coverage, not zero
missing parameters. Desired convergence remains a separate diagnostic.

When fresh observations exist, `fresh_until` is their earliest recorded expiry.
It is absent when none are fresh. Fleet reads mark the entire cached status stale
at that boundary, or when its `as_of` lies in the fleet reader's future. The
background loop also treats those boundaries as due for refresh under existing
worker, retry-spacing and read-only limits. API reads still do not fan out to
actors. A refreshed summary may legitimately report stale/missing observations
while the projection itself is fresh: the latter means current sampling, not
healthy device state.

Count bounds, their exact partition total, sample time and expiry consistency are
validated before accepting or restoring status. Invalid refreshes retain prior
evidence and mark it stale. Old rows without the optional field remain readable.
These are sampled diagnostics, not a guarantee against changes between reads;
notification hints, normal cache age and failure invalidation still apply. Clock
skew can conservatively make rows stale. Old projection binaries ignore the new
field, so upgrade serving fleet readers to enforce its additional expiry rule.

Validation on 2026-11-16 covers actor summaries before/after lifecycle invalidation,
read-only reads with failed write/reminder backends, projection persistence/reopen,
early expiry after clearing startup invalidation, malformed-summary rejection and
older-host compatibility. The change uses snapshot schema 11 without storage or
command-delivery changes. Legacy remains the default.

### Fleet observation-health totals

`GET /v2/fleet/summary` now adds `fresh_observation_coverage`. It aggregates the
managed-path counts from validated coverage diagnostics on **fresh projection
rows only**. It contains `covered_devices`, `unavailable_devices`, `managed_paths`
and the same `fresh`, `missing`, `invalid`, `future`, `mutation_invalidated`,
`topology_invalidated` and `stale` path counts. No path names or values are added.

`covered_devices + unavailable_devices` equals the existing fresh-device count.
Unavailable here means a fresh cached status omitted the optional diagnostic
(for example, an older host or a device without a managed policy). It is not the
fleet envelope's unavailable-row count. Stale and unavailable rows contribute
neither path totals nor covered/unmeasured devices. Consult the existing
`freshness` counts to assess the rest of the fleet; zero totals do not establish
that all devices are observed or healthy.

A valid covered device can report zero fresh parameters: recently sampled status
and usable parameter evidence are different measures. Per-row expiry and the
existing invalidation/failure rules apply before aggregation. Device sample times
remain on the per-device rows; `generated_at` is the local summary time, not an
atomic fleet-wide observation time. Queries remain cache-only under one local
read, without actor fanout, writes or refresh scheduling.

The response field is additive. Older summary producers omit it; treat that as
unavailable aggregation rather than zero missing paths. Actor snapshots and local
projection row formats are unchanged. Focused validation covers mixed old/current
hosts, expiry exclusion before normal cache TTL, persistence-failure exclusion
and no query-side calls or writes. Legacy remains the default.

## Optional expected twin policy

An enabled `actor_canary.fleet` configuration can set
`expected_twin_policy_fingerprint` to a reviewed `twin_policy_fingerprint` from
authoritative actor status. The optional value must be a canonical lowercase
SHA-256 digest. Omitted/empty preserves the existing acceptance of status from
older hosts without that diagnostic. This is one expected policy for the configured
projection cohort; it is not a per-device policy map.

When configured, a newly read status must contain that exact digest before it can
replace a cached snapshot. A mismatch or missing digest follows the existing
`refresh_failed` path: prior evidence remains retained and stale, the pending bit
remains set and bounded background reads can retry. A later matching read clears
the failure normally. Mismatched cached rows cannot contribute to fresh list or
summary counts. Query requests still do not invoke actors or schedule work.

Changing the expectation does not rewrite cached policy identity, delete old
rows or prevent reopening valid historical cache data. Startup already marks
restored rows pending; they need a matching status read before being trusted as
fresh. Coordinate the expected digest with the reviewed policy rollout. A bad
but well-formed expectation can leave the whole projection stale until corrected.

This option gates projection trust only. It neither pauses actor dispatch nor
changes admission, readiness, snapshot ownership or retry permissions. The digest
is not authentication or proof that all serving hosts share configuration. Core
controller behavior remains independent of the optional cache. Legacy remains
the default; actor schema 12, local projection row format and backend protocols
are unchanged.

Focused checks cover restart with cached evidence from an older policy, rejection
of mismatched/missing refreshed identity, recovery with matching status, default
compatibility and configuration bounds. Infrastructure fault drills are not
repeated for this read-side policy filter.
