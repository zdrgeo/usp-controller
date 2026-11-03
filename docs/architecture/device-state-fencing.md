# Replica snapshot concurrency and managed restore

**Superseded for replica snapshots:** replicas now require an independent etcd
store. The Redis backend and workflow below describe the earlier implementation;
the CLI permits only read-only Redis export. Use the
[current etcd configuration, migration and restore guide](device-etcd-snapshots.md).
The raw Redis limitations below explain why the authority moved.

Replica mode uses `FencedDaprBackend` for every Device snapshot. Legacy remains
the default. The single-host canary retains its existing actor-state backend and
single-owner deployment restriction. These changes do not migrate a deployed site.

## Conditional snapshots

The original backend used unconditional actor-state transactions. Silent Placement
loss allowed two hosts to acknowledge different mutations at the same revision.
Replica snapshots now use the standard Dapr state API with strong consistency and
`first-write` ETag concurrency. The pinned actor-state HTTP API exposes no ETag.

Every runtime load requires an existing snapshot and ETag. **A missing key is an
error, never an instruction to create a device or import legacy state.** Runtime
saves require a successful load and recheck the live version before writing.
Conflicts and ambiguous failures invalidate the local version; no mutation or
transport publication is automatically replayed. A failed attempting-state save
prevents sending; a failed finish save preserves the uncertainty boundary.

After a write, the backend reads back the value and ETag. It permits another save
only if the complete value still matches, with JSON comparison preserving integers
above 2^53. A failed read or competing value invalidates the local version.

Known domain errors use a typed HTTP-200 actor transport envelope, because Dapr
wraps non-2xx callbacks as generic HTTP 500. The client maps these back to domain
errors, and the public API retains its normal HTTP error statuses.

## Explicit initialization and migration

The key remains `device-snapshot-cas-v1-<encoded-actor-id>` in
`actor_canary.replica.state_store`, with the configured Dapr app/key prefix. Snapshot
schema remains 3. Existing fenced snapshots need no data conversion. Every replica
must use the same app ID, store, prefix and device identity.

`cmd/device-snapshot` is a standalone administrative command; it does not start a
Controller, MQTT connection, actor host, reminder or outbox publisher. It uses
`DAPR_API_TOKEN` from the environment and the same local sidecar/store as the
replica cohort. It reads and writes sensitive snapshot contents; protect output
files and do not send them to ordinary logs.

Before upgrading, stop admission, drain senders and retire **all** old hosts and
clients. Mixed binaries are unsupported: old hosts can still perform implicit
legacy fallback. Back up the configured inventory and snapshots, and resolve
pending/unknown work under the rollout policy.

For each device, choose exactly one operation:

- `inspect`: read an existing authoritative snapshot; never initialize it.
- `initialize`: create a genuinely new device only when both fenced and legacy
  state are absent.
- `migrate-legacy`: explicitly import a validated legacy actor snapshot only when
  fenced state is absent and legacy state exists. The old copy is left unchanged.

Both initialization paths use conditional creation. Concurrent initializers cannot
both succeed on the pinned Redis component; a loser may receive a persistence
error instead of HTTP 409. An existing fenced key always rejects initialization.
These operations are never called by the runtime. Provision the entire explicit
allowlist before enabling admission or dispatch; missing snapshots fail actor
activation/readiness rather than silently becoming empty devices.

Example for a known legacy device, with all old writers retired:

```sh
umask 077
go run ./cmd/device-snapshot -action migrate-legacy \
  -tenant tenant-a -controller controller-a -device device-a \
  -state-store device-actor-state > migrated-device-a.json
```

Use `-action initialize` only for a verified new device. Do not use either action
to repair deleted authoritative state: the tool cannot distinguish an originally
absent key from a deleted one. There is no safe binary rollback after migration;
old binaries would resume from stale legacy state.

## Managed backup and restore

The administrative export contains `{backup: {format: 1, snapshot: ...},
current_etag: ...}`. `backup` contains application state only. It never contains
Redis serialization or storage versions. Actor validation checks identity, schema,
receipts, commands, queue and event consistency before restore.

A restore **updates the existing key with the current live ETag**. Redis advances
its current version; the historical version is never restored. A writer holding a
pre-restore version loses its next conditional write, even when the restored
application data is identical to its old copy. A concurrent mutation invalidates
the operator's restore precondition. There is no automatic retry on conflict or
ambiguous failure.

1. Stop admission and drain all senders before restoring. Preserve a current export
   as well as the older backup. Review commands that may already have executed.
2. Inspect the current target and supply its ETag explicitly. Never use the ETag
   saved alongside the older backup. The command does not infer this flag from
   its input document.
3. Restore the backup through the administrative command, validate state and
   reconcile uncertainty before re-enabling traffic.

```sh
umask 077
go run ./cmd/device-snapshot -action inspect \
  -tenant tenant-a -controller controller-a -device device-a \
  -state-store device-actor-state > current-device-a.json

# Replace LIVE_ETAG with current_etag from the fresh inspection above.
go run ./cmd/device-snapshot -action restore -expected-etag LIVE_ETAG \
  -tenant tenant-a -controller controller-a -device device-a \
  -state-store device-actor-state < older-device-a.json > restored-device-a.json
```

If a request times out, inspect the target before any retry: the write may already
have committed. A successful restore can still discard receipts for externally
completed work. It cannot revoke a command already sent, or prevent a writer that
freshly reloads from making a subsequent valid mutation. Drain remains necessary.

## Store-level recovery remains restricted

Do **not** use Redis `DUMP`/`RESTORE`, AOF rollback, deletion, TTL, or external writes
on a running cohort. Restoring Redis's old version produces ABA: a surviving old
writer can become valid again. The managed restore path avoids that operation;
it is not an independent restore epoch or a storage ownership lease.

The pinned Redis component also accepts writes to missing keys even when an ETag
was supplied. The runtime's preflight rejects deletion already visible before a
save, but cannot make the existence check atomic with that save. Concurrent raw
key deletion can still race with an in-flight writer. Removing implicit fallback
prevents fresh readers from importing stale legacy state; it does not make raw
store manipulation safe. Other components require independent qualification.

Missing or corrupt authoritative state fails closed. This command deliberately
cannot recover a deleted key or an entire lost database. Such disaster recovery
requires retiring all writers, a verified authoritative inventory, and a separately
qualified offline recovery process. Do not substitute initialization or legacy
migration for recovery. Back up Scheduler/pubsub and external effects consistently
under the deployment policy; per-device CAS restore does not solve site recovery.

## Live validation

The opt-in Dapr 1.19.1 / Redis 7.4.2 suite verifies explicit legacy migration,
complete twin/receipt/queue/inbox/outbox preservation, duplicate request recovery,
conditional restore with advancing storage versions, rejection of surviving stale
writers, stale restore authorization and wrong device identities. Deleting a key
before a save causes readers, writers and managed restore to fail without importing
legacy state or recreating the key. Unit tests also inject a mutation between the
restore read and write, and an ambiguous committed response.

Earlier tests characterized unsafe raw Redis restore and deletion; those findings
remain applicable to unsupported store-level operations. Production real-device,
node-loss and whole-site restore gates remain open. See the
[live integration guide](device-dapr-integration.md) and
[Dapr state concurrency semantics](https://docs.dapr.io/reference/api/state_api/).
