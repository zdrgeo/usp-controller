# Replica snapshot concurrency repair

Replica mode now uses `FencedDaprBackend` for every Device snapshot. Legacy is
still the default, and the single-host canary profile retains its existing actor
state backend and single-owner deployment restriction. No deployed data is changed
by this source update alone.

## Cause and repair

The original backend read a snapshot and submitted an unconditional actor-state
transaction. During silent Placement loss, two hosts could load the same desired
revision and both persist different successful receipts at the next revision.
Local actor mutexes and Placement routing did not provide a storage fence.

The pinned Dapr 1.19.1 actor HTTP read endpoint does not expose the state ETag.
Replica mode therefore uses the standard Dapr state API for its single snapshot,
with strong consistency and `first-write` concurrency. Existing values must supply
an ETag; a missing ETag fails closed. A new key uses first-write creation semantics.
A losing writer returns a conflict or persistence error and must reload before
another save. In the pinned Redis component, a rejected first creation without
an ETag can be surfaced as HTTP 500; it is still rejected and never acknowledged. There is
no automatic replay of a mutation or transport publication.

State writes do not return the new ETag. After a successful write, the adapter
reads back the value and allows further saves in the same turn only if it still
matches the submitted snapshot. Comparing normalized JSON preserves exact numeric
values (including integers above 2^53). A competing value, missing version, failed
read or ambiguous write invalidates the local version. This prevents an old local
snapshot from acquiring a newer writer's version and overwriting its changes.

The backend writes one key at a time, so Redis multi-key transaction rollback is
not involved. Initial-create and stale-version behavior is tested against the
pinned Redis component; other stores require equivalent first-write and ETag
semantics to be qualified. Do not configure TTLs, external writers, or manual key
deletion for these snapshots.

A second defect affected conflict reporting: Dapr wraps non-2xx actor callbacks as
HTTP 500. Known domain errors now travel as HTTP-200 transport responses containing
`{"device_actor_error":409}` (or the corresponding supported status). The actor
client decodes this into the existing domain error, and the public management API
still returns its normal error status. Infrastructure failures remain failures.
Custom clients invoking the actor API directly must understand this envelope.

## Storage and migration

The new key is `device-snapshot-cas-v1-<encoded-actor-id>` in the configured
`actor_canary.replica.state_store`. Dapr's configured app/key prefix applies.
Keep the same app ID, store, key-prefix policy and identity on every replica.
The snapshot JSON schema remains 3; the storage key and concurrency protocol change.

Before upgrading an existing actor cohort:

1. Stop admission, drain senders, and retire **all** old hosts and clients. Mixed
   fenced/unfenced hosts are unsupported, even if they share the same app ID.
2. Preserve a consistent backup of the old actor snapshots and the state component
   configuration. Resolve pending/unknown work under the existing rollout policy.
3. Start the upgraded cohort with the same actor identity and state-store settings.
   If a new fenced key is absent, a load reads the old actor snapshot. The first
   successful mutation copies its complete updated contents into the fenced key
   using conditional creation. Receipts, queues, inbox and outbox survive this copy.
4. Once a fenced key exists, it is authoritative. The old actor key is left intact
   for diagnosis and is no longer updated by this backend. Validate the upgraded
   cohort before re-enabling real-device traffic.

There is no dual write and no safe binary rollback to an unfenced host after the
new key is used: it would resume from stale old state. Roll forward or perform an
explicit offline migration. Never delete a fenced key to reset a device; doing so
could import stale legacy state. Backup/restore must preserve the authoritative
fenced keys and exclude old writers. This patch does not migrate a running site.

## Scope of the guarantee

Conditional snapshots prevent two writers based on the same stored version from
both winning. In the command path, a failed attempting-state save prevents sending;
a failed finish save leaves the persisted uncertainty boundary for recovery.
Existing tests continue to verify these no-replay rules.

This is optimistic storage concurrency, not an ownership lease or a fencing token
understood by the device or broker. A fresh reader can still become the next writer.
It does not revoke a command already sent, guarantee availability during a partition,
or protect against restoring/deleting state or running old unfenced binaries.
Real-device, restore and deployment-level qualification gates remain in force.

See the [live integration guide](device-dapr-integration.md) for packet-loss evidence
and [Dapr state concurrency semantics](https://docs.dapr.io/reference/api/state_api/).

## Live migration and restore coverage

The opt-in live suite now builds a schema-3 snapshot through the Actor APIs with
both twin revisions, a queued command, stable receipts, a notification inbox and
an unexported outbox event. It seeds the pinned Redis legacy actor key offline and
verifies that Dapr's legacy reader returns the complete snapshot. Two fenced
backends on separate sidecars load that seed; only one conditional initializer
succeeds. Duplicate requests retain their receipts without appending queue or
outbox entries, and later fenced mutations leave the legacy copy unchanged.

The restore exercise copies the authoritative Redis key, then uses Redis
`DUMP`/`RESTORE` to replace its serialized value **including version metadata**.
After discarding all writers, fresh backends recover the complete backup, retain
idempotency and can commit subsequent mutations visible through either sidecar.
An invalid authoritative value fails closed instead of falling back to legacy.
This is a per-snapshot, quiesced restore test on disposable Redis, not a site backup,
Scheduler/pubsub restore or a cross-system recovery procedure.

Two negative characterizations make the deployment restriction executable:

- Restoring an old Redis version while a writer still holds that version permits
  that writer to save again (version reuse, or ABA). Ordinary stale writes are
  rejected before the restore. A restore is therefore **not a fencing event**;
  every writer must be retired before restoring, and admission must stay stopped
  until recovery is validated.
- Removing the authoritative key causes a fresh load to import the stale legacy
  snapshot. Missing fenced keys cannot distinguish an initial migration from an
  incomplete restore. Restore inventories must include every authoritative key;
  do not delete these keys as a recovery shortcut.

These passing characterization tests document unsupported recovery behavior;
they do not make online rollback or incomplete restores safe. Restoring an older
backup can also discard receipts for externally completed work. Resolve that
uncertainty before resuming device traffic; the suite does not replay commands or
connect to real devices. Legacy remains the default runtime.
