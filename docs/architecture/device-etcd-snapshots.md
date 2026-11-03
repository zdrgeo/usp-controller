# Independent authoritative snapshots

Replica snapshots now live in a dedicated etcd cluster, separate from Redis's
backup, restore and deletion domain. Redis remains the Dapr actor infrastructure
and event pubsub store. Legacy remains the default; the single-host canary retains
its existing backend. This change does not deploy infrastructure or migrate data.

## Why a separate authority

Restoring a Redis snapshot restores its ETag too. No check stored in that same
backup can reliably distinguish a surviving stale writer from a valid old writer.
Redis's Dapr upsert also accepts an ETag write after key deletion. Preflight reads
cannot close that race.

Replica snapshot operations now use etcd's v3 JSON gateway directly. Reads are
linearizable. Creation compares `CREATE == 0`, and updates compare `MOD == the
loaded revision`, with the put inside the **same transaction**. The adapter checks
`Succeeded` and adopts the successful transaction's revision. Failed or ambiguous
writes invalidate local state and are never automatically retried. A missing key
fails closed; deleting and recreating an etcd key produces a new global revision,
so a writer holding the deleted key's revision still loses.

The pinned Dapr components-contrib v1.19.0 etcd adapter performs initial existence
validation separately from the write and does not inspect transaction success in
its ordinary Set path. It is therefore not used for authoritative snapshots. Dapr
still provides actor invocation, reminders and event publishing; no SQL command
claiming or process-local ownership lock is introduced.

## Configuration

All replicas must specify the same endpoint and namespace:

```json
"replica": {
  "instance_id": "controller-pod-a",
  "shared_group": "site-001-controller",
  "app_id": "usp-controller-site-001",
  "state_store": "device-actor-state",
  "snapshot_etcd_url": "https://snapshot-etcd.example:2379",
  "snapshot_etcd_prefix": "/usp/site-001/snapshots/"
}
```

`state_store` still identifies the Dapr actor infrastructure component. Missing
etcd settings reject replica configuration; there is no Redis snapshot fallback.
Snapshot keys are `<snapshot_etcd_prefix><encoded-device-identity>`; Dapr app/key
prefixing does not apply. Use a dedicated stable namespace per cohort and verify
all replica configurations before admission. Activate/readiness exercises the
snapshot read through the actor, so etcd access is required for dispatch readiness.

The endpoint must expose the etcd v3 JSON gateway. HTTPS is required outside
loopback. `ETCD_CA_FILE` optionally adds a private CA; `ETCD_CERT_FILE` and
`ETCD_KEY_FILE` enable mutual TLS. `ETCD_AUTH_TOKEN` optionally supplies the etcd
Authorization token (provision/refresh it operationally; the adapter does not log
in automatically). These credentials are distinct from `DAPR_API_TOKEN`.
Redirects are rejected. Restrict etcd permissions to the cohort prefix and keep
the cluster's storage and recovery lifecycle independent of Redis. Production
etcd quorum, authentication, capacity and recovery still require qualification.

## Quiesced migration from existing Redis snapshots

Stop admission, drain senders and retire every old writer first. Preserve source
backups and the complete device inventory. Do not run old Redis-writing replicas
alongside etcd replicas. The CLI's Dapr `-state-store` mode is now **read-only** and
exists for exporting migration sources.

```sh
umask 077
# Uses DAPR_API_TOKEN. Export each authoritative fenced Redis snapshot.
go run ./cmd/device-snapshot -action inspect \
  -tenant tenant-a -controller controller-a -device device-a \
  -state-store device-actor-state > device-a.json

# Uses ETCD_* credentials. Import is validated, atomic and create-only.
go run ./cmd/device-snapshot -action import \
  -tenant tenant-a -controller controller-a -device device-a \
  -etcd-url https://snapshot-etcd.example:2379 \
  -etcd-prefix /usp/site-001/snapshots/ \
  < device-a.json > imported-device-a.json
```

The import preserves the complete application snapshot, including receipts,
queues, twin, inbox and outbox; it never imports Redis's ETag. A second import
cannot overwrite an existing target. Inspect and validate every imported device
before enabling traffic. Do not fall back to the obsolete actor snapshot when a
fenced Redis source is missing: resolve that recovery separately.

For a verified new device, use `-action initialize` with the etcd flags. For an
older cohort that never used fenced Redis keys, `-action migrate-legacy` explicitly
imports the legacy Dapr actor snapshot with those same flags. These administrative
paths consult Dapr for source validation; runtime loads never import anything.
A failed response can be ambiguous: inspect the target before retrying.

## Restore and recovery boundaries

`inspect` and `restore` work with the etcd flags. Restore consumes an exported
backup on stdin and requires `-expected-etag` from a **fresh target inspection**.
It updates application data conditionally, without restoring historical storage
revisions. Drain senders and reconcile possibly executed work before restoring;
restoring old receipts cannot undo physical device operations.

Raw Redis rollback or deletion cannot change authoritative snapshots or revive
etcd stale writers. Redis loss can still disrupt reminders, event delivery and
availability. The tests do not claim exactly-once broker delivery or external-send
fencing. Already-authorized external effects cannot be revoked by state storage.

**Do not roll back the authoritative etcd cluster with surviving writers.** Moving
the authority removes Redis from the snapshot recovery domain; it does not make
arbitrary rollback of all system state safe. A whole-etcd restore requires retiring
writers and a separately qualified disaster-recovery procedure (including revision
handling and external effects). Never point replicas to an empty namespace to
reset a device. Direct administrative imports remain explicit operator actions.

## Tests and scope

On 2026-11-03, `go test -race ./...`, `go vet ./...`, and the full opt-in live
suite passed (189 seconds). A focused rerun also passed the strengthened etcd
deletion/recreation stale-writer case.

The live fixture adds isolated etcd 3.5.17 to the pinned Dapr 1.19.1 / Redis 7.4.2
stack. A provisioned actor uses etcd while the older cohort remains as Redis
compatibility regression coverage. Tests invoke the etcd-backed actor through
Dapr, perform real Redis `DUMP`/`RESTORE` and `DEL` on obsolete snapshot keys, and
verify retained receipts and rejection of stale writers. They also cover etcd
connectivity failure/recovery, simultaneous initialization, managed restore,
atomic rejection after etcd deletion/recreation, and create-only Redis-to-etcd
import. Unit tests cover failed transaction responses, ambiguous writes, TLS
configuration and redirect/credential isolation.

The existing one-way packet-loss/half-open tests still exercise the Redis
compatibility cohort and Placement; they are not an etcd packet-loss qualification.
The new etcd outage test closes TCP streams through per-host proxies. Whole-cluster
etcd rollback, quorum loss, WAN behavior and real-device effects remain open gates.

References: [etcd API guarantees](https://etcd.io/docs/v3.5/learning/api_guarantees/),
[etcd JSON gateway](https://etcd.io/docs/v3.5/dev-guide/api_grpc_gateway/),
[pinned Dapr etcd implementation](https://github.com/dapr/components-contrib/blob/v1.19.0/state/etcd/etcd.go).
