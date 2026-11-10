# Live Dapr integration tests

The fixture now also runs a three-member etcd 3.5.17 cluster and an etcd-backed actor cohort to test
[independent snapshot authority](device-etcd-snapshots.md). Raw Redis rollback and
deletion tests verify that actor receipts remain in etcd. Existing Redis packet-loss
cases remain compatibility regressions; the etcd path has TCP outage, kernel packet-loss, majority-loss, leader peer-partition and elected-leader failover tests.

**Snapshot concurrency repair:** replica snapshots now use conditional state
writes, and typed conflict responses survive Dapr's callback wrapping. The
historical packet-loss failure below remains recorded. See the
[repair and migration requirements](device-state-fencing.md). This does not approve
production device writes or close the remaining rollout gates.

The opt-in test in `internal/integration/dapr_test.go` runs real Dapr 1.19.1
sidecars, Placement, Scheduler and Redis 7.4.2. It leaves the legacy default and
production configuration unchanged. It does not connect to MQTT or real devices.

## Run

Start Docker Desktop (macOS) or Docker Engine (Linux), then run from the repository:

```sh
USP_TEST_DAPR=1 go test -race -count=1 -v -timeout=10m ./internal/integration
```

The normal `go test ./...` run skips this test. Explicit opt-in fails if Docker or
its dependencies are unavailable; it does not silently substitute mocks. The first
run downloads the versioned `daprio/daprd`, `daprio/placement`, `daprio/scheduler`
`redis:7.4.2-alpine` and `gcr.io/etcd-development/etcd:v3.5.17` images, plus the packet-test helper described below. The Docker daemon must be local: the test uses bind
mounts and callbacks to the host. Linux requires `host-gateway` support and a
firewall that permits container-to-host callbacks. macOS Docker Desktop was tested;
Linux routing is supported by the fixture but has not been qualified here.

Each run creates a uniquely named private Docker network, eight disposable
containers, temporary component files, per-host TCP fault proxies and two
independent fleet directories.
Sidecar HTTP ports are dynamically assigned and published only on loopback.
Callback listeners bind a random host port on all interfaces so containers can
reach them. They require a fixed **test-only** callback token; use this fixture
only on a development machine. Redis and Placement use dynamically assigned loopback ports for the fault
proxies; Scheduler is not published on the host. Each proxy binds a random host
port for container access and forwards only to its test service. No existing deployment or state store is targeted.

Normal completion and test failures remove the containers, network and temporary
files. Images remain cached. Force-killing the test process can bypass cleanup;
inspect resources named `usp-dapr-test-<run-id>` and remove only that run's resources.
Failure output includes container logs. Do not run against a production Docker host.

## What the test establishes

- Two sidecars register the same app/actor type and provisioned Device inventory.
  Concurrent identical desired mutations through both return revision 1.
- Different device identities retain separate state. A wrong sidecar API token
  fails; valid calls use separate sidecar and callback credentials.
- Real Scheduler reminders reach the actor host using the existing HTTP adapter.
- A Notify enters the actor's durable inbox/twin/outbox. Reminder-driven export
  traverses Redis pub/sub and Dapr's raw CloudEvent wrapper. At least one fleet
  callback acknowledges `SUCCESS`, and both local fleet views eventually reflect
  the observation and drained outbox.
- Stopping Redis prevents successful desired-state admission. Restarting the same
  container permits retry with a stable request ID and the expected revision.
  Redis uses AOF with `appendfsync always`; this exercise is a process outage, not
  a power-loss durability proof.
- An invocation identifies the current owner; killing that sidecar forces
  relocation. The survivor retains the original desired receipt and notification
  deduplication receipt, and receives subsequent Scheduler reminders.
- The offline transport records zero device publications throughout. Reconciliation
  remains disabled in the test policy.

This is a single-machine, real-runtime integration check, not qualification for
production writes. It does **not** establish universal stale-writer fencing during asymmetric
network partitions, actual command-send crash recovery, multi-node failures,
Scheduler/Placement quorum loss, site-wide backup/restore, device behavior, or capacity.
Those remain rollout gates; the bounded scenarios below narrow only the local
connection-loss coverage. Existing simulated tests still cover application
uncertainty rules; they complement this test rather than establish distributed
fencing. The test uses the production actor backend/host/client, outbox publisher
and fleet callback, but does not boot the full Controller/MQTT process.

## Fenced snapshot migration and restore

`fenced snapshot migration and restore` uses unregistered fixture identities with
explicitly controlled writers; no reminders or fleet workers can mutate these
snapshots. Actor APIs generate the seed and exercise receipt recovery, while
both live sidecars handle storage. The test suppresses reminder registration only
for these direct storage probes; the ordinary suite still tests real reminders.

Coverage includes explicit legacy-to-fenced snapshot copying, conditional
initialization, authoritative-key precedence, and managed application-data restore
through Dapr CAS. Duplicate desired/command/notification receipts survive restore;
pre-restore writers, stale restore preconditions and wrong identities are rejected.
Deleting a key before a save causes readers, writers and restore to fail closed.
The fixture explicitly initializes its inventory before normal actor turns.

The earlier raw Redis restore tests demonstrated version reuse and stale legacy
fallback. Runtime fallback is now removed; raw Redis restore/deletion remains
unsupported. The supported restore operation preserves and advances the live
storage version. See the [administrative workflow and remaining recovery limits](device-state-fencing.md).
Legacy remains the default, and the single-host canary backend is unchanged.

## Token mapping

The Controller's existing `DAPR_APP_TOKEN` configuration is application-specific.
For a self-hosted sidecar, set **`APP_API_TOKEN` to that same value** so Dapr sends
the `dapr-api-token` callback header. Setting only `DAPR_APP_TOKEN` on `daprd` does
not configure callback authentication. Keep `DAPR_API_TOKEN` as a separate value
for Controller-to-sidecar requests. The live fixture uses distinct values to
exercise both directions.

References: [Dapr Docker hosting](https://docs.dapr.io/operations/hosting/self-hosted/self-hosted-with-docker/),
[app token authentication](https://docs.dapr.io/operations/security/app-api-token/).

## Recorded validation

After adding managed restore and removing implicit fallback on 2026-11-03, the
full race-enabled live suite passed (192 seconds), including all packet-loss modes.
The repository race suite and `go vet ./...` also passed.

On 2026-11-03, the full race-enabled live suite passed with the migration and
restore probes, all eight packet-loss/control cases, and the existing recovery
checks (189 seconds). `go test -race ./...` and `go vet ./...` also passed.

Before the packet-loss extension, on 2026-11-02 the race-enabled live suite passed
with Dapr 1.19.1, Redis 7.4.2 and
Docker Desktop Engine 29.9.2 on macOS. No production runtime defaults were changed.

## Network partitions and delayed state writers

The same opt-in run now includes these live subtests. The fault injector closes
existing TCP streams and rejects reconnections until healed. It is a real network
boundary around the selected host's dependency, not a mock error from the actor
backend. Its own race-enabled test verifies established-stream closure, rejected
reconnects and recovery.

1. **Owner-only Redis route loss.** Identify the actor owner through a real
   invocation, cut that host's Redis proxy, and submit the same desired request
   through both sidecars. Neither may acknowledge success. An independent Redis
   PING through the unaffected host proves that the service itself is healthy.
   Heal the route and retry the stable request ID; exactly the expected desired
   revision must be returned.
2. **Owner-only Placement loss with a delayed writer.** Hold a real actor snapshot
   immediately before its backend save, then close only that sidecar's Placement
   stream. Require the other host to commit a different value at the next
   revision before releasing the old write. While isolated, a new invocation
   through the old sidecar must fail without entering another mutation callback;
   a Redis PING proves its state path is still reachable.
3. **Delayed-write and recovery assertions.** Release the old snapshot using the
   production Dapr backend with a fresh cancellation context. This deliberately
   simulates a storage call that outlives its original invocation; the injection
   exists only in tests. It must not succeed. Check the replacement's exact value
   and revision, reconnect Placement, then query both sidecars repeatedly for ten
   seconds. A late overwrite or inconsistent state fails the test.

Before the snapshot repair, the pinned Dapr 1.19.1 run returned a **backend
timeout** for the delayed storage call while Placement was disconnected. With
conditional snapshots, the delayed write instead returns a conflict. The historical timeout was not an explicit stale-epoch rejection. The observed result is blocked admission and no stale commit during
this fault/recovery window. Dapr's implementation gates actor-state transactions
on Placement and checks local actor existence; the old adapter had no conditional-write protection. The repaired backend
uses version-conditional snapshots, not an ownership-epoch lease. See the pinned
[state transaction implementation](https://github.com/dapr/dapr/blob/v1.19.1/pkg/actors/state/state.go)
and [Placement disconnect handling](https://github.com/dapr/dapr/blob/v1.19.1/pkg/actors/internal/placement/loops/placement/placement.go).

Do not interpret passing these tests as proof of fencing in all failure modes.
The proxy-cut scenarios model detected connection loss. The packet-filter
scenarios below additionally cover bounded silent-loss windows. Neither pauses a Redis
transaction after server acceptance, test an uncancellable command already sent
to a device, or establish safety across restored state/ownership epochs. Those
require additional deployment-level qualification and potentially stronger
fencing. Legacy remains the default; no runtime guard is relaxed by this suite.

## One-way packet loss and half-open TCP connections

The live suite also uses `nicolaka/netshoot:v0.14` to enter only a disposable
sidecar's network namespace with `NET_ADMIN`. It does not use the host network,
privileged mode, host mounts or the Docker socket inside the helper. Docker must
allow that capability and provide kernel iptables support; missing support fails
the opt-in run. One temporary helper is created at a time per test scenario.
The helper image remains cached after cleanup.

For each of the owner's Redis and Placement paths, the suite tests:

| Fault | Kernel rule |
|---|---|
| Outbound loss | Drop TCP packets to the dependency proxy port in `OUTPUT` |
| Inbound loss | Drop TCP packets from that port in `INPUT` |
| Half-open black hole | Apply both rules while established connections remain open |

These are 100% packet-loss windows, including TCP acknowledgments and retransmits,
not random loss rates or application bytes silently discarded by a proxy. Rules
apply to the existing connections and reconnect attempts. The helper never sends
a reset or closes a socket to inject the fault. For the half-open case, `ss` must
show the **same local/remote socket tuple** still `ESTABLISHED` two seconds after
injection; comparing queue sizes alone is insufficient. Applications may later
close their sockets on timeout. Here “half-open” means an apparently established
connection with a black-holed peer path, not a TCP `shutdown(SHUT_WR)` half-close.

Packet counters must prove that the installed `DROP` rules were exercised. For a
bidirectional black hole, one direction may see zero packets because the other
rule prevented any response from being generated. All rule changes are confined
to dedicated test chains and the selected dependency port. Healing clears only
those chains; cleanup removes the exact jumps/chains and the helper container.
No dependency or sidecar restart is used to heal these scenarios.

State-path cases require failed acknowledgment during loss, a healthy independent
Redis route, and recovery of a single receipt at the expected desired revision.
Placement cases wait twelve seconds after injection before issuing two competing
compare-and-set requests through different sidecars. They permit zero or one
acknowledged winner during loss: availability depends on direction and detection
timing. Two successful mutations at the same expected revision fail the test.

After healing, every candidate is retried with its original request ID. Exactly
one winner must return its receipt and all competing candidates must conflict.
Both routes must agree on that receipt and the exact desired value/revision, which
must remain stable over another five-second observation window. Timeouts are
always treated as ambiguous; the test does not assume that lack of acknowledgment
means lack of persistence. The transport remains offline and reconciliation stays
disabled throughout.

This adds bounded silent-loss coverage to the connection-reset tests. It is not
proof for arbitrary loss durations, loss percentages, reordering, multi-node
partitions, ownership restoration, storage-transaction acknowledgment loss, or
commands already emitted to devices. The twelve-second wait is a fixture
observation interval, not a Dapr failover SLA or proof that every timer expired.
See [netshoot's network-namespace documentation](https://github.com/nicolaka/netshoot)
for the helper model.

### Packet-loss finding recorded on 2026-11-02

With Dapr 1.19.1, Redis 7.4.2, and the existing Device actor backend, the
outbound, inbound and bidirectional Placement-loss scenarios each produced
successful responses to two distinct requests using expected desired revision 1.
Both receipts claimed revision 2. The test reported
`conflicting revisions admitted during packet loss`. An earlier short-loss run
instead failed receipt recovery within the sixty-second observation bound. These
results are timing-dependent; a later passing run does not erase the observed
safety failure.

The historical assertions remain strict. They originally exposed the defect;
the subsequent [snapshot repair](device-state-fencing.md) addresses it without
weakening those assertions. Each scenario uses a separate provisioned device identity. Owner tracking is
per device. The full run and a follow-up packet-only run produced the same safety
failure; the ordinary suite (which skips live Docker tests) and `go vet` passed.

| Dependency | Outbound | Inbound | Half-open black hole |
|---|---|---|---|
| Redis | Passed | Passed | Passed |
| Placement | Failed: conflicting receipts | Failed: conflicting receipts | Failed: conflicting receipts |

Representative test evidence, using fixture-only request IDs:

```text
id=packet-placement-outbound-a receipt={"ID":"packet-placement-outbound-a","Revision":2} error=<nil>
id=packet-placement-outbound-b receipt={"ID":"packet-placement-outbound-b","Revision":2} error=<nil>
conflicting revisions admitted during packet loss
```

The DROP counters were nonzero, and half-open cases verified the original
established socket tuple before the twelve-second loss interval. Recovery checks
for a failing conflict scenario are not reported as passed: that subtest fails
immediately, heals its rules, and lets the remaining independent scenarios run.

The historical result is consistent with the existing warning that Placement turn ownership
is not a storage-enforced monotonic fence in this adapter. It does not establish
which Dapr/component change would resolve the issue. The snapshot repair supplies storage concurrency protection. Before enabling the
replica write profile, complete the remaining deployment and external-effect gates. Do not
weaken the compare-and-set assertion or convert this failure into an expected pass.

## Snapshot repair validation

The repaired fixture uses the same fenced backend as replica runtime mode and
retains the original conflicting-receipt assertions. Added healthy-network controls
require stable conflict recovery without injected faults. A live storage probe
loads the same absent key through both sidecars, permits one creator, rejects the
other, and then rejects a stale existing version. Unit coverage includes migration
seeding, missing ETags, ambiguous writes, version invalidation and avoiding reuse
of another writer's version after read-back.

The previous failure table is historical evidence, not the expected result of the
repaired suite. Replica storage migration is a quiesced operation; see the linked
repair guide before deploying this change.

The complete repaired live run passed with Dapr 1.19.1 and Redis 7.4.2 in about
188 seconds, with the race detector enabled. Results after the repair:

| Check | Result |
|---|---|
| Competing first creation and stale existing version | Passed |
| Owner-only Redis/Placement cuts and delayed stale write | Passed |
| Healthy-network state/Placement controls | Passed |
| Redis outbound, inbound and half-open packet loss | Passed |
| Placement outbound, inbound and half-open packet loss | Passed |
| Receipt recovery, reminders, events/fleet and owner loss | Passed |

The full ordinary race-enabled suite and static checks also passed. The historical
failure evidence remains above to explain why the storage protocol changed.

## Etcd client-path and quorum faults

Validated on 2026-11-05: the full race-enabled live suite passed in 273 seconds,
alongside the repository race suite and static checks.

The three etcd members use their own peer ports and persistent container data for
the duration of the test. The contacted member stays running during quorum loss.
The `etcd packet loss` subtests inject DROP on that member's **client port only**;
directions are relative to the server (outbound loses responses, inbound loses
requests). Netshoot shares only the disposable member's network namespace and
receives NET_ADMIN; no host firewall or production service is modified.

Tests check preloaded CAS ambiguity, Dapr admission failure, stable receipt recovery,
and stale-writer rejection. The `etcd quorum loss` subtest proves two-of-three
availability, one-of-three rejection despite a reachable endpoint, and recovery
using the original stopped members. See the [scope and assertions](device-etcd-snapshots.md#tests-and-scope).

## Etcd peer partition and elected-leader failover

Validated on 2026-11-05: full live suite passed in 299 seconds; the focused rejoin
assertions, repository race tests and static checks also passed.

`etcd peer partition` identifies the current elected leader and isolates its peer
port 2380 using DROP rules in that disposable member's namespace. Its client
endpoint stays alive. The majority must elect a different leader at a higher term,
while the isolated member loses leadership and rejects linearizable admission and
snapshot CAS. Majority writes, stable-ID retries and stale-writer rejection are
verified before and after healing.

`etcd leader failover` SIGKILLs the discovered leader, checks the replacement term,
then restarts the original member with its existing storage. Both tests verify
that baseline and recovery receipts survive rejoin. Fixture proxies are explicitly
retargeted to the majority and back; no automatic gateway failover is claimed.
No production runtime or default configuration changes are made.

## Automatic etcd endpoint failover

A separate actor cohort receives a fixed list of three etcd gateway proxies. The
`etcd automatic endpoint failover` cases force it to use the elected leader, then
break its connection, isolate its peer links, or kill it. Other endpoints are
available before the fault starts. Recovery must succeed without retargeting
proxies or rebuilding the backend during the fault. Stable receipts survive the
transition; preloaded writes fail ambiguously rather than being replayed, and the
next read uses an alternate endpoint. Docker's ephemeral host port mappings are
refreshed only before each scenario, never to make failover succeed.

The older single-endpoint leadership tests remain in place to exercise rejection
at an isolated endpoint directly. They are distinct from this automatic client
failover coverage. Legacy remains the default.

Validated on 2026-11-06: `go test -race ./...`, `go vet ./...`, and the complete
`USP_TEST_DAPR=1 go test -race -count=1 -v -timeout=15m ./internal/integration`
suite passed. The live suite completed in 314.470 seconds, including all three
automatic failover cases and the existing packet-loss, quorum, restore and fencing
checks. This remains single-machine qualification with offline device transport.

## Multi-endpoint packet loss and quorum loss

`etcd multi-endpoint packet loss` runs control, outbound-only, inbound-only and
half-open cases against the preferred member's client port 2379. Directions are
relative to the etcd server. The half-open case verifies that an original TCP
connection remains established during the black hole. Peer links and alternate
client endpoints remain healthy. A preloaded transaction must return an ambiguous
error through the dropped path, invalidate its loaded version, and allow the next
read through an alternate endpoint within the bounded read budget. Both Dapr
routes must admit and return the same receipt **before** the fault is healed.
After healing, the receipt remains stable and a separate stale writer is rejected.

`etcd multi-endpoint quorum loss` isolates peer port 2380 on one member, proves
majority admission, then isolates another member. All three client listeners stay
reachable and can serve local serializable reads, but no member retains leadership.
The fixed endpoint list must reject a linearizable snapshot read, preloaded CAS,
and actor reads/writes on both sidecars. The failed snapshot read is checked against
the five-second total budget with one second of scheduling tolerance. Healing one
peer restores majority admission and the stable receipt; healing the last peer
preserves it. A stale writer and a failed writer's invalidated version cannot
replace recovered state.

These scenarios refresh Docker's ephemeral port mappings and select the initial
endpoint before injection only. They do not retarget proxies, rebuild clients or
change endpoint lists during a fault. Packet-drop counters verify fault injection.
All rules are confined to disposable container namespaces; transport to devices
remains offline. Production code and configuration are unchanged; legacy remains
the default.

Validated on 2026-11-08: `go test -race ./...`, `go vet ./...`, and the complete
`USP_TEST_DAPR=1 go test -race -count=1 -v -timeout=15m ./internal/integration`
suite passed. The live suite completed in 405.894 seconds, including all four
multi-endpoint packet-loss cases and the multi-endpoint quorum-loss case.

## All-endpoint packet loss and staged recovery

`etcd all-endpoint packet loss` covers outbound-only, inbound-only and half-open
black holes across all three configured client endpoints simultaneously. Directions
are relative to the etcd servers. Each client path is warmed before injection;
the half-open case checks that an original established connection survives at each
member. Rules target only client port 2379, leaving peer port 2380 untouched. The
leader and term must remain unchanged across the scenario.

A preloaded transaction must return an ambiguous error. Snapshot reads must fail
within the five-second total budget (one second of scheduling tolerance), and a
250 ms caller deadline must also be honored (one second maximum elapsed). Both
Dapr routes must reject actor reads and desired-state admission while every client
path is black-holed. Packet-drop counters are checked on every member.

Recovery first heals only the **last configured endpoint**. The existing backend
must recover through bounded read attempts, and both actor routes must return
one stable receipt while the earlier endpoints remain blocked. Healing all paths
must restore successful reads through each individual endpoint, preserve that
receipt and reject a separately retained stale version. Endpoint readiness uses a
separate read probe, so it does not refresh the stale writer's loaded version. A failed
writer cannot reuse its invalidated version after partial recovery. Endpoints,
proxy targets and backends are not replaced during injection or recovery. The
separate stale-writer probes in both packet-loss cohorts close only their idle TCP
connections after healing,
retaining their loaded versions so that the CAS check reaches etcd rather than timing
out on a connection still in retransmission backoff. Actor recovery uses existing
clients without this intervention.

This extends the multi-endpoint control and preferred-endpoint tests above. All
faults are limited to disposable container namespaces with offline device
transport. No production code or default configuration changes are required;
legacy remains the default.

The existing automatic leader-kill check also waits for the replacement leader's
reported term to advance, rather than failing on a transient status sample that
already exposes the new leader but still reports the preceding term. Both a new
leader and a higher term remain required.

Validated on 2026-11-08: `go test -race ./...`, `go vet ./...`, and the complete
`USP_TEST_DAPR=1 go test -race -count=1 -v -timeout=15m ./internal/integration`
suite passed. The final full live run completed in 611.485 seconds, including all
three all-endpoint cases, the recovered-gateway probes, and the existing packet,
quorum, leadership, restore and fencing checks.

## Etcd rolling restart and recovery

`etcd rolling restart` gracefully stops and starts each of the three members,
one at a time, retaining the same container and data directory. It starts with a
follower, then the original leader, then the remaining member. Before the next
stop, every member must agree on an elected leader and the restarted gateway must
serve a linearizable snapshot read. Restarting the current leader must produce a
different leader with a higher term.

While each member is confirmed stopped, the original multi-endpoint reader must
recover within bounded read attempts, and both Dapr routes must admit the same
stable desired-state receipt. No proxy retargeting or member restart is allowed
before these availability checks pass. After rejoin, all member IDs must match
their original values, the receipt and exact twin must remain intact, and a writer
holding a pre-restart version must receive a CAS conflict. Every earlier receipt
is replayed on both actor routes after the entire roll, and all endpoints must
serve reads again.

Actor endpoint lists and backend instances stay fixed. Docker can reallocate an
ephemeral published port when a container starts; after proving availability with
that member down, the fixture updates only that member's proxy to its new port.
This restores access to the same member, not a route to another surviving member.
The separate single-endpoint fixture mapping is refreshed after the drill. These
are single-machine, same-version restart tests with offline device transport,
not mixed-version upgrade qualification. Production code and configuration are
unchanged; legacy remains the default.

Validated on 2026-11-09: `go test -race ./...`, `go vet ./...`, and the complete
`USP_TEST_DAPR=1 go test -race -count=1 -v -timeout=15m ./internal/integration`
suite passed. The full live suite completed in 577.264 seconds, including all
three rolling member restarts and the existing packet-loss, quorum, restore and
fencing checks. The focused rolling-restart run also passed.

## Etcd full-cluster restart and recovery

`etcd full-cluster restart` runs graceful-stop and abrupt SIGKILL scenarios against
all three members. It retains the original containers, membership and data
directories; it does not restore a backup or force a new cluster. Every member must
be confirmed stopped. Preloaded snapshot CAS, bounded snapshot reads, and actor
reads/writes on both Dapr routes must reject the outage. Restarting only member
zero must still reject reads and actor admission without a majority.

Restarting a second original member must restore the existing reader and both
actor routes. Before admitting a new desired-state change, the test inspects the
committed twin and requires a CAS conflict for a version made obsolete **before**
the outage. A writer whose outage transaction failed cannot reuse its invalidated
version. The outage command probe intentionally has an obsolete expected revision,
so a delayed turn cannot advance the fence after quorum returns and mask recovery
of an obsolete version.

The third member then rejoins. Every gateway must serve snapshot reads, each
member ID and cluster ID must match their pre-outage values, and every pre-outage
and recovery receipt must replay unchanged on both actor routes. The exact final
twin must remain intact. Process start timestamps prove actual restarts. As with
the rolling drill, only Docker's mapping to each same restarted member is refreshed;
actor endpoint lists and backend instances stay fixed. Legacy remains the default.
This is same-version, single-machine restart qualification with offline device
transport, not backup restoration or disaster recovery from lost data directories.

Validated on 2026-11-09: `go test -race ./...`, `go vet ./...`, and the complete
`USP_TEST_DAPR=1 go test -race -count=1 -v -timeout=15m ./internal/integration`
suite passed. The full live run completed in 643.766 seconds, including graceful
and abrupt full-cluster recovery plus the existing rolling restart, packet-loss,
quorum, restore and fencing checks. Both new cases also passed in a focused run.

## Native etcd snapshot backup and isolated restore

`etcd snapshot backup restore` takes a real `etcdctl snapshot save` backup from
the live source cluster after a linearizable read on the chosen member. It checks
snapshot hash/revision/key metadata with the pinned etcd 3.5.17 `etcdutl`, then
advances the source twin. A copy with a damaged appended checksum must fail restore
without `--skip-hash-check`.

All three restored members are built from the same valid backup into fresh named
volumes on a separate Docker network. The restore uses a new cluster token,
`--bump-revision 1000000` and `--mark-compacted`; the test verifies new cluster
identity, agreement among the restored members, a revision above both the backup
plus the bump and the observed post-backup source revision, and rejection of reads
at the compacted backup revision. The fixed bump is a bounded test value, not a
production sizing recommendation. See [etcd recovery guidance](https://etcd.io/docs/v3.5/op-guide/recovery/).

Fresh backend clients load the restored state without initialization or implicit
import. An offline Device Actor verifies the backup's exact desired twin and
idempotent receipt. It admits a distinct payload under the source's post-backup
request ID, proving that later receipt was absent from the backup. A second writer
retaining the earlier restored revision must receive a CAS conflict, and the
recovered twin must remain unchanged. The source's later twin and receipt must
also remain unchanged, demonstrating isolation.

The restored actor uses the real etcd backend and actor state/receipt logic but
has a no-op reminder adapter, no transport and no event publisher. It is not
registered with the source Dapr deployment. Source endpoints are never repointed;
no surviving source writer is connected to the restored cluster. This qualifies
native snapshot integrity and an isolated restore rehearsal, not an in-place
production cutover, external device reconciliation or safe rollback with surviving
writers. All restored containers, volumes and networks are disposable and cleaned
up. Production code and configuration are unchanged; legacy remains the default.

Validated on 2026-11-09: `go test -race ./...`, `go vet ./...`, and the complete
`USP_TEST_DAPR=1 go test -race -count=1 -v -timeout=15m ./internal/integration`
suite passed. The full live run completed in 659.253 seconds, including the native
snapshot backup/restore case and all existing restart, packet-loss, quorum and
fencing checks. The focused backup/restore run also passed.

## Restore cutover and stale transactions

`etcd restore cutover` uses a dedicated, quiescent device key outside the Dapr
inventory, stable gateway proxies, and two disposable native restores of the same
backup. The test actors have no reminders, publishers or device transport. The
source advances after backup, providing both pre-backup and post-backup writers.

The **cutover without per-key rewrites** now verifies an atomic runtime guard.
Native restore preserves key modification revisions, so a CAS alone is insufficient.
Each write includes a historical count-only range at the global revision observed
by its preceding linearizable read, in the same transaction as the conditional
put. etcd checks that historical revision before applying any writes. A restore
with `--bump-revision` above every source revision and `--mark-compacted` rejects
the old witness even when the key CAS would succeed.

The test holds a pre-backup transaction before network dispatch, switches the
stable endpoint URLs to the restored cluster, then releases it. It requires a
server rejection and independently verifies unchanged snapshot bytes and ETag.
It also rejects a cached single-endpoint writer without a per-key rewrite. Both
single- and multi-endpoint backends pin cluster identity, preventing an old
backend from reloading the replacement cluster. A fresh backend can immediately
write the restored snapshot; no metadata initialization or per-key rewrite is
needed for the historical guard.

The **guarded cutover** retains the additional conditional per-key rewrite using
`ExportSnapshot` / `RestoreSnapshot` before admission. It checks pre/post-backup
writers and a delayed transaction, rejects historical restore authorization,
recovers the twin and receipt with fresh clients, and admits a new request
idempotently. The source remains unchanged. The test actors have no device
transport, event publisher or active reminders.

This is not an automated production cutover. The atomic guard requires a revision
bump strictly above **all** revisions that source writers could have observed,
including after backup, and compacted old history. Restores without those
conditions remain unsupported; cluster-ID response checks alone cannot prevent a
write. Stop source writers, establish the revision bound, retire old processes and
credentials, validate the complete restored inventory, and reconcile external
effects before reopening admission. Older binaries without the historical guard
still require the per-key rewrite fence and retirement. Ordinary compaction can
reject an in-flight write too: reload and retry the stable request ID, never replay
the old transaction. Legacy remains the default.

The atomicity assumption is verified against pinned
[etcd v3.5.17 transaction validation](https://github.com/etcd-io/etcd/blob/v3.5.17/server/etcdserver/apply.go):
`Txn` checks range requests before opening the mutable transaction, and
`checkRequestRange` rejects compacted historical revisions.

Validated on 2026-11-09: `go test -race ./...`, `go vet ./...`, and the complete
`USP_TEST_DAPR=1 go test -race -count=1 -v -timeout=15m ./internal/integration`
suite passed (637.803 seconds). The focused native backup/restore and cutover
suite was rerun with the final assertion checking etcd's compacted-history error.
Unit tests also verify the transaction's historical range, create-only witness,
failed-version invalidation, and recovery after ordinary compaction by reloading
the global revision while the key revision remains unchanged.

## Live etcd compaction and recovery

`etcd compaction recovery` runs logical and physical (`etcdctl compact --physical`)
compaction against the live three-member etcd 3.5.17 fixture, with both one and
three configured endpoints. Each case uses a dedicated device outside the Dapr
inventory, an offline actor and a no-op reminder adapter. It creates no device
transport or event publisher.

The test holds the actor's first command transaction after its snapshot read,
advances an unrelated key, and compacts at that newer global revision. The device
key's modification revision remains unchanged, so its CAS alone would still
succeed. The released transaction must receive etcd's explicit compacted-history
error, with exactly one transaction attempt. Independent reads verify that both
snapshot bytes (including receipts) and ETag are unchanged. A second cached writer
is also rejected, and its invalidated version cannot be reused.

The same actor and backend then reload and retry the same request ID. They must
commit revision 2, and a subsequent duplicate must return the stored receipt
without another transaction or ETag change. A fresh actor verifies the recovered
twin and the original baseline receipt. This demonstrates that ordinary
compaction can temporarily reject an in-flight write without preventing an
unchanged old key from being updated after a fresh global-revision read.

These cases qualify explicit manual compaction and application recovery; they do
not simulate automatic retention timing, defragmentation, disk exhaustion or
packet loss during compaction. Existing tests cover network faults separately.
Production code and configuration are unchanged; legacy remains the default.

Focused validation on 2026-11-09: all four cases passed with the race detector;
the focused live run completed in 19.939 seconds.

Full validation on 2026-11-09: `go test -race ./...`, `go vet ./...`, and
`USP_TEST_DAPR=1 go test -race -count=1 -v -timeout=15m ./internal/integration`
passed. The full live suite completed in 641.986 seconds, including all four
compaction cases and existing partition, restart, backup and restore checks.

## Live etcd automatic compaction and recovery

`etcd automatic compaction recovery` starts a separate three-member etcd 3.5.17
cluster with `--auto-compaction-mode periodic --auto-compaction-retention 2s`.
The server runs its normal compactor and real clock. No manual compaction request
is sent to this cluster. The main Dapr/etcd fixture retains its existing settings.
The short retention is a test setting, not a production recommendation. See the
[pinned periodic compactor implementation](https://github.com/etcd-io/etcd/blob/v3.5.17/server/etcdserver/api/v3compactor/periodic.go)
and [etcd maintenance guide](https://etcd.io/docs/v3.5/op-guide/maintenance/).

Both single- and three-endpoint backends run two successive retention cycles.
Each case uses an offline Device actor outside the Dapr inventory, with no active
reminders, event publisher or device transport. A test gate holds a snapshot save
before the backend starts its HTTP transaction, after the actor has loaded its
state. This permits waiting for the actual retention policy without changing the
production five-second HTTP timeout. The gate is released only after every member
rejects a historical range at that exact captured read revision with the explicit
compacted-history error. An unrelated key advances the global revision while the
device key's modification revision remains unchanged. A historical read must
succeed before advancement, proving the test observes a real retention transition.

The released save must submit exactly one transaction and receive etcd's
compacted-history error. Independent exports require unchanged snapshot bytes,
receipts and ETag. A cached writer must also fail; reusing the actor backend's
invalidated version must send no transaction. The same actor then reloads and
retries the stable request ID, committing once. Its duplicate must return the
stored receipt with no additional transaction or ETag change. A fresh actor
verifies the recovered twin and all earlier receipts after each cycle. Server
logs must also contain an automatic periodic compaction completion.

This qualifies repeated periodic retention expiry and application recovery on a
local cluster. Revision-count automatic compaction, leader changes or network
faults during automatic compaction, defragmentation, disk pressure and production
retention sizing remain outside these cases. Existing tests cover network and
leader failures separately. Production code and configuration are unchanged;
legacy remains the default.

Validated on 2026-11-10: `go test -race ./...`, `go vet ./...`, and the complete
`USP_TEST_DAPR=1 go test -race -count=1 -v -timeout=15m ./internal/integration`
suite passed. The focused automatic compaction run completed in 41.076 seconds;
the full live suite completed in 671.735 seconds, including both endpoint
configurations with two retention cycles each and the existing network, restart,
manual compaction, backup and restore checks.
