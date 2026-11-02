# Live Dapr integration tests

**Qualification status: blocked.** The one-way Placement packet-loss extension
has observed two successful receipts for competing mutations at the same expected
revision. The safety assertion deliberately remains failing when this occurs;
this is not an approved actor-write deployment profile. Legacy remains the default.
See the packet-loss findings below.

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
and `redis:7.4.2-alpine` images, plus the packet-test helper described below. The Docker daemon must be local: the test uses bind
mounts and callbacks to the host. Linux requires `host-gateway` support and a
firewall that permits container-to-host callbacks. macOS Docker Desktop was tested;
Linux routing is supported by the fixture but has not been qualified here.

Each run creates a uniquely named private Docker network, five disposable
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
Scheduler/Placement quorum loss, backups/restores, device behavior, or capacity.
Those remain rollout gates; the bounded scenarios below narrow only the local
connection-loss coverage. Existing simulated tests still cover application
uncertainty rules; they complement this test rather than establish distributed
fencing. The test uses the production actor backend/host/client, outbox publisher
and fleet callback, but does not boot the full Controller/MQTT process.

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

The pinned Dapr 1.19.1 run returned a **backend timeout** for the delayed storage
call while Placement was disconnected. This is not an explicit stale-epoch
rejection. The observed result is blocked admission and no stale commit during
this fault/recovery window. Dapr's implementation gates actor-state transactions
on Placement and checks local actor existence; this adapter does not provide a
storage-enforced, monotonic fencing token. See the pinned
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

The assertions remain strict. This commit adds qualification tests and evidence,
not a runtime fencing fix. Each scenario uses a separate provisioned device identity. Owner tracking is
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

The result is consistent with the existing warning that Placement turn ownership
is not a storage-enforced monotonic fence in this adapter. It does not establish
which Dapr/component change would resolve the issue. Before enabling the replica
write profile, investigate ownership detection and enforce an appropriate storage
or execution fence; rerun these tests and the remaining deployment gates. Do not
weaken the compare-and-set assertion or convert this failure into an expected pass.
