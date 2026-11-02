# Live Dapr integration tests

The opt-in test in `internal/integration/dapr_test.go` runs real Dapr 1.19.1
sidecars, Placement, Scheduler and Redis 7.4.2. It leaves the legacy default and
production configuration unchanged. It does not connect to MQTT or real devices.

## Run

Start Docker Desktop (macOS) or Docker Engine (Linux), then run from the repository:

```sh
USP_TEST_DAPR=1 go test -race -count=1 -v -timeout=6m ./internal/integration
```

The normal `go test ./...` run skips this test. Explicit opt-in fails if Docker or
its dependencies are unavailable; it does not silently substitute mocks. The first
run downloads the versioned `daprio/daprd`, `daprio/placement`, `daprio/scheduler`
and `redis:7.4.2-alpine` images. The Docker daemon must be local: the test uses bind
mounts and callbacks to the host. Linux requires `host-gateway` support and a
firewall that permits container-to-host callbacks. macOS Docker Desktop was tested;
Linux routing is supported by the fixture but has not been qualified here.

Each run creates a uniquely named private Docker network, five disposable
containers, temporary component files and two independent fleet directories.
Sidecar HTTP ports are dynamically assigned and published only on loopback.
Callback listeners bind a random host port on all interfaces so containers can
reach them. They require a fixed **test-only** callback token; use this fixture
only on a development machine. Redis, Placement and Scheduler ports are not
published on the host. No existing deployment or state store is targeted.

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
production writes. It does **not** establish stale-writer fencing during asymmetric
network partitions, actual command-send crash recovery, multi-node failures,
Scheduler/Placement quorum loss, backups/restores, device behavior, or capacity.
Those remain rollout gates. Existing simulated tests still cover application
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

On 2026-11-02, the race-enabled live suite passed with Dapr 1.19.1, Redis 7.4.2 and
Docker Desktop Engine 29.9.2 on macOS. No production runtime defaults were changed.
