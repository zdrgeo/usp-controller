# Phase 5: experimental replica placement profile

Status: application implementation and local tests. Legacy remains the default.
Live Dapr placement, forced kill and network-partition qualification is **not yet
complete**. Do not increase replicas in the existing K3s manifest. This profile
is for controlled qualification before a production rollout.

## Topology and configuration

Each replica hosts the same DeviceActor inventory and has a local Dapr sidecar
and MQTT connection. All replicas use the same logical Controller endpoint,
tenant, Dapr app ID/namespace, actor-state component/backend and pubsub backend.
They subscribe to `$share/<group>/<device inbox>`. The broker delivers each inbound
message to a member, which validates the provisioned inbox and invokes the actor
through Dapr. The actor may be on another host. Its current host publishes commands
through that host's MQTT connection. Responses and notifications may arrive at any
member; no correlation lives in the ingress process.

Add `replica` inside an enabled phase-4 `actor_canary` configuration:

```json
"replica": {
  "instance_id": "controller-pod-a",
  "shared_group": "site-001-controller",
  "app_id": "usp-controller-site-001",
  "state_store": "device-actor-state"
}
```

These values are nonempty simple names (letters, digits, dot, underscore, hyphen).
Set a distinct `instance_id` in each replica's generated configuration. The MQTT
client ID becomes `<client_id>/<instance_id>`; never reuse a live identity or allow
broker client-ID eviction to act as failover. Keep the group stable and unique to
this logical Controller cohort. The MQTT broker must support shared subscriptions;
explicit lack of support or rejected SUBACK fails startup. Provision broker ACLs
for shared filters and the underlying per-device inboxes. The received topic and
announced/ResponseTopic remain ordinary inboxes, never `$share/...` filters.
Each connection sends the same logical Connect announcements on reconnect; devices
must tolerate duplicates. Shared subscriptions do not promise per-device ordering.

Replica mode requires **every** provisioned Agent in the explicit actor allowlist.
It rejects mixed legacy/actor cohorts, because a shared delivery could otherwise
reach the wrong synchronous waiter. To split a site, use separate provisioned
inventories and nonoverlapping broker routes. Each replica needs its own local
lock and journal files; the lock only excludes duplicate local processes using
that path. The journal is unused for the actor-only cohort and is not actor state.
Omitting `replica` preserves single-host canary behavior. Omitting/ disabling
`actor_canary` preserves legacy behavior and its existing deployment defaults.

## Shared state and readiness

Configure a shared transactional actor store with `actorStateStore: true` in Dapr,
accessible from every replica, and a durable event pubsub component. Reuse the
same backend/keyspace/app ID as the existing cohort; pointing to empty state can
forget prior attempts. Metadata checks cannot prove that equal component names
refer to the same backend or that it has been configured as the actor store.
Verify component resources, endpoints, credentials and replication directly.
Do not use per-pod SQLite, local files or independent Redis instances. Application
code uses only Dapr actor transactions; there is no SQL claiming or direct worker
state write. All callbacks must come through the authenticated local sidecar.

Enable Dapr app-health checks explicitly on the private callback `/healthz` and
configure the app API token. Pin and qualify Dapr 1.19.x plus the chosen component,
Scheduler and Placement versions. Consult the [Dapr metadata API](https://docs.dapr.io/reference/api/metadata_api/)
and [actor API](https://docs.dapr.io/reference/api/actors_api/) when provisioning.

Every five seconds each replica checks metadata for the expected app ID, the named
state component's ACTOR/ETAG/transaction capabilities, event pubsub, connected
Scheduler and `/healthz` app probes. It then invokes Activate on an inventory actor
to test placement, state reads and reminder repair. A successful check expires
after 15 seconds. Failures invalidate it immediately. This is sampled availability,
not proof that every actor, database write or pubsub publication will succeed.
Existing inventory reminder repair still covers the cohort separately.

In replica mode `/readyz` requires this check plus MQTT readiness. Dispatch and
new actor callbacks other than Activate are blocked without a fresh check.
Activate stays available to avoid a bootstrap cycle. Dapr's private app-health
endpoint tracks MQTT connectivity and drain state, independently of metadata,
so it can rebalance away from a disconnected host. Durable API admission still
uses Dapr; it does not require a live device connection. A failed call may have
committed and must be retried with the same request identity and body.

On termination the app marks itself draining before HTTP shutdown. New management
calls and actor turns are rejected, private health becomes unhealthy, and the local
publication guard closes. Already-entered callbacks are allowed to finish under
Dapr's turn contract, then MQTT disconnects. Configure sufficient termination grace
and sidecar shutdown ordering to retain state access during draining. A forced kill
cannot drain. Do not assume Dapr's rebalance drain timeout is a fencing guarantee;
it can be clamped to the placement dissemination budget.

## Failure semantics and qualification gates

A local readiness check is **not a distributed lease or external-send fence**.
Dapr placement/turn serialization and the shared store are responsible for state
ownership. A stalled host can resume an already-entered external send; MQTT QoS and
network ambiguity also permit duplicates. A persisted attempting state is recovered
as unknown and is never deliberately replayed by the next host. Consumers deduplicate
outbox exports using the stable event identity. These rules do not prove zero stale
writers during a real control-plane partition. Do not claim that guarantee until
the pinned runtime and state-store combination passes the following drills.

| Drill | Required observation |
|---|---|
| Two replicas, repeated admissions and notifications | One durable receipt/queue entry; coherent state regardless of ingress member |
| Graceful drain and actor movement | New calls route to survivor; no cached actor state or replayed explicit command |
| Kill before/after attempting commit and external publish | Unsent queue can progress; persisted attempts become unknown, not resent |
| Isolate old host from Placement, Scheduler and state separately | Verify runtime rejects stale actor turns/writes; record any external send that can still occur |
| State-store or pubsub outage | No publication after failed begin commit; pending outbox retained, readiness ages out |
| MQTT subscriber loss | Subsequent messages reach survivor; acknowledge nonpersistent-session loss window |
| Restore/rollback | Quiesce old senders first; preserve schema 2 and outbox IDs; do not replay restored stale queues |

Automated coverage includes a real Mosquitto two-subscriber test (single delivery
and subsequent survivor delivery), simulated serialized placement across two actor
objects, partitioned backend access, interrupted-send recovery, readiness expiry,
misconfiguration, redirect rejection and drain guards. The simulated placement lock
is an explicit assumption; it does not test Dapr's implementation. No live Dapr or
replicated backend is provisioned by this change. Deployment approval remains gated
on the drills above, broker ACL verification and a recorded rollout/rollback plan.

Ingress remains bounded and serial per process, and MQTT sessions remain
nonpersistent. A message already delivered to a failed subscriber can be lost;
requested USP NotifyResp supports device retry, while transport PUBACK alone does
not establish durable actor acceptance. Do not infer ordering or lossless ingress
from the shared-subscription feature. Phase-4 receipt/capacity limits still apply.
