# Go USP edge Controller

A runnable first implementation of an edge USP Controller for one OLT management domain. It uses Go, MQTT 5, and the existing Broadband Forum USP 1.5 protobuf schemas in `proto`. It can run directly or as one container on K3s. Fleet/Arc integration is intentionally independent of the application.

**Status:** development implementation, not a certified or production-ready TR-369 Controller. Real ONU interoperability, failure recovery, and site capacity must be validated before deployment. The MQTT integration test uses a simulated Agent over a real broker.

## Implemented

- USP 1.5 NoSessionContext records over MQTT 5, QoS 0 or 1.
- Get, Set, Add, Delete, Operate, GetInstances, GetSupportedDM, and GetSupportedProtocol through a protobuf-JSON HTTP API.
- Automatic request IDs, matching by Agent and message ID, expected-response validation, bounded outstanding requests, deadlines, and cleanup.
- MQTT reconnect with randomized exponential backoff, resubscription, Connect records, content type, response topic, and message expiry.
- Notification journal flushed to disk before sending a requested USP NotifyResp.
- Per-Agent Controller inboxes to support authenticated broker ACL isolation.
- TLS server verification, optional custom CA and mutual TLS, optional MQTT username/password.
- API bearer authentication, health/readiness endpoints, and graceful shutdown.
- Non-root container, single-replica K3s manifests, persistent journal volume, and example broker ACLs.

## Local development

Requires Go 1.24+ and an MQTT 5 broker. Mosquitto is suitable for local testing.

From this directory, in separate terminals:

```sh
mosquitto -c deploy/mosquitto/lab.conf
```

```sh
cp config.example.json config.json
export USP_API_TOKEN="$(openssl rand -hex 32)"
go run ./cmd/usp-controller -config config.json
```

The lab broker listens only on `127.0.0.1:18883` and allows anonymous access. The example explicitly permits plaintext MQTT. For a deployed site, use `tls://`, disable `allow_insecure_mqtt`, configure authentication, and enforce broker ACLs.

Configure the real Agent with:

| Setting | Example |
|---|---|
| Agent Endpoint ID | `os::example:onu-001` |
| Authorized Controller Endpoint ID | `os::example:controller-edge-001` |
| MQTT version | 5 |
| Agent subscription and reply topic | `usp/agents/onu-001` |
| Controller destination for this Agent | `usp/controllers/edge-001/onu-001` |
| USP version for this implementation | 1.5 |

The Agent must authorize the Controller's role for the requested data-model operations. An HTTP token does not grant access inside an Agent. Subscription objects for notifications can be created with USP Add/Set using the Agent's supported data model; this implementation does not create them automatically.

The Controller rejects records from a different Endpoint ID than the ID assigned to the receiving inbox. **This depends on broker authentication and ACLs:** each Agent may publish only to its own Controller inbox and subscribe only to its own Agent topic. See `deploy/mosquitto/acl.example`. Do not allow devices to publish to other devices' inboxes or to the Controller's outbound topics. All configured topics must be distinct, exact names, without wildcards.

## Command API

`POST /v1/requests` requires `Authorization: Bearer $USP_API_TOKEN` (minimum 32 characters). The body contains `agent_id` and a USP `Request` encoded as protobuf JSON. The API constructs the USP header; callers cannot override the sender, destination, message ID, or broker topic.

```sh
curl --fail-with-body http://127.0.0.1:8080/v1/requests \
  -H "Authorization: Bearer $USP_API_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"agent_id":"os::example:onu-001","request":{"get":{"paramPaths":["Device.DeviceInfo."],"maxDepth":1}}}'
```

Other request examples:

```json
{"agent_id":"os::example:onu-001","request":{"getSupportedDm":{"objPaths":["Device."],"firstLevelOnly":true,"returnCommands":true,"returnParams":true}}}
```

```json
{"agent_id":"os::example:onu-001","request":{"set":{"allowPartial":false,"updateObjs":[{"objPath":"Device.DeviceInfo.","paramSettings":[{"param":"ProvisioningCode","value":"edge-001","required":true}]}]}}}
```

The response is the complete USP `Msg` in protobuf JSON. HTTP 200 means a valid matching USP response was received, **not that all requested changes succeeded**. Inspect the USP Error body and operation-specific result errors. For asynchronous Operate requests, subsequent OperationComplete notifications are stored in the journal.

| HTTP status | Meaning |
|---|---|
| 400 | Invalid envelope, protobuf JSON, or unsupported request operation |
| 401 | Missing/invalid token |
| 404 | Agent not provisioned locally |
| 429 | Outstanding request limit reached |
| 503 | Broker subscriptions or journal not ready |
| 504 | Response deadline exceeded; execution outcome may be unknown |
| 502 | Transport or processing error; execution outcome may be unknown |

There are no automatic application-level command retries. MQTT QoS retransmissions can still occur. A lost response, caller disconnection, process restart, or timeout does not prove a command failed to execute. Do not blindly retry Set/Add/Delete/Operate.

`GET /healthz` reports process liveness. `GET /readyz` requires established MQTT subscriptions and a healthy journal. Neither endpoint authenticates or reports device connectivity. The HTTP listener does not terminate TLS: keep it on loopback or a restricted cluster network; use a TLS-authenticated reverse proxy for remote access. The single API token currently grants access to all configured Agents.

## Configuration and storage

See `config.example.json` and the TLS-oriented `deploy/k3s/config.json`. Configuration is validated at startup and is not hot-reloaded. Secrets are supplied separately:

- `USP_API_TOKEN`: required.
- `USP_MQTT_USERNAME`, `USP_MQTT_PASSWORD`: optional broker credentials.
- `ca_file`, `cert_file`, `key_file`: file paths; client certificate and key must be supplied together.

The JSONL journal stores UTC timestamp, Agent ID, and the notification message. It may contain sensitive device values: restrict access and define retention. Duplicate notifications can produce duplicate entries; consumers should use `(agent_id, message.header.msgId)` as a deduplication key within their retention window.

The default journal limit is 128 MiB. Once a record would exceed the limit, it is rejected without a USP acknowledgment. There is no automatic upload, rotation, retention, or garbage collection yet. Stop the Controller, archive/export the journal, replace it with an empty file, then restart. Never truncate or rename the live journal behind the process. An incomplete trailing entry after a crash causes startup to fail for explicit recovery.

MQTT sessions are intentionally nonpersistent in this first version; there is no durable command outbox or durable MQTT client session. MQTT PUBACK is separate from USP NotifyResp: the MQTT library acknowledges delivery before the application journal completes. Notifications requiring USP acknowledgment can be retried by the Agent; notifications without that requirement can be lost during crashes or disconnections. This is not an exactly-once system.

## K3s deployment

1. Build and publish the image to your registry, or import it into the site's K3s containerd. Replace `usp-controller:dev` in `deploy/k3s/usp-controller.yaml` with the chosen image, preferably pinned by digest.

```sh
docker build -t usp-controller:dev .
```

2. Edit `deploy/k3s/config.json` for the site: Endpoint ID, unique MQTT client ID, broker address, Agent topics, and per-Agent inboxes. Provision the broker and its ACLs separately. This deployment does not install a broker.

3. Create the namespace and secrets using your actual certificate and token files:

```sh
kubectl apply -f deploy/k3s/usp-system.yaml
kubectl -n usp-system create secret generic usp-controller-api --from-file=token=/secure/path/api-token
kubectl -n usp-system create secret generic usp-controller-mqtt-tls \
  --from-file=ca.crt=/secure/path/ca.crt \
  --from-file=tls.crt=/secure/path/controller.crt \
  --from-file=tls.key=/secure/path/controller.key
kubectl apply -k deploy/k3s
kubectl -n usp-system rollout status deployment/usp-controller
```

The token file must contain the token without a trailing newline. ConfigMap names are content-hashed by Kustomize, triggering a rollout when config changes. Restart the deployment after rotating environment-based or file-based credentials; the application reads credentials at startup.

The manifest uses one replica and `Recreate` to avoid overlapping Controllers during ordinary rollouts. **Do not increase replicas or run two deployments with the same identities.** There is no distributed ownership/fencing. Do not force-delete a pod on an unreachable node and start a replacement until the previous process is fenced.

The `local-path` PVC is node-local K3s storage. It survives ordinary pod replacement but is not replicated and does not provide recovery from node/disk loss. Resource requests/limits are initial development settings, not capacity claims. Benchmark them against the busiest OLT site.

The Service is ClusterIP-only. Ingress is restricted to same-namespace pods labeled `usp-api-client: "true"` when the cluster enforces NetworkPolicy. Egress is not restricted by this example. For local administration:

```sh
kubectl -n usp-system port-forward service/usp-controller 8080:8080
```

Fleet or Azure Arc can later distribute these manifests and per-site configuration. Keep private keys and API tokens out of Git.

## Build and verification

```sh
make build
make test
make vet
USP_TEST_MQTT=mqtt://127.0.0.1:18883 make integration
kubectl kustomize deploy/k3s
```

Tests cover request correlation, spoofed inboxes/senders, invalid responses, timeout cleanup, notification persistence-before-ack, journal capacity/recovery, API authentication and validation, and real MQTT request/response plus notification acknowledgment. Integration tests are skipped unless `USP_TEST_MQTT` is set. Run them against an isolated test broker.

Generated protobuf Go files are committed so normal builds need no generator. To regenerate, install `protoc` and `protoc-gen-go v1.36.6`, place the latter on PATH, and run `make generate`. The source schemas and generated files retain Broadband Forum license notices. The placeholder Go module path `example.com/usp-controller` is local and can be renamed when a repository/module publication path is chosen.

## Next production milestones

1. Interoperability testing with actual ONU vendors and a pinned TR-369 conformance profile.
2. Durable command journal, caller-supplied command keys for idempotency, uncertain-outcome reconciliation, and desired-state revisions.
3. Durable notification export, deduplication/retention, backpressure metrics, and alerting.
4. Agent inventory/configuration updates, subscription lifecycle, and supported-data-model caching.
5. Controller ownership/fencing, local failover, and recovery from node loss.
6. Certificate enrollment/rotation, per-operator authorization, audit records, and fleet orchestration.
7. Load/reconnect/outage tests and operational dashboards before site rollout.

Currently unsupported: other USP versions, MQTT 3.1.1, STOMP/WebSocket MTPs, session-context/E2E encrypted records, segmentation/reassembly, dynamic discovery, USP Services registration, complete USP Record error/disconnect responses, and broker-assigned subscription/response topics. Such broker assignments fail startup rather than weakening the per-Agent inbox policy. Operation field semantics are ultimately validated by the Agent; this is not a full data-model validator.

References: [USP specification](https://usp.technology/specification/), [Paho Go MQTT 5 client](https://github.com/eclipse-paho/paho.golang), [K3s documentation](https://docs.k3s.io/).

USP 1.5 routing fields are accepted only when absent or equal to the direct sender/recipient IDs. Forwarding between USP endpoints is not implemented.

## Storage interface design

The proposed [Command and Event store contracts](docs/storage-contracts.md) and `internal/storage` interfaces define backend-independent durability, deduplication, command recovery, delivery leases, and multi-process capabilities. No backend is selected or integrated yet; the current Journal remains active.

The alternative [Device Actor architecture proposal](docs/architecture/device-actors.md)
defines per-device digital twins, command queues and event handling using Dapr
Actors, without shared SQL command claiming. It includes conceptual actor
interfaces, state keys, simpler delivery policies, and a phased migration from the
current runtime. It is a design proposal, not an implemented deployment mode.


## Device Actor migration progress

The default runtime remains the legacy Controller and JSONL journal. Phases 3 and 4 add
an opt-in, single-host Device Actor canary for an explicit device allowlist, with
an asynchronous command API, durable admission, bounded dispatch and response
correlation, notification ingestion, OperationComplete handling and a durable event
outbox. Concrete single-target Operate is enabled for the allowlist. Dapr and a
durable pubsub component are required only for that mode. Desired-state
reconciliation remains disabled.

See the [canary configuration, API and cutover guide](docs/architecture/device-actor-canary.md)
and [architecture proposal](docs/architecture/device-actors.md) before enabling it.


Phase 5 adds an experimental, opt-in [replica placement profile](docs/architecture/device-actor-replicas.md)
with MQTT shared subscriptions, actor-only inventory, dependency readiness checks
and shutdown admission guards. Legacy remains the default; the existing K3s
manifest remains single-replica. Live Dapr partition/failover qualification is pending.
