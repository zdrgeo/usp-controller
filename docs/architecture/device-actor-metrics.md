# Device Actor operation metrics

The opt-in canary runtime exposes `GET /v2/metrics` on the management listener.
It requires the existing `Authorization: Bearer <USP_API_TOKEN>` credential. When
a twin site is configured, the response includes `X-USP-Site`; a supplied mismatch
returns 409. Missing/invalid credentials return 401. Legacy mode does not mount
this route or instrument its execution path.

This is a process-local registry, initialized with each canary Runtime. There is
no telemetry database, background actor sampling, new external dependency or
snapshot-schema change. Exposition follows the
[Prometheus text 0.0.4 format](https://prometheus.io/docs/instrumenting/exposition_formats/),
with counters and cumulative latency histogram buckets. Scrapes copy bounded
observations under a short lock, release it before formatting/writing, and never
invoke Dapr, load actor state, send device commands or change scheduling. The
endpoint sits outside the management drain gate and stays available while the
management listener remains open, even when dependencies are unavailable or
mutating management calls are draining. Normal HTTP server deadlines still apply.

## Measurements

`usp_device_operations_total` counts local calls when they return or unwind. The
`usp_device_operation_duration_seconds` histogram uses 0.001, 0.01, 0.1, 1, 5 and
15 second upper bounds, plus `+Inf`, sum and count. Both families use only the
fixed labels `boundary`, `operation` and `result`.

| Boundary | Operations | Results and interpretation |
|---|---|---|
| `actor` | Known DeviceActor callback methods; maintenance reminders count as `Tick` | `ok`, `invalid`, `conflict`, `capacity`, `not_found`, `disabled`, `cancelled`, `deadline`, `error` |
| `snapshot` | `load`, `save`, `reminder` | `ok` or `error` for the logical backend call; a failed save may have committed |
| `transport` | `publish` | `accepted`: broker acceptance reported without error; `unconfirmed`: local success without broker acknowledgement; `uncertain`: error or interrupted publication |
| `events` | `handoff` | `ok`: the configured publisher reported successful handoff; `uncertain`: error or interrupted handoff |

Known actor failures are classified from the domain error itself. In particular,
a conflict or capacity rejection is not counted as success merely because the
Dapr callback carries it in an HTTP-200 typed envelope. Invalid JSON/unknown
callback routes are classified by their callback status when the operation is
recognized. Callback timing includes local actor-lock wait, decoding, domain
work and response encoding; it excludes Dapr routing/placement wait before the
callback reaches this process. Calls rejected by outer readiness/authentication/
drain gates are outside callback measurements. Snapshot calls include their
existing internal endpoint-failover work, not one count per HTTP request.

These are nested operation boundaries, not unique command counts. A `Tick` can
return `ok` while paused or offline without sending anything; a published command
is not a completed USP operation. An error can follow a committed snapshot or a
delivered device action. Wrappers preserve the underlying context, return values,
errors and readiness, and perform exactly one underlying call. They add no retry
or availability decision. Existing no-replay and idempotent-receipt contracts
remain authoritative.

## Cardinality, privacy and operation

Operations and outcomes are allowlisted before allocating a series. The current
registry can hold at most 146 operation/result combinations (1460 Prometheus
series including histogram buckets/sum/count). Unknown method/outcome strings
are ignored rather than added as labels. Labels never contain device, controller,
tenant, request, message or operation IDs; error text, reasons, paths, parameter
values, event content, credentials and MQTT topics are not exported. Site and host
labels can be attached by the external scrape configuration rather than derived
from per-device input.

Counters/histograms reset on process restart. Each replica has independent values;
use external monitoring retention and reset-aware queries when aggregating across
hosts. An operation that crashes with the process before recording may be absent.
Series are created when observed; an absent series is not evidence that a device
has no backlog or is healthy. Scrapes never count themselves.

Use the existing per-device/fleet status endpoints for queue states, pending
outbox, paused state, unknown barriers and capacity snapshots. This exporter does
not supply authoritative gauges for those states, placement/readiness health,
broker consumer backlog, data retention, distributed tracing or real-device
convergence. Combine it with dependency monitoring and eventually refreshed fleet
status; never use metrics to claim commands or decide whether to replay an action.
No monitoring service or alert policy is provisioned by this change.

## Validation

On 2026-11-10, `go test -race ./...` and `go vet ./...` passed. Focused checks
validate bounded private labels, token/site checks, counter/histogram exposition,
a blocked scrape writer not blocking operation recording, preservation of context
and uncertain results with no additional underlying calls, and domain-error
classification through the real callback encoder. Existing canary tests also run
with all boundary wrappers wired into Runtime. The long infrastructure fault and
compaction suite was not rerun: storage protocols and dispatch semantics did not
change. Legacy remains the default.
