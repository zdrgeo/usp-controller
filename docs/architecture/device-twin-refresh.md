# Explicit asynchronous managed-twin refresh

An authenticated caller can request one read of the configured managed paths:

```http
POST /v2/devices/{device}/twin/refresh
Authorization: Bearer <USP_API_TOKEN>
X-USP-Site: <configured-site>
Content-Type: application/json

{"request_id":"refresh-001","deadline":"2026-11-15T12:00:00Z"}
```

Use an actual future absolute deadline. The endpoint is mounted with the existing
twin API for explicitly allowlisted actor devices. The actor also requires an
execution-enabled transport, a managed-twin policy and `observe_get_responses`.
It returns disabled when those prerequisites are absent. Legacy remains the
default; no feature is enabled in example deployment configuration.

The response is 202 with the ordinary command receipt and
`Location: /v2/devices/{device}/commands/{request_id}`. It means durable admission,
not publication or successful refresh. Site, bearer and inventory checks precede
actor invocation. The actor interface and private callback expose the matching
`RefreshTwin` method with `request_id` and `deadline`.

## Queue and evidence

The actor sorts all current policy paths, including reviewed read-only paths,
into one concrete Get (`max_depth=1`). Callers cannot add arbitrary paths or USP
payloads through this endpoint. The read delegates to ordinary command admission:
existing encoding/size, capacity, idempotency, deadline and reminder rules apply.
The callback does not publish immediately. Readiness, pause, per-device unknowns,
FIFO order and host concurrency/rate budgets continue to gate dispatch.

This is an explicit command, not a new generated reconciliation command kind.
Its accepted scope is immutable. A later policy change does not rewrite or cancel
that ordinary queued Get; projection at response time still filters values through
the current policy. Review/drain queued commands when withdrawing a read scope,
just as for explicit raw Gets. Generated bootstrap/verification reads retain their
own stricter policy revalidation rules.

Retry the same ID and deadline to recover a lost admission response. There is no
new queued command and no resend after an attempt. Changed deadlines, reuse for a
different command or a policy change that alters the built request produce a
conflict. Inspect the original command before choosing a new ID. A new explicit
refresh consumes another retained command/receipt and can hit bounded capacity;
there is no pruning, periodic polling or automatic retry of uncertain reads.

The response callback now assigns a controller receive timestamp, as notification
callbacks already do. Device-provided time is not accepted as observation arrival
time. Correlated results use the existing [Get projection](device-get-observations.md)
and [mutation barrier](device-observation-barrier.md), including exact scope/type
checks, latest-attempt ordering, conservative timestamp ties and freshness anchored
to attempt start. Partial/unusable results may leave some paths stale. Full USP
evidence stays on the command; duplicate callbacks do not refresh freshness.

Refresh changes neither desired state nor reconciliation policy. If reconciliation
is already enabled, newly usable observations can make an existing desired
revision eligible for its bounded managed Set on a later reminder. Refresh is not
a write acknowledgement, convergence guarantee or continuously maintained twin.

## Validation and compatibility

Snapshot schema 8, receipt format, backend fencing and legacy behavior are
unchanged. Metrics allowlist `RefreshTwin` as one additional fixed actor operation;
no per-device labels are added.

Validation on 2026-11-14: the race-enabled full Go suite and `go vet` passed. One
end-to-end API/client/callback check covers access control before invocation,
deterministic scoped queueing, idempotent/conflicting admission, no publication on
admission, callback timestamp provenance, managed projection and duplicate stability.
Focused actor checks cover shadow/absent-policy/projection-disabled rejection.
Long infrastructure fault tests were not repeated for this admission/callback change.
