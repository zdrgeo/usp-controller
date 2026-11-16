# Explicit asynchronous managed-twin refresh

An authenticated caller can request one read of all or selected managed paths:

```http
POST /v2/devices/{device}/twin/refresh
Authorization: Bearer <USP_API_TOKEN>
X-USP-Site: <configured-site>
Content-Type: application/json

{"request_id":"refresh-001","deadline":"2026-11-15T12:00:00Z"}
```

To narrow the read, include an optional `paths` array, for example
`"paths":["Device.Test.Value"]`. Every selected path must be an exact configured
managed parameter; reviewed read-only paths are allowed. Unknown paths, object or
search selectors and duplicates are rejected before admission. Omitted, null or
empty arrays mean all current policy paths, preserving the existing default.

Use an actual future absolute deadline. The endpoint is mounted with the existing
twin API for explicitly allowlisted actor devices. The actor also requires an
execution-enabled transport, a managed-twin policy and `observe_get_responses`.
It returns disabled when those prerequisites are absent. Legacy remains the
default; no feature is enabled in example deployment configuration.

The response is 202 with the ordinary command receipt and
`Location: /v2/devices/{device}/commands/{request_id}`. It means durable admission,
not publication or successful refresh. Site, bearer and inventory checks precede
actor invocation. The actor interface and private callback expose the matching
`RefreshTwin` method with `request_id`, `deadline` and optional `paths`.

## Queue and evidence

The actor copies and sorts the selected paths (or all current policy paths)
into one concrete Get (`max_depth=1`). Caller array order is not changed. The
selection cannot expand the managed policy or supply arbitrary USP payloads.
The read delegates to ordinary command admission:
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
new queued command and no resend after an attempt. Identity binds the effective
sorted Get scope: reordered selections and an all-path shorthand resolving to
that same set recover the same receipt. Changed deadlines, a changed path set,
reuse for a different command or a policy change that alters the built request
produce a conflict. Selected paths must still belong to the current policy;
explicit subsets avoid accidental expansion when new policy paths are added.
Inspect the original command before choosing a new ID. A new explicit
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

Refresh also accepts optional `expected_policy_fingerprint` from actor status.
For new admission it must match the current managed policy/freshness token. It is
part of immutable command identity; omitting it preserves historical unguarded
receipt encoding. Scope normalization and current path checks still precede
command receipt lookup, so this does not recover historical scope after policy
withdrawal. See [policy guards](device-twin-policy-guard.md) for details. It does
not freeze policy for later dispatch or replace projection guards.

## Validation and compatibility

Refresh originally used snapshot schema 8; current writers use schema 11.
Optional path selection changes no snapshot or receipt format, backend fencing
or legacy behavior. Upgrade API and callback hosts together before sending path
selections; older callback hosts reject the new request field. Metrics allowlist
`RefreshTwin` as one fixed actor operation;
no per-device labels are added.

Validation on 2026-11-14: the race-enabled full Go suite and `go vet` passed. One
end-to-end API/client/callback check covers access control before invocation,
deterministic scoped queueing, idempotent/conflicting admission, no publication on
admission, callback timestamp provenance, managed projection and duplicate stability.
Focused actor checks cover shadow/absent-policy/projection-disabled rejection.
Long infrastructure fault tests were not repeated for this admission/callback change.

Scoped-refresh validation on 2026-11-16 extends the existing API/client/callback
check with a selected subset and confirms that unrequested managed values in a
response are not projected. Focused actor checks cover sorted identity recovery
after an ambiguous commit and deadline expiry, changed-set conflicts, input
ownership and policy/duplicate rejection without saves or dispatch. The full
race-enabled Go suite and `go vet` pass; no infrastructure fault drills are needed
for this request-scope change.
