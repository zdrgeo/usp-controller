# Optional managed-policy guards for actor admission

Actor status now exposes optional `twin_policy_fingerprint`. It is a canonical
SHA-256 digest of the managed-twin policy and effective observation freshness,
with a versioned canonical input. Policy map order is normalized. It includes
site, reviewed paths/types/write flags, reconciliation/read flags and command
timeout. It excludes desired/reported state, pause state, host capacity, transport
and host rate/concurrency budgets. With no managed policy the field is absent.
The digest contains no parameter values or raw path list; treat it as an opaque
consistency token, not authentication, ownership fencing or a permission grant.

An authenticated caller can read `GET /v2/devices/{device}/status`, then include
that token as optional `expected_policy_fingerprint` in its existing desired
PATCH body, alongside request ID, expected desired revision and desired changes.
The actor checks it for **new admission** after receipt lookup and before applying
current managed-path validation or modifying desired state. A well-formed mismatch
returns conflict (HTTP 409); malformed tokens return invalid (HTTP 400). Desired
revision compare-and-set, current path permissions and existing access checks
still apply. Omitting the field preserves existing admission behavior.

## Explicit commands and twin refresh

The same optional `expected_policy_fingerprint` is accepted by command POST and
twin-refresh POST. The public command DTO and private callback carry it to actor
admission. For new commands, the actor validates the token against current policy
after immutable receipt lookup and before creating queue intent. A matching token
does not restrict or expand the raw command's protocol scope: that endpoint remains
the existing operator interface, not managed-path authorization.

Command receipt identity includes the guard alongside deterministic protocol
payload and absolute deadline. Removing or changing a nonempty guard under the
same command ID conflicts. Unguarded historical payload/deadline fingerprints
remain unchanged. Identical command retries can recover their existing receipt
after policy changes or deadline expiry without sending or rewriting state.

Refresh forwards its guard with the normalized scoped Get. It still constructs
scope under current policy before ordinary command receipt lookup. Path withdrawal
can reject a selection, and all-path policy changes can alter the built Get and
conflict with an old receipt. The guard does not reconstruct historical refresh
scope or bypass projection prerequisites; inspect the retained command in those
cases. Equivalent reordered paths retain the existing normalized identity rules.

## Receipt recovery and scope

The expected token is immutable request content. An identical desired retry recovers its
original receipt even if current policy, freshness or desired revision has since
changed. It repairs the reminder but never reapplies old values. Changing or
removing the guard under the same request ID conflicts. Unguarded historical
requests retain their original fingerprint encoding: the added domain field is
omitted when empty. No stored receipt migration or schema change is required.

This protects the admission decision on the host processing that call. It does
not pin future reconciliation policy, prove all replicas share configuration or
freeze policy between admission and execution. Generated commands continue their
existing current-policy/observation rechecks. Drain/review serving hosts for policy
rollouts as before. A token may match more than one device with the same policy;
authenticated device routing and site checks remain the authority for scope.

Fleet status rows preserve and validate the optional digest without treating it as
proof of current execution ownership. Older hosts/caches omit it, which means
unavailable policy identity rather than a matching default. Upgrade API/callback
hosts before supplying guarded requests; older callbacks reject the new field.
Clients should not reconstruct the digest, and should fetch a new one when policy
or canonicalization versions change.

Snapshot schema 12, backend protocols, command delivery and legacy defaults are
unchanged. Validation covers stale/current admission through the authenticated
API and private callback, receipt recovery after lost commit replies and policy
withdrawal, historical request-fingerprint compatibility, immutable desired
revisions versus policy identity, and optional digest persistence in the fleet
cache. No infrastructure fault drills are repeated for this admission-only change.

Command/refresh extension checks also cover forwarding through the real canary
API/callback path, refresh receipt recovery after a configuration change, removal
of an immutable guard and historical command fingerprint compatibility. No
dispatch, retry, queued-scope or persisted Get-window behavior changes.
