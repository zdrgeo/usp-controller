# Optional managed-policy guard for desired admission

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

## Receipt recovery and scope

The expected token is immutable request content. An identical retry recovers its
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
