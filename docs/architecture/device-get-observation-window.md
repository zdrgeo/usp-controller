# Durable Get observation windows

New Device Actor Get attempts now pin an observation expiry in the same atomic
snapshot save as durable attempt start. Configuration changes after that begin
cannot extend or truncate that read's observation window. Legacy remains the
default and no dispatch permission is added.

The pinned expiry is attempt start plus the configured observation freshness.
Projection must process the response strictly before it; equality is stale. A
promoted observation keeps that exact expiry, rather than recomputing it using a
later host's configuration. Controller arrival time, latest-attempt ordering,
current managed policy, request scope, type checks and mutation/lifecycle cutoffs
still apply. The expiry is not an accepted command deadline, response outcome or
permission to retry; full command evidence can settle even when projection skips.

Command details and metadata listings expose the retained timestamp as optional
`observation_fresh_until`. Missing metadata is left absent; reads do not synthesize
the compatibility fallback or substitute the accepted command deadline.

All new Get begins record the window, including raw, scoped refresh, bootstrap and
verification Gets and attempts begun while Get projection is disabled. Projection
still requires its separate opt-in. Failed or ambiguous begin saves retain existing
non-replayable attempt semantics. New queued commands sample the configuration of
the host that begins the attempt; admission alone does not pin observation time.

## Migration and validation

Current writers use snapshot schema 12. Schemas 1–11 load and upgrade on the next
atomic save without inventing windows for historical attempts. Those attempts
retain the previous fallback: the current configured duration anchored to their
stored attempt start. Drain/resolve legacy reads before changing freshness if
that fallback is unsuitable. A migrated command can legitimately lack the field
even after a schema-12 save; new attempts always record it.

The optional `ObservationFreshUntil` command metadata requires schema 12, a Get
payload, attempt identity/sequence/time, and expiry strictly after attempt start.
It is checked before loads and saves. This is consistency validation in the
existing trusted snapshot, not independent attestation of historical policy.
Older binaries reject schema 12. Drain serving hosts and roll forward or use a
reviewed state migration; do not roll old binaries over upgraded state. All new
saves use schema 12, including devices without Get projection. The extra metadata
uses existing byte capacity; no separate backend operation or store is added.

Focused checks restart an actor with both longer and shorter freshness settings,
verify equality expiry, preserve full response evidence without replay, reject
invalid/non-Get/older-schema window metadata and confirm legacy migration does
not invent configuration history. Existing atomic begin, Get projection and
legacy regression checks remain applicable; infrastructure fault drills are not
repeated for this actor-local metadata change.
