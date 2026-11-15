# Device Actor command inspection

An authenticated per-device listing now complements the existing single-command
read and operator recovery controls:

```http
GET /v2/devices/{device}/commands?limit=20&state=unknown
Authorization: Bearer <USP_API_TOKEN>
X-USP-Site: <configured-site>
```

It uses the same explicit actor inventory, bearer credential and optional site
check as other Device APIs. The callback is exposed only on execution-enabled
actors. Legacy mode does not mount this route; there is no fallback to legacy SQL
queues. Each call loads one bounded actor snapshot through the existing backend.
There is no fleet fanout, shared command claim, new persistence or schema change.

## Query and response

Supported query fields are `limit`, `state` and `cursor`, each at most once.
Unknown fields, malformed query encoding, invalid states and out-of-range limits
return 400. The default page size is 20 and the maximum is 100. State is optional;
when supplied it must be one of the existing persisted command states. Empty
state means all retained commands. The list includes generated reconciliation
commands as well as explicitly submitted commands.

The JSON response includes:

- `commands`: metadata summaries, ordered lexically by request ID.
- `total`: retained commands matching the filter, not the page size.
- `next_cursor`: an opaque continuation token when another page exists.
- `execution_paused` and `execution_revision`: control state from the same load.

Each summary has `request_id`, command `revision`, `state`, `deadline`, generated
`desired_revision` when applicable, one-based `queue_position` for queued work,
`blocking_unknown`, `unknown_acknowledged`, `has_response` and `has_completion`.
Queue positions refer to the persisted FIFO across all queued commands, not their
lexical page order or a filtered subqueue. Nonqueued entries omit queue position.
An empty result is `commands:[]` and `total:0`.

New attempts also expose `attempted_at` and `attempt_sequence` in summaries and
single-command reads. Migrated attempts omit these fields when provenance is
unknown. These fields participate in the command-view fingerprint. See
[Get observations](device-get-observations.md) for attempt timing semantics. Generated bootstrap reads expose
`observation_revision`, distinct from generated writes' `desired_revision`; see
[observation reads](device-observation-reads.md). Generated post-write reads expose
`verification_revision`; see [verification reads](device-verification-reads.md)
for schema 8 and rollout. These provenance fields also participate in the view
fingerprint.

Summaries omit USP request/result/completion payloads, protocol message IDs,
operation keys, free-form reasons and acknowledgement audit text. Request IDs are
included so an authorized operator can use the existing single-command endpoint
for evidence and audit, then supply its current revision to cancel or acknowledge.
A listing is not evidence that a device action succeeded or that a queued command
will be sent. Reads do not expire work, repair reminders, schedule reconciliation,
write state or publish anything. A deadline can therefore be past while the
persisted state is still queued until maintenance handles it. Successful responses
use `Cache-Control: no-store`.

## Continuation consistency

Pass `next_cursor` back verbatim, keeping the same state filter. Page size may
change. Cursors are versioned, bounded to 2048 bytes and scoped to the device and
current command/control view. The actor fingerprints all command summaries and
the visible execution control, including commands outside the chosen filter. A
state transition, new command, queue-position change, acknowledgement, new evidence
presence or execution decision invalidates a previous cursor. The API returns 409
with `command view changed; restart without a cursor`. It does not describe the
read as an ambiguous write or ask the caller to retry a mutation ID.

Twin observations and notification/export work that leave command metadata and
execution control unchanged do not invalidate the cursor. A matching unchanged
view permits continuation without holding a database transaction or storing a
server-side pagination session. There is no historical snapshot lease: concurrent
command/control changes require restarting from a fresh first page. Under frequent
changes, use a larger page (the current default actor command cap is 64, below the
100-item page limit) rather than treating partial results as a stable inventory.

The view fingerprint is an equality check, not a monotonic storage revision,
authorization credential or write precondition. It does not establish continuity
across an administrative restore of identical metadata. Opaque cursors must not
be used to authorize dispatch, retry an operation or bypass expected-revision
checks. All mutating endpoints retain their own revision/idempotency contracts.
Malformed or different-device/filter cursors return 400; a validly shaped cursor
whose view changed returns 409. Existing backend errors still propagate as read
failures. No automatic read or mutation retry is added.

## Validation and limits

On 2026-11-10, `go test -race -timeout=2m ./...` and `go vet ./...` passed. Focused
checks verify no expiry, scheduling, save or publication on listing; payload/reason
exclusion; lexical order versus FIFO position; cross-device/filter rejection;
command/control change conflicts; telemetry-only continuity; and the complete API
query/callback path with non-mutating stale-read messages. The long infrastructure
fault/compaction suite was not rerun because the storage/dispatch protocols did not
change.

The API inspects only the bounded retained command map already loaded by each
actor turn. It does not provide a searchable historical archive, reclaim capacity,
prune receipts, guarantee convergence, or replace the eventually consistent fleet
projection. Snapshot schema 5, delivery guarantees and the default legacy runtime
are unchanged.


New definitive mutations also expose optional `mutation_outcome_at` in summaries
and individual reads. This controller processing timestamp participates in the
view fingerprint; missing values on historical evidence are not synthesized. See
[mutation outcomes](device-mutation-outcomes.md) for schema 9 and rollout limits.


Actor API read failures now consistently omit uncertain-admission/replay guidance.
The restart-without-cursor instruction applies only to command-list view conflicts,
not single-command, execution, twin/diff or status reads. Shared access handling
sets `Cache-Control: no-store` before handled successes and failures. This does
not change mutation idempotency, dispatch or snapshot schema 9.
