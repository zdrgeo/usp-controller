# Pending Device Actor event inspection

An authenticated caller can inspect bounded outbox metadata:

```http
GET /v2/devices/{device}/events?limit=20
Authorization: Bearer <USP_API_TOKEN>
X-USP-Site: <configured-site>
```

The route uses the same explicit actor inventory, bearer and optional site check
as command inspection. It does not require managed-twin projection. Legacy remains
the default runtime; legacy mode does not mount the actor API. The actor contract
and execution-enabled callback expose the matching `ListEvents` read.

## Metadata and pagination

The result contains `events`, `total` pending events and an optional `next_cursor`.
Rows are FIFO by stored sequence and include only `epoch`, `sequence`, controller
`received_at` and a fixed notification `kind`:

- `event`
- `value_change`
- `object_creation`
- `object_deletion`
- `operation_complete`
- `on_board_request`

Notification bodies, parameter paths/values, message/subscription IDs, operation
keys, reasons and transport topics are omitted. Epoch/sequence are the stable
exported event identity, useful for investigating duplicate handoffs. Listing is
not evidence that an event was delivered to or processed by a consumer. Exported
prefixes disappear from this pending-only view; it is not a historical archive.
An empty outbox returns `events:[]` and `total:0`.

Supported query fields are `limit` (default 20, range 1–100) and `cursor`, each at
most once. Unknown/repeated fields and malformed bounds/cursors return 400. The
cursor is versioned, bounded to 2048 bytes and scoped to the full device identity
and unchanged pending-event metadata. Pass it verbatim; page size may change.

An appended event or exported prefix changes the view and returns 409 with
`event view changed; restart without a cursor`. Command/twin/control changes that
leave event metadata unchanged do not invalidate it. The fingerprint is metadata
equality, not a monotonic storage revision, payload snapshot, authorization token
or delivery acknowledgement. It does not establish continuity across restoration
of identical metadata. No pagination lease or server-side cursor state is created.

## Read boundary

Each call loads one bounded actor snapshot. It never exports events, prunes
receipts, repairs reminders, expires queued work, plans commands, saves state or
publishes MQTT. Backend errors remain ordinary read failures; clients are not
asked to replay a mutation ID. `Cache-Control: no-store` applies to handled
successes and failures. Pending notification receipts remain retained after export
under the existing bounded-capacity policy.

Snapshot schema 9, event envelopes, fencing and dispatch permissions are unchanged.
The metrics allowlist adds one fixed `ListEvents` operation without event/device
labels. No new component, queue-claim table or background worker is introduced.

Validation on 2026-11-15: the full race-enabled Go suite and `go vet` passed.
Focused checks cover read-only behavior with write/reminder failures enabled,
value-free metadata, FIFO paging, device/view cursor binding, unrelated-command
continuity, append/export invalidation, and the API/client/callback path with
access and input validation. Long infrastructure fault drills were not repeated
for this read-only feature.
