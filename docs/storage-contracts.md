# Command and Event persistence contracts

Status: proposed backend-neutral contracts, represented by `internal/storage`.
No backend has been selected or implemented. The running Controller still uses
its existing Journal and in-memory pending requests. Adopting these interfaces
will require a separate application integration change; adding this package does
not make the current Controller durable or safe for multiple replicas.

## Scope and boundaries

One Store is one transactional domain containing commands, event inbox entries,
delivery state and their deduplication indexes. Concrete candidates will include
an exclusive-process JSONL implementation, an embedded database, and a shared
external database. They must implement the same behavioral contract.

The application owns USP validation and result classification, routing policy,
retry backoff, transport authentication, and decisions about reconciliation.
The store owns durable state, unique constraints, atomic transitions, lease
validation, recovery and safe retention. No store method performs network delivery
or automatically issues a USP command. These are domain operations rather than a
generic CRUD interface or a SQL-shaped transaction API.

The interfaces are intentionally separate for consumers: command workers can
accept CommandStore; event workers can accept EventStore. A concrete Store must
coordinate both. IngestEvent's optional completion observation is a deliberate
cross-store atomic operation, avoiding a crash between storing OperationComplete
and updating its command. Independently constructed Command/Event databases do
not satisfy this contract without an equivalent atomic coordination mechanism.

## Common guarantees

- Successful mutations are committed durably according to the backend's declared
  failure model before returning. Local durability means process/OS restart with
  intact storage; it does not mean surviving destruction of that storage.
- Required uniqueness and transitions are linearizable within a scope. Competing
  replicas cannot both win a claim, cancellation or dedup insertion.
- Validation errors, conflicts and stale leases change nothing. A timeout,
  cancellation or unavailable error during a write can have an ambiguous outcome.
  Callers read back or retry the SAME stable operation identity, never manufacture
  a new command ID because a database call timed out.
- Methods take contexts for bounded execution, not rollback guarantees. Error
  wrapping preserves errors.Is against the exported sentinel errors.
- Empty identifiers, invalid states, zero/negative limits and lease durations,
  invalid scope combinations and malformed immutable metadata return ErrInvalid.
  Adapter-specific maximum batch/page sizes must be documented. Empty claims
  return an empty slice, not ErrNotFound.
- The store supplies UTC creation/receipt/update times and lease expiry using its
  authoritative clock. Callers specify deadlines, minimum retention and retry
  durations, never a trusted "current time". Shared adapters must use a common
  time authority and document clock-jump handling. Restarting a local backend
  invalidates recovered leases using persistent token generations.
- Lease tokens must never be reused. Owner is unique to a worker incarnation.
  Renewal verifies owner, token and unexpired store time. Expired workers cannot
  update work even if no replacement has claimed it yet. ExpiresAt in a handle
  is informational, not an alternative source of authority.
- Reads return detached data, including byte slices. Adapters and callers must
  not mutate stored payloads via shared Go memory. All public methods must be
  safe for concurrent goroutines; shared-process guarantees require the declared
  SharedProcesses capability as well.
- Revisions start at 1 and increase on changes to the resource. List methods use
  bounded keyset pagination ordered by immutable creation time and ID (delivery
  lists use parent-event creation time and delivery key). Cursors are bound to
  scope/filter; filter changes invalidate them. Listing is not a snapshot: state
  filters can change between pages. Claims, not listings, assign work.
- Scope is the stable logical Controller/deployment boundary, not a replica ID.
  Scope is a data isolation key, not authorization: the service must authenticate
  and authorize access before invoking a store.

## Commands

NewCommand contains a caller-supplied Command.Key, protocol identity,
canonical request bytes, an absolute deadline and minimum retention. It contains
no broker credentials or endpoint URL; transport routing is resolved separately.
The stored request must be sufficient to reproduce the intended USP message,
including message ID, without minting a different operation after restart.

SubmitCommand atomically inserts the command and unique indexes for:

- (scope, command ID)
- (scope, Agent ID, Controller ID, USP message ID)
- (scope, Agent ID, Controller ID, nonempty OperationKey), for Operate commands

FindCommandByMessage and FindCommandByOperation let any receiving replica resolve
responses/completions without the original caller or an in-memory map. Operation
keys cannot be reused within their retained correlation lifetime.

Command.Key (scope, command ID) is the submission idempotency identifier; there
is no separate caller-idempotency field or lookup. The caller allocates it before
the first submission and reuses it for every retry, including after a timeout or
lost response. GetCommand reads back the result using that same key. A new
intended execution requires a new key, even when its request content is identical.

For the same Command.Key, all immutable NewCommand fields must agree on a repeated
submission, including MessageID, OperationKey, request bytes, deadline and retention;
otherwise ErrConflict. A successful duplicate returns the existing command and
Created=false, including its latest state, without requeuing or publishing it.
A different Command.Key that collides with a retained protocol correlation index
also returns ErrConflict; correlation indexes are not alternative submission keys.
Idempotency lasts until safe retention removes the key. This guarantees submission
deduplication, not exactly-once execution on the Agent.

| State/action | Meaning and permitted next steps |
|---|---|
| queued | Not yet publish-committed; claim, cancel or expire |
| claim queued | State stays queued with an exclusive lease |
| BeginPublish | queued -> publishing, committed BEFORE transport invocation |
| MarkPublished | publishing -> awaiting_response; broker acceptance only |
| response evidence | publishing/awaiting_response -> succeeded, failed or awaiting_completion |
| completion evidence | publishing/awaiting_response/awaiting_completion -> succeeded or failed |
| MarkUncertain | publishing -> unknown after an ambiguous transport failure |
| lost publish lease | publishing -> unknown; no automatic requeue |
| deadline before publish | queued -> expired; invalidate any lease |
| deadline after publish | publishing/awaiting_response/awaiting_completion -> unknown |
| late validated evidence | unknown -> succeeded, failed or awaiting_completion |
| cancel | queued -> cancelled via revision check, invalidating the lease |

Deadline is the whole command lifecycle, including asynchronous completion.
Queued commands past their deadline cannot be claimed or published, even before
RecoverCommands runs. RecoverCommands performs bounded maintenance and never
publishes. Unexpired pending commands with expired pre-publish leases can be
reclaimed with a new token. Once BeginPublish committed, recovery is conservative
because a crash before and a crash after sending are indistinguishable.

BeginPublish and MarkPublished accept only a live matching lease. Repeating the
same already-applied transition with the same still-live token is idempotent.
If another transition already advanced the state, return ErrConflict and reread;
never regress it. MarkPublished clears publish ownership on success. A broker
PUBACK does not prove device execution and must never mark a command succeeded.

CancelCommand atomically compares revision and requires queued state. It can beat
a claimed worker before BeginPublish; that worker must then fail its transition.
It cannot cancel device execution after publish. Expired/cancelled commands
cannot be revived by late responses: reject those observations as ErrConflict.

### Observations and asynchronous commands

Any replica may persist a validated response; receiving it is not conditional on
owning the publish lease. RecordObservation correlates Agent/Controller identity,
command key and message identity, and deduplicates by (command key, observation
ID). Repeated identical evidence returns Created=false; different contents under
the same observation ID conflict. The application validates protobuf header/body,
USP errors and partial results before choosing NextState. The store rejects
NextState values outside succeeded, failed or awaiting_completion.

Response observations must carry the command's original MessageID. Completion
observations require an Operate command and a nonempty matching OperationKey;
their MessageID is the Notify ID. IngestEvent additionally requires completion
scope, identities, MessageID and Message payload to agree with the ingested event.
A completion must choose succeeded or failed. Reconciliation must identify an
authorized Actor and Reason, and may resolve only unknown to succeeded/failed.
Authorization of that actor belongs to the application, not a storage string.

A response can arrive before MarkPublished; the persisted observation takes
precedence. OperationComplete can arrive before OperateResp: it may settle the
command, and the later response is retained as evidence without regressing state.
Once succeeded/failed, validated new evidence is retained for inspection but does
not silently reverse the outcome. StateChanged=false reports this explicitly.
Observation insertion and any command state transition are one atomic commit.
Queued commands cannot acquire result observations before BeginPublish.

There is intentionally no automatic "retry unknown" operation. Retrying a
side-effecting command requires application reconciliation and an explicit new
command identity where appropriate. No API here promises exactly-once execution
at an ONU or prevents MQTT protocol-level retransmissions.

## Event inbox and delivery queues

IngestEvent commits the following together:

1. Event envelope and canonical Notify message.
2. Unique event-ID and protocol deduplication indexes.
3. One pending delivery per distinct requested logical destination.
4. Optional command completion observation/state transition.

Only that successful commit (or confirmed identical prior ingestion) permits
NotifyResp. MQTT PUBACK and USP NotifyResp remain distinct boundaries.

Event deduplication uses (scope, Agent ID, Controller ID, Notify message ID).
A duplicate must match SubscriptionID and Message (including protocol version,
message type and canonical bytes). Conflicting content is ErrConflict. The first
stored local event ID, ReceivedAt, routing destinations and ExpireAt win;
repeated ingestion cannot reset delivery state or add routes under a new policy.
An existing local event ID bound to another protocol identity is ErrConflict.
This lets a retry acknowledge the same event even when route configuration has
changed. Route changes for historical events would need a separate future API.

An optional completion on repeated ingestion is still applied/deduplicated in
the transaction. This permits an orphan event initially stored without a command
link to be associated later. If completion matching fails, the ENTIRE ingestion
fails. The caller may explicitly retry without Completion, preserving the event
for reconciliation rather than withholding notification acceptance forever.
There is no implicit matching of an arbitrary command based only on command_key.

Logical destinations include local consumers as well as cloud exporters. They
are versioned stable names, not mutable URLs. A delivery belongs to
(event ID, destination); acknowledging one destination does not acknowledge any
other. Events with no destinations are retained as inbox-only records.

| Delivery action | Atomic effect |
|---|---|
| claim eligible pending | leased, Attempts incremented, fresh token assigned |
| reclaim expired lease | new owner/token, Attempts incremented |
| renew | extend only a still-live matching lease |
| finish acknowledged | persist downstream receipt; clear lease |
| finish pending | persist failure; AvailableAt = store time + RetryAfter |
| finish dead | persist failure; requires explicit later intervention |
| requeue dead | revision-checked transition to pending; preserve attempts and reason |

The same FinishDelivery result can be retried with its original token after it
committed, provided no later claim/transition occurred; store the last completed
token/result to distinguish a retry from a stale worker. Changed results or an
older token after reclamation must fail. A first FinishDelivery call after lease
expiry returns ErrLeaseLost even if no replacement has claimed the item.

Workers acknowledge delivery only after durable downstream acceptance. If a
worker crashes after Azure accepts but before FinishDelivery commits, delivery
will repeat. Send a stable (scope, event ID, destination) idempotency key to the
receiver. This is at-least-once delivery, not exactly-once execution. Lease
expiry cannot stop a stalled worker's external network request.

The application computes bounded retries/backoff and decides when to mark dead.
Dead delivery is not successful delivery; it remains retained until requeued and
successfully processed. Destructive operator discard is deliberately outside
this initial API. No backend may silently evict unfinished data under pressure.

## Retention and cleanup

Prune belongs to the aggregate Store because commands and events can reference
each other. Before is capped by store time; Limit bounds total deleted logical
records. Minimum retention must be elapsed, and all dependency conditions must
hold. ExpireAt is never used as an unconditional database TTL.

- A command must be succeeded, failed, expired or cancelled, with no live lease.
  Unknown and in-progress commands are never automatically pruned.
- An event must have every delivery acknowledged; pending, leased and dead
  deliveries block deletion. Inbox-only events may expire after minimum retention.
- Retained events pin commands linked by completion evidence. Command observations
  are stored within the command and do not pin an already-processed source event.
- Deleting an eligible event removes its deliveries and indexes atomically;
  deleting a command removes observations and indexes atomically.
- Deleting dedup identities permits a later duplicate to appear new. Retention
  policy must exceed the agreed source/caller retry horizon. If a longer tombstone
  horizon is required, the adapter must retain those identities accordingly.

## Replica safety and backend candidates

Capabilities report actual configured behavior, not a product name. All adapters
must provide atomic ingestion, unique constraints and fencing even in local
mode. ExclusiveProcess adapters reject a second process opening the same store.
JSONL needs transaction frames, sync-before-success, crash recovery, exclusive
file ownership, and atomic compaction; today's append-only Journal is NOT a
conforming adapter. Embedded stores likewise must enforce the declared opening
and filesystem constraints.

SharedProcesses requires atomic claims and compare-and-set across independent
processes. High reliability additionally needs a declared replicated-commit
failure model, independently tested. Shared storage is not synonymous with
synchronous replication; a backend which can lose acknowledged work during its
configured failover must not advertise survival of that failure.

Storage leases fence STORE mutations, not ONU operations. Two controllers can
still send if a stalled process resumes after losing its lease. Multi-replica
operation additionally needs a device/partition ownership mechanism, transport
ownership enforcement, MQTT identity/subscription strategy, and controlled
failover. An SQL row lock, Redis lock or lease token alone cannot provide these.
These interfaces do not authorize increasing the current Deployment replica count.

## Set-based implementation evaluation

Keep candidates until measurements eliminate them. Run one backend conformance
suite against every implementation before comparing performance:

| Area | Required scenarios |
|---|---|
| submission | same-key concurrent identical/conflicting retries; read-back by key after ambiguous commit; cross-key correlation conflicts; independent scopes |
| publish | crash before/after BeginPublish; cancellation race; no replay of unknown |
| responses | other-replica receipt; response before publish marker; late/duplicate evidence |
| async | completion before response; atomic event + completion failure/recovery |
| events | duplicate/conflicting Notify; stable routes across retry; no ack before durability |
| leases | expiry, renewal, old worker resume, persistent non-reused tokens across restart |
| deliveries | downstream accept before crash; duplicate acknowledgment; stale-token rejection |
| retention | incomplete/dead work protected; scopes isolated; retry horizon honored |
| storage faults | disk full, torn writes, unavailable database, process kill, configured failover |

Evaluate p95/p99 durable-ingest and claim latency, writes/second, disk growth and
compaction pauses, memory, startup recovery time, outage backlog capacity, backup
and restore, and operational work per site. Test workload skew and peak reconnect
traffic, not only average Agent count. Shared candidates must pass multiprocess
and configured node-failure tests; local candidates are not required to pretend
they support shared deployment.

Next work: review these semantics, create an executable adapter conformance suite,
implement candidate backends, then integrate one behind the Controller. Preserve
the current Journal until migration/export and rollback behavior are defined.
