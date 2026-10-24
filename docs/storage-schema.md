# SQL storage schema

Status: proposed reference schema for the [storage contracts](storage-contracts.md).
This document does not select a backend, install a migration, or change the running
Controller. The DDL below is executable SQLite SQL. Other engines must preserve
the same constraints and transaction semantics; portability notes follow the DDL.

## Representation

- Every domain primary/foreign key includes `scope`; command IDs are supplied by
  callers and are also the submission idempotency identifiers.
- Times are signed 64-bit UTC Unix nanoseconds, including `deadline`, `expire_at`,
  and lease expiry. Adapters reject out-of-range Go times and preserve this precision
  when comparing duplicate submissions. Store-assigned times use the adapter's
  authoritative clock; shared adapters need a common time authority.
- `publish_at` records the committed BeginPublish boundary; `published_at` records
  confirmed transport acceptance. Neither establishes Agent execution.
- `expire_at` is minimum retention, never an unconditional deletion TTL.
- Messages are canonical protobuf USP Msg bytes in BLOB columns, with version/type
  alongside them. Optional Go strings use empty strings, not SQL NULL. Optional
  timestamps and absent leases use NULL. Required identifiers must be nonempty.
- Revisions and attempt counters use positive/nonnegative signed 64-bit integers.
  Although Go uses uint64, this adapter rejects overflow rather than wrapping.
- Text identifiers require exact, case-sensitive comparison. SQLite's default
  BINARY collation is used here; select equivalent semantics in other engines.

## Tables and indexes

Run this block once against an empty database, through a versioned migration.
Enable foreign keys on **every connection**, outside an active transaction.
SQLite documents both this requirement and its support for
[foreign keys](https://www.sqlite.org/foreignkeys.html),
[CHECK constraints](https://www.sqlite.org/lang_createtable.html), and
[partial unique indexes](https://www.sqlite.org/partialindex.html).

```sql
PRAGMA foreign_keys = ON;
BEGIN;

-- Internal allocator, not a public domain object. Never prune or reset it.
CREATE TABLE storage_metadata (
    singleton INTEGER NOT NULL PRIMARY KEY CHECK (singleton = 1),
    schema_version INTEGER NOT NULL CHECK (schema_version > 0),
    lease_generation INTEGER NOT NULL CHECK (lease_generation >= 0)
);
INSERT INTO storage_metadata VALUES (1, 1, 0);

CREATE TABLE commands (
    scope TEXT NOT NULL CHECK (scope <> ''),
    command_id TEXT NOT NULL CHECK (command_id <> ''),
    agent_id TEXT NOT NULL CHECK (agent_id <> ''),
    controller_id TEXT NOT NULL CHECK (controller_id <> ''),
    message_id TEXT NOT NULL CHECK (message_id <> ''),
    operation_key TEXT NOT NULL DEFAULT '',
    protocol_version TEXT NOT NULL CHECK (protocol_version <> ''),
    message_type TEXT NOT NULL CHECK (message_type <> ''),
    payload BLOB NOT NULL,
    deadline INTEGER NOT NULL,
    expire_at INTEGER NOT NULL CHECK (expire_at >= deadline),
    state TEXT NOT NULL DEFAULT 'queued' CHECK (state IN (
        'queued', 'publishing', 'awaiting_response', 'awaiting_completion',
        'succeeded', 'failed', 'unknown', 'expired', 'cancelled')),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    lease_owner TEXT,
    lease_token TEXT,
    lease_expires_at INTEGER,
    publish_at INTEGER,
    published_at INTEGER,
    reason TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (scope, command_id),
    UNIQUE (scope, agent_id, controller_id, message_id),
    CHECK ((lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
        OR (lease_owner IS NOT NULL AND lease_owner <> ''
            AND lease_token IS NOT NULL AND lease_token <> ''
            AND lease_expires_at IS NOT NULL)),
    CHECK (lease_owner IS NULL OR state IN ('queued', 'publishing')),
    CHECK (state <> 'publishing' OR (lease_owner IS NOT NULL AND publish_at IS NOT NULL)),
    CHECK (state NOT IN ('queued', 'expired', 'cancelled') OR publish_at IS NULL),
    CHECK (published_at IS NULL OR publish_at IS NOT NULL)
);
CREATE UNIQUE INDEX commands_operation_uq
    ON commands (scope, agent_id, controller_id, operation_key)
    WHERE operation_key <> '';
CREATE INDEX commands_list_idx ON commands (scope, created_at, command_id);
CREATE INDEX commands_agent_list_idx ON commands (scope, agent_id, created_at, command_id);
CREATE INDEX commands_state_list_idx ON commands (scope, state, created_at, command_id);
CREATE INDEX commands_claim_idx ON commands (scope, created_at, command_id)
    WHERE state = 'queued';
CREATE INDEX commands_lease_recovery_idx ON commands (scope, lease_expires_at, command_id)
    WHERE lease_expires_at IS NOT NULL;
CREATE INDEX commands_deadline_idx ON commands (scope, deadline, command_id)
    WHERE state IN ('queued', 'publishing', 'awaiting_response', 'awaiting_completion');
CREATE INDEX commands_prune_idx ON commands (scope, expire_at, command_id)
    WHERE state IN ('succeeded', 'failed', 'expired', 'cancelled');

CREATE TABLE command_observations (
    scope TEXT NOT NULL,
    command_id TEXT NOT NULL,
    observation_id TEXT NOT NULL CHECK (observation_id <> ''),
    kind TEXT NOT NULL CHECK (kind IN ('response', 'completion', 'reconciliation')),
    actor TEXT NOT NULL DEFAULT '',
    agent_id TEXT NOT NULL CHECK (agent_id <> ''),
    controller_id TEXT NOT NULL CHECK (controller_id <> ''),
    message_id TEXT NOT NULL DEFAULT '',
    operation_key TEXT NOT NULL DEFAULT '',
    protocol_version TEXT NOT NULL,
    message_type TEXT NOT NULL,
    payload BLOB NOT NULL,
    next_state TEXT NOT NULL CHECK (next_state IN ('succeeded', 'failed', 'awaiting_completion')),
    reason TEXT NOT NULL DEFAULT '',
    recorded_at INTEGER NOT NULL,
    PRIMARY KEY (scope, command_id, observation_id),
    FOREIGN KEY (scope, command_id) REFERENCES commands (scope, command_id) ON DELETE CASCADE,
    CHECK (kind <> 'reconciliation' OR (actor <> '' AND reason <> '')),
    CHECK (kind = 'response' OR next_state IN ('succeeded', 'failed')),
    CHECK (kind <> 'completion' OR operation_key <> ''),
    CHECK (kind = 'reconciliation' OR message_id <> '')
);

CREATE TABLE events (
    scope TEXT NOT NULL CHECK (scope <> ''),
    event_id TEXT NOT NULL CHECK (event_id <> ''),
    agent_id TEXT NOT NULL CHECK (agent_id <> ''),
    controller_id TEXT NOT NULL CHECK (controller_id <> ''),
    message_id TEXT NOT NULL CHECK (message_id <> ''),
    subscription_id TEXT NOT NULL DEFAULT '',
    protocol_version TEXT NOT NULL CHECK (protocol_version <> ''),
    message_type TEXT NOT NULL CHECK (message_type <> ''),
    payload BLOB NOT NULL,
    expire_at INTEGER NOT NULL,
    received_at INTEGER NOT NULL,
    PRIMARY KEY (scope, event_id),
    UNIQUE (scope, agent_id, controller_id, message_id)
);
CREATE INDEX events_list_idx ON events (scope, received_at, event_id);
CREATE INDEX events_agent_list_idx ON events (scope, agent_id, received_at, event_id);
CREATE INDEX events_prune_idx ON events (scope, expire_at, event_id);

-- Retained events pin their linked command observations and hence the commands.
-- Deleting the event removes the link, but never removes the observation.
CREATE TABLE event_completions (
    scope TEXT NOT NULL,
    event_id TEXT NOT NULL,
    command_id TEXT NOT NULL,
    observation_id TEXT NOT NULL,
    PRIMARY KEY (scope, event_id, command_id, observation_id),
    FOREIGN KEY (scope, event_id) REFERENCES events (scope, event_id) ON DELETE CASCADE,
    FOREIGN KEY (scope, command_id, observation_id)
        REFERENCES command_observations (scope, command_id, observation_id) ON DELETE RESTRICT
);
CREATE INDEX event_completions_command_idx
    ON event_completions (scope, command_id, observation_id);

CREATE TABLE deliveries (
    scope TEXT NOT NULL,
    event_id TEXT NOT NULL,
    destination TEXT NOT NULL CHECK (destination <> ''),
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending', 'leased', 'acknowledged', 'dead')),
    revision INTEGER NOT NULL DEFAULT 1 CHECK (revision > 0),
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    lease_owner TEXT,
    lease_token TEXT,
    lease_expires_at INTEGER,
    last_error TEXT NOT NULL DEFAULT '',
    receipt TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (scope, event_id, destination),
    FOREIGN KEY (scope, event_id) REFERENCES events (scope, event_id) ON DELETE CASCADE,
    CHECK ((state <> 'leased' AND lease_owner IS NULL AND lease_token IS NULL
            AND lease_expires_at IS NULL)
        OR (state = 'leased' AND lease_owner IS NOT NULL AND lease_owner <> ''
            AND lease_token IS NOT NULL AND lease_token <> ''
            AND lease_expires_at IS NOT NULL AND attempts > 0))
);
CREATE INDEX deliveries_claim_idx ON deliveries (scope, destination, available_at, event_id)
    WHERE state = 'pending';
CREATE INDEX deliveries_reclaim_idx ON deliveries (scope, destination, lease_expires_at, event_id)
    WHERE state = 'leased';
CREATE INDEX deliveries_filter_idx ON deliveries (scope, destination, state, event_id);

-- One latest completed FinishDelivery result, retained after its lease is cleared.
-- Internal bookkeeping: compare the original input, including RetryAfter, not
-- the computed AvailableAt. Invalidate this row on any later transition.
CREATE TABLE delivery_finish_results (
    scope TEXT NOT NULL,
    event_id TEXT NOT NULL,
    destination TEXT NOT NULL,
    lease_owner TEXT NOT NULL CHECK (lease_owner <> ''),
    lease_token TEXT NOT NULL CHECK (lease_token <> ''),
    result_revision INTEGER NOT NULL CHECK (result_revision > 0),
    result_state TEXT NOT NULL CHECK (result_state IN ('pending', 'acknowledged', 'dead')),
    retry_after_ns INTEGER NOT NULL,
    error TEXT NOT NULL,
    receipt TEXT NOT NULL,
    PRIMARY KEY (scope, event_id, destination),
    FOREIGN KEY (scope, event_id, destination)
        REFERENCES deliveries (scope, event_id, destination) ON DELETE CASCADE,
    CHECK (result_state <> 'pending' OR retry_after_ns > 0)
);

-- Preserve every explicit requeue justification, even after subsequent attempts.
CREATE TABLE delivery_requeues (
    scope TEXT NOT NULL,
    event_id TEXT NOT NULL,
    destination TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    reason TEXT NOT NULL CHECK (reason <> ''),
    recorded_at INTEGER NOT NULL,
    PRIMARY KEY (scope, event_id, destination, revision),
    FOREIGN KEY (scope, event_id, destination)
        REFERENCES deliveries (scope, event_id, destination) ON DELETE CASCADE
);

COMMIT;
```

## Mapping and index use

`commands` maps NewCommand and Command; `command_observations` supplies the
Observations slice. `events` maps Event, with Destinations reconstructed from
all its delivery rows regardless of state. Treat destinations as a distinct set;
return a deterministic ordering. No delivery row is independently pruned while its
event remains, so original routing is preserved. Events may have zero deliveries.
`event_completions`, allocator metadata, finish results and requeue history are
internal persistence details, not new public interfaces.

Primary and unique indexes support identity/correlation lookups. Listing commands
and events uses `(created_at, command_id)` and `(received_at, event_id)` respectively,
after fixing scope. Delivery listing joins events and orders by
`(events.received_at, deliveries.event_id, deliveries.destination)`; use the event
listing index plus the delivery primary key. Destination/state filters may require
additional scanning/sorting. Claim indexes have different orderings because claims
are not pagination. Tune optional indexes using query plans and actual site load;
retain every uniqueness constraint. Never index full protobuf payloads.

## Transaction requirements beyond DDL

DDL enforces identity, referential integrity and basic row shapes. It cannot by
itself implement the state machine, byte-for-byte immutable comparison, authoritative
clock, lease fencing, safe pruning, authorization or durability policy.

- **SubmitCommand:** atomically insert or compare all immutable fields for the same
  caller key. Return the existing state on identical retries. A different key
  colliding with message/operation correlation is ErrConflict. Do not overwrite
  immutable data with an upsert or treat every uniqueness failure as success.
- **Claims and renewal:** lock/select eligible rows and update ownership atomically.
  Increment `storage_metadata.lease_generation` transactionally for each new claim;
  encode its committed value as the opaque lease token. Never reset this counter,
  even when domain records are pruned. Check signed overflow and fail closed.
  Do not expose a token until commit. Renewals keep the same token and verify
  owner/token/unexpired lease using store time. Local reopen invalidates prior
  leases before serving workers; restored snapshots require retiring old workers
  and rotating the token namespace before accepting new claims.
- **Publish:** BeginPublish sets `publishing` and `publish_at` before network I/O.
  MarkPublished sets `published_at` and `awaiting_response`, clearing the lease.
  Preserve an observation that already advanced the state. Recover pre-publish
  claims for retry, but resolve uncertain publishing to `unknown`, never queued.
- **Observations:** compare immutable evidence on duplicates, validate correlation
  against the parent command and apply permitted transitions in the same transaction.
  A completion link must reference a completion observation whose identities and
  message bytes agree with the event; SQL foreign keys alone do not check this.
- **IngestEvent:** commit the inbox row, all routes, optional observation/command
  transition and completion link together. Protocol duplicates retain the first
  local event ID, routes, receipt time and expiry. A later duplicate may add its
  validated completion link. Failed completion matching rolls back everything.
- **FinishDelivery:** persist the result row and delivery change atomically. A
  repeated identical result is accepted only while its `result_revision` still
  equals the current delivery revision. Compare owner/token and all result inputs.
  A first finish requires a live lease; a committed-result retry can succeed after
  expiry. Clear the saved result on a later claim/requeue/transition. Requeue records
  its reason at the new revision without resetting attempts.
- **Prune:** lock/recheck eligibility in the same transaction as deletion. ExpireAt
  and terminal state alone are insufficient: live leases and event links block
  command deletion; every event delivery must be acknowledged. Cascades clean up
  children only after this eligibility check. Dead deliveries and unknown commands
  remain. Count commands/events/deliveries against the requested logical-record limit,
  including cascaded deliveries; skip an event if its deletion exceeds the budget.
  Restrictive completion foreign keys protect command evidence while events remain.
- **Concurrency:** use consistent lock ordering and retry transaction conflicts.
  SQLite adapters can serialize writers with BEGIN IMMEDIATE; shared engines need
  their own locking/isolation strategy. No network I/O belongs inside a transaction.
  Increment revisions on mutations and check expected revisions for CAS operations.

The global allocator is a simple correctness baseline and a possible contention
point; concrete shared adapters may substitute a durable nonreusing token scheme.
Database restoration, replication failover and local restart must preserve the
same fencing guarantees. Storage fencing still cannot prevent a stale network send.

## SQL engine adaptations

This is a logical reference, not a claim of a universally portable SQL script.

| Concern | SQLite reference | Other SQL implementations |
|---|---|---|
| Binary messages | BLOB | Map to the engine's binary type, e.g. PostgreSQL BYTEA |
| Integers/times | INTEGER, 64-bit values | Use signed BIGINT for times, counters and revisions |
| Identifier comparison | Default BINARY collation | Configure exact case-sensitive identity comparison |
| Foreign keys | PRAGMA on every connection | Preserve all composite keys and delete actions |
| Partial indexes | WHERE predicate | Use equivalent filtered indexes or separate correlation tables |
| Transactions | Serialized writer strategy | Implement atomic claims with engine-specific locking/isolation |
| Store time | Adapter-controlled authority | Prefer one database time authority; preserve precision |
| Durability | Configure and verify separately | Configure commit/replication guarantees separately |

A PostgreSQL migration would remove PRAGMA, replace BLOB with BYTEA and widen
INTEGER columns to BIGINT; its claim queries, clocks, migration tooling and failure
behavior still require implementation and testing. Engines with bounded index-key
sizes need documented identifier limits; do not silently truncate identifiers.

DDL alone does not justify SharedProcesses or ReplicatedCommit capabilities.
Concrete adapters must pass the conformance scenarios in storage-contracts.md,
including crash, ambiguous commit, fencing and configured failover cases.
