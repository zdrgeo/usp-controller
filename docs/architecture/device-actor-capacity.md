# Device Actor completion capacity reserve

This optional deployment policy preserves bounded headroom for command evidence
without changing legacy mode, the snapshot backend, delivery guarantees or schema
5. It is configured inside an already enabled `actor_canary` object:

```json
{
  "completion_reserve": {
    "state_bytes": 65536,
    "notifications": 1
  }
}
```

Omission (or both zero values) preserves the previous hard-limit behavior. The
reserve requires actor canary mode; it does not enable actors itself. With current
runtime limits, `state_bytes` must be 0–1048575 and `notifications` 0–63. Negative
values or reserves leaving no ordinary capacity are rejected at startup. Direct
actor users can set `Options.CompletionReserve`; validation uses their configured
hard limits. All replicas must use the same policy. A mixed policy cohort cannot
promise protected headroom.

## Admission and evidence budgets

The hard limits remain one MiB per snapshot, 64 retained commands/pending events
and 1024 mutation receipts/inbox identities. These are separate counts, even where
an option provides the same numeric bound. The example leaves an ordinary byte
budget of 960 KiB, 63 pending-event slots and 1023 inbox identities. It does not
reserve command-record slots or mutation-receipt slots.

New explicit commands, desired mutations, trusted observation reports, generated
reconciliation intents and execution-resume decisions use the ordinary byte
budget. Ordinary notification acceptance also stops before the reserved inbox and
outbox slots. Existing identical receipts still return normally without needing a
new allocation. Rejected notifications are not durably accepted and receive no
NotifyResp; no silent dropping or receipt eviction is introduced.

A new OperationComplete can use hard capacity only after matching the stored
operation key, concrete command path and eligible command state, and only if that
attempt's unknown outcome has not already been acknowledged by an operator. This
lets current unresolved evidence retain priority over acknowledged historical
uncertainty. Unmatched completions, duplicate completion content under a new
message ID and acknowledged old attempts are ordinary events. They remain durable
when ordinary capacity is available and otherwise receive backpressure. A
retransmission with the same already accepted message ID is still idempotent even
when capacity is full.

Validated matching responses use the hard byte limit, preserving the existing
atomic result transition. A completion records command evidence, receipt and
outbox together; failure or uncertainty never causes an acknowledgement before
commit. Normal field/correlation checks apply before priority is selected. Both
hard bounds and backend failures still apply to all evidence.

Before recording a new `attempting` boundary, the actor checks the ordinary byte
budget. When notification slots are reserved, a new Operate also requires at least
one hard inbox and pending-event slot. If it cannot fit, it remains queued with no
message ID and no publication. Exporting an event can free an outbox slot; it does
not remove its retained inbox identity. A full retained inbox therefore prevents
new Operate dispatch even after export catches up. Queued work keeps its original
deadline and may expire while waiting.

Deadline/attempt recovery, queued cancellation, event removal, explicit unknown
acknowledgement and execution pause can use hard bytes so ordinary admission
pressure does not itself prevent maintenance. These operations can also consume
headroom; the reserve is a reduced admission threshold, not separately allocated
or exclusively leased memory. Pause/acknowledgement still need room in the hard
mutation-receipt budget. Resume continues to repair reminders and obey pause,
unknown-outcome, observation and reconciliation constraints. No command is retried
or deliberately republished by this policy.

## Operation and limits

`GetStatus` and eventually refreshed fleet rows expose
`reserved_completion_bytes` and `reserved_completion_notifications`, alongside the
existing hard limits and current state/receipt/event counts. Existing operation
metrics record reduced-budget rejections as `capacity`. Management API mutations
still return 429 on capacity rejection; command/notification correlation and
idempotency remain per actor.

The reserve is not an acceptance guarantee for arbitrary payloads. Serialized
protobuf bytes expand in JSON; a completion is retained both as evidence and an
outbox event. Responses, metadata, maintenance and operator decisions may use
headroom, and old acknowledged completions may need ordinary space. Size budgets
must account for the actual serialized snapshot. When hard bytes/slots are gone,
evidence can still be rejected and command outcomes can remain unknown. Even the
pre-attempt check cannot guarantee later acceptance under all historical or
concurrent operational conditions.

Records are still bounded and retained without pruning. A reserve delays ordinary
exhaustion and prevents a new asynchronous attempt when known slot capacity is
already absent; it does not replace retention, export draining, replay-safe
migration or measured rollout budgets. Existing state already above an ordinary
threshold remains readable, with cleanup/evidence paths available within hard
limits; new ordinary writes can be blocked until capacity is recovered. Changing
this policy does not rewrite actor state or authorize replay. Legacy remains the
default and current persisted execution controls remain unchanged.

## Validation

On 2026-11-10, `go test -race -timeout=2m ./...` and `go vet ./...` passed. Focused
checks cover ordinary notification saturation, matching completion reserve use,
failed commit/no settlement, duplicate handling, exclusion of unmatched/terminal/
acknowledged historical evidence from priority, outbox and retained-inbox gates
before publication, and response byte headroom while new begins are blocked.
Existing configuration/canary checks also exercise reserve bounds and actual
runtime wiring. Storage/transport protocols are unchanged; the long infrastructure
fault/compaction suite was not rerun.
