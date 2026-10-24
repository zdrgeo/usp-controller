package storage

import (
	"context"
	"time"
)

type CommandState string

const (
	CommandQueued             CommandState = "queued"
	CommandPublishing         CommandState = "publishing"
	CommandAwaitingResponse   CommandState = "awaiting_response"
	CommandAwaitingCompletion CommandState = "awaiting_completion"
	CommandSucceeded          CommandState = "succeeded"
	CommandFailed             CommandState = "failed"
	CommandUnknown            CommandState = "unknown"
	CommandExpired            CommandState = "expired"
	CommandCancelled          CommandState = "cancelled"
)

// NewCommand is immutable after SubmitCommand. The caller supplies Key as the
// submission idempotency identifier. Key, MessageID, and all other fields are
// allocated before submission and reused unchanged on retries.
type NewCommand struct {
	Key          CommandKey
	AgentID      string
	ControllerID string
	MessageID    string // Unique within (Scope, AgentID, ControllerID).
	OperationKey string // Optional USP Operate command_key for completion matching.
	Request      Message
	Deadline     time.Time // Absolute UTC deadline, including asynchronous completion.
	ExpireAt     time.Time // Minimum deduplication/record retention, >= Deadline.
}

type Command struct {
	NewCommand
	State        CommandState
	Revision     uint64
	CreatedAt    time.Time
	UpdatedAt    time.Time
	Lease        *Lease
	PublishAt    *time.Time
	PublishedAt  *time.Time // Broker accepted publication, NOT device execution.
	Observations []CommandObservation
	Reason       string
}

type SubmitResult struct {
	Command Command
	Created bool
}
type ClaimedCommand struct {
	Command Command
	Handle  CommandLease
}

type ObservationKind string

const (
	ResponseObservation       ObservationKind = "response"
	CompletionObservation     ObservationKind = "completion"
	ReconciliationObservation ObservationKind = "reconciliation"
)

// CommandObservation is classified by the protocol/application layer. The store
// validates identity, idempotency, and allowed state transitions. An observation
// is immutable and retained even when its transition is stale (e.g. a late
// OperateResp following OperationComplete).
type CommandObservation struct {
	Actor        string // Required for reconciliation; identifies the authorized resolver.
	Command      CommandKey
	ID           string // Stable unique evidence ID within the command; reused on retry.
	Kind         ObservationKind
	AgentID      string
	ControllerID string
	MessageID    string // Response: original command ID; completion: Notify ID.
	OperationKey string // Must match the command for CompletionObservation.
	Message      Message
	NextState    CommandState // succeeded, failed, or awaiting_completion.
	Reason       string       // Classifier's explanation; may describe partial USP failure.
}

type ObservationResult struct {
	Command      Command
	Created      bool
	StateChanged bool
}

// Correlation locates a request from a response arriving on any replica.
type Correlation struct {
	Scope        Scope
	AgentID      string
	ControllerID string
	MessageID    string
}

type OperationCorrelation struct {
	Scope        Scope
	AgentID      string
	ControllerID string
	OperationKey string
}

type CommandQuery struct {
	Scope   Scope
	AgentID string         // Empty means any Agent within this scope.
	States  []CommandState // Empty means all states.
	Page    PageRequest
}
type CommandPage struct {
	Items      []Command
	NextCursor string
}

type CancelCommand struct {
	Key              CommandKey
	ExpectedRevision uint64
	Reason           string
}

type RecoverCommands struct {
	Scope Scope
	Limit int
}
type RecoveryResult struct {
	Released int // Expired pre-publish leases: work can be claimed again.
	Expired  int // Deadline reached before publish: definitely not sent.
	Unknown  int // Publish/response/completion became uncertain; never requeued.
}

type CommandStore interface {
	// SubmitCommand atomically creates a queued command and all unique indexes.
	// Same Command.Key and identical immutable fields => existing command in its
	// current state, Created=false. Changed immutable fields under the same key,
	// or protocol correlation collisions with another key, return ErrConflict.
	// No transport side effects.
	SubmitCommand(context.Context, NewCommand) (SubmitResult, error)
	GetCommand(context.Context, CommandKey) (Command, error)
	FindCommandByMessage(context.Context, Correlation) (Command, error)
	FindCommandByOperation(context.Context, OperationCorrelation) (Command, error)
	ListCommands(context.Context, CommandQuery) (CommandPage, error)
	ClaimCommands(context.Context, ClaimOptions) ([]ClaimedCommand, error)
	RenewCommandLease(context.Context, CommandLease, time.Duration) (CommandLease, error)
	// BeginPublish must commit BEFORE calling the transport. It fences the claim
	// and makes recovery conservative even if the process dies before publishing.
	BeginPublish(context.Context, CommandLease) (Command, error)
	// MarkPublished requires a live lease; it must never overwrite an observation
	// already received by another replica. Caller rereads on ErrConflict.
	MarkPublished(context.Context, CommandLease) (Command, error)
	// MarkUncertain records an ambiguous publish failure while publishing.
	// Requires the live publish lease and never requeues the command.
	MarkUncertain(context.Context, CommandLease, string) (Command, error)
	// RecordObservation accepts validated inbound results on ANY replica without
	// a publish lease. Unknown outcomes may resolve with later positive evidence.
	RecordObservation(context.Context, CommandObservation) (ObservationResult, error)
	// Cancel is CAS-based and only succeeds while queued, including leased queued
	// work. It atomically invalidates the claim so BeginPublish can no longer win.
	CancelCommand(context.Context, CancelCommand) (Command, error)
	// Bounded, repeatable recovery using store time. Never republishes work.
	RecoverCommands(context.Context, RecoverCommands) (RecoveryResult, error)
}
