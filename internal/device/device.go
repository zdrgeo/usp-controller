// Package device defines transport-neutral Device contracts for the actor migration.
// These contracts do not enable actors, durable admission, or retries in the legacy runtime.
package device

import (
	"context"
	"time"

	"example.com/usp-controller/internal/pb/usp"
)

// DeviceIdentity is a tuple, not a concatenated actor ID. Endpoint IDs retain USP semantics.
type DeviceIdentity struct {
	Tenant       string
	ControllerID string
	AgentID      string
}

type RequestID string

// DeviceRequester is the existing synchronous boundary. Its result is a USP
// response, not a durable command receipt. Timeout does not establish nonexecution.
type DeviceRequester interface {
	Request(context.Context, string, *usp.Request) (*usp.Msg, error)
}

// DeviceActor describes the proposed domain boundary, not Dapr SDK handlers.
// Identity is supplied by authenticated routing. Mutable DTOs must be copied on
// admission; callers must not mutate protobufs/maps concurrently with a call.
// The opt-in implementation lives in internal/deviceactor; legacy mode does
// not register or invoke it.
type DeviceActor interface {
	GetTwin(context.Context) (DeviceTwin, error)
	SetDesired(context.Context, SetDesiredRequest) (DesiredReceipt, error)
	EnqueueCommand(context.Context, CommandRequest) (CommandReceipt, error)
	GetCommand(context.Context, RequestID) (DeviceCommand, error)
	CancelCommand(context.Context, CancelCommandRequest) (DeviceCommand, error)
	HandleResponse(context.Context, DeviceResponse) error
	HandleNotify(context.Context, DeviceNotification) (NotifyReceipt, error)
	Tick(context.Context) error
}

// ParameterValue preserves USP's textual value and type information when available.
type ParameterValue struct{ Value, Type string }
type Observation struct {
	Value      ParameterValue
	ReceivedAt time.Time
	SourceTime *time.Time
	Source     string
	FreshUntil time.Time
}
type DeviceTwin struct {
	Identity                          DeviceIdentity
	DesiredRevision, ReportedRevision uint64
	Desired                           map[string]ParameterValue
	Reported                          map[string]Observation
	LastContact                       time.Time
	Convergence                       string
	Pending, Unknown                  []RequestID
}

// Same request ID and immutable content returns the original receipt. A changed
// payload conflicts. ExpectedRevision applies only to first admission.
type SetDesiredRequest struct {
	ID               RequestID
	ExpectedRevision uint64
	Values           map[string]ParameterValue
	// Remove relinquishes desired management of a path; it is not USP Delete.
	Remove []string
}
type DesiredReceipt struct {
	ID       RequestID
	Revision uint64
}

// CommandRequest policy is selected by the application, never a caller retry flag.
type CommandRequest struct {
	ID       RequestID
	Request  *usp.Request
	Deadline time.Time
}
type CommandState string

const (
	CommandQueued             CommandState = "queued"
	CommandAttempting         CommandState = "attempting"
	CommandAwaitingResponse   CommandState = "awaiting_response"
	CommandAwaitingCompletion CommandState = "awaiting_completion"
	CommandSucceeded          CommandState = "succeeded"
	CommandFailed             CommandState = "failed"
	CommandUnknown            CommandState = "unknown"
	CommandExpired            CommandState = "expired"
	CommandCancelled          CommandState = "cancelled"
)

type CommandReceipt struct {
	ID       RequestID
	Revision uint64
	State    CommandState
}
type DeviceCommand struct {
	CommandReceipt
	Request                            *usp.Request
	Deadline                           time.Time
	MessageID, OperationKey, AttemptID string
	Result                             *usp.Msg
	Reason                             string
}
type CancelCommandRequest struct {
	ID               RequestID
	ExpectedRevision uint64
}

// Inbound DTOs are internal contracts, not proof of authentication. The ingress
// adapter must apply protocol validation and actor-identity checks before calling
// the actor; the actor still verifies command correlation and duplicate content.
type DeviceResponse struct {
	Identity   DeviceIdentity
	Message    *usp.Msg
	ReceivedAt time.Time
}
type DeviceNotification struct {
	Identity   DeviceIdentity
	Message    *usp.Msg
	ReceivedAt time.Time
}
type NotifyReceipt struct {
	MessageID string
	Duplicate bool
}

type PublishAttempt struct {
	ID, MessageID string
	Message       *usp.Msg
	Deadline      time.Time
}
type PublishResult struct {
	// False for QoS 0: local send completion is not broker acknowledgment.
	BrokerAccepted bool
}
type DeviceTransport interface {
	// One bounded application attempt. An error may mean an external effect occurred.
	Publish(context.Context, DeviceIdentity, PublishAttempt) (PublishResult, error)
}
type DeviceEvent struct {
	Identity      DeviceIdentity
	Epoch         string
	Sequence      uint64
	SchemaVersion string
	ReceivedAt    time.Time
	Message       *usp.Msg
}
type DeviceEventPublisher interface {
	// Success is durable broker handoff, not downstream consumer completion.
	Publish(context.Context, DeviceEvent) error
}
