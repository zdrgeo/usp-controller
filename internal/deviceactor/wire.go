package deviceactor

import (
	"encoding/json"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/encoding/protojson"
)

// Wire DTOs use protobuf JSON explicitly; encoding/json cannot round-trip oneofs.
type CommandInput struct {
	RequestID device.RequestID `json:"request_id"`
	Deadline  time.Time        `json:"deadline"`
	Request   json.RawMessage  `json:"request"`
}

func (in CommandInput) Domain() (device.CommandRequest, error) {
	req := new(usp.Request)
	if err := protojson.Unmarshal(in.Request, req); err != nil {
		return device.CommandRequest{}, ErrInvalid
	}
	return device.CommandRequest{ID: in.RequestID, Deadline: in.Deadline, Request: req}, nil
}

type CommandOutput struct {
	AttemptedAt            *time.Time                     `json:"attempted_at,omitempty"`
	AttemptSequence        uint64                         `json:"attempt_sequence,omitempty"`
	UnknownAcknowledgement *device.UnknownAcknowledgement `json:"unknown_acknowledgement,omitempty"`
	VerificationRevision   uint64                         `json:"verification_revision,omitempty"`
	ObservationRevision    uint64                         `json:"observation_revision,omitempty"`
	DesiredRevision        uint64                         `json:"desired_revision,omitempty"`
	RequestID              device.RequestID               `json:"request_id"`
	Revision               uint64                         `json:"revision"`
	State                  device.CommandState            `json:"state"`
	Deadline               *time.Time                     `json:"deadline,omitempty"`
	OperationKey           string                         `json:"operation_key,omitempty"`
	Completion             json.RawMessage                `json:"completion,omitempty"`
	MessageID              string                         `json:"message_id,omitempty"`
	Reason                 string                         `json:"reason,omitempty"`
	Request                json.RawMessage                `json:"request,omitempty"`
	Result                 json.RawMessage                `json:"result,omitempty"`
}

func commandOutput(c device.DeviceCommand) (CommandOutput, error) {
	out := CommandOutput{AttemptSequence: c.AttemptSequence, UnknownAcknowledgement: c.UnknownAcknowledgement, VerificationRevision: c.VerificationRevision, ObservationRevision: c.ObservationRevision, DesiredRevision: c.DesiredRevision, RequestID: c.ID, Revision: c.Revision, State: c.State, Deadline: &c.Deadline, MessageID: c.MessageID, Reason: c.Reason, OperationKey: c.OperationKey}
	if !c.AttemptedAt.IsZero() {
		at := c.AttemptedAt
		out.AttemptedAt = &at
	}
	var err error
	if c.Request != nil {
		out.Request, err = protojson.Marshal(c.Request)
		if err != nil {
			return out, err
		}
	}
	if c.Result != nil {
		out.Result, err = protojson.Marshal(c.Result)
	}
	if err == nil && c.Completion != nil {
		out.Completion, err = protojson.Marshal(c.Completion)
	}
	return out, err
}

type CommandLookup struct {
	RequestID device.RequestID `json:"request_id"`
}
type CommandCancel struct {
	RequestID        device.RequestID `json:"request_id"`
	ExpectedRevision uint64           `json:"expected_revision"`
}
type ResponseInput struct {
	Identity device.DeviceIdentity `json:"identity"`
	Message  json.RawMessage       `json:"message"`
}

// CommandUnknownAcknowledgement is an explicit operator release, never a retry.
type CommandUnknownAcknowledgement struct {
	RequestID        device.RequestID `json:"request_id"`
	CommandID        device.RequestID `json:"command_id"`
	ExpectedRevision uint64           `json:"expected_revision"`
	Reason           string           `json:"reason"`
}

// ExecutionInput requires an explicit choice and revision on every JSON boundary.
type ExecutionInput struct {
	RequestID        device.RequestID `json:"request_id"`
	ExpectedRevision *uint64          `json:"expected_revision"`
	Paused           *bool            `json:"paused"`
	Reason           string           `json:"reason"`
}

func (in ExecutionInput) Domain() (device.SetExecutionRequest, error) {
	if in.ExpectedRevision == nil || in.Paused == nil {
		return device.SetExecutionRequest{}, ErrInvalid
	}
	return device.SetExecutionRequest{RequestID: in.RequestID, ExpectedRevision: *in.ExpectedRevision, Paused: *in.Paused, Reason: in.Reason}, nil
}
