package deviceactor

import (
	"context"
	"strings"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
)

func blocksUnknown(c storedCommand) bool {
	return c.Receipt.State == device.CommandUnknown && c.UnknownAcknowledgement == nil
}
func validAcknowledgement(ack *device.UnknownAcknowledgement) bool {
	return ack.RequestID != "" && len(ack.RequestID) <= 128 && !strings.HasPrefix(string(ack.RequestID), "reconcile-") && ack.Revision > 1 && ack.Reason != "" && strings.TrimSpace(ack.Reason) == ack.Reason && len(ack.Reason) <= 1024 && !ack.RecordedAt.IsZero()
}

// AcknowledgeUnknown durably releases the per-device uncertainty barrier. It does
// not change the outcome or create a new attempt. Valid late evidence can still
// settle the command, while its original attempt identity and audit are retained.
func (a *Actor) AcknowledgeUnknown(ctx context.Context, r device.AcknowledgeUnknownRequest) (device.DeviceCommand, error) {
	if a.transport == nil {
		return device.DeviceCommand{}, ErrDisabled
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	r.Reason = strings.TrimSpace(r.Reason)
	if r.ID == "" || r.RequestID == "" || len(r.RequestID) > 128 || strings.HasPrefix(string(r.RequestID), "reconcile-") || r.ExpectedRevision == 0 || r.Reason == "" || len(r.Reason) > 1024 {
		return device.DeviceCommand{}, ErrInvalid
	}
	fp, err := fingerprint(r)
	if err != nil {
		return device.DeviceCommand{}, err
	}
	s, err := a.load(ctx)
	if err != nil {
		return device.DeviceCommand{}, err
	}
	_, duplicate, err := check(s, r.RequestID, "unknown_ack", fp)
	if err != nil {
		return device.DeviceCommand{}, err
	}
	c, ok := s.Commands[r.ID]
	if !ok {
		return device.DeviceCommand{}, ErrNotFound
	}
	if !duplicate {
		if !blocksUnknown(c) || c.Receipt.Revision != r.ExpectedRevision {
			return device.DeviceCommand{}, ErrConflict
		}
		if err := transition(&c, device.CommandUnknown, c.Reason); err != nil {
			return device.DeviceCommand{}, err
		}
		c.UnknownAcknowledgement = &device.UnknownAcknowledgement{RequestID: r.RequestID, Revision: c.Receipt.Revision, Reason: r.Reason, RecordedAt: a.opts.Now().UTC()}
		if !validAcknowledgement(c.UnknownAcknowledgement) {
			return device.DeviceCommand{}, ErrInvalid
		}
		s.Commands[r.ID] = c
		s.Receipts[r.RequestID] = receipt{Kind: "unknown_ack", Fingerprint: fp, Revision: c.Receipt.Revision}
	}
	// Scheduling is repaired for ambiguous-commit retries as on other admissions.
	if err := a.backend.EnsureReminder(ctx); err != nil {
		return device.DeviceCommand{}, err
	}
	if !duplicate {
		if err := a.save(ctx, s); err != nil {
			return device.DeviceCommand{}, err
		}
	}
	req := new(usp.Request)
	if err := proto.Unmarshal(c.Payload, req); err != nil {
		return device.DeviceCommand{}, err
	}
	return commandView(c, req)
}
