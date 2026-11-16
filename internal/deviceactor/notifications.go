package deviceactor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"regexp"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
)

var operationPath = regexp.MustCompile(`^Device\.(?:[A-Za-z0-9_]+\.)*[A-Za-z_][A-Za-z0-9_]*\(\)$`)

func concreteOperation(path string) bool { return operationPath.MatchString(path) }

type storedEvent struct {
	Epoch      string
	Sequence   uint64
	ReceivedAt time.Time
	Payload    []byte
}

// NewWithEvents enables the canary notification/export path. New remains dormant.
func NewWithEvents(id device.DeviceIdentity, b Backend, o Options, t device.DeviceTransport, p device.DeviceEventPublisher) (*Actor, error) {
	if p == nil {
		return nil, ErrInvalid
	}
	a, err := NewExecuting(id, b, o, t)
	if err != nil {
		return nil, err
	}
	a.publisher = p
	return a, nil
}
func (a *Actor) validateEvents(s *snapshot) error {
	if len(s.Inbox) > a.opts.MaxReceipts || len(s.Outbox) > a.opts.MaxCommands {
		return ErrCapacity
	}
	for id, fp := range s.Inbox {
		if id == "" || len(fp) != 64 {
			return fmt.Errorf("invalid notification receipt")
		}
	}
	var previous uint64
	for _, e := range s.Outbox {
		if e.Epoch == "" || e.Sequence == 0 || e.Sequence <= previous || e.Sequence > s.EventSequence || e.ReceivedAt.IsZero() {
			return fmt.Errorf("invalid event outbox")
		}
		msg := new(usp.Msg)
		if err := proto.Unmarshal(e.Payload, msg); err != nil {
			return err
		}
		fp, err := fingerprint(e.Payload)
		if err != nil {
			return err
		}
		if s.Inbox[msg.GetHeader().GetMsgId()] != fp {
			return fmt.Errorf("outbox missing matching receipt")
		}
		if msg.GetHeader().GetMsgType() != usp.Header_NOTIFY {
			return fmt.Errorf("invalid outbox payload")
		}
		previous = e.Sequence
	}
	return nil
}

// HandleNotify commits receipt, twin, completion evidence and outbox atomically.
// Receipt identity is scoped to this device and USP message ID. Arrival time is
// excluded from the fingerprint so retransmission after a lost ACK is harmless.
func (a *Actor) HandleNotify(ctx context.Context, r device.DeviceNotification) (device.NotifyReceipt, error) {
	zero := device.NotifyReceipt{}
	if a.transport == nil {
		return zero, ErrDisabled
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	msg := r.Message
	if r.Identity != a.id || msg == nil || msg.GetHeader().GetMsgId() == "" || msg.GetHeader().GetMsgType() != usp.Header_NOTIFY {
		return zero, ErrInvalid
	}
	n := msg.GetBody().GetRequest().GetNotify()
	if n == nil || n.Notification == nil {
		return zero, ErrInvalid
	}
	if r.ReceivedAt.IsZero() || r.ReceivedAt.After(a.opts.Now()) {
		return zero, ErrInvalid
	}
	if v := n.GetValueChange(); v != nil && !validPath(v.ParamPath) {
		return zero, ErrInvalid
	}
	if c := n.GetOperComplete(); c != nil && (c.CommandKey == "" || !concreteOperation(c.ObjPath+c.CommandName) || c.OperationResp == nil) {
		return zero, ErrInvalid
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		return zero, err
	}
	fp, err := fingerprint(payload)
	if err != nil {
		return zero, err
	}
	s, err := a.load(ctx)
	if err != nil {
		return zero, err
	}
	mid := msg.Header.MsgId
	if old, exists := s.Inbox[mid]; exists {
		if old != fp {
			return zero, ErrConflict
		}
		if err = a.backend.EnsureReminder(ctx); err != nil {
			return zero, err
		}
		return device.NotifyReceipt{MessageID: mid, Duplicate: true}, nil
	}
	if s.EventSequence == math.MaxUint64 {
		return zero, ErrCapacity
	}
	// All notification classes are durable in this canary. No silent telemetry loss.
	s.Inbox[mid] = fp
	a.markTopologyChange(s, n, mid, fp, r.ReceivedAt)
	if v := n.GetValueChange(); v != nil && a.managedObservation(v.ParamPath, v.ParamValue) {
		old, exists := s.Reported[v.ParamPath]
		if !exists || r.ReceivedAt.After(old.ReceivedAt) {
			if s.ReportedRevision == math.MaxUint64 {
				return zero, ErrCapacity
			}
			s.ReportedRevision++
			s.Reported[v.ParamPath] = device.Observation{Value: a.observedValue(v.ParamPath, v.ParamValue), ReceivedAt: r.ReceivedAt, FreshUntil: r.ReceivedAt.Add(a.opts.Freshness), Source: "usp-value-change"}
		}
	}
	if r.ReceivedAt.After(s.LastContact) {
		s.LastContact = r.ReceivedAt
	}
	completionPriority := false
	if complete := n.GetOperComplete(); complete != nil {
		for id, c := range s.Commands {
			if c.OperationKey == "" || c.OperationKey != complete.CommandKey {
				continue
			}
			req := new(usp.Request)
			if err = proto.Unmarshal(c.Payload, req); err != nil {
				return zero, err
			}
			if req.GetOperate().GetCommand() != complete.ObjPath+complete.CommandName {
				return zero, ErrConflict
			}
			if len(c.Completion) > 0 {
				old := new(usp.Msg)
				if err = proto.Unmarshal(c.Completion, old); err != nil {
					return zero, err
				}
				if !proto.Equal(old.GetBody().GetRequest().GetNotify().GetOperComplete(), complete) {
					return zero, ErrConflict
				}
				break
			}
			switch c.Receipt.State {
			case device.CommandAttempting, device.CommandAwaitingResponse, device.CommandAwaitingCompletion, device.CommandUnknown:
			default:
				return zero, ErrConflict
			}
			state := device.CommandSucceeded
			if complete.GetCmdFailure() != nil {
				state = device.CommandFailed
			}
			if err = transition(&c, state, "matching OperationComplete received"); err != nil {
				return zero, err
			}
			// Previously acknowledged uncertainty is ordinary late evidence; it
			// must not consume the headroom for the current unresolved attempt.
			completionPriority = c.UnknownAcknowledgement == nil
			c.Completion = payload
			a.markMutationOutcome(s, &c, req)
			s.Commands[id] = c
			break
		}
	}
	// A fresh random epoch for each new event prevents ID reuse even after restoring
	// an old snapshot. Retries retain the original stored epoch and sequence.
	epoch := make([]byte, 16)
	if _, err = rand.Read(epoch); err != nil {
		return zero, err
	}
	s.EventSequence++
	s.Outbox = append(s.Outbox, storedEvent{Epoch: hex.EncodeToString(epoch), Sequence: s.EventSequence, ReceivedAt: r.ReceivedAt.UTC(), Payload: payload})
	if err = a.saveNotification(ctx, s, completionPriority); err != nil {
		return zero, err
	}
	return device.NotifyReceipt{MessageID: mid}, nil
}

const MaxEventExportsPerTurn = 32

// One FIFO batch per turn shares a single publication deadline and removal save.
// handoffFailed permits optional dispatch only when the remaining snapshot is
// trustworthy: no prefix was removed, or prefix removal committed successfully.
func (a *Actor) exportBatch(ctx context.Context, s *snapshot) (handoffFailed bool, err error) {
	if a.publisher == nil || len(s.Outbox) == 0 {
		return false, nil
	}
	publishCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	count := 0
	var exportErr error
	for count < min(a.opts.EventExportsPerTurn, len(s.Outbox)) {
		if exportErr = publishCtx.Err(); exportErr != nil {
			handoffFailed = true
			break
		}
		e := s.Outbox[count]
		msg := new(usp.Msg)
		if exportErr = proto.Unmarshal(e.Payload, msg); exportErr != nil {
			break
		}
		exportErr = a.publisher.Publish(publishCtx, device.DeviceEvent{Identity: a.id, Epoch: e.Epoch, Sequence: e.Sequence, SchemaVersion: "1", ReceivedAt: e.ReceivedAt, Message: msg})
		if exportErr != nil {
			handoffFailed = true
			break
		}
		count++
	}
	if count > 0 {
		s.Outbox = s.Outbox[count:]
		if saveErr := a.save(ctx, s); saveErr != nil {
			return false, errors.Join(exportErr, saveErr)
		}
	}
	return handoffFailed, exportErr
}
