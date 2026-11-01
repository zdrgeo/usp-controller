package deviceactor

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"example.com/usp-controller/internal/protocol"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// NewExecuting is the explicit canary opt-in. New continues to disable dispatch.
// Transport must be local, bounded, and must not retry application publication.
func NewExecuting(id device.DeviceIdentity, b Backend, o Options, t device.DeviceTransport) (*Actor, error) {
	if t == nil {
		return nil, ErrInvalid
	}
	a, err := New(id, b, o)
	if err != nil {
		return nil, err
	}
	a.transport = t
	return a, nil
}
func commandView(c storedCommand, req *usp.Request) (device.DeviceCommand, error) {
	out := device.DeviceCommand{DesiredRevision: c.DesiredRevision, CommandReceipt: c.Receipt, Request: req, Deadline: c.Deadline, Reason: c.Reason, MessageID: c.MessageID, AttemptID: c.MessageID, OperationKey: c.OperationKey}
	if len(c.Result) > 0 {
		out.Result = new(usp.Msg)
		if err := proto.Unmarshal(c.Result, out.Result); err != nil {
			return device.DeviceCommand{}, err
		}
	}
	if len(c.Completion) > 0 {
		out.Completion = new(usp.Msg)
		if err := proto.Unmarshal(c.Completion, out.Completion); err != nil {
			return device.DeviceCommand{}, err
		}
	}
	return out, nil
}
func transition(c *storedCommand, state device.CommandState, reason string) error {
	if c.Receipt.Revision == math.MaxUint64 {
		return ErrCapacity
	}
	c.Receipt.Revision++
	c.Receipt.State = state
	c.Reason = reason
	return nil
}

// Tick performs at most one publication. Durable attempting is never replayed,
// including when a previous save or transport result was ambiguous.
func (a *Actor) Tick(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	s, err := a.load(ctx)
	if err != nil {
		return err
	}
	now := a.opts.Now()
	changed := false
	for id, c := range s.Commands {
		state := c.Receipt.State
		if state == device.CommandQueued && c.DesiredRevision != 0 && !a.currentReconciliation(s, c) {
			if err = transition(&c, device.CommandCancelled, "desired revision, policy or observation no longer eligible"); err != nil {
				return err
			}
			s.Commands[id] = c
			changed = true
			continue
		}
		if state == device.CommandAttempting || ((state == device.CommandAwaitingResponse || state == device.CommandAwaitingCompletion) && !now.Before(c.Deadline)) {
			if err = transition(&c, device.CommandUnknown, "interrupted publish or response deadline; not replayed"); err != nil {
				return err
			}
			s.Commands[id] = c
			changed = true
		} else if state == device.CommandQueued && !now.Before(c.Deadline) {
			if err = transition(&c, device.CommandExpired, "deadline reached before publish"); err != nil {
				return err
			}
			s.Commands[id] = c
			changed = true
		}
	}
	a.compactQueue(s)
	if changed {
		if err = a.save(ctx, s); err != nil {
			return err
		}
	}
	if err = a.exportOne(ctx, s); err != nil {
		return err
	}
	if a.transport == nil {
		return nil
	}
	for _, c := range s.Commands {
		if c.Receipt.State == device.CommandUnknown || c.Receipt.State == device.CommandAwaitingResponse || c.Receipt.State == device.CommandAwaitingCompletion {
			return nil
		}
	}
	if ready, ok := a.transport.(interface{ Ready() bool }); ok && !ready.Ready() {
		return nil
	}
	if len(s.Queue) == 0 {
		if err = a.scheduleReconciliation(ctx, s); err != nil {
			return err
		}
		if len(s.Queue) == 0 {
			return nil
		}
	}
	id := s.Queue[0]
	c := s.Commands[id]
	// Export and queue persistence may take time. Recheck observation eligibility
	// immediately before beginning a generated write's uncertainty boundary.
	if c.DesiredRevision != 0 && !a.currentReconciliation(s, c) {
		if err = transition(&c, device.CommandCancelled, "observation no longer eligible before attempt"); err != nil {
			return err
		}
		s.Commands[id] = c
		a.compactQueue(s)
		return a.save(ctx, s)
	}
	request := new(usp.Request)
	if err = proto.Unmarshal(c.Payload, request); err != nil {
		return err
	}
	if op := request.GetOperate(); op != nil && !concreteOperation(op.Command) {
		return ErrInvalid
	}
	kind, err := protocol.RequestType(request)
	if err != nil {
		return err
	}
	random := make([]byte, 16)
	if _, err = rand.Read(random); err != nil {
		return err
	}
	c.MessageID = hex.EncodeToString(random)
	if op := request.GetOperate(); op != nil {
		c.OperationKey = c.MessageID
		op.CommandKey = c.OperationKey
		op.SendResp = true
	}
	msg := &usp.Msg{Header: &usp.Header{MsgId: c.MessageID, MsgType: kind}, Body: &usp.Body{MsgBody: &usp.Body_Request{Request: request}}}
	// Validate encoding and size before committing the uncertainty boundary.
	if _, err = protocol.Encode(a.id.ControllerID, a.id.AgentID, msg); err != nil {
		return err
	}
	if err = transition(&c, device.CommandAttempting, ""); err != nil {
		return err
	}
	s.Commands[id] = c
	a.compactQueue(s)
	if err = a.save(ctx, s); err != nil {
		return err
	} // no send after an ambiguous begin commit
	sendCtx, cancel := context.WithDeadline(ctx, c.Deadline)
	sendCtx, stop := context.WithTimeout(sendCtx, 5*time.Second)
	result, publishErr := a.transport.Publish(sendCtx, a.id, device.PublishAttempt{ID: c.MessageID, MessageID: c.MessageID, Message: msg, Deadline: c.Deadline})
	stop()
	cancel()
	state, reason := device.CommandAwaitingResponse, "local send completed; awaiting USP response"
	if result.BrokerAccepted {
		reason = "broker accepted; awaiting USP response"
	}
	if publishErr != nil {
		state = device.CommandUnknown
		reason = "publish outcome uncertain; not replayed"
	}
	if err = transition(&c, state, reason); err != nil {
		return err
	}
	s.Commands[id] = c
	// If this commit fails, durable attempting is recovered as unknown on next Tick.
	return a.save(ctx, s)
}

func (a *Actor) HandleResponse(ctx context.Context, r device.DeviceResponse) error {
	if a.transport == nil {
		return ErrDisabled
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if r.Identity != a.id {
		return ErrInvalid
	}
	if err := protocol.ValidateResponse(r.Message); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	s, err := a.load(ctx)
	if err != nil {
		return err
	}
	for id, c := range s.Commands {
		if c.MessageID == "" || c.MessageID != r.Message.Header.MsgId {
			continue
		}
		req := new(usp.Request)
		if err = proto.Unmarshal(c.Payload, req); err != nil {
			return err
		}
		kind, err := protocol.RequestType(req)
		if err != nil {
			return err
		}
		if r.Message.Header.MsgType != kind+1 && r.Message.Header.MsgType != usp.Header_ERROR {
			return ErrConflict
		}
		payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(r.Message)
		if err != nil {
			return err
		}
		if len(c.Result) > 0 {
			old := new(usp.Msg)
			if err = proto.Unmarshal(c.Result, old); err != nil {
				return err
			}
			if proto.Equal(old, r.Message) {
				return nil
			}
			return ErrConflict
		}
		state := device.CommandSucceeded
		reason := "matching USP response received"
		if r.Message.Header.MsgType == usp.Header_ERROR || hasFailure(r.Message.ProtoReflect()) {
			state = device.CommandFailed
			reason = "USP response includes an operation or parameter failure"
		}
		if op := req.GetOperate(); op != nil && r.Message.Header.MsgType != usp.Header_ERROR {
			results := r.Message.GetBody().GetResponse().GetOperateResp().GetOperationResults()
			if len(results) != 1 || results[0].GetExecutedCommand() != op.Command || results[0].GetOperationResp() == nil {
				return ErrConflict
			}
			if _, async := results[0].OperationResp.(*usp.OperateResp_OperationResult_ReqObjPath); async {
				if results[0].GetReqObjPath() == "" {
					return ErrInvalid
				}
				state, reason = device.CommandAwaitingCompletion, "operation accepted; awaiting OperationComplete"
			}
		}
		if len(c.Completion) > 0 {
			// Completion is stronger evidence. Retain the late response without regression.
			c.Result = payload
			s.Commands[id] = c
			return a.save(ctx, s)
		}
		switch c.Receipt.State {
		case device.CommandAttempting, device.CommandAwaitingResponse, device.CommandUnknown:
		default:
			return ErrConflict
		}

		if err = transition(&c, state, reason); err != nil {
			return err
		}
		c.Result = payload
		s.Commands[id] = c
		return a.save(ctx, s)
	}
	return ErrNotFound
}

// Preserve partial errors nested in Set/Add/Delete/Get results. Failure variants
// are failures even if a peer incorrectly leaves their numeric code at zero.
func hasFailure(m protoreflect.Message) bool {
	failed := false
	m.Range(func(f protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if (f.Name() == "err_code" && v.Uint() != 0) || f.Name() == "oper_failure" || f.Name() == "cmd_failure" {
			failed = true
			return false
		}
		if f.IsMap() {
			if f.MapValue().Kind() == protoreflect.MessageKind {
				v.Map().Range(func(_ protoreflect.MapKey, x protoreflect.Value) bool {
					failed = hasFailure(x.Message())
					return !failed
				})
			}
		} else if f.IsList() {
			if f.Kind() == protoreflect.MessageKind {
				for i := 0; i < v.List().Len(); i++ {
					if hasFailure(v.List().Get(i).Message()) {
						failed = true
						break
					}
				}
			}
		} else if f.Kind() == protoreflect.MessageKind {
			failed = hasFailure(v.Message())
		}
		return !failed
	})
	return failed
}
