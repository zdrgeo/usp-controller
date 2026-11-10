package deviceactor

import (
	"errors"
	"strings"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
)

func TestCompletionReserveProtectsCorrelatedEvidenceAndFutureAttempts(t *testing.T) {
	a, b, f, now := executable(t)
	a.opts.MaxCommands = 4
	a.opts.MaxReceipts = 5
	a.opts.CompletionReserve = CompletionReserve{Notifications: 1}
	enqueue := func(id device.RequestID) {
		t.Helper()
		r := command(id, now.Add(time.Hour))
		r.Request = &usp.Request{ReqType: &usp.Request_Operate{Operate: &usp.Operate{Command: "Device.Reboot()"}}}
		if _, err := a.EnqueueCommand(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	completion := func(mid string, c device.DeviceCommand) device.DeviceNotification {
		return notification(mid, &usp.Notify{Notification: &usp.Notify_OperComplete{OperComplete: &usp.Notify_OperationComplete{ObjPath: "Device.", CommandName: "Reboot()", CommandKey: c.OperationKey, OperationResp: &usp.Notify_OperationComplete_ReqOutputArgs{ReqOutputArgs: &usp.Notify_OperationComplete_OutputArgs{}}}}}, *now)
	}
	enqueue("old-acknowledged")
	f.err = errors.New("old uncertain operation")
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	f.err = nil
	old, _ := a.GetCommand(ctx, "old-acknowledged")
	if _, err := a.AcknowledgeUnknown(ctx, device.AcknowledgeUnknownRequest{ID: old.ID, RequestID: "review-old", ExpectedRevision: old.Revision, Reason: "independent review"}); err != nil {
		t.Fatal(err)
	}
	enqueue("first")
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	c, _ := a.GetCommand(ctx, "first")
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: operateResponse(c.MessageID, true)}); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"normal-1", "normal-2", "normal-3"} {
		if _, err := a.HandleNotify(ctx, valueNotification(id, *now)); err != nil {
			t.Fatal(err)
		}
	}
	before := b.saves
	if _, err := a.HandleNotify(ctx, valueNotification("overflow", *now)); !errors.Is(err, ErrCapacity) || b.saves != before {
		t.Fatal("ordinary traffic consumed reserve", err)
	}
	if _, err := a.HandleNotify(ctx, completion("older-completion", old)); !errors.Is(err, ErrCapacity) {
		t.Fatal("acknowledged old attempt consumed current reserve", err)
	}
	unchanged, _ := a.GetCommand(ctx, old.ID)
	if unchanged.State != device.CommandUnknown || unchanged.Completion != nil {
		t.Fatal("rejected old completion settled command")
	}
	matched := completion("completion", c)
	unmatched := matched
	unmatched.Message = proto.Clone(matched.Message).(*usp.Msg)
	unmatched.Message.Header.MsgId = "unmatched"
	unmatched.Message.GetBody().GetRequest().GetNotify().GetOperComplete().CommandKey = "unrelated"
	if _, err := a.HandleNotify(ctx, unmatched); !errors.Is(err, ErrCapacity) {
		t.Fatal("unrelated completion consumed priority capacity", err)
	}
	b.failSave = true
	if _, err := a.HandleNotify(ctx, matched); err == nil {
		t.Fatal("uncommitted completion acknowledged")
	}
	b.failSave = false
	c, _ = a.GetCommand(ctx, "first")
	if c.State != device.CommandAwaitingCompletion {
		t.Fatal("failed save settled operation")
	}
	if _, err := a.HandleNotify(ctx, matched); err != nil {
		t.Fatal("matching completion could not use reserve", err)
	}
	before = b.saves
	if receipt, err := a.HandleNotify(ctx, matched); err != nil || !receipt.Duplicate || b.saves != before {
		t.Fatal("duplicate needed new capacity", receipt, err)
	}
	repeated := matched
	repeated.Message = proto.Clone(matched.Message).(*usp.Msg)
	repeated.Message.Header.MsgId = "same-completion-new-id"
	if _, err := a.HandleNotify(ctx, repeated); !errors.Is(err, ErrCapacity) {
		t.Fatal("terminal completion redelivery gained reserve priority", err)
	}
	enqueue("second")
	if err := a.Tick(ctx); !errors.Is(err, ErrCapacity) || len(f.calls) != 2 {
		t.Fatal("Operate began without outbox completion capacity", err)
	}
	queued, _ := a.GetCommand(ctx, "second")
	if queued.State != device.CommandQueued || queued.MessageID != "" {
		t.Fatal("failed capacity gate crossed attempt boundary", queued)
	}
	// Export frees an event slot, but inbox identities are intentionally retained.
	sink := &eventSink{}
	var err error
	a, err = NewWithEvents(identity, b, a.opts, f, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil || len(f.calls) != 3 {
		t.Fatal("export did not restore event capacity", err)
	}
	second, _ := a.GetCommand(ctx, "second")
	if _, err := a.HandleNotify(ctx, completion("second-completion", second)); err != nil {
		t.Fatal(err)
	}
	enqueue("third")
	if err := a.Tick(ctx); !errors.Is(err, ErrCapacity) || len(f.calls) != 3 {
		t.Fatal("Operate began with full retained inbox", err)
	}
	third, _ := a.GetCommand(ctx, "third")
	if third.State != device.CommandQueued || third.MessageID != "" {
		t.Fatal("inbox capacity check was after begin commit", third)
	}
}

func TestResponseByteReservePreventsNewBeginButAllowsEvidence(t *testing.T) {
	a, b, f, now := executable(t)
	for _, id := range []device.RequestID{"first", "second"} {
		if _, err := a.EnqueueCommand(ctx, command(id, now.Add(time.Hour))); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	c, _ := a.GetCommand(ctx, "first")
	baseline := len(b.state)
	a.opts.MaxBytes = baseline + 2048
	a.opts.CompletionReserve = CompletionReserve{StateBytes: 1024}
	ordinary := desired("large-intent", 0)
	ordinary.Values["Device.Test.Value"] = device.ParameterValue{Value: strings.Repeat("x", 1600), Type: "string"}
	before := b.saves
	if _, err := a.SetDesired(ctx, ordinary); !errors.Is(err, ErrCapacity) || b.saves != before {
		t.Fatal("ordinary intent consumed byte reserve", err)
	}
	msg := response(c.MessageID)
	msg.GetBody().GetResponse().GetGetResp().ReqPathResults = []*usp.GetResp_RequestedPathResult{{RequestedPath: "Device.Test.", ResolvedPathResults: []*usp.GetResp_ResolvedPathResult{{ResolvedPath: "Device.Test.", ResultParams: map[string]string{"Value": strings.Repeat("x", 1000)}}}}}
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: msg}); err != nil {
		t.Fatal("correlated response could not use bytes", err)
	}
	if len(b.state) <= a.opts.MaxBytes-a.opts.CompletionReserve.StateBytes || len(b.state) > a.opts.MaxBytes {
		t.Fatal("fixture did not consume only reserved headroom")
	}
	c, _ = a.GetCommand(ctx, "first")
	if c.State != device.CommandSucceeded {
		t.Fatal(c)
	}
	if err := a.Tick(ctx); !errors.Is(err, ErrCapacity) || len(f.calls) != 1 {
		t.Fatal("new attempt consumed reserved bytes", err)
	}
	queued, _ := a.GetCommand(ctx, "second")
	if queued.State != device.CommandQueued || queued.MessageID != "" {
		t.Fatal("capacity failure was after uncertainty boundary")
	}
	// A repeated response remains idempotent even above the ordinary budget.
	before = b.saves
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: msg}); err != nil || b.saves != before {
		t.Fatal("duplicate response required new space", err)
	}
}
