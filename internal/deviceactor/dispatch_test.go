package deviceactor

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type fakeSender struct {
	calls     []device.PublishAttempt
	ready     bool
	err       error
	afterSend func()
}

func (f *fakeSender) Ready() bool { return f.ready }
func (f *fakeSender) Publish(_ context.Context, _ device.DeviceIdentity, p device.PublishAttempt) (device.PublishResult, error) {
	f.calls = append(f.calls, p)
	if f.afterSend != nil {
		f.afterSend()
	}
	return device.PublishResult{BrokerAccepted: f.err == nil}, f.err
}
func executable(t *testing.T) (*Actor, *memoryBackend, *fakeSender, *time.Time) {
	t.Helper()
	a, b, now := fixture(t)
	f := &fakeSender{ready: true}
	a, err := NewExecuting(identity, b, a.opts, f)
	if err != nil {
		t.Fatal(err)
	}
	return a, b, f, now
}
func response(mid string) *usp.Msg {
	return &usp.Msg{Header: &usp.Header{MsgId: mid, MsgType: usp.Header_GET_RESP}, Body: &usp.Body{MsgBody: &usp.Body_Response{Response: &usp.Response{RespType: &usp.Response_GetResp{GetResp: &usp.GetResp{}}}}}}
}
func TestDispatchOneFlightAndCorrelate(t *testing.T) {
	a, _, f, now := executable(t)
	for _, id := range []device.RequestID{"one", "two"} {
		if _, err := a.EnqueueCommand(ctx, command(id, now.Add(time.Minute))); err != nil {
			t.Fatal(err)
		}
	}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 1 {
		t.Fatal("multiple in flight")
	}
	c, _ := a.GetCommand(ctx, "one")
	if c.State != device.CommandAwaitingResponse || c.MessageID == "" {
		t.Fatal(c)
	}
	msg := response(c.MessageID)
	bad := identity
	bad.AgentID = "other"
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: bad, Message: msg}); !errors.Is(err, ErrInvalid) {
		t.Fatal("wrong identity accepted", err)
	}
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: response("wrong")}); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: msg}); err != nil {
		t.Fatal(err)
	}
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: msg}); err != nil {
		t.Fatal("duplicate", err)
	}
	c, _ = a.GetCommand(ctx, "one")
	if c.State != device.CommandSucceeded || !proto.Equal(c.Result, msg) {
		t.Fatal(c)
	}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 2 {
		t.Fatal("next did not dispatch")
	}
}
func TestUncertaintyBoundariesNeverReplay(t *testing.T) {
	for _, scenario := range []string{"begin-failure", "begin-ambiguous", "send-error", "finish-failure", "finish-ambiguous"} {
		t.Run(scenario, func(t *testing.T) {
			a, b, f, now := executable(t)
			a.EnqueueCommand(ctx, command("one", now.Add(time.Minute)))
			switch scenario {
			case "begin-failure":
				b.failSave = true
			case "begin-ambiguous":
				b.ambiguous = true
			case "send-error":
				f.err = errors.New("lost acknowledgment")
			case "finish-failure":
				f.afterSend = func() { b.failSave = true }
			case "finish-ambiguous":
				f.afterSend = func() { b.ambiguous = true }
			}
			_ = a.Tick(ctx)
			count := len(f.calls)
			if (scenario == "begin-failure" || scenario == "begin-ambiguous") && count != 0 {
				t.Fatal("sent after failed begin")
			}
			b.failSave = false
			f.afterSend = nil
			restarted, err := NewExecuting(identity, b, a.opts, f)
			if err != nil {
				t.Fatal(err)
			}
			if err = restarted.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			c, err := restarted.GetCommand(ctx, "one")
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "begin-failure" {
				if len(f.calls) != 1 {
					t.Fatal("unsent queued command not resumed")
				}
				return
			}
			if len(f.calls) != count {
				t.Fatal("replayed ambiguous attempt")
			}
			if scenario != "finish-ambiguous" && c.State != device.CommandUnknown {
				t.Fatal(c.State)
			}
		})
	}
}
func TestOfflineExpiryAndLateEvidence(t *testing.T) {
	a, _, f, now := executable(t)
	f.ready = false
	a.EnqueueCommand(ctx, command("one", now.Add(time.Minute)))
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	c, _ := a.GetCommand(ctx, "one")
	if c.State != device.CommandQueued || len(f.calls) != 0 {
		t.Fatal("offline attempt")
	}
	f.ready = true
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	c, _ = a.GetCommand(ctx, "one")
	*now = now.Add(2 * time.Minute)
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	expired, _ := a.GetCommand(ctx, "one")
	if expired.State != device.CommandUnknown {
		t.Fatal(expired.State)
	}
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: response(c.MessageID)}); err != nil {
		t.Fatal(err)
	}
	resolved, _ := a.GetCommand(ctx, "one")
	if resolved.State != device.CommandSucceeded {
		t.Fatal(resolved.State)
	}
}
func TestOperateAdmittedInCanary(t *testing.T) {
	a, _, _, now := executable(t)
	r := command("op", now.Add(time.Hour))
	r.Request = &usp.Request{ReqType: &usp.Request_Operate{Operate: &usp.Operate{Command: "Device.Reboot()", CommandKey: "key"}}}
	if _, err := a.EnqueueCommand(ctx, r); err != nil {
		t.Fatal(err)
	}
}
func TestUSPFailureResult(t *testing.T) {
	a, _, _, now := executable(t)
	a.EnqueueCommand(ctx, command("one", now.Add(time.Minute)))
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	c, _ := a.GetCommand(ctx, "one")
	msg := &usp.Msg{Header: &usp.Header{MsgId: c.MessageID, MsgType: usp.Header_ERROR}, Body: &usp.Body{MsgBody: &usp.Body_Error{Error: &usp.Error{ErrCode: 7000, ErrMsg: "failed"}}}}
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: msg}); err != nil {
		t.Fatal(err)
	}
	c, _ = a.GetCommand(ctx, "one")
	if c.State != device.CommandFailed {
		t.Fatal(c.State)
	}
}

func TestPartialSetFailureAndWrongOperation(t *testing.T) {
	a, _, _, now := executable(t)
	r := command("set", now.Add(time.Minute))
	r.Request = &usp.Request{ReqType: &usp.Request_Set{Set: &usp.Set{AllowPartial: true}}}
	if _, err := a.EnqueueCommand(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	c, _ := a.GetCommand(ctx, "set")
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: response(c.MessageID)}); !errors.Is(err, ErrConflict) {
		t.Fatal("wrong operation accepted", err)
	}
	// Use protobuf JSON to describe the nested partial-error shape independently
	// of the reflection-based classifier.
	msg := new(usp.Msg)
	body := `{"header":{"msgId":"` + c.MessageID + `","msgType":"SET_RESP"},"body":{"response":{"setResp":{"updatedObjResults":[{"requestedPath":"Device.Test.","operStatus":{"operSuccess":{"updatedInstResults":[{"affectedPath":"Device.Test.","paramErrs":[{"param":"Value","errCode":7007,"errMsg":"invalid"}]}]}}}]}}}}`
	if err := protojson.Unmarshal([]byte(body), msg); err != nil {
		t.Fatal(err)
	}
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: msg}); err != nil {
		t.Fatal(err)
	}
	c, _ = a.GetCommand(ctx, "set")
	if c.State != device.CommandFailed {
		t.Fatal("partial failure marked successful", c.State)
	}
}

func TestCancelledAndExpiredCommandsCannotPublish(t *testing.T) {
	a, _, f, now := executable(t)
	a.EnqueueCommand(ctx, command("cancel", now.Add(time.Minute)))
	if _, err := a.CancelCommand(ctx, device.CancelCommandRequest{ID: "cancel", ExpectedRevision: 1}); err != nil {
		t.Fatal(err)
	}
	a.EnqueueCommand(ctx, command("expire", now.Add(time.Second)))
	*now = now.Add(2 * time.Second)
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 0 {
		t.Fatal("cancelled/expired publication")
	}
}
