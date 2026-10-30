package deviceactor

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
)

type eventSink struct {
	events []device.DeviceEvent
	err    error
	after  func()
}

func (p *eventSink) Publish(_ context.Context, e device.DeviceEvent) error {
	p.events = append(p.events, e)
	if p.after != nil {
		p.after()
	}
	return p.err
}
func notification(mid string, n *usp.Notify, at time.Time) device.DeviceNotification {
	return device.DeviceNotification{Identity: identity, ReceivedAt: at, Message: &usp.Msg{Header: &usp.Header{MsgId: mid, MsgType: usp.Header_NOTIFY}, Body: &usp.Body{MsgBody: &usp.Body_Request{Request: &usp.Request{ReqType: &usp.Request_Notify{Notify: n}}}}}}
}
func valueNotification(mid string, at time.Time) device.DeviceNotification {
	return notification(mid, &usp.Notify{SubscriptionId: "sub", SendResp: true, Notification: &usp.Notify_ValueChange_{ValueChange: &usp.Notify_ValueChange{ParamPath: "Device.Test.Value", ParamValue: "observed"}}}, at)
}
func TestNotificationAtomicReceiptTwinAndOutbox(t *testing.T) {
	for _, mode := range []string{"normal", "save-failed", "ambiguous"} {
		t.Run(mode, func(t *testing.T) {
			a, b, _, now := executable(t)
			n := valueNotification("n", *now)
			b.failSave = mode == "save-failed"
			b.ambiguous = mode == "ambiguous"
			_, err := a.HandleNotify(ctx, n)
			if (mode == "normal") != (err == nil) {
				t.Fatal(err)
			}
			b.failSave = false
			a, err = NewExecuting(identity, b, a.opts, &fakeSender{})
			if err != nil {
				t.Fatal(err)
			}
			*now = now.Add(time.Second)
			n.ReceivedAt = *now
			receipt, err := a.HandleNotify(ctx, n)
			if err != nil || receipt.Duplicate != (mode != "save-failed") {
				t.Fatal(receipt, err)
			}
			s, err := a.load(ctx)
			if err != nil || len(s.Outbox) != 1 || len(s.Inbox) != 1 || s.ReportedRevision != 1 {
				t.Fatal(s, err)
			}
			twin, err := a.GetTwin(ctx)
			if err != nil || twin.Reported["Device.Test.Value"].Value.Value != "observed" {
				t.Fatal(twin, err)
			}
			changed := proto.Clone(n.Message).(*usp.Msg)
			changed.GetBody().GetRequest().GetNotify().GetValueChange().ParamValue = "changed"
			n.Message = changed
			if _, err = a.HandleNotify(ctx, n); !errors.Is(err, ErrConflict) {
				t.Fatal(err)
			}
		})
	}
}
func TestNotificationCapacityAndValidation(t *testing.T) {
	a, b, _, now := executable(t)
	n := valueNotification("n", *now)
	n.Identity.AgentID = "wrong"
	if _, err := a.HandleNotify(ctx, n); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	n.Identity = identity
	b.failReminder = true
	if _, err := a.HandleNotify(ctx, n); err == nil || len(b.state) != 0 {
		t.Fatal("accepted without reminder")
	}
	b.failReminder = false
	a.opts.MaxCommands = 1
	if _, err := a.HandleNotify(ctx, n); err != nil {
		t.Fatal(err)
	}
	n.Message.Header.MsgId = "second"
	if _, err := a.HandleNotify(ctx, n); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	s, _ := a.load(ctx)
	if len(s.Inbox) != 1 || s.ReportedRevision != 1 {
		t.Fatal("partial capacity commit")
	}
}
func TestOutboxFailureRecoveryAndStableDuplicates(t *testing.T) {
	for _, mode := range []string{"broker-error", "remove-failed", "remove-ambiguous"} {
		t.Run(mode, func(t *testing.T) {
			a, b, f, now := executable(t)
			sink := &eventSink{}
			a.publisher = sink
			if _, err := a.HandleNotify(ctx, valueNotification("n", *now)); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "broker-error":
				sink.err = errors.New("offline")
			case "remove-failed":
				sink.after = func() { b.failSave = true }
			case "remove-ambiguous":
				sink.after = func() { b.ambiguous = true }
			}
			if err := a.Tick(ctx); err == nil {
				t.Fatal("expected uncertain export")
			}
			b.failSave = false
			sink.err = nil
			sink.after = nil
			restarted, err := NewWithEvents(identity, b, a.opts, f, sink)
			if err != nil {
				t.Fatal(err)
			}
			if err = restarted.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			expected := 2
			if mode == "remove-ambiguous" {
				expected = 1
			}
			if len(sink.events) != expected {
				t.Fatal(len(sink.events))
			}
			if expected == 2 && (sink.events[0].Epoch != sink.events[1].Epoch || sink.events[0].Sequence != sink.events[1].Sequence || !proto.Equal(sink.events[0].Message, sink.events[1].Message)) {
				t.Fatal("unstable duplicate")
			}
			s, _ := restarted.load(ctx)
			if len(s.Outbox) != 0 || len(s.Inbox) != 1 {
				t.Fatal("lost receipt or unremoved export")
			}
			receipt, err := restarted.HandleNotify(ctx, valueNotification("n", *now))
			if err != nil || !receipt.Duplicate {
				t.Fatal(receipt, err)
			}
		})
	}
}
func operateResponse(mid string, async bool) *usp.Msg {
	result := &usp.OperateResp_OperationResult{ExecutedCommand: "Device.Reboot()", OperationResp: &usp.OperateResp_OperationResult_ReqOutputArgs{ReqOutputArgs: &usp.OperateResp_OperationResult_OutputArgs{}}}
	if async {
		result.OperationResp = &usp.OperateResp_OperationResult_ReqObjPath{ReqObjPath: "Device.LocalAgent.Request.1."}
	}
	return &usp.Msg{Header: &usp.Header{MsgId: mid, MsgType: usp.Header_OPERATE_RESP}, Body: &usp.Body{MsgBody: &usp.Body_Response{Response: &usp.Response{RespType: &usp.Response_OperateResp{OperateResp: &usp.OperateResp{OperationResults: []*usp.OperateResp_OperationResult{result}}}}}}}
}
func TestOperationCompletionOrderingAndFailures(t *testing.T) {
	for _, order := range []string{"before-response", "after-response", "late", "failure", "sync"} {
		t.Run(order, func(t *testing.T) {
			a, _, f, now := executable(t)
			r := command("op", now.Add(time.Minute))
			r.Request = &usp.Request{ReqType: &usp.Request_Operate{Operate: &usp.Operate{Command: "Device.Reboot()", CommandKey: "caller-key"}}}
			if _, err := a.EnqueueCommand(ctx, r); err != nil {
				t.Fatal(err)
			}
			if err := a.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			c, _ := a.GetCommand(ctx, "op")
			sent := f.calls[0].Message.GetBody().GetRequest().GetOperate()
			if c.OperationKey == "" || sent.CommandKey != c.OperationKey || !sent.SendResp || sent.CommandKey == "caller-key" {
				t.Fatal(sent, c)
			}
			response := device.DeviceResponse{Identity: identity, Message: operateResponse(c.MessageID, order != "sync")}
			if order != "before-response" {
				if err := a.HandleResponse(ctx, response); err != nil {
					t.Fatal(err)
				}
				c, _ = a.GetCommand(ctx, "op")
				if order == "sync" {
					if c.State != device.CommandSucceeded {
						t.Fatal(c)
					}
					return
				}
				if c.State != device.CommandAwaitingCompletion {
					t.Fatal(c)
				}
				if err := a.Tick(ctx); err != nil || len(f.calls) != 1 {
					t.Fatal(err)
				}
			}
			if order == "late" {
				*now = now.Add(2 * time.Minute)
				if err := a.Tick(ctx); err != nil {
					t.Fatal(err)
				}
				c, _ = a.GetCommand(ctx, "op")
				if c.State != device.CommandUnknown {
					t.Fatal(c)
				}
			}
			complete := &usp.Notify_OperationComplete{ObjPath: "Device.", CommandName: "Reboot()", CommandKey: c.OperationKey, OperationResp: &usp.Notify_OperationComplete_ReqOutputArgs{ReqOutputArgs: &usp.Notify_OperationComplete_OutputArgs{}}}
			want := device.CommandSucceeded
			if order == "failure" {
				want = device.CommandFailed
				complete.OperationResp = &usp.Notify_OperationComplete_CmdFailure{CmdFailure: &usp.Notify_OperationComplete_CommandFailure{ErrCode: 7000}}
			}
			n := notification("complete", &usp.Notify{Notification: &usp.Notify_OperComplete{OperComplete: complete}}, *now)
			wrong := proto.Clone(n.Message).(*usp.Msg)
			wrong.GetBody().GetRequest().GetNotify().GetOperComplete().CommandName = "Other()"
			bad := n
			bad.Message = wrong
			if _, err := a.HandleNotify(ctx, bad); !errors.Is(err, ErrConflict) {
				t.Fatal(err)
			}
			if _, err := a.HandleNotify(ctx, n); err != nil {
				t.Fatal(err)
			}
			if _, err := a.HandleNotify(ctx, n); err != nil {
				t.Fatal(err)
			}
			if err := a.HandleResponse(ctx, response); err != nil {
				t.Fatal(err)
			}
			c, err := a.GetCommand(ctx, "op")
			if err != nil || c.State != want || c.Completion == nil || c.Result == nil {
				t.Fatal(c, err)
			}
			s, _ := a.load(ctx)
			if len(s.Outbox) != 1 {
				t.Fatal("duplicate completion export")
			}
		})
	}
}
func TestDaprEventPublisherContract(t *testing.T) {
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" || r.URL.Path != "/v1.0/publish/events/device-events" || r.URL.Query().Get("metadata.rawPayload") != "true" || r.Header.Get("dapr-api-token") != "token" {
			t.Error("invalid publish request", r.URL)
		}
		b, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(b))
		if len(bodies) == 1 {
			w.WriteHeader(500)
		} else {
			w.WriteHeader(204)
		}
	}))
	defer server.Close()
	p := &DaprEventPublisher{Endpoint: server.URL, Token: "token", Pubsub: "events", Topic: "device-events"}
	e := device.DeviceEvent{Identity: identity, Epoch: "epoch", Sequence: 1, SchemaVersion: "1", ReceivedAt: time.Unix(1, 0), Message: valueNotification("n", time.Now()).Message}
	if err := p.Publish(ctx, e); err == nil {
		t.Fatal("accepted failed broker handoff")
	}
	if err := p.Publish(ctx, e); err != nil {
		t.Fatal(err)
	}
	if bodies[0] != bodies[1] {
		t.Fatal("changed retry envelope")
	}
}

func TestSchemaOneUpgradeAndOutboxIntegrity(t *testing.T) {
	a, b, _, now := executable(t)
	if _, err := a.EnqueueCommand(ctx, command("old", now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	var state map[string]json.RawMessage
	if err := json.Unmarshal(b.state, &state); err != nil {
		t.Fatal(err)
	}
	state["Schema"] = json.RawMessage(`1`)
	delete(state, "Inbox")
	delete(state, "Outbox")
	delete(state, "EventSequence")
	b.state, _ = json.Marshal(state)
	if _, err := a.HandleNotify(ctx, valueNotification("new", *now)); err != nil {
		t.Fatal(err)
	}
	s, err := a.load(ctx)
	if err != nil || s.Schema != 2 || len(s.Commands) != 1 || len(s.Outbox) != 1 {
		t.Fatal(s, err)
	}
	delete(s.Inbox, "new")
	b.state, _ = json.Marshal(s)
	if _, err = a.GetTwin(ctx); err == nil {
		t.Fatal("accepted orphan outbox")
	}
}

func TestOperateRejectsAmbiguousTargets(t *testing.T) {
	a, _, _, now := executable(t)
	for _, path := range []string{"Device.Test.*.Run()", "Device.Test.[Alias==x].Run()", "Device.", "Device.A().B()"} {
		r := command("op", now.Add(time.Hour))
		r.Request = &usp.Request{ReqType: &usp.Request_Operate{Operate: &usp.Operate{Command: path}}}
		if _, err := a.EnqueueCommand(ctx, r); !errors.Is(err, ErrInvalid) {
			t.Fatal(path, err)
		}
	}
}
