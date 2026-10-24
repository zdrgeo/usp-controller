package controller

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"example.com/usp-controller/internal/pb/record"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
)

type sent struct {
	topic string
	data  []byte
}
type fakeTransport struct{ sends chan sent }

func (f *fakeTransport) Ready() bool { return true }
func (f *fakeTransport) Publish(ctx context.Context, topic string, b []byte) error {
	select {
	case f.sends <- sent{topic, b}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type fakeJournal struct {
	mu    sync.Mutex
	count int
	err   error
}

func (j *fakeJournal) Append(_ string, _ []byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.err != nil {
		return j.err
	}
	j.count++
	return nil
}
func fixture() (*Controller, *fakeTransport, *fakeJournal) {
	f := &fakeTransport{sends: make(chan sent, 20)}
	j := new(fakeJournal)
	return New("controller", map[string]string{"agent": "agent/topic", "other": "other/topic"}, f, j, 2), f, j
}
func get() *usp.Request {
	return &usp.Request{ReqType: &usp.Request_Get{Get: &usp.Get{ParamPaths: []string{"Device.DeviceInfo."}}}}
}
func decode(t *testing.T, b []byte) *usp.Msg {
	t.Helper()
	r := new(record.Record)
	if err := proto.Unmarshal(b, r); err != nil {
		t.Fatal(err)
	}
	m := new(usp.Msg)
	if err := proto.Unmarshal(r.GetNoSessionContext().Payload, m); err != nil {
		t.Fatal(err)
	}
	return m
}
func reply(id string) *usp.Msg {
	return &usp.Msg{Header: &usp.Header{MsgId: id, MsgType: usp.Header_GET_RESP}, Body: &usp.Body{MsgBody: &usp.Body_Response{Response: &usp.Response{RespType: &usp.Response_GetResp{GetResp: &usp.GetResp{}}}}}}
}
func deliver(t *testing.T, c *Controller, from, topic, sender string, m *usp.Msg) error {
	t.Helper()
	b, err := Encode(from, "controller", m)
	if err != nil {
		t.Fatal(err)
	}
	return c.Receive(context.Background(), b, topic, sender)
}

func TestCorrelationRejectsSpoofedAndWrongResponses(t *testing.T) {
	c, f, _ := fixture()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := c.Request(ctx, "agent", get()); result <- err }()
	outgoing := <-f.sends
	m := decode(t, outgoing.data)
	if outgoing.topic != "agent/topic" || m.Header.MsgType != usp.Header_GET {
		t.Fatal("wrong outgoing request")
	}
	response := reply(m.Header.MsgId)
	if err := deliver(t, c, "agent", "agent/topic", "other", response); err == nil {
		t.Fatal("accepted wrong inbox")
	}
	if err := deliver(t, c, "other", "other/topic", "other", response); err == nil {
		t.Fatal("accepted other agent's response")
	}
	response.Header.MsgType = usp.Header_SET_RESP
	if err := deliver(t, c, "agent", "agent/topic", "agent", response); err == nil {
		t.Fatal("accepted header/body mismatch")
	}
	response.Header.MsgType = usp.Header_GET_RESP
	if err := deliver(t, c, "agent", "other/topic", "agent", response); err == nil {
		t.Fatal("accepted arbitrary reply topic")
	}
	if err := deliver(t, c, "agent", "agent/topic", "agent", response); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := deliver(t, c, "agent", "agent/topic", "agent", response); err != nil {
		t.Fatal("duplicate response should be harmless", err)
	}
}

func TestTimeoutCleansPendingAndDoesNotRetry(t *testing.T) {
	c, f, _ := fixture()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := c.Request(ctx, "agent", get()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v", err)
	}
	if len(f.sends) != 1 || len(c.pending) != 0 {
		t.Fatal("request leaked or retried")
	}
}

func TestNotificationPersistedBeforeAcknowledgment(t *testing.T) {
	c, f, j := fixture()
	msg := &usp.Msg{Header: &usp.Header{MsgId: "notify-1", MsgType: usp.Header_NOTIFY}, Body: &usp.Body{MsgBody: &usp.Body_Request{Request: &usp.Request{ReqType: &usp.Request_Notify{Notify: &usp.Notify{SubscriptionId: "sub", SendResp: true, Notification: &usp.Notify_ValueChange_{ValueChange: &usp.Notify_ValueChange{ParamPath: "Device.DeviceInfo.SoftwareVersion", ParamValue: "1"}}}}}}}}
	j.err = errors.New("disk full")
	if err := deliver(t, c, "agent", "agent/topic", "agent", msg); err == nil {
		t.Fatal("ignored persistence failure")
	}
	if len(f.sends) != 0 {
		t.Fatal("acknowledged before persistence")
	}
	j.err = nil
	if err := deliver(t, c, "agent", "agent/topic", "agent", msg); err != nil {
		t.Fatal(err)
	}
	ack := decode(t, (<-f.sends).data)
	if j.count != 1 || ack.Header.MsgId != "notify-1" || ack.Header.MsgType != usp.Header_NOTIFY_RESP || ack.Body.GetResponse().GetNotifyResp().SubscriptionId != "sub" {
		t.Fatal("bad notification acknowledgement")
	}
}

func TestUnsupportedRecordContext(t *testing.T) {
	c, _, _ := fixture()
	r := &record.Record{Version: "1.5", FromId: "agent", ToId: "controller", RecordType: &record.Record_SessionContext{SessionContext: &record.SessionContextRecord{}}}
	b, _ := proto.Marshal(r)
	if err := c.Receive(context.Background(), b, "agent/topic", "agent"); err == nil {
		t.Fatal("accepted unsupported session context")
	}
}

func TestSupportedRequestTypes(t *testing.T) {
	cases := []struct {
		req  *usp.Request
		kind usp.Header_MsgType
	}{
		{get(), usp.Header_GET},
		{&usp.Request{ReqType: &usp.Request_Set{Set: &usp.Set{}}}, usp.Header_SET},
		{&usp.Request{ReqType: &usp.Request_Add{Add: &usp.Add{}}}, usp.Header_ADD},
		{&usp.Request{ReqType: &usp.Request_Delete{Delete: &usp.Delete{}}}, usp.Header_DELETE},
		{&usp.Request{ReqType: &usp.Request_Operate{Operate: &usp.Operate{}}}, usp.Header_OPERATE},
		{&usp.Request{ReqType: &usp.Request_GetInstances{GetInstances: &usp.GetInstances{}}}, usp.Header_GET_INSTANCES},
		{&usp.Request{ReqType: &usp.Request_GetSupportedDm{GetSupportedDm: &usp.GetSupportedDM{}}}, usp.Header_GET_SUPPORTED_DM},
		{&usp.Request{ReqType: &usp.Request_GetSupportedProtocol{GetSupportedProtocol: &usp.GetSupportedProtocol{}}}, usp.Header_GET_SUPPORTED_PROTO},
	}
	for _, tc := range cases {
		got, err := RequestType(tc.req)
		if err != nil || got != tc.kind {
			t.Fatalf("%v: got %v %v", tc.kind, got, err)
		}
	}
}
