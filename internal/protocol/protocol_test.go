package protocol

import (
	"example.com/usp-controller/internal/pb/record"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestDecodeWireBoundary(t *testing.T) {
	msg := &usp.Msg{Header: &usp.Header{MsgId: "reply", MsgType: usp.Header_GET_RESP}, Body: &usp.Body{MsgBody: &usp.Body_Response{Response: &usp.Response{RespType: &usp.Response_GetResp{GetResp: &usp.GetResp{}}}}}}
	payload, err := Encode("device", "controller", msg)
	if err != nil {
		t.Fatal(err)
	}
	base := new(record.Record)
	if err := proto.Unmarshal(payload, base); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name   string
		change func(*record.Record)
		reject bool
	}{
		{"response", func(*record.Record) {}, false},
		{"wrong recipient", func(r *record.Record) { r.ToId = "other" }, true},
		{"unknown sender", func(r *record.Record) { r.FromId = "other" }, true},
		{"old version", func(r *record.Record) { r.Version = "1.4" }, true},
		{"forwarded", func(r *record.Record) { r.OriginatorId = "other" }, true},
		{"signature", func(r *record.Record) { r.MacSignature = []byte{1} }, true},
		{"session", func(r *record.Record) {
			r.RecordType = &record.Record_SessionContext{SessionContext: &record.SessionContextRecord{}}
		}, true},
		{"connect", func(r *record.Record) {
			r.RecordType = &record.Record_MqttConnect{MqttConnect: &record.MQTTConnectRecord{Version: record.MQTTConnectRecord_V5, SubscribedTopic: "device/topic"}}
		}, false},
		{"wrong connect topic", func(r *record.Record) {
			r.RecordType = &record.Record_MqttConnect{MqttConnect: &record.MQTTConnectRecord{Version: record.MQTTConnectRecord_V5, SubscribedTopic: "other"}}
		}, true},
		{"disconnect", func(r *record.Record) {
			r.RecordType = &record.Record_Disconnect{Disconnect: &record.DisconnectRecord{}}
		}, false},
		{"malformed message", func(r *record.Record) { r.GetNoSessionContext().Payload = []byte{0xff} }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := proto.Clone(base).(*record.Record)
			tc.change(r)
			b, err := proto.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			got, err := Decode(b, "device/topic", "device", "controller", map[string]string{"device": "device/topic"})
			if (err != nil) != tc.reject {
				t.Fatalf("reject=%v, got %v", tc.reject, err)
			}
			if err == nil {
				if got.Sender != "device" {
					t.Fatal("lost sender")
				}
				if r.GetNoSessionContext() != nil && !proto.Equal(got.Message, msg) {
					t.Fatal("changed message")
				}
				if r.GetNoSessionContext() == nil && got.Message != nil {
					t.Fatal("control record became response")
				}
			}
		})
	}
	for _, b := range [][]byte{{0xff}, make([]byte, MaxRecord+1)} {
		if _, err := Decode(b, "device/topic", "device", "controller", nil); err == nil {
			t.Fatal("accepted malformed or oversized record")
		}
	}
}

func TestDecodeRejectsInvalidMessageShapes(t *testing.T) {
	cases := []*usp.Msg{
		{},
		{Header: &usp.Header{MsgId: "x", MsgType: usp.Header_NOTIFY}, Body: &usp.Body{MsgBody: &usp.Body_Request{Request: &usp.Request{ReqType: &usp.Request_Notify{Notify: &usp.Notify{}}}}}},
		{Header: &usp.Header{MsgId: "x", MsgType: usp.Header_ERROR}, Body: &usp.Body{}},
		{Header: &usp.Header{MsgId: "x", MsgType: usp.Header_SET_RESP}, Body: &usp.Body{MsgBody: &usp.Body_Response{Response: &usp.Response{RespType: &usp.Response_GetResp{GetResp: &usp.GetResp{}}}}}},
	}
	for _, msg := range cases {
		b, err := Encode("device", "controller", msg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Decode(b, "topic", "device", "controller", map[string]string{"device": "topic"}); err == nil {
			t.Fatalf("accepted %v", msg)
		}
	}
}
