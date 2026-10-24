package mqtt

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"example.com/usp-controller/internal/config"
	"example.com/usp-controller/internal/controller"
	"example.com/usp-controller/internal/journal"
	"example.com/usp-controller/internal/pb/record"
	"example.com/usp-controller/internal/pb/usp"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"google.golang.org/protobuf/proto"
)

// Run against an isolated MQTT 5 broker: USP_TEST_MQTT=mqtt://127.0.0.1:18883 go test ./internal/mqtt -v
func TestMQTTRequestAndNotification(t *testing.T) {
	broker := os.Getenv("USP_TEST_MQTT")
	if broker == "" {
		t.Skip("set USP_TEST_MQTT to run against a real broker")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("usp-test/%d", time.Now().UnixNano())
	agentTopic := prefix + "/agent"
	inbox := prefix + "/controller"
	u, err := url.Parse(broker)
	if err != nil {
		t.Fatal(err)
	}
	incoming := make(chan *paho.Publish, 10)
	subscribed := make(chan error, 1)
	agent, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{ServerUrls: []*url.URL{u}, KeepAlive: 30, CleanStartOnInitialConnection: true,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, _ *paho.Connack) {
			go func() {
				ack, err := cm.Subscribe(ctx, &paho.Subscribe{Subscriptions: []paho.SubscribeOptions{{Topic: agentTopic, QoS: 1}}})
				if err == nil && (len(ack.Reasons) != 1 || ack.Reasons[0] > 1) {
					err = fmt.Errorf("agent SUBACK rejected")
				}
				select {
				case subscribed <- err:
				case <-ctx.Done():
				}
			}()
		},
		ClientConfig: paho.ClientConfig{ClientID: prefix + "-agent", OnPublishReceived: []func(paho.PublishReceived) (bool, error){func(p paho.PublishReceived) (bool, error) {
			select {
			case incoming <- p.Packet:
			case <-ctx.Done():
			}
			return true, nil
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		defer closeCancel()
		agent.Disconnect(closeCtx)
	}()
	select {
	case err := <-subscribed:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	path := filepath.Join(t.TempDir(), "notifications.jsonl")
	j, err := journal.Open(path, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	cfg := config.Config{EndpointID: "controller", ClientID: prefix + "-controller", Broker: broker, Agents: map[string]config.Agent{"agent": {Topic: agentTopic, Inbox: inbox}}, QoS: 1, RequestTimeoutSeconds: 5}
	transport := New(cfg)
	c := controller.New("controller", map[string]string{"agent": agentTopic}, transport, j, 10)
	if err := transport.Start(ctx, c.Receive); err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		defer closeCancel()
		transport.Close(closeCtx)
	}()
	for !transport.Ready() {
		select {
		case err := <-transport.Errors():
			t.Fatal(err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(10 * time.Millisecond):
		}
	}
	// Wait for the initial announcement before starting command traffic.
	select {
	case p := <-incoming:
		r := new(record.Record)
		if err := proto.Unmarshal(p.Payload, r); err != nil {
			t.Fatal(err)
		}
		if r.GetMqttConnect() == nil || r.GetMqttConnect().SubscribedTopic != inbox {
			t.Fatal("missing Controller Connect record")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	responses := make(chan error, 1)
	go func() {
		m, err := c.Request(ctx, "agent", &usp.Request{ReqType: &usp.Request_Get{Get: &usp.Get{ParamPaths: []string{"Device.DeviceInfo."}}}})
		if err == nil && m.Header.MsgType != usp.Header_GET_RESP {
			err = fmt.Errorf("wrong response")
		}
		responses <- err
	}()
	var request *usp.Msg
	for request == nil {
		select {
		case p := <-incoming:
			if p.Properties == nil || p.Properties.ContentType != "usp.msg" || p.Properties.ResponseTopic != inbox || p.QoS != 1 {
				t.Fatal("invalid MQTT properties")
			}
			r := new(record.Record)
			if err := proto.Unmarshal(p.Payload, r); err != nil {
				t.Fatal(err)
			}
			if r.GetMqttConnect() != nil {
				continue
			}
			request = new(usp.Msg)
			if err := proto.Unmarshal(r.GetNoSessionContext().Payload, request); err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	publish := func(msg *usp.Msg) {
		t.Helper()
		b, err := controller.Encode("agent", "controller", msg)
		if err != nil {
			t.Fatal(err)
		}
		ack, err := agent.Publish(ctx, &paho.Publish{QoS: 1, Topic: inbox, Payload: b, Properties: &paho.PublishProperties{ContentType: "usp.msg", ResponseTopic: agentTopic}})
		if err != nil {
			t.Fatal(err)
		}
		if ack.ReasonCode >= 0x80 {
			t.Fatal("publish rejected")
		}
	}
	publish(&usp.Msg{Header: &usp.Header{MsgId: request.Header.MsgId, MsgType: usp.Header_GET_RESP}, Body: &usp.Body{MsgBody: &usp.Body_Response{Response: &usp.Response{RespType: &usp.Response_GetResp{GetResp: &usp.GetResp{}}}}}})
	select {
	case err := <-responses:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	publish(&usp.Msg{Header: &usp.Header{MsgId: "notification", MsgType: usp.Header_NOTIFY}, Body: &usp.Body{MsgBody: &usp.Body_Request{Request: &usp.Request{ReqType: &usp.Request_Notify{Notify: &usp.Notify{SubscriptionId: "sub", SendResp: true, Notification: &usp.Notify_ValueChange_{ValueChange: &usp.Notify_ValueChange{ParamPath: "Device.DeviceInfo.SoftwareVersion", ParamValue: "1"}}}}}}}})
	select {
	case p := <-incoming:
		r := new(record.Record)
		if err := proto.Unmarshal(p.Payload, r); err != nil {
			t.Fatal(err)
		}
		m := new(usp.Msg)
		if err := proto.Unmarshal(r.GetNoSessionContext().Payload, m); err != nil {
			t.Fatal(err)
		}
		if m.Header.MsgType != usp.Header_NOTIFY_RESP || m.Header.MsgId != "notification" {
			t.Fatal("missing NotifyResp")
		}
		b, err := os.ReadFile(path)
		if err != nil || len(b) == 0 {
			t.Fatal("notification not persisted", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
