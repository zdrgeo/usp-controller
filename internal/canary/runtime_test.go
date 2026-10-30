package canary

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/usp-controller/internal/api"
	"example.com/usp-controller/internal/config"
	"example.com/usp-controller/internal/controller"
	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
	"example.com/usp-controller/internal/deviceapi"
	"example.com/usp-controller/internal/pb/record"
	"example.com/usp-controller/internal/pb/usp"
	"example.com/usp-controller/internal/protocol"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type mqttFake struct {
	mu    sync.Mutex
	sends [][]byte
	ready bool
}

func (m *mqttFake) Ready() bool { return m.ready }
func (m *mqttFake) Publish(_ context.Context, _ string, b []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sends = append(m.sends, append([]byte(nil), b...))
	return nil
}

type journalFake struct{ count int }

func (j *journalFake) Append(string, []byte) error { j.count++; return nil }

func TestCanaryAsyncEndToEnd(t *testing.T) {
	ctx := context.Background()
	appToken := strings.Repeat("p", 32)
	apiToken := strings.Repeat("a", 32)
	var runtime *Runtime
	var mu sync.Mutex
	states := map[string]json.RawMessage{}
	reminders := map[string]bool{}
	failSave := false
	exports := 0
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "/method/") {
			clone := r.Clone(r.Context())
			u := *r.URL
			clone.URL = &u
			clone.URL.Path = strings.TrimPrefix(u.Path, "/v1.0")
			clone.Method = http.MethodPut
			clone.Header = clone.Header.Clone()
			clone.Header.Set("dapr-api-token", appToken)
			runtime.Host.ServeHTTP(w, clone)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if strings.HasPrefix(r.URL.Path, "/v1.0/publish/") {
			exports++
			w.WriteHeader(204)
			return
		}
		parts := strings.Split(r.URL.Path, "/")
		id := parts[4]
		switch {
		case strings.HasSuffix(r.URL.Path, "/state/"+deviceactor.StateKey):
			if b := states[id]; b != nil {
				w.Write(b)
			} else {
				w.WriteHeader(204)
			}
		case strings.HasSuffix(r.URL.Path, "/state"):
			if failSave {
				w.WriteHeader(500)
				return
			}
			var ops []struct {
				Request struct {
					Value json.RawMessage `json:"value"`
				} `json:"request"`
			}
			if err := json.NewDecoder(r.Body).Decode(&ops); err != nil {
				t.Error(err)
				w.WriteHeader(500)
				return
			}
			states[id] = append([]byte(nil), ops[0].Request.Value...)
			w.WriteHeader(204)
		case strings.HasSuffix(r.URL.Path, "/reminders"):
			if reminders[id] {
				w.Write([]byte(`{"reminders":[{"name":"maintenance-v1","period":"@every 10s"}]}`))
			} else {
				w.Write([]byte(`{"reminders":[]}`))
			}
		case strings.HasSuffix(r.URL.Path, "/reminders/maintenance-v1"):
			reminders[id] = true
			w.WriteHeader(204)
		default:
			t.Error("unexpected sidecar path", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer sidecar.Close()
	cfg := config.Config{EndpointID: "controller", ClientID: "client", Listen: "127.0.0.1:8080", Broker: "tls://localhost:8883", JournalPath: "events", JournalMaxBytes: 1 << 20, RequestTimeoutSeconds: 1, MaxPending: 10, QoS: 1, Agents: map[string]config.Agent{"device": {Topic: "device/topic", Inbox: "controller/device"}, "legacy": {Topic: "legacy/topic", Inbox: "controller/legacy"}}, ActorCanary: &config.ActorCanary{Enabled: true, EventPubsub: "events", EventTopic: "device-events", Tenant: "tenant", Devices: []string{"device"}, DaprURL: sidecar.URL, CallbackListen: "127.0.0.1:8081", LockPath: filepath.Join(t.TempDir(), "canary.lock")}}
	mqtt := &mqttFake{ready: true}
	journal := &journalFake{}
	legacy := controller.New(cfg.EndpointID, map[string]string{"device": "device/topic", "legacy": "legacy/topic"}, mqtt, journal, 10)
	var err error
	runtime, err = New(cfg, mqtt, legacy, appToken, "")
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close()
	if duplicate, err := New(cfg, mqtt, legacy, appToken, ""); err == nil {
		duplicate.Close()
		t.Fatal("second owner acquired lock")
	}
	if _, err = runtime.Request(ctx, "device", &usp.Request{ReqType: &usp.Request_Get{Get: &usp.Get{}}}); !errors.Is(err, controller.ErrActorManaged) {
		t.Fatal("legacy fallback", err)
	}
	h := deviceapi.Handler(api.Handler(runtime, func() bool { return true }, apiToken, time.Second), runtime.Client, runtime.Devices, apiToken)
	call := func(method, path string, body []byte, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	request := deviceactor.CommandInput{RequestID: "cmd-1", Deadline: time.Now().Add(time.Minute).UTC(), Request: json.RawMessage(`{"get":{"paramPaths":["Device.DeviceInfo."]}}`)}
	body, _ := json.Marshal(request)
	if w := call("POST", "/v2/devices/device/commands", body, "wrong"); w.Code != 401 {
		t.Fatal(w.Code)
	}
	if w := call("POST", "/v2/devices/legacy/commands", body, apiToken); w.Code != 404 {
		t.Fatal("allowlist bypass", w.Code)
	}
	if w := call("POST", "/v1/requests", []byte(`{"agent_id":"device","request":{"get":{}}}`), apiToken); w.Code != 409 {
		t.Fatal("legacy API bypass", w.Code)
	}
	mqtt.ready = false
	for i := 0; i < 2; i++ {
		w := call("POST", "/v2/devices/device/commands", body, apiToken)
		if w.Code != 202 || w.Header().Get("Location") != "/v2/devices/device/commands/cmd-1" {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if len(mqtt.sends) != 0 {
		t.Fatal("admission sent MQTT")
	}
	if _, err = runtime.Client.Invoke(ctx, runtime.Devices["device"], "Tick", nil); err != nil {
		t.Fatal(err)
	}
	if len(mqtt.sends) != 0 {
		t.Fatal("offline dispatch")
	}
	mqtt.ready = true
	if _, err = runtime.Client.Invoke(ctx, runtime.Devices["device"], "Tick", nil); err != nil {
		t.Fatal(err)
	}
	if len(mqtt.sends) != 1 {
		t.Fatal("no dispatch")
	}
	rec := new(record.Record)
	if err = proto.Unmarshal(mqtt.sends[0], rec); err != nil {
		t.Fatal(err)
	}
	out := new(usp.Msg)
	proto.Unmarshal(rec.GetNoSessionContext().Payload, out)
	response := &usp.Msg{Header: &usp.Header{MsgId: out.Header.MsgId, MsgType: usp.Header_GET_RESP}, Body: &usp.Body{MsgBody: &usp.Body_Response{Response: &usp.Response{RespType: &usp.Response_GetResp{GetResp: &usp.GetResp{}}}}}}
	wire, _ := protocol.Encode("device", "controller", response)
	if err = runtime.Receive(ctx, wire, "device/topic", "device"); err != nil {
		t.Fatal(err)
	}
	w := call("GET", "/v2/devices/device/commands/cmd-1", nil, apiToken)
	var status deviceactor.CommandOutput
	if err = json.Unmarshal(w.Body.Bytes(), &status); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || status.State != device.CommandSucceeded {
		t.Fatal(w.Code, w.Body.String())
	}
	returned := new(usp.Msg)
	if err = protojson.Unmarshal(status.Result, returned); err != nil || !proto.Equal(returned, response) {
		t.Fatal("protobuf result", err)
	}
	if _, err = runtime.Client.Invoke(ctx, runtime.Devices["device"], "Tick", nil); err != nil {
		t.Fatal(err)
	}
	if len(mqtt.sends) != 1 {
		t.Fatal("duplicate dispatch")
	}
	// A second command can be cancelled without publishing; malformed bodies and
	// changed retry content never become another queued command.
	request.RequestID = "cmd-2"
	body, _ = json.Marshal(request)
	if w = call("POST", "/v2/devices/device/commands", body, apiToken); w.Code != 202 {
		t.Fatal(w.Body.String())
	}
	w = call("POST", "/v2/devices/device/commands/cmd-2/cancel", []byte(`{"expected_revision":1}`), apiToken)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	request.RequestID = "cmd-1"
	request.Request = json.RawMessage(`{"get":{}}`)
	body, _ = json.Marshal(request)
	if w = call("POST", "/v2/devices/device/commands", body, apiToken); w.Code != 409 {
		t.Fatal("conflicting retry", w.Code)
	}
	if w = call("POST", "/v2/devices/device/commands", append(body, []byte(` {}`)...), apiToken); w.Code != 400 {
		t.Fatal("trailing JSON", w.Code)
	}
	// Canary notifications commit actor state before acknowledgement, bypassing Journal.
	notify := &usp.Msg{Header: &usp.Header{MsgId: "n", MsgType: usp.Header_NOTIFY}, Body: &usp.Body{MsgBody: &usp.Body_Request{Request: &usp.Request{ReqType: &usp.Request_Notify{Notify: &usp.Notify{SubscriptionId: "sub", SendResp: true, Notification: &usp.Notify_ValueChange_{ValueChange: &usp.Notify_ValueChange{ParamPath: "Device.X", ParamValue: "v"}}}}}}}}
	wire, _ = protocol.Encode("device", "controller", notify)
	mu.Lock()
	failSave = true
	mu.Unlock()
	if err = runtime.Receive(ctx, wire, "device/topic", "device"); err == nil || len(mqtt.sends) != 1 {
		t.Fatal("acknowledged failed notification commit", err)
	}
	mu.Lock()
	failSave = false
	mu.Unlock()
	if err = runtime.Receive(ctx, wire, "device/topic", "device"); err != nil || journal.count != 0 {
		t.Fatal("actor notification route", err)
	}
	if len(mqtt.sends) != 2 {
		t.Fatal("missing NotifyResp")
	}

	if _, err = runtime.Client.Invoke(ctx, runtime.Devices["device"], "Tick", nil); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	count := exports
	mu.Unlock()
	if count != 1 {
		t.Fatal("outbox not exported", count)
	}
	if err = runtime.Receive(ctx, wire, "device/topic", "device"); err != nil {
		t.Fatal(err)
	}
	if _, err = runtime.Client.Invoke(ctx, runtime.Devices["device"], "Tick", nil); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	count = exports
	mu.Unlock()
	if count != 1 || len(mqtt.sends) != 3 {
		t.Fatal("duplicate receipt failed", count, len(mqtt.sends))
	}
	// Non-cohort notifications still go through Journal.
	wire, _ = protocol.Encode("legacy", "controller", notify)
	if err = runtime.Receive(ctx, wire, "legacy/topic", "legacy"); err != nil || journal.count != 1 {
		t.Fatal("legacy journal changed", err)
	}
	legacyCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	_, err = runtime.Request(legacyCtx, "legacy", &usp.Request{ReqType: &usp.Request_Get{Get: &usp.Get{}}})
	if !errors.Is(err, context.DeadlineExceeded) || len(mqtt.sends) != 5 {
		t.Fatal("non-cohort legacy path changed", err)
	}
}

func TestClientDoesNotFollowRedirect(t *testing.T) {
	targetCalls := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetCalls++; io.WriteString(w, `{}`) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer redirect.Close()
	client := deviceactor.Client{Endpoint: redirect.URL, Token: "secret"}
	_, err := client.Invoke(context.Background(), device.DeviceIdentity{Tenant: "t", ControllerID: "c", AgentID: "d"}, "GetTwin", nil)
	if err == nil || targetCalls != 0 {
		t.Fatal("redirect followed", err, targetCalls)
	}
}
