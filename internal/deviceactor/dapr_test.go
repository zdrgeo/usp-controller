package deviceactor

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

func TestDaprTransactionsRemindersAndReactivation(t *testing.T) {
	var mu sync.Mutex
	var state []byte
	registered := false
	registrations := 0
	lostCommit := true
	prefix := "/v1.0/actors/DeviceActor/" + ActorID(identity)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("dapr-api-token") != "sidecar-token" {
			t.Error("missing API token")
			w.WriteHeader(401)
			return
		}
		switch {
		case r.Method == "GET" && r.URL.Path == prefix+"/state/"+StateKey:
			if state == nil {
				w.WriteHeader(204)
			} else {
				w.Header().Set("Content-Type", "application/json")
				w.Write(state)
			}
		case r.Method == "GET" && r.URL.Path == prefix+"/reminders":
			if registered {
				w.Write([]byte(`{"reminders":[{"name":"maintenance-v1","period":"@every 1m0s"}]}`))
			} else {
				w.Write([]byte(`{"reminders":[]}`))
			}
		case r.Method == "POST" && r.URL.Path == prefix+"/reminders/"+ReminderName:
			var body map[string]string
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			if body["period"] != "1m0s" || body["ttl"] != "" {
				t.Error(body)
			}
			due, err := time.ParseDuration(body["dueTime"])
			if err != nil || due <= 0 || due > time.Minute {
				t.Error("invalid staggering", body)
			}
			registered = true
			registrations++
			w.WriteHeader(204)
		case r.Method == "POST" && r.URL.Path == prefix+"/state":
			if !registered {
				t.Error("save before scheduling")
			}
			var ops []struct {
				Operation string `json:"operation"`
				Request   struct {
					Key   string          `json:"key"`
					Value json.RawMessage `json:"value"`
				} `json:"request"`
			}
			if err := json.NewDecoder(r.Body).Decode(&ops); err != nil {
				t.Error(err)
			}
			if len(ops) != 1 || ops[0].Operation != "upsert" || ops[0].Request.Key != StateKey {
				t.Error("not an atomic snapshot transaction", ops)
				w.WriteHeader(500)
				return
			}
			state = append([]byte(nil), ops[0].Request.Value...)
			if lostCommit {
				lostCommit = false
				w.WriteHeader(500)
			} else {
				w.WriteHeader(204)
			}
		default:
			t.Error("unexpected request", r.Method, r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	backend, err := NewDaprBackend(server.URL, server.Client(), "sidecar-token", identity, time.Minute, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	a, err := New(identity, backend, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	request := desired("durable", 0)
	if _, err = a.SetDesired(ctx, request); err == nil {
		t.Fatal("expected ambiguous commit")
	}
	a, err = New(identity, backend, DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := a.SetDesired(ctx, request)
	if err != nil || receipt.Revision != 1 {
		t.Fatal(receipt, err)
	}
	if registrations != 1 {
		t.Fatal("retry reset reminder")
	}
	mu.Lock()
	registered = false
	mu.Unlock()
	if err = a.Activate(ctx); err != nil {
		t.Fatal(err)
	}
	if registrations != 2 {
		t.Fatal("missing reminder not repaired")
	}
}
func TestDaprFailsClosedOnErrors(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
	}{
		{"backend unavailable", 503, ""}, {"malformed", 200, "{"}, {"wrong period", 200, `{"reminders":[{"name":"maintenance-v1","period":"@every 2m"}]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			posts := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "POST" {
					posts++
				}
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			}))
			defer server.Close()
			b, _ := NewDaprBackend(server.URL, server.Client(), "", identity, time.Minute, 1024)
			if err := b.EnsureReminder(ctx); err == nil {
				t.Fatal("accepted failed scheduler")
			}
			if posts != 0 {
				t.Fatal("overwrote uncertain reminder")
			}
		})
	}
}
func TestHostOnlyExposesShadowMethods(t *testing.T) {
	a, _, _ := fixture(t)
	token := strings.Repeat("s", 32)
	h, err := NewHost([]*Actor{a}, token, 4096)
	if err != nil {
		t.Fatal(err)
	}
	call := func(method, path, body, auth string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("dapr-api-token", auth)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		return w
	}
	if w := call("GET", "/dapr/config", "", "wrong"); w.Code != 401 {
		t.Fatal(w.Code)
	}
	if w := call("GET", "/dapr/config", "", token); w.Code != 200 || !bytes.Contains(w.Body.Bytes(), []byte(`"enabled":false`)) {
		t.Fatal(w.Body.String())
	}
	path := "/actors/DeviceActor/" + ActorID(identity)
	body, _ := json.Marshal(desired("one", 0))
	if w := call("PUT", path+"/method/SetDesired", string(body), token); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call("DELETE", path, "", token); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w := call("PUT", path+"/method/GetTwin", "", token); w.Code != 200 || !strings.Contains(w.Body.String(), `"DesiredRevision":1`) {
		t.Fatal(w.Body.String())
	}
	if w := call("PUT", path+"/method/remind/"+ReminderName, `{}`, token); w.Code != 200 {
		t.Fatal(w.Code)
	}
	for _, method := range []string{"EnqueueCommand", "HandleNotify", "DispatchNext"} {
		if w := call("PUT", path+"/method/"+method, `{}`, token); w.Code != 404 {
			t.Fatal("unexpected exposed method", method, w.Code)
		}
	}
	if w := call("PUT", path+"/method/SetDesired", string(body)+` {}`, token); w.Code != 400 {
		t.Fatal("accepted trailing JSON")
	}
	if w := call("PUT", "/actors/DeviceActor/unprovisioned/method/GetTwin", "", token); w.Code != 404 {
		t.Fatal("unprovisioned actor admitted")
	}
}
func TestSeparateDevicesAndCorruptState(t *testing.T) {
	a, b, _ := fixture(t)
	if _, err := a.SetDesired(ctx, desired("one", 0)); err != nil {
		t.Fatal(err)
	}
	other := identity
	other.AgentID = "other"
	wrong, _ := New(other, b, a.opts)
	if _, err := wrong.GetTwin(context.Background()); err == nil {
		t.Fatal("cross-device state accepted")
	}
	b.state = []byte(`{"Schema":999}`)
	if _, err := a.GetTwin(ctx); err == nil {
		t.Fatal("future schema accepted")
	}
	fresh := &memoryBackend{}
	isolated, _ := New(other, fresh, a.opts)
	twin, err := isolated.GetTwin(ctx)
	if err != nil || len(twin.Desired) != 0 {
		t.Fatal("device isolation", err)
	}
}

var _ device.DeviceActor = (*Actor)(nil)
