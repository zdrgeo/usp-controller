package canary

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"example.com/usp-controller/internal/config"
	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

func TestReplicaReadinessAndDrain(t *testing.T) {
	good := `{"id":"site","components":[{"name":"actors","type":"state.redis","capabilities":["ETAG","TRANSACTIONAL","ACTOR"]},{"name":"events","type":"pubsub.redis"}],"scheduler":{"connected_addresses":["scheduler:50006"]},"appConnectionProperties":{"health":{"healthCheckPath":"/healthz"}}}`
	metadata := good
	failState := false
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("dapr-api-token") != "secret" {
			t.Error("missing sidecar token")
		}
		if r.URL.Path == "/v1.0/metadata" {
			w.Write([]byte(metadata))
			return
		}
		if !strings.HasSuffix(r.URL.Path, "/method/Activate") {
			t.Error("unexpected probe", r.URL.Path)
		}
		if failState {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(200)
	}))
	defer sidecar.Close()
	rt := &Runtime{replica: &config.Replica{AppID: "site", StateStore: "actors"}, eventPubsub: "events", Client: &deviceactor.Client{Endpoint: sidecar.URL, Token: "secret"}, Devices: map[string]device.DeviceIdentity{"d": {Tenant: "t", ControllerID: "c", AgentID: "d"}}, mqtt: &mqttFake{ready: true}}
	if rt.Ready() {
		t.Fatal("ready before dependency check")
	}
	if err := rt.RefreshPlacement(context.Background()); err != nil || !rt.Ready() {
		t.Fatal(err)
	}
	for _, bad := range []string{strings.Replace(good, `"id":"site"`, `"id":"other"`, 1), strings.Replace(good, `"ETAG",`, "", 1), strings.Replace(good, `"scheduler:50006"`, "", 1), strings.Replace(good, `"/healthz"`, `"/wrong"`, 1)} {
		metadata = bad
		if err := rt.RefreshPlacement(context.Background()); err == nil || rt.Ready() {
			t.Fatal("bad dependency accepted", err)
		}
	}
	metadata = good
	failState = true
	if err := rt.RefreshPlacement(context.Background()); err == nil || rt.Ready() {
		t.Fatal("partition accepted", err)
	}
	failState = false
	if err := rt.RefreshPlacement(context.Background()); err != nil {
		t.Fatal(err)
	}
	rt.verifiedUntil.Store(time.Now().Add(-time.Second).UnixNano())
	if rt.Ready() {
		t.Fatal("stale probe kept ready")
	}
	h := rt.callbackGate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	call := func(method, path string) int {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
		return w.Code
	}
	if call("PUT", "/actors/DeviceActor/d/method/Tick") != 503 || call("PUT", "/actors/DeviceActor/d/method/Activate") != 200 {
		t.Fatal("probe/dispatch gate")
	}
	if err := rt.RefreshPlacement(context.Background()); err != nil {
		t.Fatal(err)
	}
	rt.BeginDrain()
	if rt.Ready() || call("GET", "/healthz") != 503 || call("PUT", "/actors/DeviceActor/d/method/HandleNotify") != 503 || call("DELETE", "/actors/DeviceActor/d") != 200 {
		t.Fatal("drain gate")
	}
	if err := rt.RefreshPlacement(context.Background()); err == nil || rt.DispatchReady() {
		t.Fatal("resurrected draining replica")
	}
	w := httptest.NewRecorder()
	rt.AdmissionGate(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Fatal("admitted during drain") })).ServeHTTP(w, httptest.NewRequest("POST", "/v2/devices/d/commands", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
}

// Metadata is a capability check, not evidence of common backend endpoints.
func TestReplicaProbeRejectsRedirect(t *testing.T) {
	calls := 0
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++ }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer redirect.Close()
	rt := &Runtime{replica: &config.Replica{}, Client: &deviceactor.Client{Endpoint: redirect.URL, Token: "secret"}}
	if err := rt.RefreshPlacement(context.Background()); err == nil || calls != 0 {
		t.Fatal(err, calls)
	}
}
