package fleet

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/encoding/protojson"
)

func query(h http.Handler, path, token, site string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("GET", path, nil)
	r.Header.Set("Authorization", "Bearer "+token)
	if site != "" {
		r.Header.Set("X-USP-Site", site)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestFleetQueriesNeverFanOutAndLabelStaleData(t *testing.T) {
	p, _, now := fixture(t)
	refresh(p, "a")
	calls := 0
	p.invoker = invokeFunc(func(context.Context, device.DeviceIdentity, string, any) (json.RawMessage, error) {
		calls++
		t.Error("query fanned out to actor")
		return nil, nil
	})
	token := strings.Repeat("k", 32)
	h := p.Handler(http.NotFoundHandler(), token)
	for _, c := range []struct {
		path, auth, site string
		code             int
	}{
		{"/v2/fleet/devices", "bad", "", 401}, {"/v2/fleet/devices", token, "other", 409}, {"/v2/fleet/devices/missing", token, "site", 404}, {"/v2/fleet/devices?limit=201", token, "site", 400}, {"/v2/fleet/devices?limit=bad", token, "site", 400},
	} {
		if w := query(h, c.path, c.auth, c.site); w.Code != c.code {
			t.Fatal(c, w.Code, w.Body.String())
		}
	}
	w := query(h, "/v2/fleet/devices?limit=1", token, "site")
	var page struct {
		Consistency, NextAfter string
		Devices                []View
	}
	// The API deliberately uses snake_case for envelope metadata.
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	json.Unmarshal(raw["devices"], &page.Devices)
	json.Unmarshal(raw["next_after"], &page.NextAfter)
	json.Unmarshal(raw["consistency"], &page.Consistency)
	if w.Code != 200 || w.Header().Get("X-USP-Site") != "site" || w.Header().Get("Cache-Control") != "no-store" || page.Consistency != "eventually_consistent" || page.NextAfter != "a" || len(page.Devices) != 1 || page.Devices[0].Freshness != "fresh" {
		t.Fatal(w.Body.String())
	}
	w = query(h, "/v2/fleet/devices?limit=1&after=a", token, "")
	if !strings.Contains(w.Body.String(), `"AgentID":"b"`) || !strings.Contains(w.Body.String(), `"freshness":"unavailable"`) {
		t.Fatal(w.Body.String())
	}
	*now = now.Add(4 * time.Minute)
	w = query(h, "/v2/fleet/summary", token, "")
	if !strings.Contains(w.Body.String(), `"stale":1`) || !strings.Contains(w.Body.String(), `"unavailable":1`) || !strings.Contains(w.Body.String(), `"fresh_convergence":{}`) {
		t.Fatal(w.Body.String())
	}
	if calls != 0 {
		t.Fatal(calls)
	}
}
func callbackCall(h http.Handler, path, method string, body []byte, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(string(body)))
	r.Header.Set("dapr-api-token", token)
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func wrap(b []byte, pubsub, topic string) []byte {
	out, _ := json.Marshal(map[string]any{"specversion": "1.0", "id": "transport-id", "pubsubname": pubsub, "topic": topic, "data_base64": b})
	return out
}
func TestCallbackAuthenticationDeliveryPolicyAndPersistence(t *testing.T) {
	p, s, _ := fixture(t)
	token := strings.Repeat("x", 32)
	h := p.Callback(http.NotFoundHandler(), token, "events", "notifications")
	if w := callbackCall(h, "/dapr/subscribe", "GET", nil, "wrong"); w.Code != 401 {
		t.Fatal(w.Code)
	}
	w := callbackCall(h, "/dapr/subscribe", "GET", nil, token)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"rawPayload":"true"`) || !strings.Contains(w.Body.String(), `"route":"fleet/events"`) {
		t.Fatal(w.Body.String())
	}
	e := event(t, "a", strings.Repeat("f", 32), 1, "v")
	for _, b := range [][]byte{wrap(e, "wrong", "notifications"), wrap(e, "events", "wrong"), []byte(`{"data_base64":"not-base64"}`), append(wrap(e, "events", "notifications"), []byte(` {}`)...), wrap(event(t, "unknown", strings.Repeat("f", 32), 1, "v"), "events", "notifications")} {
		w = callbackCall(h, EventRoute, "POST", b, token)
		if w.Code != 200 || !strings.Contains(w.Body.String(), "DROP") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if len(p.rows["a"].Receipts) != 0 {
		t.Fatal("invalid event persisted")
	}
	body := wrap(e, "events", "notifications")
	w = callbackCall(h, EventRoute, "POST", body, token)
	if !strings.Contains(w.Body.String(), "SUCCESS") || len(p.rows["a"].Receipts) != 1 {
		t.Fatal(w.Body.String())
	}
	conflict := wrap(event(t, "a", strings.Repeat("f", 32), 1, "changed"), "events", "notifications")
	w = callbackCall(h, EventRoute, "POST", conflict, token)
	if !strings.Contains(w.Body.String(), "DROP") {
		t.Fatal(w.Body.String())
	}
	s.fail = true
	next := wrap(event(t, "a", strings.Repeat("f", 32), 2, "v"), "events", "notifications")
	w = callbackCall(h, EventRoute, "POST", next, token)
	if !strings.Contains(w.Body.String(), "RETRY") {
		t.Fatal("ack before persistence", w.Body.String())
	}
}
func TestRealPublisherEnvelopeCompatibility(t *testing.T) {
	p, _, _ := fixture(t)
	token := strings.Repeat("t", 32)
	callback := p.Callback(http.NotFoundHandler(), token, "events", "notifications")
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1.0/publish/events/notifications" || r.URL.Query().Get("metadata.rawPayload") != "true" {
			t.Error(r.URL)
		}
		var value json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&value); err != nil {
			t.Error(err)
			w.WriteHeader(500)
			return
		}
		delivered := callbackCall(callback, EventRoute, "POST", wrap(value, "events", "notifications"), token)
		if !strings.Contains(delivered.Body.String(), "SUCCESS") {
			t.Error(delivered.Body.String())
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(204)
	}))
	defer sidecar.Close()
	msg := new(usp.Msg)
	var e envelope
	if err := json.Unmarshal(event(t, "a", strings.Repeat("a", 32), 1, "sensitive"), &e); err != nil {
		t.Fatal(err)
	}
	if err := protojson.Unmarshal(e.Message, msg); err != nil {
		t.Fatal(err)
	}
	publisher := &deviceactor.DaprEventPublisher{Endpoint: sidecar.URL, Pubsub: "events", Topic: "notifications"}
	input := device.DeviceEvent{Identity: identity("a"), Epoch: e.Epoch, Sequence: e.Sequence, SchemaVersion: "1", ReceivedAt: e.ReceivedAt, Message: msg}
	for i := 0; i < 2; i++ {
		if err := publisher.Publish(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	}
	if len(p.rows["a"].Receipts) != 1 || !p.rows["a"].Pending {
		t.Fatal("publisher contract mismatch")
	}
}
