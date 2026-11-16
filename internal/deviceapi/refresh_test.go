package deviceapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/encoding/protojson"
)

type refreshTransport struct {
	mu       sync.Mutex
	attempts []device.PublishAttempt
}

func (p *refreshTransport) Publish(_ context.Context, _ device.DeviceIdentity, a device.PublishAttempt) (device.PublishResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.attempts = append(p.attempts, a)
	return device.PublishResult{BrokerAccepted: true}, nil
}
func (p *refreshTransport) calls() []device.PublishAttempt {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]device.PublishAttempt(nil), p.attempts...)
}

// Public API -> Dapr client -> private callback -> durable queue -> response
// callback projection. This catches missing ingress timestamps, not just DTOs.
func TestExplicitRefreshCallbackUpdatesManagedTwin(t *testing.T) {
	ctx := context.Background()
	id := device.DeviceIdentity{Tenant: "tenant", ControllerID: "controller", AgentID: "device"}
	opts := deviceactor.DefaultOptions()
	opts.Policy = &device.TwinPolicy{SiteID: "site", ObserveGetResponses: true, CommandTimeoutSeconds: 30, Paths: map[string]device.ManagedPath{"Device.Test.Value": {Type: "string"}, "Device.Test.Flag": {Type: "boolean"}}}
	backend := &twinBackend{}
	transport := &refreshTransport{}
	actor, err := deviceactor.NewExecuting(id, backend, opts, transport)
	if err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("x", 32)
	host, err := deviceactor.NewHost([]*deviceactor.Actor{actor}, token, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	invoker := &hostInvoker{host: host, token: token}
	inventory := map[string]device.DeviceIdentity{"device": id}
	handler := Handler(http.NotFoundHandler(), invoker, inventory, token, Options{TwinEnabled: true, SiteID: "site"})
	deadline := time.Now().UTC().Add(time.Hour)
	input := device.RefreshTwinRequest{RequestID: "refresh-1", Deadline: deadline, Paths: []string{"Device.Test.Value"}}
	body, _ := json.Marshal(input)
	call := func(h http.Handler, path, bearer, site string, body []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", path, strings.NewReader(string(body)))
		r.Header.Set("Authorization", "Bearer "+bearer)
		r.Header.Set("X-USP-Site", site)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	for _, check := range []struct {
		device, token, site string
		status              int
	}{{"device", "bad", "site", 401}, {"other", token, "site", 404}, {"device", token, "wrong", 409}} {
		w := call(handler, "/v2/devices/"+check.device+"/twin/refresh", check.token, check.site, body)
		if w.Code != check.status {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if invoker.calls != 0 {
		t.Fatal("invoked before access checks")
	}
	for i := 0; i < 2; i++ {
		w := call(handler, "/v2/devices/device/twin/refresh", token, "site", body)
		if w.Code != 202 || w.Header().Get("Location") != "/v2/devices/device/commands/refresh-1" || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal(w.Code, w.Header(), w.Body.String())
		}
	}
	if len(transport.calls()) != 0 {
		t.Fatal("admission published")
	}
	c, err := actor.GetCommand(ctx, "refresh-1")
	if err != nil || c.State != device.CommandQueued || !reflect.DeepEqual(c.Request.GetGet().ParamPaths, []string{"Device.Test.Value"}) {
		t.Fatal(c, err)
	}
	changed := input
	changed.Deadline = deadline.Add(time.Second)
	other, _ := json.Marshal(changed)
	if w := call(handler, "/v2/devices/device/twin/refresh", token, "site", other); w.Code != 409 {
		t.Fatal("changed retry accepted", w.Code)
	}
	if _, err = invoker.Invoke(ctx, id, "Tick", nil); err != nil {
		t.Fatal(err)
	}
	attempts := transport.calls()
	if len(attempts) != 1 {
		t.Fatal(attempts)
	}
	msg := &usp.Msg{Header: &usp.Header{MsgId: attempts[0].MessageID, MsgType: usp.Header_GET_RESP}, Body: &usp.Body{MsgBody: &usp.Body_Response{Response: &usp.Response{RespType: &usp.Response_GetResp{GetResp: &usp.GetResp{ReqPathResults: []*usp.GetResp_RequestedPathResult{
		{RequestedPath: "Device.Test.Flag", ResolvedPathResults: []*usp.GetResp_ResolvedPathResult{{ResolvedPath: "Device.Test.", ResultParams: map[string]string{"Flag": "true"}}}},
		{RequestedPath: "Device.Test.Value", ResolvedPathResults: []*usp.GetResp_ResolvedPathResult{{ResolvedPath: "Device.Test.", ResultParams: map[string]string{"Value": "observed", "Secret": "excluded"}}}},
	}}}}}}}
	raw, err := protojson.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = invoker.Invoke(ctx, id, "HandleResponse", deviceactor.ResponseInput{Identity: id, Message: raw}); err != nil {
		t.Fatal(err)
	}
	twin, err := actor.GetTwin(ctx)
	if err != nil || twin.ReportedRevision != 1 || len(twin.Reported) != 1 || twin.Reported["Device.Test.Value"].Value.Value != "observed" {
		t.Fatal("callback response did not project reviewed values", twin, err)
	}
	c, err = actor.GetCommand(ctx, "refresh-1")
	got := twin.Reported["Device.Test.Value"]
	if err != nil || c.State != device.CommandSucceeded || got.ReceivedAt.IsZero() || got.Source != "usp-get-response" || !got.FreshUntil.Equal(c.AttemptedAt.Add(opts.Freshness)) {
		t.Fatal("missing receipt-time provenance", c, got, err)
	}
	before := string(backend.state)
	if _, err = invoker.Invoke(ctx, id, "HandleResponse", deviceactor.ResponseInput{Identity: id, Message: raw}); err != nil {
		t.Fatal(err)
	}
	if w := call(handler, "/v2/devices/device/twin/refresh", token, "site", body); w.Code != 202 {
		t.Fatal(w.Code)
	}
	if string(backend.state) != before || len(transport.calls()) != 1 {
		t.Fatal("retry refreshed or replayed command")
	}
	disabled := Handler(http.NotFoundHandler(), invoker, inventory, token)
	if w := call(disabled, "/v2/devices/device/twin/refresh", token, "site", body); w.Code != 404 {
		t.Fatal("default exposed refresh", w.Code)
	}
	bad := []byte(`{"request_id":"extra","deadline":"` + deadline.Format(time.RFC3339Nano) + `","paths":["Device.Secret.Password"]}`)
	if w := call(handler, "/v2/devices/device/twin/refresh", token, "site", bad); w.Code != 400 {
		t.Fatal("caller expanded managed scope", w.Code)
	}
}
