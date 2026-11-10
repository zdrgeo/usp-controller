package deviceapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
	"example.com/usp-controller/internal/pb/usp"
)

type uncertainTransport struct{}

func (uncertainTransport) Publish(context.Context, device.DeviceIdentity, device.PublishAttempt) (device.PublishResult, error) {
	return device.PublishResult{}, errors.New("lost publish result")
}

// Exercise the complete public API -> client -> actor callback path, including
// authorization before invocation and immutable retry identity for the decision.
func TestUnknownAcknowledgementAPI(t *testing.T) {
	id := device.DeviceIdentity{Tenant: "t", ControllerID: "c", AgentID: "device"}
	actor, err := deviceactor.NewExecuting(id, &twinBackend{}, deviceactor.DefaultOptions(), uncertainTransport{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := actor.EnqueueCommand(context.Background(), device.CommandRequest{ID: "command", Deadline: time.Now().Add(time.Hour), Request: &usp.Request{ReqType: &usp.Request_Get{Get: &usp.Get{ParamPaths: []string{"Device.Test."}}}}}); err != nil {
		t.Fatal(err)
	}
	if err := actor.Tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	command, err := actor.GetCommand(context.Background(), "command")
	if err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("x", 32)
	host, err := deviceactor.NewHost([]*deviceactor.Actor{actor}, token, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	invoker := &hostInvoker{host: host, token: token}
	handler := Handler(http.NotFoundHandler(), invoker, map[string]device.DeviceIdentity{"device": id}, token, Options{SiteID: "site"})
	input := map[string]any{"request_id": "review", "expected_revision": command.Revision, "reason": "device independently checked"}
	body, _ := json.Marshal(input)
	call := func(deviceID, bearer, site string, body []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/v2/devices/"+deviceID+"/commands/command/acknowledge-unknown", strings.NewReader(string(body)))
		r.Header.Set("Authorization", "Bearer "+bearer)
		r.Header.Set("X-USP-Site", site)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	for _, test := range []struct {
		device, token, site string
		status              int
	}{{"device", "bad", "site", 401}, {"other", token, "site", 404}, {"device", token, "other", 409}} {
		if w := call(test.device, test.token, test.site, body); w.Code != test.status {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if invoker.calls != 0 {
		t.Fatal("operator decision invoked before access checks")
	}
	for i := 0; i < 2; i++ {
		w := call("device", token, "site", body)
		var out deviceactor.CommandOutput
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || w.Code != 200 || out.State != device.CommandUnknown || out.UnknownAcknowledgement == nil || out.UnknownAcknowledgement.RequestID != "review" {
			t.Fatal(w.Code, w.Body.String(), err)
		}
	}
	input["reason"] = "changed decision"
	changed, _ := json.Marshal(input)
	if w := call("device", token, "site", changed); w.Code != 409 {
		t.Fatal("changed retry did not conflict", w.Code, w.Body.String())
	}
	input["request_id"] = "other-review"
	changed, _ = json.Marshal(input)
	if w := call("device", token, "site", changed); w.Code != 409 {
		t.Fatal("stale second decision accepted", w.Code)
	}
}
