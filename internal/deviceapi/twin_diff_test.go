package deviceapi

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
)

func TestTwinDiffAPIUsesReadOnlyCallbackAndAccessChecks(t *testing.T) {
	id := device.DeviceIdentity{Tenant: "t", ControllerID: "c", AgentID: "device"}
	opts := deviceactor.DefaultOptions()
	opts.Policy = &device.TwinPolicy{SiteID: "site", CommandTimeoutSeconds: 30, Paths: map[string]device.ManagedPath{"Device.Test.Value": {Type: "string", Writable: true}, "Device.Test.ReadOnly": {Type: "string"}}}
	backend := &twinBackend{}
	actor, err := deviceactor.New(id, backend, opts)
	if err != nil {
		t.Fatal(err)
	}
	values := map[string]device.ParameterValue{"Device.Test.Value": {Type: "string", Value: "SENSITIVE_VALUE"}}
	if _, err = actor.SetDesired(context.Background(), device.SetDesiredRequest{ID: "desired", Values: values}); err != nil {
		t.Fatal(err)
	}
	if _, err = actor.Report(context.Background(), deviceactor.ReportRequest{ID: "observed", Identity: id, ReceivedAt: time.Now(), Values: values}); err != nil {
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
	call := func(h http.Handler, path, bearer, site string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", path, nil)
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
		w := call(handler, "/v2/devices/"+check.device+"/twin/diff", check.token, check.site)
		if w.Code != check.status {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if invoker.calls != 0 {
		t.Fatal("invoked before access checks")
	}
	before := string(backend.state)
	w := call(handler, "/v2/devices/device/twin/diff", token, "site")
	var diff device.TwinDiff
	if err = json.Unmarshal(w.Body.Bytes(), &diff); err != nil || w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || len(diff.Paths) != 1 || diff.Paths[0].State != "matching" || diff.Convergence != "converged" || len(diff.ObservationPaths) != 1 || diff.ObservationPaths[0].Path != "Device.Test.ReadOnly" || diff.ObservationPaths[0].State != "missing" || strings.Contains(w.Body.String(), "SENSITIVE_VALUE") || string(backend.state) != before {
		t.Fatal(w.Code, w.Body.String(), err)
	}
	calls := invoker.calls
	if w = call(handler, "/v2/devices/device/twin/diff?limit=1", token, "site"); w.Code != 400 || invoker.calls != calls {
		t.Fatal("unexpected query invoked actor", w.Code)
	}
	disabled := Handler(http.NotFoundHandler(), invoker, inventory, token)
	if w = call(disabled, "/v2/devices/device/twin/diff", token, "site"); w.Code != 404 || invoker.calls != calls {
		t.Fatal("default exposed twin diff", w.Code)
	}
}
