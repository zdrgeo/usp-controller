package deviceapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

type twinBackend struct{ state []byte }

func (b *twinBackend) Load(context.Context) ([]byte, error) {
	return append([]byte(nil), b.state...), nil
}
func (b *twinBackend) Save(_ context.Context, v []byte) error {
	b.state = append([]byte(nil), v...)
	return nil
}
func (b *twinBackend) EnsureReminder(context.Context) error { return nil }

type hostInvoker struct {
	host  http.Handler
	token string
	calls int
}

func (i *hostInvoker) Invoke(ctx context.Context, id device.DeviceIdentity, method string, input any) (json.RawMessage, error) {
	i.calls++
	b, _ := json.Marshal(input)
	r := httptest.NewRequest("PUT", "/actors/DeviceActor/"+deviceactor.ActorID(id)+"/method/"+method, strings.NewReader(string(b))).WithContext(ctx)
	r.Header.Set("dapr-api-token", i.token)
	w := httptest.NewRecorder()
	i.host.ServeHTTP(w, r)
	switch w.Code {
	case 200:
		return w.Body.Bytes(), nil
	case 400:
		return nil, deviceactor.ErrInvalid
	case 409:
		return nil, deviceactor.ErrConflict
	default:
		return nil, deviceactor.ErrDisabled
	}
}
func TestTwinAPIAuthSiteCASAndStatus(t *testing.T) {
	id := device.DeviceIdentity{Tenant: "tenant", ControllerID: "controller", AgentID: "device"}
	opts := deviceactor.DefaultOptions()
	opts.Policy = &device.TwinPolicy{SiteID: "site-a", CommandTimeoutSeconds: 30, Paths: map[string]device.ManagedPath{"Device.Test.Value": {Type: "string", Writable: true}}}
	actor, err := deviceactor.New(id, &twinBackend{}, opts)
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
	base := http.NotFoundHandler()
	h := Handler(base, invoker, inventory, token, Options{SiteID: "site-a", TwinEnabled: true})
	call := func(handler http.Handler, method, path, body, bearer, site string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+bearer)
		if site != "" {
			r.Header.Set("X-USP-Site", site)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	body := `{"request_id":"target","expected_revision":0,"values":{"Device.Test.Value":{"Value":"wanted","Type":"string"}}}`
	for _, c := range []struct {
		path, auth, site string
		status           int
	}{{"device", "bad", "site-a", 401}, {"other", token, "site-a", 404}, {"device", token, "site-b", 409}} {
		if w := call(h, "PATCH", "/v2/devices/"+c.path+"/desired", body, c.auth, c.site); w.Code != c.status {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if invoker.calls != 0 {
		t.Fatal("invoked before auth/site/allowlist validation")
	}
	for i := 0; i < 2; i++ {
		if w := call(h, "PATCH", "/v2/devices/device/desired", body, token, "site-a"); w.Code != 200 || w.Header().Get("X-USP-Site") != "site-a" {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	conflict := strings.Replace(body, `"wanted"`, `"different"`, 1)
	if w := call(h, "PATCH", "/v2/devices/device/desired", conflict, token, ""); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(h, "PATCH", "/v2/devices/device/desired", strings.Replace(body, `"target"`, `"next"`, 1), token, ""); w.Code != 409 {
		t.Fatal("CAS", w.Code)
	}
	w := call(h, "GET", "/v2/devices/device/twin", "", token, "")
	var twin device.DeviceTwin
	if err = json.Unmarshal(w.Body.Bytes(), &twin); err != nil || w.Code != 200 || twin.DesiredRevision != 1 || twin.Desired["Device.Test.Value"].Value != "wanted" {
		t.Fatal(w.Body.String(), err)
	}
	w = call(h, "GET", "/v2/devices/device/status", "", token, "")
	if w.Code != 200 || strings.Contains(w.Body.String(), "wanted") || !strings.Contains(w.Body.String(), `"site_id":"site-a"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	disabled := Handler(base, invoker, inventory, token)
	if w = call(disabled, "GET", "/v2/devices/device/twin", "", token, ""); w.Code != 404 {
		t.Fatal("default exposed twin", w.Code)
	}
	// Reserved generated-command identities cannot be claimed as public mutations.
	bad := strings.Replace(body, `"target"`, `"reconcile-1"`, 1)
	if w = call(h, "PATCH", "/v2/devices/device/desired", bad, token, ""); w.Code != 400 {
		t.Fatal(w.Code)
	}
}
