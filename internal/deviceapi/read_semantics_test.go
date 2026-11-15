package deviceapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

type readSemanticsInvoker struct {
	err    error
	calls  int
	method string
}

func (i *readSemanticsInvoker) Invoke(_ context.Context, _ device.DeviceIdentity, method string, _ any) (json.RawMessage, error) {
	i.calls++
	i.method = method
	return json.RawMessage(`{}`), i.err
}

func TestActorReadsNeverDescribeWritesOrCacheMutableEvidence(t *testing.T) {
	token := strings.Repeat("x", 32)
	invoker := &readSemanticsInvoker{}
	h := Handler(http.NotFoundHandler(), invoker, map[string]device.DeviceIdentity{"device": {Tenant: "t", ControllerID: "c", AgentID: "device"}}, token, Options{TwinEnabled: true, SiteID: "site"})
	call := func(method, path, body, bearer string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+bearer)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w
	}
	routes := []struct{ suffix, method string }{{"commands", "ListCommands"}, {"commands/request", "GetCommand"}, {"execution", "GetExecution"}, {"twin", "GetTwin"}, {"twin/diff", "GetTwinDiff"}, {"status", "GetStatus"}}
	for _, route := range routes {
		t.Run(route.method, func(t *testing.T) {
			for _, failure := range []struct {
				err    error
				status int
			}{{nil, 200}, {errors.New("SENSITIVE_BACKEND_DETAILS"), 503}, {deviceactor.ErrNotFound, 404}, {deviceactor.ErrConflict, 409}} {
				invoker.err = failure.err
				before := invoker.calls
				w := call("GET", "/v2/devices/device/"+route.suffix, "", token)
				if w.Code != failure.status || w.Header().Get("Cache-Control") != "no-store" || invoker.calls != before+1 || invoker.method != route.method {
					t.Fatal(w.Code, w.Header(), w.Body.String(), invoker.calls-before)
				}
				if strings.Contains(w.Body.String(), "admission") || strings.Contains(w.Body.String(), "retry the same") || strings.Contains(w.Body.String(), "SENSITIVE_BACKEND_DETAILS") {
					t.Fatal("read exposed mutation guidance or backend detail", w.Body.String())
				}
				cursorMessage := strings.Contains(w.Body.String(), "restart without a cursor")
				if cursorMessage != (route.method == "ListCommands" && failure.status == 409) {
					t.Fatal("cursor guidance leaked to unrelated read", w.Body.String())
				}
			}
		})
	}
	before := invoker.calls
	if w := call("GET", "/v2/devices/device/twin", "", "bad"); w.Code != 401 || w.Header().Get("Cache-Control") != "no-store" || invoker.calls != before {
		t.Fatal("access failure cached or invoked", w.Code, w.Header())
	}
	if w := call("GET", "/v2/devices/unknown/twin", "", token); w.Code != 404 || w.Header().Get("Cache-Control") != "no-store" || invoker.calls != before {
		t.Fatal("inventory failure cached or invoked", w.Code, w.Header())
	}
	invoker.err = errors.New("uncertain admission")
	body := `{"request_id":"desired","expected_revision":0,"values":{"Device.Test.Value":{"Type":"string","Value":"wanted"}}}`
	w := call("PATCH", "/v2/devices/device/desired", body, token)
	if w.Code != 503 || !strings.Contains(w.Body.String(), "admission may have committed") || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("mutation uncertainty guidance changed", w.Code, w.Body.String())
	}
}
