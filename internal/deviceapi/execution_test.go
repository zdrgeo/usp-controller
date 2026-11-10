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

func TestExecutionAPIRequiresExplicitDecision(t *testing.T) {
	id := device.DeviceIdentity{Tenant: "t", ControllerID: "c", AgentID: "device"}
	actor, err := deviceactor.NewExecuting(id, &twinBackend{}, deviceactor.DefaultOptions(), uncertainTransport{})
	if err != nil {
		t.Fatal(err)
	}
	token := strings.Repeat("x", 32)
	host, err := deviceactor.NewHost([]*deviceactor.Actor{actor}, token, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	invoker := &hostInvoker{host: host, token: token}
	handler := Handler(http.NotFoundHandler(), invoker, map[string]device.DeviceIdentity{"device": id}, token)
	call := func(method, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/v2/devices/device/execution", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	pause := `{"request_id":"pause","expected_revision":0,"paused":true,"reason":"maintenance"}`
	if w := call("PUT", pause); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	before := invoker.calls
	for _, bad := range []string{
		`{"request_id":"resume","expected_revision":1,"reason":"ready"}`,
		`{"request_id":"resume","expected_revision":1,"paused":null,"reason":"ready"}`,
		`{"request_id":"resume","paused":false,"reason":"ready"}`,
	} {
		if w := call("PUT", bad); w.Code != 400 {
			t.Fatal("implicit resume accepted", w.Code, w.Body.String())
		}
	}
	if invoker.calls != before {
		t.Fatal("invalid control reached actor")
	}
	// The private callback must enforce presence too, even if invoked directly.
	if _, err := invoker.Invoke(context.Background(), id, "SetExecution", map[string]any{"request_id": "resume", "expected_revision": 1, "reason": "ready"}); !errors.Is(err, deviceactor.ErrInvalid) {
		t.Fatal("callback accepted an implicit resume", err)
	}
	resume := `{"request_id":"resume","expected_revision":1,"paused":false,"reason":"ready"}`
	if w := call("PUT", resume); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call("PUT", strings.ReplaceAll(pause, `"expected_revision":0`, `"expected_revision":2`)); w.Code != 409 {
		t.Fatal("changed request ID reused", w.Code)
	}
	next := `{"request_id":"new-pause","expected_revision":2,"paused":true,"reason":"new maintenance"}`
	if w := call("PUT", next); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call("PUT", resume); w.Code != 200 {
		t.Fatal("immutable retry failed", w.Code, w.Body.String())
	}
	w := call("GET", "")
	var state device.ExecutionState
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil || w.Code != 200 || !state.Paused || state.Revision != 3 {
		t.Fatal("old resume retry changed latest control", w.Body.String(), err)
	}
}
