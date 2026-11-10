package deviceapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
	"example.com/usp-controller/internal/pb/usp"
)

func TestCommandListingAPIRedactsPayloadsAndReportsStaleCursor(t *testing.T) {
	id := device.DeviceIdentity{Tenant: "t", ControllerID: "c", AgentID: "device"}
	actor, err := deviceactor.NewExecuting(id, &twinBackend{}, deviceactor.DefaultOptions(), uncertainTransport{})
	if err != nil {
		t.Fatal(err)
	}
	for _, command := range []device.RequestID{"one", "two"} {
		if _, err := actor.EnqueueCommand(context.Background(), device.CommandRequest{ID: command, Deadline: time.Now().Add(time.Hour), Request: &usp.Request{ReqType: &usp.Request_Get{Get: &usp.Get{ParamPaths: []string{"Device.SecretParameter"}}}}}); err != nil {
			t.Fatal(err)
		}
	}
	token := strings.Repeat("x", 32)
	host, err := deviceactor.NewHost([]*deviceactor.Actor{actor}, token, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	invoker := &hostInvoker{host: host, token: token}
	handler := Handler(http.NotFoundHandler(), invoker, map[string]device.DeviceIdentity{"device": id}, token)
	call := func(query, bearer string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", "/v2/devices/device/commands"+query, nil)
		r.Header.Set("Authorization", "Bearer "+bearer)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if w := call("?limit=1", "bad"); w.Code != 401 || invoker.calls != 0 {
		t.Fatal("unauthorized inventory inspection", w.Code)
	}
	for _, query := range []string{"?limit=0", "?limit=101", "?state=typo", "?limit=1&limit=2", "?unknown=value"} {
		if w := call(query, token); w.Code != 400 {
			t.Fatal(query, w.Code, w.Body.String())
		}
	}
	if invoker.calls != 0 {
		t.Fatal("invalid queries reached actor")
	}
	w := call("?limit=1", token)
	var page device.CommandPage
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil || w.Code != 200 || page.Total != 2 || len(page.Commands) != 1 || page.Commands[0].RequestID != "one" || page.NextCursor == "" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal(w.Code, w.Body.String(), err)
	}
	for _, secret := range []string{"SecretParameter", `"request":`, `"result":`, `"reason":`, `"message_id":`} {
		if strings.Contains(w.Body.String(), secret) {
			t.Fatal("payload-bearing listing", w.Body.String())
		}
	}
	if _, err := actor.CancelCommand(context.Background(), device.CancelCommandRequest{ID: "two", ExpectedRevision: 1}); err != nil {
		t.Fatal(err)
	}
	stale := call("?cursor="+url.QueryEscape(page.NextCursor), token)
	if stale.Code != 409 || !strings.Contains(stale.Body.String(), "restart without a cursor") || strings.Contains(stale.Body.String(), "admission") {
		t.Fatal("misleading stale read outcome", stale.Code, stale.Body.String())
	}
	w = call("?state=cancelled", token)
	if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil || w.Code != 200 || page.Total != 1 || len(page.Commands) != 1 || page.Commands[0].RequestID != "two" || page.Commands[0].State != device.CommandCancelled {
		t.Fatal(w.Code, w.Body.String(), err)
	}
	if w := call("?cursor=invalid!", token); w.Code != 400 || strings.Contains(w.Body.String(), "admission") {
		t.Fatal("invalid cursor treated as write", w.Code, w.Body.String())
	}
}
