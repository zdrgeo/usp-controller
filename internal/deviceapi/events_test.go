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

func TestPendingEventAPIAuthRedactionAndChangedCursor(t *testing.T) {
	id := device.DeviceIdentity{Tenant: "t", ControllerID: "c", AgentID: "device"}
	backend := &twinBackend{}
	actor, err := deviceactor.NewExecuting(id, backend, deviceactor.DefaultOptions(), uncertainTransport{})
	if err != nil {
		t.Fatal(err)
	}
	notify := func(mid string) {
		t.Helper()
		msg := &usp.Msg{Header: &usp.Header{MsgId: mid, MsgType: usp.Header_NOTIFY}, Body: &usp.Body{MsgBody: &usp.Body_Request{Request: &usp.Request{ReqType: &usp.Request_Notify{Notify: &usp.Notify{SubscriptionId: "SENSITIVE_SUBSCRIPTION", Notification: &usp.Notify_ValueChange_{ValueChange: &usp.Notify_ValueChange{ParamPath: "Device.Secret.Value", ParamValue: "SENSITIVE_PAYLOAD"}}}}}}}}
		if _, err := actor.HandleNotify(context.Background(), device.DeviceNotification{Identity: id, Message: msg, ReceivedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	notify("first")
	notify("second")
	token := strings.Repeat("x", 32)
	host, err := deviceactor.NewHost([]*deviceactor.Actor{actor}, token, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	invoker := &hostInvoker{host: host, token: token}
	handler := Handler(http.NotFoundHandler(), invoker, map[string]device.DeviceIdentity{"device": id}, token, Options{SiteID: "site"})
	call := func(path, bearer, site string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+bearer)
		r.Header.Set("X-USP-Site", site)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	for _, check := range []struct {
		path, bearer, site string
		status             int
	}{{"/v2/devices/device/events", "bad", "site", 401}, {"/v2/devices/other/events", token, "site", 404}, {"/v2/devices/device/events", token, "wrong", 409}} {
		w := call(check.path, check.bearer, check.site)
		if w.Code != check.status {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	for _, query := range []string{"?limit=0", "?limit=101", "?limit=1&limit=2", "?state=pending", "?cursor=" + strings.Repeat("x", 2049)} {
		if w := call("/v2/devices/device/events"+query, token, "site"); w.Code != 400 {
			t.Fatal("unsafe query accepted", w.Code)
		}
	}
	if invoker.calls != 0 {
		t.Fatal("invoked before access/query validation")
	}
	before := string(backend.state)
	w := call("/v2/devices/device/events?limit=1", token, "site")
	var page device.EventPage
	if err = json.Unmarshal(w.Body.Bytes(), &page); err != nil || w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || page.Total != 2 || len(page.Events) != 1 || page.NextCursor == "" || strings.Contains(w.Body.String(), "SENSITIVE") || strings.Contains(w.Body.String(), "Secret") || string(backend.state) != before {
		t.Fatal(w.Code, w.Body.String(), err)
	}
	cursor := url.QueryEscape(page.NextCursor)
	notify("third")
	w = call("/v2/devices/device/events?cursor="+cursor, token, "site")
	if w.Code != 409 || !strings.Contains(w.Body.String(), "event view changed; restart without a cursor") || strings.Contains(w.Body.String(), "admission") {
		t.Fatal(w.Code, w.Body.String())
	}
}
