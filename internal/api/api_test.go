package api

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"example.com/usp-controller/internal/pb/usp"
)

type fakeRequester struct{ calls int }

func (f *fakeRequester) Request(_ context.Context, _ string, _ *usp.Request) (*usp.Msg, error) {
	f.calls++
	return &usp.Msg{Header: &usp.Header{MsgId: "id", MsgType: usp.Header_GET_RESP}}, nil
}
func TestAuthenticationAndStrictJSON(t *testing.T) {
	f := new(fakeRequester)
	token := strings.Repeat("a", 32)
	h := Handler(f, func() bool { return true }, token, time.Second)
	valid := `{"agent_id":"agent","request":{"get":{"paramPaths":["Device.DeviceInfo."]}}}`
	for _, tc := range []struct {
		body, auth string
		status     int
	}{
		{valid, "", 401}, {valid, "Bearer wrong", 401}, {valid, "Bearer " + token, 200},
		{valid + ` {}`, "Bearer " + token, 400},
		{`{"agent_id":"agent","request":{"get":{},"set":{}}}`, "Bearer " + token, 400},
		{`{"agent_id":"agent","request":{"unknown":{}}}`, "Bearer " + token, 400},
		{`{"agent_id":"agent","request":{}}`, "Bearer " + token, 400},
	} {
		r := httptest.NewRequest("POST", "/v1/requests", strings.NewReader(tc.body))
		r.Header.Set("Authorization", tc.auth)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("got %d want %d: %s", w.Code, tc.status, w.Body.String())
		}
	}
	if f.calls != 1 {
		t.Fatalf("unauthorized/invalid calls reached controller: %d", f.calls)
	}
}
func TestReadinessSeparateFromLiveness(t *testing.T) {
	h := Handler(new(fakeRequester), func() bool { return false }, "token", time.Second)
	for path, status := range map[string]int{"/healthz": 200, "/readyz": 503} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != status {
			t.Fatalf("%s: %d", path, w.Code)
		}
	}
}
