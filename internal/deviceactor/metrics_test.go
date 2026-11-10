package deviceactor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"example.com/usp-controller/internal/actormetrics"
)

func TestActorMetricsClassifyDomainEnvelopeWithoutPayloads(t *testing.T) {
	a, b, _ := fixture(t)
	metrics := actormetrics.New()
	a.backend = metrics.Backend(b)
	token := strings.Repeat("x", 32)
	host, err := NewHost([]*Actor{a}, token, 1<<20, metrics)
	if err != nil {
		t.Fatal(err)
	}
	invoke := func(method string, body []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest("PUT", "/actors/"+ActorType+"/"+ActorID(identity)+"/method/"+method, strings.NewReader(string(body)))
		r.Header.Set("dapr-api-token", token)
		w := httptest.NewRecorder()
		host.ServeHTTP(w, r)
		return w
	}
	conflict, _ := json.Marshal(desired("sensitive-request", 1))
	w := invoke("SetDesired", conflict)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"device_actor_error":409`) {
		t.Fatal("changed Dapr domain envelope", w.Code, w.Body.String())
	}
	b.failSave = true
	valid, _ := json.Marshal(desired("sensitive-request", 0))
	if w := invoke("SetDesired", valid); w.Code != 500 {
		t.Fatal("masked persistence failure", w.Code)
	}
	request := httptest.NewRequest(http.MethodGet, "/v2/metrics", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	metrics.Handler(http.NotFoundHandler(), token).ServeHTTP(w, request)
	body := w.Body.String()
	for _, label := range []string{`boundary="actor",operation="SetDesired",result="conflict"} 1`, `boundary="actor",operation="SetDesired",result="error"} 1`, `boundary="snapshot",operation="save",result="error"} 1`} {
		if !strings.Contains(body, label) {
			t.Fatal("domain failure counted as success", body)
		}
	}
	if strings.Contains(body, "sensitive") || strings.Contains(body, ActorID(identity)) || strings.Contains(body, "target") {
		t.Fatal("actor data leaked into metrics", body)
	}
}
