package deviceactor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestEtcdAtomicComparisonAndAmbiguity(t *testing.T) {
	var mode atomic.Int32
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "etcd-secret" || r.Header.Get("dapr-api-token") != "" {
			t.Error("credential isolation")
		}
		if r.URL.Path == "/v3/kv/range" {
			var request struct {
				Key          string
				Serializable bool
			}
			json.NewDecoder(r.Body).Decode(&request)
			if request.Serializable {
				t.Error("non-linearizable read")
			}
			json.NewEncoder(w).Encode(map[string]any{"header": map[string]string{"cluster_id": "11", "revision": "7"}, "kvs": []any{map[string]string{"key": request.Key, "value": base64.StdEncoding.EncodeToString([]byte(`{"n":1}`)), "mod_revision": "7"}}})
			return
		}
		posts.Add(1)
		var request struct {
			Compare []map[string]string
			Success []json.RawMessage
		}
		if e := json.NewDecoder(r.Body).Decode(&request); e != nil || len(request.Compare) != 1 || len(request.Success) != 2 {
			t.Error("invalid atomic transaction")
		}
		cmp := request.Compare[0]
		if cmp["target"] != "MOD" || cmp["mod_revision"] != "7" || cmp["result"] != "EQUAL" {
			t.Error("missing revision comparison", cmp)
		}
		switch mode.Load() {
		case 0:
			io.WriteString(w, `{"header":{"cluster_id":"11","revision":"8"},"succeeded":false}`)
		case 1:
			w.WriteHeader(500)
		case 2:
			io.WriteString(w, `{"header":{"cluster_id":"11","revision":"8"},"succeeded":true}`)
		}
	}))
	defer server.Close()
	b, e := NewEtcdSnapshotBackend("http://127.0.0.1:3500", "dapr-secret", server.URL, "/site/", "etcd-secret", server.Client(), identity, time.Second, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	for _, m := range []int32{0, 1, 2} {
		mode.Store(m)
		if _, e = b.Load(ctx); e != nil {
			t.Fatal(e)
		}
		e = b.Save(ctx, []byte(`{"n":2}`))
		if m == 0 && !errors.Is(e, ErrConflict) {
			t.Fatal("failed transaction acknowledged", e)
		}
		if m == 1 && e == nil {
			t.Fatal("ambiguous write acknowledged")
		}
		if m == 2 && e != nil {
			t.Fatal(e)
		}
		if m != 2 {
			before := posts.Load()
			if e = b.Save(ctx, []byte(`{"n":2}`)); e == nil || posts.Load() != before {
				t.Fatal("failed version reused")
			}
		}
	}
	if b.etag != "8" {
		t.Fatal("did not adopt successful transaction revision")
	}
}
func TestEtcdCreateAndRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v3/kv/range" {
			io.WriteString(w, `{"header":{"cluster_id":"11","revision":"1"}}`)
			return
		}
		var request struct {
			Compare []map[string]string
			Success []struct {
				Range *struct{ Revision string } `json:"request_range"`
			}
		}
		json.NewDecoder(r.Body).Decode(&request)
		if len(request.Success) != 2 || request.Success[0].Range == nil || request.Success[0].Range.Revision != "1" {
			t.Error("create missing read witness")
		}
		if len(request.Compare) != 1 || request.Compare[0]["target"] != "CREATE" || request.Compare[0]["create_revision"] != "0" {
			t.Error("create was not conditional")
		}
		io.WriteString(w, `{"header":{"cluster_id":"11","revision":"2"},"succeeded":true}`)
	}))
	defer server.Close()
	b, e := NewEtcdSnapshotBackend("http://127.0.0.1:3500", "", server.URL, "/site/", "", nil, identity, time.Second, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = b.etcd.save(context.Background(), []byte(`{}`), ""); e != nil {
		t.Fatal(e)
	}
	var leaked atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Store(true) }))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", target.URL)
		w.WriteHeader(307)
	}))
	defer redirect.Close()
	b, e = NewEtcdSnapshotBackend("http://127.0.0.1:3500", "", redirect.URL, "/site/", "secret", nil, identity, time.Second, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = b.Load(ctx); e == nil || leaked.Load() {
		t.Fatal("followed etcd redirect")
	}
	for _, endpoint := range []string{"http://remote:2379", "https://user:secret@host", "https://host/?q=x"} {
		if ValidateEtcdLocation(endpoint, "/site/") == nil {
			t.Fatal("unsafe endpoint", endpoint)
		}
	}
	for _, prefix := range []string{"", "site/", "/../", "/double//"} {
		if ValidateEtcdLocation("https://etcd.example", prefix) == nil {
			t.Fatal("invalid prefix", prefix)
		}
	}
	if _, e = EtcdHTTPClient("", "missing-cert", ""); e == nil {
		t.Fatal("partial client identity")
	}
	if ValidateEtcdLocation("https://etcd.example", "/site/") != nil {
		t.Fatal("valid TLS endpoint rejected")
	}
}

// The witness is the linearizable read's global revision, not the key's older
// modification revision. Ordinary compaction can therefore be recovered by Load.
func TestEtcdHistoricalGuardAndReload(t *testing.T) {
	var compacted atomic.Bool
	var writes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v3/kv/range" {
			var request struct{ Key string }
			json.NewDecoder(r.Body).Decode(&request)
			rev := "10"
			if compacted.Load() {
				rev = "20"
			}
			json.NewEncoder(w).Encode(map[string]any{"header": map[string]string{"cluster_id": "11", "revision": rev}, "kvs": []any{map[string]string{"key": request.Key, "value": "e30=", "mod_revision": "7"}}})
			return
		}
		writes.Add(1)
		var request struct {
			Compare []map[string]string
			Success []struct {
				Range *struct {
					Key, Revision string
					CountOnly     bool `json:"count_only"`
				} `json:"request_range"`
				Put *struct{ Key string } `json:"request_put"`
			}
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		if len(request.Success) != 2 || request.Success[0].Range == nil || request.Success[1].Put == nil {
			t.Error("missing atomic range and put")
			w.WriteHeader(400)
			return
		}
		guard := request.Success[0].Range
		if !guard.CountOnly || guard.Key != request.Success[1].Put.Key || len(request.Compare) != 1 || request.Compare[0]["mod_revision"] != "7" {
			t.Error("invalid guard or CAS")
		}
		if guard.Revision == "10" {
			w.WriteHeader(400)
			return
		}
		if guard.Revision != "20" {
			t.Error("did not use fresh global revision", guard.Revision)
		}
		io.WriteString(w, `{"header":{"cluster_id":"11","revision":"21"},"succeeded":true}`)
	}))
	defer server.Close()
	b := multiBackend(t, server.URL)
	if _, err := b.Load(ctx); err != nil {
		t.Fatal(err)
	}
	compacted.Store(true)
	if err := b.Save(ctx, []byte(`{}`)); err == nil {
		t.Fatal("compacted witness accepted")
	}
	if err := b.Save(ctx, []byte(`{}`)); err == nil || writes.Load() != 1 {
		t.Fatal("failed transaction replayed")
	}
	if _, err := b.Load(ctx); err != nil {
		t.Fatal(err)
	}
	if err := b.Save(ctx, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
}
