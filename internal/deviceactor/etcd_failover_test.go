package deviceactor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func failoverRead(w http.ResponseWriter, r *http.Request, cluster, value, revision string) {
	var request struct{ Key string }
	json.NewDecoder(r.Body).Decode(&request)
	json.NewEncoder(w).Encode(map[string]any{"header": map[string]string{"cluster_id": cluster}, "kvs": []any{map[string]string{"key": request.Key, "value": base64.StdEncoding.EncodeToString([]byte(value)), "mod_revision": revision}}})
}
func multiBackend(t *testing.T, endpoints ...string) *FencedDaprBackend {
	t.Helper()
	b, e := NewEtcdSnapshotBackendEndpoints("http://127.0.0.1:3500", "", endpoints, "/site/", "", nil, identity, time.Second, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func TestEtcdReadFailoverAndStickiness(t *testing.T) {
	var firstCalls, secondCalls atomic.Int32
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { firstCalls.Add(1); w.WriteHeader(503) }))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		secondCalls.Add(1)
		failoverRead(w, r, "11", `{"n":1}`, "7")
	}))
	defer second.Close()
	b := multiBackend(t, first.URL, second.URL)
	for range 2 {
		if data, e := b.Load(ctx); e != nil || string(data) != `{"n":1}` {
			t.Fatal(string(data), e)
		}
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 2 {
		t.Fatal("did not retain healthy endpoint")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, e := b.Load(cancelled); !errors.Is(e, context.Canceled) || secondCalls.Load() != 2 {
		t.Fatal("ignored cancellation", e)
	}
}
func TestEtcdAmbiguousWriteRotatesWithoutReplay(t *testing.T) {
	var mu sync.Mutex
	value := `{"n":1}`
	revision := "7"
	writes := 0
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path == "/v3/kv/range" {
			failoverRead(w, r, "11", value, revision)
			return
		}
		writes++
		value = `{"n":2}`
		revision = "8"
		w.WriteHeader(503) // committed; response lost
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.URL.Path != "/v3/kv/range" {
			writes++
			t.Error("replayed transaction")
			w.WriteHeader(500)
			return
		}
		failoverRead(w, r, "11", value, revision)
	}))
	defer second.Close()
	b := multiBackend(t, first.URL, second.URL)
	if _, e := b.Load(ctx); e != nil {
		t.Fatal(e)
	}
	if e := b.Save(ctx, []byte(`{"n":2}`)); e == nil {
		t.Fatal("ambiguous transaction acknowledged")
	}
	if e := b.Save(ctx, []byte(`{"n":2}`)); e == nil {
		t.Fatal("reused invalidated version")
	}
	if data, e := b.Load(ctx); e != nil || string(data) != `{"n":2}` || b.etcd.preferred != 1 {
		t.Fatal("did not recover through alternate endpoint", string(data), e)
	}
	mu.Lock()
	defer mu.Unlock()
	if writes != 1 {
		t.Fatal("write attempts", writes)
	}
}
func TestEtcdFailoverStopsOnSecurityAndProtocolErrors(t *testing.T) {
	for _, status := range []int{401, 403, 307, 400, 200} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var alternate atomic.Int32
			second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { alternate.Add(1) }))
			defer second.Close()
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Location", second.URL)
				w.WriteHeader(status)
				w.Write([]byte(`bad JSON`))
			}))
			defer first.Close()
			if _, e := multiBackend(t, first.URL, second.URL).Load(ctx); e == nil || alternate.Load() != 0 {
				t.Fatal("security/protocol error failed open", e)
			}
		})
	}
	var failed atomic.Bool
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failed.Load() {
			w.WriteHeader(503)
			return
		}
		failoverRead(w, r, "11", `{}`, "7")
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { failoverRead(w, r, "12", `{}`, "7") }))
	defer second.Close()
	b := multiBackend(t, first.URL, second.URL)
	if _, e := b.Load(ctx); e != nil {
		t.Fatal(e)
	}
	failed.Store(true)
	if _, e := b.Load(ctx); e == nil {
		t.Fatal("silently switched clusters")
	}
}
func TestEtcdEndpointListValidation(t *testing.T) {
	for _, list := range [][]string{nil, {"https://etcd", "https://etcd/"}, {"http://remote:2379"}, {"https://a", "https://b", "https://c", "https://d", "https://e", "https://f"}} {
		if ValidateEtcdEndpoints(list, "/site/") == nil {
			t.Fatal("invalid list accepted", list)
		}
	}
}

func TestEtcdTimeoutFailoverAndWriteRotation(t *testing.T) {
	for _, write := range []bool{false, true} {
		name := "read"
		if write {
			name = "write"
		}
		t.Run(name, func(t *testing.T) {
			release := make(chan struct{})
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if write && r.URL.Path == "/v3/kv/range" {
					failoverRead(w, r, "11", `{}`, "7")
					return
				}
				select {
				case <-r.Context().Done():
				case <-release:
				}
			}))
			defer first.Close()
			defer close(release)
			var writes atomic.Int32
			second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/v3/kv/range" {
					writes.Add(1)
					w.WriteHeader(500)
					return
				}
				failoverRead(w, r, "11", `{}`, "7")
			}))
			defer second.Close()
			b := multiBackend(t, first.URL, second.URL)
			if write {
				if _, e := b.Load(ctx); e != nil {
					t.Fatal(e)
				}
				deadline, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
				e := b.Save(deadline, []byte(`{}`))
				cancel()
				if e == nil || b.etcd.preferred != 1 {
					t.Fatal("timed-out write did not rotate next call", e)
				}
			} else {
				b.etcd.client.Timeout = 50 * time.Millisecond
			}
			if _, e := b.Load(ctx); e != nil || b.etcd.preferred != 1 {
				t.Fatal("read did not reach alternate", e)
			}
			if writes.Load() != 0 {
				t.Fatal("transaction retried")
			}
		})
	}
}
