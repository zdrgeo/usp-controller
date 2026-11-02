package deviceactor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFencedSnapshotCASAndMigration(t *testing.T) {
	var mu sync.Mutex
	var state []byte
	version := 0
	posts := 0
	failPost := false
	noTag := false
	replaceAfterSave := false
	legacy := []byte(`{"desired":1,"large":9007199254740993}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("dapr-api-token") != "secret" {
			t.Error("missing token")
			w.WriteHeader(401)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1.0/actors/") {
			w.Write(legacy)
			return
		}
		switch r.Method {
		case "GET":
			if r.URL.Query().Get("consistency") != "strong" {
				t.Error("read consistency")
			}
			if state == nil {
				w.WriteHeader(204)
				return
			}
			if !noTag {
				w.Header().Set("ETag", fmt.Sprint(version))
			}
			w.Write(state)
		case "POST":
			posts++
			var ops []struct {
				Key     string            `json:"key"`
				Value   json.RawMessage   `json:"value"`
				ETag    string            `json:"etag"`
				Options map[string]string `json:"options"`
			}
			if json.NewDecoder(r.Body).Decode(&ops) != nil || len(ops) != 1 {
				t.Error("invalid single snapshot write")
				w.WriteHeader(400)
				return
			}
			op := ops[0]
			if !strings.HasPrefix(op.Key, "device-snapshot-cas-v1-") || op.Options["concurrency"] != "first-write" || op.Options["consistency"] != "strong" {
				t.Error("missing fence options")
			}
			if state != nil && op.ETag != fmt.Sprint(version) {
				w.WriteHeader(409)
				return
			}
			state = append([]byte(nil), op.Value...)
			version++
			if replaceAfterSave {
				state = []byte(`{"winner":"other"}`)
				version++
			}
			if failPost {
				w.WriteHeader(500)
				return
			}
			w.WriteHeader(204)
		}
	}))
	defer server.Close()
	create := func() *FencedDaprBackend {
		b, e := NewFencedDaprBackend(server.URL, server.Client(), "secret", identity, time.Second, 1<<20, "actors")
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	a, b := create(), create()
	for _, v := range []*FencedDaprBackend{a, b} {
		data, e := v.Load(ctx)
		if e != nil || string(data) != string(legacy) {
			t.Fatal("migration seed", string(data), e)
		}
	}
	if e := a.Save(ctx, []byte(`{"x":1}`)); e != nil {
		t.Fatal(e)
	}
	if e := b.Save(ctx, []byte(`{"x":2}`)); !errors.Is(e, ErrConflict) {
		t.Fatal("two first writers", e)
	}
	// Stale existing versions must also lose, and cannot be reused after failure.
	if _, e := b.Load(ctx); e != nil {
		t.Fatal(e)
	}
	if e := a.Save(ctx, []byte(`{"x":3}`)); e != nil {
		t.Fatal(e)
	}
	if e := b.Save(ctx, []byte(`{"x":4}`)); !errors.Is(e, ErrConflict) {
		t.Fatal("stale write", e)
	}
	mu.Lock()
	before := posts
	mu.Unlock()
	if e := b.Save(ctx, []byte(`{"x":4}`)); e == nil {
		t.Fatal("reused failed version")
	}
	mu.Lock()
	if posts != before {
		t.Error("write without reload")
	}
	failPost = true
	mu.Unlock()
	if _, e := b.Load(ctx); e != nil {
		t.Fatal(e)
	}
	if e := b.Save(ctx, []byte(`{"x":5}`)); e == nil {
		t.Fatal("ambiguous save accepted")
	}
	mu.Lock()
	failPost = false
	mu.Unlock()
	data, e := b.Load(ctx)
	if e != nil || string(data) != `{"x":5}` {
		t.Fatal("ambiguous reload", string(data), e)
	}
	mu.Lock()
	replaceAfterSave = true
	mu.Unlock()
	if e := b.Save(ctx, []byte(`{"x":6}`)); !errors.Is(e, ErrConflict) {
		t.Fatal("adopted competitor's ETag", e)
	}
	mu.Lock()
	noTag = true
	mu.Unlock()
	if _, e := b.Load(ctx); e == nil {
		t.Fatal("accepted unversioned state")
	}
}
func TestFencedJSONComparisonPreservesIntegers(t *testing.T) {
	if sameStateJSON([]byte(`{"a":1} trailing`), []byte(`{"a":1}`)) {
		t.Fatal("accepted malformed read-back")
	}
	if !sameStateJSON([]byte(`{"a":1,"b":9007199254740993}`), []byte(`{ "b":9007199254740993,"a":1 }`)) {
		t.Fatal("object ordering")
	}
	if sameStateJSON([]byte(`{"v":9007199254740993}`), []byte(`{"v":9007199254740992}`)) {
		t.Fatal("rounded revision")
	}
}
func TestActorClientDomainErrorsThroughDapr(t *testing.T) {
	a, _, _ := fixture(t)
	h, _ := NewHost([]*Actor{a}, strings.Repeat("s", 32), 1<<20)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/v1.0")
		r.Method = "PUT"
		r.URL.Path = path
		r.Header.Set("dapr-api-token", strings.Repeat("s", 32))
		recorded := httptest.NewRecorder()
		h.ServeHTTP(recorded, r)
		// Model Dapr's non-2xx callback wrapping; a domain conflict must not use it.
		if recorded.Code != 200 {
			w.WriteHeader(500)
			return
		}
		w.Write(recorded.Body.Bytes())
	}))
	defer server.Close()
	c := Client{Endpoint: server.URL}
	if _, e := c.Invoke(context.Background(), identity, "SetDesired", desired("one", 0)); e != nil {
		t.Fatal(e)
	}
	if _, e := c.Invoke(context.Background(), identity, "SetDesired", desired("two", 0)); !errors.Is(e, ErrConflict) {
		t.Fatal("lost typed conflict", e)
	}
}
