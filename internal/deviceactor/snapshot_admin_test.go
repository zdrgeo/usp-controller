package deviceactor

import (
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

func TestSnapshotAdministration(t *testing.T) {
	var mu sync.Mutex
	var state, legacy []byte
	version, writes, legacyReads := 0, 0, 0
	raceWrite, ambiguous := false, false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		if r.Header.Get("dapr-api-token") != "secret" {
			t.Error("missing API token")
			w.WriteHeader(401)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/v1.0/actors/") {
			legacyReads++
			if len(legacy) == 0 {
				w.WriteHeader(204)
			} else {
				w.Write(legacy)
			}
			return
		}
		if r.Method == "GET" {
			if state == nil {
				w.WriteHeader(204)
				return
			}
			w.Header().Set("ETag", fmt.Sprint(version))
			w.Write(state)
			return
		}
		var ops []struct {
			Value   json.RawMessage
			ETag    string
			Options map[string]string
		}
		if e := json.NewDecoder(r.Body).Decode(&ops); e != nil || len(ops) != 1 {
			t.Error("invalid save")
			w.WriteHeader(400)
			return
		}
		if ops[0].Options["concurrency"] != "first-write" {
			t.Error("unconditional admin save")
		}
		writes++
		if raceWrite {
			version++
			raceWrite = false
		}
		if state != nil && ops[0].ETag != fmt.Sprint(version) {
			w.WriteHeader(409)
			return
		}
		// Match pinned Redis: an absent key accepts even an existing ETag. The
		// application preflight must reject already-missing keys before posting.
		state = append([]byte(nil), ops[0].Value...)
		version++
		if ambiguous {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(204)
	}))
	defer server.Close()
	fresh := func() *FencedDaprBackend {
		b, e := NewFencedDaprBackend(server.URL, server.Client(), "secret", identity, time.Second, 1<<20, "state")
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	b := fresh()
	if _, e := b.Load(ctx); !errors.Is(e, ErrNotFound) {
		t.Fatal("missing snapshot", e)
	}
	mu.Lock()
	reads := legacyReads
	mu.Unlock()
	if reads != 0 {
		t.Fatal("runtime attempted legacy fallback")
	}
	if e := b.InitializeSnapshot(ctx, true); !errors.Is(e, ErrNotFound) {
		t.Fatal("missing legacy accepted", e)
	}
	if e := b.InitializeSnapshot(ctx, false); e != nil {
		t.Fatal(e)
	}
	backup, initial, e := b.ExportSnapshot(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if e := fresh().InitializeSnapshot(ctx, false); !errors.Is(e, ErrConflict) {
		t.Fatal("initializer overwrote state", e)
	}
	stale := fresh()
	if _, e := stale.Load(ctx); e != nil {
		t.Fatal(e)
	}
	if e := b.RestoreSnapshot(ctx, backup, initial); e != nil {
		t.Fatal(e)
	}
	_, current, e := b.ExportSnapshot(ctx)
	if e != nil || current == initial {
		t.Fatal("restore reused storage version", e)
	}
	if e := stale.Save(ctx, backup.Snapshot); !errors.Is(e, ErrConflict) {
		t.Fatal("pre-restore writer accepted", e)
	}
	if e := b.RestoreSnapshot(ctx, backup, initial); !errors.Is(e, ErrConflict) {
		t.Fatal("stale restore accepted", e)
	}
	mu.Lock()
	raceWrite = true
	mu.Unlock()
	if e := b.RestoreSnapshot(ctx, backup, current); !errors.Is(e, ErrConflict) {
		t.Fatal("write racing restore was overwritten", e)
	}
	_, current, e = b.ExportSnapshot(ctx)
	if e != nil {
		t.Fatal(e)
	}
	mu.Lock()
	before := writes
	mu.Unlock()
	for _, invalid := range []SnapshotBackup{{}, {Format: 2, Snapshot: backup.Snapshot}, {Format: 1, Snapshot: json.RawMessage(`null`)}, {Format: 1, Snapshot: json.RawMessage(`{"Schema":999}`)}} {
		if e := b.RestoreSnapshot(ctx, invalid, current); e == nil {
			t.Fatal("invalid backup accepted")
		}
	}
	if e := b.RestoreSnapshot(ctx, backup, ""); e == nil {
		t.Fatal("missing restore precondition accepted")
	}
	mu.Lock()
	after := writes
	mu.Unlock()
	if after != before {
		t.Fatal("invalid restore wrote state")
	}
	mu.Lock()
	ambiguous = true
	mu.Unlock()
	if e := b.RestoreSnapshot(ctx, backup, current); e == nil {
		t.Fatal("ambiguous restore acknowledged")
	}
	mu.Lock()
	ambiguous = false
	mu.Unlock()
	if e := b.Save(ctx, backup.Snapshot); e == nil {
		t.Fatal("ambiguous version reused")
	}
	got, _, e := b.ExportSnapshot(ctx)
	if e != nil || !sameStateJSON(got.Snapshot, backup.Snapshot) {
		t.Fatal("ambiguous restore not recoverable", e)
	}
	mu.Lock()
	state = nil
	legacy = backup.Snapshot
	before = writes
	mu.Unlock()
	if e := b.Save(ctx, backup.Snapshot); e == nil {
		t.Fatal("deleted state recreated")
	}
	if _, e := fresh().Load(ctx); !errors.Is(e, ErrNotFound) {
		t.Fatal("legacy fallback after deletion", e)
	}
	if e := b.RestoreSnapshot(ctx, backup, current); !errors.Is(e, ErrNotFound) {
		t.Fatal("restore recreated deleted state", e)
	}
	mu.Lock()
	after = writes
	mu.Unlock()
	if after != before {
		t.Fatal("missing-state check submitted a write")
	}
}
