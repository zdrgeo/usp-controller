package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

// Hold only the first mutation; subsequent calls exercise the same actor/backend
// after its failed write has invalidated the loaded version.
type compactionTxnGate struct {
	*cutoverTxnGate
	intercepted atomic.Bool
	attempts    atomic.Int32
}

func (g *compactionTxnGate) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path == "/v3/kv/txn" {
		g.attempts.Add(1)
		if !g.intercepted.Swap(true) {
			return g.cutoverTxnGate.RoundTrip(r)
		}
	}
	return g.next.RoundTrip(r)
}

func testEtcdCompaction(t *testing.T, members []string, base device.DeviceIdentity) {
	var endpoints []string
	for _, member := range members {
		endpoints = append(endpoints, "http://"+docker(t, "port", member, "2379/tcp"))
	}
	for _, physical := range []bool{false, true} {
		for _, count := range []int{1, len(endpoints)} {
			t.Run(fmt.Sprintf("physical-%t/endpoints-%d", physical, count), func(t *testing.T) {
				ctx := context.Background()
				id := base
				id.AgentID = fmt.Sprintf("compaction-%t-%d", physical, count) // no Dapr background writer
				fresh := func(rt http.RoundTripper) *deviceactor.FencedDaprBackend {
					t.Helper()
					if rt == nil {
						tr := http.DefaultTransport.(*http.Transport).Clone()
						t.Cleanup(tr.CloseIdleConnections)
						rt = tr
					}
					b, err := deviceactor.NewEtcdSnapshotBackendEndpoints("http://127.0.0.1:1", "", endpoints[:count], etcdPrefix, "", &http.Client{Transport: rt, Timeout: 5 * time.Second}, id, time.Second, 1<<20)
					if err != nil {
						t.Fatal(err)
					}
					return b
				}
				inspect := fresh(nil)
				seed, _ := json.Marshal(map[string]any{"Schema": 3, "Identity": id, "Desired": map[string]any{}, "Reported": map[string]any{}, "Receipts": map[string]any{}, "Commands": map[string]any{}, "Inbox": map[string]any{}})
				if err := inspect.ImportSnapshot(ctx, deviceactor.SnapshotBackup{Format: 1, Snapshot: seed}); err != nil {
					t.Fatal(err)
				}
				baselineActor, err := deviceactor.New(id, restoredSnapshotBackend{inspect}, deviceactor.DefaultOptions())
				if err != nil {
					t.Fatal(err)
				}
				baseline := device.SetDesiredRequest{ID: "compaction-baseline", Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "baseline", Type: "string"}}}
				if _, err := baselineActor.SetDesired(ctx, baseline); err != nil {
					t.Fatal(err)
				}
				before, etag, err := inspect.ExportSnapshot(ctx)
				if err != nil {
					t.Fatal(err)
				}
				stale := fresh(nil)
				if _, err := stale.Load(ctx); err != nil {
					t.Fatal(err)
				}
				tr := http.DefaultTransport.(*http.Transport).Clone()
				t.Cleanup(tr.CloseIdleConnections)
				gate := &compactionTxnGate{cutoverTxnGate: &cutoverTxnGate{next: tr, entered: make(chan struct{}), release: make(chan struct{}), response: make(chan bool, 1)}}
				actor, err := deviceactor.New(id, restoredSnapshotBackend{fresh(gate)}, deviceactor.DefaultOptions())
				if err != nil {
					t.Fatal(err)
				}
				request := device.SetDesiredRequest{ID: "compaction-recovery", ExpectedRevision: 1, Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "recovered", Type: "string"}}}
				done := make(chan struct{})
				var writeErr error
				var release sync.Once
				go func() { _, writeErr = actor.SetDesired(ctx, request); close(done) }()
				t.Cleanup(func() { release.Do(func() { close(gate.release) }); <-done })
				select {
				case <-gate.entered:
				case <-time.After(2 * time.Second):
					t.Fatal("actor write did not reach transaction gate")
				}
				// Advance unrelated state, leaving the snapshot CAS revision unchanged.
				raw := docker(t, "exec", members[0], "/usr/local/bin/etcdctl", "put", etcdPrefix+"compaction-clock", id.AgentID, "--write-out=json")
				var result struct {
					Header struct {
						Revision int64 `json:"revision"`
					} `json:"header"`
				}
				if err := json.Unmarshal([]byte(raw), &result); err != nil || result.Header.Revision <= 0 {
					t.Fatalf("invalid revision: %s %v", raw, err)
				}
				args := []string{"exec", members[0], "/usr/local/bin/etcdctl", "compact", strconv.FormatInt(result.Header.Revision, 10), "--command-timeout=3s"}
				if physical {
					args = append(args, "--physical")
				}
				docker(t, args...)
				release.Do(func() { close(gate.release) })
				<-done
				if writeErr == nil || !strings.Contains(gate.errorBody, "required revision has been compacted") || gate.attempts.Load() != 1 {
					t.Fatalf("expected one atomically rejected write: %v %s attempts=%d", writeErr, gate.errorBody, gate.attempts.Load())
				}
				unchanged := func() {
					t.Helper()
					actual, tag, err := inspect.ExportSnapshot(ctx)
					if err != nil || tag != etag || !bytes.Equal(actual.Snapshot, before.Snapshot) {
						t.Fatalf("compaction/rejected write changed snapshot or receipt: %s %v", tag, err)
					}
				}
				unchanged()
				if err := stale.Save(ctx, []byte(`{"stale":true}`)); err == nil {
					t.Fatal("cached writer survived compaction")
				}
				unchanged()
				if err := stale.Save(ctx, []byte(`{"stale":true}`)); err == nil {
					t.Fatal("failed version reused")
				}
				unchanged()
				// Reload the same unchanged key at the new global revision and retry the
				// same request ID. Its receipt must be persisted once, not reconstructed.
				var recoveredTag string
				for i := 0; i < 2; i++ {
					receipt, err := actor.SetDesired(ctx, request)
					if err != nil || receipt.ID != request.ID || receipt.Revision != 2 {
						t.Fatalf("recovery receipt: %+v %v", receipt, err)
					}
					_, tag, err := inspect.ExportSnapshot(ctx)
					if err != nil || tag == etag || (i == 1 && tag != recoveredTag) {
						t.Fatalf("recovery/retry storage revision: %s %v", tag, err)
					}
					recoveredTag = tag
				}
				if gate.attempts.Load() != 2 {
					t.Fatalf("receipt retry wrote again: %d transactions", gate.attempts.Load())
				}
				reader, err := deviceactor.New(id, restoredSnapshotBackend{fresh(nil)}, deviceactor.DefaultOptions())
				if err != nil {
					t.Fatal(err)
				}
				twin, err := reader.GetTwin(ctx)
				if err != nil || twin.DesiredRevision != 2 || twin.Desired["Device.Test.Value"].Value != "recovered" {
					t.Fatalf("fresh reader recovery: %+v %v", twin, err)
				}
				if receipt, err := reader.SetDesired(ctx, baseline); err != nil || receipt.Revision != 1 {
					t.Fatalf("baseline receipt lost: %+v %v", receipt, err)
				}
				t.Log("compaction rejected the held transaction without changing bytes or ETag; same actor reloaded and recovered one durable receipt")
			})
		}
	}
}
