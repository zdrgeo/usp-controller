package integration

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

// Observe the real gateway response without changing requests or retry policy.
// errorBody is read only after the actor call completes.
type autoCompactionProbe struct {
	next         http.RoundTripper
	revision     atomic.Int64
	transactions atomic.Int32
	errorBody    string
}

func (p *autoCompactionProbe) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path == "/v3/kv/txn" {
		p.transactions.Add(1)
	}
	response, err := p.next.RoundTrip(r)
	if err != nil {
		return response, err
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	response.Body.Close()
	if err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	if r.URL.Path == "/v3/kv/txn" {
		p.errorBody = string(data)
	}
	if r.URL.Path == "/v3/kv/range" && response.StatusCode == http.StatusOK {
		var result struct {
			Header struct {
				Revision string `json:"revision"`
			} `json:"header"`
		}
		if json.Unmarshal(data, &result) == nil {
			revision, _ := strconv.ParseInt(result.Header.Revision, 10, 64)
			p.revision.Store(revision)
		}
	}
	return response, nil
}

// Auto compaction is enabled only on this isolated cohort, never on the main
// fixture used for outage/restore tests. The pinned server uses its real clock.
func startAutoCompactionEtcd(t *testing.T, mode, retention string) ([]string, []string) {
	t.Helper()
	network := fmt.Sprintf("usp-dapr-test-autocompact-%d", time.Now().UnixNano())
	docker(t, "network", "create", network)
	t.Cleanup(func() { docker(t, "network", "rm", network) })
	cluster := "auto0=http://auto0:2380,auto1=http://auto1:2380,auto2=http://auto2:2380"
	var members, endpoints []string
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("auto%d", i)
		full := network + "-" + name
		docker(t, "run", "-d", "--name", full, "--network", network, "--network-alias", name, "-p", "127.0.0.1::2379", "gcr.io/etcd-development/etcd:v3.5.17", "/usr/local/bin/etcd", "--name", name, "--listen-client-urls", "http://0.0.0.0:2379", "--advertise-client-urls", "http://"+name+":2379", "--listen-peer-urls", "http://0.0.0.0:2380", "--initial-advertise-peer-urls", "http://"+name+":2380", "--initial-cluster", cluster, "--initial-cluster-token", network, "--data-dir", "/tmp/etcd", "--auto-compaction-mode", mode, "--auto-compaction-retention", retention)
		t.Cleanup(func() {
			if t.Failed() {
				out, _ := exec.Command("docker", "logs", "--tail", "80", full).CombinedOutput()
				t.Logf("%s: %s", full, out)
			}
			docker(t, "rm", "-f", full)
		})
		members = append(members, full)
		endpoints = append(endpoints, "http://"+docker(t, "port", full, "2379/tcp"))
	}
	client := &http.Client{Timeout: time.Second}
	eventually(t, "automatic compaction cluster healthy", func() bool {
		for _, endpoint := range endpoints {
			response, err := client.Get(endpoint + "/health")
			if err != nil {
				return false
			}
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				return false
			}
		}
		return true
	})
	electedLeader(t, members, []int{0, 1, 2}, 0)
	return members, endpoints
}

func historyCompacted(client *http.Client, endpoint, key string, revision int64) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	data, _ := json.Marshal(map[string]any{"key": base64.StdEncoding.EncodeToString([]byte(key)), "revision": strconv.FormatInt(revision, 10), "serializable": true, "count_only": true})
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint+"/v3/kv/range", bytes.NewReader(data))
	if err != nil {
		return false, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return false, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		return false, err
	}
	var result struct {
		Code   int    `json:"code"`
		Error  string `json:"error"`
		Header struct {
			Revision string `json:"revision"`
		} `json:"header"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		return false, err
	}
	if response.StatusCode == http.StatusOK && result.Header.Revision != "" {
		return false, nil
	}
	if response.StatusCode == http.StatusBadRequest && result.Code == 11 && strings.Contains(result.Error, "required revision has been compacted") {
		return true, nil
	}
	return false, fmt.Errorf("historical range HTTP %d: %s", response.StatusCode, body)
}

func testEtcdAutomaticCompaction(t *testing.T, base device.DeviceIdentity) {
	members, endpoints := startAutoCompactionEtcd(t, "periodic", "2s")
	for _, count := range []int{1, 3} {
		t.Run(fmt.Sprintf("endpoints-%d", count), func(t *testing.T) {
			ctx := context.Background()
			id := base
			id.AgentID = fmt.Sprintf("automatic-compaction-%d", count)
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
			tr := http.DefaultTransport.(*http.Transport).Clone()
			t.Cleanup(tr.CloseIdleConnections)
			probe := &autoCompactionProbe{next: tr}
			backend := fresh(probe)
			gate := &heldSave{Backend: restoredSnapshotBackend{backend}}
			actor, err := deviceactor.New(id, gate, deviceactor.DefaultOptions())
			if err != nil {
				t.Fatal(err)
			}
			baseline := device.SetDesiredRequest{ID: "automatic-baseline", Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "baseline", Type: "string"}}}
			if _, err := actor.SetDesired(ctx, baseline); err != nil {
				t.Fatal(err)
			}
			requests := []device.SetDesiredRequest{baseline}
			for round := 0; round < 2; round++ {
				before, etag, err := inspect.ExportSnapshot(ctx)
				if err != nil {
					t.Fatal(err)
				}
				stale := fresh(nil)
				if _, err := stale.Load(ctx); err != nil {
					t.Fatal(err)
				}
				request := device.SetDesiredRequest{ID: device.RequestID(fmt.Sprintf("automatic-recovery-%d", round)), ExpectedRevision: uint64(round + 1), Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: fmt.Sprintf("recovered-%d", round), Type: "string"}}}
				gate.arm(string(request.ID))
				releaseChannel := gate.release
				attempts := probe.transactions.Load()
				done := make(chan struct{})
				var writeErr error
				var release sync.Once
				go func() { _, writeErr = actor.SetDesired(ctx, request); close(done) }()
				// Join even on a failed assertion; the held Save detaches cancellation.
				t.Cleanup(func() { release.Do(func() { close(releaseChannel) }); <-done })
				select {
				case <-gate.entered:
				case <-time.After(2 * time.Second):
					t.Fatal("actor did not reach held snapshot save")
				}
				witness := probe.revision.Load()
				if witness <= 0 {
					t.Fatal("missing captured global read revision")
				}
				if compacted, err := historyCompacted(&http.Client{Transport: tr}, endpoints[0], etcdPrefix+deviceactor.ActorID(id), witness); err != nil || compacted {
					t.Fatalf("fresh witness not readable before test advancement: compacted=%t %v", compacted, err)
				}
				// Only unrelated writes advance MVCC. No compact RPC is issued anywhere in
				// this fixture: the server's periodic retention policy must expire history.
				raw := docker(t, "exec", members[0], "/usr/local/bin/etcdctl", "put", etcdPrefix+"automatic-clock", string(request.ID), "--write-out=json")
				var advanced struct {
					Header struct {
						Revision int64 `json:"revision"`
					} `json:"header"`
				}
				if json.Unmarshal([]byte(raw), &advanced) != nil || advanced.Header.Revision <= witness {
					t.Fatalf("clock did not advance beyond witness %d: %s", witness, raw)
				}
				eventually(t, "automatic compactor expires witness on every member", func() bool {
					for _, endpoint := range endpoints {
						if compacted, err := historyCompacted(&http.Client{Transport: tr}, endpoint, etcdPrefix+deviceactor.ActorID(id), witness); err != nil || !compacted {
							return false
						}
					}
					return true
				})
				release.Do(func() { close(releaseChannel) })
				<-done
				if writeErr == nil || !strings.Contains(probe.errorBody, "required revision has been compacted") || probe.transactions.Load() != attempts+1 {
					t.Fatalf("expected one compacted-history rejection: %v %s", writeErr, probe.errorBody)
				}
				unchanged := func() {
					t.Helper()
					actual, tag, err := inspect.ExportSnapshot(ctx)
					if err != nil || tag != etag || !bytes.Equal(actual.Snapshot, before.Snapshot) {
						t.Fatalf("expired write changed snapshot/receipt or ETag: %s %v", tag, err)
					}
				}
				unchanged()
				if err := stale.Save(ctx, []byte(`{"stale":true}`)); err == nil {
					t.Fatal("cached writer survived automatic compaction")
				}
				unchanged()
				if err := backend.Save(ctx, []byte(`{"reuse":true}`)); err == nil || probe.transactions.Load() != attempts+1 {
					t.Fatal("failed version reused or transaction replayed")
				}
				unchanged()
				var recoveredTag string
				for duplicate := 0; duplicate < 2; duplicate++ {
					receipt, err := actor.SetDesired(ctx, request)
					if err != nil || receipt.ID != request.ID || receipt.Revision != uint64(round+2) {
						t.Fatalf("automatic recovery receipt: %+v %v", receipt, err)
					}
					_, tag, err := inspect.ExportSnapshot(ctx)
					if err != nil || tag == etag || (duplicate == 1 && tag != recoveredTag) {
						t.Fatalf("duplicate mutated durable receipt: %s %v", tag, err)
					}
					recoveredTag = tag
				}
				if probe.transactions.Load() != attempts+2 {
					t.Fatal("duplicate request submitted another transaction")
				}
				requests = append(requests, request)
				reader, err := deviceactor.New(id, restoredSnapshotBackend{fresh(nil)}, deviceactor.DefaultOptions())
				if err != nil {
					t.Fatal(err)
				}
				twin, err := reader.GetTwin(ctx)
				if err != nil || twin.DesiredRevision != uint64(round+2) || twin.Desired["Device.Test.Value"].Value != fmt.Sprintf("recovered-%d", round) {
					t.Fatalf("fresh twin after retention cycle: %+v %v", twin, err)
				}
				for i, prior := range requests {
					receipt, err := reader.SetDesired(ctx, prior)
					if err != nil || receipt.ID != prior.ID || receipt.Revision != uint64(i+1) {
						t.Fatalf("receipt lost across retention cycles: %+v %v", receipt, err)
					}
				}
				t.Logf("cycle %d: witness %d expired on all members; same actor recovered once, twin and historical receipts retained", round+1, witness)
			}
		})
	}
	logs := docker(t, "logs", members[0])
	// The leader may be any member; require server-side evidence from the cohort.
	for _, member := range members[1:] {
		logs += docker(t, "logs", member)
	}
	if !strings.Contains(logs, "completed auto periodic compaction") {
		t.Fatal("no automatic compactor completion recorded")
	}
}
