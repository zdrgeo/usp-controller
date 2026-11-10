package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

// The unmodified v3.5.17 revision compactor wakes every five minutes. This
// independent fixture overlaps TestLiveDapr; no fake clock or compact RPC is used.
func TestLiveEtcdRevisionCompaction(t *testing.T) {
	if os.Getenv("USP_TEST_DAPR") != "1" {
		t.Skip("set USP_TEST_DAPR=1 to run isolated live etcd tests")
	}
	t.Parallel()
	const retention int64 = 8
	ctx := context.Background()
	members, endpoints := startAutoCompactionEtcd(t, "revision", fmt.Sprint(retention))
	tr := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(tr.CloseIdleConnections)
	client := &http.Client{Transport: tr, Timeout: time.Second}
	type cohort struct {
		id                      device.DeviceIdentity
		actor                   *deviceactor.Actor
		backend, inspect, stale *deviceactor.FencedDaprBackend
		probe                   *autoCompactionProbe
		before                  deviceactor.SnapshotBackup
		etag                    string
		witness                 int64
		request                 device.SetDesiredRequest
		done                    chan struct{}
		release                 func()
		writeErr                error
	}
	fresh := func(id device.DeviceIdentity, count int, rt http.RoundTripper) *deviceactor.FencedDaprBackend {
		t.Helper()
		b, err := deviceactor.NewEtcdSnapshotBackendEndpoints("http://127.0.0.1:1", "", endpoints[:count], etcdPrefix, "", &http.Client{Transport: rt, Timeout: 5 * time.Second}, id, time.Second, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	var cohorts []*cohort
	baseline := device.SetDesiredRequest{ID: "revision-baseline", Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "baseline", Type: "string"}}}
	for _, count := range []int{1, 3} {
		c := &cohort{id: device.DeviceIdentity{Tenant: "test", ControllerID: "revision-compaction", AgentID: fmt.Sprintf("endpoints-%d", count)}, done: make(chan struct{})}
		c.inspect = fresh(c.id, count, tr)
		seed, _ := json.Marshal(map[string]any{"Schema": 3, "Identity": c.id, "Desired": map[string]any{}, "Reported": map[string]any{}, "Receipts": map[string]any{}, "Commands": map[string]any{}, "Inbox": map[string]any{}})
		if err := c.inspect.ImportSnapshot(ctx, deviceactor.SnapshotBackup{Format: 1, Snapshot: seed}); err != nil {
			t.Fatal(err)
		}
		c.probe = &autoCompactionProbe{next: tr}
		c.backend = fresh(c.id, count, c.probe)
		gate := &heldSave{Backend: restoredSnapshotBackend{c.backend}}
		var err error
		c.actor, err = deviceactor.New(c.id, gate, deviceactor.DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := c.actor.SetDesired(ctx, baseline); err != nil {
			t.Fatal(err)
		}
		c.before, c.etag, err = c.inspect.ExportSnapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		c.stale = fresh(c.id, count, tr)
		if _, err := c.stale.Load(ctx); err != nil {
			t.Fatal(err)
		}
		c.request = device.SetDesiredRequest{ID: "revision-recovery", ExpectedRevision: 1, Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "recovered", Type: "string"}}}
		gate.arm(string(c.request.ID))
		var once sync.Once
		c.release = func() { once.Do(func() { close(gate.release) }) }
		go func() { _, c.writeErr = c.actor.SetDesired(ctx, c.request); close(c.done) }()
		t.Cleanup(func() { c.release(); <-c.done })
		select {
		case <-gate.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("actor did not reach snapshot gate")
		}
		c.witness = c.probe.revision.Load()
		if c.witness <= 1 {
			t.Fatalf("missing captured witness: %d", c.witness)
		}
		cohorts = append(cohorts, c)
	}
	minWitness, maxWitness := cohorts[0].witness, cohorts[0].witness
	for _, c := range cohorts[1:] {
		if c.witness < minWitness {
			minWitness = c.witness
		}
		if c.witness > maxWitness {
			maxWitness = c.witness
		}
	}
	// Hold both clients across the same two real scheduler ticks. First target is
	// below either witness, so elapsed time alone must not expire those revisions.
	retainedCurrent := minWitness + retention - 1
	if retainedCurrent < maxWitness {
		t.Fatal("test cohort exceeds configured retention window")
	}
	advance := func(target int64) int64 {
		t.Helper()
		for {
			raw := docker(t, "exec", members[0], "/usr/local/bin/etcdctl", "put", etcdPrefix+"revision-clock", fmt.Sprint(target), "--write-out=json")
			var result struct {
				Header struct {
					Revision int64 `json:"revision"`
				} `json:"header"`
			}
			if err := json.Unmarshal([]byte(raw), &result); err != nil || result.Header.Revision <= 0 {
				t.Fatalf("invalid revision: %s %v", raw, err)
			}
			if result.Header.Revision > target {
				t.Fatalf("unexpected background writer advanced to %d past %d", result.Header.Revision, target)
			}
			if result.Header.Revision == target {
				return result.Header.Revision
			}
		}
	}
	checkHistory := func(revision int64, wantCompacted bool) error {
		for _, endpoint := range endpoints {
			compacted, err := historyCompacted(client, endpoint, etcdPrefix+"revision-clock", revision)
			if err != nil {
				return err
			}
			if compacted != wantCompacted {
				return fmt.Errorf("%s revision %d compacted=%t, want %t", endpoint, revision, compacted, wantCompacted)
			}
		}
		return nil
	}
	unchanged := func(c *cohort) {
		t.Helper()
		actual, tag, err := c.inspect.ExportSnapshot(ctx)
		if err != nil || tag != c.etag || !bytes.Equal(actual.Snapshot, c.before.Snapshot) {
			t.Fatalf("%s retention/stale write changed bytes, receipts or ETag: %s %v", c.id.AgentID, tag, err)
		}
	}
	waitForHistory := func(description string, revision int64) {
		t.Helper()
		deadline := time.Now().Add(6 * time.Minute)
		var lastErr error
		for {
			lastErr = checkHistory(revision, true)
			if lastErr == nil {
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s: %v", description, lastErr)
			}
			for _, c := range cohorts {
				select {
				case <-c.done:
					t.Fatalf("held actor returned before release: %v", c.writeErr)
				default:
				}
			}
			time.Sleep(time.Second)
		}
	}
	// Prove history is initially available, then await an actual auto compaction
	// of revision 1 while both actor witnesses remain inside the count window.
	if err := checkHistory(1, false); err != nil {
		t.Fatal(err)
	}
	advance(retainedCurrent)
	retainedTarget := retainedCurrent - retention
	waitForHistory("first automatic revision compaction", 1)
	for _, c := range cohorts {
		if err := checkHistory(c.witness, false); err != nil {
			t.Fatalf("within-window witness expired: %v", err)
		}
		if c.probe.transactions.Load() != 1 {
			t.Fatal("held actor submitted a transaction")
		}
		unchanged(c)
	}
	t.Logf("first scheduler tick: compacted target %d; witnesses %d..%d remain readable on all members despite elapsed time", retainedTarget, minWitness, maxWitness)
	// Only unrelated writes move the retention threshold beyond both witnesses.
	expiredCurrent := advance(maxWitness + retention + 1)
	expiredTarget := expiredCurrent - retention
	waitForHistory("second automatic revision compaction", maxWitness)
	for _, c := range cohorts {
		if err := checkHistory(c.witness, true); err != nil {
			t.Fatal(err)
		}
		unchanged(c)
	}
	if err := checkHistory(expiredCurrent-1, false); err != nil {
		t.Fatalf("recent history inside retention was lost: %v", err)
	}
	for _, c := range cohorts {
		c.release()
	}
	for _, c := range cohorts {
		<-c.done
		if c.writeErr == nil || !strings.Contains(c.probe.errorBody, "required revision has been compacted") || c.probe.transactions.Load() != 2 {
			t.Fatalf("%s expected one compacted-history rejection: %v %s", c.id.AgentID, c.writeErr, c.probe.errorBody)
		}
		unchanged(c)
		if err := c.stale.Save(ctx, []byte(`{"stale":true}`)); err == nil {
			t.Fatal("cached writer survived revision-count compaction")
		}
		unchanged(c)
		if err := c.backend.Save(ctx, []byte(`{"reuse":true}`)); err == nil || c.probe.transactions.Load() != 2 {
			t.Fatal("failed version reused or transaction replayed")
		}
		unchanged(c)
		var recoveryTag string
		for duplicate := 0; duplicate < 2; duplicate++ {
			receipt, err := c.actor.SetDesired(ctx, c.request)
			if err != nil || receipt.ID != c.request.ID || receipt.Revision != 2 {
				t.Fatalf("%s recovery receipt: %+v %v", c.id.AgentID, receipt, err)
			}
			_, tag, err := c.inspect.ExportSnapshot(ctx)
			if err != nil || tag == c.etag || (duplicate == 1 && tag != recoveryTag) {
				t.Fatalf("duplicate changed recovered ETag: %s %v", tag, err)
			}
			recoveryTag = tag
		}
		if c.probe.transactions.Load() != 3 {
			t.Fatal("duplicate request submitted another transaction")
		}
		reader, err := deviceactor.New(c.id, restoredSnapshotBackend{fresh(c.id, len(endpoints), tr)}, deviceactor.DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		twin, err := reader.GetTwin(ctx)
		if err != nil || twin.DesiredRevision != 2 || twin.Desired["Device.Test.Value"].Value != "recovered" {
			t.Fatalf("fresh reader twin: %+v %v", twin, err)
		}
		for i, request := range []device.SetDesiredRequest{baseline, c.request} {
			receipt, err := reader.SetDesired(ctx, request)
			if err != nil || receipt.ID != request.ID || receipt.Revision != uint64(i+1) {
				t.Fatalf("fresh reader receipt: %+v %v", receipt, err)
			}
		}
		t.Logf("%s: expired witness rejected atomically; same actor recovered once and fresh reader retained both receipts", c.id.AgentID)
	}
	// Logs independently identify count-based compaction and both expected targets.
	completed := map[int64]bool{}
	for _, member := range members {
		for _, line := range strings.Split(docker(t, "logs", member), "\n") {
			var entry struct {
				Message   string `json:"msg"`
				Revision  int64  `json:"revision"`
				Retention int64  `json:"revision-compaction-retention"`
			}
			if json.Unmarshal([]byte(line), &entry) == nil && entry.Message == "completed auto revision compaction" && entry.Retention == retention {
				completed[entry.Revision] = true
			}
		}
	}
	if !completed[retainedTarget] || !completed[expiredTarget] {
		t.Fatalf("missing count-compactor targets %d and %d: %v", retainedTarget, expiredTarget, completed)
	}
}
