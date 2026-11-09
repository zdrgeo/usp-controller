package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

// Delay a real transaction before dial, then inspect etcd's response separately
// from the backend's later cluster-ID check. An error alone is not a fence.
type cutoverTxnGate struct {
	next             http.RoundTripper
	entered, release chan struct{}
	response         chan bool
}

func (g *cutoverTxnGate) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Path != "/v3/kv/txn" {
		return g.next.RoundTrip(r)
	}
	close(g.entered)
	select {
	case <-g.release:
	case <-r.Context().Done():
		return nil, r.Context().Err()
	}
	response, err := g.next.RoundTrip(r)
	if err != nil {
		return response, err
	}
	data, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(data))
	var result struct {
		Succeeded bool `json:"succeeded"`
	}
	if response.StatusCode == http.StatusOK && json.Unmarshal(data, &result) == nil {
		g.response <- result.Succeeded
	}
	return response, nil
}

func testEtcdRestoreCutover(t *testing.T, source string, base device.DeviceIdentity) {
	ctx := context.Background()
	id := base
	id.AgentID = "restore-cutover-probe" // not in the Dapr inventory: no background writer
	fresh := func(urls []string, rt http.RoundTripper) *deviceactor.FencedDaprBackend {
		t.Helper()
		if rt == nil {
			tr := http.DefaultTransport.(*http.Transport).Clone()
			t.Cleanup(tr.CloseIdleConnections)
			rt = tr
		}
		b, err := deviceactor.NewEtcdSnapshotBackendEndpoints("http://127.0.0.1:1", "", urls, etcdPrefix, "", &http.Client{Transport: rt, Timeout: 5 * time.Second}, id, time.Second, 1<<20)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	target := docker(t, "port", source, "2379/tcp")
	original := fresh([]string{"http://" + target}, nil)
	seed, _ := json.Marshal(map[string]any{"Schema": 3, "Identity": id, "Desired": map[string]any{}, "Reported": map[string]any{}, "Receipts": map[string]any{}, "Commands": map[string]any{}, "Inbox": map[string]any{}})
	if err := original.ImportSnapshot(ctx, deviceactor.SnapshotBackup{Format: 1, Snapshot: seed}); err != nil {
		t.Fatal(err)
	}
	actor, err := deviceactor.New(id, restoredSnapshotBackend{original}, deviceactor.DefaultOptions())
	if err != nil {
		t.Fatal(err)
	}
	baseline := device.SetDesiredRequest{ID: "cutover-baseline", Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "backup", Type: "string"}}}
	if _, err := actor.SetDesired(ctx, baseline); err != nil {
		t.Fatal(err)
	}
	saved, backupETag, err := original.ExportSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	type route struct {
		proxies []*tcpPartition
		urls    []string
	}
	makeRoute := func() route {
		var r route
		for i := 0; i < 3; i++ {
			p := newTCPPartition(t, target)
			r.proxies = append(r.proxies, p)
			r.urls = append(r.urls, fmt.Sprintf("http://127.0.0.1:%d", p.listener.Addr().(*net.TCPAddr).Port))
		}
		return r
	}
	unsafeRoute, safeRoute := makeRoute(), makeRoute()
	unsafeOld := fresh(unsafeRoute.urls, nil)
	preSingle := fresh(safeRoute.urls[:1], nil)
	gateTransport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(gateTransport.CloseIdleConnections)
	gate := &cutoverTxnGate{next: gateTransport, entered: make(chan struct{}), release: make(chan struct{}), response: make(chan bool, 1)}
	delayed := fresh(safeRoute.urls, gate)
	for _, b := range []*deviceactor.FencedDaprBackend{unsafeOld, preSingle, delayed} {
		_, tag, err := b.ExportSnapshot(ctx)
		if err != nil || tag != backupETag {
			t.Fatalf("pre-backup version: %s %v", tag, err)
		}
	}
	// This dedicated key remains quiescent until the native backup completes.
	dir := t.TempDir()
	docker(t, "exec", source, "/usr/local/bin/etcdctl", "snapshot", "save", "/tmp/cutover-backup.db", "--command-timeout=30s")
	docker(t, "cp", source+":/tmp/cutover-backup.db", filepath.Join(dir, "snapshot.db"))
	later := device.SetDesiredRequest{ID: "cutover-source-later", ExpectedRevision: 1, Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "source-later", Type: "string"}}}
	if _, err := actor.SetDesired(ctx, later); err != nil {
		t.Fatal(err)
	}
	postSingle, postMulti := fresh(safeRoute.urls[:1], nil), fresh(safeRoute.urls, nil)
	for _, b := range []*deviceactor.FencedDaprBackend{postSingle, postMulti} {
		if _, err := b.Load(ctx); err != nil {
			t.Fatal(err)
		}
	}
	_, sourceETag, err := original.ExportSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sourceVersion, err := strconv.ParseInt(sourceETag, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	switchRoute := func(r route, urls []string) {
		for _, p := range r.proxies {
			p.setBlocked(true)
		}
		for i, p := range r.proxies {
			p.setTarget(strings.TrimPrefix(urls[i], "http://"))
		}
		for _, p := range r.proxies {
			p.setBlocked(false)
		}
	}
	if !t.Run("unguarded error is not fencing", func(t *testing.T) {
		_, urls := restoreLiveEtcdSnapshot(t, dir, 1000000)
		inspect := fresh(urls, nil)
		eventually(t, "unguarded restored key readable", func() bool { _, err := inspect.Load(ctx); return err == nil })
		_, tag, err := inspect.ExportSnapshot(ctx)
		if err != nil || tag != backupETag {
			t.Fatalf("native restore did not preserve old key revision: %s %v", tag, err)
		}
		switchRoute(unsafeRoute, urls)
		poison := []byte(`{"unguarded_stale_write":true}`)
		err = unsafeOld.Save(ctx, poison)
		if err == nil || !strings.Contains(err.Error(), "cluster identity") {
			t.Fatalf("expected post-response cluster mismatch: %v", err)
		}
		actual, err := inspect.Load(ctx)
		if err != nil || !bytes.Equal(actual, poison) {
			t.Fatalf("negative control did not expose committed stale write: %s %v", actual, err)
		}
		t.Log("unsupported raw cutover returned an error after committing: revision bump and response cluster-ID checks are not write fences")
	}) {
		return
	}
	t.Run("guarded cutover fences stale transactions", func(t *testing.T) {
		_, urls := restoreLiveEtcdSnapshot(t, dir, 1000000)
		admin := fresh(urls, nil)
		eventually(t, "private restored target readable", func() bool { _, err := admin.Load(ctx); return err == nil })
		current, tag, err := admin.ExportSnapshot(ctx)
		if err != nil || tag != backupETag {
			t.Fatalf("private restore baseline: %s %v", tag, err)
		}
		// Admission is still closed: routes point to the source, whose test actor is
		// retired. Rewrite every key in the admitted inventory (this one probe) using
		// the existing conditional admin API before exposing the restored authority.
		if err := admin.RestoreSnapshot(ctx, current, tag); err != nil {
			t.Fatal(err)
		}
		protected, newTag, err := admin.ExportSnapshot(ctx)
		if err != nil {
			t.Fatal(err)
		}
		version, err := strconv.ParseInt(newTag, 10, 64)
		if err != nil || version <= sourceVersion {
			t.Fatalf("cutover fence did not exceed observed source versions: %s %v", newTag, err)
		}
		unchanged := func() {
			t.Helper()
			actual, etag, err := admin.ExportSnapshot(ctx)
			if err != nil || etag != newTag || !bytes.Equal(actual.Snapshot, protected.Snapshot) {
				t.Fatalf("stale attempt changed restored bytes or version: %s %v", etag, err)
			}
		}
		// Hold a pre-backup writer before its transaction reaches the network, then
		// cut over the unchanged URLs and release the real CAS into the new cluster.
		done := make(chan struct{})
		var saveErr error
		var release sync.Once
		go func() { saveErr = delayed.Save(ctx, []byte(`{"delayed_stale_write":true}`)); close(done) }()
		t.Cleanup(func() { release.Do(func() { close(gate.release) }); <-done })
		select {
		case <-gate.entered:
		case <-time.After(2 * time.Second):
			t.Fatal("transaction did not reach gate")
		}
		switchRoute(safeRoute, urls)
		release.Do(func() { close(gate.release) })
		<-done
		if saveErr == nil {
			t.Fatal("delayed old writer acknowledged after cutover")
		}
		select {
		case succeeded := <-gate.response:
			if succeeded {
				t.Fatal("delayed CAS committed despite returned error")
			}
		default:
			t.Fatalf("delayed transaction never reached etcd: %v", saveErr)
		}
		unchanged()
		for _, b := range []*deviceactor.FencedDaprBackend{preSingle, postSingle} {
			if err := b.Save(ctx, []byte(`{"stale_write":true}`)); !errors.Is(err, deviceactor.ErrConflict) {
				t.Fatalf("single-endpoint stale version was not rejected by CAS: %v", err)
			}
			unchanged()
		}
		if err := postMulti.Save(ctx, []byte(`{"stale_write":true}`)); err == nil || !strings.Contains(err.Error(), "cluster identity") {
			t.Fatalf("old multi-endpoint writer did not observe restored cluster: %v", err)
		}
		unchanged()
		if _, err := postMulti.Load(ctx); err == nil || !strings.Contains(err.Error(), "cluster identity") {
			t.Fatal("old pinned backend adopted restored cluster")
		}
		if err := admin.RestoreSnapshot(ctx, saved, backupETag); !errors.Is(err, deviceactor.ErrConflict) {
			t.Fatalf("historical restore authorization accepted: %v", err)
		}
		unchanged()
		// Fresh processes use the stable cutover URLs. No source backend is reused.
		freshBackend := fresh(safeRoute.urls, nil)
		recovered, err := deviceactor.New(id, restoredSnapshotBackend{freshBackend}, deviceactor.DefaultOptions())
		if err != nil {
			t.Fatal(err)
		}
		twin, err := recovered.GetTwin(ctx)
		if err != nil || twin.DesiredRevision != 1 || twin.Desired["Device.Test.Value"].Value != "backup" {
			t.Fatalf("cutover twin: %+v %v", twin, err)
		}
		if receipt, err := recovered.SetDesired(ctx, baseline); err != nil || receipt.ID != baseline.ID || receipt.Revision != 1 {
			t.Fatalf("cutover receipt: %+v %v", receipt, err)
		}
		freshStale := fresh(safeRoute.urls, nil)
		staleData, err := freshStale.Load(ctx)
		if err != nil {
			t.Fatal(err)
		}
		recovery := device.SetDesiredRequest{ID: "cutover-recovery", ExpectedRevision: 1, Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "recovered", Type: "string"}}}
		for i := 0; i < 2; i++ {
			if receipt, err := recovered.SetDesired(ctx, recovery); err != nil || receipt.ID != recovery.ID || receipt.Revision != 2 {
				t.Fatalf("new admission/receipt: %+v %v", receipt, err)
			}
		}
		if err := freshStale.Save(ctx, staleData); !errors.Is(err, deviceactor.ErrConflict) {
			t.Fatalf("fresh-cohort stale writer accepted: %v", err)
		}
		twin, err = recovered.GetTwin(ctx)
		if err != nil || twin.DesiredRevision != 2 || twin.Desired["Device.Test.Value"].Value != "recovered" {
			t.Fatalf("recovered state changed: %+v %v", twin, err)
		}
		sourceTwin, err := actor.GetTwin(ctx)
		if err != nil || sourceTwin.DesiredRevision != 2 || sourceTwin.Desired["Device.Test.Value"].Value != "source-later" {
			t.Fatalf("cutover changed retired source: %+v %v", sourceTwin, err)
		}
		t.Log("conditional per-key rewrite fenced pre/post-backup versions and a delayed real CAS; fresh clients recovered twin, receipts and admission")
	})
}
