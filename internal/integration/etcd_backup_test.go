package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

// Restored actors are offline probes, never registered with the source Dapr
// deployment. No reminder, publisher or device transport can dispatch old work.
type restoredSnapshotBackend struct{ *deviceactor.FencedDaprBackend }

func (b restoredSnapshotBackend) EnsureReminder(context.Context) error { return nil }

func testEtcdSnapshotBackupRestore(t *testing.T, hosts []*liveHost, source string, id device.DeviceIdentity) {
	const etcdImage = "gcr.io/etcd-development/etcd:v3.5.17"
	const bump = int64(1000000)
	baseline := etcdMutation(t, hosts[0], id, "backup-baseline")
	assertEtcdReceipt(t, hosts, id, baseline)
	original := memberStatus(t, source)
	// A linearizable read on the chosen backup member catches up its applied
	// state before taking the point-in-time native snapshot.
	docker(t, "exec", source, "/usr/local/bin/etcdctl", "get", etcdPrefix+deviceactor.ActorID(id), "--write-out=json")
	dir := t.TempDir()
	docker(t, "exec", source, "/usr/local/bin/etcdctl", "snapshot", "save", "/tmp/live-backup.db", "--command-timeout=30s")
	docker(t, "cp", source+":/tmp/live-backup.db", filepath.Join(dir, "snapshot.db"))
	raw := docker(t, "run", "--rm", "--network", "none", "-v", dir+":/backup:ro", etcdImage, "/usr/local/bin/etcdutl", "snapshot", "status", "/backup/snapshot.db", "--write-out=json")
	var backup struct {
		Hash     uint64 `json:"hash"`
		Revision int64  `json:"revision"`
		TotalKey int64  `json:"totalKey"`
	}
	if e := json.Unmarshal([]byte(raw), &backup); e != nil || backup.Revision <= 0 || backup.TotalKey == 0 || backup.Hash == 0 {
		t.Fatalf("invalid native snapshot status: %s %v", raw, e)
	}
	// Source advances after capture. Those writes must NOT appear in the restore.
	later := etcdMutation(t, hosts[0], id, "backup-after-capture")
	assertEtcdReceipt(t, hosts, id, later)
	sourceState := docker(t, "exec", source, "/usr/local/bin/etcdctl", "get", etcdPrefix+deviceactor.ActorID(id), "--write-out=json")
	var sourceRevision struct {
		Header struct {
			Revision int64 `json:"revision"`
		} `json:"header"`
	}
	if e := json.Unmarshal([]byte(sourceState), &sourceRevision); e != nil || sourceRevision.Header.Revision <= backup.Revision {
		t.Fatal("source did not advance beyond backup")
	}

	// Alter only the appended integrity hash; restore must reject it without
	// --skip-hash-check. The failed restore has no network or persistent data dir.
	corrupt, e := os.ReadFile(filepath.Join(dir, "snapshot.db"))
	if e != nil || len(corrupt) < 32 {
		t.Fatalf("snapshot bytes: %v", e)
	}
	corrupt[len(corrupt)-1] ^= 0xff
	if e := os.WriteFile(filepath.Join(dir, "corrupt.db"), corrupt, 0600); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	output, err := exec.CommandContext(ctx, "docker", "run", "--rm", "--network", "none", "-v", dir+":/backup:ro", etcdImage, "/usr/local/bin/etcdutl", "snapshot", "restore", "/backup/corrupt.db", "--data-dir", "/tmp/rejected").CombinedOutput()
	cancel()
	if err == nil || !strings.Contains(strings.ToLower(string(output)), "expected sha256") {
		t.Fatalf("corrupt snapshot was not rejected by integrity check: %s %v", output, err)
	}

	network := fmt.Sprintf("usp-dapr-test-restore-%d", time.Now().UnixNano())
	docker(t, "network", "create", network)
	t.Cleanup(func() { docker(t, "network", "rm", network) })
	cluster := "restore0=http://restore0:2380,restore1=http://restore1:2380,restore2=http://restore2:2380"
	var members, endpoints []string
	for i := 0; i < 3; i++ {
		name := fmt.Sprintf("restore%d", i)
		volume := network + "-data-" + name
		docker(t, "volume", "create", volume)
		t.Cleanup(func() { docker(t, "volume", "rm", volume) })
		docker(t, "run", "--rm", "--network", "none", "-v", dir+":/backup:ro", "-v", volume+":/restore", etcdImage, "/usr/local/bin/etcdutl", "snapshot", "restore", "/backup/snapshot.db", "--data-dir", "/restore/data", "--name", name, "--initial-cluster", cluster, "--initial-cluster-token", network, "--initial-advertise-peer-urls", "http://"+name+":2380", "--bump-revision", strconv.FormatInt(bump, 10), "--mark-compacted")
		member := network + "-" + name
		docker(t, "run", "-d", "--name", member, "--network", network, "--network-alias", name, "-p", "127.0.0.1::2379", "-v", volume+":/restore", etcdImage, "/usr/local/bin/etcd", "--name", name, "--data-dir", "/restore/data", "--listen-client-urls", "http://0.0.0.0:2379", "--advertise-client-urls", "http://"+name+":2379", "--listen-peer-urls", "http://0.0.0.0:2380", "--initial-advertise-peer-urls", "http://"+name+":2380", "--initial-cluster", cluster, "--initial-cluster-token", network)
		t.Cleanup(func() { docker(t, "rm", "-f", member) })
		members = append(members, member)
		endpoints = append(endpoints, "http://"+docker(t, "port", member, "2379/tcp"))
	}
	fresh := func() *deviceactor.FencedDaprBackend {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		t.Cleanup(tr.CloseIdleConnections)
		b, e := deviceactor.NewEtcdSnapshotBackendEndpoints("http://127.0.0.1:1", "", endpoints, etcdPrefix, "", &http.Client{Transport: tr, Timeout: 5 * time.Second}, id, time.Second, 1<<20)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	backend := fresh()
	eventually(t, "restored three-member cluster serves snapshots", func() bool { _, e := backend.Load(context.Background()); return e == nil })
	electedLeader(t, members, []int{0, 1, 2}, 0)
	clusterID := uint64(0)
	for _, member := range members {
		status := memberStatus(t, member)
		if status.Header.ClusterID == 0 || status.Header.ClusterID == original.Header.ClusterID || status.Header.MemberID == original.Header.MemberID {
			t.Fatal("restore did not create new logical cluster identities")
		}
		if clusterID != 0 && clusterID != status.Header.ClusterID {
			t.Fatal("restored members disagree on cluster identity")
		}
		clusterID = status.Header.ClusterID
	}
	restoredRange := docker(t, "exec", members[0], "/usr/local/bin/etcdctl", "get", etcdPrefix+deviceactor.ActorID(id), "--write-out=json")
	var restoredRevision struct {
		Header struct {
			Revision int64 `json:"revision"`
		} `json:"header"`
	}
	if e := json.Unmarshal([]byte(restoredRange), &restoredRevision); e != nil || restoredRevision.Header.Revision < backup.Revision+bump || restoredRevision.Header.Revision <= sourceRevision.Header.Revision {
		t.Fatalf("restored revision was not bumped beyond observed source: %s %v", restoredRange, e)
	}
	eventually(t, "restored history is compacted", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		out, err := exec.CommandContext(ctx, "docker", "exec", members[0], "/usr/local/bin/etcdctl", "get", etcdPrefix+deviceactor.ActorID(id), "--rev", strconv.FormatInt(backup.Revision, 10), "--command-timeout=3s").CombinedOutput()
		return err != nil && strings.Contains(string(out), "required revision has been compacted")
	})
	actor, e := deviceactor.New(id, restoredSnapshotBackend{backend}, deviceactor.DefaultOptions())
	if e != nil {
		t.Fatal(e)
	}
	twin, e := actor.GetTwin(context.Background())
	if e != nil || twin.DesiredRevision != baseline.ExpectedRevision+1 || twin.Desired["Device.Test.Value"].Value != string(baseline.ID) {
		t.Fatalf("point-in-time twin not restored: %+v %v", twin, e)
	}
	if r, e := actor.SetDesired(context.Background(), baseline); e != nil || r.ID != baseline.ID || r.Revision != baseline.ExpectedRevision+1 {
		t.Fatalf("backup receipt not restored: %+v %v", r, e)
	}
	stale := fresh()
	old, e := stale.Load(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	// Same ID as the source-only write, different payload: it must be NEW here,
	// proving post-backup receipts were not copied from the still-live source.
	recovery := later
	recovery.Values = map[string]device.ParameterValue{"Device.Test.Value": {Value: "restored-only", Type: "string"}}
	for i := 0; i < 2; i++ {
		r, e := actor.SetDesired(context.Background(), recovery)
		if e != nil || r.ID != recovery.ID || r.Revision != recovery.ExpectedRevision+1 {
			t.Fatalf("fresh restored admission/receipt: %+v %v", r, e)
		}
	}
	if e := stale.Save(context.Background(), old); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("restored-cluster stale writer not fenced: %v", e)
	}
	twin, e = actor.GetTwin(context.Background())
	if e != nil || twin.DesiredRevision != recovery.ExpectedRevision+1 || twin.Desired["Device.Test.Value"].Value != "restored-only" {
		t.Fatalf("restored state changed after stale write: %+v %v", twin, e)
	}
	assertEtcdReceipt(t, hosts, id, later)
	t.Log("native snapshot integrity, isolated three-member restore, revision bump/compaction, point-in-time twin/receipt and fresh-writer fencing passed; source unchanged")
}
