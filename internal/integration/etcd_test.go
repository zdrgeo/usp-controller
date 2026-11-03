package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

const etcdPrefix = "/usp-live/snapshots/"

func newLiveEtcdBackend(h *liveHost, id device.DeviceIdentity) (*deviceactor.FencedDaprBackend, error) {
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", h.snapshots.listener.Addr().(*net.TCPAddr).Port)
	return deviceactor.NewEtcdSnapshotBackend(h.client.Endpoint, token, endpoint, etcdPrefix, "", nil, id, time.Second, 1<<20)
}
func testEtcdIsolation(t *testing.T, hosts []*liveHost, redis, etcd string, id device.DeviceIdentity) {
	ctx := context.Background()
	fresh := func(index int, identity device.DeviceIdentity) *deviceactor.FencedDaprBackend {
		b, e := newLiveEtcdBackend(hosts[index], identity)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	stale := fresh(0, id)
	backup, _, e := stale.ExportSnapshot(ctx)
	if e != nil {
		t.Fatal(e)
	}
	// Keep both obsolete Redis namespaces present, then roll them back while the
	// actual Dapr-routed actor has already advanced its independent etcd snapshot.
	keys := []string{"usp-live||device-snapshot-cas-v1-" + deviceactor.ActorID(id), "usp-live||DeviceActor||" + deviceactor.ActorID(id) + "||snapshot-v1"}
	for _, key := range keys {
		docker(t, "exec", redis, "redis-cli", "HSET", key, "data", string(backup.Snapshot), "version", "1")
		docker(t, "exec", redis, "redis-cli", "COPY", key, key+"-backup")
	}
	request := device.SetDesiredRequest{ID: "etcd-desired", Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "authoritative", Type: "string"}}}
	eventually(t, "etcd actor placement", func() bool { _, e := hosts[0].client.Invoke(ctx, id, "SetDesired", request); return e == nil })
	for _, key := range keys {
		docker(t, "exec", redis, "redis-cli", "HSET", key, "data", `{"corrupt":true}`, "version", "77")
		if result := docker(t, "exec", redis, "redis-cli", "EVAL", `return redis.call('RESTORE',KEYS[1],0,redis.call('DUMP',KEYS[2]),'REPLACE')`, "2", key, key+"-backup"); result != "OK" {
			t.Fatal(result)
		}
	}
	if e := stale.Save(ctx, backup.Snapshot); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("Redis rollback revived stale writer: %v", e)
	}
	for _, key := range keys {
		docker(t, "exec", redis, "redis-cli", "DEL", key)
	}
	for _, h := range hosts {
		data, e := h.client.Invoke(ctx, id, "SetDesired", request)
		var receipt device.DesiredReceipt
		if e != nil || json.Unmarshal(data, &receipt) != nil || receipt.Revision != 1 {
			t.Fatalf("Redis deletion lost receipt: %s %v", data, e)
		}
	}
	for _, h := range hosts {
		h.snapshots.setBlocked(true)
	}
	next := request
	next.ID = "after-etcd-outage"
	next.ExpectedRevision = 1
	if _, e := hosts[0].client.Invoke(ctx, id, "SetDesired", next); e == nil {
		t.Fatal("admitted during etcd outage")
	}
	for _, h := range hosts {
		h.snapshots.setBlocked(false)
	}
	eventually(t, "etcd connectivity recovery", func() bool { _, e := hosts[1].client.Invoke(ctx, id, "SetDesired", next); return e == nil })

	probe := id
	probe.AgentID = "etcd-cas-probe"
	a, b := fresh(0, probe), fresh(1, probe)
	results := make(chan error, 2)
	start := make(chan struct{})
	for _, candidate := range []*deviceactor.FencedDaprBackend{a, b} {
		go func(c *deviceactor.FencedDaprBackend) { <-start; results <- c.InitializeSnapshot(ctx, false) }(candidate)
	}
	close(start)
	wins := 0
	for range 2 {
		if e := <-results; e == nil {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("atomic initializers: %d winners", wins)
	}
	original, version, e := a.ExportSnapshot(ctx)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = b.Load(ctx); e != nil {
		t.Fatal(e)
	}
	if e = a.RestoreSnapshot(ctx, original, version); e != nil {
		t.Fatal(e)
	}
	if e = b.Save(ctx, original.Snapshot); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("restore revived writer: %v", e)
	}
	if _, e = b.Load(ctx); e != nil {
		t.Fatal(e)
	}
	survivor := fresh(1, probe)
	if _, e = survivor.Load(ctx); e != nil {
		t.Fatal(e)
	}
	// Delete AFTER Load. Save has no preflight: atomic MOD comparison must reject
	// missing keys, unlike the Redis component's upsert script.
	docker(t, "exec", etcd, "/usr/local/bin/etcdctl", "del", etcdPrefix+deviceactor.ActorID(probe))
	if e = b.Save(ctx, original.Snapshot); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("recreated deleted etcd key: %v", e)
	}
	if _, e = a.Load(ctx); !errors.Is(e, deviceactor.ErrNotFound) {
		t.Fatalf("missing etcd key did not fail closed: %v", e)
	}
	// Explicit recreation uses a new global modification revision, not version 1.
	if e = a.ImportSnapshot(ctx, original); e != nil {
		t.Fatal(e)
	}
	if e = b.Save(ctx, original.Snapshot); e == nil {
		t.Fatal("reused invalidated writer")
	}
	if e = survivor.Save(ctx, original.Snapshot); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("deletion/recreation revived old revision: %v", e)
	}
	_, recreated, e := a.ExportSnapshot(ctx)
	if e != nil || recreated == version {
		t.Fatalf("reused deleted revision: %s %v", recreated, e)
	}
	// Redis-to-etcd import retains data and rejects replacement of an existing key.
	migrate := id
	migrate.AgentID = "etcd-import-probe"
	source, e := deviceactor.NewFencedDaprBackend(hosts[0].client.Endpoint, nil, token, migrate, time.Second, 1<<20, "state")
	if e != nil {
		t.Fatal(e)
	}
	if e = source.InitializeSnapshot(ctx, false); e != nil {
		t.Fatal(e)
	}
	actor, e := deviceactor.New(migrate, snapshotNoReminder{source}, deviceactor.DefaultOptions())
	if e != nil {
		t.Fatal(e)
	}
	if _, e = actor.SetDesired(ctx, request); e != nil {
		t.Fatal(e)
	}
	exported, _, e := source.ExportSnapshot(ctx)
	if e != nil {
		t.Fatal(e)
	}
	target := fresh(0, migrate)
	if e = target.ImportSnapshot(ctx, exported); e != nil {
		t.Fatal(e)
	}
	if e = target.ImportSnapshot(ctx, exported); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("import overwrote authoritative state: %v", e)
	}
	data, e := fresh(1, migrate).Load(ctx)
	if e != nil {
		t.Fatal(e)
	}
	assertSnapshot(t, data, exported.Snapshot)
	t.Log("Dapr actor receipts survived raw Redis rollback/deletion; independent etcd CAS, outage, deletion and import checks passed")
}
