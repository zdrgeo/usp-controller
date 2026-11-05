package integration

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

func etcdMutation(t *testing.T, h *liveHost, id device.DeviceIdentity, name string) device.SetDesiredRequest {
	t.Helper()
	data, err := h.client.Invoke(context.Background(), id, "GetTwin", nil)
	var twin device.DeviceTwin
	if err != nil || json.Unmarshal(data, &twin) != nil {
		t.Fatalf("baseline twin: %s %v", data, err)
	}
	return device.SetDesiredRequest{ID: device.RequestID(name), ExpectedRevision: twin.DesiredRevision, Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: name, Type: "string"}}}
}
func assertEtcdReceipt(t *testing.T, hosts []*liveHost, id device.DeviceIdentity, r device.SetDesiredRequest) {
	t.Helper()
	eventually(t, "stable etcd receipt after recovery", func() bool {
		for _, h := range hosts {
			data, e := h.client.Invoke(context.Background(), id, "SetDesired", r)
			var receipt device.DesiredReceipt
			if e != nil || json.Unmarshal(data, &receipt) != nil || receipt.ID != r.ID || receipt.Revision != r.ExpectedRevision+1 {
				return false
			}
		}
		return true
	})
	competitor := r
	competitor.ID += "-conflict"
	if _, e := hosts[1].client.Invoke(context.Background(), id, "SetDesired", competitor); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("conflicting receipt after recovery: %v", e)
	}
	for _, h := range hosts {
		data, e := h.client.Invoke(context.Background(), id, "GetTwin", nil)
		var twin device.DeviceTwin
		if e != nil || json.Unmarshal(data, &twin) != nil || twin.DesiredRevision != r.ExpectedRevision+1 || twin.Desired["Device.Test.Value"].Value != string(r.ID) {
			t.Fatalf("inconsistent recovered twin: %s %v", data, e)
		}
	}
}
func testEtcdPacketLoss(t *testing.T, hosts []*liveHost, member string, id device.DeviceIdentity, mode string) {
	r := etcdMutation(t, hosts[0], id, "etcd-packets-"+mode)
	writer, e := newLiveEtcdBackend(hosts[0], id)
	if e != nil {
		t.Fatal(e)
	}
	snapshot, e := writer.Load(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	stale, e := newLiveEtcdBackend(hosts[1], id)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = stale.Load(context.Background()); e != nil {
		t.Fatal(e)
	}
	// Rules live only in the disposable etcd member's network namespace. Filter
	// client port 2379; peer traffic (2380) remains healthy. Directions are relative
	// to the etcd server, unlike the existing sidecar packet-loss tests.
	f := newPacketFault(t, &liveHost{name: member}, hosts[0].snapshots)
	f.server = true
	f.port = "2379"
	before := f.established(t)
	if before == "" {
		t.Fatal("no established etcd client connection")
	}
	if mode != "control" {
		f.install(t, mode)
	}
	defer f.heal(t)
	time.Sleep(2 * time.Second)
	if mode == "half-open" && !sameConnection(before, f.established(t)) {
		t.Fatal("no original etcd connection survived black hole")
	}
	// Exercise a transaction whose read happened before the fault. Lost response
	// packets can hide a committed write; recovery must not assume it failed.
	saveErr := writer.Save(context.Background(), snapshot)
	if mode == "control" && saveErr != nil {
		t.Fatal(saveErr)
	}
	if mode != "control" && saveErr == nil {
		t.Fatal("acknowledged CAS through black-holed etcd endpoint")
	}
	t.Logf("preloaded snapshot transaction result: %v", saveErr)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	_, err := hosts[0].client.Invoke(ctx, id, "SetDesired", r)
	cancel()
	if mode == "control" && err != nil {
		t.Fatal(err)
	}
	if mode != "control" {
		if err == nil {
			t.Fatal("acknowledged actor mutation through black-holed etcd endpoint")
		}
		f.assertDrops(t)
	}
	f.heal(t)
	assertEtcdReceipt(t, hosts, id, r)
	if e := stale.Save(context.Background(), snapshot); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("stale write survived packet-loss recovery: %v", e)
	}
	t.Logf("%s: one durable receipt after client-path recovery", mode)
}
func testEtcdQuorumLoss(t *testing.T, hosts []*liveHost, members []string, id device.DeviceIdentity) {
	// Stop members while leaving the contacted endpoint alive; a healthy
	// TCP listener alone must never permit a linearizable read or transaction.
	docker(t, "stop", "-t", "1", members[2])
	defer docker(t, "start", members[2])
	majority := etcdMutation(t, hosts[0], id, "etcd-one-member-lost")
	assertEtcdReceipt(t, hosts, id, majority)
	pending := etcdMutation(t, hosts[0], id, "etcd-quorum-recovery")
	stale, e := newLiveEtcdBackend(hosts[0], id)
	if e != nil {
		t.Fatal(e)
	}
	data, e := stale.Load(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	survivor, e := newLiveEtcdBackend(hosts[1], id)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = survivor.Load(context.Background()); e != nil {
		t.Fatal(e)
	}
	docker(t, "stop", "-t", "1", members[1])
	defer docker(t, "start", members[1])
	// Prove the surviving endpoint still accepts local (serializable) reads.
	local := docker(t, "exec", members[0], "/usr/local/bin/etcdctl", "get", etcdPrefix+deviceactor.ActorID(id), "--consistency=s", "--write-out=json")
	var localResult struct {
		Count int64 `json:"count"`
	}
	if e := json.Unmarshal([]byte(local), &localResult); e != nil || localResult.Count != 1 {
		t.Fatalf("surviving endpoint did not serve baseline: count=%d error=%v", localResult.Count, e)
	}
	for _, h := range hosts {
		ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		_, err := h.client.Invoke(ctx, id, "SetDesired", pending)
		cancel()
		if err == nil {
			t.Fatal("admitted mutation without majority")
		}
	}
	if e := stale.Save(context.Background(), data); e == nil {
		t.Fatal("snapshot CAS acknowledged without majority")
	}
	// Restart a peer with its original data directory; do not rebuild membership.
	docker(t, "start", members[1])
	assertEtcdReceipt(t, hosts, id, pending)
	if e := survivor.Save(context.Background(), data); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("old revision accepted after quorum recovery: %v", e)
	}
	// A separate writer retaining the pre-outage revision must also lose after
	// recovery. The earlier failed writer already invalidated its local version.
	if e := stale.Save(context.Background(), data); e == nil {
		t.Fatal("failed version reused after quorum recovery")
	}
	docker(t, "start", members[2])
	assertEtcdReceipt(t, hosts, id, pending)
	t.Log("2/3 members admitted; 1/3 rejected actor writes and snapshot CAS; original quorum recovered stable receipt")
}
