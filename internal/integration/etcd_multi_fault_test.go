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

// Force the initial read through member zero, refreshing Docker's published
// ports before the scenario. No target or endpoint list changes during a fault.
func prepareMultiEndpointFault(t *testing.T, hosts []*liveHost, members []string, id device.DeviceIdentity) {
	t.Helper()
	for i, member := range members {
		target := docker(t, "port", member, "2379/tcp")
		for _, h := range hosts {
			p := h.failoverSnapshots[i]
			p.setBlocked(true)
			p.setTarget(target)
			p.setBlocked(i != 0)
		}
	}
	t.Cleanup(func() { enableMultiEndpoints(hosts) })
	eventually(t, "multi-endpoint actor selects first member", func() bool {
		_, e := hosts[0].client.Invoke(context.Background(), id, "GetTwin", nil)
		return e == nil
	})
}

func enableMultiEndpoints(hosts []*liveHost) {
	for _, h := range hosts {
		for _, p := range h.failoverSnapshots {
			p.setBlocked(false)
		}
	}
}

func testMultiEndpointPacketLoss(t *testing.T, hosts []*liveHost, members []string, id device.DeviceIdentity, mode string) {
	prepareMultiEndpointFault(t, hosts, members, id)
	r := etcdMutation(t, hosts[0], id, "multi-packets-"+mode)
	writer, e := newLiveFailoverBackend(hosts[0], id)
	if e != nil {
		t.Fatal(e)
	}
	snapshot, e := writer.Load(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	stale, e := newLiveFailoverBackend(hosts[1], id)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = stale.Load(context.Background()); e != nil {
		t.Fatal(e)
	}
	enableMultiEndpoints(hosts)

	// Drop real packets at the preferred member's client port only. Peer traffic
	// and alternate gateways remain healthy throughout the fault.
	f := newPacketFault(t, &liveHost{name: members[0]}, hosts[0].failoverSnapshots[0])
	f.server, f.port = true, "2379"
	before := f.established(t)
	if before == "" {
		t.Fatal("no established preferred-endpoint connection")
	}
	if mode != "control" {
		f.install(t, mode)
	}
	defer f.heal(t)
	time.Sleep(2 * time.Second)
	if mode == "half-open" && !sameConnection(before, f.established(t)) {
		t.Fatal("original connection did not survive black hole")
	}
	// A preloaded transaction must not be replayed on another endpoint when its
	// result is ambiguous. Its next read can rotate without reusing the old fence.
	e = writer.Save(context.Background(), snapshot)
	if mode == "control" && e != nil {
		t.Fatal(e)
	}
	if mode != "control" && e == nil {
		t.Fatal("black-holed transaction was acknowledged")
	}
	if mode != "control" && writer.Save(context.Background(), snapshot) == nil {
		t.Fatal("ambiguous write retained a reusable version")
	}
	started := time.Now()
	if _, e = writer.Load(context.Background()); e != nil {
		t.Fatalf("alternate read: %v", e)
	}
	if time.Since(started) > 6*time.Second {
		t.Fatal("read exceeded five-second budget plus scheduling tolerance")
	}
	// Prove actor admission and stable retries on both sidecars BEFORE healing.
	assertEtcdReceipt(t, hosts, id, r)
	if mode != "control" {
		f.assertDrops(t)
	}
	f.heal(t)
	assertEtcdReceipt(t, hosts, id, r)
	if e := stale.Save(context.Background(), snapshot); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("stale writer survived multi-endpoint recovery: %v", e)
	}
	t.Log("fixed endpoints admitted one receipt during packet loss; ambiguous write and stale fence rejected")
}

func testMultiEndpointQuorumLoss(t *testing.T, hosts []*liveHost, members []string, id device.DeviceIdentity) {
	prepareMultiEndpointFault(t, hosts, members, id)
	enableMultiEndpoints(hosts)
	// Isolate peer links instead of stopping containers: all three configured
	// client listeners remain reachable, without quorum or proxy retargeting.
	first := isolateEtcdPeers(t, hosts, members[2])
	defer first.heal(t)
	electedLeader(t, members, []int{0, 1}, 0)
	majority := etcdMutation(t, hosts[0], id, "multi-majority")
	assertEtcdReceipt(t, hosts, id, majority)
	pending := etcdMutation(t, hosts[0], id, "multi-quorum-recovery")
	writer, e := newLiveFailoverBackend(hosts[0], id)
	if e != nil {
		t.Fatal(e)
	}
	snapshot, e := writer.Load(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	stale, e := newLiveFailoverBackend(hosts[1], id)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = stale.Load(context.Background()); e != nil {
		t.Fatal(e)
	}
	second := isolateEtcdPeers(t, hosts, members[1])
	defer second.heal(t)
	eventually(t, "all isolated members lose leadership", func() bool {
		for _, member := range members {
			if memberStatus(t, member).Leader != 0 {
				return false
			}
		}
		return true
	})
	for _, member := range members {
		raw := docker(t, "exec", member, "/usr/local/bin/etcdctl", "get", etcdPrefix+deviceactor.ActorID(id), "--consistency=s", "--write-out=json")
		var local struct {
			Count int64 `json:"count"`
		}
		if json.Unmarshal([]byte(raw), &local) != nil || local.Count != 1 {
			t.Fatalf("isolated client listener did not serve local snapshot: %s", raw)
		}
	}
	if e := writer.Save(context.Background(), snapshot); e == nil {
		t.Fatal("CAS acknowledged without quorum")
	}
	started := time.Now()
	if _, e := writer.Load(context.Background()); e == nil {
		t.Fatal("multi-endpoint linearizable read succeeded without quorum")
	}
	if time.Since(started) > 6*time.Second {
		t.Fatal("unavailable read exceeded bounded failover budget")
	}
	for _, h := range hosts {
		for _, method := range []string{"GetTwin", "SetDesired"} {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			var input any
			if method == "SetDesired" {
				input = pending
			}
			_, e := h.client.Invoke(ctx, id, method, input)
			cancel()
			if e == nil {
				t.Fatalf("%s succeeded without quorum", method)
			}
		}
	}
	first.assertDrops(t)
	second.assertDrops(t)
	// Restore just a majority first; the endpoint list remains unchanged.
	second.heal(t)
	electedLeader(t, members, []int{0, 1}, 0)
	assertEtcdReceipt(t, hosts, id, pending)
	if e := stale.Save(context.Background(), snapshot); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("stale revision accepted after quorum recovery: %v", e)
	}
	if e := writer.Save(context.Background(), snapshot); e == nil {
		t.Fatal("failed version reused after recovery")
	}
	first.heal(t)
	electedLeader(t, members, []int{0, 1, 2}, 0)
	assertEtcdReceipt(t, hosts, id, pending)
	t.Log("fixed endpoints: majority admitted; three reachable minority members rejected reads/writes; recovery retained one receipt")
}
