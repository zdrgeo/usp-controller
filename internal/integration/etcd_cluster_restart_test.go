package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

func testEtcdClusterRestart(t *testing.T, hosts []*liveHost, members []string, id device.DeviceIdentity, mode string) {
	prepareMultiEndpointFault(t, hosts, members, id)
	enableMultiEndpoints(hosts)
	defer routeSnapshots(t, hosts, members[0])
	all := []int{0, 1, 2}
	electedLeader(t, members, all, 0)
	identities := make([]etcdStatus, len(members))
	started := make([]string, len(members))
	for i, member := range members {
		identities[i] = memberStatus(t, member)
		if identities[i].Header.ClusterID == 0 {
			t.Fatal("missing cluster identity")
		}
		started[i] = docker(t, "inspect", "--format", "{{.State.StartedAt}}", member)
	}
	fresh := func() (*deviceactor.FencedDaprBackend, *http.Transport) {
		tr := http.DefaultTransport.(*http.Transport).Clone()
		t.Cleanup(tr.CloseIdleConnections)
		b, err := newLiveFailoverBackendWithClient(hosts[0], id, &http.Client{Transport: tr, Timeout: 5 * time.Second})
		if err != nil {
			t.Fatal(err)
		}
		return b, tr
	}
	baseline := etcdMutation(t, hosts[0], id, "cluster-"+mode+"-baseline")
	assertEtcdReceipt(t, hosts, id, baseline)
	stale, staleTransport := fresh()
	obsolete, e := stale.Load(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	// Advance the fence BEFORE the outage. Rejecting the stale version after
	// restart must not depend on a newly committed recovery mutation.
	durable := etcdMutation(t, hosts[0], id, "cluster-"+mode+"-durable")
	assertEtcdReceipt(t, hosts, id, durable)
	pending := etcdMutation(t, hosts[0], id, "cluster-"+mode+"-recovery")
	// An outage probe must remain invalid after recovery: a delayed actor turn
	// must not advance the fence before we verify the pre-outage version.
	outage := device.SetDesiredRequest{ID: device.RequestID("cluster-" + mode + "-outage"), ExpectedRevision: durable.ExpectedRevision, Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "outage", Type: "string"}}}
	failed, _ := fresh()
	latest, e := failed.Load(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	reader, _ := fresh()
	if _, e := reader.Load(context.Background()); e != nil {
		t.Fatal(e)
	}
	defer docker(t, append([]string{"start"}, members...)...)
	args := []string{"stop", "-t", "10"}
	if mode == "abrupt" {
		args = []string{"kill", "--signal", "KILL"}
	}
	docker(t, append(args, members...)...)
	for _, member := range members {
		if state := docker(t, "inspect", "--format", "{{.State.Running}}", member); state != "false" {
			t.Fatal("cluster was not fully stopped")
		}
	}
	assertUnavailable := func(stage string) {
		t.Helper()
		start := time.Now()
		if _, e := reader.Load(context.Background()); e == nil {
			t.Fatalf("snapshot read succeeded %s", stage)
		}
		if elapsed := time.Since(start); elapsed > 6*time.Second {
			t.Fatalf("unavailable read exceeded budget %s: %s", stage, elapsed)
		}
		for _, h := range hosts {
			for _, method := range []string{"GetTwin", "SetDesired"} {
				ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
				var input any
				if method == "SetDesired" {
					input = outage
				}
				_, e := h.client.Invoke(ctx, id, method, input)
				cancel()
				if e == nil || errors.Is(e, deviceactor.ErrConflict) {
					t.Fatalf("actor %s did not reject unavailable storage %s: %v", method, stage, e)
				}
			}
		}
	}
	if e := failed.Save(context.Background(), latest); e == nil {
		t.Fatal("CAS acknowledged with entire cluster stopped")
	}
	assertUnavailable("with all members stopped")
	restart := func(index int) {
		t.Helper()
		docker(t, "start", members[index])
		if after := docker(t, "inspect", "--format", "{{.State.StartedAt}}", members[index]); after == started[index] {
			t.Fatal("member process did not restart")
		}
		// Restore Docker's mapping to the same restarted member. Fixed actor URLs,
		// backend instances, membership and data directories are not replaced.
		target := docker(t, "port", members[index], "2379/tcp")
		for _, h := range hosts {
			p := h.failoverSnapshots[index]
			p.setBlocked(true)
			p.setTarget(target)
			p.setBlocked(false)
		}
	}
	restart(0)
	assertUnavailable("with only one member restarted")
	restart(1)
	electedLeader(t, members, []int{0, 1}, 0)
	waitLiveEtcdEndpoint(t, hosts[0], id, 0)
	waitLiveEtcdEndpoint(t, hosts[0], id, 1)
	eventually(t, "original reader recovers with restarted majority", func() bool {
		_, e := reader.Load(context.Background())
		return e == nil
	})
	// Inspect recovered state before retrying any admitted request, so replay
	// cannot hide a rollback by reconstructing the lost desired-state mutation.
	for _, h := range hosts {
		data, e := h.client.Invoke(context.Background(), id, "GetTwin", nil)
		var twin device.DeviceTwin
		if e != nil || json.Unmarshal(data, &twin) != nil || twin.DesiredRevision != durable.ExpectedRevision+1 || twin.Desired["Device.Test.Value"].Value != string(durable.ID) {
			t.Fatalf("committed twin lost across restart: %s %v", data, e)
		}
	}
	staleTransport.CloseIdleConnections()
	if e := stale.Save(context.Background(), obsolete); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("pre-outage obsolete version revived: %v", e)
	}
	if e := failed.Save(context.Background(), latest); e == nil {
		t.Fatal("failed writer reused invalidated version after restart")
	}
	assertEtcdReceipt(t, hosts, id, durable)
	assertEtcdReceipt(t, hosts, id, pending)
	restart(2)
	electedLeader(t, members, all, 0)
	for i, member := range members {
		waitLiveEtcdEndpoint(t, hosts[0], id, i)
		actual := memberStatus(t, member)
		if actual.Header != identities[i].Header {
			t.Fatalf("member or cluster identity changed for %d", i)
		}
	}
	for _, request := range []device.SetDesiredRequest{baseline, durable, pending} {
		for _, h := range hosts {
			data, e := h.client.Invoke(context.Background(), id, "SetDesired", request)
			var receipt device.DesiredReceipt
			if e != nil || json.Unmarshal(data, &receipt) != nil || receipt.ID != request.ID || receipt.Revision != request.ExpectedRevision+1 {
				t.Fatalf("receipt lost across cluster restart: %s %v", data, e)
			}
		}
	}
	assertEtcdReceipt(t, hosts, id, pending)
	t.Logf("%s: full outage and one-member recovery rejected; original majority restored twin, receipts and pre-outage fence", mode)
}
