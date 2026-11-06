package integration

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

func newLiveFailoverBackend(h *liveHost, id device.DeviceIdentity) (*deviceactor.FencedDaprBackend, error) {
	endpoints := make([]string, len(h.failoverSnapshots))
	for i, p := range h.failoverSnapshots {
		endpoints[i] = fmt.Sprintf("http://127.0.0.1:%d", p.listener.Addr().(*net.TCPAddr).Port)
	}
	return deviceactor.NewEtcdSnapshotBackendEndpoints(h.client.Endpoint, token, endpoints, etcdPrefix, "", nil, id, time.Second, 1<<20)
}
func testEtcdAutomaticFailover(t *testing.T, hosts []*liveHost, members []string, id device.DeviceIdentity) {
	for _, mode := range []string{"connection loss", "peer partition", "leader kill"} {
		if !t.Run(mode, func(t *testing.T) {
			leader, before := electedLeader(t, members, []int{0, 1, 2}, 0)
			// Refresh Docker's ephemeral host port mappings BEFORE the fault. Client
			// endpoint URLs remain fixed; there is no retargeting during failover.
			for i, member := range members {
				target := docker(t, "port", member, "2379/tcp")
				for _, h := range hosts {
					h.failoverSnapshots[i].setBlocked(true)
					h.failoverSnapshots[i].setTarget(target)
					h.failoverSnapshots[i].setBlocked(i != leader)
				}
			}
			defer func() {
				for _, h := range hosts {
					for _, p := range h.failoverSnapshots {
						p.setBlocked(false)
					}
				}
			}()
			eventually(t, "automatic client selects current leader", func() bool { _, e := hosts[0].client.Invoke(context.Background(), id, "GetTwin", nil); return e == nil })
			baseline := etcdMutation(t, hosts[0], id, "auto-"+mode+"-baseline")
			assertEtcdReceipt(t, hosts, id, baseline)
			request := etcdMutation(t, hosts[0], id, "auto-"+mode+"-recovery")
			writer, e := newLiveFailoverBackend(hosts[0], id)
			if e != nil {
				t.Fatal(e)
			}
			snapshot, e := writer.Load(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			for _, h := range hosts {
				for _, p := range h.failoverSnapshots {
					p.setBlocked(false)
				}
			}
			var fault *packetFault
			switch mode {
			case "connection loss":
				for _, h := range hosts {
					h.failoverSnapshots[leader].setBlocked(true)
				}
			case "peer partition":
				fault = isolateEtcdPeers(t, hosts, members[leader])
				defer fault.heal(t)
			case "leader kill":
				docker(t, "kill", members[leader])
				defer docker(t, "start", members[leader])
			}
			if mode != "connection loss" {
				majority := []int{}
				for i := range members {
					if i != leader {
						majority = append(majority, i)
					}
				}
				_, after := electedLeader(t, members, majority, before.Leader)
				if after.Term <= before.Term {
					t.Fatal("no replacement election")
				}
				if mode == "peer partition" {
					eventually(t, "isolated leader steps down", func() bool { return memberStatus(t, members[leader]).Leader == 0 })
				}
			}
			// Failed CAS is never silently replayed. Its next read must automatically
			// use another endpoint, and the actor must admit without proxy retargeting.
			if e := writer.Save(context.Background(), snapshot); e == nil {
				t.Fatal("write through failed preferred endpoint was acknowledged")
			}
			if _, e := writer.Load(context.Background()); e != nil {
				t.Fatalf("read did not fail over: %v", e)
			}
			assertEtcdReceipt(t, hosts, id, request)
			if fault != nil {
				fault.assertDrops(t)
				fault.heal(t)
			}
			if mode == "leader kill" {
				docker(t, "start", members[leader])
			}
			for _, h := range hosts {
				h.failoverSnapshots[leader].setBlocked(false)
			}
			assertEtcdReceipt(t, hosts, id, request)
			t.Log("automatic read failover and stable receipt recovery succeeded without retargeting configured endpoints")
		}) {
			return
		}
	}
}
