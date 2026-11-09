package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

func testEtcdRollingRestart(t *testing.T, hosts []*liveHost, members []string, id device.DeviceIdentity) {
	prepareMultiEndpointFault(t, hosts, members, id)
	enableMultiEndpoints(hosts)
	// Older single-endpoint fixtures also need member zero's new Docker mapping
	// after this drill. This does not route the multi-endpoint availability check.
	defer routeSnapshots(t, hosts, members[0])
	all := []int{0, 1, 2}
	leader, _ := electedLeader(t, members, all, 0)
	memberIDs := make([]uint64, len(members))
	for i, member := range members {
		memberIDs[i] = memberStatus(t, member).Header.MemberID
	}
	// Start with a follower, then the original leader, then the remaining member.
	// Wait for full rejoin before stopping the next member.
	order := []int{(leader + 1) % 3, leader, (leader + 2) % 3}
	baseline := etcdMutation(t, hosts[0], id, "rolling-baseline")
	assertEtcdReceipt(t, hosts, id, baseline)
	receipts := []device.SetDesiredRequest{baseline}
	readerTransport := http.DefaultTransport.(*http.Transport).Clone()
	defer readerTransport.CloseIdleConnections()
	reader, e := newLiveFailoverBackendWithClient(hosts[0], id, &http.Client{Transport: readerTransport, Timeout: 5 * time.Second})
	if e != nil {
		t.Fatal(e)
	}
	if _, e := reader.Load(context.Background()); e != nil {
		t.Fatal(e)
	}
	for round, index := range order {
		if !t.Run(fmt.Sprintf("member-%d", index), func(t *testing.T) {
			currentLeader, before := electedLeader(t, members, all, 0)
			request := etcdMutation(t, hosts[0], id, fmt.Sprintf("rolling-%d", round))
			staleTransport := http.DefaultTransport.(*http.Transport).Clone()
			defer staleTransport.CloseIdleConnections()
			stale, e := newLiveFailoverBackendWithClient(hosts[1], id, &http.Client{Transport: staleTransport, Timeout: 5 * time.Second})
			if e != nil {
				t.Fatal(e)
			}
			snapshot, e := stale.Load(context.Background())
			if e != nil {
				t.Fatal(e)
			}
			started := docker(t, "inspect", "--format", "{{.State.StartedAt}}", members[index])
			docker(t, "stop", "-t", "10", members[index])
			defer docker(t, "start", members[index])
			if state := docker(t, "inspect", "--format", "{{.State.Running}}", members[index]); state != "false" {
				t.Fatal("member did not stop")
			}
			majority := []int{}
			for _, i := range all {
				if i != index {
					majority = append(majority, i)
				}
			}
			previous := uint64(0)
			if index == currentLeader {
				previous = before.Leader
			}
			electedLeader(t, members, majority, previous)
			if index == currentLeader {
				eventually(t, "rolling restart advances leader term", func() bool {
					_, after := electedLeader(t, members, majority, previous)
					return after.Term > before.Term
				})
			}
			// Prove availability BEFORE start or any Docker port-map refresh. The same
			// reader, actor backends and endpoint lists must use the surviving majority.
			eventually(t, "persistent reader survives member stop", func() bool {
				start := time.Now()
				_, err := reader.Load(context.Background())
				if elapsed := time.Since(start); elapsed > 6*time.Second {
					t.Fatalf("rolling read exceeded request budget: %s", elapsed)
				}
				return err == nil
			})
			assertEtcdReceipt(t, hosts, id, request)
			docker(t, "start", members[index])
			if after := docker(t, "inspect", "--format", "{{.State.StartedAt}}", members[index]); after == started {
				t.Fatal("member process did not restart")
			}
			// Docker reallocates ephemeral published ports on restart. Rebind only the
			// restarted member's proxy to that SAME member, after availability is proven.
			// Actor endpoint URLs and backends remain unchanged throughout the roll.
			target := docker(t, "port", members[index], "2379/tcp")
			for _, h := range hosts {
				p := h.failoverSnapshots[index]
				p.setBlocked(true)
				p.setTarget(target)
				p.setBlocked(false)
			}
			waitLiveEtcdEndpoint(t, hosts[0], id, index)
			electedLeader(t, members, all, 0)
			for i, member := range members {
				if actual := memberStatus(t, member).Header.MemberID; actual != memberIDs[i] {
					t.Fatalf("member %d identity changed across restart", i)
				}
			}
			assertEtcdReceipt(t, hosts, id, request)
			staleTransport.CloseIdleConnections()
			if e := stale.Save(context.Background(), snapshot); !errors.Is(e, deviceactor.ErrConflict) {
				t.Fatalf("pre-restart writer not fenced after rejoin: %v", e)
			}
			assertEtcdReceipt(t, hosts, id, request)
			receipts = append(receipts, request)
			t.Logf("member %d (leader=%t): majority admitted while stopped; same member rejoined with receipt and fence intact", index, index == currentLeader)
		}) {
			return
		}
	}
	// Replay every earlier ID after the entire roll, not only the latest mutation.
	for _, request := range receipts {
		for _, h := range hosts {
			data, e := h.client.Invoke(context.Background(), id, "SetDesired", request)
			var receipt device.DesiredReceipt
			if e != nil || json.Unmarshal(data, &receipt) != nil || receipt.ID != request.ID || receipt.Revision != request.ExpectedRevision+1 {
				t.Fatalf("receipt lost across full roll: %s %v", data, e)
			}
		}
	}
	assertEtcdReceipt(t, hosts, id, receipts[len(receipts)-1])
	for i := range members {
		waitLiveEtcdEndpoint(t, hosts[0], id, i)
	}
}
