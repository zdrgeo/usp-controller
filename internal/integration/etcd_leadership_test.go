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

type etcdStatus struct {
	Header struct {
		MemberID uint64 `json:"member_id"`
	} `json:"header"`
	Leader uint64 `json:"leader"`
	Term   uint64 `json:"raftTerm"`
}

func memberStatus(t *testing.T, member string) etcdStatus {
	t.Helper()
	raw := docker(t, "exec", member, "/usr/local/bin/etcdctl", "endpoint", "status", "--write-out=json", "--command-timeout=3s")
	var result []struct {
		Status etcdStatus `json:"Status"`
	}
	if err := json.Unmarshal([]byte(raw), &result); err != nil || len(result) != 1 || result[0].Status.Header.MemberID == 0 {
		t.Fatalf("invalid etcd status: %s %v", raw, err)
	}
	return result[0].Status
}
func electedLeader(t *testing.T, members []string, indices []int, previous uint64) (int, etcdStatus) {
	t.Helper()
	winner := -1
	var elected etcdStatus
	eventually(t, "members agree on elected leader", func() bool {
		statuses := make(map[int]etcdStatus, len(indices))
		var leader uint64
		for _, i := range indices {
			status := memberStatus(t, members[i])
			statuses[i] = status
			if status.Leader == 0 || status.Leader == previous {
				return false
			}
			if leader != 0 && leader != status.Leader {
				return false
			}
			leader = status.Leader
		}
		for i, status := range statuses {
			if status.Header.MemberID == leader {
				winner = i
				elected = status
				return true
			}
		}
		return false
	})
	return winner, elected
}
func routeSnapshots(t *testing.T, hosts []*liveHost, member string) {
	t.Helper()
	target := docker(t, "port", member, "2379/tcp")
	for _, h := range hosts {
		h.snapshots.setBlocked(true)
		h.snapshots.setTarget(target)
		h.snapshots.setBlocked(false)
	}
}
func isolateEtcdPeers(t *testing.T, hosts []*liveHost, member string) *packetFault {
	t.Helper()
	f := newPacketFault(t, &liveHost{name: member}, hosts[0].snapshots)
	// Match both ends of every peer connection, including established streams
	// initiated by this member. Client port 2379 remains untouched.
	for _, direction := range []string{"INPUT", "OUTPUT"} {
		chain := "USP_LOSS_" + direction
		docker(t, "exec", f.helper, "iptables", "-w", "-N", chain)
		f.chains = append(f.chains, direction)
		t.Cleanup(func() {
			docker(t, "exec", f.helper, "iptables", "-w", "-D", direction, "-p", "tcp", "-m", "multiport", "--ports", "2380", "-j", chain)
			docker(t, "exec", f.helper, "iptables", "-w", "-F", chain)
			docker(t, "exec", f.helper, "iptables", "-w", "-X", chain)
		})
		docker(t, "exec", f.helper, "iptables", "-w", "-I", direction, "1", "-p", "tcp", "-m", "multiport", "--ports", "2380", "-j", chain)
		docker(t, "exec", f.helper, "iptables", "-w", "-A", chain, "-j", "DROP")
	}
	return f
}
func testEtcdLeadership(t *testing.T, hosts []*liveHost, members []string, id device.DeviceIdentity, partition bool) {
	all := []int{0, 1, 2}
	leader, before := electedLeader(t, members, all, 0)
	routeSnapshots(t, hosts, members[leader])
	defer routeSnapshots(t, hosts, members[0])
	name := "leader-kill"
	if partition {
		name = "peer-partition"
	}
	baseline := etcdMutation(t, hosts[0], id, "etcd-"+name+"-baseline")
	assertEtcdReceipt(t, hosts, id, baseline)
	request := etcdMutation(t, hosts[0], id, "etcd-"+name+"-recovery")
	old, e := newLiveEtcdBackend(hosts[0], id)
	if e != nil {
		t.Fatal(e)
	}
	data, e := old.Load(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	pending, e := newLiveEtcdBackend(hosts[1], id)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pending.Load(context.Background()); e != nil {
		t.Fatal(e)
	}
	rejoinStale, e := newLiveEtcdBackend(hosts[0], id)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = rejoinStale.Load(context.Background()); e != nil {
		t.Fatal(e)
	}
	var fault *packetFault
	if partition {
		fault = isolateEtcdPeers(t, hosts, members[leader])
		defer fault.heal(t)
	} else {
		docker(t, "kill", members[leader])
		defer docker(t, "start", members[leader])
	}
	majority := []int{}
	for _, i := range all {
		if i != leader {
			majority = append(majority, i)
		}
	}
	replacement, after := electedLeader(t, members, majority, before.Leader)
	if after.Term <= before.Term {
		t.Fatalf("term did not advance: %d -> %d", before.Term, after.Term)
	}
	if partition {
		// Wait for the isolated leader's own lease to expire, not merely for a
		// majority election. This also proves the original process is still alive.
		eventually(t, "isolated member relinquishes leadership", func() bool { return memberStatus(t, members[leader]).Leader == 0 })
		local := docker(t, "exec", members[leader], "/usr/local/bin/etcdctl", "get", etcdPrefix+deviceactor.ActorID(id), "--consistency=s", "--write-out=json", "--command-timeout=3s")
		var snapshot struct {
			Count int64 `json:"count"`
		}
		if json.Unmarshal([]byte(local), &snapshot) != nil || snapshot.Count != 1 {
			t.Fatal("isolated client endpoint lost local state")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	_, e = hosts[0].client.Invoke(ctx, id, "SetDesired", request)
	cancel()
	if e == nil {
		t.Fatal("mutation acknowledged through unavailable former leader")
	}
	if e = pending.Save(context.Background(), data); e == nil {
		t.Fatal("CAS acknowledged through unavailable former leader")
	}
	if partition {
		fault.assertDrops(t)
	}
	// Explicit test routing selects a reachable member. The production adapter
	// accepts one gateway URL; these tests do not imply automatic endpoint failover.
	routeSnapshots(t, hosts, members[replacement])
	assertEtcdReceipt(t, hosts, id, request)
	if e = old.Save(context.Background(), data); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("pre-election writer accepted by majority: %v", e)
	}
	if partition {
		fault.heal(t)
	} else {
		docker(t, "start", members[leader])
	}
	_, rejoined := electedLeader(t, members, all, 0)
	routeSnapshots(t, hosts, members[leader])
	assertEtcdReceipt(t, hosts, id, request)
	if e = rejoinStale.Save(context.Background(), data); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("former leader accepted pre-election writer after rejoin: %v", e)
	}
	// The original receipt must still be idempotent after the old leader rejoins.
	response, e := hosts[1].client.Invoke(context.Background(), id, "SetDesired", baseline)
	var receipt device.DesiredReceipt
	if e != nil || json.Unmarshal(response, &receipt) != nil || receipt.Revision != baseline.ExpectedRevision+1 {
		t.Fatalf("baseline receipt lost on rejoin: %s %v", response, e)
	}
	t.Logf("%s: leader %x -> %x; term %d -> %d; former leader rejoined term %d with receipts intact", name, before.Leader, after.Leader, before.Term, after.Term, rejoined.Term)
}
