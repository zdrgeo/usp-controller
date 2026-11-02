package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

const netshootImage = "nicolaka/netshoot:v0.14"

type packetFault struct {
	helper string
	port   string
	chains []string
}

func newPacketFault(t *testing.T, h *liveHost, p *tcpPartition) *packetFault {
	t.Helper()
	// NET_ADMIN is restricted to this disposable sidecar's network namespace.
	// No host namespace, Docker socket, host mount, or privileged container is used.
	name := h.name + "-packets"
	docker(t, "run", "-d", "--name", name, "--network", "container:"+h.name, "--cap-add", "NET_ADMIN", netshootImage, "sleep", "600")
	t.Cleanup(func() { docker(t, "rm", "-f", name) })
	return &packetFault{helper: name, port: strconv.Itoa(p.listener.Addr().(*net.TCPAddr).Port)}
}
func (f *packetFault) established(t *testing.T) string {
	t.Helper()
	return docker(t, "exec", f.helper, "ss", "-tnH", "state", "established", "(", "dport", "=", ":"+f.port, ")")
}
func (f *packetFault) install(t *testing.T, mode string) {
	t.Helper()
	for _, direction := range []string{"OUTPUT", "INPUT"} {
		if mode == "outbound" && direction != "OUTPUT" || mode == "inbound" && direction != "INPUT" {
			continue
		}
		chain := "USP_LOSS_" + direction
		docker(t, "exec", f.helper, "iptables", "-w", "-N", chain)
		f.chains = append(f.chains, direction)
		// Register exact cleanup before adding rules; never flush unrelated rules.
		t.Cleanup(func() {
			portFlag := "--dport"
			if direction == "INPUT" {
				portFlag = "--sport"
			}
			docker(t, "exec", f.helper, "iptables", "-w", "-D", direction, "-p", "tcp", portFlag, f.port, "-j", chain)
			docker(t, "exec", f.helper, "iptables", "-w", "-F", chain)
			docker(t, "exec", f.helper, "iptables", "-w", "-X", chain)
		})
		portFlag := "--dport"
		if direction == "INPUT" {
			portFlag = "--sport"
		}
		docker(t, "exec", f.helper, "iptables", "-w", "-I", direction, "1", "-p", "tcp", portFlag, f.port, "-j", chain)
		docker(t, "exec", f.helper, "iptables", "-w", "-A", chain, "-j", "DROP")
	}
}
func (f *packetFault) heal(t *testing.T) {
	t.Helper()
	for _, direction := range f.chains {
		docker(t, "exec", f.helper, "iptables", "-w", "-F", "USP_LOSS_"+direction)
	}
}
func (f *packetFault) assertDrops(t *testing.T) {
	t.Helper()
	var total uint64
	for _, direction := range f.chains {
		report := docker(t, "exec", f.helper, "iptables", "-w", "-L", "USP_LOSS_"+direction, "-n", "-v", "-x")
		var count uint64
		for _, line := range strings.Split(report, "\n") {
			fields := strings.Fields(line)
			if len(fields) >= 3 && fields[2] == "DROP" {
				n, err := strconv.ParseUint(fields[0], 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				count += n
			}
		}
		total += count
		t.Logf("%s packets dropped: %d", direction, count)
	}
	if total == 0 {
		t.Fatal("no packets hit the fault rules")
	}
}
func sameConnection(before, after string) bool {
	// Ignore send/receive queue sizes; compare the local and remote socket tuple.
	tuples := map[string]bool{}
	for _, line := range strings.Split(before, "\n") {
		v := strings.Fields(line)
		if len(v) >= 4 {
			tuples[v[len(v)-2]+"|"+v[len(v)-1]] = true
		}
	}
	for _, line := range strings.Split(after, "\n") {
		v := strings.Fields(line)
		if len(v) >= 4 && tuples[v[len(v)-2]+"|"+v[len(v)-1]] {
			return true
		}
	}
	return false
}

func testPacketLoss(t *testing.T, hosts []*liveHost, id device.DeviceIdentity, dependency, mode string) {
	var twin device.DeviceTwin
	b, err := hosts[0].client.Invoke(context.Background(), id, "GetTwin", nil)
	if err != nil || json.Unmarshal(b, &twin) != nil {
		t.Fatalf("baseline twin: %s %v", b, err)
	}
	prefix := "packet-" + dependency + "-" + mode
	before := hosts[0].mutations[deviceactor.ActorID(id)].Load()
	baseline := device.SetDesiredRequest{ID: device.RequestID(prefix + "-baseline"), ExpectedRevision: twin.DesiredRevision, Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: prefix, Type: "string"}}}
	b, err = hosts[0].client.Invoke(context.Background(), id, "SetDesired", baseline)
	var base device.DesiredReceipt
	if err != nil || json.Unmarshal(b, &base) != nil || base.Revision != twin.DesiredRevision+1 {
		t.Fatalf("baseline receipt: %s %v", b, err)
	}
	owner := 1
	if hosts[0].mutations[deviceactor.ActorID(id)].Load() > before {
		owner = 0
	}
	old, other := hosts[owner], hosts[1-owner]
	proxy := old.state
	if dependency == "placement" {
		proxy = old.placement
	}
	fault := newPacketFault(t, old, proxy)
	established := fault.established(t)
	if established == "" {
		t.Fatal("fault requires an established dependency connection")
	}
	if mode != "control" {
		fault.install(t, mode)
	}
	defer fault.heal(t)
	// Keep the socket open long enough to observe a true black hole. Placement
	// probes begin only after a further heartbeat-loss interval, so they do not
	// merely race ahead of dependency-failure detection.
	time.Sleep(2 * time.Second)
	if mode == "half-open" && !sameConnection(established, fault.established(t)) {
		t.Fatal("no original ESTABLISHED connection survived the black-hole interval")
	}
	if dependency == "placement" && mode != "control" {
		time.Sleep(10 * time.Second)
	}

	requests := []device.SetDesiredRequest{{ID: device.RequestID(prefix + "-a"), ExpectedRevision: base.Revision, Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: prefix + "-a", Type: "string"}}}}
	routes := []*liveHost{old}
	if dependency == "placement" {
		requests = append(requests, device.SetDesiredRequest{ID: device.RequestID(prefix + "-b"), ExpectedRevision: base.Revision, Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: prefix + "-b", Type: "string"}}})
		routes = append(routes, other)
	}
	type outcome struct {
		index int
		body  json.RawMessage
		err   error
	}
	results := make(chan outcome, len(requests))
	for i, r := range requests {
		go func(i int, r device.SetDesiredRequest) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			b, e := routes[i].client.Invoke(ctx, id, "SetDesired", r)
			results <- outcome{i, b, e}
		}(i, r)
	}
	successes := 0
	for range requests {
		r := <-results
		t.Logf("fault result id=%s receipt=%s error=%v", requests[r.index].ID, r.body, r.err)
		if r.err == nil {
			successes++
			var receipt device.DesiredReceipt
			if json.Unmarshal(r.body, &receipt) != nil || receipt.ID != requests[r.index].ID || receipt.Revision != base.Revision+1 {
				t.Fatalf("invalid receipt during fault: %s", r.body)
			}
		}
	}
	if dependency == "state" && mode != "control" && successes != 0 {
		t.Fatal("state-path packet loss acknowledged admission")
	}
	if mode != "control" {
		fault.assertDrops(t)
	}
	if successes > 1 {
		t.Fatal("conflicting revisions admitted during packet loss")
	}
	if dependency == "placement" {
		assertRedisReachable(t, old.state)
	} else {
		assertRedisReachable(t, other.state)
	}
	fault.heal(t)
	// Timeouts are ambiguous. Retry stable IDs instead of assuming no commit.
	winner := -1
	diagnostics := map[device.RequestID]string{}
	defer func() {
		if t.Failed() {
			t.Logf("last receipt recovery results: %v", diagnostics)
		}
	}()
	eventually(t, "one stable winning receipt after packet-loss recovery", func() bool {
		found := -1
		for i, r := range requests {
			b, e := other.client.Invoke(context.Background(), id, "SetDesired", r)
			diagnostics[r.ID] = string(b)
			if e != nil {
				diagnostics[r.ID] = e.Error()
			}
			if errors.Is(e, deviceactor.ErrConflict) {
				continue
			}
			if e != nil {
				return false
			}
			var receipt device.DesiredReceipt
			if json.Unmarshal(b, &receipt) != nil || receipt.Revision != base.Revision+1 || receipt.ID != r.ID {
				t.Fatalf("recovered receipt: %s", b)
			}
			if found != -1 {
				t.Fatal("two competing mutations won the same revision")
			}
			found = i
		}
		winner = found
		return found != -1
	})
	// Both routes must return the same winning receipt and exact final state.
	eventually(t, "routes converge after packet loss", func() bool {
		for _, h := range hosts {
			b, e := h.client.Invoke(context.Background(), id, "SetDesired", requests[winner])
			var r device.DesiredReceipt
			if e != nil || json.Unmarshal(b, &r) != nil || r.Revision != base.Revision+1 {
				return false
			}
			b, e = h.client.Invoke(context.Background(), id, "GetTwin", nil)
			var v device.DeviceTwin
			if e != nil || json.Unmarshal(b, &v) != nil || v.DesiredRevision != base.Revision+1 || v.Desired["Device.Test.Value"].Value != requests[winner].Values["Device.Test.Value"].Value {
				return false
			}
		}
		return true
	})
	until := time.Now().Add(5 * time.Second)
	for time.Now().Before(until) {
		for _, h := range hosts {
			b, e := h.client.Invoke(context.Background(), id, "GetTwin", nil)
			var v device.DeviceTwin
			if e != nil || json.Unmarshal(b, &v) != nil || v.DesiredRevision != base.Revision+1 || v.Desired["Device.Test.Value"].Value != requests[winner].Values["Device.Test.Value"].Value {
				t.Fatalf("late or inconsistent state after packet recovery: %s %v", b, e)
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Logf("%s %s: %d acknowledged during fault; one receipt retained after healing", dependency, mode, successes)
}

func TestSocketTupleComparison(t *testing.T) {
	if !sameConnection("0 0 10.0.0.1:123 10.0.0.2:456", "12 34 10.0.0.1:123 10.0.0.2:456") {
		t.Fatal("queue changes must not change identity")
	}
	if sameConnection("0 0 10.0.0.1:123 10.0.0.2:456", "0 0 10.0.0.1:124 10.0.0.2:456") {
		t.Fatal("new socket mistaken for half-open socket")
	}
}

func testLiveSnapshotCAS(t *testing.T, hosts []*liveHost, id device.DeviceIdentity) {
	id.AgentID = "storage-cas-probe"
	backends := make([]*deviceactor.FencedDaprBackend, 2)
	for i, h := range hosts {
		b, e := deviceactor.NewFencedDaprBackend(h.client.Endpoint, nil, token, id, time.Second, 1<<20, "state")
		if e != nil {
			t.Fatal(e)
		}
		data, e := b.Load(context.Background())
		if e != nil || len(data) != 0 {
			t.Fatalf("expected empty snapshot: %s %v", data, e)
		}
		backends[i] = b
	}
	if e := backends[0].Save(context.Background(), []byte(`{"n":1}`)); e != nil {
		t.Fatal(e)
	}
	if e := backends[1].Save(context.Background(), []byte(`{"n":2}`)); e == nil {
		t.Fatal("two initializers accepted", e)
	}
	if _, e := backends[1].Load(context.Background()); e != nil {
		t.Fatal(e)
	}
	if e := backends[0].Save(context.Background(), []byte(`{"n":3}`)); e != nil {
		t.Fatal(e)
	}
	if e := backends[1].Save(context.Background(), []byte(`{"n":4}`)); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatal("stale existing version accepted", e)
	}
	for _, b := range backends {
		data, e := b.Load(context.Background())
		if e != nil || string(data) != `{"n":3}` {
			t.Fatalf("snapshot changed after stale write: %s %v", data, e)
		}
	}
}
