package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

// tcpPartition closes established streams as well as rejecting new ones. Only
// the selected sidecar's Placement path traverses it; state and app paths do not.
type tcpPartition struct {
	listener    net.Listener
	target      string
	mu          sync.Mutex
	blocked     bool
	connections map[net.Conn]bool
	rejected    int
}

func newTCPPartition(t *testing.T, target string) *tcpPartition {
	t.Helper()
	l, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpPartition{listener: l, target: target, connections: map[net.Conn]bool{}}
	t.Cleanup(func() { l.Close(); p.setBlocked(true) })
	go func() {
		for {
			c, e := l.Accept()
			if e != nil {
				return
			}
			go p.forward(c)
		}
	}()
	return p
}
func (p *tcpPartition) address() string {
	return fmt.Sprintf("host.docker.internal:%d", p.listener.Addr().(*net.TCPAddr).Port)
}
func (p *tcpPartition) setBlocked(blocked bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocked = blocked
	if blocked {
		for c := range p.connections {
			c.Close()
		}
	}
}
func (p *tcpPartition) forward(c net.Conn) {
	defer c.Close()
	p.mu.Lock()
	if p.blocked {
		p.rejected++
		p.mu.Unlock()
		return
	}
	p.connections[c] = true
	target := p.target
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.connections, c); p.mu.Unlock() }()
	upstream, err := net.DialTimeout("tcp", target, time.Second)
	if err != nil {
		return
	}
	defer upstream.Close()
	p.mu.Lock()
	if p.blocked {
		p.rejected++
		p.mu.Unlock()
		return
	}
	p.connections[upstream] = true
	p.mu.Unlock()
	defer func() { p.mu.Lock(); delete(p.connections, upstream); p.mu.Unlock() }()
	done := make(chan struct{})
	go func() { io.Copy(upstream, c); upstream.Close(); close(done) }()
	io.Copy(c, upstream)
	c.Close()
	<-done
}

// Delay a real actor save after its snapshot has been read/modified. The original
// callback snapshot is preserved. The delayed storage call deliberately detaches
// cancellation to test the sidecar fence itself, not just a timed-out HTTP client.
// This fault injection exists only in tests. One explicitly armed save is held.
type heldSave struct {
	deviceactor.Backend
	mu      sync.Mutex
	marker  string
	entered chan struct{}
	release chan struct{}
	result  chan error
}

func (g *heldSave) arm(marker string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.marker = marker
	g.entered = make(chan struct{})
	g.release = make(chan struct{})
	g.result = make(chan error, 1)
}
func (g *heldSave) Save(ctx context.Context, b []byte) error {
	g.mu.Lock()
	hold := g.marker != "" && bytes.Contains(b, []byte(g.marker))
	if hold {
		g.marker = ""
	}
	g.mu.Unlock()
	if !hold {
		return g.Backend.Save(ctx, b)
	}
	close(g.entered)
	<-g.release
	err := g.Backend.Save(context.WithoutCancel(ctx), b)
	g.result <- err
	return err
}

func testPlacementPartition(t *testing.T, hosts []*liveHost, id device.DeviceIdentity) {
	// Identify the owner without assuming a placement hash or host order.
	before := hosts[0].writes.Load()
	baseline := device.SetDesiredRequest{ID: "partition-baseline", ExpectedRevision: 3, Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "baseline", Type: "string"}}}
	b, err := hosts[0].client.Invoke(context.Background(), id, "SetDesired", baseline)
	if err != nil {
		t.Fatal(err)
	}
	var receipt device.DesiredReceipt
	if json.Unmarshal(b, &receipt) != nil || receipt.Revision != 4 {
		t.Fatalf("baseline receipt: %s", b)
	}
	owner := 1
	if hosts[0].writes.Load() > before {
		owner = 0
	}
	old, newOwner := hosts[owner], hosts[1-owner]
	gate := old.saves[id.AgentID]
	gate.arm("partition-stale")
	released := false
	defer func() {
		if !released {
			close(gate.release)
		}
		old.placement.setBlocked(false)
	}()
	stale := device.SetDesiredRequest{ID: "partition-stale", ExpectedRevision: 4, Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "stale", Type: "string"}}}
	oldResult := make(chan error, 1)
	go func() { _, e := old.client.Invoke(context.Background(), id, "SetDesired", stale); oldResult <- e }()
	select {
	case <-gate.entered:
	case <-time.After(20 * time.Second):
		t.Fatal("stale writer did not reach save barrier")
	}
	old.placement.setBlocked(true)
	replacement := device.SetDesiredRequest{ID: "partition-new-owner", ExpectedRevision: 4, Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "new-owner", Type: "string"}}}
	eventually(t, "replacement owner commits while old Placement path is cut", func() bool {
		b, e := newOwner.client.Invoke(context.Background(), id, "SetDesired", replacement)
		var r device.DesiredReceipt
		return e == nil && json.Unmarshal(b, &r) == nil && r.Revision == 5
	})
	// A fresh admission through the isolated sidecar must not bypass its
	// Placement gate even though its state network is still healthy.
	beforeBlocked := old.writes.Load()
	blockedCtx, cancelBlocked := context.WithTimeout(context.Background(), 2*time.Second)
	_, blockedErr := old.client.Invoke(blockedCtx, id, "SetDesired", device.SetDesiredRequest{ID: "must-not-enter", ExpectedRevision: 5, Values: baseline.Values})
	cancelBlocked()
	if blockedErr == nil || old.writes.Load() != beforeBlocked {
		t.Fatal("isolated sidecar admitted another actor turn")
	}
	assertRedisReachable(t, old.state)
	old.placement.mu.Lock()
	rejected := old.placement.rejected
	old.placement.mu.Unlock()
	if rejected == 0 {
		t.Fatal("fault was not exercised: no Placement reconnect rejected")
	}
	close(gate.release)
	released = true
	select {
	case e := <-gate.result:
		if e == nil {
			t.Error("stale actor snapshot committed after reassignment")
		} else {
			t.Logf("late storage write failed while isolated: %v", e)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stale save did not finish")
	}
	select {
	case e := <-oldResult:
		if e == nil {
			t.Error("stale mutation acknowledged")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("old invocation did not finish")
	}
	assertCurrent := func(h *liveHost) bool {
		b, e := h.client.Invoke(context.Background(), id, "GetTwin", nil)
		var twin device.DeviceTwin
		return e == nil && json.Unmarshal(b, &twin) == nil && twin.DesiredRevision == 5 && twin.Desired["Device.Test.Value"].Value == "new-owner"
	}
	if !assertCurrent(newOwner) {
		t.Fatal("stale writer replaced new owner state")
	}
	old.placement.setBlocked(false)
	eventually(t, "both routes observe new owner state after healing", func() bool { return assertCurrent(old) && assertCurrent(newOwner) })
	// Observe a full backend timeout interval after healing; a timed-out write
	// must not materialize later and overwrite the replacement's state.
	until := time.Now().Add(10 * time.Second)
	for time.Now().Before(until) {
		if !assertCurrent(old) || !assertCurrent(newOwner) {
			t.Fatal("late write or inconsistent state after healing")
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Log("Placement stream partition: replacement committed, delayed storage call failed, state preserved through recovery window")
}

// Partition only the active owner's Redis route, keeping the process and
// Placement connection alive. Failed admissions must not be acknowledged.
func testStatePartition(t *testing.T, hosts []*liveHost, id device.DeviceIdentity) {
	before := hosts[0].writes.Load()
	existing := device.SetDesiredRequest{ID: "after-outage", ExpectedRevision: 1, Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "target", Type: "string"}}}
	if _, err := hosts[0].client.Invoke(context.Background(), id, "SetDesired", existing); err != nil {
		t.Fatal(err)
	}
	owner := 1
	if hosts[0].writes.Load() > before {
		owner = 0
	}
	isolated := hosts[owner]
	isolated.state.setBlocked(true)
	defer isolated.state.setBlocked(false)
	request := device.SetDesiredRequest{ID: "state-partition", ExpectedRevision: 2, Values: existing.Values}
	for _, host := range hosts {
		if _, err := host.client.Invoke(context.Background(), id, "SetDesired", request); err == nil {
			t.Fatal("mutation acknowledged across owner state partition")
		}
	}
	isolated.state.mu.Lock()
	rejected := isolated.state.rejected
	isolated.state.mu.Unlock()
	if rejected == 0 {
		t.Fatal("state partition did not reject a reconnect")
	}
	// Redis itself remains available through the other host's network route.
	assertRedisReachable(t, hosts[1-owner].state)
	isolated.state.setBlocked(false)
	eventually(t, "stable receipt after owner state route heals", func() bool {
		b, e := hosts[1-owner].client.Invoke(context.Background(), id, "SetDesired", request)
		var r device.DesiredReceipt
		return e == nil && json.Unmarshal(b, &r) == nil && r.Revision == 3
	})
	t.Log("owner-only Redis partition: no successful admission; independent route healthy; retry converged")
}

func (p *tcpPartition) setTarget(target string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.target = target
	for c := range p.connections {
		c.Close()
	}
}

// Prove that a fault closes existing streams, rejects retries, and heals, rather
// than merely changing configuration while live gRPC connections survive.
func TestTCPPartitionClosesStreamsAndHeals(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		for {
			c, e := listener.Accept()
			if e != nil {
				return
			}
			go func() { defer c.Close(); io.Copy(c, c) }()
		}
	}()
	p := newTCPPartition(t, listener.Addr().String())
	address := fmt.Sprintf("127.0.0.1:%d", p.listener.Addr().(*net.TCPAddr).Port)
	dial := func() net.Conn {
		t.Helper()
		c, e := net.DialTimeout("tcp", address, time.Second)
		if e != nil {
			t.Fatal(e)
		}
		c.SetDeadline(time.Now().Add(2 * time.Second))
		return c
	}
	echo := func(c net.Conn) {
		t.Helper()
		if _, e := c.Write([]byte("ping")); e != nil {
			t.Fatal(e)
		}
		b := make([]byte, 4)
		if _, e := io.ReadFull(c, b); e != nil || string(b) != "ping" {
			t.Fatalf("echo %q: %v", b, e)
		}
	}
	c := dial()
	defer c.Close()
	echo(c)
	p.setBlocked(true)
	if _, err = c.Read(make([]byte, 1)); err == nil {
		t.Fatal("established stream survived cut")
	}
	retry := dial()
	defer retry.Close()
	if _, err = retry.Read(make([]byte, 1)); err == nil {
		t.Fatal("retry stream survived cut")
	}
	p.mu.Lock()
	rejected := p.rejected
	p.mu.Unlock()
	if rejected == 0 {
		t.Fatal("no rejected retry observed")
	}
	p.setBlocked(false)
	healed := dial()
	defer healed.Close()
	echo(healed)
}

func assertRedisReachable(t *testing.T, p *tcpPartition) {
	t.Helper()
	address := fmt.Sprintf("127.0.0.1:%d", p.listener.Addr().(*net.TCPAddr).Port)
	c, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(time.Second))
	if _, err = c.Write([]byte("*1\r\n$4\r\nPING\r\n")); err != nil {
		t.Fatal(err)
	}
	reply := make([]byte, 7)
	if _, err = io.ReadFull(c, reply); err != nil || string(reply) != "+PONG\r\n" {
		t.Fatalf("unaffected Redis route: %q %v", reply, err)
	}
}
