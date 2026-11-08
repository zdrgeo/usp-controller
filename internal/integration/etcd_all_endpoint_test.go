package integration

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

func testAllEndpointPacketLoss(t *testing.T, hosts []*liveHost, members []string, id device.DeviceIdentity, mode string) {
	prepareMultiEndpointFault(t, hosts, members, id)
	enableMultiEndpoints(hosts)
	_, beforeElection := electedLeader(t, members, []int{0, 1, 2}, 0)
	baseline := etcdMutation(t, hosts[0], id, "all-packets-"+mode+"-baseline")
	assertEtcdReceipt(t, hosts, id, baseline)
	pending := etcdMutation(t, hosts[0], id, "all-packets-"+mode+"-recovery")
	// Keep probe connection pools local to this scenario; actor clients remain
	// unchanged across scenarios and must recover through the configured list.
	writerTransport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(writerTransport.CloseIdleConnections)
	writer, e := newLiveFailoverBackendWithClient(hosts[0], id, &http.Client{Transport: writerTransport, Timeout: 5 * time.Second})
	if e != nil {
		t.Fatal(e)
	}
	snapshot, e := writer.Load(context.Background())
	if e != nil {
		t.Fatal(e)
	}
	staleTransport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(staleTransport.CloseIdleConnections)
	stale, e := newLiveFailoverBackendWithClient(hosts[1], id, &http.Client{Transport: staleTransport, Timeout: 5 * time.Second})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = stale.Load(context.Background()); e != nil {
		t.Fatal(e)
	}

	probeTransport := http.DefaultTransport.(*http.Transport).Clone()
	t.Cleanup(probeTransport.CloseIdleConnections)
	probeClient := &http.Client{Transport: probeTransport, Timeout: 5 * time.Second}
	faults := make([]*packetFault, len(members))
	established := make([]string, len(members))
	for i, member := range members {
		// Warm every configured client path before injection, including endpoints
		// not yet selected by the sticky multi-endpoint backend.
		endpoint := fmt.Sprintf("http://127.0.0.1:%d", hosts[0].failoverSnapshots[i].listener.Addr().(*net.TCPAddr).Port)
		probe, e := deviceactor.NewEtcdSnapshotBackend(hosts[0].client.Endpoint, token, endpoint, etcdPrefix, "", probeClient, id, time.Second, 1<<20)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = probe.Load(context.Background()); e != nil {
			t.Fatal(e)
		}
		f := newPacketFault(t, &liveHost{name: member}, hosts[0].failoverSnapshots[i])
		f.server, f.port = true, "2379"
		faults[i] = f
		established[i] = f.established(t)
		if established[i] == "" {
			t.Fatalf("endpoint %d has no established client connection", i)
		}
	}
	// Client traffic is black-holed at every member. Peer port 2380 is untouched;
	// no backend, proxy target or configured endpoint changes during this scenario.
	for _, f := range faults {
		f.install(t, mode)
		defer f.heal(t)
	}
	time.Sleep(2 * time.Second)
	if mode == "half-open" {
		for i, f := range faults {
			if !sameConnection(established[i], f.established(t)) {
				t.Fatalf("endpoint %d lost its original half-open connection", i)
			}
		}
	}
	if e := writer.Save(context.Background(), snapshot); e == nil {
		t.Fatal("transaction acknowledged with every client path black-holed")
	}
	started := time.Now()
	if _, e := writer.Load(context.Background()); e == nil {
		t.Fatal("read succeeded with every endpoint black-holed")
	}
	if elapsed := time.Since(started); elapsed > 6*time.Second {
		t.Fatalf("all-endpoint read exceeded total budget: %s", elapsed)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	started = time.Now()
	_, e = writer.Load(ctx)
	cancel()
	if e == nil || time.Since(started) > time.Second {
		t.Fatalf("short caller deadline was not respected: %v", e)
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
				t.Fatalf("actor %s acknowledged while all endpoints were black-holed", method)
			}
		}
	}
	for i, f := range faults {
		t.Logf("endpoint %d fault counters", i)
		f.assertDrops(t)
	}
	// Recover only the LAST configured endpoint first. The existing backend and
	// both actor routes must recover while all earlier endpoints remain blocked.
	faults[len(faults)-1].heal(t)
	if e := writer.Save(context.Background(), snapshot); e == nil {
		t.Fatal("failed writer reused an invalidated version after partial recovery")
	}
	eventually(t, "existing backend recovers through last endpoint", func() bool {
		started := time.Now()
		_, err := writer.Load(context.Background())
		if elapsed := time.Since(started); elapsed > 6*time.Second {
			t.Fatalf("partial recovery exceeded total read budget: %s", elapsed)
		}
		return err == nil
	})
	assertEtcdReceipt(t, hosts, id, pending)
	for _, f := range faults {
		f.heal(t)
	}
	for i := range members {
		waitLiveEtcdEndpoint(t, hosts[0], id, i)
	}
	assertEtcdReceipt(t, hosts, id, pending)
	// Discard only this probe's idle TCP connection, retaining its loaded version.
	// Packet-loss retransmission backoff can outlast one request; the fence check
	// must reach etcd rather than merely observe another transport timeout.
	staleTransport.CloseIdleConnections()
	if e := stale.Save(context.Background(), snapshot); !errors.Is(e, deviceactor.ErrConflict) {
		t.Fatalf("stale writer survived all-endpoint recovery: %v", e)
	}
	_, afterElection := electedLeader(t, members, []int{0, 1, 2}, 0)
	if beforeElection.Leader != afterElection.Leader || beforeElection.Term != afterElection.Term {
		t.Fatal("client-path fault unexpectedly changed peer leadership")
	}
	t.Log("all endpoints rejected reads/writes within deadline; last-endpoint recovery retained one receipt and fenced stale state")
}
