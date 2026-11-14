package canary

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

func TestRepairSweepBoundsWorkAndDoesNotRetryFailedDevice(t *testing.T) {
	ids := []device.DeviceIdentity{{AgentID: "fail"}, {AgentID: "a"}, {AgentID: "b"}, {AgentID: "c"}}
	entered := make(chan string, 4)
	release := make(chan struct{})
	done := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	var active, peak atomic.Int32
	invoke := func(_ context.Context, id device.DeviceIdentity) error {
		current := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); current > old; old = peak.Load() {
			if peak.CompareAndSwap(old, current) {
				break
			}
		}
		entered <- id.AgentID
		if id.AgentID == "fail" {
			return errors.New("one unavailable actor")
		}
		<-release
		return nil
	}
	go func() {
		defer close(done)
		repairSweep(context.Background(), ids, 2, invoke, func() bool { return false })
	}()
	defer func() { unblock(); <-done }()
	seen := map[string]bool{}
	for i := 0; i < 3; i++ {
		select {
		case id := <-entered:
			if seen[id] {
				t.Fatal("device retried in sweep")
			}
			seen[id] = true
		case <-time.After(2 * time.Second):
			t.Fatal("failure blocked remaining actors")
		}
	}
	select {
	case <-entered:
		t.Fatal("exceeded worker bound")
	default:
	}
	if peak.Load() > 2 || active.Load() != 2 {
		t.Fatal("worker limit", peak.Load(), active.Load())
	}
	unblock()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sweep did not finish")
	}
	select {
	case id := <-entered:
		if id != "c" {
			t.Fatal("unexpected retry", id)
		}
	default:
		t.Fatal("last actor was skipped")
	}

}

func TestRepairCancellationJoinsWorkersAndStopsQueuedInvocations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ids := []device.DeviceIdentity{{AgentID: "a"}, {AgentID: "b"}, {AgentID: "c"}, {AgentID: "d"}}
	entered := make(chan struct{}, 4)
	done := make(chan struct{})
	var calls, active atomic.Int32
	invoke := func(ctx context.Context, _ device.DeviceIdentity) error {
		calls.Add(1)
		active.Add(1)
		defer active.Add(-1)
		entered <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	go func() { defer close(done); repairSweep(ctx, ids, 2, invoke, func() bool { return false }) }()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			t.Fatal("workers did not enter")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelled repair did not join workers")
	}
	if calls.Load() != 2 || active.Load() != 0 {
		t.Fatal("queued invocation or worker escaped cancellation", calls.Load(), active.Load())
	}
}

func TestRuntimeRepairInvokesOnlyActivateAndRejectsOverlappingRunner(t *testing.T) {
	entered := make(chan struct{}, 1)
	releaseRemote := make(chan struct{})
	sidecar := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/method/Activate") {
			t.Error("repair invoked a dispatch method", r.URL.Path)
			w.WriteHeader(500)
			return
		}
		entered <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-releaseRemote:
		}
	}))
	defer sidecar.Close()
	defer close(releaseRemote) // remote work may outlive a cancelled client request
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := &Runtime{Devices: map[string]device.DeviceIdentity{"device": {Tenant: "t", ControllerID: "c", AgentID: "device"}}, Client: &deviceactor.Client{Endpoint: sidecar.URL, HTTP: sidecar.Client()}, repairWorkers: 2}
	done := make(chan error, 1)
	go func() { done <- rt.RunReminderRepair(ctx) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("repair did not activate through Dapr")
	}
	if err := rt.RunReminderRepair(context.Background()); err == nil {
		t.Fatal("overlapping runner accepted")
	}
	rt.BeginDrain()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runtime repair failed to stop")
	}
	if rt.repairRunning.Load() {
		t.Fatal("repair runner remained active")
	}
}
