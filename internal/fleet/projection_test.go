package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

type memoryStore struct {
	data            map[string][]byte
	fail, ambiguous bool
	saves           int
}

func (s *memoryStore) Load(k string) ([]byte, error) { return append([]byte(nil), s.data[k]...), nil }
func (s *memoryStore) Save(k string, b []byte) error {
	if s.fail {
		return errors.New("disk full")
	}
	s.data[k] = append([]byte(nil), b...)
	s.saves++
	if s.ambiguous {
		s.ambiguous = false
		return errors.New("lost fsync result")
	}
	return nil
}
func (s *memoryStore) Close() error { return nil }

type invokeFunc func(context.Context, device.DeviceIdentity, string, any) (json.RawMessage, error)

func (f invokeFunc) Invoke(c context.Context, id device.DeviceIdentity, m string, v any) (json.RawMessage, error) {
	return f(c, id, m, v)
}
func identity(id string) device.DeviceIdentity {
	return device.DeviceIdentity{Tenant: "tenant", ControllerID: "controller", AgentID: id}
}
func status(rev uint64) deviceactor.Status {
	return deviceactor.Status{SiteID: "site", Convergence: "pending", DesiredRevision: rev, Reconciliation: "waiting_observation", CommandStates: map[device.CommandState]int{device.CommandQueued: 1}, MaxCommands: 64, MaxReceipts: 1024, MaxStateBytes: 1 << 20, StateBytes: 200}
}
func fixture(t *testing.T) (*Projection, *memoryStore, *time.Time) {
	t.Helper()
	now := time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC)
	s := &memoryStore{data: map[string][]byte{}}
	o := Options{Directory: "test", SiteID: "site", RefreshInterval: time.Minute, StaleAfter: 3 * time.Minute, Workers: 2, Now: func() time.Time { return now }}
	inv := invokeFunc(func(_ context.Context, _ device.DeviceIdentity, m string, _ any) (json.RawMessage, error) {
		if m != "GetStatus" {
			t.Errorf("mutating method %s", m)
		}
		return json.Marshal(status(1))
	})
	p, err := newProjection(o, map[string]device.DeviceIdentity{"a": identity("a"), "b": identity("b")}, inv, s)
	if err != nil {
		t.Fatal(err)
	}
	return p, s, &now
}
func refresh(p *Projection, id string) {
	p.mu.Lock()
	p.inFlight[id] = true
	p.mu.Unlock()
	p.refresh(context.Background(), id)
}
func event(t *testing.T, id, epoch string, seq uint64, value string) []byte {
	t.Helper()
	message := json.RawMessage(fmt.Sprintf(`{"header":{"msgId":"n-%d","msgType":"NOTIFY"},"body":{"request":{"notify":{"subscriptionId":"s","valueChange":{"paramPath":"Device.Secret.Value","paramValue":%q}}}}}`, seq, value))
	b, err := json.Marshal(envelope{ActorID: deviceactor.ActorID(identity(id)), Identity: identity(id), Epoch: epoch, Sequence: seq, SchemaVersion: "1", ReceivedAt: time.Date(2026, 11, 2, 0, 0, 0, 0, time.UTC), Message: message})
	if err != nil {
		t.Fatal(err)
	}
	return b
}
func TestEventsAreDurableDeduplicatedHints(t *testing.T) {
	p, s, now := fixture(t)
	refresh(p, "a")
	original := event(t, "a", strings.Repeat("a", 32), 2, "SECRET-NOT-STORED")
	if err := p.Ingest(original); err != nil {
		t.Fatal(err)
	}
	count := s.saves
	if err := p.Ingest(original); err != nil || s.saves != count {
		t.Fatal("duplicate committed", err)
	}
	if !p.rows["a"].Pending || p.rows["a"].Snapshot.DesiredRevision != 1 {
		t.Fatal("event overwrote authoritative snapshot")
	}
	for _, b := range s.data {
		if strings.Contains(string(b), "SECRET-NOT-STORED") {
			t.Fatal("stored payload")
		}
	}
	if err := p.Ingest(event(t, "a", strings.Repeat("a", 32), 2, "different")); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	// A lower sequence in a different epoch is another hint, not older/newer state.
	if err := p.Ingest(event(t, "a", strings.Repeat("b", 32), 1, "v")); err != nil {
		t.Fatal(err)
	}
	restarted, err := newProjection(p.opts, map[string]device.DeviceIdentity{"a": identity("a"), "b": identity("b")}, p.invoker, s)
	if err != nil {
		t.Fatal(err)
	}
	if !restarted.rows["a"].Pending || len(restarted.rows["a"].Receipts) != 2 {
		t.Fatal("lost durable invalidation")
	}
	refresh(restarted, "a")
	if restarted.view(restarted.rows["a"], *now).Freshness != "fresh" {
		t.Fatal("not refreshed")
	}
	*now = now.Add(4 * time.Minute)
	if restarted.view(restarted.rows["a"], *now).Freshness != "stale" {
		t.Fatal("age not reflected")
	}
}
func TestReceiptEvictionIsSafeAndBounded(t *testing.T) {
	p, _, _ := fixture(t)
	for i := uint64(1); i <= receiptLimit+5; i++ {
		if err := p.Ingest(event(t, "a", strings.Repeat("c", 32), i, "v")); err != nil {
			t.Fatal(err)
		}
	}
	if len(p.rows["a"].Receipts) != receiptLimit {
		t.Fatal("unbounded receipts")
	}
	if err := p.Ingest(event(t, "a", strings.Repeat("c", 32), 1, "v")); err != nil {
		t.Fatal(err)
	}
	if len(p.rows["a"].Receipts) != receiptLimit || !p.rows["a"].Pending {
		t.Fatal("old replay not bounded")
	}
}
func TestPersistenceFailureRequiresRestart(t *testing.T) {
	for _, ambiguous := range []bool{false, true} {
		t.Run(fmt.Sprint(ambiguous), func(t *testing.T) {
			p, s, _ := fixture(t)
			s.fail = !ambiguous
			s.ambiguous = ambiguous
			e := event(t, "a", strings.Repeat("d", 32), 1, "v")
			if err := p.Ingest(e); !errors.Is(err, ErrUnavailable) {
				t.Fatal(err)
			}
			s.fail = false
			if err := p.Ingest(e); !errors.Is(err, ErrUnavailable) {
				t.Fatal("continued after uncertain persistence", err)
			}
			restarted, err := newProjection(p.opts, map[string]device.DeviceIdentity{"a": identity("a"), "b": identity("b")}, p.invoker, s)
			if err != nil {
				t.Fatal(err)
			}
			if err = restarted.Ingest(e); err != nil {
				t.Fatal(err)
			}
			if !restarted.rows["a"].Pending || len(restarted.rows["a"].Receipts) != 1 {
				t.Fatal("retry lost receipt")
			}
		})
	}
}
func TestConcurrentInvalidationAndRefreshFailure(t *testing.T) {
	p, _, now := fixture(t)
	refresh(p, "a")
	entered, release := make(chan struct{}), make(chan struct{})
	p.invoker = invokeFunc(func(context.Context, device.DeviceIdentity, string, any) (json.RawMessage, error) {
		close(entered)
		<-release
		return json.Marshal(status(2))
	})
	done := make(chan struct{})
	go func() { refresh(p, "a"); close(done) }()
	<-entered
	if err := p.Ingest(event(t, "a", strings.Repeat("e", 32), 1, "v")); err != nil {
		t.Fatal(err)
	}
	close(release)
	<-done
	if !p.rows["a"].Pending || p.rows["a"].Snapshot.DesiredRevision != 2 {
		t.Fatal("lost event during status read")
	}
	p.invoker = invokeFunc(func(context.Context, device.DeviceIdentity, string, any) (json.RawMessage, error) {
		return nil, errors.New("sidecar secret should not leak")
	})
	refresh(p, "a")
	if p.rows["a"].LastError != "refresh_failed" || p.view(p.rows["a"], *now).Freshness != "stale" {
		t.Fatal("failed refresh reported fresh")
	}
	// Authoritative state may regress after restore. Accept its snapshot, never
	// select a winner from random event epochs or prior cached revision numbers.
	p.invoker = invokeFunc(func(context.Context, device.DeviceIdentity, string, any) (json.RawMessage, error) {
		return json.Marshal(status(1))
	})
	refresh(p, "a")
	if p.rows["a"].Snapshot.DesiredRevision != 1 || p.rows["a"].Pending {
		t.Fatal("ignored restored actor truth")
	}
}
func TestRunBoundsConcurrencyAndCancels(t *testing.T) {
	p, _, _ := fixture(t)
	p.opts.Now = time.Now
	var mu sync.Mutex
	active, maxActive, calls := 0, 0, map[string]int{}
	entered := make(chan struct{}, 2)
	p.invoker = invokeFunc(func(ctx context.Context, id device.DeviceIdentity, m string, _ any) (json.RawMessage, error) {
		if m != "GetStatus" {
			t.Error(m)
		}
		mu.Lock()
		active++
		if active > maxActive {
			maxActive = active
		}
		calls[id.AgentID]++
		mu.Unlock()
		entered <- struct{}{}
		<-ctx.Done()
		mu.Lock()
		active--
		mu.Unlock()
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	for i := 0; i < 2; i++ {
		select {
		case <-entered:
		case <-time.After(2 * time.Second):
			cancel()
			t.Fatal("workers did not start")
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not stop")
	}
	mu.Lock()
	defer mu.Unlock()
	if maxActive != 2 || calls["a"] != 1 || calls["b"] != 1 {
		t.Fatal(maxActive, calls)
	}
}
func TestFileStoreLockScopeAndRestart(t *testing.T) {
	now := time.Now()
	o := Options{Directory: t.TempDir(), SiteID: "site", RefreshInterval: time.Minute, StaleAfter: 3 * time.Minute, Workers: 1, Now: func() time.Time { return now }}
	inv := invokeFunc(func(context.Context, device.DeviceIdentity, string, any) (json.RawMessage, error) {
		return json.Marshal(status(1))
	})
	inventory := map[string]device.DeviceIdentity{"a": identity("a")}
	p, err := Open(o, inventory, inv)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := Open(o, inventory, inv); err == nil {
		other.Close()
		t.Fatal("two owners")
	}
	refresh(p, "a")
	if err = p.Close(); err != nil {
		t.Fatal(err)
	}
	p, err = Open(o, inventory, inv)
	if err != nil {
		t.Fatal(err)
	}
	if p.rows["a"].Snapshot == nil || p.view(p.rows["a"], now).Freshness != "stale" {
		t.Fatal("restart claimed freshness")
	}
	p.Close()
	o.SiteID = "other"
	if other, err := Open(o, inventory, inv); err == nil {
		other.Close()
		t.Fatal("reused directory across sites")
	}
}

func TestPeriodicRefreshAndEventStormCoalescing(t *testing.T) {
	p, _, now := fixture(t)
	var clock atomic.Int64
	clock.Store(now.UnixNano())
	p.opts.Now = func() time.Time { return time.Unix(0, clock.Load()) }
	calls := make(chan string, 20)
	p.invoker = invokeFunc(func(_ context.Context, id device.DeviceIdentity, _ string, _ any) (json.RawMessage, error) {
		calls <- id.AgentID
		return json.Marshal(status(1))
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- p.Run(ctx) }()
	defer func() { cancel(); <-done }()
	wait := func(n int) {
		t.Helper()
		for i := 0; i < n; i++ {
			select {
			case <-calls:
			case <-time.After(2 * time.Second):
				t.Fatal("refresh missing")
			}
		}
	}
	wait(2)
	// Wait until commit completes so the following observations are deterministic.
	deadline := time.Now().Add(2 * time.Second)
	for {
		p.mu.Lock()
		busy := len(p.inFlight)
		p.mu.Unlock()
		if busy == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("refresh stuck")
		}
		time.Sleep(time.Millisecond)
	}
	for i := uint64(1); i <= 10; i++ {
		if err := p.Ingest(event(t, "a", strings.Repeat("a", 32), i, "v")); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-calls:
		t.Fatal("event storm bypassed minimum refresh spacing")
	case <-time.After(20 * time.Millisecond):
	}
	clock.Add(int64(6 * time.Second))
	p.signal()
	wait(1)
	deadline = time.Now().Add(2 * time.Second)
	for {
		p.mu.Lock()
		busy := len(p.inFlight)
		p.mu.Unlock()
		if busy == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("refresh stuck")
		}
		time.Sleep(time.Millisecond)
	}
	clock.Add(int64(time.Minute))
	p.signal()
	wait(2) // captures mutations with no Notify
}

func TestInvalidStatusNeverReplacesSnapshot(t *testing.T) {
	p, _, now := fixture(t)
	refresh(p, "a")
	p.invoker = invokeFunc(func(context.Context, device.DeviceIdentity, string, any) (json.RawMessage, error) {
		bad := status(99)
		bad.SiteID = "other"
		return json.Marshal(bad)
	})
	refresh(p, "a")
	if p.rows["a"].Snapshot.DesiredRevision != 1 || p.view(p.rows["a"], *now).Freshness != "stale" {
		t.Fatal("invalid site overwrote snapshot")
	}
}

func TestProjectionDiskCorruptionFailsClosedAndRemovedDevicesStayHidden(t *testing.T) {
	o := Options{Directory: t.TempDir(), SiteID: "site", RefreshInterval: time.Minute, StaleAfter: 3 * time.Minute, Workers: 1, Now: time.Now}
	inv := invokeFunc(func(context.Context, device.DeviceIdentity, string, any) (json.RawMessage, error) {
		return json.Marshal(status(1))
	})
	inventory := map[string]device.DeviceIdentity{"a": identity("a"), "b": identity("b")}
	p, err := Open(o, inventory, inv)
	if err != nil {
		t.Fatal(err)
	}
	refresh(p, "a")
	refresh(p, "b")
	filename := p.store.(*fileStore).path(deviceactor.ActorID(identity("a")))
	p.Close()
	delete(inventory, "b")
	p, err = Open(o, inventory, inv)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.rows) != 1 {
		t.Fatal("removed inventory still projected")
	}
	p.Close()
	if err = os.WriteFile(filename, []byte(`{"Schema":99}`), 0600); err != nil {
		t.Fatal(err)
	}
	if corrupt, err := Open(o, inventory, inv); err == nil {
		corrupt.Close()
		t.Fatal("accepted corrupt row")
	}
	files, err := filepath.Glob(filepath.Join(o.Directory, "*.json"))
	if err != nil || len(files) != 3 {
		t.Fatal("silently deleted cache files", files, err)
	}
}
