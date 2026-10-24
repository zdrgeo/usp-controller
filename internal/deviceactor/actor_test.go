package deviceactor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
)

type memoryBackend struct {
	mu                                sync.Mutex
	state                             []byte
	reminder                          bool
	failSave, failReminder, ambiguous bool
	saves                             int
}

func (m *memoryBackend) Load(context.Context) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.state...), nil
}
func (m *memoryBackend) Save(_ context.Context, b []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failSave {
		return errors.New("save failed")
	}
	m.state = append([]byte(nil), b...)
	m.saves++
	if m.ambiguous {
		m.ambiguous = false
		return errors.New("commit response lost")
	}
	return nil
}
func (m *memoryBackend) EnsureReminder(context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failReminder {
		return errors.New("scheduler unavailable")
	}
	m.reminder = true
	return nil
}

var ctx = context.Background()
var identity = device.DeviceIdentity{Tenant: "test", ControllerID: "controller", AgentID: "device"}
var value = device.ParameterValue{Value: "target", Type: "string"}

func fixture(t *testing.T) (*Actor, *memoryBackend, *time.Time) {
	t.Helper()
	now := time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC)
	b := &memoryBackend{}
	opts := DefaultOptions()
	opts.Now = func() time.Time { return now }
	a, err := New(identity, b, opts)
	if err != nil {
		t.Fatal(err)
	}
	return a, b, &now
}
func desired(id device.RequestID, rev uint64) device.SetDesiredRequest {
	return device.SetDesiredRequest{ID: id, ExpectedRevision: rev, Values: map[string]device.ParameterValue{"Device.Test.Value": value}}
}
func command(id device.RequestID, deadline time.Time) device.CommandRequest {
	return device.CommandRequest{ID: id, Deadline: deadline, Request: &usp.Request{ReqType: &usp.Request_Get{Get: &usp.Get{ParamPaths: []string{"Device.Test."}}}}}
}

func TestDesiredReceiptSurvivesRestartAndLaterRevision(t *testing.T) {
	a, b, _ := fixture(t)
	r := desired("first", 0)
	first, err := a.SetDesired(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.SetDesired(ctx, desired("second", 1)); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(identity, b, a.opts)
	if err != nil {
		t.Fatal(err)
	}
	got, err := restarted.SetDesired(ctx, r)
	if err != nil || got != first {
		t.Fatalf("retry: %v %v", got, err)
	}
	r.Values["Device.Test.Value"] = device.ParameterValue{Value: "different"}
	if _, err = restarted.SetDesired(ctx, r); !errors.Is(err, ErrConflict) {
		t.Fatalf("changed retry: %v", err)
	}
	if _, err = restarted.SetDesired(ctx, desired("third", 0)); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale revision: %v", err)
	}
	twin, _ := restarted.GetTwin(ctx)
	twin.Desired["Device.Test.Value"] = device.ParameterValue{}
	again, _ := restarted.GetTwin(ctx)
	if again.DesiredRevision != 2 || again.Desired["Device.Test.Value"] != value {
		t.Fatal("mutation escaped")
	}
}
func TestAdmissionFailureAndAmbiguousCommit(t *testing.T) {
	for _, kind := range []string{"reminder", "save", "ambiguous"} {
		t.Run(kind, func(t *testing.T) {
			a, b, _ := fixture(t)
			r := desired("one", 0)
			switch kind {
			case "reminder":
				b.failReminder = true
			case "save":
				b.failSave = true
			case "ambiguous":
				b.ambiguous = true
			}
			if _, err := a.SetDesired(ctx, r); err == nil {
				t.Fatal("expected failure")
			}
			twin, _ := a.GetTwin(ctx)
			if kind != "ambiguous" && twin.DesiredRevision != 0 {
				t.Fatal("partial mutation")
			}
			b.failReminder = false
			b.failSave = false
			receipt, err := a.SetDesired(ctx, r)
			if err != nil || receipt.Revision != 1 {
				t.Fatalf("retry: %v %v", receipt, err)
			}
			if !b.reminder || b.saves != 1 {
				t.Fatalf("reminder=%v commits=%d", b.reminder, b.saves)
			}
		})
	}
}
func TestReportedStateFreshnessAndOutOfOrder(t *testing.T) {
	a, _, now := fixture(t)
	if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
		t.Fatal(err)
	}
	r := ReportRequest{ID: "observation", Identity: identity, ReceivedAt: *now, Values: map[string]device.ParameterValue{"Device.Test.Value": value}}
	rev, err := a.Report(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := a.Report(ctx, r); err != nil || got != rev {
		t.Fatal("duplicate report", got, err)
	}
	twin, _ := a.GetTwin(ctx)
	if twin.Convergence != "converged" {
		t.Fatal(twin.Convergence)
	}
	old := r
	old.ID = "old"
	old.ReceivedAt = now.Add(-time.Minute)
	old.Values = map[string]device.ParameterValue{"Device.Test.Value": {Value: "old"}}
	if _, err = a.Report(ctx, old); err != nil {
		t.Fatal(err)
	}
	twin, _ = a.GetTwin(ctx)
	if twin.Reported["Device.Test.Value"].Value != value {
		t.Fatal("old observation replaced new")
	}
	*now = now.Add(6 * time.Minute)
	twin, _ = a.GetTwin(ctx)
	if twin.Convergence != "unknown" {
		t.Fatal("stale reported value marked converged")
	}
	r.Identity.AgentID = "other"
	if _, err = a.Report(ctx, r); !errors.Is(err, ErrInvalid) {
		t.Fatal("accepted another device")
	}
}
func TestQueuedCommandsNeverDispatch(t *testing.T) {
	a, b, now := fixture(t)
	r := command("cmd", now.Add(time.Minute))
	first, err := a.EnqueueCommand(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = a.EnqueueCommand(ctx, r); err != nil {
		t.Fatal(err)
	}
	r.Request.GetGet().ParamPaths[0] = "Device.Changed."
	stored, err := a.GetCommand(ctx, "cmd")
	if err != nil || stored.Request.GetGet().ParamPaths[0] != "Device.Test." {
		t.Fatal("mutable request stored", err)
	}
	if _, err = a.EnqueueCommand(ctx, r); !errors.Is(err, ErrConflict) {
		t.Fatal("conflicting command accepted")
	}
	if err = a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	stored, _ = a.GetCommand(ctx, "cmd")
	if stored.State != device.CommandQueued {
		t.Fatal("dispatch enabled")
	}
	*now = now.Add(time.Minute)
	if err = a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	n := b.saves
	if err = a.Tick(ctx); err != nil || b.saves != n {
		t.Fatal("repeated tick mutated state")
	}
	stored, _ = a.GetCommand(ctx, "cmd")
	if stored.State != device.CommandExpired || stored.Revision != first.Revision+1 {
		t.Fatal(stored)
	}
	if _, err = a.HandleNotify(ctx, device.DeviceNotification{}); !errors.Is(err, ErrDisabled) {
		t.Fatal("notification accepted")
	}
	if err = a.HandleResponse(ctx, device.DeviceResponse{}); !errors.Is(err, ErrDisabled) {
		t.Fatal("response accepted")
	}
}
func TestConcurrentRevisionAndCapacity(t *testing.T) {
	a, _, _ := fixture(t)
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, id := range []device.RequestID{"a", "b"} {
		wg.Add(1)
		go func(id device.RequestID) {
			defer wg.Done()
			_, err := a.SetDesired(ctx, desired(id, 0))
			results <- err
		}(id)
	}
	wg.Wait()
	close(results)
	wins := 0
	for err := range results {
		if err == nil {
			wins++
		} else if !errors.Is(err, ErrConflict) {
			t.Fatal(err)
		}
	}
	if wins != 1 {
		t.Fatal(wins)
	}
	a.opts.MaxReceipts = 1
	if _, err := a.SetDesired(ctx, desired("next", 1)); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	twin, _ := a.GetTwin(ctx)
	if twin.DesiredRevision != 1 {
		t.Fatal("capacity failure partially committed")
	}
}
func TestReminderRepairAndCancel(t *testing.T) {
	a, b, now := fixture(t)
	r := command("cmd", now.Add(time.Hour))
	receipt, err := a.EnqueueCommand(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	b.reminder = false
	restarted, _ := New(identity, b, a.opts)
	if err = restarted.Activate(ctx); err != nil || !b.reminder {
		t.Fatal("reminder not repaired", err)
	}
	if _, err = restarted.CancelCommand(ctx, device.CancelCommandRequest{ID: "cmd", ExpectedRevision: 99}); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	c, err := restarted.CancelCommand(ctx, device.CancelCommandRequest{ID: "cmd", ExpectedRevision: receipt.Revision})
	if err != nil || c.State != device.CommandCancelled {
		t.Fatal(c, err)
	}
	again, err := restarted.EnqueueCommand(ctx, r)
	if err != nil || again.State != device.CommandCancelled {
		t.Fatal("retry requeued cancellation", err)
	}
}
func TestDesiredRemovalAndNamespace(t *testing.T) {
	a, _, now := fixture(t)
	a.SetDesired(ctx, desired("same", 0))
	if _, err := a.EnqueueCommand(ctx, command("same", now.Add(time.Hour))); !errors.Is(err, ErrConflict) {
		t.Fatal("cross-kind identity reused", err)
	}
	if _, err := a.SetDesired(ctx, device.SetDesiredRequest{ID: "remove", ExpectedRevision: 1, Remove: []string{"Device.Test.Value"}}); err != nil {
		t.Fatal(err)
	}
	twin, _ := a.GetTwin(ctx)
	if len(twin.Desired) != 0 {
		t.Fatal("remove failed")
	}
	if ActorID(device.DeviceIdentity{Tenant: "a/b", ControllerID: "c", AgentID: "d"}) == ActorID(device.DeviceIdentity{Tenant: "a", ControllerID: "b/c", AgentID: "d"}) {
		t.Fatal("ambiguous identity")
	}
}

func TestBoundsAndInvalidState(t *testing.T) {
	a, b, _ := fixture(t)
	for _, bad := range []string{`{}`, `null`, `{"Schema":2}`} {
		b.state = []byte(bad)
		if _, err := a.GetTwin(ctx); err == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
	b.state = nil
	a.opts.MaxBytes = 20
	if _, err := a.SetDesired(ctx, desired("too-large", 0)); !errors.Is(err, ErrCapacity) {
		t.Fatal(err)
	}
	if b.state != nil {
		t.Fatal("oversized state committed")
	}
}

func TestInvalidInputDoesNotAdmit(t *testing.T) {
	a, b, now := fixture(t)
	cases := []device.SetDesiredRequest{
		{ID: "empty"},
		{ID: "bad", Values: map[string]device.ParameterValue{"InternetGatewayDevice.X": value}},
		{ID: "overlap", Values: map[string]device.ParameterValue{"Device.X": value}, Remove: []string{"Device.X"}},
		{ID: "duplicates", Remove: []string{"Device.X", "Device.X"}},
	}
	for _, r := range cases {
		if _, err := a.SetDesired(ctx, r); !errors.Is(err, ErrInvalid) {
			t.Fatal(err)
		}
	}
	if _, err := a.EnqueueCommand(ctx, command("late", now.Add(-time.Second))); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if _, err := a.EnqueueCommand(ctx, command("unencodable", time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC))); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	if b.saves != 0 {
		t.Fatal("invalid input saved")
	}
}

func TestCancellationAmbiguousCommitRetry(t *testing.T) {
	a, b, now := fixture(t)
	r, err := a.EnqueueCommand(ctx, command("cmd", now.Add(time.Hour)))
	if err != nil {
		t.Fatal(err)
	}
	cancel := device.CancelCommandRequest{ID: r.ID, ExpectedRevision: r.Revision}
	b.ambiguous = true
	if _, err = a.CancelCommand(ctx, cancel); err == nil {
		t.Fatal("expected lost commit result")
	}
	c, err := a.CancelCommand(ctx, cancel)
	if err != nil || c.State != device.CommandCancelled || c.Revision != 2 {
		t.Fatal(c, err)
	}
}
