package deviceactor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

type heldPublisher struct {
	entered, release chan struct{}
	once             sync.Once
	calls            atomic.Int64
}

func (p *heldPublisher) Publish(ctx context.Context, _ device.DeviceIdentity, _ device.PublishAttempt) (device.PublishResult, error) {
	p.calls.Add(1)
	p.once.Do(func() { close(p.entered) })
	select {
	case <-p.release:
		return device.PublishResult{BrokerAccepted: true}, nil
	case <-ctx.Done():
		return device.PublishResult{}, ctx.Err()
	}
}

func TestSharedBudgetKeepsOtherActorsBeforeAttemptBoundary(t *testing.T) {
	budget, err := device.NewAttemptBudget(1)
	if err != nil {
		t.Fatal(err)
	}
	makeActor := func(name string, transport device.DeviceTransport) (*Actor, *memoryBackend) {
		t.Helper()
		id := identity
		id.AgentID = name
		b := &memoryBackend{}
		opts := DefaultOptions()
		opts.AttemptBudget = budget
		a, err := NewExecuting(id, b, opts, transport)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := a.EnqueueCommand(ctx, command("queued", time.Now().Add(time.Hour))); err != nil {
			t.Fatal(err)
		}
		return a, b
	}
	held := &heldPublisher{entered: make(chan struct{}), release: make(chan struct{})}
	first, _ := makeActor("first", held)
	sender := &fakeSender{ready: true}
	second, backend := makeActor("second", sender)
	done := make(chan struct{})
	var firstErr error
	var release sync.Once
	go func() { firstErr = first.Tick(ctx); close(done) }()
	t.Cleanup(func() { release.Do(func() { close(held.release) }); <-done })
	select {
	case <-held.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first actor did not start publication")
	}
	saves := backend.saves
	if err := second.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	queued, err := second.GetCommand(ctx, "queued")
	if err != nil || queued.State != device.CommandQueued || queued.MessageID != "" || backend.saves != saves || len(sender.calls) != 0 || budget.InUse() != 1 || budget.Denied() != 1 {
		t.Fatal("denied turn crossed attempt boundary", queued, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := second.Tick(cancelled); !errors.Is(err, context.Canceled) || backend.saves != saves || budget.Denied() != 1 {
		t.Fatal("cancelled turn acquired or persisted an attempt", err)
	}
	release.Do(func() { close(held.release) })
	<-done
	if firstErr != nil || budget.InUse() != 0 || held.calls.Load() != 1 {
		t.Fatal("first permit leaked or send retried", firstErr)
	}
	if err := second.Tick(ctx); err != nil || len(sender.calls) != 1 || budget.InUse() != 0 {
		t.Fatal("queued work did not progress after release", err)
	}
	if err := first.Tick(ctx); err != nil || held.calls.Load() != 1 {
		t.Fatal("awaiting attempt was replayed", err)
	}
}

func TestAttemptPermitReleasedAcrossUncertainFailures(t *testing.T) {
	for _, mode := range []string{"begin-save", "result-save", "publisher-panic"} {
		t.Run(mode, func(t *testing.T) {
			a, b, f, now := executable(t)
			budget, err := device.NewAttemptBudget(1)
			if err != nil {
				t.Fatal(err)
			}
			a.opts.AttemptBudget = budget
			if _, err := a.EnqueueCommand(ctx, command("one", now.Add(time.Hour))); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "begin-save":
				b.failSave = true
			case "result-save":
				f.afterSend = func() { b.failSave = true }
			case "publisher-panic":
				f.afterSend = func() { panic("interrupted publisher") }
			}
			var recovered any
			func() { defer func() { recovered = recover() }(); _ = a.Tick(ctx) }()
			if (mode == "publisher-panic") != (recovered != nil) || budget.InUse() != 0 {
				t.Fatal("uncertain failure leaked a host-wide permit", recovered)
			}
			before := len(f.calls)
			b.failSave = false
			f.afterSend = nil
			if err := a.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			if mode == "begin-save" {
				if len(f.calls) != 1 {
					t.Fatal("known unsent command did not progress")
				}
			} else if len(f.calls) != before {
				t.Fatal("uncertain attempt was replayed")
			}
			if budget.InUse() != 0 {
				t.Fatal("recovery leaked permit")
			}
		})
	}
}
