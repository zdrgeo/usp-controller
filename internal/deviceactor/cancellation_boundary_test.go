package deviceactor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

type cancellingReadyTransport struct {
	*fakeSender
	cancel context.CancelFunc
}

func (t cancellingReadyTransport) Ready() bool { t.cancel(); return true }

type cancellingBeginBackend struct {
	*memoryBackend
	cancel context.CancelFunc
}

func (b cancellingBeginBackend) Save(ctx context.Context, data []byte) error {
	if err := b.memoryBackend.Save(ctx, data); err != nil {
		return err
	}
	var state snapshot
	if err := json.Unmarshal(data, &state); err != nil {
		return err
	}
	if state.Commands["command"].Receipt.State == device.CommandAttempting {
		b.cancel()
	}
	return nil
}

func TestCancellationBeforeBeginLeavesIntentQueuedWithoutOptionalBudgets(t *testing.T) {
	for _, stage := range []string{"before-turn", "after-readiness", "after-export"} {
		t.Run(stage, func(t *testing.T) {
			a, b, f, now := executable(t)
			if _, err := a.EnqueueCommand(ctx, command("command", now.Add(time.Hour))); err != nil {
				t.Fatal(err)
			}
			turn, cancel := context.WithCancel(ctx)
			defer cancel()
			extraSaves := 0
			switch stage {
			case "before-turn":
				cancel()
			case "after-readiness":
				a.transport = cancellingReadyTransport{fakeSender: f, cancel: cancel}
			case "after-export":
				if _, err := a.HandleNotify(ctx, valueNotification("event", *now)); err != nil {
					t.Fatal(err)
				}
				a.publisher = &eventSink{after: cancel}
				extraSaves = 1 // Confirmed event removal remains valid.
			}
			saves := b.saves
			if err := a.Tick(turn); !errors.Is(err, context.Canceled) {
				t.Fatal("cancelled turn did not stop", err)
			}
			c, err := a.GetCommand(ctx, "command")
			if err != nil || c.State != device.CommandQueued || c.MessageID != "" || c.AttemptSequence != 0 || !c.AttemptedAt.IsZero() || len(f.calls) != 0 || b.saves != saves+extraSaves {
				t.Fatal("cancelled work crossed durable begin", c, err, b.saves-saves)
			}
		})
	}
}

func TestCancellationAfterConfirmedBeginDoesNotHandoffOrReplay(t *testing.T) {
	a, b, f, now := executable(t)
	if _, err := a.EnqueueCommand(ctx, command("command", now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	turn, cancel := context.WithCancel(ctx)
	defer cancel()
	a.backend = cancellingBeginBackend{memoryBackend: b, cancel: cancel}
	if err := a.Tick(turn); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	c, err := a.GetCommand(ctx, "command")
	if err != nil || c.State != device.CommandAttempting || c.MessageID == "" || c.AttemptSequence != 1 || len(f.calls) != 0 {
		t.Fatal("confirmed begin lost or handed off", c, err)
	}
	restarted, err := NewExecuting(identity, b, a.opts, f)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = restarted.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	recovered, err := restarted.GetCommand(ctx, "command")
	if err != nil || recovered.State != device.CommandUnknown || recovered.MessageID != c.MessageID || recovered.AttemptSequence != 1 || len(f.calls) != 0 {
		t.Fatal("cancelled durable attempt replayed", recovered, err)
	}
}
