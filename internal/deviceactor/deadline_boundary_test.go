package deviceactor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

type delayedBeginBackend struct {
	*memoryBackend
	afterBegin func()
}

func (b delayedBeginBackend) Save(ctx context.Context, data []byte) error {
	if err := b.memoryBackend.Save(ctx, data); err != nil {
		return err
	}
	var s snapshot
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s.Commands["command"].Receipt.State == device.CommandAttempting {
		b.afterBegin()
	}
	return nil
}

func TestDeadlineDuringConfirmedBeginSkipsHandoffAndNeverReplays(t *testing.T) {
	for _, offset := range []time.Duration{0, time.Second} {
		t.Run(offset.String(), func(t *testing.T) {
			a, b, f, now := executable(t)
			deadline := now.Add(time.Minute)
			if _, err := a.EnqueueCommand(ctx, command("command", deadline)); err != nil {
				t.Fatal(err)
			}
			a.backend = delayedBeginBackend{memoryBackend: b, afterBegin: func() { *now = deadline.Add(offset) }}
			if err := a.Tick(ctx); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("elapsed deadline did not stop handoff", err)
			}
			begun, err := a.GetCommand(ctx, "command")
			if err != nil || begun.State != device.CommandAttempting || begun.AttemptSequence != 1 || begun.MessageID == "" || len(f.calls) != 0 {
				t.Fatal("durable begin was lost or handed off", begun, err)
			}
			restarted, err := NewExecuting(identity, b, a.opts, f)
			if err != nil {
				t.Fatal(err)
			}
			// Clock recovery must not make a previously begun attempt eligible again.
			*now = deadline.Add(-time.Second)
			for i := 0; i < 2; i++ {
				if err := restarted.Tick(ctx); err != nil {
					t.Fatal(err)
				}
			}
			recovered, err := restarted.GetCommand(ctx, "command")
			if err != nil || recovered.State != device.CommandUnknown || recovered.MessageID != begun.MessageID || recovered.AttemptSequence != 1 || len(f.calls) != 0 {
				t.Fatal("elapsed durable attempt replayed", recovered, err)
			}
		})
	}
}
