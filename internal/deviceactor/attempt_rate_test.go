package deviceactor

import (
	"context"
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

func TestSharedRateDenialDoesNotBeginAndFailedBeginIsNotRefunded(t *testing.T) {
	first, _, sender, now := executable(t)
	rate, err := device.NewAttemptRateBudget(1, 1, func() time.Time { return *now })
	if err != nil {
		t.Fatal(err)
	}
	first.opts.AttemptRateBudget = rate
	if _, err = first.EnqueueCommand(ctx, command("first", now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err = first.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	second, b, secondSender, _ := executable(t)
	second.opts.Now = func() time.Time { return *now }
	second.opts.AttemptRateBudget = rate
	second.opts.AttemptBudget, _ = device.NewAttemptBudget(1)
	if _, err = second.EnqueueCommand(ctx, command("second", now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	saves := b.saves
	if err = second.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	c, err := second.GetCommand(ctx, "second")
	if err != nil || c.State != device.CommandQueued || c.MessageID != "" || c.AttemptSequence != 0 || b.saves != saves || len(secondSender.calls) != 0 || second.opts.AttemptBudget.InUse() != 0 {
		t.Fatal("rate denial crossed begin boundary", c, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, _, _, before := rate.Stats()
	if err = second.Tick(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	_, _, _, after := rate.Stats()
	if before != after {
		t.Fatal("cancelled turn consumed budget")
	}
	*now = now.Add(time.Second)
	b.failSave = true
	if err = second.Tick(ctx); err == nil {
		t.Fatal("failed begin hidden")
	}
	b.failSave = false
	saves = b.saves
	if err = second.Tick(ctx); err != nil || b.saves != saves || len(secondSender.calls) != 0 {
		t.Fatal("failed begin refunded token", err)
	}
	*now = now.Add(time.Second)
	if err = second.Tick(ctx); err != nil || len(secondSender.calls) != 1 || len(sender.calls) != 1 {
		t.Fatal("refilled budget did not progress", err)
	}
	if err = second.Tick(ctx); err != nil || len(secondSender.calls) != 1 {
		t.Fatal("attempt replayed", err)
	}
}
