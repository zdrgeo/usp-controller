package deviceactor

import (
	"context"
	"fmt"
)

// CompletionReserve is deployment policy, not a caller-selected priority. Zero
// values preserve the original hard-limit behavior. Hard limits always apply.
type CompletionReserve struct {
	StateBytes    int `json:"state_bytes"`
	Notifications int `json:"notifications"`
}

func (r CompletionReserve) Validate(maxBytes, maxCommands, maxReceipts int) error {
	if r.StateBytes < 0 || r.StateBytes >= maxBytes || r.Notifications < 0 || r.Notifications >= maxCommands || r.Notifications >= maxReceipts {
		return fmt.Errorf("completion reserve must leave positive ordinary byte, event and inbox capacity")
	}
	return nil
}
func (a *Actor) saveOrdinary(ctx context.Context, s *snapshot) error {
	return a.saveWithBudget(ctx, s, a.opts.MaxBytes-a.opts.CompletionReserve.StateBytes)
}
func (a *Actor) saveNotification(ctx context.Context, s *snapshot, priority bool) error {
	reserve := a.opts.CompletionReserve
	if !priority && (len(s.Inbox) > a.opts.MaxReceipts-reserve.Notifications || len(s.Outbox) > a.opts.MaxCommands-reserve.Notifications) {
		return ErrCapacity
	}
	if err := a.backend.EnsureReminder(ctx); err != nil {
		return err
	}
	if priority {
		return a.save(ctx, s)
	}
	return a.saveOrdinary(ctx, s)
}
