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

// CapacityStatus is remaining space in an already-loaded snapshot, not an
// admission promise: new payloads, receipts and evidence still consume capacity.
type CapacityStatus struct {
	OrdinaryStateBytesLimit                   int `json:"ordinary_state_bytes_limit"`
	OrdinaryStateBytesRemaining               int `json:"ordinary_state_bytes_remaining"`
	HardStateBytesRemaining                   int `json:"hard_state_bytes_remaining"`
	CommandSlotsRemaining                     int `json:"command_slots_remaining"`
	MutationReceiptSlotsRemaining             int `json:"mutation_receipt_slots_remaining"`
	EventSlotsRemaining                       int `json:"event_slots_remaining"`
	OrdinaryEventSlotsRemaining               int `json:"ordinary_event_slots_remaining"`
	NotificationReceiptSlotsRemaining         int `json:"notification_receipt_slots_remaining"`
	OrdinaryNotificationReceiptSlotsRemaining int `json:"ordinary_notification_receipt_slots_remaining"`
}

// RemainingCapacity derives headroom from validated status counters. All retained
// command states count; ordinary notification budgets exclude completion reserve.
func (s Status) RemainingCapacity() CapacityStatus {
	commands := 0
	for _, n := range s.CommandStates {
		commands += n
	}
	ordinaryBytes := max(0, s.MaxStateBytes-s.ReservedCompletionBytes)
	return CapacityStatus{
		OrdinaryStateBytesLimit:                   ordinaryBytes,
		OrdinaryStateBytesRemaining:               max(0, ordinaryBytes-s.StateBytes),
		HardStateBytesRemaining:                   max(0, s.MaxStateBytes-s.StateBytes),
		CommandSlotsRemaining:                     max(0, s.MaxCommands-commands),
		MutationReceiptSlotsRemaining:             max(0, s.MaxReceipts-s.MutationReceipts),
		EventSlotsRemaining:                       max(0, s.MaxCommands-s.PendingEvents),
		OrdinaryEventSlotsRemaining:               max(0, s.MaxCommands-s.ReservedCompletionNotifications-s.PendingEvents),
		NotificationReceiptSlotsRemaining:         max(0, s.MaxReceipts-s.NotificationReceipts),
		OrdinaryNotificationReceiptSlotsRemaining: max(0, s.MaxReceipts-s.ReservedCompletionNotifications-s.NotificationReceipts),
	}
}
