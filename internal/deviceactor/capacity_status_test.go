package deviceactor

import (
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
)

func TestCapacityStatusSeparatesOrdinaryAndHardBudgetsWithoutMutation(t *testing.T) {
	a, b, f, now := executable(t)
	a.opts.MaxCommands = 4
	a.opts.MaxReceipts = 5
	a.opts.CompletionReserve = CompletionReserve{StateBytes: 128, Notifications: 2}
	if _, err := a.EnqueueCommand(ctx, command("retained", now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if _, err := a.CancelCommand(ctx, device.CancelCommandRequest{ID: "retained", ExpectedRevision: 1}); err != nil {
		t.Fatal(err)
	}
	for _, mid := range []string{"one", "two"} {
		if _, err := a.HandleNotify(ctx, valueNotification(mid, *now)); err != nil {
			t.Fatal(err)
		}
	}
	b.failSave = true
	b.failReminder = true
	before := string(b.state)
	saves := b.saves
	status, err := a.GetStatus(ctx)
	if err != nil || status.Capacity == nil {
		t.Fatal(status, err)
	}
	c := status.Capacity
	if c.CommandSlotsRemaining != 3 || c.MutationReceiptSlotsRemaining != 4 || c.EventSlotsRemaining != 2 || c.OrdinaryEventSlotsRemaining != 0 || c.NotificationReceiptSlotsRemaining != 3 || c.OrdinaryNotificationReceiptSlotsRemaining != 1 || c.HardStateBytesRemaining-c.OrdinaryStateBytesRemaining != 128 {
		t.Fatal("incorrect independent headroom", c)
	}
	if b.saves != saves || string(b.state) != before || len(f.calls) != 0 {
		t.Fatal("capacity read changed state")
	}
	b.failSave = false
	b.failReminder = false
	r := command("operation", now.Add(time.Hour))
	r.Request = &usp.Request{ReqType: &usp.Request_Operate{Operate: &usp.Operate{Command: "Device.Reboot()"}}}
	if _, err = a.EnqueueCommand(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err = a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	op, err := a.GetCommand(ctx, "operation")
	if err != nil {
		t.Fatal(err)
	}
	complete := &usp.Notify{Notification: &usp.Notify_OperComplete{OperComplete: &usp.Notify_OperationComplete{ObjPath: "Device.", CommandName: "Reboot()", CommandKey: op.OperationKey, OperationResp: &usp.Notify_OperationComplete_ReqOutputArgs{ReqOutputArgs: &usp.Notify_OperationComplete_OutputArgs{}}}}}
	if _, err = a.HandleNotify(ctx, notification("completed", complete, *now)); err != nil {
		t.Fatal(err)
	}
	status, err = a.GetStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	a.opts.MaxBytes = status.StateBytes + 32 // Within hard limit but inside soft reserve.
	b.failSave = true
	b.failReminder = true
	saves = b.saves
	before = string(b.state)
	status, err = a.GetStatus(ctx)
	if err != nil || status.Capacity == nil {
		t.Fatal(status, err)
	}
	c = status.Capacity
	if c.HardStateBytesRemaining != 32 || c.OrdinaryStateBytesRemaining != 0 || c.OrdinaryStateBytesLimit != a.opts.MaxBytes-128 || c.EventSlotsRemaining != 1 || c.OrdinaryEventSlotsRemaining != 0 || c.NotificationReceiptSlotsRemaining != 2 || c.OrdinaryNotificationReceiptSlotsRemaining != 0 || c.CommandSlotsRemaining != 2 || c.MutationReceiptSlotsRemaining != 3 {
		t.Fatal("reserve/retained evidence headroom incorrect", c)
	}
	if b.saves != saves || string(b.state) != before || len(f.calls) != 1 {
		t.Fatal("capacity read performed work")
	}
}
