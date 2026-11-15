package deviceactor

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

func TestPendingEventPagesAreReadOnlyValueFreeAndViewBound(t *testing.T) {
	a, b, f, now := executable(t)
	a.publisher = &eventSink{}
	for _, id := range []string{"z-SENSITIVE_ID", "a-SENSITIVE_ID", "m-SENSITIVE_ID"} {
		n := valueNotification(id, *now)
		v := n.Message.GetBody().GetRequest().GetNotify().GetValueChange()
		v.ParamPath = "Device.Sensitive.Value"
		v.ParamValue = "SENSITIVE_PAYLOAD"
		if _, err := a.HandleNotify(ctx, n); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := a.EnqueueCommand(ctx, command("expired-command", now.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(2 * time.Second)
	b.failSave = true
	b.failReminder = true
	before := string(b.state)
	saves := b.saves
	first, err := a.ListEvents(ctx, device.EventQuery{Limit: 1})
	if err != nil || first.Total != 3 || len(first.Events) != 1 || first.Events[0].Sequence != 1 || first.Events[0].Kind != "value_change" || first.Events[0].Epoch == "" || first.NextCursor == "" {
		t.Fatal(first, err)
	}
	rest, err := a.ListEvents(ctx, device.EventQuery{Limit: 2, Cursor: first.NextCursor})
	if err != nil || rest.Total != 3 || len(rest.Events) != 2 || rest.Events[0].Sequence != 2 || rest.Events[1].Sequence != 3 || rest.NextCursor != "" {
		t.Fatal(rest, err)
	}
	c, err := a.GetCommand(ctx, "expired-command")
	if err != nil || c.State != device.CommandQueued || b.saves != saves || string(b.state) != before || len(f.calls) != 0 {
		t.Fatal("event read performed maintenance", c, err)
	}
	raw, _ := json.Marshal(first)
	for _, secret := range []string{"SENSITIVE", "Sensitive", "subscription", "message", "Param"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("event metadata leaked payload", string(raw))
		}
	}
	otherID := identity
	otherID.AgentID = "other"
	other, err := NewExecuting(otherID, &memoryBackend{}, a.opts, &fakeSender{ready: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = other.ListEvents(ctx, device.EventQuery{Cursor: first.NextCursor}); !errors.Is(err, ErrInvalid) {
		t.Fatal("cross-device cursor accepted", err)
	}
	for _, query := range []device.EventQuery{{Limit: -1}, {Limit: 101}, {Cursor: "invalid!"}, {Cursor: strings.Repeat("x", MaxEventCursorBytes+1)}} {
		if _, err = a.ListEvents(ctx, query); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid event query accepted", err)
		}
	}
	b.failSave = false
	b.failReminder = false
	if _, err = a.EnqueueCommand(ctx, command("unrelated", now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if _, err = a.ListEvents(ctx, device.EventQuery{Cursor: first.NextCursor}); err != nil {
		t.Fatal("command metadata invalidated event view", err)
	}
	if _, err = a.HandleNotify(ctx, valueNotification("new-event", *now)); err != nil {
		t.Fatal(err)
	}
	if _, err = a.ListEvents(ctx, device.EventQuery{Cursor: first.NextCursor}); !errors.Is(err, ErrConflict) {
		t.Fatal("appended event did not invalidate view", err)
	}
	current, err := a.ListEvents(ctx, device.EventQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	f.ready = false
	if err = a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = a.ListEvents(ctx, device.EventQuery{Cursor: current.NextCursor}); !errors.Is(err, ErrConflict) {
		t.Fatal("exported prefix did not invalidate view", err)
	}
}
