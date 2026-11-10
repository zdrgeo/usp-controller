package deviceactor

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

func TestCommandPagesAreReadOnlyAndRejectChangedViews(t *testing.T) {
	a, b, f, now := executable(t)
	f.ready = false
	for _, id := range []device.RequestID{"z", "a", "m"} {
		deadline := now.Add(time.Hour)
		if id == "z" {
			deadline = now.Add(time.Minute)
		}
		request := command(id, deadline)
		request.Request.GetGet().ParamPaths = []string{"Device.SecretParameter"}
		if _, err := a.EnqueueCommand(ctx, request); err != nil {
			t.Fatal(err)
		}
	}
	*now = now.Add(2 * time.Minute)
	b.failReminder = true
	saves := b.saves
	first, err := a.ListCommands(ctx, device.CommandQuery{Limit: 1})
	if err != nil || first.Total != 3 || len(first.Commands) != 1 || first.Commands[0].RequestID != "a" || first.Commands[0].QueuePosition != 2 || first.NextCursor == "" {
		t.Fatal(first, err)
	}
	rest, err := a.ListCommands(ctx, device.CommandQuery{Limit: 2, Cursor: first.NextCursor})
	if err != nil || len(rest.Commands) != 2 || rest.Commands[0].RequestID != "m" || rest.Commands[1].RequestID != "z" || rest.Commands[1].State != device.CommandQueued || rest.Commands[1].QueuePosition != 1 || rest.NextCursor != "" {
		t.Fatal(rest, err)
	}
	if b.saves != saves || len(f.calls) != 0 {
		t.Fatal("listing mutated, expired or dispatched work")
	}
	body, _ := json.Marshal(rest)
	if strings.Contains(string(body), "SecretParameter") {
		t.Fatal("request payload leaked into listing")
	}
	other := identity
	other.AgentID = "other"
	foreign, err := New(other, &memoryBackend{}, a.opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := foreign.ListCommands(ctx, device.CommandQuery{Cursor: first.NextCursor}); !errors.Is(err, ErrInvalid) {
		t.Fatal("cross-device cursor accepted", err)
	}
	if _, err := a.ListCommands(ctx, device.CommandQuery{Cursor: first.NextCursor, State: device.CommandUnknown}); !errors.Is(err, ErrInvalid) {
		t.Fatal("changed-filter cursor accepted", err)
	}
	if _, err := a.CancelCommand(ctx, device.CancelCommandRequest{ID: "m", ExpectedRevision: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ListCommands(ctx, device.CommandQuery{Cursor: first.NextCursor}); !errors.Is(err, ErrConflict) {
		t.Fatal("changed command view silently skipped work", err)
	}
	queued, err := a.ListCommands(ctx, device.CommandQuery{State: device.CommandQueued, Limit: 1})
	if err != nil || queued.Total != 2 || queued.NextCursor == "" {
		t.Fatal(queued, err)
	}
	// Twin-only telemetry does not invalidate an unchanged command view.
	b.failReminder = false
	if _, err := a.Report(ctx, ReportRequest{ID: "telemetry", Identity: identity, ReceivedAt: *now, Values: map[string]device.ParameterValue{"Device.Test.Value": value}}); err != nil {
		t.Fatal(err)
	}
	if next, err := a.ListCommands(ctx, device.CommandQuery{State: device.CommandQueued, Cursor: queued.NextCursor}); err != nil || len(next.Commands) != 1 || next.Commands[0].RequestID != "z" {
		t.Fatal("telemetry invalidated command cursor", next, err)
	}
	if _, err := a.SetExecution(ctx, device.SetExecutionRequest{RequestID: "pause", Paused: true, Reason: "SecretOperatorReason"}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.ListCommands(ctx, device.CommandQuery{State: device.CommandQueued, Cursor: queued.NextCursor}); !errors.Is(err, ErrConflict) {
		t.Fatal("execution decision not bound to cursor", err)
	}
	page, err := a.ListCommands(ctx, device.CommandQuery{State: device.CommandQueued})
	if err != nil || !page.ExecutionPaused || page.ExecutionRevision != 1 {
		t.Fatal(page, err)
	}
	body, _ = json.Marshal(page)
	if strings.Contains(string(body), "SecretOperatorReason") || strings.Contains(string(body), "SecretParameter") {
		t.Fatal("private control/request detail leaked")
	}
}
