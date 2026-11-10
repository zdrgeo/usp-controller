package deviceactor

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

func TestExecutionControlDurabilityAndStaleRetries(t *testing.T) {
	for _, mode := range []string{"committed", "failed", "reply-lost"} {
		t.Run(mode, func(t *testing.T) {
			a, b, f, now := executable(t)
			if _, err := a.EnqueueCommand(ctx, command("queued", now.Add(time.Hour))); err != nil {
				t.Fatal(err)
			}
			// Existing schema-4 queues upgrade without enabling a second attempt.
			var prior snapshot
			json.Unmarshal(b.state, &prior)
			prior.Schema = 4
			prior.Execution = nil
			b.state, _ = json.Marshal(prior)
			pause := device.SetExecutionRequest{RequestID: "pause", Paused: true, Reason: "maintenance"}
			b.failSave = mode == "failed"
			b.ambiguous = mode == "reply-lost"
			b.failReminder = true
			_, err := a.SetExecution(ctx, pause)
			if (mode == "committed") != (err == nil) {
				t.Fatal(mode, err)
			}
			b.failSave = false
			state, err := a.GetExecution(ctx)
			if err != nil || state.Paused != (mode != "failed") {
				t.Fatal("reported uncommitted pause", state, err)
			}
			// Pause and its identical retry do not depend on reminder availability.
			if _, err := a.SetExecution(ctx, pause); err != nil {
				t.Fatal(err)
			}
			b.failReminder = false
			sink := &eventSink{}
			a, err = NewWithEvents(identity, b, a.opts, f, sink)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := a.HandleNotify(ctx, valueNotification("observed", *now)); err != nil {
				t.Fatal(err)
			}
			if err := a.Tick(ctx); err != nil || len(f.calls) != 0 || len(sink.events) != 1 {
				t.Fatal("pause blocked ingress/export or sent a command", err)
			}
			twin, err := a.GetTwin(ctx)
			if err != nil || !twin.Execution.Paused || twin.Reported["Device.Test.Value"].Value.Value != "observed" {
				t.Fatal(twin, err)
			}
			resume := device.SetExecutionRequest{RequestID: "resume", ExpectedRevision: 1, Reason: "maintenance complete"}
			b.failReminder = true
			if _, err := a.SetExecution(ctx, resume); err == nil {
				t.Fatal("resumed without reminder repair")
			}
			state, _ = a.GetExecution(ctx)
			if !state.Paused {
				t.Fatal("failed resume persisted")
			}
			b.failReminder = false
			b.ambiguous = true
			if _, err := a.SetExecution(ctx, resume); err == nil {
				t.Fatal("lost commit reply acknowledged")
			}
			nextPause := device.SetExecutionRequest{RequestID: "pause-again", ExpectedRevision: 2, Paused: true, Reason: "new maintenance"}
			if _, err := a.SetExecution(ctx, nextPause); err != nil {
				t.Fatal(err)
			}
			saves := b.saves
			receipt, err := a.SetExecution(ctx, resume)
			state, _ = a.GetExecution(ctx)
			if err != nil || receipt.Revision != 2 || receipt.Paused || !state.Paused || state.Revision != 3 || b.saves != saves {
				t.Fatal("old resume retry undid a later pause", receipt, state, err)
			}
			stale := resume
			stale.RequestID = "stale-resume"
			if _, err := a.SetExecution(ctx, stale); !errors.Is(err, ErrConflict) {
				t.Fatal("stale control revision accepted", err)
			}
			changed := resume
			changed.Reason = "changed decision"
			if _, err := a.SetExecution(ctx, changed); !errors.Is(err, ErrConflict) {
				t.Fatal("changed retry accepted", err)
			}
			// Missing control metadata and a changed pause bit must fail closed.
			original := append([]byte(nil), b.state...)
			var corrupted snapshot
			json.Unmarshal(original, &corrupted)
			corrupted.Execution.Paused = false
			b.state, _ = json.Marshal(corrupted)
			if _, err := a.GetExecution(ctx); err == nil {
				t.Fatal("pause bit not bound to decision receipt")
			}
			corrupted.Execution = nil
			b.state, _ = json.Marshal(corrupted)
			if _, err := a.GetExecution(ctx); err == nil {
				t.Fatal("missing control silently resumed execution")
			}
			b.state = original
			if _, err := a.SetExecution(ctx, device.SetExecutionRequest{RequestID: "resume-current", ExpectedRevision: 3, Reason: "review complete"}); err != nil {
				t.Fatal(err)
			}
			a, err = NewExecuting(identity, b, a.opts, f)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if err := a.Tick(ctx); err != nil {
					t.Fatal(err)
				}
			}
			if len(f.calls) != 1 {
				t.Fatal("restart lost pause/resume control or replayed an attempt")
			}
			if _, err := a.SetExecution(ctx, pause); err != nil {
				t.Fatal(err)
			}
			state, _ = a.GetExecution(ctx)
			if state.Paused || state.Revision != 4 {
				t.Fatal("old pause retry changed current state", state)
			}
		})
	}
}

func TestPausedReconciliationMaintainsSafetyChecks(t *testing.T) {
	a, _, f, now := managedActor(t)
	observe(t, a, "before", *now, "old")
	if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.EnqueueCommand(ctx, command("expires", now.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	if _, err := a.SetExecution(ctx, device.SetExecutionRequest{RequestID: "pause", Paused: true, Reason: "maintenance"}); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(6 * time.Minute)
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	expired, err := a.GetCommand(ctx, "expires")
	if err != nil || expired.State != device.CommandExpired || len(f.calls) != 0 {
		t.Fatal("paused maintenance did not expire queued work", expired, err)
	}
	if _, err := a.GetCommand(ctx, "reconcile-1"); !errors.Is(err, ErrNotFound) {
		t.Fatal("paused actor planned a new write", err)
	}
	if _, err := a.SetExecution(ctx, device.SetExecutionRequest{RequestID: "resume", ExpectedRevision: 1, Reason: "maintenance done"}); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil || len(f.calls) != 0 {
		t.Fatal("resume bypassed stale-observation guard", err)
	}
	observe(t, a, "fresh", *now, "old")
	f.err = errors.New("uncertain write")
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	f.err = nil
	if _, err := a.SetExecution(ctx, device.SetExecutionRequest{RequestID: "pause-again", ExpectedRevision: 2, Paused: true, Reason: "review unknown"}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.SetExecution(ctx, device.SetExecutionRequest{RequestID: "resume-again", ExpectedRevision: 3, Reason: "resume is not an acknowledgement"}); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil || len(f.calls) != 1 {
		t.Fatal("resume bypassed unknown-outcome barrier", err)
	}
	if _, err := a.SetExecution(ctx, device.SetExecutionRequest{RequestID: "paused-for-evidence", ExpectedRevision: 4, Paused: true, Reason: "await evidence"}); err != nil {
		t.Fatal(err)
	}
	c, _ := a.GetCommand(ctx, "reconcile-1")
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: setResponse(c.MessageID)}); err != nil {
		t.Fatal(err)
	}
	c, err = a.GetCommand(ctx, "reconcile-1")
	if err != nil || c.State != device.CommandSucceeded {
		t.Fatal("pause blocked response settlement", c, err)
	}
	status, err := a.GetStatus(ctx)
	if err != nil || !status.ExecutionPaused || status.Reconciliation != "paused" {
		t.Fatal(status, err)
	}
}
