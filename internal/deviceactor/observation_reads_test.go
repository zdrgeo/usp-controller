package deviceactor

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

func TestObservationReadBootstrapsWriteWithoutInventingConvergence(t *testing.T) {
	a, b, f, now := managedActor(t)
	a.opts.Policy.ObserveGetResponses = true
	a.opts.Policy.ReadBeforeReconcile = true
	if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
		t.Fatal(err)
	}
	b.failSave = true
	if err := a.Tick(ctx); err == nil || len(f.calls) != 0 {
		t.Fatal("read sent before durable queue", err)
	}
	b.failSave = false
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 1 {
		t.Fatal(f.calls)
	}
	get := f.calls[0].Message.GetBody().GetRequest().GetGet()
	if get == nil || len(get.ParamPaths) != 1 || get.ParamPaths[0] != "Device.Test.Value" {
		t.Fatal("unbounded read", get)
	}
	c, err := a.GetCommand(ctx, "reconcile-observe-1")
	if err != nil || c.ObservationRevision != 1 || c.DesiredRevision != 0 {
		t.Fatal(c, err)
	}
	page, err := a.ListCommands(ctx, device.CommandQuery{})
	if err != nil || len(page.Commands) != 1 || page.Commands[0].ObservationRevision != 1 {
		t.Fatal(page, err)
	}
	*now = now.Add(time.Second)
	response := getObservationResponse(c.MessageID, getPathResult("Device.Test.Value", "Device.Test.", map[string]string{"Value": "old"}))
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: response, ReceivedAt: *now}); err != nil {
		t.Fatal(err)
	}
	restarted, err := NewExecuting(identity, b, a.opts, f)
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.Tick(ctx); err != nil || len(f.calls) != 2 || f.calls[1].Message.GetBody().GetRequest().GetSet() == nil {
		t.Fatal("fresh read did not authorize managed write", err, f.calls)
	}
	write, err := restarted.GetCommand(ctx, "reconcile-1")
	if err != nil {
		t.Fatal(err)
	}
	if err = restarted.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: setResponse(write.MessageID), ReceivedAt: *now}); err != nil {
		t.Fatal(err)
	}
	twin, _ := restarted.GetTwin(ctx)
	if twin.Convergence != "unknown" {
		t.Fatal("Set response fabricated verification", twin)
	}
	*now = now.Add(10 * time.Minute)
	if err = restarted.Tick(ctx); err != nil || len(f.calls) != 2 {
		t.Fatal("bootstrap became polling or write retry", err)
	}
}

func TestObservationReadFailureIsNotReplayedAndProvenanceIsGuarded(t *testing.T) {
	a, b, f, _ := managedActor(t)
	a.opts.Policy.ObserveGetResponses = true
	a.opts.Policy.ReadBeforeReconcile = true
	if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
		t.Fatal(err)
	}
	f.err = errors.New("uncertain publication")
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	c, err := a.GetCommand(ctx, "reconcile-observe-1")
	if err != nil || c.State != device.CommandUnknown {
		t.Fatal(c, err)
	}
	f.err = nil
	if err = a.Tick(ctx); err != nil || len(f.calls) != 1 {
		t.Fatal("read retried", err)
	}
	if _, err = a.AcknowledgeUnknown(ctx, device.AcknowledgeUnknownRequest{ID: c.ID, RequestID: "reviewed", ExpectedRevision: c.Revision, Reason: "independently checked"}); err != nil {
		t.Fatal(err)
	}
	if err = a.Tick(ctx); err != nil || len(f.calls) != 1 {
		t.Fatal("acknowledgement retried bootstrap read", err)
	}
	status, err := a.GetStatus(ctx)
	if err != nil || status.Reconciliation != "observation_recorded" {
		t.Fatal(status, err)
	}
	original := append([]byte(nil), b.state...)
	var s snapshot
	if err = json.Unmarshal(original, &s); err != nil {
		t.Fatal(err)
	}
	s.Schema = 6
	b.state, _ = json.Marshal(s)
	if _, err = a.GetTwin(ctx); err == nil {
		t.Fatal("old schema accepted observation provenance")
	}
	b.state = original
}

func TestQueuedObservationReadRechecksPolicyRevisionAndPause(t *testing.T) {
	for _, mode := range []string{"disabled", "revision", "paused", "already-observed"} {
		t.Run(mode, func(t *testing.T) {
			a, _, f, now := managedActor(t)
			a.opts.Policy.ObserveGetResponses = true
			a.opts.Policy.ReadBeforeReconcile = true
			if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
				t.Fatal(err)
			}
			s, err := a.load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = a.scheduleReconciliation(ctx, s); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "disabled":
				a.opts.Policy.ReadBeforeReconcile = false
			case "revision":
				if _, err = a.SetDesired(ctx, device.SetDesiredRequest{ID: "remove", ExpectedRevision: 1, Remove: []string{"Device.Test.Value"}}); err != nil {
					t.Fatal(err)
				}
			case "paused":
				// Existing control tests cover receipt integrity; use its public helper.
				if _, err = a.SetExecution(ctx, device.SetExecutionRequest{RequestID: "pause", ExpectedRevision: 0, Paused: true, Reason: "maintenance"}); err != nil {
					t.Fatal(err)
				}
			case "already-observed":
				observe(t, a, "seen", *now, "target")
			}
			if err = a.Tick(ctx); err != nil || len(f.calls) != 0 {
				t.Fatal("ineligible read sent", err, f.calls)
			}
			c, err := a.GetCommand(ctx, "reconcile-observe-1")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "paused" && c.State != device.CommandQueued || mode != "paused" && c.State != device.CommandCancelled {
				t.Fatal(c)
			}
		})
	}
}
