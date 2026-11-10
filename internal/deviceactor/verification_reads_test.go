package deviceactor

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
)

func verificationActor(t *testing.T) (*Actor, *memoryBackend, *fakeSender, *time.Time) {
	t.Helper()
	a, b, f, now := managedActor(t)
	a.opts.Policy.ObserveGetResponses = true
	a.opts.Policy.VerifyAfterReconcile = true
	observe(t, a, "initial", *now, "old")
	if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	write, err := a.GetCommand(ctx, "reconcile-1")
	if err != nil {
		t.Fatal(err)
	}
	if err = a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: setResponse(write.MessageID), ReceivedAt: *now}); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	return a, b, f, now
}

func TestVerificationReadObservesWithoutRetryingWrites(t *testing.T) {
	for _, mode := range []string{"matches", "mismatch", "uncertain"} {
		t.Run(mode, func(t *testing.T) {
			a, b, f, now := verificationActor(t)
			twin, _ := a.GetTwin(ctx)
			if twin.Convergence != "pending" {
				t.Fatal("Set fabricated convergence", twin)
			}
			status, err := a.GetStatus(ctx)
			if err != nil || status.Reconciliation != "verification_eligible" {
				t.Fatal(status, err)
			}
			b.failSave = true
			if err = a.Tick(ctx); err == nil || len(f.calls) != 1 {
				t.Fatal("verification published before queue commit", err)
			}
			b.failSave = false
			if mode == "uncertain" {
				f.err = errors.New("lost publication reply")
			}
			if err = a.Tick(ctx); err != nil || len(f.calls) != 2 {
				t.Fatal(err, f.calls)
			}
			c, err := a.GetCommand(ctx, "reconcile-verify-1")
			if err != nil || c.VerificationRevision != 1 || c.DesiredRevision != 0 || c.ObservationRevision != 0 || c.AttemptSequence != 2 {
				t.Fatal(c, err)
			}
			get := f.calls[1].Message.GetBody().GetRequest().GetGet()
			if get == nil || len(get.ParamPaths) != 1 || get.ParamPaths[0] != "Device.Test.Value" {
				t.Fatal(get)
			}
			page, err := a.ListCommands(ctx, device.CommandQuery{})
			if err != nil || len(page.Commands) != 2 || page.Commands[1].VerificationRevision != 1 {
				t.Fatal(page, err)
			}
			*now = now.Add(time.Second)
			if mode != "uncertain" {
				value := "target"
				if mode == "mismatch" {
					value = "still-old"
				}
				msg := getObservationResponse(c.MessageID, getPathResult("Device.Test.Value", "Device.Test.", map[string]string{"Value": value}))
				if err = a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: msg, ReceivedAt: *now}); err != nil {
					t.Fatal(err)
				}
			} else {
				if c.State != device.CommandUnknown {
					t.Fatal(c)
				}
				if _, err = a.AcknowledgeUnknown(ctx, device.AcknowledgeUnknownRequest{ID: c.ID, RequestID: "review", ExpectedRevision: c.Revision, Reason: "independent inspection"}); err != nil {
					t.Fatal(err)
				}
			}
			restarted, err := NewExecuting(identity, b, a.opts, f)
			if err != nil {
				t.Fatal(err)
			}
			if err = restarted.Tick(ctx); err != nil || len(f.calls) != 2 {
				t.Fatal("verification or write replayed", err)
			}
			twin, _ = restarted.GetTwin(ctx)
			if mode == "matches" && twin.Convergence != "converged" || mode != "matches" && twin.Convergence != "pending" {
				t.Fatal(twin)
			}
			*now = now.Add(10 * time.Minute)
			if err = restarted.Tick(ctx); err != nil || len(f.calls) != 2 {
				t.Fatal("verification became polling", err)
			}
		})
	}
}

func TestQueuedVerificationRechecksAndProtectsProvenance(t *testing.T) {
	for _, mode := range []string{"disabled", "revision", "paused", "observed", "downgrade"} {
		t.Run(mode, func(t *testing.T) {
			a, b, f, now := verificationActor(t)
			s, err := a.load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = a.scheduleReconciliation(ctx, s); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "disabled":
				a.opts.Policy.VerifyAfterReconcile = false
			case "revision":
				if _, err = a.SetDesired(ctx, device.SetDesiredRequest{ID: "remove", ExpectedRevision: 1, Remove: []string{"Device.Test.Value"}}); err != nil {
					t.Fatal(err)
				}
			case "paused":
				if _, err = a.SetExecution(ctx, device.SetExecutionRequest{RequestID: "pause", ExpectedRevision: 0, Paused: true, Reason: "maintenance"}); err != nil {
					t.Fatal(err)
				}
			case "observed":
				observe(t, a, "verified", *now, "target")
			case "downgrade":
				var prior snapshot
				if err = json.Unmarshal(b.state, &prior); err != nil {
					t.Fatal(err)
				}
				prior.Schema = 7
				b.state, _ = json.Marshal(prior)
				if _, err = a.GetTwin(ctx); err == nil {
					t.Fatal("old schema accepted verification provenance")
				}
				return
			}
			if err = a.Tick(ctx); err != nil || len(f.calls) != 1 {
				t.Fatal("obsolete verification dispatched", err)
			}
			c, err := a.GetCommand(ctx, "reconcile-verify-1")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "paused" && c.State != device.CommandQueued || mode != "paused" && c.State != device.CommandCancelled {
				t.Fatal(c)
			}
		})
	}
}

func TestVerificationRequiresSuccessfulManagedWrite(t *testing.T) {
	for _, mode := range []string{"pending", "failed", "unknown"} {
		t.Run(mode, func(t *testing.T) {
			a, _, f, now := managedActor(t)
			a.opts.Policy.ObserveGetResponses = true
			a.opts.Policy.VerifyAfterReconcile = true
			observe(t, a, "initial", *now, "old")
			if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
				t.Fatal(err)
			}
			if mode == "unknown" {
				f.err = errors.New("publication uncertain")
			}
			if err := a.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			c, err := a.GetCommand(ctx, "reconcile-1")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "failed" {
				msg := &usp.Msg{Header: &usp.Header{MsgId: c.MessageID, MsgType: usp.Header_ERROR}, Body: &usp.Body{MsgBody: &usp.Body_Error{Error: &usp.Error{ErrCode: 7000, ErrMsg: "failed"}}}}
				if err = a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: msg, ReceivedAt: *now}); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "unknown" {
				if _, err = a.AcknowledgeUnknown(ctx, device.AcknowledgeUnknownRequest{ID: c.ID, RequestID: "review", ExpectedRevision: c.Revision, Reason: "independent inspection"}); err != nil {
					t.Fatal(err)
				}
			}
			if err = a.Tick(ctx); err != nil || len(f.calls) != 1 {
				t.Fatal("verification without successful write", err)
			}
			if _, err = a.GetCommand(ctx, "reconcile-verify-1"); !errors.Is(err, ErrNotFound) {
				t.Fatal("verification intent created", err)
			}
		})
	}
}
