package deviceactor

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
)

func mutationRequest(id device.RequestID, deadline time.Time) device.CommandRequest {
	return device.CommandRequest{ID: id, Deadline: deadline, Request: &usp.Request{ReqType: &usp.Request_Set{Set: &usp.Set{UpdateObjs: []*usp.Set_UpdateObject{{ObjPath: "Device.Other.", ParamSettings: []*usp.Set_UpdateParamSetting{{Param: "Value", Value: "new", Required: true}}}}}}}}
}

func TestMutationBarrierFollowsDurableBeginNotOutcome(t *testing.T) {
	for _, mode := range []string{"normal", "begin-failed", "begin-ambiguous", "publish-uncertain", "clock-rollback"} {
		t.Run(mode, func(t *testing.T) {
			a, b, f, now := managedActor(t)
			observe(t, a, "initial", *now, "target")
			if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(time.Second)
			if _, err := a.EnqueueCommand(ctx, mutationRequest("explicit", now.Add(time.Hour))); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "begin-failed":
				b.failSave = true
			case "begin-ambiguous":
				b.ambiguous = true
			case "clock-rollback":
				f.afterSend = func() { *now = now.Add(-2 * time.Second) }
			case "publish-uncertain":
				f.err = errors.New("publication uncertain")
			}
			err := a.Tick(ctx)
			if (mode == "begin-failed" || mode == "begin-ambiguous") != (err != nil) {
				t.Fatal(err)
			}
			b.failSave = false
			restarted, err := NewExecuting(identity, b, a.opts, f)
			if err != nil {
				t.Fatal(err)
			}
			saves := b.saves
			twin, err := restarted.GetTwin(ctx)
			if err != nil || b.saves != saves || twin.ReportedRevision != 1 || twin.Reported["Device.Test.Value"].Value != value {
				t.Fatal("view changed evidence or wrote", twin, err)
			}
			expected := "unknown"
			if mode == "begin-failed" {
				expected = "converged"
			}
			if twin.Convergence != expected {
				t.Fatal("freshness ignored durable mutation begin", twin)
			}
			if mode != "begin-failed" && !twin.Reported["Device.Test.Value"].FreshUntil.IsZero() {
				t.Fatal("historical value exposed as fresh")
			}
			var stored snapshot
			if err = json.Unmarshal(b.state, &stored); err != nil {
				t.Fatal(err)
			}
			if !stored.Reported["Device.Test.Value"].FreshUntil.After(*now) {
				t.Fatal("read modified stored evidence")
			}
			if mode == "begin-ambiguous" && len(f.calls) != 0 {
				t.Fatal("sent after ambiguous begin")
			}
		})
	}
}

func TestPostMutationEvidenceAndReadOnlyAttempt(t *testing.T) {
	a, _, f, now := managedActor(t)
	a.opts.Policy.ObserveGetResponses = true
	a.opts.Policy.ReadBeforeReconcile = true
	observe(t, a, "initial", *now, "target")
	if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	barrier := *now
	if _, err := a.EnqueueCommand(ctx, mutationRequest("explicit", now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	c, err := a.GetCommand(ctx, "explicit")
	if err != nil {
		t.Fatal(err)
	}
	if err = a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: setResponse(c.MessageID), ReceivedAt: *now}); err != nil {
		t.Fatal(err)
	}
	// Equal controller timestamps do not establish post-mutation evidence.
	observe(t, a, "equal-time", barrier, "target")
	twin, _ := a.GetTwin(ctx)
	if twin.Convergence != "unknown" {
		t.Fatal("equal-time evidence crossed mutation barrier", twin)
	}
	*now = now.Add(time.Second)
	if err = a.Tick(ctx); err != nil || len(f.calls) != 2 || f.calls[1].Message.GetBody().GetRequest().GetGet() == nil {
		t.Fatal("stale twin did not plan bounded read", err)
	}
	read, err := a.GetCommand(ctx, "reconcile-observe-1")
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	msg := getObservationResponse(read.MessageID, getPathResult("Device.Test.Value", "Device.Test.", map[string]string{"Value": "target"}))
	if err = a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: msg, ReceivedAt: *now}); err != nil {
		t.Fatal(err)
	}
	twin, _ = a.GetTwin(ctx)
	if twin.Convergence != "converged" {
		t.Fatal("newer Get failed to refresh twin", twin)
	}
	if _, err = a.EnqueueCommand(ctx, command("read-only", now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	if err = a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	twin, _ = a.GetTwin(ctx)
	if twin.Convergence != "converged" {
		t.Fatal("read-only attempt invalidated evidence", twin)
	}
}
