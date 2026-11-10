package deviceactor

import (
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

func TestQueuedReconcileRechecksUnchangedPathFreshness(t *testing.T) {
	for _, stage := range []string{"maintenance", "after-export"} {
		t.Run(stage, func(t *testing.T) {
			a, _, f, now := managedActor(t)
			a.opts.Policy.Paths["Device.Test.Stable"] = device.ManagedPath{Type: "string", Writable: true}
			if _, err := a.Report(ctx, ReportRequest{ID: "stable", Identity: identity, ReceivedAt: *now, Values: map[string]device.ParameterValue{"Device.Test.Stable": value}}); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(4 * time.Minute)
			observe(t, a, "changing", *now, "old")
			if _, err := a.SetDesired(ctx, device.SetDesiredRequest{ID: "desired", Values: map[string]device.ParameterValue{"Device.Test.Value": value, "Device.Test.Stable": value}}); err != nil {
				t.Fatal(err)
			}
			s, err := a.load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			request, _ := a.planReconciliation(s)
			if request == nil || len(request.GetSet().UpdateObjs[0].ParamSettings) != 1 {
				t.Fatal("fixture must omit already-converged path", request)
			}
			if err = a.scheduleReconciliation(ctx, s); err != nil {
				t.Fatal(err)
			}
			if stage == "maintenance" {
				*now = now.Add(2 * time.Minute)
			} else {
				if _, err = a.HandleNotify(ctx, valueNotification("export", *now)); err != nil {
					t.Fatal(err)
				}
				a.publisher = &eventSink{after: func() { *now = now.Add(2 * time.Minute) }}
			}
			if err = a.Tick(ctx); err != nil || len(f.calls) != 0 {
				t.Fatal("sent with stale unchanged desired path", err)
			}
			c, err := a.GetCommand(ctx, "reconcile-1")
			if err != nil || c.State != device.CommandCancelled || c.MessageID != "" || c.AttemptSequence != 0 {
				t.Fatal("stale diff crossed begin", c, err)
			}
		})
	}
}

func TestQueuedReconcileRequiresCompleteCurrentDiff(t *testing.T) {
	for _, mode := range []string{"new-drift", "converged-path", "same-diff"} {
		t.Run(mode, func(t *testing.T) {
			a, _, f, now := managedActor(t)
			a.opts.Policy.Paths["Device.Test.Stable"] = device.ManagedPath{Type: "string", Writable: true}
			initial := map[string]device.ParameterValue{"Device.Test.Value": {Type: "string", Value: "old"}, "Device.Test.Stable": value}
			if _, err := a.Report(ctx, ReportRequest{ID: "initial", Identity: identity, ReceivedAt: *now, Values: initial}); err != nil {
				t.Fatal(err)
			}
			if _, err := a.SetDesired(ctx, device.SetDesiredRequest{ID: "desired", Values: map[string]device.ParameterValue{"Device.Test.Value": value, "Device.Test.Stable": value}}); err != nil {
				t.Fatal(err)
			}
			s, err := a.load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err = a.scheduleReconciliation(ctx, s); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(time.Second)
			updated := map[string]device.ParameterValue{}
			switch mode {
			case "new-drift":
				updated["Device.Test.Stable"] = device.ParameterValue{Type: "string", Value: "drifted"}
			case "converged-path":
				updated["Device.Test.Value"] = value
			case "same-diff":
				updated["Device.Test.Value"] = device.ParameterValue{Type: "string", Value: "another-old-value"}
			}
			if _, err = a.Report(ctx, ReportRequest{ID: "update", Identity: identity, ReceivedAt: *now, Values: updated}); err != nil {
				t.Fatal(err)
			}
			if err = a.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			c, err := a.GetCommand(ctx, "reconcile-1")
			if err != nil {
				t.Fatal(err)
			}
			if mode == "same-diff" {
				if len(f.calls) != 1 || c.State != device.CommandAwaitingResponse {
					t.Fatal("valid unchanged diff rejected", c)
				}
			} else {
				if len(f.calls) != 0 || c.State != device.CommandCancelled || c.AttemptSequence != 0 {
					t.Fatal("obsolete diff sent", c)
				}
				if err = a.Tick(ctx); err != nil || len(f.calls) != 0 {
					t.Fatal("cancelled revision replanned", err)
				}
			}
		})
	}
}
