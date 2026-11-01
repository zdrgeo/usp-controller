package deviceactor

import (
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
)

func managedActor(t *testing.T) (*Actor, *memoryBackend, *fakeSender, *time.Time) {
	t.Helper()
	a, b, now := fixture(t)
	opts := a.opts
	opts.Policy = &device.TwinPolicy{SiteID: "site-1", Reconcile: true, CommandTimeoutSeconds: 30, Paths: map[string]device.ManagedPath{"Device.Test.Value": {Type: "string", Writable: true}, "Device.Test.ReadOnly": {Type: "string"}}}
	f := &fakeSender{ready: true}
	a, err := NewExecuting(identity, b, opts, f)
	if err != nil {
		t.Fatal(err)
	}
	return a, b, f, now
}
func observe(t *testing.T, a *Actor, id device.RequestID, now time.Time, v string) {
	t.Helper()
	if _, err := a.Report(ctx, ReportRequest{ID: id, Identity: identity, ReceivedAt: now, Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: v, Type: "string"}}}); err != nil {
		t.Fatal(err)
	}
}
func setResponse(mid string) *usp.Msg {
	return &usp.Msg{Header: &usp.Header{MsgId: mid, MsgType: usp.Header_SET_RESP}, Body: &usp.Body{MsgBody: &usp.Body_Response{Response: &usp.Response{RespType: &usp.Response_SetResp{SetResp: &usp.SetResp{}}}}}}
}
func TestManagedReconcileWaitsCoalescesAndVerifies(t *testing.T) {
	a, _, f, now := managedActor(t)
	if _, err := a.SetDesired(ctx, desired("first", 0)); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil || len(f.calls) != 0 {
		t.Fatal("blind write", err)
	}
	status, _ := a.GetStatus(ctx)
	if status.Reconciliation != "waiting_observation" {
		t.Fatal(status)
	}
	observe(t, a, "observation", *now, "old")
	f.ready = false
	r := desired("latest", 1)
	r.Values["Device.Test.Value"] = device.ParameterValue{Value: "latest", Type: "string"}
	if _, err := a.SetDesired(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil || len(f.calls) != 0 {
		t.Fatal(err)
	}
	f.ready = true
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	if len(f.calls) != 1 {
		t.Fatal(f.calls)
	}
	sent := f.calls[0].Message.GetBody().GetRequest().GetSet()
	if sent.AllowPartial || len(sent.UpdateObjs) != 1 || sent.UpdateObjs[0].ObjPath != "Device.Test." || sent.UpdateObjs[0].ParamSettings[0].Value != "latest" || !sent.UpdateObjs[0].ParamSettings[0].Required {
		t.Fatal(sent)
	}
	c, err := a.GetCommand(ctx, "reconcile-2")
	if err != nil || c.DesiredRevision != 2 {
		t.Fatal(c, err)
	}
	if err = a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: setResponse(c.MessageID)}); err != nil {
		t.Fatal(err)
	}
	twin, _ := a.GetTwin(ctx)
	if twin.Convergence != "pending" {
		t.Fatal("response fabricated convergence", twin)
	}
	if err = a.Tick(ctx); err != nil || len(f.calls) != 1 {
		t.Fatal("repeated set", err)
	}
	*now = now.Add(time.Second)
	n := valueNotification("verified", *now)
	n.Message.GetBody().GetRequest().GetNotify().GetValueChange().ParamValue = "latest"
	if _, err = a.HandleNotify(ctx, n); err != nil {
		t.Fatal(err)
	}
	twin, _ = a.GetTwin(ctx)
	if twin.Convergence != "converged" {
		t.Fatal(twin)
	}
	*now = now.Add(10 * time.Minute)
	if err = a.Tick(ctx); err != nil || len(f.calls) != 1 {
		t.Fatal("stale observation caused retry", err)
	}
	twin, _ = a.GetTwin(ctx)
	if twin.Convergence != "unknown" {
		t.Fatal(twin)
	}
}
func TestReconcileUnknownSurvivesRestartAndBlocksNewDesired(t *testing.T) {
	a, b, f, now := managedActor(t)
	observe(t, a, "observed", *now, "old")
	if _, err := a.SetDesired(ctx, desired("first", 0)); err != nil {
		t.Fatal(err)
	}
	s, _ := a.load(ctx)
	if err := a.scheduleReconciliation(ctx, s); err != nil {
		t.Fatal(err)
	}
	b.ambiguous = true
	if err := a.Tick(ctx); err == nil || len(f.calls) != 0 {
		t.Fatal("sent after ambiguous begin", err)
	}
	restarted, err := NewExecuting(identity, b, a.opts, f)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = restarted.SetDesired(ctx, desired("newer", 1)); err != nil {
		t.Fatal(err)
	}
	if err = restarted.Tick(ctx); err != nil || len(f.calls) != 0 {
		t.Fatal("replayed old revision", err)
	}
	c, _ := restarted.GetCommand(ctx, "reconcile-1")
	if c.State != device.CommandUnknown {
		t.Fatal(c)
	}
	status, _ := restarted.GetStatus(ctx)
	if status.Reconciliation != "blocked_unknown" {
		t.Fatal(status)
	}
}
func TestReconcileQueuedPolicyAndRevisionChanges(t *testing.T) {
	for _, change := range []string{"revision", "disabled", "stale", "removed-path"} {
		t.Run(change, func(t *testing.T) {
			a, b, f, now := managedActor(t)
			observe(t, a, "seen", *now, "old")
			if _, err := a.SetDesired(ctx, desired("target", 0)); err != nil {
				t.Fatal(err)
			}
			s, _ := a.load(ctx)
			if err := a.scheduleReconciliation(ctx, s); err != nil {
				t.Fatal(err)
			}
			opts := a.opts
			switch change {
			case "revision":
				r := device.SetDesiredRequest{ID: "remove", ExpectedRevision: 1, Remove: []string{"Device.Test.Value"}}
				if _, err := a.SetDesired(ctx, r); err != nil {
					t.Fatal(err)
				}
			case "disabled":
				p := *opts.Policy
				p.Reconcile = false
				opts.Policy = &p
			case "removed-path":
				p := *opts.Policy
				p.Paths = map[string]device.ManagedPath{"Device.Other.Value": {Type: "string", Writable: true}}
				opts.Policy = &p
			case "stale":
				*now = now.Add(10 * time.Minute)
			}
			restarted, err := NewExecuting(identity, b, opts, f)
			if err != nil {
				t.Fatal(err)
			}
			if err = restarted.Tick(ctx); err != nil || len(f.calls) != 0 {
				t.Fatal("ineligible queued work sent", err)
			}
			c, _ := restarted.GetCommand(ctx, "reconcile-1")
			if c.State != device.CommandCancelled {
				t.Fatal(c)
			}
		})
	}
}
func TestManagedPathsAndDiagnostics(t *testing.T) {
	a, _, _, now := managedActor(t)
	for _, path := range []string{"Device.Secret.Password", "Device.Test.ReadOnly", "Device.Test.*"} {
		r := desired("bad", 0)
		r.Values = map[string]device.ParameterValue{path: {Value: "secret", Type: "string"}}
		if _, err := a.SetDesired(ctx, r); !errors.Is(err, ErrInvalid) {
			t.Fatal(path, err)
		}
	}
	r := desired("wrong-type", 0)
	r.Values["Device.Test.Value"] = device.ParameterValue{Value: "1", Type: "int"}
	if _, err := a.SetDesired(ctx, r); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	// Unmanaged notifications remain durable events, but do not expand the twin.
	n := valueNotification("unmanaged", *now)
	n.Message.GetBody().GetRequest().GetNotify().GetValueChange().ParamPath = "Device.Secret.Password"
	if _, err := a.HandleNotify(ctx, n); err != nil {
		t.Fatal(err)
	}
	twin, _ := a.GetTwin(ctx)
	if len(twin.Reported) != 0 {
		t.Fatal("unmanaged twin value leaked")
	}
	status, err := a.GetStatus(ctx)
	if err != nil || status.PendingEvents != 1 || status.NotificationReceipts != 1 || status.SiteID != "site-1" || status.StateBytes == 0 || status.OldestEventAt == nil {
		t.Fatal(status, err)
	}
	if _, err = a.EnqueueCommand(ctx, command("reconcile-1", now.Add(time.Minute))); !errors.Is(err, ErrInvalid) {
		t.Fatal("reserved ID", err)
	}
}
func TestReconcileDisabledByDefaultAndCapacity(t *testing.T) {
	a, b, f, now := managedActor(t)
	observe(t, a, "seen", *now, "old")
	if _, err := a.SetDesired(ctx, desired("target", 0)); err != nil {
		t.Fatal(err)
	}
	opts := a.opts
	opts.Policy = nil
	noPolicy, err := NewExecuting(identity, b, opts, f)
	if err != nil {
		t.Fatal(err)
	}
	if err = noPolicy.Tick(ctx); err != nil || len(f.calls) != 0 {
		t.Fatal("default reconciled", err)
	}
	a.opts.MaxReceipts = 2
	if err = a.Tick(ctx); !errors.Is(err, ErrCapacity) || len(f.calls) != 0 {
		t.Fatal("sent despite capacity", err)
	}
}

func TestOldReconcileResponseCannotConvergeNewRevision(t *testing.T) {
	a, _, f, now := managedActor(t)
	observe(t, a, "seen", *now, "old")
	if _, err := a.SetDesired(ctx, desired("first", 0)); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	first, _ := a.GetCommand(ctx, "reconcile-1")
	r := desired("newer", 1)
	r.Values["Device.Test.Value"] = device.ParameterValue{Value: "new-target", Type: "string"}
	if _, err := a.SetDesired(ctx, r); err != nil {
		t.Fatal(err)
	}
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: setResponse(first.MessageID)}); err != nil {
		t.Fatal(err)
	}
	twin, _ := a.GetTwin(ctx)
	if twin.DesiredRevision != 2 || twin.Convergence != "pending" || twin.Reported["Device.Test.Value"].Value.Value != "old" {
		t.Fatal(twin)
	}
	*now = now.Add(time.Second)
	observe(t, a, "after-old", *now, "target")
	if err := a.Tick(ctx); err != nil || len(f.calls) != 2 {
		t.Fatal(err, len(f.calls))
	}
	second, _ := a.GetCommand(ctx, "reconcile-2")
	if second.DesiredRevision != 2 {
		t.Fatal(second)
	}
}

func TestPolicySnapshotIsCopiedAndOldTwinValuesAreHidden(t *testing.T) {
	original, b, now := fixture(t)
	if _, err := original.Report(ctx, ReportRequest{ID: "old", Identity: identity, ReceivedAt: *now, Values: map[string]device.ParameterValue{"Device.Old.Secret": {Value: "secret", Type: "string"}}}); err != nil {
		t.Fatal(err)
	}
	p := &device.TwinPolicy{SiteID: "s", CommandTimeoutSeconds: 30, Paths: map[string]device.ManagedPath{"Device.Test.Value": {Type: "string", Writable: true}}}
	opts := original.opts
	opts.Policy = p
	a, err := New(identity, b, opts)
	if err != nil {
		t.Fatal(err)
	}
	p.Paths["Device.Old.Secret"] = device.ManagedPath{Type: "string"}
	twin, err := a.GetTwin(ctx)
	if err != nil || len(twin.Reported) != 0 {
		t.Fatal(twin, err)
	}
}

func TestReconcileGroupsObjectsAndRechecksAfterExport(t *testing.T) {
	a, _, f, now := managedActor(t)
	paths := []string{"Device.Test.A", "Device.Test.B.Value", "Device.Test.C"}
	values := map[string]device.ParameterValue{}
	for _, path := range paths {
		a.opts.Policy.Paths[path] = device.ManagedPath{Type: "string", Writable: true}
		values[path] = device.ParameterValue{Value: "old", Type: "string"}
	}
	if _, err := a.Report(ctx, ReportRequest{ID: "seen", Identity: identity, ReceivedAt: *now, Values: values}); err != nil {
		t.Fatal(err)
	}
	for path := range values {
		values[path] = device.ParameterValue{Value: "target", Type: "string"}
	}
	if _, err := a.SetDesired(ctx, device.SetDesiredRequest{ID: "desired", Values: values}); err != nil {
		t.Fatal(err)
	}
	s, _ := a.load(ctx)
	request, _ := a.planReconciliation(s)
	objs := request.GetSet().UpdateObjs
	if len(objs) != 2 || objs[0].ObjPath != "Device.Test." || len(objs[0].ParamSettings) != 2 || objs[1].ObjPath != "Device.Test.B." {
		t.Fatal(objs)
	}
	if err := a.scheduleReconciliation(ctx, s); err != nil {
		t.Fatal(err)
	}
	if _, err := a.HandleNotify(ctx, valueNotification("event", *now)); err != nil {
		t.Fatal(err)
	}
	a.publisher = &eventSink{after: func() { *now = now.Add(10 * time.Minute) }}
	if err := a.Tick(ctx); err != nil || len(f.calls) != 0 {
		t.Fatal("sent stale after export", err)
	}
	c, _ := a.GetCommand(ctx, "reconcile-1")
	if c.State != device.CommandCancelled {
		t.Fatal(c)
	}
}
