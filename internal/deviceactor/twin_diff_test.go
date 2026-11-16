package deviceactor

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

func TestTwinDiffIsReadOnlyPolicyScopedAndValueFree(t *testing.T) {
	a, b, now := fixture(t)
	wanted := map[string]device.ParameterValue{}
	for _, name := range []string{"A", "B", "Hidden", "E", "F", "Stale"} {
		wanted["Device."+name+".Value"] = device.ParameterValue{Type: "string", Value: "SENSITIVE_TARGET"}
	}
	wanted["Device.Invalid.Value"] = device.ParameterValue{Type: "boolean", Value: "true"}
	if _, err := a.SetDesired(ctx, device.SetDesiredRequest{ID: "desired", Values: wanted}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Report(ctx, ReportRequest{ID: "observed", Identity: identity, ReceivedAt: *now, Values: map[string]device.ParameterValue{
		"Device.A.Value": wanted["Device.A.Value"], "Device.E.Value": wanted["Device.E.Value"], "Device.F.Value": {Type: "string", Value: "SENSITIVE_OBSERVED"}, "Device.Invalid.Value": {Type: "boolean", Value: "INVALID_BOOLEAN"}, "Device.ReportOnly.Value": {Type: "string", Value: "not-desired"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Report(ctx, ReportRequest{ID: "old", Identity: identity, ReceivedAt: now.Add(-10 * time.Minute), Values: map[string]device.ParameterValue{"Device.Stale.Value": wanted["Device.Stale.Value"]}}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.EnqueueCommand(ctx, command("expired-but-not-maintained", now.Add(time.Second))); err != nil {
		t.Fatal(err)
	}
	opts := a.opts
	opts.Policy = &device.TwinPolicy{SiteID: "site", Reconcile: true, CommandTimeoutSeconds: 30, Paths: map[string]device.ManagedPath{}}
	for path, want := range wanted {
		if path != "Device.Hidden.Value" {
			opts.Policy.Paths[path] = device.ManagedPath{Type: want.Type, Writable: path != "Device.E.Value"}
		}
	}
	f := &fakeSender{ready: true}
	a, err := NewExecuting(identity, b, opts, f)
	if err != nil {
		t.Fatal(err)
	}
	*now = now.Add(2 * time.Second)
	b.failSave = true
	b.failReminder = true
	before := string(b.state)
	saves := b.saves
	diff, err := a.GetTwinDiff(ctx)
	if err != nil || diff.Convergence != "unknown" || diff.HiddenDesiredPaths != 1 || diff.DesiredRevision != 1 || diff.ReportedRevision != 2 || !diff.AsOf.Equal(*now) {
		t.Fatal(diff, err)
	}
	paths, states := []string{}, []string{}
	for _, row := range diff.Paths {
		paths = append(paths, row.Path)
		states = append(states, row.State)
	}
	if !reflect.DeepEqual(paths, []string{"Device.A.Value", "Device.B.Value", "Device.E.Value", "Device.F.Value", "Device.Invalid.Value", "Device.Stale.Value"}) || !reflect.DeepEqual(states, []string{"matching", "missing", "policy_blocked", "different", "invalid", "stale"}) {
		t.Fatal(diff)
	}
	if diff.Paths[1].HasObservation || diff.Paths[1].ReceivedAt != nil || !diff.Paths[0].HasObservation || diff.Paths[0].ReceivedAt == nil {
		t.Fatal("incorrect observation metadata", diff)
	}
	raw, _ := json.Marshal(diff)
	for _, secret := range []string{"SENSITIVE_TARGET", "SENSITIVE_OBSERVED", "INVALID_BOOLEAN", "Device.Hidden.Value", "Device.ReportOnly.Value", "expired-but-not-maintained"} {
		if strings.Contains(string(raw), secret) {
			t.Fatal("diff leaked values or out-of-scope data", string(raw))
		}
	}
	c, err := a.GetCommand(ctx, "expired-but-not-maintained")
	if err != nil || c.State != device.CommandQueued || b.saves != saves || string(b.state) != before || len(f.calls) != 0 {
		t.Fatal("diff performed maintenance or dispatch", c, err)
	}
}

func TestTwinDiffClockRollbackAndMutationAgreeWithExecutionGuards(t *testing.T) {
	a, _, f, now := managedActor(t)
	observe(t, a, "initial", *now, "old")
	if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
		t.Fatal(err)
	}
	original := *now
	*now = now.Add(-time.Second)
	diff, err := a.GetTwinDiff(ctx)
	twin, twinErr := a.GetTwin(ctx)
	if err != nil || twinErr != nil || diff.Paths[0].State != "future" || diff.Convergence != "unknown" || twin.Convergence != "unknown" || !twin.Reported["Device.Test.Value"].FreshUntil.IsZero() {
		t.Fatal("future evidence looked usable", diff, twin, err, twinErr)
	}
	if err = a.Tick(ctx); err != nil || len(f.calls) != 0 {
		t.Fatal("clock rollback authorized write", err)
	}
	*now = original.Add(time.Second)
	diff, err = a.GetTwinDiff(ctx)
	if err != nil || diff.Paths[0].State != "different" || diff.Convergence != "pending" {
		t.Fatal(diff, err)
	}
	if err = a.Tick(ctx); err != nil || len(f.calls) != 1 {
		t.Fatal("recovered clock failed to authorize eligible work", err)
	}
	diff, err = a.GetTwinDiff(ctx)
	if err != nil || diff.Paths[0].State != "mutation_invalidated" || diff.Convergence != "unknown" || diff.MutationBarrierAt == nil || !diff.MutationBarrierAt.Equal(*now) || !diff.Paths[0].FreshUntil.IsZero() {
		t.Fatal("mutation boundary not explained", diff, err)
	}
}

func TestTwinDiffObservationOnlyCoverageAndLifecycleInvalidation(t *testing.T) {
	a, b, now := fixture(t)
	if _, err := a.Report(ctx, ReportRequest{ID: "recent", Identity: identity, ReceivedAt: *now, Values: map[string]device.ParameterValue{
		"Device.Fresh.Value":     {Type: "string", Value: "SECRET_FRESH"},
		"Device.Retyped.Value":   {Type: "string", Value: "SECRET_RETYPE"},
		"Device.Withdrawn.Value": {Type: "string", Value: "SECRET_WITHDRAWN"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Report(ctx, ReportRequest{ID: "old", Identity: identity, ReceivedAt: now.Add(-10 * time.Minute), Values: map[string]device.ParameterValue{"Device.Stale.Value": {Type: "string", Value: "SECRET_OLD"}}}); err != nil {
		t.Fatal(err)
	}
	opts := a.opts
	opts.Policy = &device.TwinPolicy{SiteID: "site", CommandTimeoutSeconds: 30, Paths: map[string]device.ManagedPath{
		"Device.Fresh.Value": {Type: "string"}, "Device.Missing.Value": {Type: "string"},
		"Device.Retyped.Value": {Type: "boolean"}, "Device.Stale.Value": {Type: "string"},
	}}
	f := &fakeSender{ready: true}
	a, err := NewExecuting(identity, b, opts, f)
	if err != nil {
		t.Fatal(err)
	}
	check := func(want []string) device.TwinDiff {
		t.Helper()
		b.failSave, b.failReminder = true, true
		before, saves := string(b.state), b.saves
		diff, err := a.GetTwinDiff(ctx)
		if err != nil || diff.Convergence != "converged" || len(diff.Paths) != 0 || diff.HiddenReportedPaths != 1 || len(diff.ObservationPaths) != 4 {
			t.Fatal("observation coverage changed desired convergence or scope", diff, err)
		}
		paths, states := []string{}, []string{}
		for _, row := range diff.ObservationPaths {
			paths, states = append(paths, row.Path), append(states, row.State)
			if row.State == "missing" && (row.HasObservation || row.ReceivedAt != nil || row.FreshUntil != nil) {
				t.Fatal("missing observation acquired metadata", row)
			}
			if row.State == "topology_invalidated" && (row.FreshUntil == nil || !row.FreshUntil.IsZero()) {
				t.Fatal("invalidated observation exposes effective freshness", row)
			}
		}
		if !reflect.DeepEqual(states, want) || !reflect.DeepEqual(paths, []string{"Device.Fresh.Value", "Device.Missing.Value", "Device.Retyped.Value", "Device.Stale.Value"}) {
			t.Fatal(diff)
		}
		status, err := a.GetStatus(ctx)
		coverage := status.ObservationCoverage
		if err != nil || coverage == nil || !coverage.Valid() || !coverage.AsOf.Equal(*now) || coverage.ManagedPaths != 4 || coverage.Missing != 1 || coverage.Invalid != 1 || status.Convergence != "converged" {
			t.Fatal("status lost sampled observation health", status, err)
		}
		if want[0] == "fresh" {
			if coverage.Fresh != 1 || coverage.Stale != 1 || coverage.FreshUntil == nil || !coverage.FreshUntil.Equal(now.Add(a.opts.Freshness)) {
				t.Fatal("fresh coverage/expiry incorrect", coverage)
			}
		} else if coverage.TopologyInvalidated != 2 || coverage.Fresh != 0 || coverage.FreshUntil != nil {
			t.Fatal("lifecycle coverage remained fresh", coverage)
		}
		raw, _ := json.Marshal(diff)
		if strings.Contains(string(raw), "SECRET_") || strings.Contains(string(raw), "Device.Withdrawn.Value") || string(b.state) != before || b.saves != saves || len(f.calls) != 0 {
			t.Fatal("coverage leaked values/withdrawn paths or changed state", string(raw))
		}
		b.failSave, b.failReminder = false, false
		return diff
	}
	check([]string{"fresh", "missing", "invalid", "stale"})
	*now = now.Add(time.Second)
	if _, err := a.HandleNotify(ctx, topologyNotification("onboard", "on_board_request", "", *now)); err != nil {
		t.Fatal(err)
	}
	check([]string{"topology_invalidated", "missing", "invalid", "topology_invalidated"})
}
