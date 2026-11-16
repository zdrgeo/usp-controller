package deviceactor

import (
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

func TestDesiredReceiptRecoverySurvivesPolicyWithdrawalWithoutReapplyingIntent(t *testing.T) {
	for _, mode := range []string{"removed", "read-only", "type-changed"} {
		t.Run(mode, func(t *testing.T) {
			a, b, f, _ := managedActor(t)
			original := desired("accepted", 0)
			b.ambiguous = true
			if _, err := a.SetDesired(ctx, original); err == nil {
				t.Fatal("ambiguous admission reported success")
			}
			latest := desired("latest", 1)
			latest.Values["Device.Test.Value"] = device.ParameterValue{Type: "string", Value: "latest"}
			if _, err := a.SetDesired(ctx, latest); err != nil {
				t.Fatal(err)
			}
			opts := a.opts
			p := *opts.Policy
			switch mode {
			case "removed":
				p.Paths = map[string]device.ManagedPath{"Device.Other.Value": {Type: "string", Writable: true}}
			case "read-only":
				p.Paths = map[string]device.ManagedPath{"Device.Test.Value": {Type: "string"}}
			case "type-changed":
				p.Paths = map[string]device.ManagedPath{"Device.Test.Value": {Type: "boolean", Writable: true}}
			}
			opts.Policy = &p
			opts.MaxReceipts = 2 // Receipts are full; recovery adds no record.
			restarted, err := NewExecuting(identity, b, opts, f)
			if err != nil {
				t.Fatal(err)
			}
			b.failSave = true
			before := string(b.state)
			saves := b.saves
			receipt, err := restarted.SetDesired(ctx, original)
			if err != nil || receipt.ID != original.ID || receipt.Revision != 1 || b.saves != saves || string(b.state) != before || len(f.calls) != 0 {
				t.Fatal("old admission was lost/reapplied", receipt, err)
			}
			changed := desired(original.ID, original.ExpectedRevision)
			changed.Values["Device.Test.Value"] = device.ParameterValue{Type: "string", Value: "changed"}
			if _, err = restarted.SetDesired(ctx, changed); !errors.Is(err, ErrConflict) {
				t.Fatal("changed retry evaded immutable receipt", err)
			}
			fresh := desired("new-intent", 2)
			if _, err = restarted.SetDesired(ctx, fresh); !errors.Is(err, ErrInvalid) {
				t.Fatal("new intent bypassed current policy", err)
			}
			if b.saves != saves || string(b.state) != before {
				t.Fatal("recovery or rejected intent changed current state")
			}
		})
	}
}

func TestReportReceiptRecoverySurvivesPolicyChangeAndClockRollback(t *testing.T) {
	a, b, f, now := managedActor(t)
	originalTime := *now
	original := ReportRequest{ID: "accepted-report", Identity: identity, ReceivedAt: *now, Values: map[string]device.ParameterValue{"Device.Test.Value": {Type: "string", Value: "old"}}}
	b.ambiguous = true
	if _, err := a.Report(ctx, original); err == nil {
		t.Fatal("ambiguous report reported success")
	}
	*now = now.Add(time.Second)
	if _, err := a.Report(ctx, ReportRequest{ID: "newer-report", Identity: identity, ReceivedAt: *now, Values: map[string]device.ParameterValue{"Device.Test.Value": {Type: "string", Value: "newer"}}}); err != nil {
		t.Fatal(err)
	}
	opts := a.opts
	p := *opts.Policy
	p.Paths = map[string]device.ManagedPath{"Device.Test.Value": {Type: "boolean", Writable: true}}
	opts.Policy = &p
	opts.MaxReceipts = 2
	*now = originalTime.Add(-time.Second)
	restarted, err := NewExecuting(identity, b, opts, f)
	if err != nil {
		t.Fatal(err)
	}
	b.failSave = true
	before := string(b.state)
	saves := b.saves
	revision, err := restarted.Report(ctx, original)
	if err != nil || revision != 1 || b.saves != saves || string(b.state) != before || len(f.calls) != 0 {
		t.Fatal("report receipt lost or observation refreshed", revision, err)
	}
	changed := original
	changed.Values = map[string]device.ParameterValue{"Device.Test.Value": {Type: "boolean", Value: "true"}}
	if _, err = restarted.Report(ctx, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("changed report retry did not conflict", err)
	}
	changed.ID = "future-new-report" // Valid new type, but receive time is now future.
	if _, err = restarted.Report(ctx, changed); !errors.Is(err, ErrInvalid) {
		t.Fatal("new future report bypassed clock check", err)
	}
	fresh := original
	fresh.ID = "new-disallowed-type"
	fresh.ReceivedAt = *now
	if _, err = restarted.Report(ctx, fresh); !errors.Is(err, ErrInvalid) {
		t.Fatal("new report bypassed current policy", err)
	}
	if b.saves != saves || string(b.state) != before {
		t.Fatal("recovery or rejected report changed stored evidence")
	}
}
