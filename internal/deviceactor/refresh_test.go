package deviceactor

import (
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

func TestRefreshRequiresExecutionAndProjectionPolicy(t *testing.T) {
	for _, mode := range []string{"shadow", "no-policy", "projection-off"} {
		t.Run(mode, func(t *testing.T) {
			a, b, _, now := managedActor(t)
			a.opts.Policy.ObserveGetResponses = true
			switch mode {
			case "shadow":
				a.transport = nil
			case "no-policy":
				a.opts.Policy = nil
			case "projection-off":
				a.opts.Policy.ObserveGetResponses = false
			}
			saves := b.saves
			if _, err := a.RefreshTwin(ctx, device.RefreshTwinRequest{RequestID: "refresh", Deadline: now.Add(time.Hour)}); !errors.Is(err, ErrDisabled) || b.saves != saves {
				t.Fatal("refresh accepted without opt-in projection", err)
			}
		})
	}
}

func TestRefreshScopeAdmissionAndReceiptRecovery(t *testing.T) {
	a, b, f, now := managedActor(t)
	a.opts.Policy.ObserveGetResponses = true
	a.opts.Policy.Paths = map[string]device.ManagedPath{"Device.Test.Value": {Type: "string", Writable: true}, "Device.Test.Flag": {Type: "boolean"}} // Read-only paths are eligible.
	r := device.RefreshTwinRequest{RequestID: "refresh", Deadline: now.Add(time.Hour), Paths: []string{"Device.Test.Value", "Device.Test.Flag"}}
	status, err := a.GetStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r.ExpectedPolicyFingerprint = status.TwinPolicyFingerprint
	b.ambiguous = true
	if _, err := a.RefreshTwin(ctx, r); err == nil {
		t.Fatal("ambiguous admission reported success")
	}
	opts := a.opts
	opts.Freshness += time.Second
	restarted, err := NewExecuting(identity, b, opts, f)
	if err != nil {
		t.Fatal(err)
	}
	*now = r.Deadline.Add(time.Second)
	before, saves := string(b.state), b.saves
	b.failSave = true
	// Normalization must not mutate caller input; equivalent path orders and
	// all-path shorthand recover the original receipt, even after its deadline.
	for _, paths := range [][]string{{"Device.Test.Flag", "Device.Test.Value"}, nil} {
		retry := r
		retry.Paths = paths
		got, err := restarted.RefreshTwin(ctx, retry)
		if err != nil || got.ID != r.RequestID || got.Revision != 1 || got.State != device.CommandQueued {
			t.Fatal("equivalent scope lost its receipt", got, err)
		}
	}
	if r.Paths[0] != "Device.Test.Value" {
		t.Fatal("admission reordered caller input")
	}
	unguarded := r
	unguarded.ExpectedPolicyFingerprint = ""
	if _, err := restarted.RefreshTwin(ctx, unguarded); !errors.Is(err, ErrConflict) {
		t.Fatal("guard was removed from immutable request identity", err)
	}
	stale := r
	stale.RequestID = "new-stale-policy"
	stale.Deadline = now.Add(time.Hour)
	if _, err := restarted.RefreshTwin(ctx, stale); !errors.Is(err, ErrConflict) {
		t.Fatal("stale policy admitted a new refresh", err)
	}
	changed := r
	changed.Paths = []string{"Device.Test.Value"}
	if _, err := restarted.RefreshTwin(ctx, changed); !errors.Is(err, ErrConflict) {
		t.Fatal("changed effective scope reused receipt", err)
	}
	for _, paths := range [][]string{{"Device.Secret.Password"}, {"Device.Test."}, {"Device.Test.*"}, {"Device.Test.Value", "Device.Test.Value"}, {"Device.Test.Value", "Device.Test.Flag", "Device.Extra.Value"}} {
		fresh := r
		fresh.RequestID = "new-refresh"
		fresh.Deadline = now.Add(time.Hour)
		fresh.Paths = paths
		if _, err := restarted.RefreshTwin(ctx, fresh); !errors.Is(err, ErrInvalid) {
			t.Fatal("scope escaped policy or duplicate check", paths, err)
		}
	}
	if b.saves != saves || string(b.state) != before || len(f.calls) != 0 {
		t.Fatal("recovery/rejection changed state or dispatched work")
	}
}
