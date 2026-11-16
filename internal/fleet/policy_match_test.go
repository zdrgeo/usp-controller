package fleet

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"example.com/usp-controller/internal/device"
)

func TestExpectedPolicyRejectsMismatchedRefreshWithoutLosingCachedEvidence(t *testing.T) {
	p, store, now := fixture(t)
	oldPolicy, expected := strings.Repeat("a", 64), strings.Repeat("b", 64)
	current := status(1)
	current.TwinPolicyFingerprint = oldPolicy
	p.invoker = invokeFunc(func(_ context.Context, _ device.DeviceIdentity, method string, _ any) (json.RawMessage, error) {
		if method != "GetStatus" {
			t.Fatal("policy matching invoked an actor mutation", method)
		}
		return json.Marshal(current)
	})
	refresh(p, "a")
	opts := p.opts
	opts.ExpectedTwinPolicyFingerprint = expected
	inventory := map[string]device.DeviceIdentity{"a": identity("a"), "b": identity("b")}
	guarded, err := newProjection(opts, inventory, p.invoker, store)
	if err != nil || guarded.rows["a"].Snapshot.TwinPolicyFingerprint != oldPolicy {
		t.Fatal("policy rollout could not restore historical evidence", err)
	}
	for _, digest := range []string{oldPolicy, ""} {
		current.TwinPolicyFingerprint = digest
		refresh(guarded, "a")
		r := guarded.rows["a"]
		if r.LastError != "refresh_failed" || !r.Pending || r.Snapshot == nil || r.Snapshot.TwinPolicyFingerprint != oldPolicy || guarded.view(r, *now).Freshness != "stale" {
			t.Fatal("mismatched or unavailable policy replaced trusted evidence", r)
		}
	}
	current.TwinPolicyFingerprint = expected
	refresh(guarded, "a")
	r := guarded.rows["a"]
	if r.LastError != "" || r.Pending || r.Snapshot.TwinPolicyFingerprint != expected || guarded.view(r, *now).Freshness != "fresh" {
		t.Fatal("matching policy did not restore a current projection", r)
	}
	// Empty configuration preserves compatibility with hosts without diagnostics.
	opts.ExpectedTwinPolicyFingerprint = ""
	unguarded, err := newProjection(opts, inventory, p.invoker, store)
	if err != nil {
		t.Fatal(err)
	}
	current.TwinPolicyFingerprint = ""
	refresh(unguarded, "a")
	if unguarded.view(unguarded.rows["a"], *now).Freshness != "fresh" {
		t.Fatal("default projection rejected older status")
	}
}
