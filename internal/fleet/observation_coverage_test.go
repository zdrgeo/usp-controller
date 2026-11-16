package fleet

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

func TestObservationCoverageExpiresAndSurvivesProjectionRestore(t *testing.T) {
	p, store, now := fixture(t)
	until := now.Add(10 * time.Second) // Earlier than both normal refresh and cache TTL.
	current := status(1)
	current.ObservationCoverage = &deviceactor.ObservationCoverage{AsOf: *now, ManagedPaths: 2, Fresh: 1, Missing: 1, FreshUntil: &until}
	p.invoker = invokeFunc(func(_ context.Context, _ device.DeviceIdentity, method string, _ any) (json.RawMessage, error) {
		if method != "GetStatus" {
			t.Fatal("observation refresh mutated actor", method)
		}
		return json.Marshal(current)
	})
	refresh(p, "a")
	if p.rows["a"].LastError != "" || p.view(p.rows["a"], *now).Freshness != "fresh" {
		t.Fatal("valid coverage not projected", p.rows["a"])
	}
	reopened, err := newProjection(p.opts, map[string]device.DeviceIdentity{"a": identity("a"), "b": identity("b")}, p.invoker, store)
	if err != nil || reopened.rows["a"].Snapshot.ObservationCoverage == nil || !reopened.rows["a"].Snapshot.ObservationCoverage.FreshUntil.Equal(until) {
		t.Fatal("coverage lost on projection restore", err)
	}
	refresh(reopened, "a") // Clear the ordinary startup invalidation first.
	if reopened.view(reopened.rows["a"], *now).Freshness != "fresh" {
		t.Fatal("restored projection did not become current after refresh")
	}
	*now = until
	if reopened.view(reopened.rows["a"], *now).Freshness != "stale" {
		t.Fatal("expired observation health looked current")
	}
	current.ObservationCoverage.Missing++ // Counters no longer partition managed paths.
	refresh(reopened, "a")
	r := reopened.rows["a"]
	if r.LastError != "refresh_failed" || !r.Pending || r.Snapshot.ObservationCoverage.Missing != 1 {
		t.Fatal("malformed coverage replaced retained evidence", r)
	}
	current.ObservationCoverage = &deviceactor.ObservationCoverage{AsOf: *now, ManagedPaths: 2, Stale: 1, Missing: 1}
	refresh(reopened, "a")
	if reopened.view(reopened.rows["a"], *now).Freshness != "fresh" || reopened.rows["a"].Snapshot.ObservationCoverage.Stale != 1 {
		t.Fatal("newly sampled stale observations confused with a stale projection")
	}
	current.ObservationCoverage.FreshUntil = &until // No fresh observation can justify an expiry.
	if validateStatus(current, "site") == nil {
		t.Fatal("expiry without fresh evidence accepted")
	}
	// Older hosts and cached rows still omit the optional diagnostic.
	current.ObservationCoverage = nil
	if err := validateStatus(current, "site"); err != nil {
		t.Fatal("older status rejected", err)
	}
}
