package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
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

func TestFleetObservationTotalsExcludeExpiredRowsAndExposeMissingDiagnostics(t *testing.T) {
	p, store, now := fixture(t)
	until := now.Add(10 * time.Second)
	current := status(1)
	current.ObservationCoverage = &deviceactor.ObservationCoverage{AsOf: *now, ManagedPaths: 7, Fresh: 1, Missing: 1, Invalid: 1, Future: 1, MutationInvalidated: 1, TopologyInvalidated: 1, Stale: 1, FreshUntil: &until}
	p.invoker = invokeFunc(func(_ context.Context, id device.DeviceIdentity, _ string, _ any) (json.RawMessage, error) {
		if id.AgentID == "a" {
			return json.Marshal(current)
		}
		return json.Marshal(status(1)) // An older host has no coverage diagnostic.
	})
	refresh(p, "a")
	refresh(p, "b")
	p.invoker = invokeFunc(func(context.Context, device.DeviceIdentity, string, any) (json.RawMessage, error) {
		t.Fatal("summary query fanned out to actor")
		return nil, nil
	})
	token := strings.Repeat("k", 32)
	h := p.Handler(http.NotFoundHandler(), token)
	saves := store.saves
	read := func() (ObservationTotals, map[string]int) {
		t.Helper()
		w := query(h, "/v2/fleet/summary", token, "site")
		var result struct {
			Coverage  ObservationTotals `json:"fresh_observation_coverage"`
			Freshness map[string]int    `json:"freshness"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || store.saves != saves {
			t.Fatal("summary did not remain a cache-only read", w.Code, w.Body.String(), err)
		}
		return result.Coverage, result.Freshness
	}
	got, freshness := read()
	want := ObservationTotals{CoveredDevices: 1, UnavailableDevices: 1, ManagedPaths: 7, Fresh: 1, Missing: 1, Invalid: 1, Future: 1, MutationInvalidated: 1, TopologyInvalidated: 1, Stale: 1}
	if got != want || freshness["fresh"] != 2 {
		t.Fatal("partial observation coverage was misrepresented", got, freshness)
	}
	*now = until
	got, freshness = read()
	if got != (ObservationTotals{UnavailableDevices: 1}) || freshness["fresh"] != 1 || freshness["stale"] != 1 {
		t.Fatal("expired evidence remained in fleet totals", got, freshness)
	}
	p.fault = errors.New("projection persistence failed")
	got, freshness = read()
	if got != (ObservationTotals{}) || freshness["fresh"] != 0 || freshness["stale"] != 2 {
		t.Fatal("untrusted projection contributed observation totals", got, freshness)
	}
}
