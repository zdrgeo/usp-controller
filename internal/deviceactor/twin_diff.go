package deviceactor

import (
	"context"
	"sort"
	"time"

	"example.com/usp-controller/internal/device"
)

func (a *Actor) desiredPathState(s *snapshot, path string, want device.ParameterValue, now time.Time) string {
	if a.opts.Policy != nil && !a.opts.Policy.Allows(path, want, true) {
		return "policy_blocked"
	}
	got, ok := s.Reported[path]
	if !ok {
		return "missing"
	}
	if a.opts.Policy != nil && !a.opts.Policy.Allows(path, got.Value, false) {
		return "invalid"
	}
	if got.ReceivedAt.After(now) {
		return "future"
	}
	if !s.mutationBarrier.IsZero() && !got.ReceivedAt.After(s.mutationBarrier) {
		return "mutation_invalidated"
	}
	if s.TopologyBarrier != nil && !got.ReceivedAt.After(s.TopologyBarrier.At) {
		return "topology_invalidated"
	}
	if !now.Before(got.FreshUntil) {
		return "stale"
	}
	if got.Value != want {
		return "different"
	}
	return "matching"
}

func (a *Actor) convergenceAt(s *snapshot, now time.Time) string {
	state := "converged"
	for path, want := range s.Desired {
		switch a.desiredPathState(s, path, want, now) {
		case "matching":
		case "different":
			state = "pending"
		default:
			return "unknown"
		}
	}
	return state
}

// GetTwinDiff performs a single bounded load. It never plans commands, repairs
// reminders, expires queued intent or writes state. One clock reading governs it.
func (a *Actor) GetTwinDiff(ctx context.Context) (device.TwinDiff, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s, err := a.load(ctx)
	if err != nil {
		return device.TwinDiff{}, err
	}
	now := a.opts.Now().UTC()
	out := device.TwinDiff{AsOf: now, DesiredRevision: s.DesiredRevision, ReportedRevision: s.ReportedRevision, Convergence: a.convergenceAt(s, now), Paths: []device.TwinPathDiff{}}
	if !s.mutationBarrier.IsZero() {
		at := s.mutationBarrier
		out.MutationBarrierAt = &at
	}
	if s.TopologyBarrier != nil {
		at := s.TopologyBarrier.At
		out.TopologyBarrierAt = &at
	}
	for path, want := range s.Desired {
		if a.opts.Policy != nil {
			if _, ok := a.opts.Policy.Paths[path]; !ok {
				out.HiddenDesiredPaths++
				continue
			}
		}
		row := device.TwinPathDiff{Path: path, State: a.desiredPathState(s, path, want, now)}
		if got, ok := s.Reported[path]; ok {
			row.HasObservation = true
			at, until := got.ReceivedAt, got.FreshUntil
			if row.State == "future" || row.State == "mutation_invalidated" || row.State == "topology_invalidated" {
				until = time.Time{}
			}
			row.ReceivedAt = &at
			row.FreshUntil = &until
		}
		out.Paths = append(out.Paths, row)
	}
	sort.Slice(out.Paths, func(i, j int) bool { return out.Paths[i].Path < out.Paths[j].Path })
	return out, nil
}
