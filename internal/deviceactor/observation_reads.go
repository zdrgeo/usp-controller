package deviceactor

import (
	"sort"

	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
)

// One bounded bootstrap read per desired revision, never a polling/retry loop.
// Reconciliation itself still requires fresh, typed observations for all paths.
func (a *Actor) planObservation(s *snapshot) (*usp.Request, string) {
	p := a.opts.Policy
	if p == nil || !p.ReadBeforeReconcile || !p.ObserveGetResponses || !p.Reconcile {
		return nil, "waiting_observation"
	}
	if s.Execution.Paused {
		return nil, "paused"
	}
	if s.DesiredRevision == 0 || len(s.Desired) == 0 {
		return nil, "idle"
	}
	for _, c := range s.Commands {
		if blocksUnknown(c) {
			return nil, "blocked_unknown"
		}
		if c.DesiredRevision == s.DesiredRevision {
			return nil, "attempt_recorded"
		}
		if c.ObservationRevision == s.DesiredRevision {
			return nil, "observation_recorded"
		}
	}
	paths := make([]string, 0, len(s.Desired))
	for path, want := range s.Desired {
		if !p.Allows(path, want, true) {
			return nil, "policy_blocked"
		}
		got, ok := s.Reported[path]
		if !ok || !a.opts.Now().Before(got.FreshUntil) || !p.Allows(path, got.Value, false) {
			paths = append(paths, path)
		}
	}
	if len(paths) == 0 {
		return nil, "observation_available"
	}
	sort.Strings(paths)
	return &usp.Request{ReqType: &usp.Request_Get{Get: &usp.Get{ParamPaths: paths, MaxDepth: 1}}}, "observation_eligible"
}

func (a *Actor) currentObservation(s *snapshot, c storedCommand) bool {
	p := a.opts.Policy
	if p == nil || !p.ReadBeforeReconcile || !p.ObserveGetResponses || !p.Reconcile || c.ObservationRevision != s.DesiredRevision {
		return false
	}
	req := new(usp.Request)
	if proto.Unmarshal(c.Payload, req) != nil || req.GetGet() == nil || len(req.GetGet().ParamPaths) == 0 {
		return false
	}
	for path, want := range s.Desired {
		if !p.Allows(path, want, true) {
			return false
		}
	}
	needed := false
	for _, path := range req.GetGet().ParamPaths {
		want, ok := s.Desired[path]
		if !ok || !p.Allows(path, want, true) {
			return false
		}
		got, ok := s.Reported[path]
		if !ok || !a.opts.Now().Before(got.FreshUntil) || !p.Allows(path, got.Value, false) {
			needed = true
		}
	}
	for _, other := range s.Commands {
		if other.DesiredRevision == s.DesiredRevision {
			return false
		}
	}
	return needed
}
