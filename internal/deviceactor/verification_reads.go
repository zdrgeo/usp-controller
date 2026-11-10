package deviceactor

import (
	"fmt"
	"sort"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
)

// Verification is one observation opportunity, not permission to retry a write.
func (a *Actor) verificationPaths(s *snapshot) ([]string, string) {
	p := a.opts.Policy
	if p == nil || !p.VerifyAfterReconcile || !p.Reconcile || !p.ObserveGetResponses {
		return nil, "attempt_recorded"
	}
	if s.DesiredRevision == 0 || len(s.Desired) == 0 {
		return nil, "idle"
	}
	for _, c := range s.Commands {
		if blocksUnknown(c) {
			return nil, "blocked_unknown"
		}
	}
	write, ok := s.Commands[device.RequestID(fmt.Sprintf("reconcile-%d", s.DesiredRevision))]
	if !ok || write.DesiredRevision != s.DesiredRevision || write.Receipt.State != device.CommandSucceeded || write.AttemptSequence == 0 {
		return nil, "attempt_recorded"
	}
	paths := make([]string, 0, len(s.Desired))
	for path, want := range s.Desired {
		if !p.Allows(path, want, true) {
			return nil, "policy_blocked"
		}
		paths = append(paths, path)
	}
	if a.convergence(s) == "converged" {
		return nil, "converged"
	}
	sort.Strings(paths)
	return paths, "verification_eligible"
}

func (a *Actor) planVerification(s *snapshot) (*usp.Request, string) {
	if s.Execution.Paused {
		return nil, "paused"
	}
	paths, reason := a.verificationPaths(s)
	if paths == nil {
		return nil, reason
	}
	for _, c := range s.Commands {
		if c.VerificationRevision == s.DesiredRevision {
			return nil, "verification_recorded"
		}
	}
	return &usp.Request{ReqType: &usp.Request_Get{Get: &usp.Get{ParamPaths: paths, MaxDepth: 1}}}, reason
}

func (a *Actor) currentVerification(s *snapshot, c storedCommand) bool {
	if c.VerificationRevision != s.DesiredRevision {
		return false
	}
	paths, _ := a.verificationPaths(s)
	if paths == nil {
		return false
	}
	req := new(usp.Request)
	if proto.Unmarshal(c.Payload, req) != nil || req.GetGet() == nil {
		return false
	}
	expected := &usp.Request{ReqType: &usp.Request_Get{Get: &usp.Get{ParamPaths: paths, MaxDepth: 1}}}
	return proto.Equal(req, expected)
}
