package deviceactor

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
)

var concreteObject = regexp.MustCompile(`^Device\.(?:[A-Za-z0-9_]+\.)*$`)
var parameterName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validateAttempts(s *snapshot, sourceSchema int) error {
	if sourceSchema < 6 && s.AttemptSequence != 0 {
		return fmt.Errorf("attempt metadata in an older schema")
	}
	seen := map[uint64]bool{}
	for _, c := range s.Commands {
		if c.AttemptSequence == 0 {
			if !c.AttemptedAt.IsZero() {
				return fmt.Errorf("attempt time without sequence")
			}
			continue
		}
		if sourceSchema < 6 || c.AttemptedAt.IsZero() || c.MessageID == "" || c.AttemptSequence > s.AttemptSequence || seen[c.AttemptSequence] {
			return fmt.Errorf("inconsistent attempt provenance")
		}
		switch c.Receipt.State {
		case device.CommandAttempting, device.CommandAwaitingResponse, device.CommandAwaitingCompletion, device.CommandUnknown, device.CommandSucceeded, device.CommandFailed:
		default:
			return fmt.Errorf("attempt provenance on unsent command")
		}
		seen[c.AttemptSequence] = true
	}
	if uint64(len(seen)) != s.AttemptSequence {
		return fmt.Errorf("incomplete attempt history")
	}
	return nil
}

func requestedGetPath(get *usp.Get, requested, resolved, path string) bool {
	found := false
	for _, entry := range get.ParamPaths {
		if entry == requested {
			found = true
			break
		}
	}
	if !found {
		return false
	}
	if requested == path {
		return true
	} // Exact managed parameter, never a search path.
	if !concreteObject.MatchString(requested) || !strings.HasPrefix(resolved, requested) {
		return false
	}
	depth := uint32(strings.Count(strings.TrimPrefix(resolved, requested), ".") + 1)
	return get.MaxDepth == 0 || depth <= get.MaxDepth
}

// Projection is conservative and optional; complete protocol evidence remains on
// the command even when result paths/types/timing cannot safely update the twin.
func (a *Actor) projectGet(s *snapshot, c storedCommand, req *usp.Request, r device.DeviceResponse) {
	policy := a.opts.Policy
	get := req.GetGet()
	response := r.Message.GetBody().GetResponse().GetGetResp()
	if policy == nil || !policy.ObserveGetResponses || get == nil || response == nil {
		return
	}
	now := a.opts.Now()
	if r.ReceivedAt.IsZero() || r.ReceivedAt.After(now) {
		return
	}
	if r.ReceivedAt.After(s.LastContact) {
		s.LastContact = r.ReceivedAt.UTC()
	}
	if (!s.observationBarrier.IsZero() && !c.AttemptedAt.After(s.observationBarrier)) || c.AttemptSequence == 0 || c.AttemptSequence != s.AttemptSequence || c.AttemptedAt.IsZero() || r.ReceivedAt.Before(c.AttemptedAt) || !now.Before(c.AttemptedAt.Add(a.opts.Freshness)) {
		return
	}
	values := map[string]device.ParameterValue{}
	conflicts := map[string]bool{}
	for _, result := range response.ReqPathResults {
		if result.GetErrCode() != 0 || result.GetErrMsg() != "" {
			continue
		}
		for _, object := range result.GetResolvedPathResults() {
			if !concreteObject.MatchString(object.GetResolvedPath()) {
				continue
			}
			for name, value := range object.GetResultParams() {
				if !parameterName.MatchString(name) {
					continue
				}
				path := object.ResolvedPath + name
				if _, managed := policy.Paths[path]; !managed || !requestedGetPath(get, result.RequestedPath, object.ResolvedPath, path) {
					continue
				}
				// Detect contradictions before type/value validation can discard a
				// conflicting candidate. Only requested managed paths participate.
				observed := a.observedValue(path, value)
				if prior, ok := values[path]; ok && prior != observed {
					conflicts[path] = true
				}
				values[path] = observed
			}
		}
	}
	projected := make(map[string]device.Observation, len(s.Reported)+len(values))
	for path, old := range s.Reported {
		projected[path] = old
	}
	changed := false
	for path, value := range values {
		if conflicts[path] || !policy.Allows(path, value, false) {
			continue
		}
		if old, ok := s.Reported[path]; ok && !old.ReceivedAt.Before(c.AttemptedAt) {
			continue
		}
		projected[path] = device.Observation{Value: value, ReceivedAt: r.ReceivedAt.UTC(), FreshUntil: c.AttemptedAt.Add(a.opts.Freshness), Source: "usp-get-response"}
		changed = true
	}
	if changed && len(projected) <= a.opts.MaxParameters && s.ReportedRevision < math.MaxUint64 {
		s.Reported = projected
		s.ReportedRevision++
	}
}

// All fallback decisions are made locally before the single backend Save. Never
// retry a possibly committed backend operation to recover optional twin growth.
func (a *Actor) saveGetResponse(ctx context.Context, s *snapshot, c storedCommand, req *usp.Request, r device.DeviceResponse) error {
	reported, revision, contact := s.Reported, s.ReportedRevision, s.LastContact
	a.projectGet(s, c, req, r)
	data, err := a.encodeSnapshot(s, a.opts.MaxBytes)
	if errors.Is(err, ErrCapacity) && (s.ReportedRevision != revision || !s.LastContact.Equal(contact)) {
		s.Reported = reported
		s.ReportedRevision = revision
		s.LastContact = contact
		data, err = a.encodeSnapshot(s, a.opts.MaxBytes)
	}
	if err != nil {
		return err
	}
	return a.backend.Save(ctx, data)
}
