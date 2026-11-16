package deviceactor

import (
	"fmt"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
)

// Mutations may change paths beyond the requested target (notably Operate).
// Invalidate the whole twin conservatively; do not claim device-side causality.
func mutatesDevice(req *usp.Request) bool {
	return req.GetSet() != nil || req.GetAdd() != nil || req.GetDelete() != nil || req.GetOperate() != nil
}

func advanceObservationBarrier(s *snapshot, req *usp.Request, at time.Time) {
	if mutatesDevice(req) {
		if at.After(s.mutationBarrier) {
			s.mutationBarrier = at
		}
		if at.After(s.observationBarrier) {
			s.observationBarrier = at
		}
	}
}

// Rebuild from durable begin/outcome provenance on every load. This field is derived,
// never persisted as a second authority and never guessed for legacy attempts.
func rebuildObservationBarrier(s *snapshot) error {
	s.mutationBarrier = time.Time{}
	s.observationBarrier = time.Time{}
	for _, c := range s.Commands {
		if (c.AttemptSequence == 0 || c.AttemptedAt.IsZero()) && c.MutationOutcomeAt.IsZero() && c.MutationBarrierAt == nil {
			continue
		}
		req := new(usp.Request)
		if err := proto.Unmarshal(c.Payload, req); err != nil {
			return fmt.Errorf("decode attempt for observation barrier: %w", err)
		}
		advanceObservationBarrier(s, req, c.AttemptedAt)
		advanceObservationBarrier(s, req, c.MutationOutcomeAt)
		if c.MutationBarrierAt != nil {
			advanceObservationBarrier(s, req, *c.MutationBarrierAt)
		}
	}
	if s.TopologyBarrier != nil && s.TopologyBarrier.At.After(s.observationBarrier) {
		s.observationBarrier = s.TopologyBarrier.At
	}
	return nil
}

func observationFresh(s *snapshot, got device.Observation, now time.Time) bool {
	return !got.ReceivedAt.After(now) && now.Before(got.FreshUntil) && (s.observationBarrier.IsZero() || got.ReceivedAt.After(s.observationBarrier))
}

// Record definitive evidence at processing time, not a device-provided clock.
// Duplicate evidence and an Operate acceptance awaiting completion do not move it.
func (a *Actor) markMutationOutcome(s *snapshot, c *storedCommand, req *usp.Request) {
	if !mutatesDevice(req) || !c.MutationOutcomeAt.IsZero() || (c.Receipt.State != device.CommandSucceeded && c.Receipt.State != device.CommandFailed) {
		return
	}
	at := a.opts.Now().UTC()
	if at.Before(c.AttemptedAt) {
		at = c.AttemptedAt
	}
	c.MutationOutcomeAt = at
	cutoff := observationCutoff(s, at)
	c.MutationBarrierAt = &cutoff
	advanceObservationBarrier(s, req, cutoff)
}

func validateMutationOutcomes(s *snapshot, sourceSchema int) error {
	for _, c := range s.Commands {
		if c.MutationOutcomeAt.IsZero() {
			continue
		} // historical outcomes lack time
		req := new(usp.Request)
		if sourceSchema < 9 || c.MessageID == "" || c.MutationOutcomeAt.Before(c.AttemptedAt) || (c.Receipt.State != device.CommandSucceeded && c.Receipt.State != device.CommandFailed) || (len(c.Result) == 0 && len(c.Completion) == 0) || proto.Unmarshal(c.Payload, req) != nil || !mutatesDevice(req) {
			return fmt.Errorf("invalid mutation outcome provenance")
		}
	}
	return nil
}

// A cutoff must invalidate every already-known reading, even if the wall clock
// has moved backwards since that reading was received. Timestamp ties are stale.
func observationCutoff(s *snapshot, at time.Time) time.Time {
	if s.observationBarrier.After(at) {
		at = s.observationBarrier
	}
	for _, got := range s.Reported {
		if got.ReceivedAt.After(at) {
			at = got.ReceivedAt
		}
	}
	return at.UTC()
}

func validateMutationBarriers(s *snapshot, sourceSchema int) error {
	for _, c := range s.Commands {
		if c.MutationBarrierAt == nil {
			continue
		}
		req := new(usp.Request)
		if sourceSchema < 10 || c.MutationBarrierAt.IsZero() || c.MessageID == "" || (c.AttemptSequence == 0 && c.MutationOutcomeAt.IsZero()) || c.MutationBarrierAt.Before(c.AttemptedAt) || c.MutationBarrierAt.Before(c.MutationOutcomeAt) || proto.Unmarshal(c.Payload, req) != nil || !mutatesDevice(req) {
			return fmt.Errorf("invalid mutation observation cutoff")
		}
	}
	return nil
}
