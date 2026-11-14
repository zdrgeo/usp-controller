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
	if mutatesDevice(req) && at.After(s.observationBarrier) {
		s.observationBarrier = at
	}
}

// Rebuild from durable begin provenance on every load. This field is derived,
// never persisted as a second authority and never guessed for legacy attempts.
func rebuildObservationBarrier(s *snapshot) error {
	for _, c := range s.Commands {
		if c.AttemptSequence == 0 || c.AttemptedAt.IsZero() {
			continue
		}
		req := new(usp.Request)
		if err := proto.Unmarshal(c.Payload, req); err != nil {
			return fmt.Errorf("decode attempt for observation barrier: %w", err)
		}
		advanceObservationBarrier(s, req, c.AttemptedAt)
	}
	return nil
}

func observationFresh(s *snapshot, got device.Observation, now time.Time) bool {
	return now.Before(got.FreshUntil) && (s.observationBarrier.IsZero() || got.ReceivedAt.After(s.observationBarrier))
}
