package deviceactor

import (
	"fmt"

	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
)

// The observation window is pinned by durable Get begin, not reconstructed from
// a later host's configuration. Legacy attempts remain absent on schema upgrade.
func validateObservationWindows(s *snapshot, sourceSchema int) error {
	for _, c := range s.Commands {
		if c.ObservationFreshUntil == nil {
			continue
		}
		req := new(usp.Request)
		if sourceSchema < 12 || c.AttemptSequence == 0 || c.AttemptedAt.IsZero() || c.MessageID == "" || !c.ObservationFreshUntil.After(c.AttemptedAt) || proto.Unmarshal(c.Payload, req) != nil || req.GetGet() == nil {
			return fmt.Errorf("invalid Get observation window provenance")
		}
	}
	return nil
}
