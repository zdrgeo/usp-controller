package deviceactor

import (
	"fmt"
	"time"

	"example.com/usp-controller/internal/pb/usp"
)

// One bounded device-wide cutoff. Object changes and onboarding can invalidate
// dependent parameters; retain historical values but require newer observations.
type topologyBarrier struct {
	At          time.Time
	MessageID   string
	Fingerprint string
	ObjectPath  string
	Kind        string
}

func (a *Actor) markTopologyChange(s *snapshot, n *usp.Notify, mid, fp string, received time.Time) {
	path, kind := "", ""
	if created := n.GetObjCreation(); created != nil {
		path, kind = created.ObjPath, "object_creation"
	}
	if deleted := n.GetObjDeletion(); deleted != nil {
		path, kind = deleted.ObjPath, "object_deletion"
	}
	if n.GetOnBoardReq() != nil {
		// Identity comes from authenticated routing, not onboarding attributes.
		// Treat onboarding as a conservative lifecycle hint, not proof of reset.
		path, kind = "Device.", "on_board_request"
	}
	if kind == "" || !concreteObject.MatchString(path) {
		return
	}
	at := a.opts.Now().UTC()
	if received.After(at) {
		at = received.UTC()
	}
	at = observationCutoff(s, at)
	s.TopologyBarrier = &topologyBarrier{At: at, MessageID: mid, Fingerprint: fp, ObjectPath: path, Kind: kind}
	if at.After(s.observationBarrier) {
		s.observationBarrier = at
	}
}

func validateTopology(s *snapshot, sourceSchema int) error {
	b := s.TopologyBarrier
	if b == nil {
		return nil
	}
	validKind := b.Kind == "object_creation" || b.Kind == "object_deletion" ||
		(b.Kind == "on_board_request" && sourceSchema >= 11 && b.ObjectPath == "Device.")
	if sourceSchema < 10 || b.At.IsZero() || b.MessageID == "" || len(b.Fingerprint) != 64 || s.Inbox[b.MessageID] != b.Fingerprint || !concreteObject.MatchString(b.ObjectPath) || !validKind {
		return fmt.Errorf("invalid topology observation barrier")
	}
	return nil
}
