package deviceactor

import (
	"encoding/json"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
)

func topologyNotification(mid, kind, path string, at time.Time) device.DeviceNotification {
	n := &usp.Notify{SubscriptionId: "topology", SendResp: true}
	if kind == "object_creation" {
		n.Notification = &usp.Notify_ObjCreation{ObjCreation: &usp.Notify_ObjectCreation{ObjPath: path, UniqueKeys: map[string]string{"Alias": "SENSITIVE_KEY"}}}
	} else if kind == "on_board_request" {
		n.Notification = &usp.Notify_OnBoardReq{OnBoardReq: &usp.Notify_OnBoardRequest{Oui: "test-oui", SerialNumber: "test-serial"}}
	} else {
		n.Notification = &usp.Notify_ObjDeletion{ObjDeletion: &usp.Notify_ObjectDeletion{ObjPath: path}}
	}
	return notification(mid, n, at)
}

func TestTopologyReceiptAndBarrierCommitTogetherAndSurviveExport(t *testing.T) {
	for _, tc := range []struct{ kind, mode string }{{"object_creation", "normal"}, {"object_deletion", "normal"}, {"object_deletion", "save-failed"}, {"object_deletion", "ambiguous"}, {"on_board_request", "normal"}, {"on_board_request", "save-failed"}, {"on_board_request", "ambiguous"}} {
		t.Run(tc.kind+"/"+tc.mode, func(t *testing.T) {
			a, b, f, now := managedActor(t)
			observe(t, a, "initial", *now, "target")
			if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(time.Second)
			cutoff := *now
			n := topologyNotification("topology", tc.kind, "Device.Other.1.", *now)
			b.failSave = tc.mode == "save-failed"
			b.ambiguous = tc.mode == "ambiguous"
			_, err := a.HandleNotify(ctx, n)
			if (tc.mode == "normal") != (err == nil) {
				t.Fatal(err)
			}
			b.failSave = false
			if tc.mode == "save-failed" {
				twin, _ := a.GetTwin(ctx)
				if twin.Convergence != "converged" {
					t.Fatal("uncommitted notice invalidated observations", twin)
				}
				if _, err = a.HandleNotify(ctx, n); err != nil {
					t.Fatal(err)
				}
			}
			restarted, err := NewWithEvents(identity, b, a.opts, f, &eventSink{})
			if err != nil {
				t.Fatal(err)
			}
			twin, err := restarted.GetTwin(ctx)
			diff, diffErr := restarted.GetTwinDiff(ctx)
			if err != nil || diffErr != nil || twin.Convergence != "unknown" || twin.ReportedRevision != 1 || len(twin.Reported) != 1 || twin.Reported["Device.Test.Value"].Value != value || !twin.Reported["Device.Test.Value"].FreshUntil.IsZero() || diff.TopologyBarrierAt == nil || !diff.TopologyBarrierAt.Equal(cutoff) || diff.MutationBarrierAt != nil || diff.Paths[0].State != "topology_invalidated" {
				t.Fatal("topology cutoff/evidence mismatch", twin, diff, err, diffErr)
			}
			events, err := restarted.ListEvents(ctx, device.EventQuery{})
			if err != nil || len(events.Events) != 1 || events.Events[0].Kind != tc.kind {
				t.Fatal(events, err)
			}
			if err = restarted.Tick(ctx); err != nil || len(f.calls) != 0 {
				t.Fatal("notice triggered command or export failed", err)
			}
			state, err := restarted.load(ctx)
			if err != nil || len(state.Outbox) != 0 || state.TopologyBarrier == nil || state.Inbox["topology"] != state.TopologyBarrier.Fingerprint {
				t.Fatal("export removed cutoff provenance", state, err)
			}
			*now = now.Add(time.Second)
			n.ReceivedAt = *now
			saves := b.saves
			receipt, err := restarted.HandleNotify(ctx, n)
			if err != nil || !receipt.Duplicate || b.saves != saves {
				t.Fatal("notice retry reapplied invalidation", receipt, err)
			}
			diff, _ = restarted.GetTwinDiff(ctx)
			if !diff.TopologyBarrierAt.Equal(cutoff) {
				t.Fatal("duplicate moved topology cutoff", diff)
			}
		})
	}
}

func TestTopologyChangeFencesEarlierGetButAllowsLaterRefresh(t *testing.T) {
	for _, kind := range []string{"object_deletion", "on_board_request"} {
		t.Run(kind, func(t *testing.T) {
			a, _, _, now := managedActor(t)
			a.opts.Policy.ObserveGetResponses = true
			observe(t, a, "initial", *now, "target")
			if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(time.Second)
			if _, err := a.EnqueueCommand(ctx, command("before", now.Add(time.Hour))); err != nil {
				t.Fatal(err)
			}
			if err := a.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			before, _ := a.GetCommand(ctx, "before")
			*now = now.Add(time.Second)
			if _, err := a.HandleNotify(ctx, topologyNotification("deleted", kind, "Device.Test.1.", *now)); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(time.Second)
			response := func(mid string) device.DeviceResponse {
				return device.DeviceResponse{Identity: identity, Message: getObservationResponse(mid, getPathResult("Device.Test.", "Device.Test.", map[string]string{"Value": "target"})), ReceivedAt: *now}
			}
			if err := a.HandleResponse(ctx, response(before.MessageID)); err != nil {
				t.Fatal(err)
			}
			twin, _ := a.GetTwin(ctx)
			before, _ = a.GetCommand(ctx, "before")
			if twin.Convergence != "unknown" || twin.ReportedRevision != 1 || before.State != device.CommandSucceeded || before.Result == nil {
				t.Fatal("old Get crossed topology cutoff or lost evidence", twin, before)
			}
			*now = now.Add(time.Second)
			if _, err := a.EnqueueCommand(ctx, command("after", now.Add(time.Hour))); err != nil {
				t.Fatal(err)
			}
			if err := a.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			after, _ := a.GetCommand(ctx, "after")
			*now = now.Add(time.Second)
			if err := a.HandleResponse(ctx, response(after.MessageID)); err != nil {
				t.Fatal(err)
			}
			twin, _ = a.GetTwin(ctx)
			if twin.Convergence != "converged" || twin.ReportedRevision != 2 {
				t.Fatal("later read could not refresh twin", twin)
			}
			// A later notice under a backwards clock must invalidate the known newer reading.
			newest := *now
			*now = now.Add(-4 * time.Second)
			if _, err := a.HandleNotify(ctx, topologyNotification("created", kind, "Device.Other.1.", *now)); err != nil {
				t.Fatal(err)
			}
			*now = newest.Add(time.Second)
			diff, err := a.GetTwinDiff(ctx)
			if err != nil || diff.Convergence != "unknown" || diff.Paths[0].State != "topology_invalidated" || !diff.TopologyBarrierAt.Equal(newest) {
				t.Fatal("clock recovery resurrected pre-notice data", diff, err)
			}
		})
	}
}

func TestTopologyProvenanceValidationAndLegacyUpgrade(t *testing.T) {
	a, b, _, now := managedActor(t)
	if _, err := a.HandleNotify(ctx, topologyNotification("notice", "object_deletion", "Device.Test.1.", *now)); err != nil {
		t.Fatal(err)
	}
	a.publisher = &eventSink{}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), b.state...)
	for _, mode := range []string{"downgrade", "missing-receipt"} {
		t.Run(mode, func(t *testing.T) {
			var prior snapshot
			if err := json.Unmarshal(original, &prior); err != nil {
				t.Fatal(err)
			}
			if mode == "downgrade" {
				prior.Schema = 9
			} else {
				delete(prior.Inbox, "notice")
			}
			b.state, _ = json.Marshal(prior)
			if _, err := a.GetTwin(ctx); err == nil {
				t.Fatal("broken topology provenance accepted")
			}
		})
	}
	var legacy snapshot
	if err := json.Unmarshal(original, &legacy); err != nil {
		t.Fatal(err)
	}
	legacy.Schema = 9
	legacy.TopologyBarrier = nil
	b.state, _ = json.Marshal(legacy)
	upgraded, err := a.load(ctx)
	if err != nil || upgraded.Schema != 11 || upgraded.TopologyBarrier != nil || len(upgraded.Inbox) != 1 {
		t.Fatal("legacy upgrade invented cutoff or lost receipt", upgraded, err)
	}
	b.state = original
	// Unsupported object selectors remain durable events without trusted projection.
	before := a.opts.Now()
	if _, err = a.HandleNotify(ctx, topologyNotification("unsupported", "object_deletion", "Device.Test.*.", *now)); err != nil {
		t.Fatal(err)
	}
	loaded, err := a.load(ctx)
	if err != nil || loaded.TopologyBarrier.MessageID != "notice" || !loaded.TopologyBarrier.At.Equal(before) {
		t.Fatal("unsupported selector changed cutoff", loaded, err)
	}
}

func TestOnboardingCutoffSchemaCompatibility(t *testing.T) {
	a, b, _, now := managedActor(t)
	if _, err := a.HandleNotify(ctx, topologyNotification("onboard", "on_board_request", "", *now)); err != nil {
		t.Fatal(err)
	}
	a.publisher = &eventSink{}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), b.state...)
	for _, mode := range []string{"schema-10-marker", "non-root-path", "missing-receipt"} {
		t.Run(mode, func(t *testing.T) {
			var s snapshot
			if err := json.Unmarshal(original, &s); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "schema-10-marker":
				s.Schema = 10
			case "non-root-path":
				s.TopologyBarrier.ObjectPath = "Device.Test."
			case "missing-receipt":
				delete(s.Inbox, "onboard")
			}
			b.state, _ = json.Marshal(s)
			if _, err := a.load(ctx); err == nil {
				t.Fatal("inconsistent onboarding cutoff accepted")
			}
		})
	}
	var legacy snapshot
	if err := json.Unmarshal(original, &legacy); err != nil {
		t.Fatal(err)
	}
	legacy.Schema = 10
	legacy.TopologyBarrier = nil // Previously exported onboarding events had no cutoff.
	b.state, _ = json.Marshal(legacy)
	s, err := a.load(ctx)
	if err != nil || s.Schema != 11 || s.TopologyBarrier != nil || len(s.Inbox) != 1 {
		t.Fatal("schema-10 upgrade invented lifecycle evidence", s, err)
	}
	if r, err := a.HandleNotify(ctx, topologyNotification("onboard", "on_board_request", "", *now)); err != nil || !r.Duplicate {
		t.Fatal("legacy receipt was not recovered", r, err)
	}
	s, err = a.load(ctx)
	if err != nil || s.TopologyBarrier != nil {
		t.Fatal("old duplicate invented a cutoff", s, err)
	}
	// Schema-10 object cutoffs remain valid and retain their exact provenance.
	if _, err := a.HandleNotify(ctx, topologyNotification("object", "object_deletion", "Device.Test.", *now)); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b.state, &legacy); err != nil {
		t.Fatal(err)
	}
	legacy.Schema = 10
	b.state, _ = json.Marshal(legacy)
	s, err = a.load(ctx)
	if err != nil || s.Schema != 11 || s.TopologyBarrier.Kind != "object_deletion" || !s.TopologyBarrier.At.Equal(*now) {
		t.Fatal("schema-10 object cutoff was not preserved", s, err)
	}
}
