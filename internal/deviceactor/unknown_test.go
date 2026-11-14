package deviceactor

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

// Validate the high-risk boundary: only a durable explicit acknowledgement can
// release dispatch, and retries/restarts must never replay the uncertain attempt.
func TestUnknownAcknowledgementDurabilityAndNoReplay(t *testing.T) {
	for _, mode := range []string{"commit", "save-fails", "reply-lost"} {
		t.Run(mode, func(t *testing.T) {
			a, b, f, now := executable(t)
			for _, id := range []device.RequestID{"one", "two"} {
				if _, err := a.EnqueueCommand(ctx, command(id, now.Add(time.Hour))); err != nil {
					t.Fatal(err)
				}
			}
			// Existing schema-3 queues upgrade without losing accepted commands.
			if mode == "commit" {
				var prior snapshot
				if err := json.Unmarshal(b.state, &prior); err != nil {
					t.Fatal(err)
				}
				prior.Schema = 3
				prior.Execution = nil
				b.state, _ = json.Marshal(prior)
			}
			f.err = errors.New("publish uncertain")
			if err := a.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			f.err = nil
			unknown, _ := a.GetCommand(ctx, "one")
			request := device.AcknowledgeUnknownRequest{ID: "one", RequestID: "review-1", ExpectedRevision: unknown.Revision, Reason: " independently reviewed; allow subsequent work "}
			stale := request
			stale.ExpectedRevision--
			if _, err := a.AcknowledgeUnknown(ctx, stale); !errors.Is(err, ErrConflict) {
				t.Fatal("stale operator decision accepted", err)
			}
			b.failSave = mode == "save-fails"
			b.ambiguous = mode == "reply-lost"
			_, err := a.AcknowledgeUnknown(ctx, request)
			if (mode == "commit") != (err == nil) {
				t.Fatalf("commit result: %v", err)
			}
			b.failSave = false
			a, err = NewExecuting(identity, b, a.opts, f)
			if err != nil {
				t.Fatal(err)
			}
			twin, err := a.GetTwin(ctx)
			if err != nil || len(twin.Unknown) != 1 {
				t.Fatal(twin, err)
			}
			wantBlocking := 0
			if mode == "save-fails" {
				wantBlocking = 1
			}
			if len(twin.BlockingUnknown) != wantBlocking {
				t.Fatal("barrier not governed by durable commit", twin)
			}
			if err := a.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			if mode == "save-fails" && len(f.calls) != 1 {
				t.Fatal("failed acknowledgement released dispatch")
			}
			acknowledged, err := a.AcknowledgeUnknown(ctx, request)
			if err != nil {
				t.Fatal(err)
			}
			if acknowledged.State != device.CommandUnknown || acknowledged.MessageID != unknown.MessageID || acknowledged.UnknownAcknowledgement == nil || acknowledged.UnknownAcknowledgement.Revision != unknown.Revision+1 || acknowledged.UnknownAcknowledgement.Reason != "independently reviewed; allow subsequent work" {
				t.Fatal("fabricated outcome or lost audit", acknowledged)
			}
			saves := b.saves
			if _, err := a.AcknowledgeUnknown(ctx, request); err != nil || b.saves != saves {
				t.Fatal("duplicate decision wrote again", err)
			}
			changed := request
			changed.Reason = "different decision"
			if _, err := a.AcknowledgeUnknown(ctx, changed); !errors.Is(err, ErrConflict) {
				t.Fatal("changed retry accepted", err)
			}
			if err := a.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			next, _ := a.GetCommand(ctx, "two")
			if len(f.calls) != 2 || f.calls[1].MessageID != next.MessageID || next.MessageID == unknown.MessageID {
				t.Fatal("uncertain command replayed or queue not resumed")
			}
			if _, err := a.EnqueueCommand(ctx, command("one", now.Add(time.Hour))); err != nil {
				t.Fatal(err)
			}
			if err := a.Tick(ctx); err != nil || len(f.calls) != 2 {
				t.Fatal("original submission was requeued", err)
			}
			if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: response(unknown.MessageID)}); err != nil {
				t.Fatal(err)
			}
			settled, err := a.AcknowledgeUnknown(ctx, request)
			if err != nil || settled.State != device.CommandSucceeded || settled.UnknownAcknowledgement == nil || settled.UnknownAcknowledgement.Revision != unknown.Revision+1 {
				t.Fatal("late evidence lost acknowledgement", settled, err)
			}
			status, err := a.GetStatus(ctx)
			if err != nil || status.BlockingUnknown != 0 {
				t.Fatal(status, err)
			}
			// Metadata cannot be smuggled into a pre-acknowledgement schema.
			state := map[string]json.RawMessage{}
			json.Unmarshal(b.state, &state)
			if string(state["Schema"]) != "9" {
				t.Fatal("new semantics not versioned")
			}
			state["Schema"] = json.RawMessage(`3`)
			delete(state, "Execution")
			b.state, _ = json.Marshal(state)
			if _, err := a.GetCommand(ctx, "one"); err == nil {
				t.Fatal("schema downgrade bypassed acknowledgement validation")
			}
		})
	}
}

func TestAcknowledgedReconciliationRequiresNewDesiredRevision(t *testing.T) {
	a, _, f, now := managedActor(t)
	observe(t, a, "observed", *now, "old")
	if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
		t.Fatal(err)
	}
	f.err = errors.New("uncertain generated write")
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	f.err = nil
	old, _ := a.GetCommand(ctx, "reconcile-1")
	if _, err := a.AcknowledgeUnknown(ctx, device.AcknowledgeUnknownRequest{ID: old.ID, RequestID: "review", ExpectedRevision: old.Revision, Reason: "reviewed device state"}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := a.Tick(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(f.calls) != 1 {
		t.Fatal("acknowledgement retried the same desired revision")
	}
	if _, err := a.SetDesired(ctx, desired("new-authorization", 1)); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil || len(f.calls) != 1 {
		t.Fatal("new revision reused pre-mutation evidence", err)
	}
	*now = now.Add(time.Second)
	observe(t, a, "post-mutation", *now, "old")
	if err := a.Tick(ctx); err != nil || len(f.calls) != 2 {
		t.Fatal("new desired revision did not authorize work", err)
	}
	current, err := a.GetCommand(ctx, "reconcile-2")
	if err != nil || current.MessageID == old.MessageID {
		t.Fatal(current, err)
	}
	status, err := a.GetStatus(ctx)
	if err != nil || status.BlockingUnknown != 0 || status.CommandStates[device.CommandUnknown] != 1 {
		t.Fatal("unknown history was hidden", status, err)
	}
}
