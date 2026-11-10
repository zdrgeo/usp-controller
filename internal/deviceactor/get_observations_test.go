package deviceactor

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
)

func getObservationResponse(mid string, results ...*usp.GetResp_RequestedPathResult) *usp.Msg {
	msg := response(mid)
	msg.GetBody().GetResponse().GetGetResp().ReqPathResults = results
	return msg
}
func getPathResult(requested, object string, values map[string]string) *usp.GetResp_RequestedPathResult {
	return &usp.GetResp_RequestedPathResult{RequestedPath: requested, ResolvedPathResults: []*usp.GetResp_ResolvedPathResult{{ResolvedPath: object, ResultParams: values}}}
}

func TestGetProjectionScopesPathsAndCommitsAtomically(t *testing.T) {
	a, b, _, now := managedActor(t)
	a.opts.Policy.ObserveGetResponses = true
	a.opts.Policy.Paths["Device.Test.Flag"] = device.ManagedPath{Type: "boolean"}
	a.opts.Policy.Paths["Device.Test.Child.Value"] = device.ManagedPath{Type: "string"}
	a.opts.Policy.Paths["Device.Other.Value"] = device.ManagedPath{Type: "string"}
	if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
		t.Fatal(err)
	}
	request := command("read", now.Add(time.Hour))
	request.Request.GetGet().MaxDepth = 1
	if _, err := a.EnqueueCommand(ctx, request); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	c, _ := a.GetCommand(ctx, "read")
	if c.AttemptSequence != 1 || c.AttemptedAt.IsZero() {
		t.Fatal("missing durable read provenance", c)
	}
	*now = now.Add(time.Second)
	failed := getPathResult("Device.Test.", "Device.Test.", map[string]string{"Value": "bad"})
	failed.ErrCode = 7000
	msg := getObservationResponse(c.MessageID,
		getPathResult("Device.Test.", "Device.Test.", map[string]string{"Value": "target", "ReadOnly": "observed", "Flag": "not-a-boolean", "Private": "secret"}),
		getPathResult("Device.Test.", "Device.Test.Child.", map[string]string{"Value": "too-deep"}),
		getPathResult("Device.Other.", "Device.Other.", map[string]string{"Value": "not-requested"}), failed)
	incoming := device.DeviceResponse{Identity: identity, Message: msg, ReceivedAt: *now}
	b.failSave = true
	if err := a.HandleResponse(ctx, incoming); err == nil {
		t.Fatal("uncommitted result accepted")
	}
	b.failSave = false
	twin, err := a.GetTwin(ctx)
	if err != nil || twin.ReportedRevision != 0 || len(twin.Reported) != 0 {
		t.Fatal("projection escaped failed commit", twin, err)
	}
	b.ambiguous = true
	saves := b.saves
	if err := a.HandleResponse(ctx, incoming); err == nil {
		t.Fatal("lost commit reply claimed success")
	}
	a, err = NewExecuting(identity, b, a.opts, &fakeSender{ready: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.HandleResponse(ctx, incoming); err != nil || b.saves != saves+1 {
		t.Fatal("ambiguous result was written twice", err)
	}
	twin, err = a.GetTwin(ctx)
	if err != nil || twin.ReportedRevision != 1 || len(twin.Reported) != 2 || twin.Reported["Device.Test.Value"].Value.Value != "target" || twin.Reported["Device.Test.ReadOnly"].Value.Value != "observed" {
		t.Fatal("unsafe result projection", twin, err)
	}
	observed := twin.Reported["Device.Test.Value"]
	if observed.Source != "usp-get-response" || !observed.ReceivedAt.Equal(*now) || !observed.FreshUntil.Equal(c.AttemptedAt.Add(a.opts.Freshness)) {
		t.Fatal("arrival time inflated read freshness", observed)
	}
	settled, err := a.GetCommand(ctx, "read")
	if err != nil || settled.State != device.CommandFailed || settled.Result == nil {
		t.Fatal("partial response outcome/evidence lost", settled, err)
	}
	*now = now.Add(time.Minute)
	incoming.ReceivedAt = *now
	if err := a.HandleResponse(ctx, incoming); err != nil {
		t.Fatal(err)
	}
	after, _ := a.GetTwin(ctx)
	if after.ReportedRevision != 1 || !after.Reported["Device.Test.Value"].FreshUntil.Equal(observed.FreshUntil) {
		t.Fatal("duplicate refreshed observations")
	}
}

func TestGetProjectionRejectsUnsafeObservationWindows(t *testing.T) {
	for _, mode := range []string{"disabled", "new-observation", "new-attempt", "late-window", "legacy", "ambiguous-path"} {
		t.Run(mode, func(t *testing.T) {
			a, b, f, now := managedActor(t)
			a.opts.Policy.ObserveGetResponses = mode != "disabled"
			request := command("old-read", now.Add(time.Hour))
			if _, err := a.EnqueueCommand(ctx, request); err != nil {
				t.Fatal(err)
			}
			if mode == "new-attempt" {
				f.err = errors.New("uncertain old read")
			}
			if err := a.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			f.err = nil
			c, _ := a.GetCommand(ctx, "old-read")
			want := ""
			switch mode {
			case "new-observation":
				*now = now.Add(time.Second)
				observe(t, a, "newer", *now, "newer")
				want = "newer"
			case "new-attempt":
				if _, err := a.AcknowledgeUnknown(ctx, device.AcknowledgeUnknownRequest{ID: c.ID, RequestID: "review", ExpectedRevision: c.Revision, Reason: "reviewed"}); err != nil {
					t.Fatal(err)
				}
				*now = now.Add(time.Second)
				if _, err := a.EnqueueCommand(ctx, command("new-read", now.Add(time.Hour))); err != nil {
					t.Fatal(err)
				}
				if err := a.Tick(ctx); err != nil {
					t.Fatal(err)
				}
			case "late-window":
				*now = now.Add(6 * time.Minute)
			case "legacy":
				var prior snapshot
				json.Unmarshal(b.state, &prior)
				prior.Schema = 5
				prior.AttemptSequence = 0
				for id, stored := range prior.Commands {
					stored.AttemptSequence = 0
					stored.AttemptedAt = time.Time{}
					prior.Commands[id] = stored
				}
				b.state, _ = json.Marshal(prior)
			}
			*now = now.Add(time.Second)
			results := []*usp.GetResp_RequestedPathResult{getPathResult("Device.Test.", "Device.Test.", map[string]string{"Value": "old"})}
			if mode == "ambiguous-path" {
				results = append(results, getPathResult("Device.Test.", "Device.Test.", map[string]string{"Value": "different"}))
			}
			if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: getObservationResponse(c.MessageID, results...), ReceivedAt: *now}); err != nil {
				t.Fatal(err)
			}
			twin, err := a.GetTwin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if want == "" {
				if len(twin.Reported) != 0 || twin.ReportedRevision != 0 {
					t.Fatal("unsafe evidence promoted", twin)
				}
			} else if twin.Reported["Device.Test.Value"].Value.Value != want || twin.ReportedRevision != 1 {
				t.Fatal("older read overwrote newer observation", twin)
			}
			settled, err := a.GetCommand(ctx, c.ID)
			if err != nil || settled.State != device.CommandSucceeded || settled.Result == nil {
				t.Fatal("skipped projection discarded response evidence", settled, err)
			}
		})
	}
}

func TestGetProjectionCapacityFallsBackBeforeOneBackendSave(t *testing.T) {
	a, b, _, now := managedActor(t)
	a.opts.Policy.ObserveGetResponses = true
	if _, err := a.EnqueueCommand(ctx, command("read", now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	c, _ := a.GetCommand(ctx, "read")
	*now = now.Add(time.Second)
	a.opts.MaxBytes = len(b.state) + 1100
	msg := getObservationResponse(c.MessageID, getPathResult("Device.Test.", "Device.Test.", map[string]string{"Value": strings.Repeat("x", 500)}))
	incoming := device.DeviceResponse{Identity: identity, Message: msg, ReceivedAt: *now}
	saves := b.saves
	if err := a.HandleResponse(ctx, incoming); err != nil || b.saves != saves+1 {
		t.Fatal("optional projection prevented evidence or retried save", err)
	}
	twin, err := a.GetTwin(ctx)
	if err != nil || len(twin.Reported) != 0 || twin.ReportedRevision != 0 {
		t.Fatal("fixture did not fall back atomically", twin, err)
	}
	c, err = a.GetCommand(ctx, "read")
	if err != nil || c.State != device.CommandSucceeded || c.Result == nil {
		t.Fatal(c, err)
	}
	a.opts.MaxBytes += 4096
	if err := a.HandleResponse(ctx, incoming); err != nil || b.saves != saves+1 {
		t.Fatal("duplicate reprojected after capacity recovered", err)
	}
}

func TestDeadlineRecheckedAfterExportBeforeAttempt(t *testing.T) {
	a, b, f, now := executable(t)
	budget, err := device.NewAttemptBudget(1)
	if err != nil {
		t.Fatal(err)
	}
	a.opts.AttemptBudget = budget
	sink := &eventSink{after: func() { *now = now.Add(2 * time.Minute) }}
	a.publisher = sink
	if _, err := a.EnqueueCommand(ctx, command("expires-during-export", now.Add(time.Minute))); err != nil {
		t.Fatal(err)
	}
	if _, err := a.HandleNotify(ctx, valueNotification("event", *now)); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	c, err := a.GetCommand(ctx, "expires-during-export")
	if err != nil || c.State != device.CommandExpired || c.MessageID != "" || c.AttemptSequence != 0 || !c.AttemptedAt.IsZero() || len(f.calls) != 0 || budget.InUse() != 0 {
		t.Fatal("expired queued command crossed begin boundary", c, err)
	}
	var persisted snapshot
	json.Unmarshal(b.state, &persisted)
	if persisted.AttemptSequence != 0 {
		t.Fatal("expired work consumed an attempt sequence")
	}
}

func TestAttemptProvenanceRejectsInconsistentSnapshots(t *testing.T) {
	a, b, _, now := managedActor(t)
	if _, err := a.EnqueueCommand(ctx, command("read", now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), b.state...)
	for _, mode := range []string{"downgrade", "missing-time", "missing-sequence", "history-gap", "queued-attempt"} {
		t.Run(mode, func(t *testing.T) {
			var s snapshot
			if err := json.Unmarshal(original, &s); err != nil {
				t.Fatal(err)
			}
			c := s.Commands["read"]
			switch mode {
			case "downgrade":
				s.Schema = 5
			case "missing-time":
				c.AttemptedAt = time.Time{}
			case "missing-sequence":
				c.AttemptSequence = 0
			case "history-gap":
				s.AttemptSequence++
			case "queued-attempt":
				c.Receipt.State = device.CommandQueued
			}
			s.Commands["read"] = c
			b.state, _ = json.Marshal(s)
			if _, err := a.GetTwin(ctx); err == nil {
				t.Fatal("inconsistent attempt provenance accepted")
			}
		})
	}
}
