package deviceactor

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
)

func TestMutationOutcomeBarrierCommitsWithEvidenceAndDoesNotRefreshOnRetry(t *testing.T) {
	for _, mode := range []string{"success", "failure", "save-failed", "ambiguous"} {
		t.Run(mode, func(t *testing.T) {
			a, b, _, now := managedActor(t)
			observe(t, a, "initial", *now, "target")
			if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
				t.Fatal(err)
			}
			*now = now.Add(time.Second)
			if _, err := a.EnqueueCommand(ctx, mutationRequest("mutation", now.Add(time.Hour))); err != nil {
				t.Fatal(err)
			}
			if err := a.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			c, _ := a.GetCommand(ctx, "mutation")
			*now = now.Add(time.Second)
			observe(t, a, "during", *now, "target")
			twin, _ := a.GetTwin(ctx)
			if twin.Convergence != "converged" {
				t.Fatal(twin)
			}
			*now = now.Add(time.Second)
			at := *now
			msg := setResponse(c.MessageID)
			if mode == "failure" {
				msg = &usp.Msg{Header: &usp.Header{MsgId: c.MessageID, MsgType: usp.Header_ERROR}, Body: &usp.Body{MsgBody: &usp.Body_Error{Error: &usp.Error{ErrCode: 7000, ErrMsg: "partial effects unknown"}}}}
			}
			b.failSave = mode == "save-failed"
			b.ambiguous = mode == "ambiguous"
			err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: msg})
			if (mode == "save-failed" || mode == "ambiguous") != (err != nil) {
				t.Fatal(err)
			}
			b.failSave = false
			if mode == "save-failed" {
				twin, _ = a.GetTwin(ctx)
				if twin.Convergence != "converged" {
					t.Fatal("uncommitted outcome changed barrier", twin)
				}
				if err = a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: msg}); err != nil {
					t.Fatal(err)
				}
			}
			restarted, err := NewExecuting(identity, b, a.opts, &fakeSender{ready: true})
			if err != nil {
				t.Fatal(err)
			}
			twin, _ = restarted.GetTwin(ctx)
			c, err = restarted.GetCommand(ctx, "mutation")
			if err != nil || twin.Convergence != "unknown" || !c.MutationOutcomeAt.Equal(at) {
				t.Fatal("outcome did not invalidate in-flight observation", twin, c, err)
			}
			page, err := restarted.ListCommands(ctx, device.CommandQuery{})
			out, wireErr := commandOutput(c)
			if err != nil || wireErr != nil || len(page.Commands) != 1 || page.Commands[0].MutationOutcomeAt == nil || !page.Commands[0].MutationOutcomeAt.Equal(at) || out.MutationOutcomeAt == nil || !out.MutationOutcomeAt.Equal(at) {
				t.Fatal("outcome provenance not inspectable", page, out, err, wireErr)
			}
			*now = now.Add(time.Second)
			observe(t, restarted, "after", *now, "target")
			*now = now.Add(time.Second)
			saves := b.saves
			if err = restarted.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: msg}); err != nil || b.saves != saves {
				t.Fatal("duplicate outcome written", err)
			}
			twin, _ = restarted.GetTwin(ctx)
			if twin.Convergence != "converged" {
				t.Fatal("duplicate moved outcome barrier", twin)
			}
			if mode == "success" {
				original := append([]byte(nil), b.state...)
				var prior snapshot
				if err = json.Unmarshal(original, &prior); err != nil {
					t.Fatal(err)
				}
				prior.Schema = 8
				b.state, _ = json.Marshal(prior)
				if _, err = restarted.GetTwin(ctx); err == nil {
					t.Fatal("outcome provenance hidden in old schema")
				}
				legacy := prior.Commands["mutation"]
				legacy.MutationOutcomeAt = time.Time{}
				legacy.MutationBarrierAt = nil
				prior.Commands["mutation"] = legacy
				b.state, _ = json.Marshal(prior)
				migrated, err := restarted.load(ctx)
				if err != nil || migrated.Schema != 11 || !migrated.Commands["mutation"].MutationOutcomeAt.IsZero() || len(migrated.Commands["mutation"].Result) == 0 {
					t.Fatal("schema-8 upgrade invented outcome time or lost evidence", migrated, err)
				}
				b.state = original
			}
		})
	}
}

func TestAsyncCompletionSetsOutcomeOnceRegardlessOfResponseOrder(t *testing.T) {
	for _, order := range []string{"accepted-first", "completion-first"} {
		t.Run(order, func(t *testing.T) {
			a, _, _, now := managedActor(t)
			observe(t, a, "initial", *now, "target")
			if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
				t.Fatal(err)
			}
			r := command("operate", now.Add(time.Hour))
			r.Request = &usp.Request{ReqType: &usp.Request_Operate{Operate: &usp.Operate{Command: "Device.Reboot()"}}}
			if _, err := a.EnqueueCommand(ctx, r); err != nil {
				t.Fatal(err)
			}
			if err := a.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			c, _ := a.GetCommand(ctx, "operate")
			*now = now.Add(time.Second)
			observe(t, a, "during", *now, "target")
			response := device.DeviceResponse{Identity: identity, Message: operateResponse(c.MessageID, true)}
			if order == "accepted-first" {
				*now = now.Add(time.Second)
				if err := a.HandleResponse(ctx, response); err != nil {
					t.Fatal(err)
				}
				accepted, _ := a.GetCommand(ctx, "operate")
				twin, _ := a.GetTwin(ctx)
				if !accepted.MutationOutcomeAt.IsZero() || twin.Convergence != "converged" {
					t.Fatal("async acceptance treated as completion", accepted, twin)
				}
			}
			*now = now.Add(time.Second)
			at := *now
			complete := &usp.Notify{Notification: &usp.Notify_OperComplete{OperComplete: &usp.Notify_OperationComplete{ObjPath: "Device.", CommandName: "Reboot()", CommandKey: c.OperationKey, OperationResp: &usp.Notify_OperationComplete_ReqOutputArgs{ReqOutputArgs: &usp.Notify_OperationComplete_OutputArgs{}}}}}
			if _, err := a.HandleNotify(ctx, notification("complete", complete, *now)); err != nil {
				t.Fatal(err)
			}
			twin, _ := a.GetTwin(ctx)
			if twin.Convergence != "unknown" {
				t.Fatal("completion reused in-flight observation", twin)
			}
			*now = now.Add(time.Second)
			observe(t, a, "after", *now, "target")
			*now = now.Add(time.Second)
			if err := a.HandleResponse(ctx, response); err != nil {
				t.Fatal(err)
			}
			if _, err := a.HandleNotify(ctx, notification("duplicate-evidence", complete, *now)); err != nil {
				t.Fatal(err)
			}
			c, _ = a.GetCommand(ctx, "operate")
			twin, _ = a.GetTwin(ctx)
			if !c.MutationOutcomeAt.Equal(at) || twin.Convergence != "converged" {
				t.Fatal("late acceptance/duplicate completion moved barrier", c, twin)
			}
		})
	}
}

func TestReadStartedBeforeLateMutationOutcomeCannotRefreshTwin(t *testing.T) {
	a, _, f, now := managedActor(t)
	a.opts.Policy.ObserveGetResponses = true
	observe(t, a, "initial", *now, "target")
	if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
		t.Fatal(err)
	}
	f.err = errors.New("publish uncertain")
	if _, err := a.EnqueueCommand(ctx, mutationRequest("mutation", now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	mutation, _ := a.GetCommand(ctx, "mutation")
	if _, err := a.AcknowledgeUnknown(ctx, device.AcknowledgeUnknownRequest{ID: mutation.ID, RequestID: "review", ExpectedRevision: mutation.Revision, Reason: "independent inspection"}); err != nil {
		t.Fatal(err)
	}
	f.err = nil
	*now = now.Add(time.Second)
	if _, err := a.EnqueueCommand(ctx, command("read", now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	read, _ := a.GetCommand(ctx, "read")
	*now = now.Add(time.Second)
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: setResponse(mutation.MessageID)}); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(time.Second)
	msg := getObservationResponse(read.MessageID, getPathResult("Device.Test.", "Device.Test.", map[string]string{"Value": "target"}))
	if err := a.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: msg, ReceivedAt: *now}); err != nil {
		t.Fatal(err)
	}
	twin, _ := a.GetTwin(ctx)
	read, _ = a.GetCommand(ctx, "read")
	if twin.Convergence != "unknown" || twin.ReportedRevision != 1 || read.State != device.CommandSucceeded || read.Result == nil {
		t.Fatal("read crossed late outcome barrier or lost evidence", twin, read)
	}
}
