package deviceactor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

func TestEventHandoffFailureRetainsEventAndOptionalCommandProgress(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "default", true: "enabled"}[enabled], func(t *testing.T) {
			a, b, f, now := executable(t)
			a.opts.DispatchWithEventBacklog = enabled
			offline := errors.New("event destination offline")
			sink := &eventSink{err: offline}
			a.publisher = sink
			if _, err := a.HandleNotify(ctx, valueNotification("event", *now)); err != nil {
				t.Fatal(err)
			}
			if _, err := a.EnqueueCommand(ctx, command("command", now.Add(time.Hour))); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if err := a.Tick(ctx); !errors.Is(err, offline) {
					t.Fatal("handoff error hidden", err)
				}
			}
			expected := 0
			if enabled {
				expected = 1
			}
			c, err := a.GetCommand(ctx, "command")
			if err != nil || len(f.calls) != expected {
				t.Fatal("dispatch policy/replay", err, f.calls)
			}
			if enabled && c.State != device.CommandAwaitingResponse || !enabled && c.State != device.CommandQueued {
				t.Fatal(c)
			}
			s, err := a.load(ctx)
			if err != nil || len(s.Outbox) != 1 || len(s.Inbox) != 1 {
				t.Fatal("pending event lost", s, err)
			}
			sink.err = nil
			restarted, err := NewWithEvents(identity, b, a.opts, f, sink)
			if err != nil {
				t.Fatal(err)
			}
			if err = restarted.Tick(ctx); err != nil || len(f.calls) != 1 {
				t.Fatal("recovery replayed or stranded command", err)
			}
			s, err = restarted.load(ctx)
			if err != nil || len(s.Outbox) != 0 || len(s.Inbox) != 1 || len(sink.events) != 3 {
				t.Fatal("event recovery failed", s, err)
			}
			for _, e := range sink.events[1:] {
				if e.Epoch != sink.events[0].Epoch || e.Sequence != sink.events[0].Sequence {
					t.Fatal("unstable event identity")
				}
			}
		})
	}
}

func TestBacklogDispatchStopsOnExportStorageFailureOrCancellation(t *testing.T) {
	for _, mode := range []string{"remove-failed", "remove-ambiguous", "cancelled", "command-save-failed"} {
		t.Run(mode, func(t *testing.T) {
			a, _, f, now := executable(t)
			b := a.backend.(*memoryBackend)
			a.opts.DispatchWithEventBacklog = true
			sink := &eventSink{}
			a.publisher = sink
			if _, err := a.HandleNotify(ctx, valueNotification("event", *now)); err != nil {
				t.Fatal(err)
			}
			if _, err := a.EnqueueCommand(ctx, command("command", now.Add(time.Hour))); err != nil {
				t.Fatal(err)
			}
			turn, cancel := context.WithCancel(ctx)
			defer cancel()
			offline := errors.New("handoff uncertain")
			switch mode {
			case "remove-failed":
				sink.after = func() { b.failSave = true }
			case "remove-ambiguous":
				sink.after = func() { b.ambiguous = true }
			case "cancelled":
				sink.err = offline
				sink.after = cancel
			case "command-save-failed":
				sink.err = offline
				b.failSave = true
			}
			err := a.Tick(turn)
			if err == nil || len(f.calls) != 0 {
				t.Fatal("unsafe progress after export/storage failure", err)
			}
			if mode == "command-save-failed" && (!errors.Is(err, offline) || !strings.Contains(err.Error(), "save failed")) {
				t.Fatal("lost one of the two failures", err)
			}
			b.failSave = false
			c, err := a.GetCommand(ctx, "command")
			if err != nil || c.State != device.CommandQueued || c.AttemptSequence != 0 || c.MessageID != "" {
				t.Fatal("begin escaped failed turn", c, err)
			}
		})
	}
}
