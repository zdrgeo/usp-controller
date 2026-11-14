package deviceactor

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

type batchSink struct {
	events    []device.DeviceEvent
	deadlines []time.Time
	failAt    int
	err       error
}

func (p *batchSink) Publish(ctx context.Context, e device.DeviceEvent) error {
	p.events = append(p.events, e)
	deadline, _ := ctx.Deadline()
	p.deadlines = append(p.deadlines, deadline)
	if len(p.events) == p.failAt {
		return p.err
	}
	return nil
}
func eventBatchActor(t *testing.T, size int) (*Actor, *memoryBackend, *fakeSender, *batchSink, *time.Time) {
	t.Helper()
	a, b, f, now := executable(t)
	a.opts.EventExportsPerTurn = size
	sink := &batchSink{}
	a.publisher = sink
	for i := 0; i < 3; i++ {
		if _, err := a.HandleNotify(ctx, valueNotification(fmt.Sprintf("event-%d", i), *now)); err != nil {
			t.Fatal(err)
		}
	}
	return a, b, f, sink, now
}

func TestEventBatchBoundAndSingleRemovalCommit(t *testing.T) {
	for _, size := range []int{1, 2} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			a, b, _, sink, _ := eventBatchActor(t, size)
			saves := b.saves
			if err := a.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			s, err := a.load(ctx)
			if err != nil || len(sink.events) != size || len(s.Outbox) != 3-size || b.saves != saves+1 {
				t.Fatal("batch bound/removal commit", len(sink.events), s, err)
			}
			for i, e := range sink.events {
				if e.Sequence != uint64(i+1) || sink.deadlines[i].IsZero() || !sink.deadlines[i].Equal(sink.deadlines[0]) {
					t.Fatal("FIFO or shared publication deadline changed", sink.events, sink.deadlines)
				}
			}
		})
	}
}

func TestPartialBatchCommitsPrefixAndKeepsFailedEvent(t *testing.T) {
	for _, dispatch := range []bool{false, true} {
		t.Run(fmt.Sprint(dispatch), func(t *testing.T) {
			a, b, f, sink, now := eventBatchActor(t, 3)
			a.opts.DispatchWithEventBacklog = dispatch
			offline := errors.New("destination uncertain")
			sink.failAt = 2
			sink.err = offline
			if _, err := a.EnqueueCommand(ctx, command("command", now.Add(time.Hour))); err != nil {
				t.Fatal(err)
			}
			if err := a.Tick(ctx); !errors.Is(err, offline) {
				t.Fatal("partial handoff failure hidden", err)
			}
			s, err := a.load(ctx)
			expected := 0
			if dispatch {
				expected = 1
			}
			if err != nil || len(s.Outbox) != 2 || s.Outbox[0].Sequence != 2 || len(sink.events) != 2 || len(f.calls) != expected {
				t.Fatal("partial prefix or dispatch boundary", s, err)
			}
			sink.failAt = 0
			restarted, err := NewWithEvents(identity, b, a.opts, f, sink)
			if err != nil {
				t.Fatal(err)
			}
			if err = restarted.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			s, err = restarted.load(ctx)
			if err != nil || len(s.Outbox) != 0 || len(s.Inbox) != 3 || len(f.calls) != 1 || len(sink.events) != 4 {
				t.Fatal("recovery lost/replayed work", s, err)
			}
			if sink.events[1].Epoch != sink.events[2].Epoch || sink.events[1].Sequence != sink.events[2].Sequence || sink.events[3].Sequence != 3 {
				t.Fatal("failed tail lost stable FIFO identity", sink.events)
			}
		})
	}
}

func TestBatchRemovalFailureNeverAllowsCommandDispatch(t *testing.T) {
	for _, mode := range []string{"failed", "ambiguous", "handoff-and-save"} {
		t.Run(mode, func(t *testing.T) {
			a, b, f, sink, now := eventBatchActor(t, 3)
			a.opts.DispatchWithEventBacklog = true
			if _, err := a.EnqueueCommand(ctx, command("command", now.Add(time.Hour))); err != nil {
				t.Fatal(err)
			}
			if mode == "ambiguous" {
				b.ambiguous = true
			} else {
				b.failSave = true
			}
			offline := errors.New("handoff failed")
			if mode == "handoff-and-save" {
				sink.failAt = 2
				sink.err = offline
			}
			err := a.Tick(ctx)
			if err == nil || len(f.calls) != 0 {
				t.Fatal("dispatched after uncertain removal commit", err)
			}
			if mode == "handoff-and-save" && !errors.Is(err, offline) {
				t.Fatal("lost handoff failure", err)
			}
			b.failSave = false
			sink.failAt = 0
			s, err := a.load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			expected := 3
			if mode == "ambiguous" {
				expected = 0
			}
			if len(s.Outbox) != expected {
				t.Fatal("unexpected removal durability", s)
			}
			if err = a.Tick(ctx); err != nil || len(f.calls) != 1 {
				t.Fatal("recovery failed or replayed command", err)
			}
			if mode == "failed" {
				if len(sink.events) != 6 {
					t.Fatal(sink.events)
				}
				for i := 0; i < 3; i++ {
					if sink.events[i].Epoch != sink.events[i+3].Epoch || sink.events[i].Sequence != sink.events[i+3].Sequence {
						t.Fatal("removal failure changed retry identity")
					}
				}
			}
		})
	}
}
