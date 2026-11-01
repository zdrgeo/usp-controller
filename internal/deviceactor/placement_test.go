package deviceactor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

// The fake placement lock explicitly models Dapr's serialization contract. These
// tests establish app recovery properties, not actual Dapr partition fencing.
type placementBackend struct {
	*memoryBackend
	unavailable bool
}

func (b *placementBackend) Load(ctx context.Context) ([]byte, error) {
	if b.unavailable {
		return nil, errors.New("state partition")
	}
	return b.memoryBackend.Load(ctx)
}
func (b *placementBackend) Save(ctx context.Context, v []byte) error {
	if b.unavailable {
		return errors.New("state partition")
	}
	return b.memoryBackend.Save(ctx, v)
}

func TestActorRelocationAndStatePartition(t *testing.T) {
	original, b, now := fixture(t)
	shared := &placementBackend{memoryBackend: b}
	firstSender := &fakeSender{ready: true}
	secondSender := &fakeSender{ready: true}
	first, err := NewExecuting(identity, shared, original.opts, firstSender)
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewExecuting(identity, shared, original.opts, secondSender)
	if err != nil {
		t.Fatal(err)
	}
	var placement sync.Mutex
	var wg sync.WaitGroup
	// Concurrent identical admissions through different frontends reach one owner
	// turn at a time and share a single durable queue entry.
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			placement.Lock()
			defer placement.Unlock()
			host := first
			if i%2 == 1 {
				host = second
			}
			if _, err := host.EnqueueCommand(ctx, command("same", now.Add(time.Minute))); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	shared.unavailable = true
	if err = first.Tick(ctx); err == nil || len(firstSender.calls) != 0 {
		t.Fatal("sent across state partition", err)
	}
	shared.unavailable = false
	// Crash after external send but before finish persistence: new owner must
	// classify the durable attempt as unknown and never re-publish it.
	firstSender.afterSend = func() { b.failSave = true }
	if err = first.Tick(ctx); err == nil || len(firstSender.calls) != 1 {
		t.Fatal(err)
	}
	b.failSave = false
	if err = second.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	c, err := second.GetCommand(ctx, "same")
	if err != nil || c.State != device.CommandUnknown || len(secondSender.calls) != 0 {
		t.Fatal(c, err)
	}
	if err = second.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: response(c.MessageID)}); err != nil {
		t.Fatal(err)
	}
	c, err = first.GetCommand(ctx, "same")
	if err != nil || c.State != device.CommandSucceeded {
		t.Fatal("stale cached state after relocation", c, err)
	}
	// Duplicate notification routed through a different host retains one receipt.
	n := valueNotification("notice", *now)
	if _, err = first.HandleNotify(ctx, n); err != nil {
		t.Fatal(err)
	}
	receipt, err := second.HandleNotify(ctx, n)
	if err != nil || !receipt.Duplicate {
		t.Fatal(receipt, err)
	}
	s, err := second.load(ctx)
	if err != nil || len(s.Outbox) != 1 || len(s.Commands) != 1 {
		t.Fatal(s, err)
	}
}
