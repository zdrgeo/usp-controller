package device

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestAttemptRateBoundsConcurrentBurstAndClockRefill(t *testing.T) {
	now := time.Unix(100, 0)
	b, err := NewAttemptRateBudget(2, 3, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	var granted atomic.Int64
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.TryTake() {
				granted.Add(1)
			}
		}()
	}
	wg.Wait()
	if granted.Load() != 3 {
		t.Fatal("burst exceeded", granted.Load())
	}
	now = now.Add(-time.Second)
	if b.TryTake() {
		t.Fatal("backwards clock refilled")
	}
	now = time.Unix(100, 0).Add(250 * time.Millisecond)
	if b.TryTake() {
		t.Fatal("fractional token admitted")
	}
	now = now.Add(250 * time.Millisecond)
	if !b.TryTake() || b.TryTake() {
		t.Fatal("fractional refill lost or duplicated")
	}
	now = now.Add(time.Hour)
	for i := 0; i < 3; i++ {
		if !b.TryTake() {
			t.Fatal("capped refill missing")
		}
	}
	if b.TryTake() {
		t.Fatal("idle time exceeded burst cap")
	}
	_, _, count, _ := b.Stats()
	if count != 7 {
		t.Fatal(count)
	}
}
