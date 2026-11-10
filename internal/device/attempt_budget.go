package device

import (
	"fmt"
	"sync"
	"sync/atomic"
)

const MaxAttemptConcurrency = 256

// AttemptBudget bounds simultaneous begin/publish/result sections on one host.
// It is not a distributed lease, response-in-flight limit or per-second quota.
type AttemptBudget struct {
	slots  chan struct{}
	denied atomic.Uint64
}

func NewAttemptBudget(limit int) (*AttemptBudget, error) {
	if limit < 1 || limit > MaxAttemptConcurrency {
		return nil, fmt.Errorf("attempt concurrency must be 1–%d", MaxAttemptConcurrency)
	}
	return &AttemptBudget{slots: make(chan struct{}, limit)}, nil
}
func (b *AttemptBudget) Limit() int     { return cap(b.slots) }
func (b *AttemptBudget) InUse() int     { return len(b.slots) }
func (b *AttemptBudget) Denied() uint64 { return b.denied.Load() }

// TryAcquire never queues or waits. The release is idempotent so cancellation or
// multiple cleanup paths cannot release another actor's permit.
func (b *AttemptBudget) TryAcquire() (release func(), ok bool) {
	select {
	case b.slots <- struct{}{}:
		var once sync.Once
		return func() { once.Do(func() { <-b.slots }) }, true
	default:
		b.denied.Add(1)
		return nil, false
	}
}
