package device

import (
	"fmt"
	"sync"
	"time"
)

const MaxAttemptRate = 10000
const MaxAttemptBurst = 256

// AttemptRateBudget is a host-local token bucket, shared across device actors.
// Reservations are never refunded: even a failed begin commit consumes one.
// Restarting the host creates a full bucket; this is not a durable site quota.
type AttemptRateBudget struct {
	mu              sync.Mutex
	rate, burst     int
	tokens          float64
	last            time.Time
	now             func() time.Time
	granted, denied uint64
}

func NewAttemptRateBudget(rate, burst int, now func() time.Time) (*AttemptRateBudget, error) {
	if rate < 1 || rate > MaxAttemptRate || burst < 1 || burst > MaxAttemptBurst || now == nil {
		return nil, fmt.Errorf("attempt rate requires 1–%d per second and burst 1–%d", MaxAttemptRate, MaxAttemptBurst)
	}
	return &AttemptRateBudget{rate: rate, burst: burst, tokens: float64(burst), last: now(), now: now}, nil
}
func (b *AttemptRateBudget) Valid() bool {
	return b != nil && b.now != nil && b.rate > 0 && b.rate <= MaxAttemptRate && b.burst > 0 && b.burst <= MaxAttemptBurst
}

// TryTake never waits or creates a secondary queue. A backwards clock does not
// refill tokens or move the refill boundary backwards.
func (b *AttemptRateBudget) TryTake() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	if now.After(b.last) {
		b.tokens += now.Sub(b.last).Seconds() * float64(b.rate)
		if b.tokens > float64(b.burst) {
			b.tokens = float64(b.burst)
		}
		b.last = now
	}
	if b.tokens < 1 {
		b.denied++
		return false
	}
	b.tokens--
	b.granted++
	return true
}

// Stats is passive: inspection does not refill or consume the bucket.
func (b *AttemptRateBudget) Stats() (rate, burst int, granted, denied uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rate, b.burst, b.granted, b.denied
}
