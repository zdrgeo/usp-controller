// Package fleet provides an optional, rebuildable per-site status projection.
// It never mutates actor state or publishes device commands.
package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

var (
	ErrInvalid     = errors.New("invalid projection input")
	ErrConflict    = errors.New("conflicting event identity")
	ErrUnavailable = errors.New("projection persistence unavailable")
)

const maxDevices = 4096
const receiptLimit = 32

type Invoker interface {
	Invoke(context.Context, device.DeviceIdentity, string, any) (json.RawMessage, error)
}
type Options struct {
	Directory, SiteID           string
	RefreshInterval, StaleAfter time.Duration
	Workers                     int
	Now                         func() time.Time
}

func (o Options) validate() error {
	if o.Directory == "" || o.SiteID == "" || len(o.SiteID) > 128 || o.Now == nil || o.RefreshInterval < 5*time.Second || o.RefreshInterval > time.Hour || o.StaleAfter < o.RefreshInterval || o.StaleAfter > 24*time.Hour || o.Workers < 1 || o.Workers > 16 {
		return ErrInvalid
	}
	return nil
}

type eventReceipt struct{ Key, Fingerprint string }
type row struct {
	Schema                               int
	Identity                             device.DeviceIdentity
	SiteID                               string
	Snapshot                             *deviceactor.Status
	CapturedAt, RefreshedAt, AttemptedAt time.Time
	Pending                              bool
	LastError                            string
	Receipts                             []eventReceipt
}

// Projection owns a bounded local store. Replicas must use separate directories;
// their snapshots can differ until the next refresh. Actor reads are authoritative.
type Projection struct {
	mu              sync.Mutex
	opts            Options
	store           store
	invoker         Invoker
	rows            map[string]row
	ids             []string
	inFlight        map[string]bool
	cursor          int
	running, closed bool
	fault           error
	wake            chan struct{}
}

func Open(o Options, inventory map[string]device.DeviceIdentity, invoker Invoker) (*Projection, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	s, err := openStore(o.Directory)
	if err != nil {
		return nil, err
	}
	p, err := newProjection(o, inventory, invoker, s)
	if err != nil {
		s.Close()
		return nil, err
	}
	return p, nil
}
func newProjection(o Options, inventory map[string]device.DeviceIdentity, invoker Invoker, s store) (*Projection, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	if invoker == nil || len(inventory) == 0 || len(inventory) > maxDevices {
		return nil, ErrInvalid
	}
	p := &Projection{opts: o, store: s, invoker: invoker, rows: map[string]row{}, inFlight: map[string]bool{}, wake: make(chan struct{}, 1)}
	var tenant, controller string
	for id, identity := range inventory {
		if id == "" || len(id) > 1024 || id != identity.AgentID || identity.Tenant == "" || identity.ControllerID == "" || len(identity.Tenant) > 1024 || len(identity.ControllerID) > 1024 {
			return nil, ErrInvalid
		}
		if tenant == "" {
			tenant = identity.Tenant
			controller = identity.ControllerID
		}
		if tenant != identity.Tenant || controller != identity.ControllerID {
			return nil, ErrInvalid
		}
		p.ids = append(p.ids, id)
	}
	sort.Strings(p.ids)
	scope, _ := json.Marshal(struct{ Site, Tenant, Controller string }{o.SiteID, tenant, controller})
	prior, err := s.Load("scope")
	if err != nil {
		return nil, err
	}
	if len(prior) == 0 {
		if err = s.Save("scope", scope); err != nil {
			return nil, err
		}
	} else if string(prior) != string(scope) {
		return nil, errors.New("projection directory belongs to another site or controller")
	}
	for _, id := range p.ids {
		identity := inventory[id]
		r := row{Schema: 1, Identity: identity, SiteID: o.SiteID, Pending: true}
		b, err := s.Load(deviceactor.ActorID(identity))
		if err != nil {
			return nil, err
		}
		if len(b) > 0 {
			if err = json.Unmarshal(b, &r); err != nil {
				return nil, err
			}
			if r.Schema != 1 || r.Identity != identity || r.SiteID != o.SiteID || len(r.Receipts) > receiptLimit {
				return nil, errors.New("inconsistent projection row")
			}
			if r.LastError != "" && r.LastError != "refresh_failed" {
				return nil, errors.New("invalid projection error code")
			}
			if r.Snapshot != nil {
				if r.CapturedAt.IsZero() || r.RefreshedAt.IsZero() {
					return nil, errors.New("projection snapshot missing timestamp")
				}
				if err = validateStatus(*r.Snapshot, o.SiteID); err != nil {
					return nil, err
				}
			}
			seen := map[string]bool{}
			for _, receipt := range r.Receipts {
				if receipt.Key == "" || len(receipt.Fingerprint) != 64 || seen[receipt.Key] {
					return nil, errors.New("invalid projection receipts")
				}
				seen[receipt.Key] = true
			}
			// Persisted snapshots are always stale until revalidated in this process.
			r.Pending = true
			r.AttemptedAt = time.Time{}
		}
		p.rows[id] = r
	}
	return p, nil
}
func (p *Projection) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	return p.store.Close()
}
func (p *Projection) commit(id string, r row) error {
	if p.closed || p.fault != nil {
		return ErrUnavailable
	}
	b, err := json.Marshal(r)
	if err == nil {
		err = p.store.Save(deviceactor.ActorID(r.Identity), b)
	}
	if err != nil {
		p.fault = err
		return ErrUnavailable
	}
	p.rows[id] = r
	return nil
}
func (p *Projection) signal() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// Run bounds concurrent reads and gives round-robin service to the inventory.
// Events coalesce into a pending bit; poll fallback captures desired/command
// changes that do not generate Notify events. No refresh occurs on a query path.
func (p *Projection) Run(ctx context.Context) error {
	p.mu.Lock()
	if p.running || p.closed {
		p.mu.Unlock()
		return ErrUnavailable
	}
	p.running = true
	p.mu.Unlock()
	var wg sync.WaitGroup
	defer func() { wg.Wait(); p.mu.Lock(); p.running = false; p.mu.Unlock() }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return nil
		}
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil
		}
		if p.fault != nil {
			p.mu.Unlock()
			return ErrUnavailable
		}
		jobs := []string{}
		now := p.opts.Now()
		if p.fault == nil {
			for scanned := 0; scanned < len(p.ids) && len(p.inFlight) < p.opts.Workers; scanned++ {
				id := p.ids[p.cursor]
				p.cursor = (p.cursor + 1) % len(p.ids)
				r := p.rows[id]
				retryReady := r.AttemptedAt.IsZero() || r.AttemptedAt.After(now) || !now.Before(r.AttemptedAt.Add(5*time.Second))
				due := r.Pending || r.CapturedAt.IsZero() || r.CapturedAt.After(now) || !now.Before(r.CapturedAt.Add(p.opts.RefreshInterval))
				if !p.inFlight[id] && retryReady && due {
					p.inFlight[id] = true
					jobs = append(jobs, id)
				}
			}
		}
		p.mu.Unlock()
		for _, id := range jobs {
			wg.Add(1)
			go func(id string) { defer wg.Done(); p.refresh(ctx, id) }(id)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		case <-p.wake:
		}
	}
}
func (p *Projection) refresh(ctx context.Context, id string) {
	p.mu.Lock()
	if p.closed || p.fault != nil {
		delete(p.inFlight, id)
		p.mu.Unlock()
		return
	}
	before := p.rows[id]
	// Clear pending in memory while reading. A later event sets it again; saving
	// the result must preserve that newer invalidation. Restart refreshes all rows.
	current := before
	current.Pending = false
	current.AttemptedAt = p.opts.Now()
	p.rows[id] = current
	started := current.AttemptedAt
	p.mu.Unlock()
	callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	b, err := p.invoker.Invoke(callCtx, before.Identity, "GetStatus", nil)
	cancel()
	var status deviceactor.Status
	if err == nil {
		err = json.Unmarshal(b, &status)
	}
	if err == nil {
		err = validateStatus(status, p.opts.SiteID)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	defer p.signal()
	delete(p.inFlight, id)
	if p.closed || p.fault != nil {
		return
	}
	current = p.rows[id]
	if err != nil {
		current.Pending = true
		current.LastError = "refresh_failed"
	} else {
		current.Snapshot = &status
		current.CapturedAt = started
		current.RefreshedAt = p.opts.Now()
		current.LastError = ""
	}
	_ = p.commit(id, current) // failure poisons persistence; old snapshots remain stale
}
func validateStatus(s deviceactor.Status, site string) error {
	if s.SiteID != site || s.MaxCommands < 1 || s.MaxCommands > 100000 || s.MaxReceipts < 1 || s.MaxReceipts > 1000000 || s.MaxStateBytes < 1 || s.MaxStateBytes > 64<<20 || s.StateBytes < 0 || s.StateBytes > s.MaxStateBytes || s.PendingEvents < 0 || s.PendingEvents > s.MaxCommands || s.NotificationReceipts < 0 || s.NotificationReceipts > s.MaxReceipts || s.MutationReceipts < 0 || s.MutationReceipts > s.MaxReceipts || s.CommandStates == nil {
		return ErrInvalid
	}
	switch s.Convergence {
	case "converged", "pending", "unknown":
	default:
		return ErrInvalid
	}
	switch s.Reconciliation {
	case "disabled", "idle", "policy_blocked", "waiting_observation", "converged", "eligible", "attempt_recorded", "blocked_unknown":
	default:
		return ErrInvalid
	}
	total := 0
	for state, n := range s.CommandStates {
		if n < 0 || n > s.MaxCommands {
			return ErrInvalid
		}
		total += n
		if total > s.MaxCommands {
			return ErrInvalid
		}
		switch state {
		case device.CommandQueued, device.CommandAttempting, device.CommandAwaitingResponse, device.CommandAwaitingCompletion, device.CommandSucceeded, device.CommandFailed, device.CommandUnknown, device.CommandExpired, device.CommandCancelled:
		default:
			return fmt.Errorf("%w: unknown command state", ErrInvalid)
		}
	}
	return nil
}
