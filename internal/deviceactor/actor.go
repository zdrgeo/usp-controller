// Package deviceactor implements Device state and opt-in single-host command execution.
package deviceactor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"example.com/usp-controller/internal/protocol"
	"google.golang.org/protobuf/proto"
)

var (
	ErrInvalid  = errors.New("deviceactor: invalid input")
	ErrConflict = errors.New("deviceactor: conflict")
	ErrCapacity = errors.New("deviceactor: capacity exhausted")
	ErrNotFound = errors.New("deviceactor: not found")
	ErrDisabled = errors.New("deviceactor: execution and protocol ingestion disabled")
)

// Backend is scoped to exactly one actor. Load returns nil for absent state.
// Save atomically replaces one bounded snapshot and returns only after commit.
// A failed Save may have committed. Dapr must serialize turns across hosts;
// the Actor mutex serializes only callers of this local instance.
type Backend interface {
	Load(context.Context) ([]byte, error)
	Save(context.Context, []byte) error
	EnsureReminder(context.Context) error
}

type Options struct {
	Policy                                            *device.TwinPolicy
	MaxBytes, MaxReceipts, MaxCommands, MaxParameters int
	Freshness                                         time.Duration
	Now                                               func() time.Time
}

func DefaultOptions() Options {
	return Options{MaxBytes: 1 << 20, MaxReceipts: 1024, MaxCommands: 64, MaxParameters: 1024, Freshness: 5 * time.Minute, Now: time.Now}
}

type Actor struct {
	mu        sync.Mutex
	id        device.DeviceIdentity
	backend   Backend
	opts      Options
	transport device.DeviceTransport
	publisher device.DeviceEventPublisher
}

func New(id device.DeviceIdentity, backend Backend, opts Options) (*Actor, error) {
	if id.Tenant == "" || id.ControllerID == "" || id.AgentID == "" || backend == nil || opts.Now == nil || opts.MaxBytes <= 0 || opts.MaxReceipts <= 0 || opts.MaxCommands <= 0 || opts.MaxParameters <= 0 || opts.Freshness <= 0 {
		return nil, ErrInvalid
	}
	if opts.Policy != nil {
		if err := opts.Policy.Validate(); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
		copyPolicy := *opts.Policy
		copyPolicy.Paths = map[string]device.ManagedPath{}
		for path, rule := range opts.Policy.Paths {
			copyPolicy.Paths[path] = rule
		}
		opts.Policy = &copyPolicy
	}
	return &Actor{id: id, backend: backend, opts: opts}, nil
}

type receipt struct {
	Kind, Fingerprint string
	Revision          uint64
}
type storedCommand struct {
	DesiredRevision uint64
	Receipt         device.CommandReceipt
	Payload         []byte // deterministic protobuf, never encoding/json of a protobuf oneof
	Deadline        time.Time
	Reason          string
	MessageID       string
	Result          []byte
	OperationKey    string
	Completion      []byte
}
type snapshot struct {
	Schema                            int
	Identity                          device.DeviceIdentity
	DesiredRevision, ReportedRevision uint64
	Desired                           map[string]device.ParameterValue
	Reported                          map[string]device.Observation
	LastContact                       time.Time
	Receipts                          map[device.RequestID]receipt
	Commands                          map[device.RequestID]storedCommand
	Queue                             []device.RequestID
	Inbox                             map[string]string
	Outbox                            []storedEvent
	EventSequence                     uint64
}

func (a *Actor) load(ctx context.Context) (*snapshot, error) {
	b, err := a.backend.Load(ctx)
	if err != nil {
		return nil, err
	}
	s := &snapshot{Schema: 3, Identity: a.id, Desired: map[string]device.ParameterValue{}, Reported: map[string]device.Observation{}, Receipts: map[device.RequestID]receipt{}, Commands: map[device.RequestID]storedCommand{}}
	s.Inbox = map[string]string{}
	if len(b) == 0 {
		return s, nil
	}
	if len(b) > a.opts.MaxBytes {
		return nil, ErrCapacity
	}
	s = new(snapshot)
	if err = json.Unmarshal(b, s); err != nil {
		return nil, fmt.Errorf("decode actor state: %w", err)
	}
	if (s.Schema >= 2 && s.Inbox == nil) || (s.Schema != 1 && s.Schema != 2 && s.Schema != 3) || s.Identity != a.id || s.Desired == nil || s.Reported == nil || s.Receipts == nil || s.Commands == nil {
		return nil, fmt.Errorf("unsupported or inconsistent actor state")
	}
	if len(s.Receipts) > a.opts.MaxReceipts || len(s.Commands) > a.opts.MaxCommands || len(s.Desired) > a.opts.MaxParameters || len(s.Reported) > a.opts.MaxParameters {
		return nil, ErrCapacity
	}
	s.Schema = 3 // upgrade on the next atomic save; older binaries must reject this state
	if s.Inbox == nil {
		s.Inbox = map[string]string{}
	}
	if err := a.validateEvents(s); err != nil {
		return nil, err
	}
	queued := map[device.RequestID]bool{}
	for _, id := range s.Queue {
		c, ok := s.Commands[id]
		if !ok || queued[id] || c.Receipt.State != device.CommandQueued {
			return nil, fmt.Errorf("inconsistent command queue")
		}
		queued[id] = true
	}
	for id, c := range s.Commands {
		r, ok := s.Receipts[id]
		if !ok || r.Kind != "command" || c.Receipt.ID != id || c.Receipt.Revision == 0 || c.Deadline.IsZero() {
			return nil, fmt.Errorf("inconsistent command state")
		}
		if c.DesiredRevision != 0 {
			if c.DesiredRevision > s.DesiredRevision || string(id) != fmt.Sprintf("reconcile-%d", c.DesiredRevision) {
				return nil, fmt.Errorf("invalid reconciliation provenance")
			}
			req := new(usp.Request)
			if proto.Unmarshal(c.Payload, req) != nil || req.GetSet() == nil {
				return nil, fmt.Errorf("invalid reconciliation command")
			}
		}
		switch c.Receipt.State {
		case device.CommandQueued:
			if !queued[id] {
				return nil, fmt.Errorf("queued command missing from index")
			}
		case device.CommandCancelled, device.CommandExpired:
		case device.CommandAttempting, device.CommandAwaitingResponse, device.CommandAwaitingCompletion, device.CommandUnknown, device.CommandSucceeded, device.CommandFailed:
			if c.MessageID == "" {
				return nil, fmt.Errorf("missing publish identity")
			}
		default:
			return nil, fmt.Errorf("unsupported command state")
		}
	}
	for id, r := range s.Receipts {
		if id == "" || r.Revision == 0 || len(r.Fingerprint) != 64 {
			return nil, fmt.Errorf("inconsistent receipt")
		}
		switch r.Kind {
		case "desired", "reported":
		case "command":
			if _, ok := s.Commands[id]; !ok {
				return nil, fmt.Errorf("receipt missing command")
			}
		default:
			return nil, fmt.Errorf("unsupported receipt kind")
		}
	}
	return s, nil
}
func (a *Actor) save(ctx context.Context, s *snapshot) error {
	if err := a.validateEvents(s); err != nil {
		return err
	}
	if len(s.Receipts) > a.opts.MaxReceipts || len(s.Commands) > a.opts.MaxCommands || len(s.Desired) > a.opts.MaxParameters || len(s.Reported) > a.opts.MaxParameters {
		return ErrCapacity
	}
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if len(b) > a.opts.MaxBytes {
		return ErrCapacity
	}
	return a.backend.Save(ctx, b)
}
func fingerprint(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}
func check(s *snapshot, id device.RequestID, kind, fp string) (receipt, bool, error) {
	if id == "" {
		return receipt{}, false, ErrInvalid
	}
	r, ok := s.Receipts[id]
	if ok && (r.Kind != kind || r.Fingerprint != fp) {
		return receipt{}, false, ErrConflict
	}
	return r, ok, nil
}
func validPath(p string) bool {
	return strings.HasPrefix(p, "Device.") && len(p) > len("Device.") && strings.TrimSpace(p) == p
}

func (a *Actor) GetTwin(ctx context.Context) (device.DeviceTwin, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s, err := a.load(ctx)
	if err != nil {
		return device.DeviceTwin{}, err
	}
	t := device.DeviceTwin{Identity: a.id, DesiredRevision: s.DesiredRevision, ReportedRevision: s.ReportedRevision, Desired: s.Desired, Reported: s.Reported, LastContact: s.LastContact, Convergence: "converged"}
	t.Convergence = a.convergence(s)
	for id, c := range s.Commands {
		if c.Receipt.State == device.CommandUnknown {
			t.Unknown = append(t.Unknown, id)
		}
		if c.Receipt.State == device.CommandQueued || c.Receipt.State == device.CommandAttempting || c.Receipt.State == device.CommandAwaitingResponse || c.Receipt.State == device.CommandAwaitingCompletion {
			t.Pending = append(t.Pending, id)
		}
	}
	sort.Slice(t.Unknown, func(i, j int) bool { return t.Unknown[i] < t.Unknown[j] })
	sort.Slice(t.Pending, func(i, j int) bool { return t.Pending[i] < t.Pending[j] })
	if a.opts.Policy != nil {
		for p := range t.Desired {
			if _, ok := a.opts.Policy.Paths[p]; !ok {
				delete(t.Desired, p)
			}
		}
		for p := range t.Reported {
			if _, ok := a.opts.Policy.Paths[p]; !ok {
				delete(t.Reported, p)
			}
		}
	}
	return t, nil // all maps came from a fresh decode; no state is cached
}
func (a *Actor) SetDesired(ctx context.Context, r device.SetDesiredRequest) (device.DesiredReceipt, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	zero := device.DesiredReceipt{}
	if r.ID == "" || strings.HasPrefix(string(r.ID), "reconcile-") || len(r.Values)+len(r.Remove) == 0 {
		return zero, ErrInvalid
	}
	seen := map[string]bool{}
	remove := append([]string(nil), r.Remove...)
	sort.Strings(remove)
	for p, v := range r.Values {
		if a.opts.Policy != nil && !a.opts.Policy.Allows(p, v, true) {
			return zero, ErrInvalid
		}
		if !validPath(p) {
			return zero, ErrInvalid
		}
		seen[p] = true
	}
	for _, p := range remove {
		if !validPath(p) || seen[p] {
			return zero, ErrInvalid
		}
		seen[p] = true
	}
	r.Remove = remove
	fp, err := fingerprint(r)
	if err != nil {
		return zero, err
	}
	s, err := a.load(ctx)
	if err != nil {
		return zero, err
	}
	prior, ok, err := check(s, r.ID, "desired", fp)
	if err != nil {
		return zero, err
	}
	// Repair scheduling on identical retries too. Never reset an existing reminder.
	if ok {
		if err = a.backend.EnsureReminder(ctx); err != nil {
			return zero, err
		}
		return device.DesiredReceipt{ID: r.ID, Revision: prior.Revision}, nil
	}
	if r.ExpectedRevision != s.DesiredRevision {
		return zero, ErrConflict
	}
	if s.DesiredRevision == math.MaxUint64 {
		return zero, ErrCapacity
	}
	for p, v := range r.Values {
		s.Desired[p] = v
	}
	for _, p := range remove {
		delete(s.Desired, p)
	}
	s.DesiredRevision++
	s.Receipts[r.ID] = receipt{"desired", fp, s.DesiredRevision}
	if err = a.backend.EnsureReminder(ctx); err != nil {
		return zero, err
	}
	if err = a.save(ctx, s); err != nil {
		return zero, err
	}
	return device.DesiredReceipt{ID: r.ID, Revision: s.DesiredRevision}, nil
}

// ReportRequest is a trusted observation adapter input, not a public device API.
// ReceivedAt must be assigned by ingress, not copied from the device clock.
type ReportRequest struct {
	ID         device.RequestID
	Identity   device.DeviceIdentity
	ReceivedAt time.Time
	Values     map[string]device.ParameterValue
}

func (a *Actor) Report(ctx context.Context, r ReportRequest) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if r.ID == "" || strings.HasPrefix(string(r.ID), "reconcile-") || r.Identity != a.id || r.ReceivedAt.IsZero() || r.ReceivedAt.After(a.opts.Now()) || len(r.Values) == 0 {
		return 0, ErrInvalid
	}
	for p, v := range r.Values {
		if a.opts.Policy != nil && !a.opts.Policy.Allows(p, v, false) {
			return 0, ErrInvalid
		}
		if !validPath(p) {
			return 0, ErrInvalid
		}
	}
	r.ReceivedAt = r.ReceivedAt.UTC()
	fp, err := fingerprint(r)
	if err != nil {
		return 0, err
	}
	s, err := a.load(ctx)
	if err != nil {
		return 0, err
	}
	prior, ok, err := check(s, r.ID, "reported", fp)
	if err != nil {
		return 0, err
	}
	if ok {
		if err = a.backend.EnsureReminder(ctx); err != nil {
			return 0, err
		}
		return prior.Revision, nil
	}
	if s.ReportedRevision == math.MaxUint64 {
		return 0, ErrCapacity
	}
	for p, v := range r.Values {
		old, exists := s.Reported[p]
		if exists && !r.ReceivedAt.After(old.ReceivedAt) {
			continue
		}
		s.Reported[p] = device.Observation{Value: v, ReceivedAt: r.ReceivedAt, FreshUntil: r.ReceivedAt.Add(a.opts.Freshness), Source: "validated-ingress"}
	}
	if r.ReceivedAt.After(s.LastContact) {
		s.LastContact = r.ReceivedAt
	}
	s.ReportedRevision++
	s.Receipts[r.ID] = receipt{"reported", fp, s.ReportedRevision}
	if err = a.backend.EnsureReminder(ctx); err != nil {
		return 0, err
	}
	if err = a.save(ctx, s); err != nil {
		return 0, err
	}
	return s.ReportedRevision, nil
}
func (a *Actor) EnqueueCommand(ctx context.Context, r device.CommandRequest) (device.CommandReceipt, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	zero := device.CommandReceipt{}
	if r.ID == "" || strings.HasPrefix(string(r.ID), "reconcile-") || r.Deadline.IsZero() {
		return zero, ErrInvalid
	}
	if _, err := protocol.RequestType(r.Request); err != nil {
		return zero, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if a.transport != nil && r.Request.GetOperate() != nil && !concreteOperation(r.Request.GetOperate().Command) {
		return zero, fmt.Errorf("%w: Operate requires one concrete command path", ErrInvalid)
	}
	if a.transport != nil {
		kind, _ := protocol.RequestType(r.Request)
		msg := &usp.Msg{Header: &usp.Header{MsgId: strings.Repeat("0", 32), MsgType: kind}, Body: &usp.Body{MsgBody: &usp.Body_Request{Request: r.Request}}}
		if _, err := protocol.Encode(a.id.ControllerID, a.id.AgentID, msg); err != nil {
			return zero, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
	}

	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(r.Request)
	if err != nil {
		return zero, err
	}
	r.Deadline = r.Deadline.UTC()
	fp, err := fingerprint(struct {
		Payload  []byte
		Deadline time.Time
	}{payload, r.Deadline})
	if err != nil {
		return zero, err
	}
	s, err := a.load(ctx)
	if err != nil {
		return zero, err
	}
	_, ok, err := check(s, r.ID, "command", fp)
	if err != nil {
		return zero, err
	}
	if ok {
		if err = a.backend.EnsureReminder(ctx); err != nil {
			return zero, err
		}
		return s.Commands[r.ID].Receipt, nil
	}
	if !r.Deadline.After(a.opts.Now()) {
		return zero, ErrInvalid
	}
	result := device.CommandReceipt{ID: r.ID, Revision: 1, State: device.CommandQueued}
	s.Commands[r.ID] = storedCommand{Receipt: result, Payload: payload, Deadline: r.Deadline}
	s.Queue = append(s.Queue, r.ID)
	s.Receipts[r.ID] = receipt{"command", fp, 1}
	if err = a.backend.EnsureReminder(ctx); err != nil {
		return zero, err
	}
	if err = a.save(ctx, s); err != nil {
		return zero, err
	}
	return result, nil
}
func (a *Actor) GetCommand(ctx context.Context, id device.RequestID) (device.DeviceCommand, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s, err := a.load(ctx)
	if err != nil {
		return device.DeviceCommand{}, err
	}
	c, ok := s.Commands[id]
	if !ok {
		return device.DeviceCommand{}, ErrNotFound
	}
	req := new(usp.Request)
	if err = proto.Unmarshal(c.Payload, req); err != nil {
		return device.DeviceCommand{}, err
	}
	return commandView(c, req)
}
func (a *Actor) CancelCommand(ctx context.Context, r device.CancelCommandRequest) (device.DeviceCommand, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s, err := a.load(ctx)
	if err != nil {
		return device.DeviceCommand{}, err
	}
	c, ok := s.Commands[r.ID]
	if !ok {
		return device.DeviceCommand{}, ErrNotFound
	}
	// A retry after an ambiguous cancellation commit returns the same result.
	if c.Receipt.State == device.CommandCancelled && r.ExpectedRevision < math.MaxUint64 && c.Receipt.Revision == r.ExpectedRevision+1 {
		req := new(usp.Request)
		if err = proto.Unmarshal(c.Payload, req); err != nil {
			return device.DeviceCommand{}, err
		}
		return commandView(c, req)
	}
	if c.Receipt.State != device.CommandQueued || c.Receipt.Revision != r.ExpectedRevision {
		return device.DeviceCommand{}, ErrConflict
	}
	if c.Receipt.Revision == math.MaxUint64 {
		return device.DeviceCommand{}, ErrCapacity
	}
	c.Receipt.State = device.CommandCancelled
	c.Receipt.Revision++
	c.Reason = "cancelled before dispatch"
	s.Commands[r.ID] = c
	a.compactQueue(s)
	if err = a.save(ctx, s); err != nil {
		return device.DeviceCommand{}, err
	}
	req := new(usp.Request)
	if err = proto.Unmarshal(c.Payload, req); err != nil {
		return device.DeviceCommand{}, err
	}
	return commandView(c, req)
}
func (a *Actor) compactQueue(s *snapshot) {
	out := s.Queue[:0]
	for _, id := range s.Queue {
		if s.Commands[id].Receipt.State == device.CommandQueued {
			out = append(out, id)
		}
	}
	s.Queue = out
}

// Activate repairs scheduling without publishing anything. Repeated activation is safe.
func (a *Actor) Activate(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, err := a.load(ctx); err != nil {
		return err
	}
	return a.backend.EnsureReminder(ctx)
}

var _ device.DeviceActor = (*Actor)(nil)
