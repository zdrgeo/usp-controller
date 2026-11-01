package deviceactor

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
)

func (a *Actor) observedValue(path, value string) device.ParameterValue {
	v := device.ParameterValue{Value: value}
	if a.opts.Policy != nil {
		v.Type = a.opts.Policy.Paths[path].Type
	}
	return v
}
func (a *Actor) managedObservation(path, value string) bool {
	return a.opts.Policy == nil || a.opts.Policy.Allows(path, a.observedValue(path, value), false)
}
func (a *Actor) currentReconciliation(s *snapshot, c storedCommand) bool {
	if a.opts.Policy == nil || !a.opts.Policy.Reconcile || c.DesiredRevision != s.DesiredRevision {
		return false
	}
	for path, v := range s.Desired {
		if !a.opts.Policy.Allows(path, v, true) {
			return false
		}
	}
	req := new(usp.Request)
	if proto.Unmarshal(c.Payload, req) != nil || req.GetSet() == nil {
		return false
	}
	for _, obj := range req.GetSet().UpdateObjs {
		for _, param := range obj.ParamSettings {
			path := obj.ObjPath + param.Param
			want, ok := s.Desired[path]
			if !ok || want.Value != param.Value {
				return false
			}
			got, ok := s.Reported[path]
			if !ok || !a.opts.Now().Before(got.FreshUntil) || !a.opts.Policy.Allows(path, got.Value, false) || got.Value == want {
				return false
			}
		}
	}
	return true
}

// planReconciliation requires a fresh observation for every managed desired path.
// A revision gets at most one generated command, even after failure, expiry or
// later drift. A new operator revision is required to authorize another attempt.
func (a *Actor) planReconciliation(s *snapshot) (*usp.Request, string) {
	if a.opts.Policy == nil || !a.opts.Policy.Reconcile {
		return nil, "disabled"
	}
	if s.DesiredRevision == 0 || len(s.Desired) == 0 {
		return nil, "idle"
	}
	recorded := false
	for _, c := range s.Commands {
		if c.Receipt.State == device.CommandUnknown {
			return nil, "blocked_unknown"
		}
		if c.DesiredRevision == s.DesiredRevision {
			recorded = true
		}
	}
	if recorded {
		return nil, "attempt_recorded"
	}
	paths := make([]string, 0, len(s.Desired))
	for path, want := range s.Desired {
		if !a.opts.Policy.Allows(path, want, true) {
			return nil, "policy_blocked"
		}
		got, ok := s.Reported[path]
		if !ok || !a.opts.Now().Before(got.FreshUntil) || !a.opts.Policy.Allows(path, got.Value, false) {
			return nil, "waiting_observation"
		}
		if got.Value != want {
			paths = append(paths, path)
		}
	}
	if len(paths) == 0 {
		return nil, "converged"
	}
	sort.Strings(paths)
	set := &usp.Set{AllowPartial: false}
	objects := map[string]*usp.Set_UpdateObject{}
	objectPaths := []string{}
	for _, path := range paths {
		dot := strings.LastIndex(path, ".")
		objectPath := path[:dot+1]
		object := objects[objectPath]
		if object == nil {
			object = &usp.Set_UpdateObject{ObjPath: objectPath}
			objects[objectPath] = object
			objectPaths = append(objectPaths, objectPath)
		}
		object.ParamSettings = append(object.ParamSettings, &usp.Set_UpdateParamSetting{Param: path[dot+1:], Value: s.Desired[path].Value, Required: true})
	}
	sort.Strings(objectPaths)
	for _, path := range objectPaths {
		set.UpdateObjs = append(set.UpdateObjs, objects[path])
	}
	return &usp.Request{ReqType: &usp.Request_Set{Set: set}}, "eligible"
}
func (a *Actor) scheduleReconciliation(ctx context.Context, s *snapshot) error {
	req, _ := a.planReconciliation(s)
	if req == nil {
		return nil
	}
	id := device.RequestID(fmt.Sprintf("reconcile-%d", s.DesiredRevision))
	if _, exists := s.Receipts[id]; exists {
		return ErrConflict
	}
	payload, err := proto.MarshalOptions{Deterministic: true}.Marshal(req)
	if err != nil {
		return err
	}
	deadline := a.opts.Now().Add(time.Duration(a.opts.Policy.CommandTimeoutSeconds) * time.Second).UTC()
	fp, err := fingerprint(struct {
		Payload  []byte
		Deadline time.Time
	}{payload, deadline})
	if err != nil {
		return err
	}
	s.Commands[id] = storedCommand{DesiredRevision: s.DesiredRevision, Receipt: device.CommandReceipt{ID: id, Revision: 1, State: device.CommandQueued}, Payload: payload, Deadline: deadline}
	s.Receipts[id] = receipt{Kind: "command", Fingerprint: fp, Revision: 1}
	s.Queue = append(s.Queue, id)
	return a.save(ctx, s)
}

// Status contains no desired/reported values and performs no fleet fanout.
type Status struct {
	Convergence          string                      `json:"convergence"`
	SiteID               string                      `json:"site_id"`
	DesiredRevision      uint64                      `json:"desired_revision"`
	ReportedRevision     uint64                      `json:"reported_revision"`
	Reconciliation       string                      `json:"reconciliation"`
	CommandStates        map[device.CommandState]int `json:"command_states"`
	PendingEvents        int                         `json:"pending_events"`
	NotificationReceipts int                         `json:"notification_receipts"`
	MutationReceipts     int                         `json:"mutation_receipts"`
	StateBytes           int                         `json:"state_bytes"`
	MaxStateBytes        int                         `json:"max_state_bytes"`
	MaxCommands          int                         `json:"max_commands"`
	MaxReceipts          int                         `json:"max_receipts"`
	OldestEventAt        *time.Time                  `json:"oldest_event_at,omitempty"`
	LastContact          time.Time                   `json:"last_contact"`
}

func (a *Actor) GetStatus(ctx context.Context) (Status, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s, err := a.load(ctx)
	if err != nil {
		return Status{}, err
	}
	_, reason := a.planReconciliation(s)
	b, err := json.Marshal(s)
	if err != nil {
		return Status{}, err
	}
	out := Status{Convergence: a.convergence(s), DesiredRevision: s.DesiredRevision, ReportedRevision: s.ReportedRevision, Reconciliation: reason, CommandStates: map[device.CommandState]int{}, PendingEvents: len(s.Outbox), NotificationReceipts: len(s.Inbox), MutationReceipts: len(s.Receipts), StateBytes: len(b), MaxStateBytes: a.opts.MaxBytes, MaxCommands: a.opts.MaxCommands, MaxReceipts: a.opts.MaxReceipts, LastContact: s.LastContact}
	if a.opts.Policy != nil {
		out.SiteID = a.opts.Policy.SiteID
	}
	for _, c := range s.Commands {
		out.CommandStates[c.Receipt.State]++
	}
	if len(s.Outbox) > 0 {
		at := s.Outbox[0].ReceivedAt
		out.OldestEventAt = &at
	}
	return out, nil
}

func (a *Actor) convergence(s *snapshot) string {
	state := "converged"
	for path, want := range s.Desired {
		if a.opts.Policy != nil && !a.opts.Policy.Allows(path, want, true) {
			return "unknown"
		}
		got, ok := s.Reported[path]
		if !ok || !a.opts.Now().Before(got.FreshUntil) {
			return "unknown"
		}
		if got.Value != want {
			state = "pending"
		}
	}
	return state
}
