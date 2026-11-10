package deviceactor

import (
	"context"
	"fmt"
	"math"
	"strings"

	"example.com/usp-controller/internal/device"
)

func validExecutionID(id device.RequestID) bool {
	return id != "" && len(id) <= 128 && !strings.HasPrefix(string(id), "reconcile-")
}

// Bind the latest control state to its durable mutation receipt. A missing pause
// field or a forged downgrade must not silently reopen execution after restart.
func validateExecution(s *snapshot, sourceSchema int) error {
	if sourceSchema < 5 {
		if s.Execution != nil {
			return fmt.Errorf("execution control in an older schema")
		}
		s.Execution = &device.ExecutionState{}
	} else if s.Execution == nil {
		return fmt.Errorf("execution control missing")
	}
	control := s.Execution
	revisions := map[uint64]bool{}
	for _, receipt := range s.Receipts {
		if receipt.Kind != "execution_control" {
			continue
		}
		if receipt.Revision == 0 || receipt.Revision > control.Revision || revisions[receipt.Revision] {
			return fmt.Errorf("inconsistent execution control history")
		}
		revisions[receipt.Revision] = true
	}
	if uint64(len(revisions)) != control.Revision {
		return fmt.Errorf("incomplete execution control history")
	}
	if control.Revision == 0 {
		if control.Paused || control.RequestID != "" || control.Reason != "" || !control.UpdatedAt.IsZero() {
			return fmt.Errorf("invalid initial execution control")
		}
		return nil
	}
	if !validExecutionID(control.RequestID) || control.Reason == "" || strings.TrimSpace(control.Reason) != control.Reason || len(control.Reason) > 1024 || control.UpdatedAt.IsZero() {
		return fmt.Errorf("invalid execution control")
	}
	fp, err := fingerprint(device.SetExecutionRequest{RequestID: control.RequestID, ExpectedRevision: control.Revision - 1, Paused: control.Paused, Reason: control.Reason})
	receipt, ok := s.Receipts[control.RequestID]
	if err != nil || !ok || receipt.Kind != "execution_control" || receipt.Revision != control.Revision || receipt.Fingerprint != fp {
		return fmt.Errorf("inconsistent execution control receipt")
	}
	return nil
}

func (a *Actor) GetExecution(ctx context.Context) (device.ExecutionState, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	s, err := a.load(ctx)
	if err != nil {
		return device.ExecutionState{}, err
	}
	return *s.Execution, nil
}

// SetExecution changes only future dispatch eligibility. Stable request IDs bind
// the original decision; retrying an old resume cannot undo a newer pause.
func (a *Actor) SetExecution(ctx context.Context, r device.SetExecutionRequest) (device.ExecutionReceipt, error) {
	if a.transport == nil {
		return device.ExecutionReceipt{}, ErrDisabled
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	r.Reason = strings.TrimSpace(r.Reason)
	if !validExecutionID(r.RequestID) || r.Reason == "" || len(r.Reason) > 1024 {
		return device.ExecutionReceipt{}, ErrInvalid
	}
	fp, err := fingerprint(r)
	if err != nil {
		return device.ExecutionReceipt{}, err
	}
	s, err := a.load(ctx)
	if err != nil {
		return device.ExecutionReceipt{}, err
	}
	prior, duplicate, err := check(s, r.RequestID, "execution_control", fp)
	if err != nil {
		return device.ExecutionReceipt{}, err
	}
	if !duplicate {
		if s.Execution.Revision != r.ExpectedRevision {
			return device.ExecutionReceipt{}, ErrConflict
		}
		if s.Execution.Revision == math.MaxUint64 {
			return device.ExecutionReceipt{}, ErrCapacity
		}
	}
	// A pause can be persisted even when scheduling is unavailable. Resuming and
	// resume retries repair the reminder before claiming the decision is accepted.
	if !r.Paused {
		if err := a.backend.EnsureReminder(ctx); err != nil {
			return device.ExecutionReceipt{}, err
		}
	}
	if duplicate {
		return device.ExecutionReceipt{RequestID: r.RequestID, Revision: prior.Revision, Paused: r.Paused}, nil
	}
	revision := s.Execution.Revision + 1
	s.Execution = &device.ExecutionState{Paused: r.Paused, Revision: revision, RequestID: r.RequestID, Reason: r.Reason, UpdatedAt: a.opts.Now().UTC()}
	s.Receipts[r.RequestID] = receipt{Kind: "execution_control", Fingerprint: fp, Revision: revision}
	if err := a.save(ctx, s); err != nil {
		return device.ExecutionReceipt{}, err
	}
	return device.ExecutionReceipt{RequestID: r.RequestID, Revision: revision, Paused: r.Paused}, nil
}
