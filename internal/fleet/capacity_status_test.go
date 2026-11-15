package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"example.com/usp-controller/internal/device"
)

func TestProjectionAcceptsCurrentReasonsAndValidatedOptionalCapacity(t *testing.T) {
	if err := validateStatus(status(1), "site"); err != nil {
		t.Fatal("legacy status without headroom rejected", err)
	}
	p, store, _ := fixture(t)
	current := status(2)
	capacity := current.RemainingCapacity()
	current.Capacity = &capacity
	p.invoker = invokeFunc(func(_ context.Context, _ device.DeviceIdentity, method string, _ any) (json.RawMessage, error) {
		if method != "GetStatus" {
			t.Fatal("projection mutated actor", method)
		}
		return json.Marshal(current)
	})
	for _, reason := range []string{"observation_eligible", "observation_recorded", "observation_available", "verification_eligible", "verification_recorded"} {
		current.Reconciliation = reason
		refresh(p, "a")
		row := p.rows["a"]
		if row.LastError != "" || row.Snapshot == nil || row.Snapshot.Reconciliation != reason || row.Snapshot.Capacity == nil || *row.Snapshot.Capacity != capacity {
			t.Fatal("current actor status rejected", row)
		}
	}
	reopened, err := newProjection(p.opts, map[string]device.DeviceIdentity{"a": identity("a"), "b": identity("b")}, p.invoker, store)
	if err != nil || reopened.rows["a"].Snapshot.Capacity == nil || *reopened.rows["a"].Snapshot.Capacity != capacity {
		t.Fatal("capacity status did not survive projection restart", err)
	}
	expected := capacity
	capacity.CommandSlotsRemaining++ // Inconsistent with retained command counters.
	refresh(p, "a")
	row := p.rows["a"]
	if row.LastError != "refresh_failed" || !row.Pending || row.Snapshot == nil || *row.Snapshot.Capacity != expected {
		t.Fatal("bad headroom replaced valid cached evidence", row)
	}
	current.Capacity = nil
	current.Reconciliation = "unreviewed-reason"
	if err = validateStatus(current, "site"); !errors.Is(err, ErrInvalid) {
		t.Fatal("unknown reason accepted", err)
	}
}
