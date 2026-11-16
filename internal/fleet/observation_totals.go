package fleet

import "example.com/usp-controller/internal/deviceactor"

// ObservationTotals aggregates only fresh projection rows. Missing diagnostics
// remain explicit so older hosts cannot silently appear to have full coverage.
// It contains neither path names nor values; sample times remain on each row.
type ObservationTotals struct {
	CoveredDevices      int `json:"covered_devices"`
	UnavailableDevices  int `json:"unavailable_devices"`
	ManagedPaths        int `json:"managed_paths"`
	Fresh               int `json:"fresh"`
	Missing             int `json:"missing"`
	Invalid             int `json:"invalid"`
	Future              int `json:"future"`
	MutationInvalidated int `json:"mutation_invalidated"`
	TopologyInvalidated int `json:"topology_invalidated"`
	Stale               int `json:"stale"`
}

func (t *ObservationTotals) add(c *deviceactor.ObservationCoverage) {
	if c == nil {
		t.UnavailableDevices++
		return
	}
	t.CoveredDevices++
	t.ManagedPaths += c.ManagedPaths
	t.Fresh += c.Fresh
	t.Missing += c.Missing
	t.Invalid += c.Invalid
	t.Future += c.Future
	t.MutationInvalidated += c.MutationInvalidated
	t.TopologyInvalidated += c.TopologyInvalidated
	t.Stale += c.Stale
}
