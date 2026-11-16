package device

import "time"

// TwinDiff is a bounded observation-only explanation, not a dispatch plan.
// Values, command bodies and free-form reasons are deliberately excluded.
type TwinDiff struct {
	TopologyBarrierAt   *time.Time     `json:"topology_barrier_at,omitempty"`
	AsOf                time.Time      `json:"as_of"`
	DesiredRevision     uint64         `json:"desired_revision"`
	ReportedRevision    uint64         `json:"reported_revision"`
	MutationBarrierAt   *time.Time     `json:"mutation_barrier_at,omitempty"`
	Convergence         string         `json:"convergence"`
	HiddenDesiredPaths  int            `json:"hidden_desired_paths"`
	Paths               []TwinPathDiff `json:"paths"`
	HiddenReportedPaths int            `json:"hidden_reported_paths"`
	ObservationPaths    []TwinPathDiff `json:"observation_paths"`
}

type TwinPathDiff struct {
	Path           string     `json:"path"`
	State          string     `json:"state"`
	HasObservation bool       `json:"has_observation"`
	ReceivedAt     *time.Time `json:"received_at,omitempty"`
	FreshUntil     *time.Time `json:"fresh_until,omitempty"`
}
