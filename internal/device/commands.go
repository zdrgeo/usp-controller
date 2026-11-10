package device

import "time"

// CommandQuery pages a single actor's retained command metadata. Cursor is opaque;
// it is bound to the device, state filter and an unchanged command/control view.
type CommandQuery struct {
	Limit  int          `json:"limit"`
	Cursor string       `json:"cursor,omitempty"`
	State  CommandState `json:"state,omitempty"`
}

type CommandSummary struct {
	AttemptedAt         *time.Time   `json:"attempted_at,omitempty"`
	AttemptSequence     uint64       `json:"attempt_sequence,omitempty"`
	RequestID           RequestID    `json:"request_id"`
	Revision            uint64       `json:"revision"`
	State               CommandState `json:"state"`
	Deadline            time.Time    `json:"deadline"`
	ObservationRevision uint64       `json:"observation_revision,omitempty"`
	DesiredRevision     uint64       `json:"desired_revision,omitempty"`
	QueuePosition       int          `json:"queue_position,omitempty"` // one-based in persisted FIFO
	BlockingUnknown     bool         `json:"blocking_unknown"`
	UnknownAcknowledged bool         `json:"unknown_acknowledged"`
	HasResponse         bool         `json:"has_response"`
	HasCompletion       bool         `json:"has_completion"`
}
type CommandPage struct {
	Commands          []CommandSummary `json:"commands"`
	Total             int              `json:"total"`
	NextCursor        string           `json:"next_cursor,omitempty"`
	ExecutionPaused   bool             `json:"execution_paused"`
	ExecutionRevision uint64           `json:"execution_revision"`
}
