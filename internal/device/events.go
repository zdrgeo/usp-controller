package device

import "time"

// EventQuery inspects pending outbox metadata only, never published history.
type EventQuery struct {
	Limit  int    `json:"limit"`
	Cursor string `json:"cursor,omitempty"`
}
type EventSummary struct {
	Epoch      string    `json:"epoch"`
	Sequence   uint64    `json:"sequence"`
	ReceivedAt time.Time `json:"received_at"`
	Kind       string    `json:"kind"`
}
type EventPage struct {
	Events     []EventSummary `json:"events"`
	Total      int            `json:"total"`
	NextCursor string         `json:"next_cursor,omitempty"`
}
