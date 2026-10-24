package storage

import (
	"context"
	"time"
)

// NewEvent represents one validated incoming Notify. The protocol identity
// (Scope, AgentID, ControllerID, MessageID) is also a unique deduplication key.
// Key.ID is allocated before ingestion and reused after an ambiguous write.
type NewEvent struct {
	Key            EventKey
	AgentID        string
	ControllerID   string
	MessageID      string
	SubscriptionID string
	Message        Message
	ExpireAt       time.Time
	// Logical, versioned destinations (e.g. local-handler/v1 or cloud-export/v1).
	// No URLs or credentials. Routes are frozen at first successful ingestion.
	Destinations []string
}

type IngestEvent struct {
	Event NewEvent
	// Optional completion evidence for an existing asynchronous command. Event,
	// delivery rows, and observation commit together. A missing/mismatched command
	// rejects the entire transaction; retry ingestion without this field to keep
	// an orphan notification for explicit later reconciliation.
	Completion *CommandObservation
}

type Event struct {
	NewEvent
	ReceivedAt time.Time // Store-assigned; retries never change first receipt time.
}

type IngestResult struct {
	Event   Event
	Created bool
	// Non-nil when this transaction applied/deduplicated Completion evidence.
	Completion *ObservationResult
}

type DeliveryState string

const (
	DeliveryPending      DeliveryState = "pending"
	DeliveryLeased       DeliveryState = "leased"
	DeliveryAcknowledged DeliveryState = "acknowledged"
	DeliveryDead         DeliveryState = "dead"
)

type Delivery struct {
	Key         DeliveryKey
	State       DeliveryState
	Revision    uint64
	Attempts    uint64 // Incremented on each claim, not a network-send count.
	AvailableAt time.Time
	Lease       *Lease
	LastError   string
	Receipt     string // Optional downstream durable-acceptance identifier.
	UpdatedAt   time.Time
}

type DeliveryClaim struct {
	ClaimOptions
	Destination string
}
type ClaimedDelivery struct {
	Event    Event
	Delivery Delivery
	Handle   DeliveryLease
}

type DeliveryResult struct {
	Handle     DeliveryLease
	State      DeliveryState // acknowledged, pending (retry), or dead.
	RetryAfter time.Duration // Required positive duration when returning to pending.
	Error      string
	Receipt    string
}

type EventQuery struct {
	Scope   Scope
	AgentID string
	Page    PageRequest
}
type EventPage struct {
	Items      []Event
	NextCursor string
}
type DeliveryQuery struct {
	Scope       Scope
	Destination string
	States      []DeliveryState
	Page        PageRequest
}
type DeliveryPage struct {
	Items      []Delivery
	NextCursor string
}

type RequeueDelivery struct {
	Key              DeliveryKey
	ExpectedRevision uint64
	Reason           string // Operator/application justification; recorded durably.
}

// Prune requires bounded work. ExpireAt is a minimum, not an unconditional TTL.
// Unfinished commands, unknown outcomes, pending/leased/dead deliveries and
// references from retained records block deletion. Pruning also ends dedup for
// removed identities; adapters must document and enforce the retry horizon.
type Prune struct {
	Scope  Scope
	Before time.Time
	Limit  int
}
type PruneResult struct {
	Commands   int
	Events     int
	Deliveries int
}

type EventStore interface {
	// IngestEvent atomically inserts an inbox entry, one pending delivery per
	// destination, dedup indexes and optional command completion. Only a successful
	// commit (including an identical duplicate) permits USP NotifyResp.
	IngestEvent(context.Context, IngestEvent) (IngestResult, error)
	GetEvent(context.Context, EventKey) (Event, error)
	ListEvents(context.Context, EventQuery) (EventPage, error)
	GetDelivery(context.Context, DeliveryKey) (Delivery, error)
	ListDeliveries(context.Context, DeliveryQuery) (DeliveryPage, error)
	// ClaimDeliveries also reclaims expired delivery leases. All new claims get
	// fresh tokens; an old worker cannot acknowledge/retry after losing its lease.
	ClaimDeliveries(context.Context, DeliveryClaim) ([]ClaimedDelivery, error)
	RenewDeliveryLease(context.Context, DeliveryLease, time.Duration) (DeliveryLease, error)
	FinishDelivery(context.Context, DeliveryResult) (Delivery, error)
	// Dead deliveries require explicit operator/application action to retry.
	RequeueDelivery(context.Context, RequeueDelivery) (Delivery, error)
}
