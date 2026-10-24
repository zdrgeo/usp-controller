// Package storage defines the backend-neutral persistence contracts for the
// Controller. See docs/storage-contracts.md for normative transition, durability,
// retention, and concurrency rules. No implementation is selected here.
package storage

import (
	"context"
	"errors"
	"time"
)

var (
	ErrNotFound    = errors.New("storage: not found")
	ErrConflict    = errors.New("storage: conflicting content or state")
	ErrLeaseLost   = errors.New("storage: lease expired or superseded")
	ErrInvalid     = errors.New("storage: invalid input")
	ErrCapacity    = errors.New("storage: capacity exhausted")
	ErrUnavailable = errors.New("storage: unavailable")
	ErrUnsupported = errors.New("storage: unsupported capability")
	ErrClosed      = errors.New("storage: closed")
)

// Scope isolates a logical Controller's data; it remains stable across replicas.
// It is not a process ID. Stores must never match keys across scopes.
type Scope string

// CommandKey is the caller-supplied command identity and submission idempotency
// identifier. Reuse it for retries; use a new ID for a new intended execution.
type CommandKey struct {
	Scope Scope
	ID    string
}
type EventKey struct {
	Scope Scope
	ID    string
}
type DeliveryKey struct {
	Event       EventKey
	Destination string
}

// Message is an immutable, canonical encoding of a USP Msg, not a transport
// Record. Canonical bytes permit duplicate-content comparison across transports.
// Backend adapters do not interpret protobufs or classify USP results.
type Message struct {
	ProtocolVersion string
	MessageType     string // USP enum name, e.g. GET, GET_RESP, NOTIFY.
	Payload         []byte // Deterministic protobuf serialization of the USP Msg.
}

// Lease tokens are opaque, durable, and never reused for the same resource.
// ExpiresAt is informational: the store's clock decides whether a lease is valid.
type Lease struct {
	Owner     string // Unique worker incarnation, not a reusable pod name.
	Token     string
	ExpiresAt time.Time
}

type CommandLease struct {
	Key   CommandKey
	Lease Lease
}
type DeliveryLease struct {
	Key   DeliveryKey
	Lease Lease
}

// ClaimOptions requires a positive bounded Limit and LeaseDuration. Claiming is
// atomic; no two callers can hold valid leases for the same work item.
type ClaimOptions struct {
	Scope         Scope
	Owner         string
	Limit         int
	LeaseDuration time.Duration
}

type Topology string

const (
	ExclusiveProcess Topology = "exclusive_process"
	SharedProcesses  Topology = "shared_processes"
)

type Durability string

const (
	// LocalCommit survives process/OS restart with intact durable storage.
	LocalCommit Durability = "local_commit"
	// ReplicatedCommit additionally survives failures specified by the adapter's
	// configured replication policy. It is not an unlimited no-data-loss promise.
	ReplicatedCommit Durability = "replicated_commit"
)

type Capabilities struct {
	Topology   Topology
	Durability Durability
	// FailureModel names the configured guarantee, such as surviving one node
	// loss. The adapter must document its assumptions and failover limitations.
	FailureModel string
}

// Store is one transactional domain. IngestEvent can atomically update Commands
// and Events; implementations cannot expose unrelated stores behind this API.
// All required methods are supported in both topology modes. Capabilities
// describe deployment guarantees, not permission to weaken atomicity.
type Store interface {
	CommandStore
	EventStore
	Prune(context.Context, Prune) (PruneResult, error)
	Capabilities() Capabilities
	Ping(context.Context) error
	// Close is idempotent. Operations after Close return ErrClosed.
	Close() error
}

type PageRequest struct {
	Limit  int
	Cursor string // Opaque; bound to the original scope and filter.
}
