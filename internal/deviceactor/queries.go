package deviceactor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sort"
	"time"

	"example.com/usp-controller/internal/device"
)

const MaxCommandPageSize = 100
const MaxCommandCursorBytes = 2048

type commandCursor struct {
	Version int                 `json:"version"`
	Device  string              `json:"device"`
	View    string              `json:"view"`
	State   device.CommandState `json:"state,omitempty"`
	After   device.RequestID    `json:"after"`
}

func ValidCommandFilter(state device.CommandState) bool {
	switch state {
	case "", device.CommandQueued, device.CommandAttempting, device.CommandAwaitingResponse, device.CommandAwaitingCompletion, device.CommandSucceeded, device.CommandFailed, device.CommandUnknown, device.CommandExpired, device.CommandCancelled:
		return true
	default:
		return false
	}
}

// ListCommands performs one bounded snapshot load and no writes or reminder
// repairs. Sort order is lexical request ID; queue_position conveys FIFO order.
func (a *Actor) ListCommands(ctx context.Context, q device.CommandQuery) (device.CommandPage, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	zero := device.CommandPage{}
	if q.Limit == 0 {
		q.Limit = 20
	}
	if q.Limit < 1 || q.Limit > MaxCommandPageSize || len(q.Cursor) > MaxCommandCursorBytes || !ValidCommandFilter(q.State) {
		return zero, ErrInvalid
	}
	identity, err := fingerprint(a.id)
	if err != nil {
		return zero, err
	}
	var cursor commandCursor
	if q.Cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(q.Cursor)
		if err != nil || json.Unmarshal(raw, &cursor) != nil || cursor.Version != 1 || cursor.Device != identity || len(cursor.View) != 64 || cursor.State != q.State || cursor.After == "" {
			return zero, ErrInvalid
		}
	}
	s, err := a.load(ctx)
	if err != nil {
		return zero, err
	}
	positions := map[device.RequestID]int{}
	for i, id := range s.Queue {
		positions[id] = i + 1
	}
	commands := make([]device.CommandSummary, 0, len(s.Commands))
	for id, c := range s.Commands {
		var outcomeAt *time.Time
		if !c.MutationOutcomeAt.IsZero() {
			at := c.MutationOutcomeAt
			outcomeAt = &at
		}
		var attemptedAt *time.Time
		if !c.AttemptedAt.IsZero() {
			at := c.AttemptedAt
			attemptedAt = &at
		}
		commands = append(commands, device.CommandSummary{MutationOutcomeAt: outcomeAt, AttemptedAt: attemptedAt, AttemptSequence: c.AttemptSequence, RequestID: id, Revision: c.Receipt.Revision, State: c.Receipt.State, Deadline: c.Deadline, VerificationRevision: c.VerificationRevision, ObservationRevision: c.ObservationRevision, DesiredRevision: c.DesiredRevision, QueuePosition: positions[id], BlockingUnknown: blocksUnknown(c), UnknownAcknowledged: c.UnknownAcknowledgement != nil, HasResponse: len(c.Result) > 0, HasCompletion: len(c.Completion) > 0})
	}
	sort.Slice(commands, func(i, j int) bool { return commands[i].RequestID < commands[j].RequestID })
	// Include all command metadata and the visible control revision, not just the
	// filtered page, so a command moving in/out of the filter cannot be missed.
	view, err := fingerprint(struct {
		Commands []device.CommandSummary
		Paused   bool
		Revision uint64
	}{commands, s.Execution.Paused, s.Execution.Revision})
	if err != nil {
		return zero, err
	}
	if q.Cursor != "" && cursor.View != view {
		return zero, ErrConflict
	}
	matches := make([]device.CommandSummary, 0, len(commands))
	for _, c := range commands {
		if q.State == "" || c.State == q.State {
			matches = append(matches, c)
		}
	}
	start := 0
	if q.Cursor != "" {
		index := sort.Search(len(matches), func(i int) bool { return matches[i].RequestID >= cursor.After })
		if index == len(matches) || matches[index].RequestID != cursor.After {
			return zero, ErrInvalid
		}
		start = index + 1
	}
	end := min(start+q.Limit, len(matches))
	page := device.CommandPage{Commands: append([]device.CommandSummary{}, matches[start:end]...), Total: len(matches), ExecutionPaused: s.Execution.Paused, ExecutionRevision: s.Execution.Revision}
	if end < len(matches) {
		raw, err := json.Marshal(commandCursor{Version: 1, Device: identity, View: view, State: q.State, After: matches[end-1].RequestID})
		if err != nil {
			return zero, err
		}
		page.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
		if len(page.NextCursor) > MaxCommandCursorBytes {
			return zero, ErrCapacity
		}
	}
	return page, nil
}
