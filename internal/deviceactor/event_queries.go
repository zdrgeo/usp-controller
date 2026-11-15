package deviceactor

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
)

const MaxEventPageSize = 100
const MaxEventCursorBytes = 2048

type eventCursor struct {
	Version int    `json:"version"`
	Device  string `json:"device"`
	View    string `json:"view"`
	After   uint64 `json:"after"`
}

func eventSummary(e storedEvent) (device.EventSummary, error) {
	msg := new(usp.Msg)
	if err := proto.Unmarshal(e.Payload, msg); err != nil {
		return device.EventSummary{}, err
	}
	n := msg.GetBody().GetRequest().GetNotify()
	kind := ""
	switch n.GetNotification().(type) {
	case *usp.Notify_Event_:
		kind = "event"
	case *usp.Notify_ValueChange_:
		kind = "value_change"
	case *usp.Notify_ObjCreation:
		kind = "object_creation"
	case *usp.Notify_ObjDeletion:
		kind = "object_deletion"
	case *usp.Notify_OperComplete:
		kind = "operation_complete"
	case *usp.Notify_OnBoardReq:
		kind = "on_board_request"
	default:
		return device.EventSummary{}, fmt.Errorf("invalid pending event notification")
	}
	return device.EventSummary{Epoch: e.Epoch, Sequence: e.Sequence, ReceivedAt: e.ReceivedAt, Kind: kind}, nil
}

// ListEvents reads one bounded snapshot without exporting, repairing reminders,
// expiry, saves or dispatch. FIFO sequence order is already validated on load.
func (a *Actor) ListEvents(ctx context.Context, q device.EventQuery) (device.EventPage, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	zero := device.EventPage{}
	if q.Limit == 0 {
		q.Limit = 20
	}
	if q.Limit < 1 || q.Limit > MaxEventPageSize || len(q.Cursor) > MaxEventCursorBytes {
		return zero, ErrInvalid
	}
	identity, err := fingerprint(a.id)
	if err != nil {
		return zero, err
	}
	var cursor eventCursor
	if q.Cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(q.Cursor)
		if err != nil || json.Unmarshal(raw, &cursor) != nil || cursor.Version != 1 || cursor.Device != identity || len(cursor.View) != 64 || cursor.After == 0 {
			return zero, ErrInvalid
		}
	}
	s, err := a.load(ctx)
	if err != nil {
		return zero, err
	}
	events := make([]device.EventSummary, 0, len(s.Outbox))
	for _, e := range s.Outbox {
		row, err := eventSummary(e)
		if err != nil {
			return zero, err
		}
		events = append(events, row)
	}
	view, err := fingerprint(events)
	if err != nil {
		return zero, err
	}
	if q.Cursor != "" && cursor.View != view {
		return zero, ErrConflict
	}
	start := 0
	if q.Cursor != "" {
		index := sort.Search(len(events), func(i int) bool { return events[i].Sequence >= cursor.After })
		if index == len(events) || events[index].Sequence != cursor.After {
			return zero, ErrInvalid
		}
		start = index + 1
	}
	end := min(start+q.Limit, len(events))
	page := device.EventPage{Events: append([]device.EventSummary{}, events[start:end]...), Total: len(events)}
	if end < len(events) {
		raw, err := json.Marshal(eventCursor{Version: 1, Device: identity, View: view, After: events[end-1].Sequence})
		if err != nil {
			return zero, err
		}
		page.NextCursor = base64.RawURLEncoding.EncodeToString(raw)
		if len(page.NextCursor) > MaxEventCursorBytes {
			return zero, ErrCapacity
		}
	}
	return page, nil
}
