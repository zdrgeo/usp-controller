package deviceactor

import (
	"context"
	"sort"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
)

// RefreshTwin is explicit command admission, never immediate dispatch. Policy is
// copied at construction and immutable. Accepted scope is an ordinary Get; later
// policy changes may limit projection but do not rewrite accepted command intent.
func (a *Actor) RefreshTwin(ctx context.Context, r device.RefreshTwinRequest) (device.CommandReceipt, error) {
	if a.transport == nil || a.opts.Policy == nil || !a.opts.Policy.ObserveGetResponses {
		return device.CommandReceipt{}, ErrDisabled
	}
	if !validExecutionID(r.RequestID) || r.Deadline.IsZero() {
		return device.CommandReceipt{}, ErrInvalid
	}
	paths := make([]string, 0, len(a.opts.Policy.Paths))
	for path := range a.opts.Policy.Paths {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return a.EnqueueCommand(ctx, device.CommandRequest{ID: r.RequestID, Deadline: r.Deadline, Request: &usp.Request{ReqType: &usp.Request_Get{Get: &usp.Get{ParamPaths: paths, MaxDepth: 1}}}})
}
