package deviceactor

import (
	"errors"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

func TestRefreshRequiresExecutionAndProjectionPolicy(t *testing.T) {
	for _, mode := range []string{"shadow", "no-policy", "projection-off"} {
		t.Run(mode, func(t *testing.T) {
			a, b, _, now := managedActor(t)
			a.opts.Policy.ObserveGetResponses = true
			switch mode {
			case "shadow":
				a.transport = nil
			case "no-policy":
				a.opts.Policy = nil
			case "projection-off":
				a.opts.Policy.ObserveGetResponses = false
			}
			saves := b.saves
			if _, err := a.RefreshTwin(ctx, device.RefreshTwinRequest{RequestID: "refresh", Deadline: now.Add(time.Hour)}); !errors.Is(err, ErrDisabled) || b.saves != saves {
				t.Fatal("refresh accepted without opt-in projection", err)
			}
		})
	}
}
