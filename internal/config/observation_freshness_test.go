package config

import (
	"testing"
	"time"

	"example.com/usp-controller/internal/deviceactor"
)

func TestObservationFreshnessRequiresActorModeAndPreservesDefault(t *testing.T) {
	if got := (ActorCanary{}).ObservationFreshness(); got != deviceactor.DefaultOptions().Freshness {
		t.Fatal("default actor observation window changed", got)
	}
	for _, seconds := range []int{1, 75, MaxObservationFreshnessSeconds} {
		c := fleetConfig()
		c.ActorCanary.ObservationFreshnessSeconds = seconds
		if err := c.Validate(); err != nil || c.ActorCanary.ObservationFreshness() != time.Duration(seconds)*time.Second {
			t.Fatal("bounded actor freshness rejected", seconds, err)
		}
	}
	for _, seconds := range []int{-1, MaxObservationFreshnessSeconds + 1} {
		c := fleetConfig()
		c.ActorCanary.ObservationFreshnessSeconds = seconds
		if c.Validate() == nil {
			t.Fatal("unbounded freshness accepted", seconds)
		}
	}
	c, err := Load("../../config.example.json")
	if err != nil {
		t.Fatal(err)
	}
	c.ActorCanary = &ActorCanary{ObservationFreshnessSeconds: 75}
	if c.Validate() == nil {
		t.Fatal("freshness configuration enabled in legacy mode")
	}
}
