package deviceactor

import (
	"encoding/json"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
)

func TestGetObservationWindowSurvivesFreshnessConfigurationChange(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		initial, replacement, arrival time.Duration
		project                       bool
	}{
		{"longer-config-cannot-extend", time.Minute, 3 * time.Minute, 90 * time.Second, false},
		{"shorter-config-cannot-truncate", 3 * time.Minute, time.Minute, 90 * time.Second, true},
		{"expiry-equality-is-stale", time.Minute, 3 * time.Minute, time.Minute, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a, b, f, now := managedActor(t)
			a.opts.Policy.ObserveGetResponses = true
			a.opts.Freshness = tc.initial
			started := *now
			if _, err := a.EnqueueCommand(ctx, command("read", now.Add(time.Hour))); err != nil {
				t.Fatal(err)
			}
			if err := a.Tick(ctx); err != nil {
				t.Fatal(err)
			}
			c, _ := a.GetCommand(ctx, "read")
			s, err := a.load(ctx)
			if err != nil || s.Commands["read"].ObservationFreshUntil == nil || !s.Commands["read"].ObservationFreshUntil.Equal(started.Add(tc.initial)) {
				t.Fatal("Get begin did not pin its window", s, err)
			}
			opts := a.opts
			opts.Freshness = tc.replacement
			restarted, err := NewExecuting(identity, b, opts, f)
			if err != nil {
				t.Fatal(err)
			}
			*now = started.Add(tc.arrival)
			msg := getObservationResponse(c.MessageID, getPathResult("Device.Test.", "Device.Test.", map[string]string{"Value": "observed"}))
			if err := restarted.HandleResponse(ctx, device.DeviceResponse{Identity: identity, Message: msg, ReceivedAt: *now}); err != nil {
				t.Fatal(err)
			}
			twin, err := restarted.GetTwin(ctx)
			if err != nil || (len(twin.Reported) > 0) != tc.project {
				t.Fatal("replacement configuration changed begun read eligibility", twin, err)
			}
			if tc.project && !twin.Reported["Device.Test.Value"].FreshUntil.Equal(started.Add(tc.initial)) {
				t.Fatal("read freshness was re-anchored", twin)
			}
			settled, err := restarted.GetCommand(ctx, "read")
			if err != nil || settled.State != device.CommandSucceeded || !proto.Equal(settled.Result, msg) || len(f.calls) != 1 {
				t.Fatal("projection changed command outcome/evidence or replayed", settled, err)
			}
		})
	}
}

func TestGetObservationWindowProvenanceAndLegacyUpgrade(t *testing.T) {
	a, b, _, now := managedActor(t)
	if _, err := a.EnqueueCommand(ctx, command("read", now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if err := a.Tick(ctx); err != nil {
		t.Fatal(err)
	}
	original := append([]byte(nil), b.state...)
	for _, mode := range []string{"older-schema", "nonpositive-window", "non-get"} {
		t.Run(mode, func(t *testing.T) {
			var s snapshot
			if err := json.Unmarshal(original, &s); err != nil {
				t.Fatal(err)
			}
			c := s.Commands["read"]
			switch mode {
			case "older-schema":
				s.Schema = 11
			case "nonpositive-window":
				at := c.AttemptedAt
				c.ObservationFreshUntil = &at
			case "non-get":
				c.Payload, _ = proto.Marshal(&usp.Request{ReqType: &usp.Request_Set{Set: &usp.Set{}}})
			}
			s.Commands["read"] = c
			b.state, _ = json.Marshal(s)
			if _, err := a.load(ctx); err == nil {
				t.Fatal("invalid observation window accepted")
			}
		})
	}
	var legacy snapshot
	if err := json.Unmarshal(original, &legacy); err != nil {
		t.Fatal(err)
	}
	legacy.Schema = 11
	c := legacy.Commands["read"]
	c.ObservationFreshUntil = nil
	legacy.Commands["read"] = c
	b.state, _ = json.Marshal(legacy)
	s, err := a.load(ctx)
	if err != nil || s.Schema != 12 || s.Commands["read"].ObservationFreshUntil != nil {
		t.Fatal("upgrade invented an observation window", s, err)
	}
	if err := a.save(ctx, s); err != nil {
		t.Fatal(err)
	}
	s, err = a.load(ctx)
	if err != nil || s.Commands["read"].ObservationFreshUntil != nil {
		t.Fatal("migration save invented historical configuration", s, err)
	}
}
