package config

import (
	"example.com/usp-controller/internal/device"
	"testing"
)

func fleetConfig() Config {
	return Config{EndpointID: "controller", ClientID: "client", Broker: "tls://localhost:8883", Agents: map[string]Agent{"device": {Topic: "agent", Inbox: "inbox"}}, Listen: "127.0.0.1:8080", JournalPath: "events", JournalMaxBytes: 100, RequestTimeoutSeconds: 30, MaxPending: 10, QoS: 1, ActorCanary: &ActorCanary{Enabled: true, Tenant: "tenant", Devices: []string{"device"}, DaprURL: "http://127.0.0.1:3500", CallbackListen: "127.0.0.1:8081", LockPath: "data/canary.lock", EventPubsub: "events", EventTopic: "notifications", Twin: &device.TwinPolicy{SiteID: "site", CommandTimeoutSeconds: 30, Paths: map[string]device.ManagedPath{"Device.Test.Value": {Type: "string"}}}, Fleet: &FleetProjection{Directory: "data/fleet"}}}
}
func TestFleetExplicitOptInAndBounds(t *testing.T) {
	c := fleetConfig()
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.ActorCanary.Enabled = false }, func(c *Config) { c.ActorCanary.Twin = nil }, func(c *Config) { c.ActorCanary.Fleet.Directory = "" }, func(c *Config) { c.ActorCanary.Fleet.Workers = 17 }, func(c *Config) { c.ActorCanary.Fleet.RefreshSeconds = 1 }, func(c *Config) { c.ActorCanary.Fleet.StaleSeconds = 30 }, func(c *Config) { c.ActorCanary.Fleet.Workers = -1 },
	} {
		c := fleetConfig()
		mutate(&c)
		if c.Validate() == nil {
			t.Fatal("unsafe fleet config")
		}
	}
	legacy, err := Load("../../config.example.json")
	if err != nil {
		t.Fatal(err)
	}
	if legacy.ActorCanary != nil && legacy.ActorCanary.Fleet != nil {
		t.Fatal("legacy enables fleet projection")
	}
	c = fleetConfig()
	c.ActorCanary.Fleet = nil
	if err = c.Validate(); err != nil {
		t.Fatal("existing actor config changed", err)
	}
}
