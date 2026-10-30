package config

import "testing"

func TestRejectInsecureAndAmbiguousTopics(t *testing.T) {
	c := Config{EndpointID: "controller", ClientID: "client", Broker: "tls://localhost:8883", Agents: map[string]Agent{"agent": {Topic: "agent", Inbox: "controller/agent"}}, JournalPath: "events", JournalMaxBytes: 100, RequestTimeoutSeconds: 30, MaxPending: 10, QoS: 1}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.Broker = "mqtt://localhost:1883"
	if c.Validate() == nil {
		t.Fatal("plaintext accepted by default")
	}
	c.AllowInsecureMQTT = true
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	c.Agents["second"] = Agent{Topic: "other", Inbox: "controller/agent"}
	if c.Validate() == nil {
		t.Fatal("duplicate inbox accepted")
	}
	delete(c.Agents, "second")
	c.Agents["agent"] = Agent{Topic: "agent/#", Inbox: "inbox"}
	if c.Validate() == nil {
		t.Fatal("wildcard accepted")
	}
}

func TestActorCanaryExplicitAllowlist(t *testing.T) {
	base := Config{EndpointID: "controller", ClientID: "client", Broker: "tls://localhost:8883", Agents: map[string]Agent{"device": {Topic: "agent", Inbox: "inbox"}}, Listen: "127.0.0.1:8080", JournalPath: "events", JournalMaxBytes: 100, RequestTimeoutSeconds: 30, MaxPending: 10, QoS: 1}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	base.ActorCanary = &ActorCanary{Enabled: true, EventPubsub: "events", EventTopic: "device-events", Tenant: "tenant", Devices: []string{"device"}, DaprURL: "http://127.0.0.1:3500", CallbackListen: "127.0.0.1:8081", LockPath: "data/canary.lock"}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*ActorCanary){func(a *ActorCanary) { a.EventPubsub = "" }, func(a *ActorCanary) { a.EventTopic = "../escape" }, func(a *ActorCanary) { a.Devices = nil }, func(a *ActorCanary) { a.Devices = []string{"other"} }, func(a *ActorCanary) { a.Devices = []string{"device", "device"} }, func(a *ActorCanary) { a.CallbackListen = "0.0.0.0:8081" }, func(a *ActorCanary) { a.DaprURL = "http://remote:3500" }, func(a *ActorCanary) { a.LockPath = "" }} {
		a := *base.ActorCanary
		mutate(&a)
		c := base
		c.ActorCanary = &a
		if err := c.Validate(); err == nil {
			t.Fatal("accepted unsafe canary config", a)
		}
	}
}

func TestLegacyConfigRemainsDefault(t *testing.T) {
	c, err := Load("../../config.example.json")
	if err != nil {
		t.Fatal(err)
	}
	if c.ActorCanary != nil && c.ActorCanary.Enabled {
		t.Fatal("legacy example enabled actor execution")
	}
}
