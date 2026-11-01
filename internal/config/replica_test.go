package config

import "testing"

func TestReplicaProfileValidation(t *testing.T) {
	base := Config{EndpointID: "controller", ClientID: "site", Broker: "tls://broker:8883", Agents: map[string]Agent{"device": {Topic: "d", Inbox: "c/d"}}, Listen: "127.0.0.1:8080", JournalPath: "events", JournalMaxBytes: 100, RequestTimeoutSeconds: 30, MaxPending: 10, QoS: 1}
	base.ActorCanary = &ActorCanary{Enabled: true, Tenant: "tenant", Devices: []string{"device"}, DaprURL: "http://127.0.0.1:3500", CallbackListen: "127.0.0.1:8081", LockPath: "data/replica.lock", EventPubsub: "events", EventTopic: "notifications", Replica: &Replica{InstanceID: "pod-a", SharedGroup: "site", AppID: "controller", StateStore: "actors"}}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	if base.MQTTClientID() != "site/pod-a" || base.InboxSubscription("c/d") != "$share/site/c/d" {
		t.Fatal("replica routing")
	}
	for _, mutate := range []func(*Config){
		func(c *Config) { c.ActorCanary.Enabled = false },
		func(c *Config) { c.ActorCanary.Devices = nil },
		func(c *Config) { c.ActorCanary.Replica.InstanceID = "" },
		func(c *Config) { c.ActorCanary.Replica.SharedGroup = "bad/group" },
		func(c *Config) { c.ActorCanary.Replica.AppID = "" },
		func(c *Config) { c.ActorCanary.Replica.StateStore = "" },
	} {
		c := base
		a := *base.ActorCanary
		r := *a.Replica
		c.ActorCanary = &a
		a.Replica = &r
		mutate(&c)
		if err := c.Validate(); err == nil {
			t.Fatal("unsafe replica config", c)
		}
	}
	base.ActorCanary.Replica = nil
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	if base.MQTTClientID() != "site" || base.InboxSubscription("c/d") != "c/d" {
		t.Fatal("changed single-host mode")
	}
	base.ActorCanary = nil
	if base.ReplicaMode() {
		t.Fatal("changed legacy mode")
	}
}
