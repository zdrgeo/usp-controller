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
