package config

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"
)

type ActorCanary struct {
	Enabled        bool     `json:"enabled"`
	Tenant         string   `json:"tenant"`
	Devices        []string `json:"devices"`
	DaprURL        string   `json:"dapr_url"`
	CallbackListen string   `json:"callback_listen"`
	LockPath       string   `json:"lock_path"`
}

type Config struct {
	ActorCanary           *ActorCanary     `json:"actor_canary,omitempty"`
	EndpointID            string           `json:"endpoint_id"`
	Listen                string           `json:"listen"`
	Broker                string           `json:"broker"`
	ClientID              string           `json:"client_id"`
	Agents                map[string]Agent `json:"agents"`
	CAFile                string           `json:"ca_file"`
	CertFile              string           `json:"cert_file"`
	KeyFile               string           `json:"key_file"`
	AllowInsecureMQTT     bool             `json:"allow_insecure_mqtt"`
	JournalPath           string           `json:"journal_path"`
	JournalMaxBytes       int64            `json:"journal_max_bytes"`
	RequestTimeoutSeconds int              `json:"request_timeout_seconds"`
	MaxPending            int              `json:"max_pending"`
	QoS                   byte             `json:"qos"`
}

type Agent struct {
	Topic string `json:"topic"`
	Inbox string `json:"inbox"`
}

func Topic(s string) bool { return s != "" && len(s) <= 65535 && !strings.ContainsAny(s, "+#\x00") }

func Load(path string) (Config, error) {
	c := Config{Listen: "127.0.0.1:8080", JournalPath: "data/notifications.jsonl", JournalMaxBytes: 128 << 20, RequestTimeoutSeconds: 30, MaxPending: 256, QoS: 1}
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return c, fmt.Errorf("config must contain one JSON object")
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.EndpointID == "" || c.ClientID == "" {
		return fmt.Errorf("endpoint_id and client_id are required")
	}
	u, err := url.Parse(c.Broker)
	if err != nil || u.Hostname() == "" || u.User != nil || (u.Scheme != "tls" && u.Scheme != "mqtt") {
		return fmt.Errorf("broker must be tls://host:port or mqtt://host:port without credentials")
	}
	if u.Scheme == "mqtt" && !c.AllowInsecureMQTT {
		return fmt.Errorf("plaintext MQTT requires allow_insecure_mqtt (lab only)")
	}
	if (c.CertFile == "") != (c.KeyFile == "") {
		return fmt.Errorf("cert_file and key_file must be provided together")
	}
	if c.QoS > 1 || c.MaxPending < 1 || c.RequestTimeoutSeconds < 1 || c.RequestTimeoutSeconds > 300 || c.JournalMaxBytes < 1 || c.JournalPath == "" {
		return fmt.Errorf("invalid qos, capacity, journal or timeout")
	}
	if len(c.Agents) == 0 {
		return fmt.Errorf("at least one agent is required")
	}
	seen := map[string]bool{}
	for id, agent := range c.Agents {
		if id == "" || id == c.EndpointID {
			return fmt.Errorf("invalid agent ID")
		}
		for _, topic := range []string{agent.Topic, agent.Inbox} {
			if !Topic(topic) || seen[topic] {
				return fmt.Errorf("agent topics and inboxes must be valid and unique")
			}
			seen[topic] = true
		}
	}
	if a := c.ActorCanary; a != nil && a.Enabled {
		if a.Tenant == "" || len(a.Devices) == 0 || a.LockPath == "" {
			return fmt.Errorf("actor canary requires tenant, explicit devices and lock_path")
		}
		host, port, err := net.SplitHostPort(a.CallbackListen)
		ip := net.ParseIP(host)
		if err != nil || port == "" || port == "0" || ip == nil || !ip.IsLoopback() || a.CallbackListen == c.Listen {
			return fmt.Errorf("actor callback must use a distinct explicit loopback listener")
		}
		sidecar, err := url.Parse(a.DaprURL)
		if err != nil || sidecar.Scheme != "http" || sidecar.User != nil || sidecar.RawQuery != "" || sidecar.Fragment != "" || sidecar.Path != "" || net.ParseIP(sidecar.Hostname()) == nil || !net.ParseIP(sidecar.Hostname()).IsLoopback() {
			return fmt.Errorf("single-host dapr_url must be an HTTP loopback origin")
		}
		selected := map[string]bool{}
		for _, id := range a.Devices {
			if _, ok := c.Agents[id]; !ok || selected[id] {
				return fmt.Errorf("canary devices must be unique provisioned endpoint IDs")
			}
			selected[id] = true
		}
	}
	return nil
}
