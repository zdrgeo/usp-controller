package config

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strings"

	"example.com/usp-controller/internal/device"
)

// Replica selects the experimental shared-subscription topology. All replicas
// must use identical inventory, actor app identity, state component and group.
type Replica struct {
	InstanceID  string `json:"instance_id"`
	SharedGroup string `json:"shared_group"`
	AppID       string `json:"app_id"`
	StateStore  string `json:"state_store"`
}

type ActorCanary struct {
	Twin           *device.TwinPolicy `json:"twin,omitempty"`
	Replica        *Replica           `json:"replica,omitempty"`
	EventPubsub    string             `json:"event_pubsub"`
	EventTopic     string             `json:"event_topic"`
	Enabled        bool               `json:"enabled"`
	Tenant         string             `json:"tenant"`
	Devices        []string           `json:"devices"`
	DaprURL        string             `json:"dapr_url"`
	CallbackListen string             `json:"callback_listen"`
	LockPath       string             `json:"lock_path"`
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
		if a.EventPubsub == "" || a.EventTopic == "" || strings.ContainsAny(a.EventPubsub+a.EventTopic, "/?#%\\") || a.EventPubsub == "." || a.EventPubsub == ".." || a.EventTopic == "." || a.EventTopic == ".." {
			return fmt.Errorf("actor canary requires event_pubsub and event_topic as simple path segments")
		}
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
	if a := c.ActorCanary; a != nil && a.Twin != nil {
		if !a.Enabled {
			return fmt.Errorf("twin policy requires actor canary")
		}
		if err := a.Twin.Validate(); err != nil {
			return err
		}
	}
	if a := c.ActorCanary; a != nil && a.Replica != nil {
		r := a.Replica
		if !a.Enabled || len(a.Devices) != len(c.Agents) {
			return fmt.Errorf("replica mode requires actor ownership of every provisioned device")
		}
		for _, v := range []string{r.InstanceID, r.SharedGroup, r.AppID, r.StateStore} {
			if !replicaName(v) {
				return fmt.Errorf("replica identity, shared group, app ID and state store must be nonempty simple names")
			}
		}
		for _, agent := range c.Agents {
			if strings.HasPrefix(agent.Inbox, "$share/") || len("$share/"+r.SharedGroup+"/"+agent.Inbox) > 65535 {
				return fmt.Errorf("invalid shared inbox filter")
			}
		}
		if len(c.MQTTClientID()) > 65535 {
			return fmt.Errorf("replica MQTT client ID too long")
		}
	}
	return nil
}

func replicaName(s string) bool {
	if s == "" || len(s) > 128 || s == "." || s == ".." {
		return false
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.') {
			return false
		}
	}
	return true
}
func (c Config) ReplicaMode() bool {
	return c.ActorCanary != nil && c.ActorCanary.Enabled && c.ActorCanary.Replica != nil
}
func (c Config) MQTTClientID() string {
	if c.ReplicaMode() {
		return c.ClientID + "/" + c.ActorCanary.Replica.InstanceID
	}
	return c.ClientID
}
func (c Config) InboxSubscription(inbox string) string {
	if c.ReplicaMode() {
		return "$share/" + c.ActorCanary.Replica.SharedGroup + "/" + inbox
	}
	return inbox
}
