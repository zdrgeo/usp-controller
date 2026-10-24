package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"sort"
	"sync"
	"time"

	"example.com/usp-controller/internal/config"
	"example.com/usp-controller/internal/controller"
	"example.com/usp-controller/internal/pb/record"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
	"google.golang.org/protobuf/proto"
)

type Receiver func(context.Context, []byte, string, string) error
type incoming struct {
	payload       []byte
	reply, sender string
}
type Transport struct {
	cfg        config.Config
	mu         sync.Mutex
	cm         *autopaho.ConnectionManager
	ready      bool
	generation uint64
	inboxes    map[string]string
	replies    map[string]string
	fatal      chan error
}

func New(cfg config.Config) *Transport {
	t := &Transport{cfg: cfg, inboxes: map[string]string{}, replies: map[string]string{}, fatal: make(chan error, 1)}
	for id, a := range cfg.Agents {
		t.inboxes[a.Inbox] = id
		t.replies[a.Topic] = a.Inbox
	}
	return t
}
func (t *Transport) Errors() <-chan error { return t.fatal }
func (t *Transport) Ready() bool          { t.mu.Lock(); defer t.mu.Unlock(); return t.ready }
func (t *Transport) fail(err error) {
	select {
	case t.fatal <- err:
	default:
	}
}

func (t *Transport) Start(ctx context.Context, receive Receiver) error {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if t.cfg.CAFile != "" {
		b, err := os.ReadFile(t.cfg.CAFile)
		if err != nil {
			return err
		}
		pool, err := x509.SystemCertPool()
		if err != nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(b) {
			return errors.New("CA file contains no certificates")
		}
		tlsConfig.RootCAs = pool
	}
	if t.cfg.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(t.cfg.CertFile, t.cfg.KeyFile)
		if err != nil {
			return err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	u, err := url.Parse(t.cfg.Broker)
	if err != nil {
		return err
	}
	queue := make(chan incoming, 64)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case m := <-queue:
				callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
				if err := receive(callCtx, m.payload, m.reply, m.sender); err != nil {
					slog.Warn("USP record rejected", "agent", m.sender, "error", err)
				}
				cancel()
			}
		}
	}()
	cm, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{
		ServerUrls: []*url.URL{u}, TlsCfg: tlsConfig, KeepAlive: 60,
		CleanStartOnInitialConnection: true, SessionExpiryInterval: 0,
		ReconnectBackoff: autopaho.DefaultExponentialBackoff(),
		ConnectUsername:  os.Getenv("USP_MQTT_USERNAME"), ConnectPassword: []byte(os.Getenv("USP_MQTT_PASSWORD")),
		ConnectPacketBuilder: func(p *paho.Connect, _ *url.URL) (*paho.Connect, error) {
			if p.Properties == nil {
				p.Properties = &paho.ConnectProperties{}
			}
			max := uint32(controller.MaxRecord + 65536)
			p.Properties.MaximumPacketSize = &max
			return p, nil
		},
		OnConnectionUp: func(cm *autopaho.ConnectionManager, ack *paho.Connack) {
			t.mu.Lock()
			t.cm = cm
			t.generation++
			gen := t.generation
			t.ready = false
			t.mu.Unlock()
			go func() {
				if err := t.subscribe(ctx, cm, ack); err != nil {
					t.fail(err)
					return
				}
				t.mu.Lock()
				current := t.generation == gen
				if current {
					t.ready = true
				}
				t.mu.Unlock()
				if current {
					if err := t.announce(ctx, cm); err != nil {
						t.fail(err)
					}
				}
			}()
		},
		OnConnectionDown: func() bool { t.mu.Lock(); t.generation++; t.ready = false; t.mu.Unlock(); return true },
		OnConnectError:   func(err error) { slog.Warn("MQTT connection failed", "error", err) },
		ClientConfig: paho.ClientConfig{ClientID: t.cfg.ClientID, OnPublishReceived: []func(paho.PublishReceived) (bool, error){
			func(pr paho.PublishReceived) (bool, error) {
				p := pr.Packet
				sender, ok := t.inboxes[p.Topic]
				if !ok || p.Retain || len(p.Payload) > controller.MaxRecord || p.Properties == nil || (p.Properties.ContentType != "usp.msg" && p.Properties.ContentType != "application/vnd.bbf.usp.msg") {
					return true, nil
				}
				m := incoming{payload: append([]byte(nil), p.Payload...), reply: p.Properties.ResponseTopic, sender: sender}
				select {
				case queue <- m:
				case <-ctx.Done():
				}
				return true, nil
			},
		}},
	})
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.cm = cm
	t.mu.Unlock()
	return nil
}

func (t *Transport) subscribe(ctx context.Context, cm *autopaho.ConnectionManager, ack *paho.Connack) error {
	// This deployment profile uses per-agent inbox ACLs. Broker-assigned common
	// reply topics would break the identity binding, so fail closed.
	if ack.Properties != nil {
		if ack.Properties.ResponseInfo != "" {
			return errors.New("broker-assigned response topics are incompatible with per-agent inboxes")
		}
		for _, p := range ack.Properties.User {
			if p.Key == "subscribe-topic" {
				return errors.New("broker-assigned subscriptions are not supported")
			}
		}
	}
	topics := make([]string, 0, len(t.inboxes))
	for topic := range t.inboxes {
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	for start := 0; start < len(topics); start += 32 {
		end := min(start+32, len(topics))
		subs := make([]paho.SubscribeOptions, 0, end-start)
		for _, topic := range topics[start:end] {
			subs = append(subs, paho.SubscribeOptions{Topic: topic, QoS: t.cfg.QoS})
		}
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		ack, err := cm.Subscribe(callCtx, &paho.Subscribe{Subscriptions: subs})
		cancel()
		if err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}
		if len(ack.Reasons) != len(subs) {
			return errors.New("incomplete SUBACK")
		}
		for _, reason := range ack.Reasons {
			if reason > 1 {
				return fmt.Errorf("subscription rejected: %x", reason)
			}
		}
	}
	slog.Info("MQTT subscriptions established", "agents", len(t.cfg.Agents))
	return nil
}

func (t *Transport) announce(ctx context.Context, cm *autopaho.ConnectionManager) error {
	// Subscriptions are ready before announcements, so immediate notifications
	// triggered by a Connect record can be acknowledged.
	for id, a := range t.cfg.Agents {
		r := &record.Record{Version: controller.ProtocolVersion, FromId: t.cfg.EndpointID, ToId: id, RecordType: &record.Record_MqttConnect{MqttConnect: &record.MQTTConnectRecord{Version: record.MQTTConnectRecord_V5, SubscribedTopic: a.Inbox}}}
		b, err := proto.Marshal(r)
		if err != nil {
			return err
		}
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = t.publish(callCtx, cm, a.Topic, b)
		cancel()
		if err != nil {
			return err
		}
	}
	return nil
}
func (t *Transport) Publish(ctx context.Context, topic string, payload []byte) error {
	t.mu.Lock()
	cm := t.cm
	ready := t.ready
	t.mu.Unlock()
	if cm == nil || !ready {
		return errors.New("MQTT not ready")
	}
	return t.publish(ctx, cm, topic, payload)
}
func (t *Transport) publish(ctx context.Context, cm *autopaho.ConnectionManager, topic string, payload []byte) error {
	reply, ok := t.replies[topic]
	if !ok {
		return errors.New("unprovisioned publish topic")
	}
	expiry := uint32(t.cfg.RequestTimeoutSeconds)
	if deadline, ok := ctx.Deadline(); ok {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return context.DeadlineExceeded
		}
		expiry = uint32(max(1, int64((remaining+time.Second-1)/time.Second)))
	}
	response, err := cm.Publish(ctx, &paho.Publish{Topic: topic, QoS: t.cfg.QoS, Payload: payload, Properties: &paho.PublishProperties{ContentType: "usp.msg", ResponseTopic: reply, MessageExpiry: &expiry}})
	if err != nil {
		return err
	}
	if response != nil && response.ReasonCode >= 0x80 {
		return fmt.Errorf("publish rejected: %x", response.ReasonCode)
	}
	return nil
}
func (t *Transport) Close(ctx context.Context) error {
	t.mu.Lock()
	cm := t.cm
	t.generation++
	t.ready = false
	t.mu.Unlock()
	if cm != nil {
		return cm.Disconnect(ctx)
	}
	return nil
}
