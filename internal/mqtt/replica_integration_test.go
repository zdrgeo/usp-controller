package mqtt

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"example.com/usp-controller/internal/config"
	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/paho"
)

// Exercises real MQTT 5 shared delivery and survivor routing. This does not model
// Dapr placement or promise delivery of messages in a dead subscriber's session.
func TestReplicaSharedSubscriptionSurvivor(t *testing.T) {
	broker := os.Getenv("USP_TEST_MQTT")
	if broker == "" {
		t.Skip("set USP_TEST_MQTT for shared subscription tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	prefix := fmt.Sprintf("replica-%d", time.Now().UnixNano())
	cfg := config.Config{EndpointID: "controller", ClientID: prefix, Broker: broker, Agents: map[string]config.Agent{"agent": {Topic: prefix + "/device", Inbox: prefix + "/inbox"}}, QoS: 1, RequestTimeoutSeconds: 5, ActorCanary: &config.ActorCanary{Enabled: true, Replica: &config.Replica{InstanceID: "a", SharedGroup: prefix}}}
	type received struct{ host, body string }
	seen := make(chan received, 64)
	start := func(c config.Config, host string) *Transport {
		tr := New(c)
		if err := tr.Start(ctx, func(_ context.Context, b []byte, reply, sender string) error {
			if sender != "agent" || reply != prefix+"/device" {
				t.Error("routing changed", sender, reply)
			}
			seen <- received{host, string(b)}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			closeCtx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			tr.Close(closeCtx)
		})
		for !tr.Ready() {
			select {
			case err := <-tr.Errors():
				t.Fatal(err)
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
		return tr
	}
	first := start(cfg, "a")
	other := *cfg.ActorCanary
	replica := *other.Replica
	replica.InstanceID = "b"
	other.Replica = &replica
	cfg.ActorCanary = &other
	start(cfg, "b")
	u, _ := url.Parse(broker)
	peer, err := autopaho.NewConnection(ctx, autopaho.ClientConfig{ServerUrls: []*url.URL{u}, KeepAlive: 30, CleanStartOnInitialConnection: true, ClientConfig: paho.ClientConfig{ClientID: prefix + "-peer"}})
	if err != nil {
		t.Fatal(err)
	}
	defer peer.Disconnect(context.Background())
	if err = peer.AwaitConnection(ctx); err != nil {
		t.Fatal(err)
	}
	publish := func(body string) {
		t.Helper()
		if _, err := peer.Publish(ctx, &paho.Publish{Topic: prefix + "/inbox", QoS: 1, Payload: []byte(body), Properties: &paho.PublishProperties{ContentType: "usp.msg", ResponseTopic: prefix + "/device"}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 20; i++ {
		publish(fmt.Sprint(i))
	}
	counts := map[string]int{}
	for len(counts) < 20 {
		select {
		case m := <-seen:
			counts[m.body]++
			if counts[m.body] != 1 {
				t.Fatal("duplicate shared delivery", m)
			}
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	if err = first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	publish("after-close")
	select {
	case m := <-seen:
		if m.host != "b" || m.body != "after-close" {
			t.Fatal("survivor routing", m)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case m := <-seen:
		t.Fatal("unexpected duplicate", m)
	case <-time.After(100 * time.Millisecond):
	}
}
