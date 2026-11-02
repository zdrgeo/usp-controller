package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
	"example.com/usp-controller/internal/fleet"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/encoding/protojson"
)

const token = "isolated-integration-test-token-0001"
const appToken = "isolated-callback-test-token-00001"
const daprVersion = "1.19.1"

// No MQTT connection is created. Any accidental publication fails the test.
type offlineTransport struct{ attempts atomic.Int64 }

func (*offlineTransport) Ready() bool { return false }
func (s *offlineTransport) Publish(context.Context, device.DeviceIdentity, device.PublishAttempt) (device.PublishResult, error) {
	s.attempts.Add(1)
	return device.PublishResult{}, fmt.Errorf("test transport is offline")
}
func docker(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	b, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %v: %v\n%s", args, err, b)
	}
	return strings.TrimSpace(string(b))
}
func eventually(t *testing.T, description string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out: %s", description)
}

type liveHost struct {
	name                      string
	client                    *deviceactor.Client
	projection                *fleet.Projection
	reminders, events, writes atomic.Int64
}

// Opt-in only: creates a private Docker network and disposable containers. Never
// points at an existing Dapr deployment or real devices. Requires Docker Desktop
// or Linux Docker with host-gateway support and access to the host callback ports.
func TestLiveDapr(t *testing.T) {
	if os.Getenv("USP_TEST_DAPR") != "1" {
		t.Skip("set USP_TEST_DAPR=1 to run isolated live Dapr tests")
	}
	docker(t, "info", "--format", "{{.ServerVersion}}")
	network := fmt.Sprintf("usp-dapr-test-%d", time.Now().UnixNano())
	docker(t, "network", "create", network)
	t.Cleanup(func() { docker(t, "network", "rm", network) })
	start := func(name, image string, args ...string) string {
		t.Helper()
		full := network + "-" + name
		cmd := []string{"run", "-d", "--name", full, "--network", network, "--network-alias", name}
		cmd = append(cmd, image)
		cmd = append(cmd, args...)
		docker(t, cmd...)
		t.Cleanup(func() {
			if t.Failed() {
				b, _ := exec.Command("docker", "logs", "--tail", "100", full).CombinedOutput()
				t.Logf("%s logs:\n%s", name, b)
			}
			docker(t, "rm", "-f", full)
		})
		return full
	}
	redis := start("redis", "redis:7.4.2-alpine", "redis-server", "--appendonly", "yes", "--appendfsync", "always")
	start("placement", "daprio/placement:"+daprVersion, "./placement", "--port", "50005")
	start("scheduler", "daprio/scheduler:"+daprVersion, "./scheduler", "--port", "50006", "--etcd-data-dir", "/tmp/scheduler")
	resources := t.TempDir()
	for name, kind := range map[string]string{"state": "state.redis", "events": "pubsub.redis"} {
		content := fmt.Sprintf("apiVersion: dapr.io/v1alpha1\nkind: Component\nmetadata:\n  name: %s\nspec:\n  type: %s\n  version: v1\n  metadata:\n  - name: redisHost\n    value: redis:6379\n  - name: redisPassword\n    value: \"\"\n", name, kind)
		if name == "state" {
			content += "  - name: actorStateStore\n    value: \"true\"\n"
		}
		if err := os.WriteFile(filepath.Join(resources, name+".yaml"), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	id := device.DeviceIdentity{Tenant: "test", ControllerID: "live", AgentID: "device-1"}
	other := id
	other.AgentID = "device-2"
	transport := &offlineTransport{}
	newHost := func(index int) *liveHost {
		t.Helper()
		h := &liveHost{}
		listener, err := net.Listen("tcp", "0.0.0.0:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { listener.Close() })
		port := listener.Addr().(*net.TCPAddr).Port
		// Daprd waits for the callback listener before initializing actors.
		name := fmt.Sprintf("host%d", index)
		full := network + "-" + name
		args := []string{"run", "-d", "--name", full, "--network", network, "--add-host", "host.docker.internal:host-gateway", "-p", "127.0.0.1::3500", "-e", "DAPR_API_TOKEN=" + token, "-e", "APP_API_TOKEN=" + appToken, "-v", resources + ":/components:ro", "daprio/daprd:" + daprVersion, "./daprd", "--app-id", "usp-live", "--app-port", fmt.Sprint(port), "--app-channel-address", "host.docker.internal", "--placement-host-address", "placement:50005", "--scheduler-host-address", "scheduler:50006", "--resources-path", "/components", "--log-level", "warn"}
		docker(t, args...)
		h.name = full
		t.Cleanup(func() {
			if t.Failed() {
				b, _ := exec.Command("docker", "logs", "--tail", "100", full).CombinedOutput()
				t.Logf("%s: %s", name, b)
			}
			docker(t, "rm", "-f", full)
		})
		endpoint := "http://" + docker(t, "port", full, "3500/tcp")
		h.client = &deviceactor.Client{Endpoint: endpoint, Token: token}
		opts := deviceactor.DefaultOptions()
		opts.Policy = &device.TwinPolicy{SiteID: "test-site", CommandTimeoutSeconds: 30, Paths: map[string]device.ManagedPath{"Device.Test.Value": {Type: "string", Writable: true}}}
		actors := []*deviceactor.Actor{}
		for _, identity := range []device.DeviceIdentity{id, other} {
			backend, err := deviceactor.NewDaprBackend(endpoint, nil, token, identity, time.Second, opts.MaxBytes)
			if err != nil {
				t.Fatal(err)
			}
			actor, err := deviceactor.NewWithEvents(identity, backend, opts, transport, &deviceactor.DaprEventPublisher{Endpoint: endpoint, Token: token, Pubsub: "events", Topic: "device-events"})
			if err != nil {
				t.Fatal(err)
			}
			actors = append(actors, actor)
		}
		host, err := deviceactor.NewHost(actors, appToken, 2<<20)
		if err != nil {
			t.Fatal(err)
		}
		p, err := fleet.Open(fleet.Options{Directory: t.TempDir(), SiteID: "test-site", RefreshInterval: 5 * time.Second, StaleAfter: 30 * time.Second, Workers: 2, Now: time.Now}, map[string]device.DeviceIdentity{id.AgentID: id, other.AgentID: other}, h.client)
		if err != nil {
			t.Fatal(err)
		}
		h.projection = p
		callback := p.Callback(host, appToken, "events", "device-events")
		server := &http.Server{ReadHeaderTimeout: 5 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			reminder := strings.Contains(r.URL.Path, "/"+deviceactor.ActorID(id)+"/method/remind/")
			if r.URL.Path == fleet.EventRoute || reminder {
				recorded := httptest.NewRecorder()
				callback.ServeHTTP(recorded, r)
				if reminder && recorded.Code == http.StatusOK {
					h.reminders.Add(1)
				}
				var result struct {
					Status string `json:"status"`
				}
				if json.Unmarshal(recorded.Body.Bytes(), &result) == nil && result.Status == "SUCCESS" {
					h.events.Add(1)
				}
				for key, values := range recorded.Header() {
					w.Header()[key] = values
				}
				w.WriteHeader(recorded.Code)
				_, _ = w.Write(recorded.Body.Bytes())
				return
			}
			if strings.HasSuffix(r.URL.Path, "/SetDesired") {
				h.writes.Add(1)
			}
			callback.ServeHTTP(w, r)
		})}
		go server.Serve(listener)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- p.Run(ctx) }()
		t.Cleanup(func() { cancel(); <-done; server.Close(); p.Close() })
		eventually(t, "sidecar health", func() bool {
			req, _ := http.NewRequest("GET", endpoint+"/v1.0/healthz", nil)
			req.Header.Set("dapr-api-token", token)
			c := http.Client{Timeout: time.Second}
			r, e := c.Do(req)
			if e != nil {
				return false
			}
			r.Body.Close()
			return r.StatusCode == 204
		})
		return h
	}
	hosts := []*liveHost{newHost(0), newHost(1)}
	unauthorized := *hosts[0].client
	unauthorized.Token = "wrong"
	if _, err := unauthorized.Invoke(context.Background(), id, "GetTwin", nil); err == nil {
		t.Fatal("sidecar accepted wrong API token")
	}
	invoke := func(h *liveHost, identity device.DeviceIdentity, method string, input any) json.RawMessage {
		t.Helper()
		v, err := h.client.Invoke(context.Background(), identity, method, input)
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		return v
	}
	eventually(t, "actor placement ready", func() bool {
		_, err := hosts[0].client.Invoke(context.Background(), id, "GetTwin", nil)
		return err == nil
	})
	request := device.SetDesiredRequest{ID: "desired-1", Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "target", Type: "string"}}}
	t.Run("concurrent receipts across sidecars", func(t *testing.T) {
		var wg sync.WaitGroup
		for i := 0; i < 12; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				b, err := hosts[i%2].client.Invoke(context.Background(), id, "SetDesired", request)
				var receipt device.DesiredReceipt
				if err != nil || json.Unmarshal(b, &receipt) != nil || receipt.Revision != 1 {
					t.Errorf("receipt %s: %v", b, err)
				}
			}(i)
		}
		wg.Wait()
	})
	if t.Failed() {
		return
	}
	var twin device.DeviceTwin
	if err := json.Unmarshal(invoke(hosts[1], other, "GetTwin", nil), &twin); err != nil || twin.DesiredRevision != 0 {
		t.Fatal("device state isolation", twin, err)
	}
	eventually(t, "real scheduler callbacks", func() bool { return hosts[0].reminders.Load()+hosts[1].reminders.Load() > 0 })
	// Actual actor outbox -> Redis pubsub -> Dapr raw CloudEvent -> fleet callback.
	msg := &usp.Msg{Header: &usp.Header{MsgId: "notify-1", MsgType: usp.Header_NOTIFY}, Body: &usp.Body{MsgBody: &usp.Body_Request{Request: &usp.Request{ReqType: &usp.Request_Notify{Notify: &usp.Notify{SubscriptionId: "sub", Notification: &usp.Notify_ValueChange_{ValueChange: &usp.Notify_ValueChange{ParamPath: "Device.Test.Value", ParamValue: "observed"}}}}}}}}
	raw, err := protojson.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}
	input := deviceactor.ResponseInput{Identity: id, Message: raw}
	invoke(hosts[0], id, "HandleNotify", input)
	eventually(t, "pubsub callback", func() bool { return hosts[0].events.Load()+hosts[1].events.Load() > 0 })
	eventually(t, "both fleet replicas converge", func() bool {
		for _, h := range hosts {
			req := httptest.NewRequest("GET", "/v2/fleet/devices/device-1", nil)
			req.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			h.projection.Handler(http.NotFoundHandler(), token).ServeHTTP(w, req)
			var v fleet.View
			if json.Unmarshal(w.Body.Bytes(), &v) != nil || v.Snapshot == nil || v.Snapshot.ReportedRevision != 1 || v.Snapshot.PendingEvents != 0 {
				return false
			}
		}
		return true
	})
	// No successful admission while the actual state service is unavailable.
	// Recovery uses the same request ID because a timeout can be ambiguous.
	docker(t, "stop", "-t", "1", redis)
	recovery := device.SetDesiredRequest{ID: "after-outage", ExpectedRevision: 1, Values: request.Values}
	if _, err := hosts[0].client.Invoke(context.Background(), id, "SetDesired", recovery); err == nil {
		t.Fatal("admitted desired change while Redis was stopped")
	}
	docker(t, "start", redis)
	eventually(t, "state service recovery", func() bool {
		b, err := hosts[1].client.Invoke(context.Background(), id, "SetDesired", recovery)
		var receipt device.DesiredReceipt
		return err == nil && json.Unmarshal(b, &receipt) == nil && receipt.Revision == 2
	})
	// Locate current owner by an invocation, then kill its sidecar ungracefully.
	before := hosts[0].writes.Load()
	invoke(hosts[1], id, "SetDesired", request)
	owner := 1
	if hosts[0].writes.Load() > before {
		owner = 0
	}
	survivor := hosts[1-owner]
	docker(t, "kill", hosts[owner].name)
	eventually(t, "receipt recovery after owner loss", func() bool {
		b, err := survivor.client.Invoke(context.Background(), id, "SetDesired", request)
		var receipt device.DesiredReceipt
		return err == nil && json.Unmarshal(b, &receipt) == nil && receipt.Revision == 1
	})
	var receipt struct{ Duplicate bool }
	if err := json.Unmarshal(invoke(survivor, id, "HandleNotify", input), &receipt); err != nil || !receipt.Duplicate {
		t.Fatal("notification receipt lost after relocation", receipt, err)
	}
	beforeReminder := survivor.reminders.Load()
	eventually(t, "reminders follow surviving owner", func() bool { return survivor.reminders.Load() > beforeReminder })
	if transport.attempts.Load() != 0 {
		t.Fatal("unexpected device publication")
	}
	t.Logf("Dapr %s: two hosts, receipts, isolation, scheduler, outbox/pubsub/fleet, state outage, owner loss passed", daprVersion)
}
