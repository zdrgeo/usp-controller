package canary

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// DispatchReady is a local availability guard, not a cross-host fencing lease.
// Dapr remains responsible for serializing turns against shared actor state.
func (r *Runtime) DispatchReady() bool {
	return !r.draining.Load() && (r.replica == nil || time.Now().UnixNano() < r.verifiedUntil.Load())
}
func (r *Runtime) Ready() bool { return r.DispatchReady() && r.mqtt.Ready() }
func (r *Runtime) BeginDrain() { r.draining.Store(true); r.verifiedUntil.Store(0) }

// RefreshPlacement checks the sidecar identity and actor-capable state component,
// then performs an actual state/reminder turn through placement. It never writes
// state directly. Metadata cannot prove that replicas share database endpoints.
func (r *Runtime) RefreshPlacement(ctx context.Context) (err error) {
	if r.replica == nil {
		return nil
	}
	if r.draining.Load() {
		return errors.New("replica draining")
	}
	defer func() {
		if err != nil {
			r.verifiedUntil.Store(0)
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.Client.Endpoint+"/v1.0/metadata", nil)
	if err != nil {
		return err
	}
	req.Header.Set("dapr-api-token", r.Client.Token)
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("sidecar metadata HTTP %d", resp.StatusCode)
	}
	var m struct {
		ID         string `json:"id"`
		Components []struct {
			Name, Type   string
			Capabilities []string
		} `json:"components"`
		Scheduler struct {
			Connected []string `json:"connected_addresses"`
		} `json:"scheduler"`
		App struct {
			Health *struct {
				Path string `json:"healthCheckPath"`
			} `json:"health"`
		} `json:"appConnectionProperties"`
	}
	if err = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&m); err != nil {
		return err
	}
	if m.ID != r.replica.AppID || len(m.Scheduler.Connected) == 0 || m.App.Health == nil || m.App.Health.Path != "/healthz" {
		return errors.New("replica requires matching Dapr app ID, Scheduler and /healthz app probes")
	}
	state, pubsub := false, false
	for _, c := range m.Components {
		if c.Name == r.eventPubsub && strings.HasPrefix(c.Type, "pubsub.") {
			pubsub = true
		}
		if c.Name != r.replica.StateStore || !strings.HasPrefix(c.Type, "state.") {
			continue
		}
		caps := map[string]bool{}
		for _, v := range c.Capabilities {
			caps[v] = true
		}
		state = caps["ETAG"] && caps["ACTOR"] && (caps["TRANSACTIONAL"] || caps["TRANSACTION"])
	}
	if !state || !pubsub {
		return errors.New("replica requires actor/ETag/transaction state and event pubsub components")
	}
	// One inventory actor checks routing, state access and reminder persistence.
	// Inventory repair in main continues to cover the full configured cohort.
	for _, id := range r.Devices {
		if _, err = r.Client.Invoke(ctx, id, "Activate", nil); err != nil {
			return err
		}
		break
	}
	if !r.draining.Load() {
		r.verifiedUntil.Store(time.Now().Add(15 * time.Second).UnixNano())
	}
	return nil
}

func (r *Runtime) callbackGate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Keep config/deactivation available during drain. Reject new turns, including
		// reminders, while allowing already-entered turns to finish under Dapr's lock.
		if req.URL.Path == "/healthz" && (!r.mqtt.Ready() || r.draining.Load()) || r.draining.Load() && req.Method == http.MethodPut && strings.Contains(req.URL.Path, "/method/") {
			http.Error(w, "actor host unavailable", 503)
			return
		}
		if req.Method == http.MethodPut && strings.Contains(req.URL.Path, "/method/") && !strings.HasSuffix(req.URL.Path, "/method/Activate") && !r.DispatchReady() {
			http.Error(w, "replica dependencies not verified", 503)
			return
		}
		next.ServeHTTP(w, req)
	})
}

// AdmissionGate closes new management requests on shutdown; it does not couple
// durable admission to MQTT availability. Unavailability still propagates through
// the Dapr invocation result. Readiness tells load balancers when to route here.
func (r *Runtime) AdmissionGate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if r.draining.Load() && (strings.HasPrefix(req.URL.Path, "/v2/") || req.URL.Path == "/v1/requests") {
			http.Error(w, "controller draining", 503)
			return
		}
		next.ServeHTTP(w, req)
	})
}
