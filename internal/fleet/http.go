package fleet

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
	"example.com/usp-controller/internal/protocol"
)

const EventRoute = "/fleet/events"

type View struct {
	Identity       device.DeviceIdentity `json:"identity"`
	Freshness      string                `json:"freshness"`
	CapturedAt     time.Time             `json:"captured_at"`
	RefreshedAt    time.Time             `json:"refreshed_at"`
	RefreshPending bool                  `json:"refresh_pending"`
	LastError      string                `json:"last_error,omitempty"`
	Snapshot       *deviceactor.Status   `json:"snapshot"`
}

func (p *Projection) view(r row, now time.Time) View {
	freshness := "fresh"
	if r.Snapshot == nil {
		freshness = "unavailable"
	} else if r.Pending || p.inFlight[r.Identity.AgentID] || r.LastError != "" || p.fault != nil || p.closed || r.CapturedAt.After(now) || !now.Before(r.CapturedAt.Add(p.opts.StaleAfter)) {
		freshness = "stale"
	}
	return View{Identity: r.Identity, Freshness: freshness, CapturedAt: r.CapturedAt, RefreshedAt: r.RefreshedAt, RefreshPending: r.Pending || p.inFlight[r.Identity.AgentID], LastError: r.LastError, Snapshot: r.Snapshot}
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func authorized(got, want string) bool {
	return len(want) >= 32 && subtle.ConstantTimeCompare([]byte(got), []byte(want)) == 1
}

// Handler only reads the materialized local view; API traffic never fans out to
// actors. Each response is a consistent local read, not a cross-device transaction.
func (p *Projection) Handler(next http.Handler, token string) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", next)
	auth := func(fn http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if !authorized(r.Header.Get("Authorization"), "Bearer "+token) || len(token) < 32 {
				http.Error(w, "unauthorized", 401)
				return
			}
			w.Header().Set("X-USP-Site", p.opts.SiteID)
			if site := r.Header.Get("X-USP-Site"); site != "" && site != p.opts.SiteID {
				http.Error(w, "wrong site", 409)
				return
			}
			fn(w, r)
		}
	}
	mux.HandleFunc("GET /v2/fleet/devices", auth(func(w http.ResponseWriter, r *http.Request) {
		limit := 100
		if raw := r.URL.Query().Get("limit"); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < 1 || n > 200 {
				http.Error(w, "limit must be 1–200", 400)
				return
			}
			limit = n
		}
		after := r.URL.Query().Get("after")
		if len(after) > 1024 {
			http.Error(w, "invalid cursor", 400)
			return
		}
		p.mu.Lock()
		start := sort.Search(len(p.ids), func(i int) bool { return p.ids[i] > after })
		end := min(start+limit, len(p.ids))
		now := p.opts.Now()
		rows := make([]View, 0, end-start)
		for _, id := range p.ids[start:end] {
			rows = append(rows, p.view(p.rows[id], now))
		}
		cursor := ""
		if end < len(p.ids) && end > start {
			cursor = p.ids[end-1]
		}
		healthy := p.fault == nil && !p.closed
		total := len(p.ids)
		p.mu.Unlock()
		writeJSON(w, 200, struct {
			SiteID             string    `json:"site_id"`
			Consistency        string    `json:"consistency"`
			GeneratedAt        time.Time `json:"generated_at"`
			PersistenceHealthy bool      `json:"persistence_healthy"`
			Total              int       `json:"total"`
			NextAfter          string    `json:"next_after,omitempty"`
			Devices            []View    `json:"devices"`
		}{p.opts.SiteID, "eventually_consistent", now, healthy, total, cursor, rows})
	}))
	mux.HandleFunc("GET /v2/fleet/devices/{device}", auth(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		row, ok := p.rows[r.PathValue("device")]
		if !ok {
			p.mu.Unlock()
			http.NotFound(w, r)
			return
		}
		view := p.view(row, p.opts.Now())
		p.mu.Unlock()
		writeJSON(w, 200, view)
	}))
	mux.HandleFunc("GET /v2/fleet/summary", auth(func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		now := p.opts.Now()
		freshness := map[string]int{"fresh": 0, "stale": 0, "unavailable": 0}
		convergence := map[string]int{}
		states := map[device.CommandState]int{}
		for _, row := range p.rows {
			v := p.view(row, now)
			freshness[v.Freshness]++
			if v.Freshness == "fresh" {
				convergence[v.Snapshot.Convergence]++
				for k, n := range v.Snapshot.CommandStates {
					states[k] += n
				}
			}
		}
		healthy := p.fault == nil && !p.closed
		total := len(p.rows)
		p.mu.Unlock()
		writeJSON(w, 200, struct {
			SiteID             string                      `json:"site_id"`
			Consistency        string                      `json:"consistency"`
			GeneratedAt        time.Time                   `json:"generated_at"`
			PersistenceHealthy bool                        `json:"persistence_healthy"`
			Total              int                         `json:"total"`
			Freshness          map[string]int              `json:"freshness"`
			FreshConvergence   map[string]int              `json:"fresh_convergence"`
			FreshCommandStates map[device.CommandState]int `json:"fresh_command_states"`
		}{p.opts.SiteID, "eventually_consistent", now, healthy, total, freshness, convergence, states})
	}))
	return mux
}

// Callback handles Dapr's raw-payload CloudEvent wrapper (data_base64). Invalid,
// unrelated or conflicting hints are explicitly dropped: periodic refresh repairs
// the view. Valid hints are acknowledged only after durable local invalidation.
func (p *Projection) Callback(next http.Handler, token, pubsub, topic string) http.Handler {
	slots := make(chan struct{}, p.opts.Workers)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dapr/subscribe" && r.URL.Path != EventRoute {
			next.ServeHTTP(w, r)
			return
		}
		if !authorized(r.Header.Get("dapr-api-token"), token) {
			http.Error(w, "unauthorized", 401)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/dapr/subscribe" {
			writeJSON(w, 200, []any{map[string]any{"pubsubname": pubsub, "topic": topic, "route": strings.TrimPrefix(EventRoute, "/"), "metadata": map[string]string{"rawPayload": "true"}}})
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != EventRoute {
			http.NotFound(w, r)
			return
		}
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			writeJSON(w, 200, map[string]string{"status": "RETRY"})
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 3*protocol.MaxRecord)
		var event struct {
			Pubsub string `json:"pubsubname"`
			Topic  string `json:"topic"`
			Data   []byte `json:"data_base64"`
		}
		d := json.NewDecoder(r.Body)
		err := d.Decode(&event)
		if err == nil && d.Decode(new(any)) != io.EOF {
			err = ErrInvalid
		}
		if err == nil && (event.Pubsub != pubsub || event.Topic != topic || len(event.Data) == 0) {
			err = ErrInvalid
		}
		if err == nil {
			err = p.Ingest(event.Data)
		}
		status := "SUCCESS"
		if err != nil {
			if errors.Is(err, ErrUnavailable) {
				status = "RETRY"
			} else {
				status = "DROP"
			}
		}
		writeJSON(w, 200, map[string]string{"status": status})
	})
}
