// Package actormetrics records bounded process-local observations. It never
// invokes actors, reads snapshots, retries operations, or exports device labels.
package actormetrics

import (
	"crypto/subtle"
	"example.com/usp-controller/internal/device"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

var bounds = [...]float64{.001, .01, .1, 1, 5, 15}

type key struct{ boundary, operation, result string }
type sample struct {
	count   uint64
	seconds float64
	buckets [len(bounds)]uint64
}
type Registry struct {
	attemptBudget     *device.AttemptBudget
	attemptRateBudget *device.AttemptRateBudget
	mu                sync.Mutex
	samples           map[key]sample
}

func New() *Registry { return &Registry{samples: map[key]sample{}} }

func allowed(boundary, operation, result string) bool {
	switch boundary {
	case "actor":
		switch operation {
		case "Activate", "Tick", "GetTwin", "RefreshTwin", "SetDesired", "Report", "GetStatus", "GetExecution", "SetExecution", "EnqueueCommand", "GetCommand", "ListCommands", "CancelCommand", "AcknowledgeUnknown", "HandleNotify", "HandleResponse":
		default:
			return false
		}
		switch result {
		case "ok", "invalid", "conflict", "capacity", "not_found", "disabled", "cancelled", "deadline", "error":
			return true
		}
	case "snapshot":
		if operation != "load" && operation != "save" && operation != "reminder" {
			return false
		}
		return result == "ok" || result == "error"
	case "transport":
		return operation == "publish" && (result == "accepted" || result == "unconfirmed" || result == "uncertain")
	case "events":
		return operation == "handoff" && (result == "ok" || result == "uncertain")
	}
	return false
}

// Observe accepts only fixed operations and outcomes. Unknown labels cannot grow
// the registry or put caller-controlled strings into an exposition response.
func (m *Registry) Observe(boundary, operation, result string, elapsed time.Duration) {
	if m == nil || !allowed(boundary, operation, result) {
		return
	}
	seconds := elapsed.Seconds()
	if seconds < 0 {
		seconds = 0
	}
	// Clone fixed-width labels so an operation substring cannot retain an entire
	// callback URL (and its device identity) as backing storage.
	k := key{strings.Clone(boundary), strings.Clone(operation), strings.Clone(result)}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.samples == nil {
		m.samples = map[key]sample{}
	}
	value := m.samples[k]
	value.count++
	value.seconds += seconds
	for i, bound := range bounds {
		if seconds <= bound {
			value.buckets[i]++
		}
	}
	m.samples[k] = value
}

// Handler mounts only the operator metrics route. Scrapes copy bounded counters
// under the lock; formatting and network writes happen after releasing it.
func (m *Registry) Handler(next http.Handler, token string, site ...string) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/", next)
	mux.HandleFunc("GET /v2/metrics", func(w http.ResponseWriter, r *http.Request) {
		if len(token) < 32 || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
			http.Error(w, "unauthorized", 401)
			return
		}
		if len(site) > 0 && site[0] != "" {
			w.Header().Set("X-USP-Site", site[0])
			if requested := r.Header.Get("X-USP-Site"); requested != "" && requested != site[0] {
				http.Error(w, "wrong site", 409)
				return
			}
		}
		m.mu.Lock()
		budget := m.attemptBudget
		rateBudget := m.attemptRateBudget
		values := make(map[key]sample, len(m.samples))
		for k, v := range m.samples {
			values[k] = v
		}
		m.mu.Unlock()
		keys := make([]key, 0, len(values))
		for k := range values {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			a, b := keys[i], keys[j]
			if a.boundary != b.boundary {
				return a.boundary < b.boundary
			}
			if a.operation != b.operation {
				return a.operation < b.operation
			}
			return a.result < b.result
		})
		var body strings.Builder
		body.WriteString("# HELP usp_device_operations_total Local calls observed at return or unwind; errors can have committed or published.\n# TYPE usp_device_operations_total counter\n")
		for _, k := range keys {
			v := values[k]
			fmt.Fprintf(&body, "usp_device_operations_total{boundary=%q,operation=%q,result=%q} %d\n", k.boundary, k.operation, k.result, v.count)
		}
		body.WriteString("# HELP usp_device_operation_duration_seconds Local operation latency including failed calls.\n# TYPE usp_device_operation_duration_seconds histogram\n")
		for _, k := range keys {
			v := values[k]
			for i, bound := range bounds {
				fmt.Fprintf(&body, "usp_device_operation_duration_seconds_bucket{boundary=%q,operation=%q,result=%q,le=%q} %d\n", k.boundary, k.operation, k.result, fmt.Sprint(bound), v.buckets[i])
			}
			fmt.Fprintf(&body, "usp_device_operation_duration_seconds_bucket{boundary=%q,operation=%q,result=%q,le=\"+Inf\"} %d\n", k.boundary, k.operation, k.result, v.count)
			fmt.Fprintf(&body, "usp_device_operation_duration_seconds_sum{boundary=%q,operation=%q,result=%q} %g\n", k.boundary, k.operation, k.result, v.seconds)
			fmt.Fprintf(&body, "usp_device_operation_duration_seconds_count{boundary=%q,operation=%q,result=%q} %d\n", k.boundary, k.operation, k.result, v.count)
		}
		if budget != nil {
			body.WriteString("# HELP usp_device_attempt_slots_limit Local maximum simultaneous attempt sections.\n# TYPE usp_device_attempt_slots_limit gauge\n")
			fmt.Fprintf(&body, "usp_device_attempt_slots_limit %d\n", budget.Limit())
			body.WriteString("# HELP usp_device_attempt_slots_in_use Local attempt sections holding a permit.\n# TYPE usp_device_attempt_slots_in_use gauge\n")
			fmt.Fprintf(&body, "usp_device_attempt_slots_in_use %d\n", budget.InUse())
			body.WriteString("# HELP usp_device_attempt_budget_denied_total Eligible turns left queued because the local budget was full.\n# TYPE usp_device_attempt_budget_denied_total counter\n")
			fmt.Fprintf(&body, "usp_device_attempt_budget_denied_total %d\n", budget.Denied())
		}
		if rateBudget != nil {
			rate, burst, granted, denied := rateBudget.Stats()
			body.WriteString("# HELP usp_device_attempt_rate_per_second Local token refill rate.\n# TYPE usp_device_attempt_rate_per_second gauge\n")
			fmt.Fprintf(&body, "usp_device_attempt_rate_per_second %d\n", rate)
			body.WriteString("# HELP usp_device_attempt_rate_burst Local maximum token bucket burst.\n# TYPE usp_device_attempt_rate_burst gauge\n")
			fmt.Fprintf(&body, "usp_device_attempt_rate_burst %d\n", burst)
			body.WriteString("# HELP usp_device_attempt_rate_granted_total Local rate reservations including failed begins.\n# TYPE usp_device_attempt_rate_granted_total counter\n")
			fmt.Fprintf(&body, "usp_device_attempt_rate_granted_total %d\n", granted)
			body.WriteString("# HELP usp_device_attempt_rate_denied_total Eligible turns left queued by the local rate limit.\n# TYPE usp_device_attempt_rate_denied_total counter\n")
			fmt.Fprintf(&body, "usp_device_attempt_rate_denied_total %d\n", denied)
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write([]byte(body.String()))
	})
	return mux
}

// SetAttemptBudget attaches only concrete local counters; scraping never invokes
// an external observer or actor. Replacing a budget resets its local readings.
func (m *Registry) SetAttemptBudget(budget *device.AttemptBudget) {
	m.mu.Lock()
	m.attemptBudget = budget
	m.mu.Unlock()
}

func (m *Registry) SetAttemptRateBudget(budget *device.AttemptRateBudget) {
	m.mu.Lock()
	m.attemptRateBudget = budget
	m.mu.Unlock()
}
