package deviceactor

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"example.com/usp-controller/internal/device"
)

// Host is an opt-in Dapr callback handler for a fixed provisioned device set.
// The production main does not mount it. It deliberately exposes only phase-2
// twin methods; command/Notify HTTP ingress remains a later migration phase.
// Put it on a private app channel; token must match Dapr's app API token.
type Host struct {
	actors  map[string]*Actor
	token   string
	maxBody int64
}

func NewHost(actors []*Actor, token string, maxBody int64) (*Host, error) {
	if len(token) < 32 || maxBody <= 0 {
		return nil, ErrInvalid
	}
	h := &Host{actors: map[string]*Actor{}, token: token, maxBody: maxBody}
	for _, a := range actors {
		if a == nil {
			return nil, ErrInvalid
		}
		id := ActorID(a.id)
		if _, ok := h.actors[id]; ok {
			return nil, ErrConflict
		}
		h.actors[id] = a
	}
	return h, nil
}
func (h *Host) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if subtle.ConstantTimeCompare([]byte(r.Header.Get("dapr-api-token")), []byte(h.token)) != 1 {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/dapr/config" {
		writeJSON(w, map[string]any{"entities": []string{ActorType}, "reentrancy": map[string]bool{"enabled": false}, "drainRebalancedActors": true})
		return
	}
	if r.Method == http.MethodGet && r.URL.Path == "/healthz" {
		w.WriteHeader(http.StatusOK)
		return
	}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
	if len(parts) < 3 || parts[0] != "actors" || parts[1] != ActorType {
		http.NotFound(w, r)
		return
	}
	a, ok := h.actors[parts[2]]
	if !ok {
		http.NotFound(w, r)
		return
	}
	// No mutable state cache exists to flush or discard at deactivation. Actor
	// objects contain only dependencies and local locks, bounded by inventory.
	if r.Method == http.MethodDelete && len(parts) == 3 {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodPut || len(parts) < 5 || parts[3] != "method" {
		http.NotFound(w, r)
		return
	}
	if len(parts) == 6 && parts[4] == "remind" && parts[5] == ReminderName {
		respond(w, nil, a.Tick(r.Context()))
		return
	}
	if len(parts) != 5 {
		http.NotFound(w, r)
		return
	}
	switch parts[4] {
	case "GetTwin":
		v, err := a.GetTwin(r.Context())
		respond(w, v, err)
	case "SetDesired":
		var v device.SetDesiredRequest
		if !h.decode(w, r, &v) {
			return
		}
		out, err := a.SetDesired(r.Context(), v)
		respond(w, out, err)
	case "Report":
		var v ReportRequest
		if !h.decode(w, r, &v) {
			return
		}
		revision, err := a.Report(r.Context(), v)
		respond(w, map[string]uint64{"Revision": revision}, err)
	case "Activate":
		respond(w, nil, a.Activate(r.Context()))
	default:
		http.NotFound(w, r)
	}
}
func (h *Host) decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, h.maxBody)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		http.Error(w, "invalid JSON", http.StatusBadRequest)
		return false
	}
	if err := d.Decode(new(any)); err != io.EOF {
		http.Error(w, "expected one JSON value", http.StatusBadRequest)
		return false
	}
	return true
}
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
func respond(w http.ResponseWriter, v any, err error) {
	if err != nil {
		status := http.StatusInternalServerError
		switch {
		case errors.Is(err, ErrInvalid):
			status = http.StatusBadRequest
		case errors.Is(err, ErrConflict):
			status = http.StatusConflict
		case errors.Is(err, ErrCapacity):
			status = http.StatusTooManyRequests
		case errors.Is(err, ErrDisabled):
			status = http.StatusServiceUnavailable
		case errors.Is(err, ErrNotFound):
			status = http.StatusNotFound
		}
		http.Error(w, http.StatusText(status), status)
		return
	}
	if v == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	writeJSON(w, v)
}
