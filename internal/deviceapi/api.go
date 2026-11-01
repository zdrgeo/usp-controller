// Package deviceapi exposes opt-in asynchronous commands. It never publishes MQTT.
package deviceapi

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
	"example.com/usp-controller/internal/protocol"
)

type Invoker interface {
	Invoke(context.Context, device.DeviceIdentity, string, any) (json.RawMessage, error)
}

type Options struct {
	SiteID      string
	TwinEnabled bool
}
type DesiredInput struct {
	RequestID        device.RequestID                 `json:"request_id"`
	ExpectedRevision uint64                           `json:"expected_revision"`
	Values           map[string]device.ParameterValue `json:"values"`
	Remove           []string                         `json:"remove"`
}

// Handler routes only explicit inventory entries; readiness of MQTT is not an
// admission prerequisite. A durable 202 survives caller disconnection.
func Handler(next http.Handler, invoker Invoker, devices map[string]device.DeviceIdentity, token string, options ...Options) http.Handler {
	var opts Options
	if len(options) > 0 {
		opts = options[0]
	}
	inventory := make(map[string]device.DeviceIdentity, len(devices))
	for k, v := range devices {
		inventory[k] = v
	}
	mux := http.NewServeMux()
	mux.Handle("/", next)
	auth := func(fn func(http.ResponseWriter, *http.Request, device.DeviceIdentity)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if len(token) < 32 || subtle.ConstantTimeCompare([]byte(r.Header.Get("Authorization")), []byte("Bearer "+token)) != 1 {
				http.Error(w, "unauthorized", 401)
				return
			}
			if opts.SiteID != "" {
				w.Header().Set("X-USP-Site", opts.SiteID)
				if site := r.Header.Get("X-USP-Site"); site != "" && site != opts.SiteID {
					http.Error(w, "wrong site", 409)
					return
				}
			}
			id, ok := inventory[r.PathValue("device")]
			if !ok {
				http.Error(w, "device not enabled for actor commands", 404)
				return
			}
			fn(w, r, id)
		}
	}
	mux.HandleFunc("POST /v2/devices/{device}/commands", auth(func(w http.ResponseWriter, r *http.Request, id device.DeviceIdentity) {
		var input deviceactor.CommandInput
		if !decode(w, r, &input) {
			return
		}
		if !validID(string(input.RequestID)) || input.Deadline.IsZero() {
			http.Error(w, "request_id and absolute deadline required", 400)
			return
		}
		if _, err := input.Domain(); err != nil {
			http.Error(w, "invalid USP request", 400)
			return
		}
		b, err := invoker.Invoke(r.Context(), id, "EnqueueCommand", input)
		if err != nil {
			failure(w, err)
			return
		}
		w.Header().Set("Location", "/v2/devices/"+url.PathEscape(id.AgentID)+"/commands/"+url.PathEscape(string(input.RequestID)))
		output(w, 202, b)
	}))
	mux.HandleFunc("GET /v2/devices/{device}/commands/{command}", auth(func(w http.ResponseWriter, r *http.Request, id device.DeviceIdentity) {
		if !validID(r.PathValue("command")) {
			http.Error(w, "invalid command ID", 400)
			return
		}
		b, err := invoker.Invoke(r.Context(), id, "GetCommand", deviceactor.CommandLookup{RequestID: device.RequestID(r.PathValue("command"))})
		if err != nil {
			failure(w, err)
			return
		}
		output(w, 200, b)
	}))
	mux.HandleFunc("POST /v2/devices/{device}/commands/{command}/cancel", auth(func(w http.ResponseWriter, r *http.Request, id device.DeviceIdentity) {
		var input struct {
			ExpectedRevision uint64 `json:"expected_revision"`
		}
		if !decode(w, r, &input) {
			return
		}
		if !validID(r.PathValue("command")) || input.ExpectedRevision == 0 {
			http.Error(w, "invalid cancellation", 400)
			return
		}
		b, err := invoker.Invoke(r.Context(), id, "CancelCommand", deviceactor.CommandCancel{RequestID: device.RequestID(r.PathValue("command")), ExpectedRevision: input.ExpectedRevision})
		if err != nil {
			failure(w, err)
			return
		}
		output(w, 200, b)
	}))
	if opts.TwinEnabled {
		mux.HandleFunc("GET /v2/devices/{device}/twin", auth(func(w http.ResponseWriter, r *http.Request, id device.DeviceIdentity) {
			b, err := invoker.Invoke(r.Context(), id, "GetTwin", nil)
			if err != nil {
				failure(w, err)
				return
			}
			output(w, 200, b)
		}))
		mux.HandleFunc("PATCH /v2/devices/{device}/desired", auth(func(w http.ResponseWriter, r *http.Request, id device.DeviceIdentity) {
			var input DesiredInput
			if !decode(w, r, &input) {
				return
			}
			if !validID(string(input.RequestID)) || strings.HasPrefix(string(input.RequestID), "reconcile-") {
				http.Error(w, "invalid or reserved request ID", 400)
				return
			}
			b, err := invoker.Invoke(r.Context(), id, "SetDesired", device.SetDesiredRequest{ID: input.RequestID, ExpectedRevision: input.ExpectedRevision, Values: input.Values, Remove: input.Remove})
			if err != nil {
				failure(w, err)
				return
			}
			output(w, 200, b)
		}))
		mux.HandleFunc("GET /v2/devices/{device}/status", auth(func(w http.ResponseWriter, r *http.Request, id device.DeviceIdentity) {
			b, err := invoker.Invoke(r.Context(), id, "GetStatus", nil)
			if err != nil {
				failure(w, err)
				return
			}
			output(w, 200, b)
		}))
	}
	return mux
}
func validID(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for _, c := range s {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return false
		}
	}
	return s != "." && s != ".."
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, protocol.MaxRecord)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		http.Error(w, "invalid JSON", 400)
		return false
	}
	if err := d.Decode(new(any)); err != io.EOF {
		http.Error(w, "expected one JSON object", 400)
		return false
	}
	return true
}
func output(w http.ResponseWriter, status int, b []byte) {
	if !json.Valid(b) {
		http.Error(w, "invalid actor response", 502)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
func failure(w http.ResponseWriter, err error) {
	status := 503
	switch {
	case errors.Is(err, deviceactor.ErrInvalid):
		status = 400
	case errors.Is(err, deviceactor.ErrConflict):
		status = 409
	case errors.Is(err, deviceactor.ErrNotFound):
		status = 404
	case errors.Is(err, deviceactor.ErrCapacity):
		status = 429
	}
	http.Error(w, http.StatusText(status)+"; admission may have committed: retry the same request_id and body", status)
}

// Timeout limits API work independently of the command's durable deadline.
func Timeout(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
		defer cancel()
		h.ServeHTTP(w, r.WithContext(ctx))
	})
}
