package deviceactor

import (
	"context"
	"errors"
	"net/http"
	"time"

	"example.com/usp-controller/internal/actormetrics"
)

// Known domain errors travel in a successful Dapr envelope. Record their actual
// outcome without inspecting, buffering or exposing response/command payloads.
type callMetricsWriter struct {
	http.ResponseWriter
	status      int
	domainError error
}

func (w *callMetricsWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *callMetricsWriter) Write(data []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	return w.ResponseWriter.Write(data)
}
func (w *callMetricsWriter) actorError(err error) { w.domainError = err }

func actorOutcome(err error, status int) string {
	switch {
	case errors.Is(err, ErrInvalid):
		return "invalid"
	case errors.Is(err, ErrConflict):
		return "conflict"
	case errors.Is(err, ErrCapacity):
		return "capacity"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrDisabled):
		return "disabled"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case err != nil:
		return "error"
	}
	switch status {
	case 200:
		return "ok"
	case 400, 413:
		return "invalid"
	case 404:
		return "not_found"
	default:
		return "error"
	}
}
func observeActor(m *actormetrics.Registry, operation string, start time.Time, w *callMetricsWriter) {
	m.Observe("actor", operation, actorOutcome(w.domainError, w.status), time.Since(start))
}
