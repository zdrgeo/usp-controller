package api

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"example.com/usp-controller/internal/controller"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/encoding/protojson"
)

type Requester interface {
	Request(context.Context, string, *usp.Request) (*usp.Msg, error)
}

func Handler(c Requester, ready func() bool, token string, timeout time.Duration) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if !ready() {
			http.Error(w, "controller not ready", 503)
			return
		}
		w.WriteHeader(200)
	})
	mux.HandleFunc("POST /v1/requests", func(w http.ResponseWriter, r *http.Request) {
		supplied := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if token == "" || subtle.ConstantTimeCompare([]byte(supplied), []byte(token)) != 1 || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			http.Error(w, "unauthorized", 401)
			return
		}
		if !ready() {
			http.Error(w, "controller not ready", 503)
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, controller.MaxRecord)
		var input struct {
			AgentID string          `json:"agent_id"`
			Request json.RawMessage `json:"request"`
		}
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			http.Error(w, "invalid request JSON", 400)
			return
		}
		if err := decoder.Decode(new(any)); err != io.EOF {
			http.Error(w, "expected one JSON object", 400)
			return
		}
		req := new(usp.Request)
		if err := protojson.Unmarshal(input.Request, req); err != nil {
			http.Error(w, "invalid USP request: "+err.Error(), 400)
			return
		}
		if _, err := controller.RequestType(req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		response, err := c.Request(ctx, input.AgentID, req)
		if err != nil {
			status := http.StatusBadGateway
			switch {
			case errors.Is(err, controller.ErrUnknownAgent):
				status = 404
			case errors.Is(err, controller.ErrBusy):
				status = 429
			case errors.Is(err, context.DeadlineExceeded):
				status = 504
			case errors.Is(err, context.Canceled):
				status = 408
			}
			http.Error(w, err.Error()+"; execution may be unknown; do not blindly retry", status)
			return
		}
		b, err := protojson.Marshal(response)
		if err != nil {
			http.Error(w, "response encoding failed", 500)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Write(b)
	})
	return mux
}
