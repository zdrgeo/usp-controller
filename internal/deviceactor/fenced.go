package deviceactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"example.com/usp-controller/internal/device"
)

// FencedDaprBackend stores one snapshot with first-write ETag concurrency through
// the standard state API. Dapr's actor HTTP read API does not expose ETags. The
// actor still supplies turn routing and reminders; storage enforces write ordering.
// One instance belongs to one Actor and is used under its local mutex.
type FencedDaprBackend struct {
	*DaprBackend
	stateURL, key string
	etag          string
	loaded        bool
}

func NewFencedDaprBackend(endpoint string, client *http.Client, token string, id device.DeviceIdentity, period time.Duration, maxBytes int, store string) (*FencedDaprBackend, error) {
	if store == "" || strings.ContainsAny(store, "/|?# ") {
		return nil, ErrInvalid
	}
	b, err := NewDaprBackend(endpoint, client, token, id, period, maxBytes)
	if err != nil {
		return nil, err
	}
	return &FencedDaprBackend{DaprBackend: b, stateURL: strings.TrimRight(endpoint, "/") + "/v1.0/state/" + url.PathEscape(store), key: "device-snapshot-cas-v1-" + ActorID(id)}, nil
}
func (b *FencedDaprBackend) request(ctx context.Context, method, path string, body []byte) ([]byte, string, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, b.stateURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, "", 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	if b.token != "" {
		req.Header.Set("dapr-api-token", b.token)
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, "", 0, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, int64(b.maxBytes)+1))
	if err != nil {
		return nil, "", resp.StatusCode, err
	}
	if len(data) > b.maxBytes {
		return nil, "", resp.StatusCode, ErrCapacity
	}
	return data, resp.Header.Get("ETag"), resp.StatusCode, nil
}
func (b *FencedDaprBackend) read(ctx context.Context) ([]byte, string, int, error) {
	return b.request(ctx, http.MethodGet, "/"+url.PathEscape(b.key)+"?consistency=strong", nil)
}
func (b *FencedDaprBackend) Load(ctx context.Context) ([]byte, error) {
	b.loaded = false
	b.etag = ""
	data, etag, status, err := b.read(ctx)
	if err != nil {
		return nil, err
	}
	switch status {
	case http.StatusNoContent:
		// Quiesced migration: old hosts must be retired before enabling this backend.
		// First-write concurrency elects one initializer; old state is never modified.
		data, err = b.DaprBackend.Load(ctx)
		if err != nil {
			return nil, err
		}
	case http.StatusOK:
		if etag == "" || !json.Valid(data) || bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
			return nil, fmt.Errorf("fenced snapshot requires valid state and ETag")
		}
	default:
		return nil, fmt.Errorf("fenced load: HTTP %d", status)
	}
	b.etag = etag
	b.loaded = true
	return data, nil
}
func (b *FencedDaprBackend) Save(ctx context.Context, data []byte) error {
	if !b.loaded {
		return fmt.Errorf("fenced save requires a successful load")
	}
	if !json.Valid(data) || len(data) > b.maxBytes {
		return ErrInvalid
	}
	etag := b.etag
	b.loaded = false
	b.etag = ""
	request := map[string]any{"key": b.key, "value": json.RawMessage(data), "options": map[string]string{"concurrency": "first-write", "consistency": "strong"}}
	if etag != "" {
		request["etag"] = etag
	}
	body, _ := json.Marshal([]any{request})
	_, _, status, err := b.request(ctx, http.MethodPost, "", body)
	if err != nil {
		return err
	}
	if status == http.StatusConflict {
		return ErrConflict
	}
	if status != http.StatusNoContent {
		return fmt.Errorf("fenced commit: HTTP %d", status)
	}
	// Writes do not return ETags. Never pair a stale local snapshot with a newer
	// writer's ETag: verify the exact saved value before allowing another save.
	saved, next, status, err := b.read(ctx)
	if err != nil {
		return err
	}
	if status != http.StatusOK || next == "" || !sameStateJSON(saved, data) {
		return ErrConflict
	}
	b.etag = next
	b.loaded = true
	return nil
}

func sameStateJSON(a, b []byte) bool {
	if !json.Valid(a) || !json.Valid(b) {
		return false
	}
	normalize := func(data []byte) ([]byte, error) {
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.UseNumber()
		var value any
		if err := decoder.Decode(&value); err != nil {
			return nil, err
		}
		return json.Marshal(value)
	}
	x, err := normalize(a)
	if err != nil {
		return false
	}
	y, err := normalize(b)
	return err == nil && bytes.Equal(x, y)
}
