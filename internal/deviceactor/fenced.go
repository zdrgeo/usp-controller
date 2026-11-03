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

// FencedDaprBackend coordinates versioned snapshots and Dapr reminders. Replica
// runtime uses the independent etcd adapter; standard Dapr state access remains
// available for exporting older Redis snapshots and compatibility tests.
// One instance belongs to one Actor and is used under its local mutex.
type FencedDaprBackend struct {
	*DaprBackend
	etcd          *etcdSnapshots
	stateURL, key string
	identity      device.DeviceIdentity
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
	return &FencedDaprBackend{DaprBackend: b, stateURL: strings.TrimRight(endpoint, "/") + "/v1.0/state/" + url.PathEscape(store), key: "device-snapshot-cas-v1-" + ActorID(id), identity: id}, nil
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
	if b.etcd != nil {
		return b.etcd.read(ctx)
	}
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
		return nil, fmt.Errorf("%w: fenced snapshot missing; explicit initialization or recovery required", ErrNotFound)
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
	if !b.loaded || b.etag == "" {
		return fmt.Errorf("fenced save requires a successful load")
	}
	if !json.Valid(data) || len(data) > b.maxBytes {
		return ErrInvalid
	}
	etag := b.etag
	if b.etcd != nil {
		return b.commit(ctx, data, etag)
	}
	// Redis accepts an ETag write when a key is absent. Reject already-visible
	// deletion before sending. This is not atomic with Save: concurrent raw key
	// deletion remains unsupported and must never be part of a restore workflow.
	if _, current, status, err := b.read(ctx); err != nil || status != http.StatusOK || current != etag {
		b.loaded = false
		b.etag = ""
		if err != nil {
			return err
		}
		return ErrConflict
	}
	return b.commit(ctx, data, etag)
}

// commit is also used by explicit initialization. Only initialization omits ETag.
func (b *FencedDaprBackend) commit(ctx context.Context, data []byte, etag string) error {
	b.loaded = false
	b.etag = ""
	if b.etcd != nil {
		next, err := b.etcd.save(ctx, data, etag)
		if err != nil {
			return err
		}
		b.etag = next
		b.loaded = true
		return nil
	}
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
