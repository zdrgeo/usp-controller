package deviceactor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"example.com/usp-controller/internal/device"
)

const ActorType = "DeviceActor"
const ReminderName = "maintenance-v1"
const StateKey = "snapshot-v1"

// ActorID encodes the identity tuple without delimiter collisions. Site is routing
// metadata, not part of identity. No device identifier is changed on the wire.
func ActorID(id device.DeviceIdentity) string {
	b, _ := json.Marshal([3]string{id.Tenant, id.ControllerID, id.AgentID})
	return base64.RawURLEncoding.EncodeToString(b)
}

// DaprBackend uses the stable Dapr HTTP actor API, avoiding an SDK dependency.
// It must only be used under a Dapr actor turn, not as an independent state client.
// Tokens are supplied by the embedding host, never read from persisted state.
type DaprBackend struct {
	client      *http.Client
	base, token string
	period      time.Duration
	maxBytes    int
	actorID     string
}

func NewDaprBackend(endpoint string, client *http.Client, token string, id device.DeviceIdentity, period time.Duration, maxBytes int) (*DaprBackend, error) {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" || u.User != nil || period < time.Second || maxBytes <= 0 {
		return nil, ErrInvalid
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	// Reject redirects to avoid sending API credentials to another origin.
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	actorID := ActorID(id)
	return &DaprBackend{client: &copyClient, base: strings.TrimRight(endpoint, "/") + "/v1.0/actors/" + ActorType + "/" + actorID, token: token, period: period, maxBytes: maxBytes, actorID: actorID}, nil
}
func (d *DaprBackend) call(ctx context.Context, method, path string, body []byte) ([]byte, int, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, d.base+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if d.token != "" {
		req.Header.Set("dapr-api-token", d.token)
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, int64(d.maxBytes)+1))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	if len(b) > d.maxBytes {
		return nil, resp.StatusCode, ErrCapacity
	}
	return b, resp.StatusCode, nil
}
func (d *DaprBackend) Load(ctx context.Context) ([]byte, error) {
	b, status, err := d.call(ctx, http.MethodGet, "/state/"+StateKey, nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNoContent {
		return nil, nil
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("Dapr load: HTTP %d", status)
	}
	if len(bytes.TrimSpace(b)) == 0 || bytes.Equal(bytes.TrimSpace(b), []byte("null")) {
		return nil, fmt.Errorf("Dapr load: invalid empty state")
	}
	return b, nil
}
func (d *DaprBackend) Save(ctx context.Context, b []byte) error {
	if !json.Valid(b) || len(b) > d.maxBytes {
		return ErrInvalid
	}
	ops := []any{map[string]any{"operation": "upsert", "request": map[string]any{"key": StateKey, "value": json.RawMessage(b)}}}
	body, err := json.Marshal(ops)
	if err != nil {
		return err
	}
	_, status, err := d.call(ctx, http.MethodPost, "/state", body)
	if err != nil {
		return err
	}
	if status != http.StatusNoContent {
		return fmt.Errorf("Dapr commit: HTTP %d", status)
	}
	return nil
}
func (d *DaprBackend) EnsureReminder(ctx context.Context) error {
	// Listing distinguishes an absent reminder from an operational error. This
	// adapter targets the Dapr 1.19 actor reminder-list API.
	b, status, err := d.call(ctx, http.MethodGet, "/reminders", nil)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("Dapr reminders: HTTP %d", status)
	}
	if !bytes.HasPrefix(bytes.TrimSpace(b), []byte("{")) {
		return fmt.Errorf("Dapr reminders: invalid response")
	}
	var list struct {
		Reminders []struct {
			Name   string `json:"name"`
			Period string `json:"period"`
			TTL    string `json:"ttl"`
		} `json:"reminders"`
	}
	if err = json.Unmarshal(b, &list); err != nil {
		return err
	}
	if list.Reminders == nil {
		return fmt.Errorf("Dapr reminders: missing list")
	}
	for _, r := range list.Reminders {
		if r.Name == ReminderName {
			period := strings.TrimPrefix(r.Period, "@every ")
			if parsed, e := time.ParseDuration(period); e != nil || parsed != d.period || r.TTL != "" {
				return fmt.Errorf("maintenance reminder configuration conflict")
			}
			return nil
		}
	}
	h := sha256.Sum256([]byte(d.actorID))
	delay := time.Duration(binary.BigEndian.Uint64(h[:8])%uint64(d.period)) + time.Nanosecond
	body, _ := json.Marshal(map[string]string{"dueTime": delay.String(), "period": d.period.String()})
	_, status, err = d.call(ctx, http.MethodPost, "/reminders/"+ReminderName, body)
	if err != nil {
		return err
	}
	if status != http.StatusNoContent {
		return fmt.Errorf("Dapr reminder registration: HTTP %d", status)
	}
	return nil
}
