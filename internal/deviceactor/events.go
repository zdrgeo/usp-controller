package deviceactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"example.com/usp-controller/internal/device"
	"google.golang.org/protobuf/encoding/protojson"
)

// DaprEventPublisher requires a durable, acknowledged pubsub component. Success
// means handoff to that component, not consumer completion. Never use a volatile
// component for this contract. The outer envelope is stable on every retry.
type DaprEventPublisher struct{ Endpoint, Token, Pubsub, Topic string }

func (p *DaprEventPublisher) Publish(ctx context.Context, e device.DeviceEvent) error {
	message, err := protojson.Marshal(e.Message)
	if err != nil {
		return err
	}
	body, err := json.Marshal(struct {
		ActorID       string                `json:"actor_id"`
		Identity      device.DeviceIdentity `json:"identity"`
		Epoch         string                `json:"epoch"`
		Sequence      uint64                `json:"sequence"`
		SchemaVersion string                `json:"schema_version"`
		ReceivedAt    time.Time             `json:"received_at"`
		Message       json.RawMessage       `json:"message"`
	}{ActorID(e.Identity), e.Identity, e.Epoch, e.Sequence, e.SchemaVersion, e.ReceivedAt, message})
	if err != nil {
		return err
	}
	endpoint := p.Endpoint + "/v1.0/publish/" + url.PathEscape(p.Pubsub) + "/" + url.PathEscape(p.Topic) + "?metadata.rawPayload=true"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if p.Token != "" {
		req.Header.Set("dapr-api-token", p.Token)
	}
	client := &http.Client{Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("event publish failed: HTTP %d", resp.StatusCode)
	}
	return nil
}
