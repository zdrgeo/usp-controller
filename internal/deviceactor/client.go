package deviceactor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"example.com/usp-controller/internal/device"
)

// Client always invokes through Dapr, so callbacks execute under actor turn locks.
// The transport is not retried by this client. Stable submission IDs make runtime
// invocation retries harmless; Tick never replays a persisted attempt.
type Client struct {
	Endpoint, Token string
	HTTP            *http.Client
}

func (c *Client) Invoke(ctx context.Context, id device.DeviceIdentity, method string, input any) (json.RawMessage, error) {
	body, err := json.Marshal(input)
	if err != nil {
		return nil, ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.Endpoint, "/")+"/v1.0/actors/"+ActorType+"/"+ActorID(id)+"/method/"+method, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.Token != "" {
		req.Header.Set("dapr-api-token", c.Token)
	}
	client := http.DefaultClient
	if c.HTTP != nil {
		client = c.HTTP
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := copyClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		switch resp.StatusCode {
		case 400:
			return nil, ErrInvalid
		case 404:
			return nil, ErrNotFound
		case 409:
			return nil, ErrConflict
		case 429:
			return nil, ErrCapacity
		default:
			return nil, fmt.Errorf("actor invocation HTTP %d", resp.StatusCode)
		}
	}
	return b, nil
}
