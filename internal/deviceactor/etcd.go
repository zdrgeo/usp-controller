package deviceactor

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"example.com/usp-controller/internal/device"
)

// etcdSnapshots uses the v3 JSON gateway directly: compare and put are one
// transaction, including create-if-absent. A false transaction is never success.
// This cluster must have a recovery lifecycle independent of actor Redis.
type etcdSnapshots struct {
	endpoint, key, token string
	client               *http.Client
	maxBytes             int
}

func ValidateEtcdLocation(endpoint, prefix string) error {
	u, e := url.Parse(endpoint)
	if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "http" && u.Scheme != "https") {
		return ErrInvalid
	}
	if u.Scheme == "http" && u.Hostname() != "localhost" && !net.ParseIP(u.Hostname()).IsLoopback() {
		return fmt.Errorf("etcd requires HTTPS outside loopback")
	}
	if !strings.HasPrefix(prefix, "/") || !strings.HasSuffix(prefix, "/") || len(prefix) > 512 || strings.Contains(prefix, "..") || strings.Contains(prefix, "//") {
		return ErrInvalid
	}
	return nil
}

// EtcdHTTPClient supports private CAs and mutual TLS; no insecure TLS option.
func EtcdHTTPClient(caFile, certFile, keyFile string) (*http.Client, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		data, e := os.ReadFile(caFile)
		if e != nil {
			return nil, e
		}
		roots, e := x509.SystemCertPool()
		if e != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(data) {
			return nil, fmt.Errorf("invalid etcd CA")
		}
		cfg.RootCAs = roots
	}
	if (certFile == "") != (keyFile == "") {
		return nil, fmt.Errorf("both etcd client certificate and key required")
	}
	if certFile != "" {
		cert, e := tls.LoadX509KeyPair(certFile, keyFile)
		if e != nil {
			return nil, e
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = cfg
	return &http.Client{Transport: transport, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, nil
}

// NewEtcdSnapshotBackend keeps Dapr only for reminders and explicit legacy import.
func NewEtcdSnapshotBackend(daprEndpoint, daprToken, endpoint, prefix, token string, client *http.Client, id device.DeviceIdentity, period time.Duration, maxBytes int) (*FencedDaprBackend, error) {
	if e := ValidateEtcdLocation(endpoint, prefix); e != nil {
		return nil, e
	}
	b, e := NewFencedDaprBackend(daprEndpoint, nil, daprToken, id, period, maxBytes, "unused-etcd-snapshots")
	if e != nil {
		return nil, e
	}
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	b.etcd = &etcdSnapshots{endpoint: strings.TrimRight(endpoint, "/"), key: prefix + ActorID(id), token: token, client: &copyClient, maxBytes: maxBytes}
	return b, nil
}
func (e *etcdSnapshots) call(ctx context.Context, path string, input, output any) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.endpoint+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if e.token != "" {
		req.Header.Set("Authorization", e.token)
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("etcd request: HTTP %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, int64(e.maxBytes)*2+4097))
	if err != nil {
		return err
	}
	if len(data) > e.maxBytes*2+4096 {
		return ErrCapacity
	}
	return json.Unmarshal(data, output)
}
func (e *etcdSnapshots) read(ctx context.Context) ([]byte, string, int, error) {
	var result struct {
		KVs []struct {
			Key, Value  string
			ModRevision string `json:"mod_revision"`
		} `json:"kvs"`
	}
	err := e.call(ctx, "/v3/kv/range", map[string]any{"key": base64.StdEncoding.EncodeToString([]byte(e.key)), "serializable": false}, &result)
	if err != nil {
		return nil, "", 0, err
	}
	if len(result.KVs) == 0 {
		return nil, "", http.StatusNoContent, nil
	}
	if len(result.KVs) != 1 {
		return nil, "", 0, ErrInvalid
	}
	kv := result.KVs[0]
	key, err := base64.StdEncoding.DecodeString(kv.Key)
	if err != nil || string(key) != e.key {
		return nil, "", 0, ErrInvalid
	}
	revision, err := strconv.ParseInt(kv.ModRevision, 10, 64)
	if err != nil || revision <= 0 {
		return nil, "", 0, ErrInvalid
	}
	data, err := base64.StdEncoding.DecodeString(kv.Value)
	if err != nil {
		return nil, "", 0, err
	}
	if len(data) > e.maxBytes {
		return nil, "", 0, ErrCapacity
	}
	return data, kv.ModRevision, http.StatusOK, nil
}
func (e *etcdSnapshots) save(ctx context.Context, data []byte, etag string) (string, error) {
	revision := "0"
	target := "CREATE"
	field := "create_revision"
	if etag != "" {
		n, err := strconv.ParseInt(etag, 10, 64)
		if err != nil || n <= 0 {
			return "", ErrInvalid
		}
		revision = etag
		target = "MOD"
		field = "mod_revision"
	}
	key := base64.StdEncoding.EncodeToString([]byte(e.key))
	compare := map[string]any{"key": key, "target": target, "result": "EQUAL", field: revision}
	input := map[string]any{"compare": []any{compare}, "success": []any{map[string]any{"request_put": map[string]string{"key": key, "value": base64.StdEncoding.EncodeToString(data)}}}}
	var result struct {
		Header struct {
			Revision string `json:"revision"`
		} `json:"header"`
		Succeeded bool `json:"succeeded"`
	}
	if err := e.call(ctx, "/v3/kv/txn", input, &result); err != nil {
		return "", err
	}
	n, err := strconv.ParseInt(result.Header.Revision, 10, 64)
	if err != nil || n <= 0 {
		return "", ErrInvalid
	}
	if !result.Succeeded {
		return "", ErrConflict
	}
	return result.Header.Revision, nil
}
