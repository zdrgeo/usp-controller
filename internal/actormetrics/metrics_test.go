package actormetrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
)

type blockedWriter struct {
	header           http.Header
	entered, release chan struct{}
	once             sync.Once
}

func (w *blockedWriter) Header() http.Header { return w.header }
func (w *blockedWriter) WriteHeader(int)     {}
func (w *blockedWriter) Write(data []byte) (int, error) {
	w.once.Do(func() { close(w.entered) })
	<-w.release
	return len(data), nil
}

func TestMetricsPrivacyBoundsAndSlowScrape(t *testing.T) {
	m := New()
	m.Observe("actor", "Tick", "conflict", 5*time.Millisecond)
	// Arbitrary caller input must not become a label or allocate a series.
	for i := 0; i < 1000; i++ {
		m.Observe("actor", fmt.Sprintf("sensitive-device-%d", i), "ok", time.Second)
		m.Observe("actor", "Tick", fmt.Sprintf("sensitive-reason-%d", i), time.Second)
	}
	if len(m.samples) != 1 {
		t.Fatal("unbounded labels admitted")
	}
	token := strings.Repeat("x", 32)
	handler := m.Handler(http.NotFoundHandler(), token, "site")
	request := func() *http.Request {
		r := httptest.NewRequest("GET", "/v2/metrics", nil)
		r.Header.Set("Authorization", "Bearer "+token)
		return r
	}
	unauthorized := request()
	unauthorized.Header.Set("Authorization", "Bearer wrong")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, unauthorized)
	if w.Code != 401 {
		t.Fatal("metrics exposed without authentication")
	}
	wrongSite := request()
	wrongSite.Header.Set("X-USP-Site", "other")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, wrongSite)
	if w.Code != 409 {
		t.Fatal("wrong site accepted")
	}
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, request())
	body := w.Body.String()
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), "version=0.0.4") || strings.Contains(body, "sensitive-") || !strings.HasSuffix(body, "\n") {
		t.Fatal("invalid or leaking exposition", body)
	}
	if !strings.Contains(body, `usp_device_operations_total{boundary="actor",operation="Tick",result="conflict"} 1`) || !strings.Contains(body, `result="conflict",le="+Inf"} 1`) {
		t.Fatal("missing counter/histogram", body)
	}
	slow := &blockedWriter{header: http.Header{}, entered: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	go func() { handler.ServeHTTP(slow, request()); close(done) }()
	var release sync.Once
	t.Cleanup(func() { release.Do(func() { close(slow.release) }); <-done })
	select {
	case <-slow.entered:
	case <-time.After(time.Second):
		t.Fatal("scrape did not reach writer")
	}
	recorded := make(chan struct{})
	go func() { m.Observe("actor", "Tick", "ok", time.Millisecond); close(recorded) }()
	select {
	case <-recorded:
	case <-time.After(time.Second):
		t.Fatal("slow scrape blocked operation recording")
	}
	release.Do(func() { close(slow.release) })
	<-done
}

type uncertainBoundary struct {
	calls int
	err   error
	ctx   context.Context
}

func (b *uncertainBoundary) Load(ctx context.Context) ([]byte, error) {
	b.calls++
	b.ctx = ctx
	return []byte("sensitive-payload"), b.err
}
func (b *uncertainBoundary) Save(ctx context.Context, _ []byte) error {
	b.calls++
	b.ctx = ctx
	return b.err
}
func (b *uncertainBoundary) EnsureReminder(ctx context.Context) error {
	b.calls++
	b.ctx = ctx
	return b.err
}
func (b *uncertainBoundary) Ready() bool { return false }
func (b *uncertainBoundary) Publish(ctx context.Context, _ device.DeviceIdentity, _ device.PublishAttempt) (device.PublishResult, error) {
	b.calls++
	b.ctx = ctx
	return device.PublishResult{BrokerAccepted: true}, b.err
}

type uncertainPublisher struct {
	calls int
	err   error
}

func (p *uncertainPublisher) Publish(context.Context, device.DeviceEvent) error {
	p.calls++
	return p.err
}

func TestBoundaryInstrumentationNeverRetriesOrChangesUncertainty(t *testing.T) {
	m := New()
	original := errors.New("possibly committed")
	underlying := &uncertainBoundary{err: original}
	backend := m.Backend(underlying)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	data, err := backend.Load(ctx)
	if err != original || string(data) != "sensitive-payload" {
		t.Fatal("changed load result")
	}
	if backend.Save(ctx, data) != original || backend.EnsureReminder(ctx) != original || underlying.calls != 3 || underlying.ctx != ctx {
		t.Fatal("retried, changed error or detached cancellation")
	}
	transport := m.Transport(underlying)
	if transport.(interface{ Ready() bool }).Ready() {
		t.Fatal("changed readiness")
	}
	result, err := transport.Publish(ctx, device.DeviceIdentity{AgentID: "sensitive-device"}, device.PublishAttempt{})
	if err != original || !result.BrokerAccepted || underlying.calls != 4 || underlying.ctx != ctx {
		t.Fatal("changed or retried publish")
	}
	p := &uncertainPublisher{err: original}
	if m.Publisher(p).Publish(ctx, device.DeviceEvent{}) != original || p.calls != 1 {
		t.Fatal("retried event handoff")
	}
	if m.samples[key{"transport", "publish", "uncertain"}].count != 1 || m.samples[key{"transport", "publish", "accepted"}].count != 0 || m.samples[key{"events", "handoff", "uncertain"}].count != 1 {
		t.Fatal("claimed success after uncertain result")
	}
	token := strings.Repeat("x", 32)
	request := httptest.NewRequest("GET", "/v2/metrics", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	m.Handler(http.NotFoundHandler(), token).ServeHTTP(w, request)
	if strings.Contains(w.Body.String(), "sensitive") || strings.Contains(w.Body.String(), original.Error()) {
		t.Fatal("payload/error detail leaked")
	}
}
