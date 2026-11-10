package actormetrics

import (
	"context"
	"time"

	"example.com/usp-controller/internal/device"
)

type Backend interface {
	Load(context.Context) ([]byte, error)
	Save(context.Context, []byte) error
	EnsureReminder(context.Context) error
}
type backend struct {
	Backend
	metrics *Registry
}

func (m *Registry) Backend(b Backend) Backend {
	if b == nil {
		return nil
	}
	return &backend{Backend: b, metrics: m}
}
func outcome(err error) string {
	if err != nil {
		return "error"
	}
	return "ok"
}
func (b *backend) Load(ctx context.Context) ([]byte, error) {
	start := time.Now()
	label := "error"
	defer func() { b.metrics.Observe("snapshot", "load", label, time.Since(start)) }()
	data, err := b.Backend.Load(ctx)
	label = outcome(err)
	return data, err
}
func (b *backend) Save(ctx context.Context, data []byte) error {
	start := time.Now()
	label := "error"
	defer func() { b.metrics.Observe("snapshot", "save", label, time.Since(start)) }()
	err := b.Backend.Save(ctx, data)
	label = outcome(err)
	return err
}
func (b *backend) EnsureReminder(ctx context.Context) error {
	start := time.Now()
	label := "error"
	defer func() { b.metrics.Observe("snapshot", "reminder", label, time.Since(start)) }()
	err := b.Backend.EnsureReminder(ctx)
	label = outcome(err)
	return err
}

type transport struct {
	inner   device.DeviceTransport
	metrics *Registry
}

func (m *Registry) Transport(t device.DeviceTransport) device.DeviceTransport {
	if t == nil {
		return nil
	}
	return &transport{inner: t, metrics: m}
}
func (t *transport) Ready() bool {
	if ready, ok := t.inner.(interface{ Ready() bool }); ok {
		return ready.Ready()
	}
	return true
}
func (t *transport) Publish(ctx context.Context, id device.DeviceIdentity, attempt device.PublishAttempt) (device.PublishResult, error) {
	start := time.Now()
	label := "uncertain"
	defer func() { t.metrics.Observe("transport", "publish", label, time.Since(start)) }()
	result, err := t.inner.Publish(ctx, id, attempt)
	if err == nil {
		label = "unconfirmed"
		if result.BrokerAccepted {
			label = "accepted"
		}
	}
	return result, err
}

type publisher struct {
	inner   device.DeviceEventPublisher
	metrics *Registry
}

func (m *Registry) Publisher(p device.DeviceEventPublisher) device.DeviceEventPublisher {
	if p == nil {
		return nil
	}
	return &publisher{inner: p, metrics: m}
}
func (p *publisher) Publish(ctx context.Context, event device.DeviceEvent) error {
	start := time.Now()
	label := "uncertain"
	defer func() { p.metrics.Observe("events", "handoff", label, time.Since(start)) }()
	err := p.inner.Publish(ctx, event)
	if err == nil {
		label = "ok"
	}
	return err
}
