// Package canary wires opt-in actor execution and an experimental replica profile. It does not change
// default legacy behavior or introduce a second execution path for a device.
package canary

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"

	"example.com/usp-controller/internal/config"
	"example.com/usp-controller/internal/controller"
	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
	"example.com/usp-controller/internal/pb/usp"
	"example.com/usp-controller/internal/protocol"
	"google.golang.org/protobuf/encoding/protojson"
)

type MQTT interface {
	Publish(context.Context, string, []byte) error
	Ready() bool
}
type transport struct {
	mqtt      MQTT
	topic     string
	qos       byte
	available func() bool
}

func (t transport) Ready() bool { return t.mqtt.Ready() && (t.available == nil || t.available()) }
func (t transport) Publish(ctx context.Context, id device.DeviceIdentity, attempt device.PublishAttempt) (device.PublishResult, error) {
	if !t.Ready() {
		return device.PublishResult{}, errors.New("actor transport unavailable or draining")
	}
	b, err := protocol.Encode(id.ControllerID, id.AgentID, attempt.Message)
	if err != nil {
		return device.PublishResult{}, err
	}
	err = t.mqtt.Publish(ctx, t.topic, b)
	return device.PublishResult{BrokerAccepted: err == nil && t.qos == 1}, err
}

type Runtime struct {
	Devices       map[string]device.DeviceIdentity
	Client        *deviceactor.Client
	Host          http.Handler
	legacy        *controller.Controller
	routes        map[string]string
	controllerID  string
	lock          *os.File
	mqtt          MQTT
	replica       *config.Replica
	eventPubsub   string
	verifiedUntil atomic.Int64
	draining      atomic.Bool
}

func New(cfg config.Config, mqtt MQTT, legacy *controller.Controller, appToken, apiToken string) (*Runtime, error) {
	a := cfg.ActorCanary
	if a == nil || !a.Enabled {
		return nil, errors.New("actor canary not enabled")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if len(appToken) < 32 {
		return nil, errors.New("DAPR_APP_TOKEN must contain at least 32 characters")
	}
	if err := os.MkdirAll(filepath.Dir(a.LockPath), 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(a.LockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("single-host canary lock unavailable: %w", err)
	}
	rt := &Runtime{Devices: map[string]device.DeviceIdentity{}, Client: &deviceactor.Client{Endpoint: a.DaprURL, Token: apiToken}, legacy: legacy, controllerID: cfg.EndpointID, routes: map[string]string{}, lock: lock, mqtt: mqtt, replica: a.Replica, eventPubsub: a.EventPubsub}
	fail := func(err error) (*Runtime, error) { rt.Close(); return nil, err }
	for id, agent := range cfg.Agents {
		rt.routes[id] = agent.Topic
	}
	var actors []*deviceactor.Actor
	for _, id := range a.Devices {
		identity := device.DeviceIdentity{Tenant: a.Tenant, ControllerID: cfg.EndpointID, AgentID: id}
		rt.Devices[id] = identity
		opts := deviceactor.DefaultOptions()
		opts.Policy = a.Twin
		backend, err := deviceactor.NewDaprBackend(a.DaprURL, nil, apiToken, identity, 10*time.Second, opts.MaxBytes)
		if err != nil {
			return fail(err)
		}
		actor, err := deviceactor.NewWithEvents(identity, backend, opts, transport{mqtt: mqtt, topic: cfg.Agents[id].Topic, qos: cfg.QoS, available: rt.DispatchReady}, &deviceactor.DaprEventPublisher{Endpoint: a.DaprURL, Token: apiToken, Pubsub: a.EventPubsub, Topic: a.EventTopic})
		if err != nil {
			return fail(err)
		}
		actors = append(actors, actor)
	}
	rt.Host, err = deviceactor.NewHost(actors, appToken, 2<<20)
	if err != nil {
		return fail(err)
	}
	if rt.replica != nil {
		rt.Host = rt.callbackGate(rt.Host)
	}
	return rt, nil
}
func (r *Runtime) Close() error {
	r.BeginDrain()
	if r.lock == nil {
		return nil
	}
	err := r.lock.Close()
	r.lock = nil
	return err
}

// Request excludes canaries from /v1/requests, including when the actor service
// is unavailable. There is deliberately no fallback to legacy publication.
func (r *Runtime) Request(ctx context.Context, id string, req *usp.Request) (*usp.Msg, error) {
	if _, ok := r.Devices[id]; ok {
		return nil, controller.ErrActorManaged
	}
	return r.legacy.Request(ctx, id, req)
}
func (r *Runtime) Receive(ctx context.Context, payload []byte, reply, sender string) error {
	identity, selected := r.Devices[sender]
	if !selected {
		return r.legacy.Receive(ctx, payload, reply, sender)
	}
	inbound, err := protocol.Decode(payload, reply, sender, r.controllerID, r.routes)
	if err != nil {
		return err
	}
	// Control records retain the existing behavior.
	if inbound.Message == nil {
		return r.legacy.Receive(ctx, payload, reply, sender)
	}
	b, err := protojson.Marshal(inbound.Message)
	if err != nil {
		return err
	}
	if inbound.Message.Header.MsgType == usp.Header_NOTIFY {
		if _, err = r.Client.Invoke(ctx, identity, "HandleNotify", deviceactor.ResponseInput{Identity: identity, Message: b}); err != nil {
			return err
		}
		n := inbound.Message.GetBody().GetRequest().GetNotify()
		if !n.SendResp {
			return nil
		}
		response := &usp.Msg{Header: &usp.Header{MsgId: inbound.Message.Header.MsgId, MsgType: usp.Header_NOTIFY_RESP}, Body: &usp.Body{MsgBody: &usp.Body_Response{Response: &usp.Response{RespType: &usp.Response_NotifyResp{NotifyResp: &usp.NotifyResp{SubscriptionId: n.SubscriptionId}}}}}}
		encoded, err := protocol.Encode(r.controllerID, sender, response)
		if err != nil {
			return err
		}
		return r.mqtt.Publish(ctx, reply, encoded)
	}
	_, err = r.Client.Invoke(ctx, identity, "HandleResponse", deviceactor.ResponseInput{Identity: identity, Message: b})
	if errors.Is(err, deviceactor.ErrNotFound) {
		return nil
	}
	return err
}
