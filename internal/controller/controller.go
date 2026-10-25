package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"example.com/usp-controller/internal/protocol"
	"google.golang.org/protobuf/encoding/protojson"
)

// ProtocolVersion identifies the supported USP profile.
const ProtocolVersion = protocol.ProtocolVersion

const MaxRecord = protocol.MaxRecord

var ErrActorManaged = errors.New("device uses asynchronous actor command API")

var ErrBusy = errors.New("pending request capacity reached")
var ErrUnknownAgent = errors.New("unknown agent")

type Transport interface {
	Publish(context.Context, string, []byte) error
	Ready() bool
}
type Journal interface{ Append(string, []byte) error }
type pending struct {
	agent    string
	expected usp.Header_MsgType
	result   chan *usp.Msg
}
type Controller struct {
	id        string
	agents    map[string]string
	transport Transport
	journal   Journal
	mu        sync.Mutex
	pending   map[string]*pending
	capacity  int
}

func New(id string, agents map[string]string, transport Transport, journal Journal, capacity int) *Controller {
	copyAgents := make(map[string]string, len(agents))
	for k, v := range agents {
		copyAgents[k] = v
	}
	return &Controller{id: id, agents: copyAgents, transport: transport, journal: journal, pending: make(map[string]*pending), capacity: capacity}
}

// RequestType is retained for existing callers; wire rules live in protocol.
func RequestType(req *usp.Request) (usp.Header_MsgType, error) {
	return protocol.RequestType(req)
}

func (c *Controller) Request(ctx context.Context, agent string, req *usp.Request) (*usp.Msg, error) {
	topic, ok := c.agents[agent]
	if !ok {
		return nil, ErrUnknownAgent
	}
	kind, err := RequestType(req)
	if err != nil {
		return nil, err
	}
	if !c.transport.Ready() {
		return nil, errors.New("MQTT transport is not ready")
	}
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return nil, err
	}
	id := hex.EncodeToString(idBytes)
	p := &pending{agent: agent, expected: kind + 1, result: make(chan *usp.Msg, 1)}
	c.mu.Lock()
	if len(c.pending) >= c.capacity {
		c.mu.Unlock()
		return nil, ErrBusy
	}
	c.pending[id] = p
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.pending, id); c.mu.Unlock() }()
	msg := &usp.Msg{Header: &usp.Header{MsgId: id, MsgType: kind}, Body: &usp.Body{MsgBody: &usp.Body_Request{Request: req}}}
	payload, err := Encode(c.id, agent, msg)
	if err != nil {
		return nil, err
	}
	// No automatic application retry: a timeout can mean the operation executed.
	if err := c.transport.Publish(ctx, topic, payload); err != nil {
		return nil, fmt.Errorf("publish outcome may be unknown: %w", err)
	}
	select {
	case response := <-p.result:
		return response, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Encode is retained for existing callers; wire rules live in protocol.
func Encode(from, to string, msg *usp.Msg) ([]byte, error) {
	return protocol.Encode(from, to, msg)
}

func (c *Controller) Receive(ctx context.Context, payload []byte, replyTopic, sender string) error {
	inbound, err := protocol.Decode(payload, replyTopic, sender, c.id, c.agents)
	if err != nil {
		return err
	}
	msg := inbound.Message
	if msg == nil {
		return nil
	}
	if msg.Header.MsgType == usp.Header_NOTIFY {
		n := msg.GetBody().GetRequest().GetNotify()
		b, err := protojson.Marshal(msg)
		if err != nil {
			return err
		}
		if err := c.journal.Append(inbound.Sender, b); err != nil {
			return fmt.Errorf("persist notification: %w", err)
		}
		if n.SendResp {
			response := &usp.Msg{Header: &usp.Header{MsgId: msg.Header.MsgId, MsgType: usp.Header_NOTIFY_RESP}, Body: &usp.Body{MsgBody: &usp.Body_Response{Response: &usp.Response{RespType: &usp.Response_NotifyResp{NotifyResp: &usp.NotifyResp{SubscriptionId: n.SubscriptionId}}}}}}
			b, err := Encode(c.id, inbound.Sender, response)
			if err != nil {
				return err
			}
			return c.transport.Publish(ctx, replyTopic, b)
		}
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pending[msg.Header.MsgId]
	if !ok {
		return nil
	} // late/duplicate responses
	if p.agent != inbound.Sender {
		return errors.New("response sender does not match request")
	}
	if msg.Header.MsgType != p.expected && msg.Header.MsgType != usp.Header_ERROR {
		return errors.New("response operation does not match request")
	}
	select {
	case p.result <- msg:
	default:
	}
	return nil
}

// The legacy controller continues to implement the synchronous Device boundary.
var _ device.DeviceRequester = (*Controller)(nil)
