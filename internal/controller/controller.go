package controller

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	"example.com/usp-controller/internal/pb/record"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// ProtocolVersion identifies the supported USP profile.
const ProtocolVersion = "1.5"

const MaxRecord = 1 << 20

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

// RequestType derives the header from the protobuf oneof, avoiding header/body mismatches.
func RequestType(req *usp.Request) (usp.Header_MsgType, error) {
	if req == nil {
		return 0, errors.New("request required")
	}
	r := req.ProtoReflect()
	field := r.WhichOneof(r.Descriptor().Oneofs().ByName("req_type"))
	if field == nil {
		return 0, errors.New("request operation required")
	}
	name := strings.ToUpper(string(field.Name()))
	if name == "GET_SUPPORTED_PROTOCOL" {
		name = "GET_SUPPORTED_PROTO"
	}
	n, ok := usp.Header_MsgType_value[name]
	if !ok || name == "NOTIFY" || name == "REGISTER" || name == "DEREGISTER" {
		return 0, errors.New("unsupported controller operation")
	}
	return usp.Header_MsgType(n), nil
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

func Encode(from, to string, msg *usp.Msg) ([]byte, error) {
	b, err := proto.Marshal(msg)
	if err != nil {
		return nil, err
	}
	r := &record.Record{Version: ProtocolVersion, FromId: from, ToId: to, RecordType: &record.Record_NoSessionContext{NoSessionContext: &record.NoSessionContextRecord{Payload: b}}}
	result, err := proto.Marshal(r)
	if len(result) > MaxRecord {
		return nil, errors.New("USP record exceeds limit")
	}
	return result, err
}

func (c *Controller) Receive(ctx context.Context, payload []byte, replyTopic, sender string) error {
	if len(payload) > MaxRecord {
		return errors.New("oversized USP record")
	}
	r := new(record.Record)
	if err := proto.Unmarshal(payload, r); err != nil {
		return err
	}
	topic, ok := c.agents[r.FromId]
	if !ok || r.ToId != c.id || r.FromId != sender {
		return errors.New("unknown sender, inbox mismatch or wrong recipient")
	}
	// Broker ACLs must restrict each agent to its own Controller inbox.
	if replyTopic != topic {
		return errors.New("reply topic does not match provisioned agent")
	}
	if (r.OriginatorId != "" && r.OriginatorId != r.FromId) || (r.DestinationId != "" && r.DestinationId != r.ToId) {
		return errors.New("forwarded USP records are not supported by this profile")
	}
	if r.Version != ProtocolVersion {
		return errors.New("only USP 1.5 is enabled")
	}
	if r.PayloadSecurity != record.Record_PLAINTEXT || len(r.MacSignature) > 0 || len(r.SenderCert) > 0 {
		return errors.New("secured record contexts are not supported")
	}
	if connect := r.GetMqttConnect(); connect != nil {
		if connect.Version != record.MQTTConnectRecord_V5 || connect.SubscribedTopic != topic {
			return errors.New("incompatible MQTT connect record")
		}
		return nil
	}
	if r.GetDisconnect() != nil {
		return nil
	}
	session := r.GetNoSessionContext()
	if session == nil {
		return errors.New("only no-session-context records are supported")
	}
	msg := new(usp.Msg)
	if err := proto.Unmarshal(session.Payload, msg); err != nil {
		return err
	}
	if msg.Header == nil || msg.Header.MsgId == "" || msg.Body == nil {
		return errors.New("missing USP header, message ID or body")
	}
	if msg.Header.MsgType == usp.Header_NOTIFY {
		n := msg.GetBody().GetRequest().GetNotify()
		if n == nil || n.Notification == nil {
			return errors.New("invalid Notify body")
		}
		b, err := protojson.Marshal(msg)
		if err != nil {
			return err
		}
		if err := c.journal.Append(r.FromId, b); err != nil {
			return fmt.Errorf("persist notification: %w", err)
		}
		if n.SendResp {
			response := &usp.Msg{Header: &usp.Header{MsgId: msg.Header.MsgId, MsgType: usp.Header_NOTIFY_RESP}, Body: &usp.Body{MsgBody: &usp.Body_Response{Response: &usp.Response{RespType: &usp.Response_NotifyResp{NotifyResp: &usp.NotifyResp{SubscriptionId: n.SubscriptionId}}}}}}
			b, err := Encode(c.id, r.FromId, response)
			if err != nil {
				return err
			}
			return c.transport.Publish(ctx, replyTopic, b)
		}
		return nil
	}
	if err := validateResponse(msg); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	p, ok := c.pending[msg.Header.MsgId]
	if !ok {
		return nil
	} // late/duplicate responses
	if p.agent != r.FromId {
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

func validateResponse(msg *usp.Msg) error {
	if msg.Header.MsgType == usp.Header_ERROR {
		if msg.Body.GetError() == nil {
			return errors.New("invalid Error body")
		}
		return nil
	}
	response := msg.Body.GetResponse()
	if response == nil {
		return errors.New("expected Response body")
	}
	r := response.ProtoReflect()
	f := r.WhichOneof(r.Descriptor().Oneofs().ByName("resp_type"))
	if f == nil {
		return errors.New("missing response operation")
	}
	name := strings.ToUpper(string(f.Name()))
	name = strings.Replace(name, "GET_SUPPORTED_PROTOCOL_RESP", "GET_SUPPORTED_PROTO_RESP", 1)
	n, ok := usp.Header_MsgType_value[name]
	if !ok || usp.Header_MsgType(n) != msg.Header.MsgType {
		return errors.New("response header/body mismatch")
	}
	return nil
}
