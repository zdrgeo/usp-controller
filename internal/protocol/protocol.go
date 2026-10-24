// Package protocol implements the supported USP wire profile without persistence or delivery effects.
package protocol

import (
	"errors"
	"strings"

	"example.com/usp-controller/internal/pb/record"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
)

const ProtocolVersion = "1.5"
const MaxRecord = 1 << 20

// Inbound is a decoded, validated record. Message is nil for Connect/Disconnect.
// Validation assumes the caller binds sender to an authenticated broker inbox.
// The returned protobuf is owned by the caller and must not be mutated concurrently.
type Inbound struct {
	Sender  string
	Message *usp.Msg
}

// Decode validates a record against provisioned endpoint/topic routing. It does
// not correlate responses with outstanding requests or classify operation success.
// routes must remain immutable during the call.
func Decode(payload []byte, replyTopic, sender, controllerID string, routes map[string]string) (*Inbound, error) {
	if len(payload) > MaxRecord {
		return nil, errors.New("oversized USP record")
	}
	r := new(record.Record)
	if err := proto.Unmarshal(payload, r); err != nil {
		return nil, err
	}
	topic, ok := routes[r.FromId]
	if !ok || r.ToId != controllerID || r.FromId != sender {
		return nil, errors.New("unknown sender, inbox mismatch or wrong recipient")
	}
	// Broker ACLs must restrict each agent to its own Controller inbox.
	if replyTopic != topic {
		return nil, errors.New("reply topic does not match provisioned agent")
	}
	if (r.OriginatorId != "" && r.OriginatorId != r.FromId) || (r.DestinationId != "" && r.DestinationId != r.ToId) {
		return nil, errors.New("forwarded USP records are not supported by this profile")
	}
	if r.Version != ProtocolVersion {
		return nil, errors.New("only USP 1.5 is enabled")
	}
	if r.PayloadSecurity != record.Record_PLAINTEXT || len(r.MacSignature) > 0 || len(r.SenderCert) > 0 {
		return nil, errors.New("secured record contexts are not supported")
	}
	if connect := r.GetMqttConnect(); connect != nil {
		if connect.Version != record.MQTTConnectRecord_V5 || connect.SubscribedTopic != topic {
			return nil, errors.New("incompatible MQTT connect record")
		}
		return &Inbound{Sender: r.FromId}, nil
	}
	if r.GetDisconnect() != nil {
		return &Inbound{Sender: r.FromId}, nil
	}
	session := r.GetNoSessionContext()
	if session == nil {
		return nil, errors.New("only no-session-context records are supported")
	}
	msg := new(usp.Msg)
	if err := proto.Unmarshal(session.Payload, msg); err != nil {
		return nil, err
	}
	if msg.Header == nil || msg.Header.MsgId == "" || msg.Body == nil {
		return nil, errors.New("missing USP header, message ID or body")
	}
	if msg.Header.MsgType == usp.Header_NOTIFY {
		n := msg.GetBody().GetRequest().GetNotify()
		if n == nil || n.Notification == nil {
			return nil, errors.New("invalid Notify body")
		}
	} else if err := validateResponse(msg); err != nil {
		return nil, err
	}
	return &Inbound{Sender: r.FromId, Message: msg}, nil
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

// Encode wraps a USP message in the current NoSessionContext record profile.
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
