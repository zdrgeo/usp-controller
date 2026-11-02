package fleet

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
	"example.com/usp-controller/internal/pb/usp"
	"example.com/usp-controller/internal/protocol"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type envelope struct {
	ActorID       string                `json:"actor_id"`
	Identity      device.DeviceIdentity `json:"identity"`
	Epoch         string                `json:"epoch"`
	Sequence      uint64                `json:"sequence"`
	SchemaVersion string                `json:"schema_version"`
	ReceivedAt    time.Time             `json:"received_at"`
	Message       json.RawMessage       `json:"message"`
}

// Ingest treats notifications as invalidation hints, never as ordered status
// snapshots. Epochs are random per event, so sequence comparisons across epochs
// cannot safely resolve ordering or actor-state restoration.
func (p *Projection) Ingest(body []byte) error {
	if len(body) > 2*protocol.MaxRecord {
		return ErrInvalid
	}
	var e envelope
	if json.Unmarshal(body, &e) != nil || e.SchemaVersion != "1" || e.Sequence == 0 || e.ReceivedAt.IsZero() || e.ActorID != deviceactor.ActorID(e.Identity) {
		return ErrInvalid
	}
	epoch, err := hex.DecodeString(e.Epoch)
	if err != nil || len(epoch) != 16 || hex.EncodeToString(epoch) != e.Epoch {
		return ErrInvalid
	}
	msg := new(usp.Msg)
	if protojson.Unmarshal(e.Message, msg) != nil || msg.GetHeader().GetMsgType() != usp.Header_NOTIFY || msg.GetHeader().GetMsgId() == "" || msg.GetBody().GetRequest().GetNotify().GetNotification() == nil {
		return ErrInvalid
	}
	message, err := proto.MarshalOptions{Deterministic: true}.Marshal(msg)
	if err != nil {
		return ErrInvalid
	}
	canonical, _ := json.Marshal(struct {
		Identity   device.DeviceIdentity
		Epoch      string
		Sequence   uint64
		ReceivedAt time.Time
		Message    []byte
	}{e.Identity, e.Epoch, e.Sequence, e.ReceivedAt.UTC(), message})
	sum := sha256.Sum256(canonical)
	fp := hex.EncodeToString(sum[:])
	key := e.Epoch + ":" + strconv.FormatUint(e.Sequence, 10)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed || p.fault != nil {
		return ErrUnavailable
	}
	r, ok := p.rows[e.Identity.AgentID]
	if !ok || r.Identity != e.Identity {
		return ErrInvalid
	}
	for _, prior := range r.Receipts {
		if prior.Key == key {
			if prior.Fingerprint != fp {
				return ErrConflict
			}
			return nil
		}
	}
	r.Receipts = append(append([]eventReceipt(nil), r.Receipts...), eventReceipt{Key: key, Fingerprint: fp})
	if len(r.Receipts) > receiptLimit {
		r.Receipts = r.Receipts[len(r.Receipts)-receiptLimit:]
	}
	r.Pending = true
	if err = p.commit(e.Identity.AgentID, r); err != nil {
		return err
	}
	p.signal()
	return nil
}
