package controller

import (
	"context"
	"example.com/usp-controller/internal/pb/record"
	"google.golang.org/protobuf/proto"
	"testing"
)

func TestProtocol15Records(t *testing.T) {
	c, _, _ := fixture()
	b, err := Encode("agent", "controller", reply("version"))
	if err != nil {
		t.Fatal(err)
	}
	var base record.Record
	if err := proto.Unmarshal(b, &base); err != nil {
		t.Fatal(err)
	}
	if base.Version != "1.5" {
		t.Fatalf("emitted version %q", base.Version)
	}
	for _, tc := range []struct {
		name, version, originator, destination string
		reject                                 bool
	}{
		{"direct", "1.5", "", "", false},
		{"explicit direct", "1.5", "agent", "controller", false},
		{"old version", "1.4", "", "", true},
		{"forwarded originator", "1.5", "other", "", true},
		{"forwarded destination", "1.5", "", "other", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := proto.Clone(&base).(*record.Record)
			r.Version, r.OriginatorId, r.DestinationId = tc.version, tc.originator, tc.destination
			payload, err := proto.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			err = c.Receive(context.Background(), payload, "agent/topic", "agent")
			if (err != nil) != tc.reject {
				t.Fatalf("reject=%v, got %v", tc.reject, err)
			}
		})
	}
}
