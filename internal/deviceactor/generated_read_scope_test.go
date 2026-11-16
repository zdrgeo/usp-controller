package deviceactor

import (
	"encoding/json"
	"testing"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/pb/usp"
	"google.golang.org/protobuf/proto"
)

func TestRestoredGeneratedReadsCannotExpandTheirCanonicalScope(t *testing.T) {
	for _, kind := range []string{"bootstrap", "verification"} {
		t.Run(kind, func(t *testing.T) {
			a, b, f, _ := managedActor(t)
			id := "reconcile-observe-1"
			if kind == "verification" {
				a, b, f, _ = verificationActor(t)
				id = "reconcile-verify-1"
			} else {
				a.opts.Policy.ObserveGetResponses = true
				a.opts.Policy.ReadBeforeReconcile = true
				if _, err := a.SetDesired(ctx, desired("desired", 0)); err != nil {
					t.Fatal(err)
				}
			}
			s, err := a.load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := a.scheduleReconciliation(ctx, s); err != nil {
				t.Fatal(err)
			}
			original := append([]byte(nil), b.state...)
			saves, calls := b.saves, len(f.calls)
			for _, mode := range []string{"object-scope", "search-scope", "unbounded-depth", "duplicate", "unordered"} {
				t.Run(mode, func(t *testing.T) {
					var restored snapshot
					if err := json.Unmarshal(original, &restored); err != nil {
						t.Fatal(err)
					}
					c := restored.Commands[device.RequestID(id)]
					req := new(usp.Request)
					if err := proto.Unmarshal(c.Payload, req); err != nil {
						t.Fatal(err)
					}
					get := req.GetGet()
					switch mode {
					case "object-scope":
						get.ParamPaths = []string{"Device.Test."}
					case "search-scope":
						get.ParamPaths = []string{"Device.Test.*"}
					case "unbounded-depth":
						get.MaxDepth = 0
					case "duplicate":
						get.ParamPaths = []string{"Device.Test.Value", "Device.Test.Value"}
					case "unordered":
						get.ParamPaths = []string{"Device.Test.Value", "Device.Test.ReadOnly"}
					}
					c.Payload, _ = proto.Marshal(req)
					restored.Commands[device.RequestID(id)] = c
					b.state, _ = json.Marshal(restored)
					before := string(b.state)
					if err := a.Tick(ctx); err == nil || b.saves != saves || string(b.state) != before || len(f.calls) != calls {
						t.Fatal("invalid restored scope crossed the attempt boundary", err)
					}
				})
			}
			b.state = original
			if err := a.Tick(ctx); err != nil || len(f.calls) != calls+1 || f.calls[calls].Message.GetBody().GetRequest().GetGet() == nil {
				t.Fatal("valid restored generated read could not progress", err)
			}
		})
	}
}
