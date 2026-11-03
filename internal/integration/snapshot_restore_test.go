package integration

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
	"example.com/usp-controller/internal/pb/usp"
)

// These identities are deliberately not registered with Placement or reminders.
// All writers are controlled by the test, making the quiesced restore boundary
// explicit. Persistence still goes through both real sidecars and Redis.
type snapshotMemory struct{ data []byte }

func (m *snapshotMemory) Load(context.Context) ([]byte, error) {
	return append([]byte(nil), m.data...), nil
}
func (m *snapshotMemory) Save(_ context.Context, b []byte) error {
	m.data = append([]byte(nil), b...)
	return nil
}
func (*snapshotMemory) EnsureReminder(context.Context) error { return nil }

type snapshotNoReminder struct{ deviceactor.Backend }

func (snapshotNoReminder) EnsureReminder(context.Context) error { return nil }

func assertSnapshot(t *testing.T, got, want []byte) {
	t.Helper()
	var a, b any
	if json.Unmarshal(got, &a) != nil || json.Unmarshal(want, &b) != nil || !reflect.DeepEqual(a, b) {
		t.Fatalf("snapshot mismatch\ngot: %s\nwant: %s", got, want)
	}
}

func testLiveSnapshotMigrationRestore(t *testing.T, hosts []*liveHost, redis string, id device.DeviceIdentity) {
	ctx := context.Background()
	id.AgentID = "quiesced-migration-restore"
	opts := deviceactor.DefaultOptions()
	memory := new(snapshotMemory)
	newActor := func(b deviceactor.Backend) *deviceactor.Actor {
		a, e := deviceactor.NewWithEvents(id, snapshotNoReminder{b}, opts, &offlineTransport{}, &deviceactor.DaprEventPublisher{})
		if e != nil {
			t.Fatal(e)
		}
		return a
	}
	seed := newActor(memory)
	desired := device.SetDesiredRequest{ID: "migrated-desired", Values: map[string]device.ParameterValue{"Device.Test.Value": {Value: "target", Type: "string"}}}
	receipt, e := seed.SetDesired(ctx, desired)
	if e != nil {
		t.Fatal(e)
	}
	command := device.CommandRequest{ID: "migrated-command", Deadline: time.Now().Add(time.Hour), Request: &usp.Request{ReqType: &usp.Request_Get{Get: &usp.Get{ParamPaths: []string{"Device.Test.Value"}}}}}
	queued, e := seed.EnqueueCommand(ctx, command)
	if e != nil {
		t.Fatal(e)
	}
	notification := device.DeviceNotification{Identity: id, ReceivedAt: time.Now().UTC(), Message: &usp.Msg{Header: &usp.Header{MsgId: "migrated-notification", MsgType: usp.Header_NOTIFY}, Body: &usp.Body{MsgBody: &usp.Body_Request{Request: &usp.Request{ReqType: &usp.Request_Notify{Notify: &usp.Notify{SubscriptionId: "sub", SendResp: true, Notification: &usp.Notify_ValueChange_{ValueChange: &usp.Notify_ValueChange{ParamPath: "Device.Test.Value", ParamValue: "observed"}}}}}}}}}
	notified, e := seed.HandleNotify(ctx, notification)
	if e != nil {
		t.Fatal(e)
	}
	notified.Duplicate = true // replay acknowledges the persisted inbox entry
	original := append([]byte(nil), memory.data...)
	// Seed the pinned Redis actor-state layout offline, then verify through Dapr's
	// legacy reader. No unfenced writer runs concurrently with the new backend.
	oldKey := "usp-live||" + deviceactor.ActorType + "||" + deviceactor.ActorID(id) + "||" + deviceactor.StateKey
	docker(t, "exec", redis, "redis-cli", "HSET", oldKey, "data", string(original), "version", "1")
	fresh := func(index int) *deviceactor.FencedDaprBackend {
		b, e := deviceactor.NewFencedDaprBackend(hosts[index].client.Endpoint, nil, token, id, time.Second, opts.MaxBytes, "state")
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	load := func(b deviceactor.Backend) []byte {
		v, e := b.Load(ctx)
		if e != nil {
			t.Fatal(e)
		}
		return v
	}
	save := func(b deviceactor.Backend, v []byte) {
		if e := b.Save(ctx, v); e != nil {
			t.Fatal(e)
		}
	}
	first, second := fresh(0), fresh(1)
	assertSnapshot(t, load(first.DaprBackend), original)
	assertSnapshot(t, load(first), original)
	assertSnapshot(t, load(second), original)
	save(first, original)
	if e := second.Save(ctx, original); e == nil {
		t.Fatal("both migration initializers committed")
	}
	assertSnapshot(t, load(second), original)
	assertSnapshot(t, load(first.DaprBackend), original)
	migrated := newActor(second)
	if got, e := migrated.SetDesired(ctx, desired); e != nil || got != receipt {
		t.Fatalf("desired receipt: %+v %v", got, e)
	}
	if got, e := migrated.EnqueueCommand(ctx, command); e != nil || !reflect.DeepEqual(got, queued) {
		t.Fatalf("command receipt: %+v %v", got, e)
	}
	if got, e := migrated.HandleNotify(ctx, notification); e != nil || !reflect.DeepEqual(got, notified) {
		t.Fatalf("notification receipt: %+v %v", got, e)
	}
	assertSnapshot(t, load(second), original) // duplicates must not append queue/outbox

	// Find the actual prefixed key instead of assuming standard-state keyPrefix.
	key := docker(t, "exec", redis, "redis-cli", "--raw", "KEYS", "*device-snapshot-cas-v1-"+deviceactor.ActorID(id))
	if key == "" {
		t.Fatal("missing authoritative fenced key")
	}
	backup := key + "-offline-backup"
	if got := docker(t, "exec", redis, "redis-cli", "COPY", key, backup); got != "1" {
		t.Fatalf("backup: %s", got)
	}
	// Restore the serialized Redis value, including its ETag/version field.
	restore := func() {
		if got := docker(t, "exec", redis, "redis-cli", "EVAL", `return redis.call('RESTORE',KEYS[1],0,redis.call('DUMP',KEYS[2]),'REPLACE')`, "2", key, backup); got != "OK" {
			t.Fatalf("restore: %s", got)
		}
	}
	next := desired
	next.ID = "after-backup"
	next.ExpectedRevision = receipt.Revision
	next.Values = map[string]device.ParameterValue{"Device.Test.Value": {Value: "new-target", Type: "string"}}
	if _, e := migrated.SetDesired(ctx, next); e != nil {
		t.Fatal(e)
	}
	advanced := load(second)
	assertSnapshot(t, load(first.DaprBackend), original) // legacy copy is now stale
	assertSnapshot(t, load(fresh(0)), advanced)          // fenced state always wins

	t.Run("quiesced restore preserves snapshot and receipts", func(t *testing.T) {
		restore() // discard all existing backend instances at this boundary
		restored := fresh(0)
		assertSnapshot(t, load(restored), original)
		a := newActor(restored)
		if got, e := a.SetDesired(ctx, desired); e != nil || got != receipt {
			t.Fatalf("restored receipt: %+v %v", got, e)
		}
		if got, e := a.EnqueueCommand(ctx, command); e != nil || !reflect.DeepEqual(got, queued) {
			t.Fatalf("restored queue: %+v %v", got, e)
		}
		if got, e := a.HandleNotify(ctx, notification); e != nil || !reflect.DeepEqual(got, notified) {
			t.Fatalf("restored inbox: %+v %v", got, e)
		}
		assertSnapshot(t, load(restored), original)
		if _, e := a.SetDesired(ctx, next); e != nil {
			t.Fatal(e)
		}
		assertSnapshot(t, load(fresh(1)), advanced)
	})
	t.Run("restore does not fence surviving pre-backup writers", func(t *testing.T) {
		restore()
		survivor := fresh(0)
		assertSnapshot(t, load(survivor), original)
		writer := fresh(1)
		load(writer)
		save(writer, advanced)
		if e := survivor.Save(ctx, original); !errors.Is(e, deviceactor.ErrConflict) {
			t.Fatalf("normal stale writer not fenced: %v", e)
		}
		restore()
		load(survivor) // capture the backup version again
		load(writer)
		save(writer, advanced)
		restore()                // unsupported online rollback reintroduces that version (ABA)
		save(survivor, advanced) // characterization: no external restore epoch exists
		assertSnapshot(t, load(fresh(1)), advanced)
	})
	t.Run("invalid authoritative snapshot never falls back", func(t *testing.T) {
		docker(t, "exec", redis, "redis-cli", "HSET", key, "data", "null")
		if _, e := fresh(0).Load(ctx); e == nil {
			t.Fatal("invalid fenced snapshot silently used legacy state")
		}
		restore()
	})
	t.Run("missing fenced key reimports stale legacy state", func(t *testing.T) {
		docker(t, "exec", redis, "redis-cli", "DEL", key)
		assertSnapshot(t, load(fresh(0)), original)
		// Recover the authoritative backup before leaving this isolated fixture.
		restore()
		assertSnapshot(t, load(fresh(1)), original)
	})
}
