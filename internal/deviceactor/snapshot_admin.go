package deviceactor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// SnapshotBackup contains application data only. Storage ETags must never be
// restored from a backup: the live version must advance on every replacement.
type SnapshotBackup struct {
	Format   int             `json:"format"`
	Snapshot json.RawMessage `json:"snapshot"`
}

// ExportSnapshot returns a validated portable backup and the current live ETag.
// Treat all snapshot contents as sensitive device data.
func (b *FencedDaprBackend) ExportSnapshot(ctx context.Context) (SnapshotBackup, string, error) {
	data, err := b.Load(ctx)
	if err != nil {
		return SnapshotBackup{}, "", err
	}
	if _, err = b.validateSnapshot(ctx, data); err != nil {
		b.loaded = false
		return SnapshotBackup{}, "", err
	}
	return SnapshotBackup{Format: 1, Snapshot: data}, b.etag, nil
}

// InitializeSnapshot is an explicit, create-only administrative operation. It is
// never called by the runtime. legacy=true requires an existing legacy snapshot;
// false requires no legacy snapshot and creates a genuinely new device.
// Retire old binaries before migration. Never use initialization to repair a
// deleted authoritative key; recover from a verified inventory/backup offline.
func (b *FencedDaprBackend) InitializeSnapshot(ctx context.Context, legacy bool) error {
	b.loaded = false
	b.etag = ""
	_, _, status, err := b.read(ctx)
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		return ErrConflict
	}
	if status != http.StatusNoContent {
		return fmt.Errorf("snapshot initialization: HTTP %d", status)
	}
	data, err := b.DaprBackend.Load(ctx)
	if err != nil {
		return err
	}
	if legacy && len(data) == 0 {
		return fmt.Errorf("%w: legacy snapshot missing", ErrNotFound)
	}
	if !legacy && len(data) != 0 {
		return fmt.Errorf("%w: legacy snapshot exists; explicit migration required", ErrConflict)
	}
	data, err = b.validateSnapshot(ctx, data)
	if err != nil {
		return err
	}
	return b.commit(ctx, data, "")
}

// RestoreSnapshot conditionally replaces application data without replacing the
// live storage version. An operator must supply a freshly inspected ETag, never
// one from a backup. No automatic retry: a timeout may have committed. Drain
// admission and senders before restore; this cannot revoke external side effects.
// Missing keys fail closed. Raw store restore/deletion remains unsupported.
func (b *FencedDaprBackend) RestoreSnapshot(ctx context.Context, backup SnapshotBackup, expectedETag string) error {
	b.loaded = false
	b.etag = ""
	if expectedETag == "" || backup.Format != 1 || len(backup.Snapshot) == 0 {
		return ErrInvalid
	}
	data, err := b.validateSnapshot(ctx, backup.Snapshot)
	if err != nil {
		return err
	}
	if _, err = b.Load(ctx); err != nil {
		return err
	}
	if b.etag != expectedETag {
		b.loaded = false
		b.etag = ""
		return ErrConflict
	}
	return b.commit(ctx, data, expectedETag)
}

// Reuse the Actor's full snapshot validation and schema upgrade without running
// reminders, command dispatch, or outbox publication.
func (b *FencedDaprBackend) validateSnapshot(ctx context.Context, data []byte) ([]byte, error) {
	opts := DefaultOptions()
	opts.MaxBytes = b.maxBytes
	a, err := New(b.identity, snapshotInput(data), opts)
	if err != nil {
		return nil, err
	}
	s, err := a.load(ctx)
	if err != nil {
		return nil, err
	}
	normalized, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	if len(normalized) > b.maxBytes {
		return nil, ErrCapacity
	}
	return normalized, nil
}

type snapshotInput []byte

func (s snapshotInput) Load(context.Context) ([]byte, error) { return s, nil }
func (snapshotInput) Save(context.Context, []byte) error {
	return fmt.Errorf("read-only snapshot validation")
}
func (snapshotInput) EnsureReminder(context.Context) error {
	return fmt.Errorf("read-only snapshot validation")
}

// ImportSnapshot explicitly creates a missing target from a verified backup.
// It never overwrites existing state and never executes queued work. All source
// writers must be retired before copying a Redis cohort into independent etcd.
func (b *FencedDaprBackend) ImportSnapshot(ctx context.Context, backup SnapshotBackup) error {
	b.loaded = false
	b.etag = ""
	if b.etcd == nil || backup.Format != 1 || len(backup.Snapshot) == 0 {
		return ErrInvalid
	}
	data, err := b.validateSnapshot(ctx, backup.Snapshot)
	if err != nil {
		return err
	}
	return b.commit(ctx, data, "")
}
