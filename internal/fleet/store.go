package fleet

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const maxRowBytes = 64 << 10

type store interface {
	Load(string) ([]byte, error)
	Save(string, []byte) error
	Close() error
}
type fileStore struct {
	dir  string
	lock *os.File
}

func openStore(dir string) (*fileStore, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, ".lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		return nil, fmt.Errorf("projection directory already owned: %w", err)
	}
	return &fileStore{dir: dir, lock: lock}, nil
}
func (s *fileStore) path(key string) string {
	h := sha256.Sum256([]byte(key))
	return filepath.Join(s.dir, hex.EncodeToString(h[:])+".json")
}
func (s *fileStore) Load(key string) ([]byte, error) {
	f, err := os.Open(s.path(key))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxRowBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxRowBytes {
		return nil, errors.New("projection row exceeds limit")
	}
	return b, nil
}
func (s *fileStore) Save(key string, b []byte) error {
	if len(b) > maxRowBytes {
		return errors.New("projection row exceeds limit")
	}
	f, err := os.CreateTemp(s.dir, ".projection-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if _, err = f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(f.Name(), s.path(key)); err != nil {
		return err
	}
	dir, err := os.Open(s.dir)
	if err != nil {
		return err
	}
	defer dir.Close()
	// A failure here is ambiguous: callers fail closed until restart/reload.
	return dir.Sync()
}
func (s *fileStore) Close() error { return s.lock.Close() }
