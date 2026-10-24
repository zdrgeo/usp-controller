// Package journal durably appends notifications before a USP NotifyResp is sent.
package journal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Journal struct {
	mu          sync.Mutex
	f           *os.File
	size, limit int64
	fault       error
}

func Open(path string, limit int64) (*Journal, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	fail := func(err error) (*Journal, error) { f.Close(); return nil, err }
	st, err := f.Stat()
	if err != nil {
		return fail(err)
	}
	// A torn trailing entry must be repaired explicitly, never silently acknowledged.
	if st.Size() > 0 {
		b := []byte{0}
		if _, err := f.ReadAt(b, st.Size()-1); err != nil {
			return fail(err)
		}
		if b[0] != '\n' {
			return fail(fmt.Errorf("journal has an incomplete trailing entry; recover before starting"))
		}
	}
	if _, err := f.Seek(0, 2); err != nil {
		return fail(err)
	}
	return &Journal{f: f, size: st.Size(), limit: limit}, nil
}

func (j *Journal) Append(agent string, message []byte) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.fault != nil {
		return j.fault
	}
	entry := struct {
		Time    time.Time       `json:"time"`
		Agent   string          `json:"agent_id"`
		Message json.RawMessage `json:"message"`
	}{time.Now().UTC(), agent, message}
	b, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if j.size+int64(len(b)) > j.limit {
		j.fault = fmt.Errorf("notification journal full")
		return j.fault
	}
	n, err := j.f.Write(b)
	j.size += int64(n)
	if err != nil {
		j.fault = err
		return err
	}
	j.fault = j.f.Sync()
	return j.fault
}
func (j *Journal) Healthy() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.fault == nil && j.size < j.limit
}
func (j *Journal) Close() error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.fault = fmt.Errorf("journal closed")
	return j.f.Close()
}
