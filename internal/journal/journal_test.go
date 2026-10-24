package journal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPersistenceAndCapacity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	j, err := Open(path, 200)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Append("agent", []byte(`{"header":{"msgId":"1"}}`)); err != nil {
		t.Fatal(err)
	}
	j.Close()
	b, err := os.ReadFile(path)
	if err != nil || len(b) == 0 || b[len(b)-1] != '\n' {
		t.Fatal("missing complete entry", err)
	}
	j, err = Open(path, int64(len(b)))
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	if err := j.Append("agent", []byte(`{}`)); err == nil {
		t.Fatal("journal exceeded capacity")
	}
	after, _ := os.ReadFile(path)
	if string(after) != string(b) {
		t.Fatal("capacity failure changed journal")
	}
}
func TestTornJournalFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	if err := os.WriteFile(path, []byte(`{"partial":`), 0600); err != nil {
		t.Fatal(err)
	}
	if j, err := Open(path, 1000); err == nil {
		j.Close()
		t.Fatal("accepted torn journal")
	}
}
