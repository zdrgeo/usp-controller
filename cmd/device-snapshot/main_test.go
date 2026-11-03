package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRestoreRequiresExplicitLiveVersion(t *testing.T) {
	t.Setenv("DAPR_API_TOKEN", "test-token")
	snapshot := `{"Schema":3,"Identity":{"Tenant":"t","ControllerID":"c","AgentID":"d"},"Desired":{},"Reported":{},"Receipts":{},"Commands":{},"Inbox":{}}`
	written := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("dapr-api-token") != "test-token" {
			t.Error("missing token")
		}
		if r.Method == "GET" {
			tag := "7"
			if written {
				tag = "8"
			}
			w.Header().Set("ETag", tag)
			io.WriteString(w, snapshot)
			return
		}
		var ops []struct {
			ETag  string
			Value json.RawMessage
		}
		if e := json.NewDecoder(r.Body).Decode(&ops); e != nil || len(ops) != 1 || ops[0].ETag != "7" {
			t.Error("restore used backup ETag")
		}
		snapshot = string(ops[0].Value)
		written = true
		w.WriteHeader(204)
	}))
	defer server.Close()
	args := []string{"-action", "restore", "-tenant", "t", "-controller", "c", "-device", "d", "-state-store", "state", "-dapr-url", server.URL}
	document := `{"current_etag":"1","backup":{"format":1,"snapshot":` + snapshot + `}}`
	if e := run(args, strings.NewReader(document), io.Discard); e == nil {
		t.Fatal("backup ETag silently authorized restore")
	}
	var output bytes.Buffer
	if e := run(append(args, "-expected-etag", "7"), strings.NewReader(document), &output); e != nil {
		t.Fatal(e)
	}
	if !written || !strings.Contains(output.String(), `"current_etag":"8"`) {
		t.Fatal("missing updated inspection", output.String())
	}
}
