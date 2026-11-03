package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRestoreRequiresExplicitLiveVersion(t *testing.T) {
	t.Setenv("ETCD_AUTH_TOKEN", "test-token")
	snapshot := `{"Schema":3,"Identity":{"Tenant":"t","ControllerID":"c","AgentID":"d"},"Desired":{},"Reported":{},"Receipts":{},"Commands":{},"Inbox":{}}`
	written := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "test-token" {
			t.Error("missing token")
		}
		if r.URL.Path == "/v3/kv/range" {
			tag := "7"
			if written {
				tag = "8"
			}
			var req map[string]string
			json.NewDecoder(r.Body).Decode(&req)
			json.NewEncoder(w).Encode(map[string]any{"kvs": []any{map[string]string{"key": req["key"], "value": base64.StdEncoding.EncodeToString([]byte(snapshot)), "mod_revision": tag}}})
			return
		}
		var req struct {
			Compare []struct {
				Revision string `json:"mod_revision"`
			}
			Success []struct {
				Put struct{ Value string } `json:"request_put"`
			}
		}
		if e := json.NewDecoder(r.Body).Decode(&req); e != nil || len(req.Compare) != 1 || req.Compare[0].Revision != "7" {
			t.Fatal("wrong live revision")
		}
		data, e := base64.StdEncoding.DecodeString(req.Success[0].Put.Value)
		if e != nil {
			t.Error(e)
		}
		snapshot = string(data)
		written = true
		io.WriteString(w, `{"header":{"revision":"8"},"succeeded":true}`)
	}))
	defer server.Close()
	args := []string{"-action", "restore", "-tenant", "t", "-controller", "c", "-device", "d", "-etcd-prefix", "/test/", "-etcd-url", server.URL}
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
