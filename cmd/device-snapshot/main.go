// device-snapshot performs explicit fenced-state provisioning, backup and restore.
// It never starts a Controller, actor host, MQTT client or command dispatcher.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"example.com/usp-controller/internal/device"
	"example.com/usp-controller/internal/deviceactor"
)

func run(args []string, input io.Reader, output io.Writer) error {
	flags := flag.NewFlagSet("device-snapshot", flag.ContinueOnError)
	action := flags.String("action", "", "inspect, initialize, migrate-legacy, or restore")
	endpoint := flags.String("dapr-url", "http://127.0.0.1:3500", "local Dapr endpoint")
	store := flags.String("state-store", "", "configured replica state store")
	tenant := flags.String("tenant", "", "device tenant")
	controller := flags.String("controller", "", "controller endpoint ID")
	agent := flags.String("device", "", "device endpoint ID")
	etag := flags.String("expected-etag", "", "current inspected ETag required for restore")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *tenant == "" || *controller == "" || *agent == "" || *store == "" {
		return fmt.Errorf("tenant, controller, device and state-store are required; positional arguments are not accepted")
	}
	if *action != "inspect" && *action != "initialize" && *action != "migrate-legacy" && *action != "restore" {
		return fmt.Errorf("unknown action")
	}
	if (*action == "restore") != (*etag != "") {
		return fmt.Errorf("expected-etag is required only for restore")
	}
	b, err := deviceactor.NewFencedDaprBackend(*endpoint, nil, os.Getenv("DAPR_API_TOKEN"), device.DeviceIdentity{Tenant: *tenant, ControllerID: *controller, AgentID: *agent}, time.Second, 1<<20, *store)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	switch *action {
	case "initialize", "migrate-legacy":
		if err = b.InitializeSnapshot(ctx, *action == "migrate-legacy"); err != nil {
			return err
		}
	case "restore":
		// inspect writes {backup,current_etag}; restore consumes only backup. The
		// explicit flag must come from a fresh inspection of the live target.
		var document struct {
			Backup      deviceactor.SnapshotBackup `json:"backup"`
			CurrentETag string                     `json:"current_etag"`
		}
		raw, e := io.ReadAll(io.LimitReader(input, (2<<20)+1))
		if e != nil {
			return e
		}
		if len(raw) > 2<<20 {
			return deviceactor.ErrCapacity
		}
		if err = json.Unmarshal(raw, &document); err != nil {
			return err
		}
		if err = b.RestoreSnapshot(ctx, document.Backup, *etag); err != nil {
			return err
		}
	}
	backup, version, err := b.ExportSnapshot(ctx)
	if err != nil {
		return err
	}
	return json.NewEncoder(output).Encode(struct {
		Backup      deviceactor.SnapshotBackup `json:"backup"`
		CurrentETag string                     `json:"current_etag"`
	}{backup, version})
}
func main() {
	if err := run(os.Args[1:], os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
