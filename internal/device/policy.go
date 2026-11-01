package device

import (
	"fmt"
	"regexp"
	"strconv"
)

// TwinPolicy is deployment policy, never caller-provided permission to retry.
// Only reviewed nonsensitive concrete paths should be listed. Reconcile is off
// by default; Writable must additionally be true for each desired-state path.
type TwinPolicy struct {
	SiteID                string                 `json:"site_id"`
	Reconcile             bool                   `json:"reconcile"`
	CommandTimeoutSeconds int                    `json:"command_timeout_seconds"`
	Paths                 map[string]ManagedPath `json:"paths"`
}
type ManagedPath struct {
	Type     string `json:"type"`
	Writable bool   `json:"writable"`
}

var parameterPath = regexp.MustCompile(`^Device\.(?:[A-Za-z0-9_]+\.)*[A-Za-z_][A-Za-z0-9_]*$`)
var siteName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)

func (p TwinPolicy) Validate() error {
	if !siteName.MatchString(p.SiteID) || len(p.Paths) == 0 || len(p.Paths) > 1024 || p.CommandTimeoutSeconds < 1 || p.CommandTimeoutSeconds > 300 {
		return fmt.Errorf("twin requires a site ID, 1–1024 concrete paths and timeout 1–300 seconds")
	}
	for path, rule := range p.Paths {
		if !parameterPath.MatchString(path) {
			return fmt.Errorf("twin path must be a concrete parameter")
		}
		switch rule.Type {
		case "string", "boolean", "int", "unsignedInt":
		default:
			return fmt.Errorf("unsupported managed parameter type")
		}
	}
	return nil
}
func (p TwinPolicy) Allows(path string, v ParameterValue, write bool) bool {
	rule, ok := p.Paths[path]
	if !ok || write && !rule.Writable || v.Type != rule.Type || len(v.Value) > 4096 {
		return false
	}
	switch v.Type {
	case "string":
		return true
	case "boolean":
		return v.Value == "true" || v.Value == "false"
	case "int":
		_, err := strconv.ParseInt(v.Value, 10, 32)
		return err == nil
	case "unsignedInt":
		_, err := strconv.ParseUint(v.Value, 10, 32)
		return err == nil
	}
	return false
}
