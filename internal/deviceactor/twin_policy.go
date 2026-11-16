package deviceactor

import (
	"encoding/hex"
	"strings"
	"time"

	"example.com/usp-controller/internal/device"
)

// ValidTwinPolicyFingerprint accepts canonical SHA-256 diagnostics. The digest
// is an admission consistency token, not authentication or execution ownership.
func ValidTwinPolicyFingerprint(value string) bool {
	if len(value) != 64 || value != strings.ToLower(value) {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// Version the canonical input. Map ordering is normalized by fingerprint's JSON
// encoder; only managed policy and observation freshness participate, not live
// observations, desired revisions, pause state or host transport/rate budgets.
func (a *Actor) twinPolicyFingerprint() (string, error) {
	if a.opts.Policy == nil {
		return "", nil
	}
	return fingerprint(struct {
		Version   int
		Policy    *device.TwinPolicy
		Freshness time.Duration
	}{1, a.opts.Policy, a.opts.Freshness})
}
