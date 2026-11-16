package deviceactor

import "time"

// ObservationCoverage is sampled, value-free health across all managed paths,
// including read-only paths and paths without desired intent. It is not a plan.
type ObservationCoverage struct {
	AsOf                time.Time  `json:"as_of"`
	ManagedPaths        int        `json:"managed_paths"`
	Fresh               int        `json:"fresh"`
	Missing             int        `json:"missing"`
	Invalid             int        `json:"invalid"`
	Future              int        `json:"future"`
	MutationInvalidated int        `json:"mutation_invalidated"`
	TopologyInvalidated int        `json:"topology_invalidated"`
	Stale               int        `json:"stale"`
	FreshUntil          *time.Time `json:"fresh_until,omitempty"`
}

// Valid checks the optional diagnostic at projection/restore boundaries. Older
// hosts omit it; an absent diagnostic must not be interpreted as zero coverage.
func (c ObservationCoverage) Valid() bool {
	if c.AsOf.IsZero() || c.ManagedPaths < 1 || c.ManagedPaths > 1024 {
		return false
	}
	total := 0
	for _, n := range []int{c.Fresh, c.Missing, c.Invalid, c.Future, c.MutationInvalidated, c.TopologyInvalidated, c.Stale} {
		if n < 0 || n > c.ManagedPaths {
			return false
		}
		total += n
	}
	if total != c.ManagedPaths {
		return false
	}
	if c.Fresh == 0 {
		return c.FreshUntil == nil
	}
	return c.FreshUntil != nil && c.FreshUntil.After(c.AsOf)
}

// CurrentAt expires sampled health at its earliest known freshness boundary.
// This does not promise that no notification or policy change occurred meanwhile.
func (c ObservationCoverage) CurrentAt(now time.Time) bool {
	return !c.AsOf.After(now) && (c.FreshUntil == nil || now.Before(*c.FreshUntil))
}

func (a *Actor) observationCoverage(s *snapshot, now time.Time) *ObservationCoverage {
	if a.opts.Policy == nil {
		return nil
	}
	c := &ObservationCoverage{AsOf: now, ManagedPaths: len(a.opts.Policy.Paths)}
	for path := range a.opts.Policy.Paths {
		switch a.observationPathState(s, path, now) {
		case "fresh":
			c.Fresh++
			until := s.Reported[path].FreshUntil
			if c.FreshUntil == nil || until.Before(*c.FreshUntil) {
				c.FreshUntil = &until
			}
		case "missing":
			c.Missing++
		case "invalid":
			c.Invalid++
		case "future":
			c.Future++
		case "mutation_invalidated":
			c.MutationInvalidated++
		case "topology_invalidated":
			c.TopologyInvalidated++
		case "stale":
			c.Stale++
		}
	}
	return c
}
