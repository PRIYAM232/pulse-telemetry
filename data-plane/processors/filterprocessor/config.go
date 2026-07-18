package filterprocessor

import (
	"errors"
	"fmt"
	"net/url"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
)

// Config defines the user-facing configuration of the Pulse filter processor.
//
// Phase 2 adds the control-plane sync settings. The rule document itself is
// NOT configured here — it is fetched at runtime from the control plane and
// held behind a sync.RWMutex inside the processor. This struct is only the
// static bootstrap configuration parsed from the collector YAML.
type Config struct {
	// LogSpanDetails enables per-span debug logging (trace ID, span ID, name).
	// Batch-level count logging is always on at debug level. Keep this false
	// in any latency-sensitive deployment: it walks every span in the batch.
	LogSpanDetails bool `mapstructure:"log_span_details"`

	// SyncEndpoint is the full control-plane policy URL for this collector's
	// fleet, e.g. http://localhost:3000/api/v1/policies/<fleetId>.
	// Empty disables syncing entirely: the processor runs as a static
	// pass-through, exactly like Phase 1.
	SyncEndpoint string `mapstructure:"sync_endpoint"`

	// SyncInterval is how often the background goroutine polls SyncEndpoint.
	// confmap parses Go duration strings ("10s", "1m30s").
	SyncInterval time.Duration `mapstructure:"sync_interval"`

	// FleetKey is the fleet's API key, sent as `Authorization: Bearer <key>`.
	// configopaque.String so the collector redacts it from config dumps/logs.
	// Shared by the policy pull and the stats push.
	FleetKey configopaque.String `mapstructure:"fleet_key"`

	// StatsEndpoint is the control-plane telemetry sink for this fleet, e.g.
	// http://localhost:3000/api/v1/telemetry/<fleetId>/stats. Every
	// stats_interval the processor POSTs {"received": N, "dropped": N} span
	// counts accumulated since the last successful report. Empty disables
	// stats reporting.
	StatsEndpoint string `mapstructure:"stats_endpoint"`

	// StatsInterval is how often the background goroutine reports to
	// StatsEndpoint. Also the heartbeat cadence: a report is sent even when
	// both counters are zero so the control plane can tell idle from dead.
	StatsInterval time.Duration `mapstructure:"stats_interval"`
}

var _ component.Config = (*Config)(nil)

// minSyncInterval guards the control plane against misconfigured fleets
// hammering the policy and telemetry endpoints.
const minSyncInterval = time.Second

// Validate is called by the collector's confmap machinery after the YAML is
// unmarshaled into Config. Returning an error here fails collector startup
// with a clear message instead of misbehaving at runtime.
func (cfg *Config) Validate() error {
	if cfg.SyncEndpoint == "" && cfg.StatsEndpoint == "" {
		// Static pass-through mode; intervals and fleet_key are unused.
		return nil
	}

	if cfg.SyncEndpoint != "" {
		if err := validateControlPlaneURL("sync_endpoint", cfg.SyncEndpoint); err != nil {
			return err
		}
		if cfg.SyncInterval < minSyncInterval {
			return fmt.Errorf("sync_interval must be at least %s, got %s", minSyncInterval, cfg.SyncInterval)
		}
	}
	if cfg.StatsEndpoint != "" {
		if err := validateControlPlaneURL("stats_endpoint", cfg.StatsEndpoint); err != nil {
			return err
		}
		if cfg.StatsInterval < minSyncInterval {
			return fmt.Errorf("stats_interval must be at least %s, got %s", minSyncInterval, cfg.StatsInterval)
		}
	}
	if cfg.FleetKey == "" {
		return errors.New("fleet_key is required when sync_endpoint or stats_endpoint is set")
	}
	return nil
}

func validateControlPlaneURL(field, raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%s %q is not a valid URL: %w", field, raw, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%s %q must use http or https, got %q", field, raw, u.Scheme)
	}
	if u.Host == "" {
		return fmt.Errorf("%s %q has no host", field, raw)
	}
	return nil
}

// --- Control-plane wire format -----------------------------------------------
//
// These mirror the Prisma models in control-plane/prisma/schema.prisma and the
// JSON payload served by GET /api/v1/policies/[fleetId]. The enum values are
// the Prisma enum literal names. Keep both sides in sync.

// Action types.
const (
	ActionDrop   = "DROP"
	ActionSample = "SAMPLE"
	ActionRedact = "REDACT"
	// ActionRoute never drops: a match stamps the record's RESOURCE with
	// pulse.routing.destination = TargetDestination so the collector's native
	// routing connector can fork the batch downstream. Resource-level on
	// purpose — the routing connector evaluates at resource granularity to
	// keep batches intact.
	ActionRoute = "ROUTE"
	// ActionThrottle drops only the records that exceed ThrottleRate
	// events/sec, enforced by a per-rule token-bucket map partitioned on the
	// value of the ThrottleGroupBy attribute (see ratelimit.go). Rate
	// limiting, not sampling: traffic under the rate passes untouched.
	ActionThrottle = "THROTTLE"
)

// Target signals.
const (
	SignalTraces  = "TRACES"
	SignalLogs    = "LOGS"
	SignalMetrics = "METRICS"
)

// Condition operators.
const (
	OpEquals      = "EQUALS"
	OpGreaterThan = "GREATER_THAN"
	OpContains    = "CONTAINS"
	OpExists      = "EXISTS"
	// OpRegexMatch matches when the rule's conditionValue, treated as a Go
	// regexp, finds at least one match in the field. Patterns are compiled
	// once at ruleset-publication time (compileRules), never on the consume
	// hot paths. Combined with REDACT it masks only the matched substrings.
	OpRegexMatch = "REGEX_MATCH"
)

// PolicyRule is one dynamic processing rule, as served by the control plane.
// Unknown enum values (e.g. an action type added in a future control-plane
// release) deserialize fine as strings and are skipped at evaluation time —
// old collectors degrade to ignoring rules they don't understand.
type PolicyRule struct {
	ID       string `json:"id"`
	FleetID  string `json:"fleetId"`
	IsActive bool   `json:"isActive"`
	Name     string `json:"name"`

	ActionType string `json:"actionType"`
	// SampleRate is only set for SAMPLE rules (fraction to keep, in [0,1]).
	SampleRate *float64 `json:"sampleRate"`
	// TargetDestination is only set for ROUTE rules: the destination stamped
	// into pulse.routing.destination for the routing connector to key on.
	// Prisma serializes NULL for other actions; JSON null leaves the zero
	// string, so "" reliably means "no destination" (which disables the rule).
	TargetDestination string `json:"targetDestination"`
	// ThrottleRate is only set for THROTTLE rules: the token-bucket rate in
	// events per second. Pointer (like SampleRate) so a THROTTLE rule missing
	// its rate is detectably malformed and skipped, never treated as rate 0.
	ThrottleRate *int `json:"throttleRate"`
	// ThrottleGroupBy is only set for THROTTLE rules: the attribute key whose
	// value partitions the rule's token buckets (e.g. "tenant_id"). JSON null
	// leaves the zero string; "" means one shared bucket — a global limit.
	ThrottleGroupBy string `json:"throttleGroupBy"`

	TargetSignal string `json:"targetSignal"`

	ConditionField string `json:"conditionField"`
	ConditionOp    string `json:"conditionOp"`
	ConditionValue string `json:"conditionValue"`

	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"`
}

// FleetPolicyResponse is the top-level payload of the policy endpoint:
//
//	{ "fleet_id": "<uuid>", "rules": [ ... ] }
type FleetPolicyResponse struct {
	FleetID string       `json:"fleet_id"`
	Rules   []PolicyRule `json:"rules"`
}
