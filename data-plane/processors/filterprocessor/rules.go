package filterprocessor

import (
	"regexp"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
)

// Rule-evaluation helpers shared by every signal processor. Each signal owns
// its own field RESOLUTION (where a condition field lives on a span vs. a log
// record vs. a metric); the operator semantics below are identical across
// signals so one rule language behaves the same everywhere.

// redactedPlaceholder is the value written over any field matched by a REDACT
// rule. Kept a plain string (not configurable) so the mask is recognizable
// fleet-wide and never itself leaks structure.
const redactedPlaceholder = "[REDACTED]"

// redactedPatternPlaceholder is the mask written over each REGEX_MATCH hit
// when a REDACT rule masks partially: only the matched substrings are
// replaced, the surrounding text survives. Distinct from redactedPlaceholder
// so a scrubbed value still tells the reader WHICH kind of scrub happened.
const redactedPatternPlaceholder = "[REDACTED_PATTERN]"

// attrRoutingDestination is the RESOURCE attribute a ROUTE rule stamps with
// its TargetDestination. Resource-level, not record-level, because the
// collector's native routing connector evaluates its route table against the
// resource — that is what lets it fork whole resource groups without
// splitting batches. The pipeline contract is: pulse_filter writes this key,
// the routing connector's table reads it, and the two must agree on the name
// (see data-plane/config/otelcol-dev.yaml).
//
// Stamping a resource attribute routes EVERY record under that resource, the
// same blast radius as REDACT on a resource attribute. When several ROUTE
// rules match records under one resource, the last match wins (PutStr
// upserts).
const attrRoutingDestination = "pulse.routing.destination"

// compiledRule is a PolicyRule in its evaluation-ready form: the wire rule
// plus derived state that must never be computed on the consume hot path.
// Instances are created by compileRules at ruleset-publication time and are
// immutable afterwards, so the hot paths share them lock-free exactly like
// the PolicyRule slices they replace.
type compiledRule struct {
	PolicyRule

	// regex is the pre-compiled REGEX_MATCH pattern. nil for every other
	// operator — and for a REGEX_MATCH rule whose pattern failed to compile,
	// which conditionUsable turns into "rule not enforced" (fail open).
	regex *regexp.Regexp

	// limiters is the THROTTLE rule's token-bucket map (see ratelimit.go),
	// created once per sync like the regex. nil for every other action — and
	// for a THROTTLE rule without a positive rate, which ruleApplies* turns
	// into "rule not enforced" (fail open, like rate-less SAMPLE). The
	// limiterGroup is internally synchronized; sharing it lock-free across
	// the consume hot paths is safe even though the rest of compiledRule is
	// immutable-by-convention.
	limiters *limiterGroup
}

// compileRules derives the evaluation-ready ruleset from a freshly synced
// payload. It runs once per successful sync — never per batch — which is what
// keeps regexp.Compile off the ConsumeTraces/ConsumeLogs/ConsumeMetrics hot
// paths. A pattern that fails to compile disables its rule with one warning
// here instead of a per-record error storm at evaluation time.
func compileRules(rules []PolicyRule, logger *zap.Logger) []compiledRule {
	compiled := make([]compiledRule, len(rules))
	for i := range rules {
		compiled[i] = compiledRule{PolicyRule: rules[i]}

		// THROTTLE buckets are allocated here, not on first match, so the
		// hot paths only ever read the pointer. A rule whose rate is missing
		// or non-positive gets no group and is skipped at evaluation time.
		if rules[i].ActionType == ActionThrottle {
			if rules[i].ThrottleRate != nil && *rules[i].ThrottleRate > 0 {
				compiled[i].limiters = newLimiterGroup(*rules[i].ThrottleRate)
			} else {
				logger.Warn("pulse_filter: THROTTLE rule has no positive rate, rule will not be enforced",
					zap.String("rule", rules[i].Name),
				)
			}
		}

		if rules[i].ConditionOp != OpRegexMatch {
			continue
		}
		re, err := regexp.Compile(rules[i].ConditionValue)
		if err != nil {
			logger.Warn("pulse_filter: REGEX_MATCH pattern does not compile, rule will not be enforced",
				zap.String("rule", rules[i].Name),
				zap.String("pattern", rules[i].ConditionValue),
				zap.Error(err),
			)
			continue
		}
		compiled[i].regex = re
	}
	return compiled
}

// throttleKey resolves the group key for one record: the value of the rule's
// ThrottleGroupBy attribute, record-level first, then resource-level — the
// same precedence conditions resolve with. A missing attribute (or a rule
// with no group-by at all) falls back to the shared default bucket rather
// than exempting the record: a producer cannot dodge its tenant's limit by
// simply not sending tenant_id.
func throttleKey(rule *compiledRule, attrs, resAttrs pcommon.Map) string {
	if rule.ThrottleGroupBy == "" {
		return throttleDefaultKey
	}
	if v, ok := attrs.Get(rule.ThrottleGroupBy); ok {
		return v.AsString()
	}
	if v, ok := resAttrs.Get(rule.ThrottleGroupBy); ok {
		return v.AsString()
	}
	return throttleDefaultKey
}

// maskMatches replaces every non-overlapping match of the rule's pre-compiled
// pattern with the pattern mask, keeping the surrounding text intact.
// ReplaceAllLiteralString so a mask can never be reinterpreted as a $-style
// expansion template. Only meaningful for REGEX_MATCH rules whose pattern
// compiled; callers gate on that.
func (r *compiledRule) maskMatches(s string) string {
	return r.regex.ReplaceAllLiteralString(s, redactedPatternPlaceholder)
}

// conditionUsable reports whether the rule's condition can be evaluated at
// all: a REGEX_MATCH rule whose pattern failed to compile has no working
// condition, so it must be skipped — and not counted as enforced.
func conditionUsable(rule *compiledRule) bool {
	return rule.ConditionOp != OpRegexMatch || rule.regex != nil
}

// ruleAppliesTraces reports whether the traces processor enforces the rule at
// all: it must be active, target traces, and use an action we implement
// (DROP, SAMPLE, REDACT, ROUTE, THROTTLE). Any action added by a newer
// control plane is fetched and cached but intentionally skipped. A SAMPLE
// rule without a rate is malformed and likewise skipped — fail open rather
// than guess a rate — and a ROUTE rule without a destination or a THROTTLE
// rule without a positive rate (no limiterGroup compiled) get the same
// treatment.
func ruleAppliesTraces(rule *compiledRule) bool {
	if !rule.IsActive || rule.TargetSignal != SignalTraces || !conditionUsable(rule) {
		return false
	}
	switch rule.ActionType {
	case ActionDrop, ActionRedact:
		return true
	case ActionSample:
		return rule.SampleRate != nil
	case ActionRoute:
		return rule.TargetDestination != ""
	case ActionThrottle:
		return rule.limiters != nil
	default:
		return false
	}
}

// ruleAppliesLogs is the logs-signal counterpart. SAMPLE is deliberately
// excluded: log records carry no TraceID to key deterministic sampling on, so
// until a later phase defines log sampling semantics those rules are stored
// and synced but not enforced.
func ruleAppliesLogs(rule *compiledRule) bool {
	if !rule.IsActive || rule.TargetSignal != SignalLogs || !conditionUsable(rule) {
		return false
	}
	switch rule.ActionType {
	case ActionDrop, ActionRedact:
		return true
	case ActionRoute:
		return rule.TargetDestination != ""
	case ActionThrottle:
		return rule.limiters != nil
	default:
		return false
	}
}

// ruleAppliesMetrics is the metrics-signal counterpart. Phase 6 added DROP;
// Phase 7 added ROUTE (resource-level stamp, no datapoint semantics needed);
// Phase 8 adds THROTTLE, well-defined for metrics because it drops at the
// same whole-Metric granularity DROP already operates at. REDACT (which
// datapoint field?) and SAMPLE (which stable key?) still have no metrics
// semantics — those rules are stored and synced but skipped, mirroring how
// LOGS SAMPLE is handled.
func ruleAppliesMetrics(rule *compiledRule) bool {
	if !rule.IsActive || rule.TargetSignal != SignalMetrics || !conditionUsable(rule) {
		return false
	}
	switch rule.ActionType {
	case ActionDrop:
		return true
	case ActionRoute:
		return rule.TargetDestination != ""
	case ActionThrottle:
		return rule.limiters != nil
	default:
		return false
	}
}

// matchConditionValue applies the rule's operator to an already-resolved
// attribute value. Resolution failures are handled by the callers (a missing
// field never matches, including EXISTS, which is true exactly when the field
// is present).
func matchConditionValue(rule *compiledRule, val pcommon.Value) bool {
	switch rule.ConditionOp {
	case OpExists:
		return true
	case OpEquals:
		// String comparison on the canonical rendering, so a rule value of
		// "404" matches both Str("404") and Int(404) attributes.
		return val.AsString() == rule.ConditionValue
	case OpContains:
		return strings.Contains(val.AsString(), rule.ConditionValue)
	case OpGreaterThan:
		attr, attrOK := attributeAsFloat(val)
		threshold, err := strconv.ParseFloat(rule.ConditionValue, 64)
		return attrOK && err == nil && attr > threshold
	case OpRegexMatch:
		// regex is pre-compiled at sync time (compileRules); nil means the
		// pattern was rejected there and the rule is unenforceable.
		return rule.regex != nil && rule.regex.MatchString(val.AsString())
	default:
		// Operator from a newer control plane; fail closed (no match).
		return false
	}
}

// matchConditionString applies the rule's operator to a raw string field
// (e.g. a log record's severity text) without boxing it into a pcommon.Value,
// which would heap-allocate on the hot path.
func matchConditionString(rule *compiledRule, s string) bool {
	switch rule.ConditionOp {
	case OpExists:
		return true
	case OpEquals:
		return s == rule.ConditionValue
	case OpContains:
		return strings.Contains(s, rule.ConditionValue)
	case OpGreaterThan:
		attr, attrErr := strconv.ParseFloat(s, 64)
		threshold, err := strconv.ParseFloat(rule.ConditionValue, 64)
		return attrErr == nil && err == nil && attr > threshold
	case OpRegexMatch:
		return rule.regex != nil && rule.regex.MatchString(s)
	default:
		return false
	}
}

// attributeAsFloat coerces numeric and numeric-string attribute values for
// GREATER_THAN comparisons.
func attributeAsFloat(val pcommon.Value) (float64, bool) {
	switch val.Type() {
	case pcommon.ValueTypeInt:
		return float64(val.Int()), true
	case pcommon.ValueTypeDouble:
		return val.Double(), true
	case pcommon.ValueTypeStr:
		f, err := strconv.ParseFloat(val.Str(), 64)
		return f, err == nil
	default:
		return 0, false
	}
}
