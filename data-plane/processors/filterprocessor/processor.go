package filterprocessor

import (
	"context"
	"math"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// tracesProcessor filters spans against the dynamic ruleset held by the
// shared ruleEngine (see engine.go for the sync/stats/concurrency story) and
// feeds the engine's per-signal counters.
//
// Hot-path rules:
//   - No heap allocation per batch in the common path.
//   - Anything that walks individual spans is gated behind an explicit
//     opt-in AND a level check, so disabled features cost ~zero.
type tracesProcessor struct {
	cfg    *Config
	logger *zap.Logger
	next   consumer.Traces
	engine *ruleEngine
}

// Compile-time guarantee that we satisfy the full processor contract:
// component.Component (Start/Shutdown) + consumer.Traces (Capabilities/ConsumeTraces).
var _ processor.Traces = (*tracesProcessor)(nil)

func newTracesProcessor(set processor.Settings, cfg *Config, engine *ruleEngine, next consumer.Traces) *tracesProcessor {
	return &tracesProcessor{
		cfg:    cfg,
		logger: set.Logger,
		next:   next,
		engine: engine,
	}
}

// Start/Shutdown delegate to the refcounted engine: the first signal
// processor to start spawns the control-plane goroutines, the last one to
// shut down tears them down.
func (p *tracesProcessor) Start(_ context.Context, _ component.Host) error {
	p.engine.start()
	return nil
}

func (p *tracesProcessor) Shutdown(_ context.Context) error {
	p.engine.stop()
	p.logger.Info("pulse_filter traces processor stopped")
	return nil
}

// Capabilities: we drop spans from the batches we forward and rewrite
// attributes in place (REDACT), so downstream fan-out consumers must not
// assume the data is shared — MutatesData is true.
func (p *tracesProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// ConsumeTraces evaluates the current ruleset against every span and forwards
// whatever survives. Fully-dropped batches are consumed here (returning nil
// acknowledges the data) rather than forwarding an empty container.
func (p *tracesProcessor) ConsumeTraces(ctx context.Context, td ptrace.Traces) error {
	rules := p.engine.snapshotRules()

	spansIn := td.SpanCount()

	if len(rules) > 0 {
		p.applyRules(rules, td)
	}

	spansOut := td.SpanCount()

	// Feeds both the stats heartbeat and the collector's self-metrics; safe on
	// every batch, concurrently across receivers.
	p.engine.observeTraces(ctx, int64(spansIn), int64(spansIn-spansOut))

	p.logger.Debug("pulse_filter processed trace batch",
		zap.Int("spans_in", spansIn),
		zap.Int("spans_dropped", spansIn-spansOut),
		zap.Int("spans_out", spansOut),
	)

	if p.cfg.LogSpanDetails && p.logger.Core().Enabled(zap.DebugLevel) {
		p.logSpanDetails(td)
	}

	if spansOut == 0 {
		return nil
	}
	return p.next.ConsumeTraces(ctx, td)
}

// applyRules removes every span matched by an enforced DROP rule and scrubs
// attributes matched by REDACT rules in place, then prunes scope/resource
// containers left empty so downstream components never see hollow envelopes.
func (p *tracesProcessor) applyRules(rules []compiledRule, td ptrace.Traces) {
	td.ResourceSpans().RemoveIf(func(rs ptrace.ResourceSpans) bool {
		resAttrs := rs.Resource().Attributes()
		rs.ScopeSpans().RemoveIf(func(ss ptrace.ScopeSpans) bool {
			ss.Spans().RemoveIf(func(span ptrace.Span) bool {
				return p.evaluateSpan(rules, span, resAttrs)
			})
			return ss.Spans().Len() == 0
		})
		return rs.ScopeSpans().Len() == 0
	})
}

// evaluateSpan runs every enforced rule against one span and reports whether
// the span should be dropped. REDACT and ROUTE rules mutate the span (or its
// resource) as a side effect but never drop it. Rules are independent
// filters: a span survives only if no rule drops it, so neither a SAMPLE
// keep-verdict nor a REDACT scrub nor a ROUTE tag shields the span from a
// later DROP rule.
func (p *tracesProcessor) evaluateSpan(rules []compiledRule, span ptrace.Span, resAttrs pcommon.Map) bool {
	for i := range rules {
		rule := &rules[i]
		if !ruleAppliesTraces(rule) {
			continue
		}
		if !evaluateCondition(rule, span, resAttrs) {
			continue
		}

		switch rule.ActionType {
		case ActionDrop:
			p.logger.Debug("pulse_filter dropping span",
				zap.String("rule", rule.Name),
				zap.String("span_name", span.Name()),
				zap.String("trace_id", span.TraceID().String()),
			)
			return true
		case ActionSample:
			// Deterministic head sampling keyed on the TraceID: every span of
			// a trace computes the same bucket, so traces are kept or dropped
			// whole — never split into orphaned children — with no coordination
			// between spans, batches, or collector instances.
			if traceIDBucket(span.TraceID()) >= sampleKeepThreshold(*rule.SampleRate) {
				p.logger.Debug("pulse_filter dropping span (trace not sampled)",
					zap.String("rule", rule.Name),
					zap.String("span_name", span.Name()),
					zap.String("trace_id", span.TraceID().String()),
				)
				return true
			}
			// Trace sampled in; later rules may still drop the span.
		case ActionRedact:
			p.redactAttribute(rule, span, resAttrs)
			// Span survives with the attribute scrubbed; later rules may
			// still drop it.
		case ActionRoute:
			// Stamp the RESOURCE, not the span: the routing connector matches
			// at resource granularity (see attrRoutingDestination). Every span
			// under this resource inherits the destination; later ROUTE
			// matches overwrite it, and later rules may still drop the span.
			resAttrs.PutStr(attrRoutingDestination, rule.TargetDestination)
			p.logger.Debug("pulse_filter tagged span resource for routing",
				zap.String("rule", rule.Name),
				zap.String("destination", rule.TargetDestination),
				zap.String("span_name", span.Name()),
				zap.String("trace_id", span.TraceID().String()),
			)
		case ActionThrottle:
			// One token per span from the group's bucket; the excess over the
			// rule's rate is dropped. Unlike SAMPLE this is not deterministic
			// per trace — a clamped burst sheds whichever spans arrive after
			// the bucket empties, so throttled traces can lose spans.
			if !rule.limiters.allow(throttleKey(rule, span.Attributes(), resAttrs)) {
				p.logger.Debug("pulse_filter dropping span (throttle rate exceeded)",
					zap.String("rule", rule.Name),
					zap.String("span_name", span.Name()),
					zap.String("trace_id", span.TraceID().String()),
				)
				return true
			}
			// Under the rate; later rules may still drop the span.
		}
	}
	return false
}

// redactAttribute scrubs the rule's condition field, wherever the attribute
// actually lives — span first, then resource, the same precedence
// evaluateCondition matched it with.
//
// Two masking modes, keyed on the operator:
//   - REGEX_MATCH: partial. Only the substrings matched by the pre-compiled
//     pattern are replaced with the pattern mask; the surrounding text
//     survives (credit card inside a URL, key inside a header). Non-string
//     values are masked on their canonical AsString rendering — the same
//     rendering the condition matched on — and stored back as a string.
//   - Every other operator: wholesale. PutStr upserts, so an existing value
//     of any pcommon type (Int, Double, Map, Slice, ...) is replaced by the
//     string mask: neither the original value nor its shape survives.
//
// Redacting a resource attribute scrubs it for every span under that
// resource — the attribute exists once, so that is what removing the PII
// means. Mutating the resource while iterating its spans is safe: RemoveIf
// walks the span slice, a different sub-message.
func (p *tracesProcessor) redactAttribute(rule *compiledRule, span ptrace.Span, resAttrs pcommon.Map) {
	target := span.Attributes()
	val, ok := target.Get(rule.ConditionField)
	if !ok {
		// evaluateCondition matched, so the attribute must be on the resource.
		target = resAttrs
		val, _ = target.Get(rule.ConditionField)
	}
	if rule.ConditionOp == OpRegexMatch {
		target.PutStr(rule.ConditionField, rule.maskMatches(val.AsString()))
	} else {
		target.PutStr(rule.ConditionField, redactedPlaceholder)
	}

	p.logger.Debug("pulse_filter redacted attribute",
		zap.String("rule", rule.Name),
		zap.String("attribute", rule.ConditionField),
		zap.String("span_name", span.Name()),
		zap.String("trace_id", span.TraceID().String()),
	)
}

// FNV-1a 64-bit constants (FNV offset basis and prime).
const (
	fnvOffset64 = 14695981039346656037
	fnvPrime64  = 1099511628211
)

// traceIDBucket hashes the 16-byte TraceID into a stable bucket in [0, 100).
// FNV-1a is inlined rather than taken from hash/fnv because the stdlib
// implementation allocates its state on the heap — this runs per span on the
// hot path.
func traceIDBucket(id pcommon.TraceID) uint64 {
	h := uint64(fnvOffset64)
	for _, b := range id {
		h ^= uint64(b)
		h *= fnvPrime64
	}
	return h % 100
}

// sampleKeepThreshold converts a keep-fraction in [0, 1] to the number of
// buckets kept: a trace passes iff traceIDBucket < threshold. Out-of-range
// rates from a buggy control plane clamp to "drop all" / "keep all" instead
// of producing nonsense thresholds.
func sampleKeepThreshold(rate float64) uint64 {
	switch {
	case rate <= 0 || math.IsNaN(rate):
		return 0
	case rate >= 1:
		return 100
	default:
		return uint64(math.Round(rate * 100))
	}
}

// evaluateCondition resolves the rule's condition field against the span's
// own attributes first, then the enclosing resource's, and applies the
// operator. A missing attribute never matches (including EXISTS, which is
// true exactly when the attribute is present).
func evaluateCondition(rule *compiledRule, span ptrace.Span, resAttrs pcommon.Map) bool {
	val, ok := span.Attributes().Get(rule.ConditionField)
	if !ok {
		val, ok = resAttrs.Get(rule.ConditionField)
	}
	if !ok {
		return false
	}
	return matchConditionValue(rule, val)
}

// logSpanDetails walks the batch read-only. Kept out of ConsumeTraces so the
// hot path stays small enough to inline and pays nothing when disabled.
func (p *tracesProcessor) logSpanDetails(td ptrace.Traces) {
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				p.logger.Debug("pulse_filter span",
					zap.String("trace_id", span.TraceID().String()),
					zap.String("span_id", span.SpanID().String()),
					zap.String("name", span.Name()),
					zap.String("kind", span.Kind().String()),
				)
			}
		}
	}
}
