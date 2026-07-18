package filterprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// Virtual condition fields for log records: rules whose condition_field is
// one of these names match against the record's own severity text / body
// instead of an attribute. Namespaced under "log." so they can never shadow
// a real attribute key by accident.
const (
	fieldLogSeverity = "log.severity"
	fieldLogBody     = "log.body"
)

// logsProcessor filters log records against the dynamic ruleset held by the
// shared ruleEngine — the SAME engine instance the traces processor uses, so
// one collector polls the policy endpoint once and sends one heartbeat no
// matter how many signal pipelines reference pulse_filter.
//
// The hot-path rules from the traces processor apply unchanged: no heap
// allocation per batch in the common path, snapshotRules holds the read lock
// only long enough to copy the slice header.
type logsProcessor struct {
	cfg    *Config
	logger *zap.Logger
	next   consumer.Logs
	engine *ruleEngine
}

// Compile-time guarantee that we satisfy the full processor contract:
// component.Component (Start/Shutdown) + consumer.Logs (Capabilities/ConsumeLogs).
var _ processor.Logs = (*logsProcessor)(nil)

func newLogsProcessor(set processor.Settings, cfg *Config, engine *ruleEngine, next consumer.Logs) *logsProcessor {
	return &logsProcessor{
		cfg:    cfg,
		logger: set.Logger,
		next:   next,
		engine: engine,
	}
}

// Start/Shutdown delegate to the refcounted engine: the first signal
// processor to start spawns the control-plane goroutines, the last one to
// shut down tears them down.
func (p *logsProcessor) Start(_ context.Context, _ component.Host) error {
	p.engine.start()
	return nil
}

func (p *logsProcessor) Shutdown(_ context.Context) error {
	p.engine.stop()
	p.logger.Info("pulse_filter logs processor stopped")
	return nil
}

// Capabilities: we drop log records from the batches we forward and rewrite
// attributes/bodies in place (REDACT), so downstream fan-out consumers must
// not assume the data is shared — MutatesData is true.
func (p *logsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// ConsumeLogs evaluates the current ruleset against every log record and
// forwards whatever survives. Fully-dropped batches are consumed here
// (returning nil acknowledges the data) rather than forwarding an empty
// container.
func (p *logsProcessor) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	rules := p.engine.snapshotRules()

	recordsIn := ld.LogRecordCount()

	if len(rules) > 0 {
		p.applyRules(rules, ld)
	}

	recordsOut := ld.LogRecordCount()

	// Feeds both the stats heartbeat and the collector's self-metrics; safe on
	// every batch, concurrently across receivers.
	p.engine.observeLogs(ctx, int64(recordsIn), int64(recordsIn-recordsOut))

	p.logger.Debug("pulse_filter processed log batch",
		zap.Int("records_in", recordsIn),
		zap.Int("records_dropped", recordsIn-recordsOut),
		zap.Int("records_out", recordsOut),
	)

	if recordsOut == 0 {
		return nil
	}
	return p.next.ConsumeLogs(ctx, ld)
}

// applyRules removes every log record matched by an enforced DROP rule and
// scrubs fields matched by REDACT rules in place, then prunes scope/resource
// containers left empty so downstream components never see hollow envelopes.
func (p *logsProcessor) applyRules(rules []compiledRule, ld plog.Logs) {
	ld.ResourceLogs().RemoveIf(func(rl plog.ResourceLogs) bool {
		resAttrs := rl.Resource().Attributes()
		rl.ScopeLogs().RemoveIf(func(sl plog.ScopeLogs) bool {
			sl.LogRecords().RemoveIf(func(lr plog.LogRecord) bool {
				return p.evaluateLogRecord(rules, lr, resAttrs)
			})
			return sl.LogRecords().Len() == 0
		})
		return rl.ScopeLogs().Len() == 0
	})
}

// evaluateLogRecord runs every enforced rule against one log record and
// reports whether the record should be dropped. REDACT and ROUTE rules mutate
// the record (or its resource) as a side effect but never drop it. Rules are
// independent filters: a record survives only if no rule drops it, so neither
// a REDACT scrub nor a ROUTE tag shields the record from a later DROP rule.
func (p *logsProcessor) evaluateLogRecord(rules []compiledRule, lr plog.LogRecord, resAttrs pcommon.Map) bool {
	for i := range rules {
		rule := &rules[i]
		if !ruleAppliesLogs(rule) {
			continue
		}
		if !logConditionMatches(rule, lr, resAttrs) {
			continue
		}

		switch rule.ActionType {
		case ActionDrop:
			p.logger.Debug("pulse_filter dropping log record",
				zap.String("rule", rule.Name),
				zap.String("severity", lr.SeverityText()),
			)
			return true
		case ActionRedact:
			p.redactLogField(rule, lr, resAttrs)
			// Record survives with the field scrubbed; later rules may
			// still drop it.
		case ActionRoute:
			// Stamp the RESOURCE, not the record: the routing connector
			// matches at resource granularity (see attrRoutingDestination).
			// Every record under this resource inherits the destination;
			// later ROUTE matches overwrite it, and later rules may still
			// drop the record.
			resAttrs.PutStr(attrRoutingDestination, rule.TargetDestination)
			p.logger.Debug("pulse_filter tagged log resource for routing",
				zap.String("rule", rule.Name),
				zap.String("destination", rule.TargetDestination),
			)
		case ActionThrottle:
			// One token per record from the group's bucket; the excess over
			// the rule's rate is dropped. This is the Phase 8 headline: a
			// tenant's exploding debug logs clamp to throttleRate/sec while
			// every other group's bucket stays untouched.
			if !rule.limiters.allow(throttleKey(rule, lr.Attributes(), resAttrs)) {
				p.logger.Debug("pulse_filter dropping log record (throttle rate exceeded)",
					zap.String("rule", rule.Name),
					zap.String("severity", lr.SeverityText()),
				)
				return true
			}
			// Under the rate; later rules may still drop the record.
		}
	}
	return false
}

// logConditionMatches resolves the rule's condition field on a log record and
// applies the operator. Resolution order: the virtual record fields
// (log.severity, log.body) first, then the record's own attributes, then the
// enclosing resource's — mirroring the span-then-resource precedence on the
// traces side. A missing field never matches (including EXISTS, which is true
// exactly when the field is present and non-empty).
func logConditionMatches(rule *compiledRule, lr plog.LogRecord, resAttrs pcommon.Map) bool {
	switch rule.ConditionField {
	case fieldLogSeverity:
		// Severity text compared as a raw string to avoid boxing it into a
		// pcommon.Value (a heap allocation) per rule per record. Records that
		// only carry SeverityNumber have empty text and never match; MVP
		// scope, same as the dashboard's severity examples.
		s := lr.SeverityText()
		return s != "" && matchConditionString(rule, s)
	case fieldLogBody:
		body := lr.Body()
		return body.Type() != pcommon.ValueTypeEmpty && matchConditionValue(rule, body)
	}

	val, ok := lr.Attributes().Get(rule.ConditionField)
	if !ok {
		val, ok = resAttrs.Get(rule.ConditionField)
	}
	if !ok {
		return false
	}
	return matchConditionValue(rule, val)
}

// redactLogField scrubs the rule's condition field, wherever the field
// actually lives — virtual record field, record attribute, or resource
// attribute, the same precedence logConditionMatches matched it with.
//
// REGEX_MATCH rules mask partially: only the substrings matched by the
// pre-compiled pattern are replaced, so a credit card or API key inside an
// unstructured body is scrubbed without blowing away the rest of the line.
// Non-string values are masked on their canonical AsString rendering — the
// same rendering the condition matched on — and stored back as a string.
// Every other operator overwrites the field wholesale: a non-string body
// (Map, Slice, ...) is replaced by the string mask, matching the traces-side
// REDACT contract — neither the original value nor its shape survives.
// SeverityNumber is left untouched — it is a closed enum, not PII, and
// downstream routing may depend on it.
func (p *logsProcessor) redactLogField(rule *compiledRule, lr plog.LogRecord, resAttrs pcommon.Map) {
	partial := rule.ConditionOp == OpRegexMatch
	switch rule.ConditionField {
	case fieldLogSeverity:
		if partial {
			lr.SetSeverityText(rule.maskMatches(lr.SeverityText()))
		} else {
			lr.SetSeverityText(redactedPlaceholder)
		}
	case fieldLogBody:
		if partial {
			lr.Body().SetStr(rule.maskMatches(lr.Body().AsString()))
		} else {
			lr.Body().SetStr(redactedPlaceholder)
		}
	default:
		target := lr.Attributes()
		val, ok := target.Get(rule.ConditionField)
		if !ok {
			// logConditionMatches matched, so the attribute must be on the
			// resource. Scrubbing there scrubs it for every record under the
			// resource — the attribute exists once. Mutating the resource
			// while iterating its records is safe: RemoveIf walks the record
			// slice, a different sub-message.
			target = resAttrs
			val, _ = target.Get(rule.ConditionField)
		}
		if partial {
			target.PutStr(rule.ConditionField, rule.maskMatches(val.AsString()))
		} else {
			target.PutStr(rule.ConditionField, redactedPlaceholder)
		}
	}

	p.logger.Debug("pulse_filter redacted log field",
		zap.String("rule", rule.Name),
		zap.String("field", rule.ConditionField),
	)
}
