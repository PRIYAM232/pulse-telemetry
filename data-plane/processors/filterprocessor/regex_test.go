package filterprocessor

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

// Patterns used across the tests: a 16-digit card number with optional
// separators, and a prefixed API key.
const (
	cardPattern   = `\b(?:\d[ -]?){15}\d\b`
	apiKeyPattern = `sk_live_[A-Za-z0-9]+`
)

func regexRedactRule(name, field, pattern string) PolicyRule {
	return PolicyRule{
		IsActive:       true,
		Name:           name,
		ActionType:     ActionRedact,
		TargetSignal:   SignalLogs,
		ConditionField: field,
		ConditionOp:    OpRegexMatch,
		ConditionValue: pattern,
	}
}

// compileRules is the publication-time step that keeps regexp.Compile off the
// hot path: valid patterns come out pre-compiled, invalid ones come out
// disabled (nil regex, not enforceable) instead of erroring per record.
func TestCompileRulesPrecompilesAndDisablesInvalid(t *testing.T) {
	rules := compileRules([]PolicyRule{
		regexRedactRule("valid", fieldLogBody, cardPattern),
		regexRedactRule("invalid", fieldLogBody, `[unclosed`),
		dropLogsRule("non-regex", fieldLogBody, OpContains, "["), // literal "[", not a pattern
	}, zap.NewNop())

	if rules[0].regex == nil {
		t.Errorf("valid pattern was not pre-compiled")
	}
	if !ruleAppliesLogs(&rules[0]) {
		t.Errorf("valid REGEX_MATCH rule must be enforced")
	}
	if rules[1].regex != nil {
		t.Errorf("invalid pattern produced a compiled regex")
	}
	if ruleAppliesLogs(&rules[1]) {
		t.Errorf("REGEX_MATCH rule with a broken pattern must not be enforced")
	}
	if rules[2].regex != nil {
		t.Errorf("non-regex operators must not compile their condition value")
	}
	if !ruleAppliesLogs(&rules[2]) {
		t.Errorf("CONTAINS rule with regex-looking value must still be enforced")
	}
}

// The headline REGEX_MATCH + REDACT contract: only the matched substrings are
// masked; the rest of the log body survives verbatim. Multiple hits in one
// body are all masked.
func TestRegexRedactMasksOnlyMatchesInLogBody(t *testing.T) {
	p := newTestLogsProcessor(t, []PolicyRule{
		regexRedactRule("mask-cards", fieldLogBody, cardPattern),
	})

	ld := plog.NewLogs()
	slr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	addLogRecord(slr, "INFO",
		"charge declined for 4111 1111 1111 1111, retried with 5500-0000-0000-0004 and succeeded")
	addLogRecord(slr, "INFO", "no card here, body must be untouched")

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}

	if ld.LogRecordCount() != 2 {
		t.Fatalf("LogRecordCount = %d, want 2 — REDACT must never drop", ld.LogRecordCount())
	}
	records := ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords()
	want := "charge declined for " + redactedPatternPlaceholder +
		", retried with " + redactedPatternPlaceholder + " and succeeded"
	if got := records.At(0).Body().Str(); got != want {
		t.Errorf("masked body = %q, want %q", got, want)
	}
	if got := records.At(1).Body().Str(); got != "no card here, body must be untouched" {
		t.Errorf("non-matching body was modified: %q", got)
	}
	if d := p.engine.logsDropped.Load(); d != 0 {
		t.Errorf("logsDropped = %d, want 0", d)
	}
}

// Partial masking on a string span attribute: the API key inside the header
// value is masked, the header shape and every other attribute survive.
func TestRegexRedactMasksSpanAttribute(t *testing.T) {
	p := newTestProcessor(t, []PolicyRule{{
		IsActive:       true,
		Name:           "mask-api-keys",
		ActionType:     ActionRedact,
		TargetSignal:   SignalTraces,
		ConditionField: "http.request.header.authorization",
		ConditionOp:    OpRegexMatch,
		ConditionValue: apiKeyPattern,
	}})

	td := ptrace.NewTraces()
	span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID(traceIDFromByte(21))
	span.SetName("charge")
	span.Attributes().PutStr("http.request.header.authorization", "Bearer sk_live_8f2m3k9d7a1")
	span.Attributes().PutStr("http.route", "/charge")

	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}

	got := firstSpan(t, td)
	if val, ok := got.Attributes().Get("http.request.header.authorization"); !ok ||
		val.Str() != "Bearer "+redactedPatternPlaceholder {
		t.Errorf("authorization = %q, want %q", val.AsString(), "Bearer "+redactedPatternPlaceholder)
	}
	if val, ok := got.Attributes().Get("http.route"); !ok || val.Str() != "/charge" {
		t.Errorf("http.route = %q, want untouched \"/charge\"", val.AsString())
	}
}

// A REGEX_MATCH rule matching via the enclosing resource must mask the
// resource attribute where it lives, preserving the unmatched text.
func TestRegexRedactMasksResourceAttributeInPlace(t *testing.T) {
	p := newTestLogsProcessor(t, []PolicyRule{{
		IsActive:       true,
		Name:           "mask-key-in-resource",
		ActionType:     ActionRedact,
		TargetSignal:   SignalLogs,
		ConditionField: "process.command_line",
		ConditionOp:    OpRegexMatch,
		ConditionValue: apiKeyPattern,
	}})

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("process.command_line", "worker --key=sk_live_abc123 --verbose")
	addLogRecord(rl.ScopeLogs().AppendEmpty().LogRecords(), "INFO", "started")

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}

	resAttrs := ld.ResourceLogs().At(0).Resource().Attributes()
	want := "worker --key=" + redactedPatternPlaceholder + " --verbose"
	if val, ok := resAttrs.Get("process.command_line"); !ok || val.Str() != want {
		t.Errorf("process.command_line = %q, want %q", val.AsString(), want)
	}
	if _, ok := firstLogRecord(t, ld).Attributes().Get("process.command_line"); ok {
		t.Errorf("command line leaked onto the record; redaction must stay on the resource")
	}
}

// REGEX_MATCH is a general condition operator, not a REDACT-only feature:
// combined with DROP it drops the matching records outright.
func TestRegexMatchDropsLogRecords(t *testing.T) {
	p := newTestLogsProcessor(t, []PolicyRule{
		dropLogsRule("drop-bot-traffic", fieldLogBody, OpRegexMatch, `GET /(healthz|readyz|livez)\b`),
	})

	ld := plog.NewLogs()
	slr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	addLogRecord(slr, "INFO", "200 GET /healthz 0.4ms")
	addLogRecord(slr, "INFO", "200 GET /readyz 0.3ms")
	addLogRecord(slr, "INFO", "200 GET /api/orders 11ms")

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}

	got := firstLogRecord(t, ld)
	if body := got.Body().Str(); body != "200 GET /api/orders 11ms" {
		t.Errorf("surviving body = %q, want the orders line", body)
	}
	if d := p.engine.logsDropped.Load(); d != 2 {
		t.Errorf("logsDropped = %d, want 2", d)
	}
}

// A broken pattern must disable its rule entirely — nothing dropped, nothing
// masked — rather than take down the pipeline or spam per-record errors.
func TestRegexInvalidPatternFailsOpen(t *testing.T) {
	p := newTestLogsProcessor(t, []PolicyRule{
		dropLogsRule("broken-drop", fieldLogBody, OpRegexMatch, `(unclosed`),
		regexRedactRule("broken-redact", fieldLogBody, `[z-a]`),
	})

	ld := plog.NewLogs()
	slr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	addLogRecord(slr, "INFO", "unclosed business as usual")

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}

	got := firstLogRecord(t, ld)
	if body := got.Body().Str(); body != "unclosed business as usual" {
		t.Errorf("body = %q, want untouched original", body)
	}
}
