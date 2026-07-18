package filterprocessor

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type nopLogsConsumer struct{}

func (nopLogsConsumer) Capabilities() consumer.Capabilities          { return consumer.Capabilities{} }
func (nopLogsConsumer) ConsumeLogs(context.Context, plog.Logs) error { return nil }

// captureLogsConsumer records every batch forwarded to it, so tests can
// assert that fully-dropped batches are consumed rather than passed on.
type captureLogsConsumer struct {
	batches []plog.Logs
}

func (c *captureLogsConsumer) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }
func (c *captureLogsConsumer) ConsumeLogs(_ context.Context, ld plog.Logs) error {
	c.batches = append(c.batches, ld)
	return nil
}

func newTestLogsProcessor(t *testing.T, rules []PolicyRule) *logsProcessor {
	t.Helper()
	set := processor.Settings{
		TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()},
	}
	engine := newRuleEngine(zap.NewNop(), &Config{})
	engine.rules = compileRules(rules, zap.NewNop())
	return newLogsProcessor(set, &Config{}, engine, nopLogsConsumer{})
}

// addLogRecord appends one record with a severity, a string body, and a
// marker attribute the test rules match on.
func addLogRecord(slr plog.LogRecordSlice, severity, body string) plog.LogRecord {
	lr := slr.AppendEmpty()
	lr.SetSeverityText(severity)
	lr.Body().SetStr(body)
	lr.Attributes().PutStr("test.marker", "yes")
	return lr
}

func dropLogsRule(name, field, op, value string) PolicyRule {
	return PolicyRule{
		IsActive:       true,
		Name:           name,
		ActionType:     ActionDrop,
		TargetSignal:   SignalLogs,
		ConditionField: field,
		ConditionOp:    op,
		ConditionValue: value,
	}
}

// firstLogRecord digs the only record back out of the batch; plog handles can
// be invalidated by RemoveIf, so tests must re-read after ConsumeLogs.
func firstLogRecord(t *testing.T, ld plog.Logs) plog.LogRecord {
	t.Helper()
	if ld.LogRecordCount() != 1 {
		t.Fatalf("LogRecordCount = %d, want 1", ld.LogRecordCount())
	}
	return ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
}

// The Phase 5 headline: DROP by severity keeps INFO flowing and counts the
// dropped DEBUG records, per signal, on the shared engine.
func TestConsumeLogsDropsBySeverity(t *testing.T) {
	p := newTestLogsProcessor(t, []PolicyRule{
		dropLogsRule("drop-debug", fieldLogSeverity, OpEquals, "DEBUG"),
	})

	ld := plog.NewLogs()
	slr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	addLogRecord(slr, "DEBUG", "cache miss for key user:42")
	addLogRecord(slr, "DEBUG", "retrying connection")
	addLogRecord(slr, "INFO", "user logged in")

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}

	got := firstLogRecord(t, ld)
	if got.SeverityText() != "INFO" {
		t.Errorf("surviving record severity = %q, want INFO", got.SeverityText())
	}
	if r := p.engine.logsReceived.Load(); r != 3 {
		t.Errorf("logsReceived = %d, want 3", r)
	}
	if d := p.engine.logsDropped.Load(); d != 2 {
		t.Errorf("logsDropped = %d, want 2", d)
	}
	// Log traffic must never bleed into the trace counters.
	if r := p.engine.tracesReceived.Load(); r != 0 {
		t.Errorf("tracesReceived = %d, want 0", r)
	}
}

// Body matching goes through the same pcommon.Value operators as attributes:
// CONTAINS on a string body, and empty bodies never match anything.
func TestConsumeLogsDropsByBodyContains(t *testing.T) {
	p := newTestLogsProcessor(t, []PolicyRule{
		dropLogsRule("drop-healthchecks", fieldLogBody, OpContains, "GET /healthz"),
	})

	ld := plog.NewLogs()
	slr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	addLogRecord(slr, "INFO", `200 GET /healthz 0.8ms`)
	addLogRecord(slr, "INFO", `200 GET /api/orders 12ms`)
	// EXISTS on log.body must not match a record with no body at all.
	empty := slr.AppendEmpty()
	empty.SetSeverityText("INFO")

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}

	if ld.LogRecordCount() != 2 {
		t.Fatalf("LogRecordCount = %d, want 2 (healthcheck dropped)", ld.LogRecordCount())
	}
}

// A fully-matched batch must be consumed (nil error, nothing forwarded) and
// the emptied scope/resource containers pruned before the count check.
func TestConsumeLogsPrunesEmptyContainers(t *testing.T) {
	sink := &captureLogsConsumer{}
	set := processor.Settings{
		TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()},
	}
	engine := newRuleEngine(zap.NewNop(), &Config{})
	engine.rules = compileRules([]PolicyRule{
		dropLogsRule("drop-marked", "test.marker", OpExists, ""),
	}, zap.NewNop())
	p := newLogsProcessor(set, &Config{}, engine, sink)

	ld := plog.NewLogs()
	for i := 0; i < 2; i++ {
		slr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
		addLogRecord(slr, "DEBUG", "noise")
	}

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}
	if ld.ResourceLogs().Len() != 0 {
		t.Errorf("ResourceLogs().Len() = %d, want 0 — empty containers must be pruned", ld.ResourceLogs().Len())
	}
	if len(sink.batches) != 0 {
		t.Errorf("fully-dropped batch was forwarded downstream")
	}
}

// REDACT on logs: attribute masked in place, body replaceable wholesale, and
// the record itself always survives with nothing counted as dropped.
func TestRedactLogAttributeAndBody(t *testing.T) {
	redact := func(field string) PolicyRule {
		return PolicyRule{
			IsActive:       true,
			Name:           "redact-" + field,
			ActionType:     ActionRedact,
			TargetSignal:   SignalLogs,
			ConditionField: field,
			ConditionOp:    OpExists,
		}
	}
	p := newTestLogsProcessor(t, []PolicyRule{
		redact("user.email"),
		redact(fieldLogBody),
	})

	ld := plog.NewLogs()
	slr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	lr := addLogRecord(slr, "INFO", "password reset for alice@example.com")
	lr.Attributes().PutStr("user.email", "alice@example.com")
	lr.Attributes().PutStr("http.route", "/reset")

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}

	got := firstLogRecord(t, ld)
	if val, ok := got.Attributes().Get("user.email"); !ok || val.Str() != redactedPlaceholder {
		t.Errorf("user.email = %v, want %q", val.AsString(), redactedPlaceholder)
	}
	if val, ok := got.Attributes().Get("http.route"); !ok || val.Str() != "/reset" {
		t.Errorf("http.route = %v, want untouched \"/reset\"", val.AsString())
	}
	if got.Body().Type() != pcommon.ValueTypeStr || got.Body().Str() != redactedPlaceholder {
		t.Errorf("body = (%s) %v, want (Str) %q", got.Body().Type(), got.Body().AsString(), redactedPlaceholder)
	}
	if d := p.engine.logsDropped.Load(); d != 0 {
		t.Errorf("logsDropped = %d, want 0 — REDACT must never count as a drop", d)
	}
}

// A rule matching via the enclosing resource must scrub the resource
// attribute where it lives, not conjure a redacted copy onto the record.
func TestRedactLogResourceAttributeInPlace(t *testing.T) {
	p := newTestLogsProcessor(t, []PolicyRule{{
		IsActive:       true,
		Name:           "redact-host",
		ActionType:     ActionRedact,
		TargetSignal:   SignalLogs,
		ConditionField: "host.name",
		ConditionOp:    OpExists,
	}})

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("host.name", "ip-10-1-2-3.internal")
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetSeverityText("INFO")
	lr.Body().SetStr("started")

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}

	resAttrs := ld.ResourceLogs().At(0).Resource().Attributes()
	if val, ok := resAttrs.Get("host.name"); !ok || val.Str() != redactedPlaceholder {
		t.Errorf("resource host.name = %v, want %q", val.AsString(), redactedPlaceholder)
	}
	if _, ok := firstLogRecord(t, ld).Attributes().Get("host.name"); ok {
		t.Errorf("host.name leaked onto the record; redaction must stay on the resource")
	}
}

// Signal isolation: TRACES-targeted rules and LOGS SAMPLE rules (unsupported
// this phase) must both leave log records untouched.
func TestLogsIgnoreForeignAndSampleRules(t *testing.T) {
	rate := 0.0 // would drop everything if (incorrectly) enforced
	p := newTestLogsProcessor(t, []PolicyRule{
		{
			IsActive:       true,
			Name:           "traces-only-drop",
			ActionType:     ActionDrop,
			TargetSignal:   SignalTraces,
			ConditionField: "test.marker",
			ConditionOp:    OpExists,
		},
		{
			IsActive:       true,
			Name:           "logs-sample-unsupported",
			ActionType:     ActionSample,
			SampleRate:     &rate,
			TargetSignal:   SignalLogs,
			ConditionField: "test.marker",
			ConditionOp:    OpExists,
		},
	})

	ld := plog.NewLogs()
	slr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	addLogRecord(slr, "INFO", "survives both rules")

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}
	if ld.LogRecordCount() != 1 {
		t.Errorf("LogRecordCount = %d, want 1 — foreign-signal/SAMPLE rules must not touch logs", ld.LogRecordCount())
	}
}

// The reverse direction: LOGS rules must never drop spans.
func TestTracesIgnoreLogsRules(t *testing.T) {
	p := newTestProcessor(t, []PolicyRule{
		dropLogsRule("logs-only-drop", "test.marker", OpExists, ""),
	})

	td := ptrace.NewTraces()
	addSpan(td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans(), traceIDFromByte(9), "kept")
	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}
	if td.SpanCount() != 1 {
		t.Errorf("SpanCount = %d, want 1 — LOGS rules must not touch traces", td.SpanCount())
	}
}

// TestFactorySharesEngineAcrossSignals pins the no-double-polling contract:
// the traces, logs, and metrics processors created under one component ID
// share one ruleEngine, and the engine only stops with the LAST processor.
func TestFactorySharesEngineAcrossSignals(t *testing.T) {
	shared := &sharedEngines{engines: make(map[component.ID]*ruleEngine)}
	set := processor.Settings{
		ID:                component.NewID(Type),
		TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()},
	}
	cfg := createDefaultConfig()

	tp, err := shared.createTraces(context.Background(), set, cfg, nopConsumer{})
	if err != nil {
		t.Fatalf("createTraces: %v", err)
	}
	lp, err := shared.createLogs(context.Background(), set, cfg, nopLogsConsumer{})
	if err != nil {
		t.Fatalf("createLogs: %v", err)
	}
	mp, err := shared.createMetrics(context.Background(), set, cfg, nopMetricsConsumer{})
	if err != nil {
		t.Fatalf("createMetrics: %v", err)
	}

	tEngine := tp.(*tracesProcessor).engine
	lEngine := lp.(*logsProcessor).engine
	mEngine := mp.(*metricsProcessor).engine
	if tEngine != lEngine || tEngine != mEngine {
		t.Fatalf("signal processors got different engines; policy endpoint would be polled more than once")
	}

	otherSet := set
	otherSet.ID = component.NewIDWithName(Type, "edge")
	lp2, err := shared.createLogs(context.Background(), otherSet, cfg, nopLogsConsumer{})
	if err != nil {
		t.Fatalf("createLogs(other id): %v", err)
	}
	if lp2.(*logsProcessor).engine == tEngine {
		t.Fatalf("distinct component IDs must get distinct engines")
	}

	// Refcounted lifecycle: engine stays up until the last Shutdown.
	ctx := context.Background()
	if err := tp.Start(ctx, nil); err != nil {
		t.Fatalf("traces Start: %v", err)
	}
	if err := lp.Start(ctx, nil); err != nil {
		t.Fatalf("logs Start: %v", err)
	}
	if err := mp.Start(ctx, nil); err != nil {
		t.Fatalf("metrics Start: %v", err)
	}
	if tEngine.refs != 3 {
		t.Fatalf("engine refs = %d after three starts, want 3", tEngine.refs)
	}
	if err := tp.Shutdown(ctx); err != nil {
		t.Fatalf("traces Shutdown: %v", err)
	}
	if err := lp.Shutdown(ctx); err != nil {
		t.Fatalf("logs Shutdown: %v", err)
	}
	if tEngine.refs != 1 {
		t.Fatalf("engine refs = %d after two shutdowns, want 1", tEngine.refs)
	}
	if err := mp.Shutdown(ctx); err != nil {
		t.Fatalf("metrics Shutdown: %v", err)
	}
	if tEngine.refs != 0 {
		t.Fatalf("engine refs = %d after all shutdowns, want 0", tEngine.refs)
	}
}
