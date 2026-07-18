package filterprocessor

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

type nopMetricsConsumer struct{}

func (nopMetricsConsumer) Capabilities() consumer.Capabilities { return consumer.Capabilities{} }
func (nopMetricsConsumer) ConsumeMetrics(context.Context, pmetric.Metrics) error {
	return nil
}

// captureMetricsConsumer records every batch forwarded to it, so tests can
// assert that fully-dropped batches are consumed rather than passed on.
type captureMetricsConsumer struct {
	batches []pmetric.Metrics
}

func (c *captureMetricsConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{}
}
func (c *captureMetricsConsumer) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	c.batches = append(c.batches, md)
	return nil
}

func newTestMetricsProcessor(t *testing.T, rules []PolicyRule) *metricsProcessor {
	t.Helper()
	set := processor.Settings{
		TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()},
	}
	engine := newRuleEngine(zap.NewNop(), &Config{})
	engine.rules = compileRules(rules, zap.NewNop())
	return newMetricsProcessor(set, &Config{}, engine, nopMetricsConsumer{})
}

// addGauge appends one gauge metric with a single datapoint, the shape
// telemetrygen metrics emits.
func addGauge(ms pmetric.MetricSlice, name string) {
	m := ms.AppendEmpty()
	m.SetName(name)
	m.SetEmptyGauge().DataPoints().AppendEmpty().SetIntValue(42)
}

func dropMetricsRule(name, field, op, value string) PolicyRule {
	return PolicyRule{
		IsActive:       true,
		Name:           name,
		ActionType:     ActionDrop,
		TargetSignal:   SignalMetrics,
		ConditionField: field,
		ConditionOp:    op,
		ConditionValue: value,
	}
}

// survivingMetricNames flattens whatever survived ConsumeMetrics; pmetric
// handles can be invalidated by RemoveIf, so tests must re-read afterwards.
func survivingMetricNames(md pmetric.Metrics) []string {
	var names []string
	rms := md.ResourceMetrics()
	for i := 0; i < rms.Len(); i++ {
		sms := rms.At(i).ScopeMetrics()
		for j := 0; j < sms.Len(); j++ {
			ms := sms.At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				names = append(names, ms.At(k).Name())
			}
		}
	}
	return names
}

// The Phase 6 headline: DROP by metric name removes exactly the matched
// metric and counts it, per signal, on the shared engine.
func TestConsumeMetricsDropsByNameEquals(t *testing.T) {
	p := newTestMetricsProcessor(t, []PolicyRule{
		dropMetricsRule("drop-http-duration", fieldMetricName, OpEquals, "http.server.duration"),
	})

	md := pmetric.NewMetrics()
	ms := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	addGauge(ms, "http.server.duration")
	addGauge(ms, "http.server.active_requests")

	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	got := survivingMetricNames(md)
	if len(got) != 1 || got[0] != "http.server.active_requests" {
		t.Errorf("surviving metrics = %v, want [http.server.active_requests]", got)
	}
	if r := p.engine.metricsReceived.Load(); r != 2 {
		t.Errorf("metricsReceived = %d, want 2", r)
	}
	if d := p.engine.metricsDropped.Load(); d != 1 {
		t.Errorf("metricsDropped = %d, want 1", d)
	}
	// Metric traffic must never bleed into the other signals' counters.
	if r := p.engine.tracesReceived.Load(); r != 0 {
		t.Errorf("tracesReceived = %d, want 0", r)
	}
	if r := p.engine.logsReceived.Load(); r != 0 {
		t.Errorf("logsReceived = %d, want 0", r)
	}
}

func TestConsumeMetricsDropsByNameContains(t *testing.T) {
	p := newTestMetricsProcessor(t, []PolicyRule{
		dropMetricsRule("drop-runtime-noise", fieldMetricName, OpContains, "runtime.go"),
	})

	md := pmetric.NewMetrics()
	ms := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	addGauge(ms, "runtime.go.mem.heap_alloc")
	addGauge(ms, "runtime.go.goroutines")
	addGauge(ms, "http.server.duration")

	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	got := survivingMetricNames(md)
	if len(got) != 1 || got[0] != "http.server.duration" {
		t.Errorf("surviving metrics = %v, want [http.server.duration]", got)
	}
}

// REGEX_MATCH as a DROP condition on metric names: the pattern is
// pre-compiled at publication time and anchors work as usual.
func TestConsumeMetricsDropsByNameRegex(t *testing.T) {
	p := newTestMetricsProcessor(t, []PolicyRule{
		dropMetricsRule("drop-system-metrics", fieldMetricName, OpRegexMatch, `^system\.(cpu|memory)\.`),
	})

	md := pmetric.NewMetrics()
	ms := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	addGauge(ms, "system.cpu.utilization")
	addGauge(ms, "system.memory.usage")
	addGauge(ms, "system.disk.io")        // not cpu/memory: survives
	addGauge(ms, "app.system.cpu.shadow") // unanchored lookalike: survives

	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	got := survivingMetricNames(md)
	want := []string{"system.disk.io", "app.system.cpu.shadow"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("surviving metrics = %v, want %v", got, want)
	}
}

// Resource-attribute conditions let fleets drop whole workloads (e.g. a
// staging service) without enumerating metric names.
func TestConsumeMetricsDropsByResourceAttribute(t *testing.T) {
	p := newTestMetricsProcessor(t, []PolicyRule{
		dropMetricsRule("drop-staging", "service.name", OpEquals, "staging-worker"),
	})

	md := pmetric.NewMetrics()
	staging := md.ResourceMetrics().AppendEmpty()
	staging.Resource().Attributes().PutStr("service.name", "staging-worker")
	addGauge(staging.ScopeMetrics().AppendEmpty().Metrics(), "http.server.duration")
	prod := md.ResourceMetrics().AppendEmpty()
	prod.Resource().Attributes().PutStr("service.name", "prod-worker")
	addGauge(prod.ScopeMetrics().AppendEmpty().Metrics(), "http.server.duration")

	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	if md.MetricCount() != 1 {
		t.Fatalf("MetricCount = %d, want 1 (staging dropped)", md.MetricCount())
	}
	attrs := md.ResourceMetrics().At(0).Resource().Attributes()
	if val, ok := attrs.Get("service.name"); !ok || val.Str() != "prod-worker" {
		t.Errorf("surviving resource = %v, want prod-worker", val.AsString())
	}
}

// A fully-matched batch must be consumed (nil error, nothing forwarded) and
// the emptied scope/resource containers pruned before the count check.
func TestConsumeMetricsPrunesEmptyContainers(t *testing.T) {
	sink := &captureMetricsConsumer{}
	set := processor.Settings{
		TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()},
	}
	engine := newRuleEngine(zap.NewNop(), &Config{})
	engine.rules = compileRules([]PolicyRule{
		dropMetricsRule("drop-everything", fieldMetricName, OpExists, ""),
	}, zap.NewNop())
	p := newMetricsProcessor(set, &Config{}, engine, sink)

	md := pmetric.NewMetrics()
	for i := 0; i < 2; i++ {
		ms := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
		addGauge(ms, "noise.metric")
	}

	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	if md.ResourceMetrics().Len() != 0 {
		t.Errorf("ResourceMetrics().Len() = %d, want 0 — empty containers must be pruned", md.ResourceMetrics().Len())
	}
	if len(sink.batches) != 0 {
		t.Errorf("fully-dropped batch was forwarded downstream")
	}
}

// Signal isolation and Phase 6 action scope: TRACES/LOGS rules and METRICS
// REDACT/SAMPLE rules (unsupported this phase) must all leave metrics
// untouched.
func TestMetricsIgnoreForeignAndUnsupportedRules(t *testing.T) {
	rate := 0.0 // would drop everything if (incorrectly) enforced
	p := newTestMetricsProcessor(t, []PolicyRule{
		{
			IsActive:       true,
			Name:           "traces-only-drop",
			ActionType:     ActionDrop,
			TargetSignal:   SignalTraces,
			ConditionField: fieldMetricName,
			ConditionOp:    OpExists,
		},
		{
			IsActive:       true,
			Name:           "metrics-redact-unsupported",
			ActionType:     ActionRedact,
			TargetSignal:   SignalMetrics,
			ConditionField: fieldMetricName,
			ConditionOp:    OpExists,
		},
		{
			IsActive:       true,
			Name:           "metrics-sample-unsupported",
			ActionType:     ActionSample,
			SampleRate:     &rate,
			TargetSignal:   SignalMetrics,
			ConditionField: fieldMetricName,
			ConditionOp:    OpExists,
		},
	})

	md := pmetric.NewMetrics()
	ms := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	addGauge(ms, "survives.all.rules")

	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}
	if md.MetricCount() != 1 {
		t.Errorf("MetricCount = %d, want 1 — foreign/unsupported rules must not touch metrics", md.MetricCount())
	}
	if d := p.engine.metricsDropped.Load(); d != 0 {
		t.Errorf("metricsDropped = %d, want 0", d)
	}
}

// The reverse direction: METRICS rules must never drop spans or log records.
func TestOtherSignalsIgnoreMetricsRules(t *testing.T) {
	metricsRule := dropMetricsRule("metrics-only-drop", "test.marker", OpExists, "")

	tp := newTestProcessor(t, []PolicyRule{metricsRule})
	td := ptrace.NewTraces()
	addSpan(td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans(), traceIDFromByte(11), "kept")
	if err := tp.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}
	if td.SpanCount() != 1 {
		t.Errorf("SpanCount = %d, want 1 — METRICS rules must not touch traces", td.SpanCount())
	}

	lp := newTestLogsProcessor(t, []PolicyRule{metricsRule})
	ld := plog.NewLogs()
	addLogRecord(ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords(), "INFO", "kept")
	if err := lp.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}
	if ld.LogRecordCount() != 1 {
		t.Errorf("LogRecordCount = %d, want 1 — METRICS rules must not touch logs", ld.LogRecordCount())
	}
}
