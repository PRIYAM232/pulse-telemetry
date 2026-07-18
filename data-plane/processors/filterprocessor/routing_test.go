package filterprocessor

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

// The ROUTE contract, shared by all three signals: a match stamps the
// enclosing RESOURCE with pulse.routing.destination and never drops — the
// fork itself happens downstream in the collector's routing connector.

func routeRule(name, signal, field, op, value, destination string) PolicyRule {
	return PolicyRule{
		IsActive:          true,
		Name:              name,
		ActionType:        ActionRoute,
		TargetSignal:      signal,
		ConditionField:    field,
		ConditionOp:       op,
		ConditionValue:    value,
		TargetDestination: destination,
	}
}

func resourceDestination(t *testing.T, attrs pcommon.Map) string {
	t.Helper()
	val, ok := attrs.Get(attrRoutingDestination)
	if !ok {
		t.Fatalf("resource attribute %q not set", attrRoutingDestination)
	}
	return val.Str()
}

func TestRouteTagsTraceResourceAndKeepsSpans(t *testing.T) {
	p := newTestProcessor(t, []PolicyRule{
		routeRule("route-marked", SignalTraces, "test.marker", OpEquals, "yes", "cold-storage"),
	})

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	ss := rs.ScopeSpans().AppendEmpty().Spans()
	addSpan(ss, traceIDFromByte(1), "span-a")
	addSpan(ss, traceIDFromByte(2), "span-b")

	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}

	if td.SpanCount() != 2 {
		t.Errorf("SpanCount = %d, want 2 (ROUTE must never drop)", td.SpanCount())
	}
	got := resourceDestination(t, td.ResourceSpans().At(0).Resource().Attributes())
	if got != "cold-storage" {
		t.Errorf("destination = %q, want %q", got, "cold-storage")
	}
	if d := p.engine.tracesDropped.Load(); d != 0 {
		t.Errorf("tracesDropped = %d, want 0", d)
	}
}

func TestRouteDoesNotShieldFromDrop(t *testing.T) {
	p := newTestProcessor(t, []PolicyRule{
		routeRule("route-marked", SignalTraces, "test.marker", OpEquals, "yes", "cold-storage"),
		{
			IsActive:       true,
			Name:           "drop-marked",
			ActionType:     ActionDrop,
			TargetSignal:   SignalTraces,
			ConditionField: "test.marker",
			ConditionOp:    OpEquals,
			ConditionValue: "yes",
		},
	})

	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	addSpan(ss, traceIDFromByte(1), "span-a")

	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}
	if td.SpanCount() != 0 {
		t.Errorf("SpanCount = %d, want 0 (a ROUTE tag must not shield from a later DROP)", td.SpanCount())
	}
}

// A ROUTE rule without a destination is malformed: stamping "" would poison
// the routing connector's table lookups, so the rule is synced but skipped —
// the same fail-open contract as a SAMPLE rule without a rate.
func TestRouteWithoutDestinationIsNotEnforced(t *testing.T) {
	p := newTestProcessor(t, []PolicyRule{
		routeRule("route-empty", SignalTraces, "test.marker", OpEquals, "yes", ""),
	})

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	addSpan(rs.ScopeSpans().AppendEmpty().Spans(), traceIDFromByte(1), "span-a")

	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}
	if _, ok := td.ResourceSpans().At(0).Resource().Attributes().Get(attrRoutingDestination); ok {
		t.Errorf("resource attribute %q set by a destination-less ROUTE rule", attrRoutingDestination)
	}
}

func TestRouteTagsLogResourceAndKeepsRecords(t *testing.T) {
	p := newTestLogsProcessor(t, []PolicyRule{
		routeRule("route-errors", SignalLogs, fieldLogSeverity, OpEquals, "ERROR", "cold-storage"),
	})

	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	slr := rl.ScopeLogs().AppendEmpty().LogRecords()
	addLogRecord(slr, "ERROR", "boom")
	addLogRecord(slr, "INFO", "fine")

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}

	if ld.LogRecordCount() != 2 {
		t.Errorf("LogRecordCount = %d, want 2 (ROUTE must never drop)", ld.LogRecordCount())
	}
	got := resourceDestination(t, ld.ResourceLogs().At(0).Resource().Attributes())
	if got != "cold-storage" {
		t.Errorf("destination = %q, want %q", got, "cold-storage")
	}
}

func TestRouteTagsMetricResourceAndKeepsMetrics(t *testing.T) {
	p := newTestMetricsProcessor(t, []PolicyRule{
		routeRule("route-runtime", SignalMetrics, fieldMetricName, OpContains, "runtime.", "cold-storage"),
	})

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	ms := rm.ScopeMetrics().AppendEmpty().Metrics()
	addGauge(ms, "runtime.heap.alloc")
	addGauge(ms, "http.server.duration")

	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	if md.MetricCount() != 2 {
		t.Errorf("MetricCount = %d, want 2 (ROUTE must never drop)", md.MetricCount())
	}
	got := resourceDestination(t, md.ResourceMetrics().At(0).Resource().Attributes())
	if got != "cold-storage" {
		t.Errorf("destination = %q, want %q", got, "cold-storage")
	}
	if d := p.engine.metricsDropped.Load(); d != 0 {
		t.Errorf("metricsDropped = %d, want 0", d)
	}
}

// Last ROUTE match wins: PutStr upserts, so when two ROUTE rules match records
// under one resource the destination reflects the later rule in sync order.
func TestRouteLastMatchWins(t *testing.T) {
	p := newTestLogsProcessor(t, []PolicyRule{
		routeRule("route-first", SignalLogs, fieldLogSeverity, OpEquals, "ERROR", "premium-analytics"),
		routeRule("route-second", SignalLogs, fieldLogSeverity, OpEquals, "ERROR", "cold-storage"),
	})

	ld := plog.NewLogs()
	slr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	addLogRecord(slr, "ERROR", "boom")

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}
	got := resourceDestination(t, ld.ResourceLogs().At(0).Resource().Attributes())
	if got != "cold-storage" {
		t.Errorf("destination = %q, want %q (last match wins)", got, "cold-storage")
	}
}
