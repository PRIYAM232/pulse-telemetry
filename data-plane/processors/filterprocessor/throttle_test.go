package filterprocessor

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func throttleRuleFor(name, signal string, ratePerSec int, groupBy, condField, condOp, condValue string) PolicyRule {
	return PolicyRule{
		IsActive:        true,
		Name:            name,
		ActionType:      ActionThrottle,
		ThrottleRate:    &ratePerSec,
		ThrottleGroupBy: groupBy,
		TargetSignal:    signal,
		ConditionField:  condField,
		ConditionOp:     condOp,
		ConditionValue:  condValue,
	}
}

// --- limiterGroup unit behavior ------------------------------------------------

// Burst equals the rate, so a cold bucket admits exactly ratePerSec events
// from a burst that arrives faster than tokens refill — and each key's bucket
// is independent.
func TestLimiterGroupClampsAndPartitions(t *testing.T) {
	g := newLimiterGroup(5)

	for i := 0; i < 5; i++ {
		if !g.allow("tenant-a") {
			t.Fatalf("tenant-a event %d denied, want first 5 allowed", i+1)
		}
	}
	if g.allow("tenant-a") {
		t.Error("tenant-a event 6 allowed, want denied (bucket drained)")
	}
	if !g.allow("tenant-b") {
		t.Error("tenant-b denied, want allowed: buckets must be independent per key")
	}
}

// Generational eviction bounds retention at two generations regardless of
// how many distinct keys arrive — the millions-of-unique-keys memory case.
func TestLimiterGroupBoundedUnderKeyChurn(t *testing.T) {
	g := newLimiterGroup(1)

	for i := 0; i < 3*maxThrottleKeysPerGeneration; i++ {
		g.allow(fmt.Sprintf("churn-%d", i))
	}

	g.mu.RLock()
	cur, prev := len(g.cur), len(g.prev)
	g.mu.RUnlock()
	if cur > maxThrottleKeysPerGeneration || prev > maxThrottleKeysPerGeneration {
		t.Errorf("generations hold %d + %d keys, want each <= %d",
			cur, prev, maxThrottleKeysPerGeneration)
	}
}

// A key that survives rotation into `prev` keeps its token state when
// promoted back — no free burst just because the map rotated underneath it.
func TestLimiterGroupPromotionKeepsState(t *testing.T) {
	g := newLimiterGroup(1)

	if !g.allow("hot") {
		t.Fatal("first hot event denied, want allowed")
	}
	if g.allow("hot") {
		t.Fatal("second hot event allowed, want denied (burst 1)")
	}

	// Exactly enough unique keys to trigger ONE rotation, leaving "hot" in
	// the previous generation. (A second rotation would drop it entirely —
	// that eviction is TestLimiterGroupBoundedUnderKeyChurn's subject.)
	for i := 0; i < maxThrottleKeysPerGeneration; i++ {
		g.allow(fmt.Sprintf("churn-%d", i))
	}

	// Promotion must carry the drained bucket, not mint a fresh one. (The
	// churn loop runs in well under the 1s a rate-1 bucket needs to refill.)
	if g.allow("hot") {
		t.Error("hot allowed after rotation, want denied: promotion must keep token state")
	}
}

// Oversized group values are truncated before keying the map, so both halves
// of a pathological value land in one bucket and key memory stays bounded.
func TestLimiterGroupTruncatesOversizedKeys(t *testing.T) {
	g := newLimiterGroup(1)

	long := make([]byte, maxThrottleKeyBytes+512)
	for i := range long {
		long[i] = 'a'
	}
	if !g.allow(string(long)) {
		t.Fatal("first oversized-key event denied, want allowed")
	}
	// Same prefix, different tail: must hit the SAME truncated bucket.
	long[len(long)-1] = 'b'
	if g.allow(string(long)) {
		t.Error("second oversized-key event allowed, want denied via shared truncated bucket")
	}
}

// allow must be race-free under concurrent hot-path use (run with -race) and
// never admit more than one burst plus negligible refill.
func TestLimiterGroupConcurrentAllow(t *testing.T) {
	g := newLimiterGroup(100)

	var mu sync.Mutex
	allowed := 0
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := 0
			for i := 0; i < 500; i++ {
				if g.allow("shared") {
					local++
				}
			}
			mu.Lock()
			allowed += local
			mu.Unlock()
		}()
	}
	wg.Wait()

	// 4000 attempts in microseconds against burst 100: refill can add at
	// most a token or two.
	if allowed < 100 || allowed > 110 {
		t.Errorf("allowed = %d of 4000 concurrent events, want ~100 (one burst)", allowed)
	}
}

// --- Signal integration ---------------------------------------------------------

// The Phase 8 headline: one tenant's log flood clamps to its own bucket's
// rate while a quiet tenant's records all pass, and the clamp shows up in the
// dropped counters.
func TestThrottleLogsPerGroupKey(t *testing.T) {
	p := newTestLogsProcessor(t, []PolicyRule{
		throttleRuleFor("throttle-per-tenant", SignalLogs, 5, "tenant_id",
			"test.marker", OpExists, ""),
	})

	ld := plog.NewLogs()
	slr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i := 0; i < 20; i++ {
		addLogRecord(slr, "DEBUG", "flood").Attributes().PutStr("tenant_id", "noisy")
	}
	for i := 0; i < 3; i++ {
		addLogRecord(slr, "INFO", "steady").Attributes().PutStr("tenant_id", "quiet")
	}

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}

	if got := ld.LogRecordCount(); got != 8 {
		t.Errorf("surviving records = %d, want 8 (5 of noisy's burst + all 3 quiet)", got)
	}
	if d := p.engine.logsDropped.Load(); d != 15 {
		t.Errorf("logsDropped = %d, want 15", d)
	}
}

// Records missing the group-by attribute share the default bucket — absent
// tenant_id is not a way around the limit.
func TestThrottleMissingGroupAttributeSharesDefaultBucket(t *testing.T) {
	p := newTestLogsProcessor(t, []PolicyRule{
		throttleRuleFor("throttle-per-tenant", SignalLogs, 3, "tenant_id",
			"test.marker", OpExists, ""),
	})

	ld := plog.NewLogs()
	slr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i := 0; i < 10; i++ {
		addLogRecord(slr, "DEBUG", "anonymous")
	}

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}
	if got := ld.LogRecordCount(); got != 3 {
		t.Errorf("surviving records = %d, want 3 (one shared default bucket)", got)
	}
}

// A THROTTLE rule with no group-by is a global limit: one bucket for every
// matching span. The group key resolves via resource attributes here, the
// same span-then-resource precedence conditions use.
func TestThrottleTracesGlobalBucket(t *testing.T) {
	p := newTestProcessor(t, []PolicyRule{
		throttleRuleFor("throttle-all", SignalTraces, 2, "",
			"service.name", OpExists, ""),
	})

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("service.name", "burster")
	spans := rs.ScopeSpans().AppendEmpty().Spans()
	for i := 0; i < 10; i++ {
		addSpan(spans, traceIDFromByte(byte(i+1)), fmt.Sprintf("op-%d", i))
	}

	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}
	if got := td.SpanCount(); got != 2 {
		t.Errorf("surviving spans = %d, want 2 (global bucket, rate 2)", got)
	}
	if d := p.engine.tracesDropped.Load(); d != 8 {
		t.Errorf("tracesDropped = %d, want 8", d)
	}
}

// Metrics group on the metric.name virtual field: each metric name gets its
// own bucket, so capping one exploding series family leaves the others whole.
func TestThrottleMetricsPerMetricName(t *testing.T) {
	p := newTestMetricsProcessor(t, []PolicyRule{
		throttleRuleFor("throttle-per-metric", SignalMetrics, 1, fieldMetricName,
			fieldMetricName, OpExists, ""),
	})

	md := pmetric.NewMetrics()
	ms := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
	for i := 0; i < 4; i++ {
		addGauge(ms, "http.server.duration")
	}
	for i := 0; i < 4; i++ {
		addGauge(ms, "db.client.latency")
	}

	if err := p.ConsumeMetrics(context.Background(), md); err != nil {
		t.Fatalf("ConsumeMetrics: %v", err)
	}

	names := survivingMetricNames(md)
	if len(names) != 2 || names[0] != "http.server.duration" || names[1] != "db.client.latency" {
		t.Errorf("surviving metrics = %v, want one of each name", names)
	}
}

// A THROTTLE rule without a positive rate is malformed: fail open (nothing
// dropped), matching the rate-less SAMPLE and destination-less ROUTE
// contracts.
func TestThrottleWithoutRateNotEnforced(t *testing.T) {
	zero := 0
	rules := []PolicyRule{
		{
			IsActive:       true,
			Name:           "throttle-no-rate",
			ActionType:     ActionThrottle,
			TargetSignal:   SignalLogs,
			ConditionField: "test.marker",
			ConditionOp:    OpExists,
		},
		{
			IsActive:       true,
			Name:           "throttle-zero-rate",
			ActionType:     ActionThrottle,
			ThrottleRate:   &zero,
			TargetSignal:   SignalLogs,
			ConditionField: "test.marker",
			ConditionOp:    OpExists,
		},
	}
	p := newTestLogsProcessor(t, rules)

	ld := plog.NewLogs()
	slr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i := 0; i < 5; i++ {
		addLogRecord(slr, "INFO", "fine")
	}

	if err := p.ConsumeLogs(context.Background(), ld); err != nil {
		t.Fatalf("ConsumeLogs: %v", err)
	}
	if got := ld.LogRecordCount(); got != 5 {
		t.Errorf("surviving records = %d, want all 5 (malformed rule fails open)", got)
	}
}
