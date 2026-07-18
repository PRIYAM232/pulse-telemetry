package filterprocessor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// nopConsumer accepts everything; tests only inspect the processor's own
// mutations and counters.
type nopConsumer struct{}

func (nopConsumer) Capabilities() consumer.Capabilities                { return consumer.Capabilities{} }
func (nopConsumer) ConsumeTraces(context.Context, ptrace.Traces) error { return nil }

func newTestProcessor(t *testing.T, rules []PolicyRule) *tracesProcessor {
	t.Helper()
	set := processor.Settings{
		TelemetrySettings: component.TelemetrySettings{Logger: zap.NewNop()},
	}
	engine := newRuleEngine(zap.NewNop(), &Config{})
	engine.rules = compileRules(rules, zap.NewNop())
	return newTracesProcessor(set, &Config{}, engine, nopConsumer{})
}

func traceIDFromByte(b byte) pcommon.TraceID {
	var id [16]byte
	for i := range id {
		id[i] = b + byte(i)
	}
	return id
}

// addSpan appends one span with the given trace ID and a marker attribute the
// test rules match on.
func addSpan(ss ptrace.SpanSlice, id pcommon.TraceID, name string) {
	span := ss.AppendEmpty()
	span.SetTraceID(id)
	span.SetName(name)
	span.Attributes().PutStr("test.marker", "yes")
}

func TestTraceIDBucketDeterministicAndBounded(t *testing.T) {
	for b := 0; b < 256; b++ {
		id := traceIDFromByte(byte(b))
		first := traceIDBucket(id)
		if first >= 100 {
			t.Fatalf("bucket %d out of range [0,100) for trace %v", first, id)
		}
		if again := traceIDBucket(id); again != first {
			t.Fatalf("bucket not deterministic: %d then %d for trace %v", first, again, id)
		}
	}
}

func TestSampleKeepThreshold(t *testing.T) {
	cases := []struct {
		rate float64
		want uint64
	}{
		{0, 0},
		{-0.5, 0},
		{0.1, 10},
		{0.25, 25},
		{0.29, 29},
		{0.999, 100},
		{1, 100},
		{1.7, 100},
	}
	for _, c := range cases {
		if got := sampleKeepThreshold(c.rate); got != c.want {
			t.Errorf("sampleKeepThreshold(%v) = %d, want %d", c.rate, got, c.want)
		}
	}
}

// TestSampleKeepsOrDropsWholeTraces is the core Phase 3 guarantee: spans
// sharing a TraceID — even across separate ResourceSpans, as split batches
// produce — are kept or dropped together, and the verdict matches the
// bucket/threshold rule exactly.
func TestSampleKeepsOrDropsWholeTraces(t *testing.T) {
	rate := 0.3
	p := newTestProcessor(t, []PolicyRule{{
		IsActive:       true,
		Name:           "sample-30pct",
		ActionType:     ActionSample,
		SampleRate:     &rate,
		TargetSignal:   SignalTraces,
		ConditionField: "test.marker",
		ConditionOp:    OpExists,
	}})

	td := ptrace.NewTraces()
	traceIDs := make([]pcommon.TraceID, 0, 64)
	for b := 0; b < 64; b++ {
		id := traceIDFromByte(byte(b * 4))
		traceIDs = append(traceIDs, id)
		// Same trace spread across two resources with two spans each.
		for r := 0; r < 2; r++ {
			ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
			addSpan(ss, id, "parent")
			addSpan(ss, id, "child")
		}
	}

	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}

	surviving := map[pcommon.TraceID]int{}
	rss := td.ResourceSpans()
	for i := 0; i < rss.Len(); i++ {
		sss := rss.At(i).ScopeSpans()
		for j := 0; j < sss.Len(); j++ {
			spans := sss.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				surviving[spans.At(k).TraceID()]++
			}
		}
	}

	threshold := sampleKeepThreshold(rate)
	kept := 0
	for _, id := range traceIDs {
		got := surviving[id]
		if traceIDBucket(id) < threshold {
			kept++
			if got != 4 {
				t.Errorf("trace %v in keep bucket: want all 4 spans, got %d", id, got)
			}
		} else if got != 0 {
			t.Errorf("trace %v in drop bucket: want 0 spans, got %d (trace split!)", id, got)
		}
	}
	if kept == 0 || kept == len(traceIDs) {
		t.Errorf("degenerate sample: %d/%d traces kept — bucket spread is broken", kept, len(traceIDs))
	}
}

// A SAMPLE keep-verdict must not shield a span from a later DROP rule.
func TestSampleKeepDoesNotShieldFromDrop(t *testing.T) {
	keepAll := 1.0
	p := newTestProcessor(t, []PolicyRule{
		{
			IsActive:       true,
			Name:           "sample-keep-everything",
			ActionType:     ActionSample,
			SampleRate:     &keepAll,
			TargetSignal:   SignalTraces,
			ConditionField: "test.marker",
			ConditionOp:    OpExists,
		},
		{
			IsActive:       true,
			Name:           "drop-parents",
			ActionType:     ActionDrop,
			TargetSignal:   SignalTraces,
			ConditionField: "test.marker",
			ConditionOp:    OpExists,
		},
	})

	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	addSpan(ss, traceIDFromByte(7), "parent")

	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}
	if td.SpanCount() != 0 {
		t.Errorf("span survived: SAMPLE keep verdict shielded it from the DROP rule")
	}
}

// firstSpan digs the only span back out of the batch; ptrace.Span handles can
// be invalidated by RemoveIf, so tests must re-read after ConsumeTraces.
func firstSpan(t *testing.T, td ptrace.Traces) ptrace.Span {
	t.Helper()
	if td.SpanCount() != 1 {
		t.Fatalf("SpanCount = %d, want 1", td.SpanCount())
	}
	return td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0)
}

func redactRule(name, field string) PolicyRule {
	return PolicyRule{
		IsActive:       true,
		Name:           name,
		ActionType:     ActionRedact,
		TargetSignal:   SignalTraces,
		ConditionField: field,
		ConditionOp:    OpExists,
	}
}

// The core REDACT contract: the matched attribute is masked, everything else
// about the span — other attributes included — survives untouched.
func TestRedactMasksAttributeAndKeepsSpan(t *testing.T) {
	p := newTestProcessor(t, []PolicyRule{redactRule("redact-email", "user.email")})

	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	span := ss.AppendEmpty()
	span.SetTraceID(traceIDFromByte(1))
	span.SetName("checkout")
	span.Attributes().PutStr("user.email", "alice@example.com")
	span.Attributes().PutStr("http.route", "/checkout")

	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}

	got := firstSpan(t, td)
	if val, ok := got.Attributes().Get("user.email"); !ok || val.Str() != redactedPlaceholder {
		t.Errorf("user.email = %v, want %q", val.AsString(), redactedPlaceholder)
	}
	if val, ok := got.Attributes().Get("http.route"); !ok || val.Str() != "/checkout" {
		t.Errorf("http.route = %v, want untouched \"/checkout\"", val.AsString())
	}
	if dropped := p.engine.tracesDropped.Load(); dropped != 0 {
		t.Errorf("tracesDropped = %d, want 0 — REDACT must never count as a drop", dropped)
	}
}

// Non-string values (ints, maps, ...) must be replaced wholesale by the
// string mask: neither the value nor its original type may survive.
func TestRedactReplacesNonStringValueTypes(t *testing.T) {
	p := newTestProcessor(t, []PolicyRule{
		redactRule("redact-ssn", "user.ssn"),
		redactRule("redact-profile", "user.profile"),
	})

	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	span := ss.AppendEmpty()
	span.SetTraceID(traceIDFromByte(2))
	span.SetName("signup")
	span.Attributes().PutInt("user.ssn", 123456789)
	span.Attributes().PutEmptyMap("user.profile").PutStr("email", "alice@example.com")

	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}

	got := firstSpan(t, td)
	for _, key := range []string{"user.ssn", "user.profile"} {
		val, ok := got.Attributes().Get(key)
		if !ok {
			t.Errorf("%s missing after redaction, want string mask", key)
			continue
		}
		if val.Type() != pcommon.ValueTypeStr || val.Str() != redactedPlaceholder {
			t.Errorf("%s = (%s) %v, want (Str) %q", key, val.Type(), val.AsString(), redactedPlaceholder)
		}
	}
}

// A rule matching via the enclosing resource must scrub the resource
// attribute where it lives, not conjure a redacted copy onto the span.
func TestRedactScrubsResourceAttributeInPlace(t *testing.T) {
	p := newTestProcessor(t, []PolicyRule{redactRule("redact-email", "user.email")})

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	rs.Resource().Attributes().PutStr("user.email", "alice@example.com")
	span := rs.ScopeSpans().AppendEmpty().Spans().AppendEmpty()
	span.SetTraceID(traceIDFromByte(3))
	span.SetName("checkout")

	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}

	if td.SpanCount() != 1 {
		t.Fatalf("SpanCount = %d, want 1", td.SpanCount())
	}
	resAttrs := td.ResourceSpans().At(0).Resource().Attributes()
	if val, ok := resAttrs.Get("user.email"); !ok || val.Str() != redactedPlaceholder {
		t.Errorf("resource user.email = %v, want %q", val.AsString(), redactedPlaceholder)
	}
	spanAttrs := td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).Attributes()
	if _, ok := spanAttrs.Get("user.email"); ok {
		t.Errorf("user.email leaked onto the span; redaction must stay on the resource")
	}
}

// Mirrors the SAMPLE shielding test: a REDACT scrub must not save the span
// from a later DROP rule.
func TestRedactDoesNotShieldFromDrop(t *testing.T) {
	p := newTestProcessor(t, []PolicyRule{
		redactRule("redact-email", "user.email"),
		{
			IsActive:       true,
			Name:           "drop-marked",
			ActionType:     ActionDrop,
			TargetSignal:   SignalTraces,
			ConditionField: "test.marker",
			ConditionOp:    OpExists,
		},
	})

	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	addSpan(ss, traceIDFromByte(4), "doomed")
	td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0).
		Attributes().PutStr("user.email", "alice@example.com")

	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}
	if td.SpanCount() != 0 {
		t.Errorf("span survived: REDACT shielded it from the DROP rule")
	}
}

// TestSyncOnceETagFlow drives syncOnce through the full conditional-polling
// lifecycle against a fake control plane: unconditional first poll, 304 on an
// unchanged ruleset, then a real update under a new ETag.
func TestSyncOnceETagFlow(t *testing.T) {
	const (
		etagV1 = `"etag-v1"`
		etagV2 = `"etag-v2"`
	)
	bodyV1 := `{"fleet_id":"test-fleet","rules":[{"id":"r1","fleetId":"test-fleet","isActive":true,"name":"drop-marked","actionType":"DROP","sampleRate":null,"targetSignal":"TRACES","conditionField":"test.marker","conditionOp":"EXISTS","conditionValue":"","createdAt":"2026-07-14T00:00:00Z","updatedAt":"2026-07-14T00:00:00Z"}]}`
	bodyV2 := `{"fleet_id":"test-fleet","rules":[]}`

	// Mutated only between the sequential syncOnce calls below; the handler
	// runs one request at a time, so no synchronization is needed.
	etag, body := etagV1, bodyV1
	var inmHeaders []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inmHeaders = append(inmHeaders, r.Header.Get("If-None-Match"))
		if r.Header.Get("If-None-Match") == etag {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	e := newRuleEngine(zap.NewNop(), &Config{SyncEndpoint: srv.URL, FleetKey: "test-key"})
	ctx := context.Background()

	// First poll: no cached ETag, expect a full 200 fetch.
	e.syncOnce(ctx)
	if inmHeaders[0] != "" {
		t.Errorf("first poll sent If-None-Match %q, want none", inmHeaders[0])
	}
	if e.lastETag != etagV1 {
		t.Errorf("lastETag = %q, want %q", e.lastETag, etagV1)
	}
	if len(e.rules) != 1 {
		t.Fatalf("rules after first sync = %d, want 1", len(e.rules))
	}

	// Second poll: same ruleset, expect a 304 that leaves everything intact.
	e.syncOnce(ctx)
	if inmHeaders[1] != etagV1 {
		t.Errorf("second poll sent If-None-Match %q, want %q", inmHeaders[1], etagV1)
	}
	if len(e.rules) != 1 || e.lastETag != etagV1 {
		t.Errorf("304 must not touch state: rules=%d etag=%q", len(e.rules), e.lastETag)
	}

	// Control plane publishes a new ruleset: stale ETag misses, 200 replaces.
	etag, body = etagV2, bodyV2
	e.syncOnce(ctx)
	if inmHeaders[2] != etagV1 {
		t.Errorf("third poll sent If-None-Match %q, want %q", inmHeaders[2], etagV1)
	}
	if e.lastETag != etagV2 {
		t.Errorf("lastETag = %q, want %q after update", e.lastETag, etagV2)
	}
	if len(e.rules) != 0 {
		t.Errorf("rules after update = %d, want 0", len(e.rules))
	}
}

func TestConsumeTracesCountsReceivedAndDropped(t *testing.T) {
	p := newTestProcessor(t, []PolicyRule{{
		IsActive:       true,
		Name:           "drop-marked",
		ActionType:     ActionDrop,
		TargetSignal:   SignalTraces,
		ConditionField: "test.marker",
		ConditionOp:    OpExists,
	}})

	td := ptrace.NewTraces()
	ss := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans()
	addSpan(ss, traceIDFromByte(1), "dropped-1")
	addSpan(ss, traceIDFromByte(2), "dropped-2")
	unmatched := ss.AppendEmpty()
	unmatched.SetTraceID(traceIDFromByte(3))
	unmatched.SetName("kept")

	if err := p.ConsumeTraces(context.Background(), td); err != nil {
		t.Fatalf("ConsumeTraces: %v", err)
	}

	if got := p.engine.tracesReceived.Load(); got != 3 {
		t.Errorf("tracesReceived = %d, want 3", got)
	}
	if got := p.engine.tracesDropped.Load(); got != 2 {
		t.Errorf("tracesDropped = %d, want 2", got)
	}
}
