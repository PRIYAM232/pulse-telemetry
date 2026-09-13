// Command loadgen is the Pulse sandbox's synthetic workload. It speaks raw
// OTLP/gRPC to the proxy and emits streams built to exercise one pulse_filter
// path each:
//
//   - Noisy neighbor: tenant "globex" has a runaway reindex job spamming DEBUG
//     logs from service search-indexer at -noisy-rate records/sec. PII-free,
//     so the THROTTLE effect is measurable on its own.
//   - PII: well-behaved tenants "acme" and "initech" send payments-api INFO
//     logs at -tenant-rate records/sec each. Every body embeds a synthetic
//     email, SSN, or card number; every record carries a user.email attribute.
//   - Traces: payments-api checkout traces (-trace-rate per tenant) whose
//     db.query.text attributes embed the same PII, for inspection in Jaeger.
//   - Metrics: cumulative payments.requests / search.cache.misses counters
//     every 5s, so Prometheus has app metrics that also cross the proxy.
//
// It builds pdata and calls the OTLP export service directly instead of using
// the OTel SDK: the SDK's batch processors silently drop records when their
// queues fill, which at these rates would muddy exactly the numbers the
// sandbox compares. Here every record is either acknowledged by the proxy or
// counted as an export error.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	scopeName     = "pulse-sandbox/loadgen"
	noisyTenant   = "globex"
	tickInterval  = 100 * time.Millisecond
	metricsEvery  = 5 * time.Second
	exportTimeout = 5 * time.Second
)

var quietTenants = []string{"acme", "initech"}

func main() {
	endpoint := flag.String("endpoint", envString("OTLP_ENDPOINT", "localhost:4317"), "proxy OTLP/gRPC endpoint (host:port)")
	noisyRate := flag.Float64("noisy-rate", envFloat("NOISY_RATE", 500), "DEBUG logs/sec from the noisy tenant")
	tenantRate := flag.Float64("tenant-rate", envFloat("TENANT_RATE", 5), "PII-laden INFO logs/sec from each well-behaved tenant")
	traceRate := flag.Float64("trace-rate", envFloat("TRACE_RATE", 1), "checkout traces/sec from each well-behaved tenant")
	reportEvery := flag.Duration("report-every", 10*time.Second, "how often to print send counts")
	flag.Parse()

	conn, err := grpc.NewClient(*endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("loadgen: dialing %s: %v", *endpoint, err)
	}
	defer conn.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	g := newGenerator(conn)
	log.Printf("loadgen: sending to %s — noisy %s: %.0f logs/s · %v: %.0f logs/s + %.1f traces/s each",
		*endpoint, noisyTenant, *noisyRate, quietTenants, *tenantRate, *traceRate)
	log.Printf("loadgen: REDACT pattern for the runbook (REGEX_MATCH):\n\n    %s\n", RedactPattern)

	noisy := pacer{rate: *noisyRate}
	payments := make([]pacer, len(quietTenants))
	checkouts := make([]pacer, len(quietTenants))
	for i := range quietTenants {
		payments[i] = pacer{rate: *tenantRate}
		checkouts[i] = pacer{rate: *traceRate}
	}

	tick := time.NewTicker(tickInterval)
	defer tick.Stop()
	metricsTick := time.NewTicker(metricsEvery)
	defer metricsTick.Stop()
	reportTick := time.NewTicker(*reportEvery)
	defer reportTick.Stop()

	last := time.Now()
	for {
		select {
		case <-ctx.Done():
			g.report("final")
			return
		case now := <-tick.C:
			// Real elapsed time, not the nominal tick: a slow export makes the
			// next batch bigger instead of silently lowering the rate. Capped
			// so a long stall (proxy down) doesn't come back as one huge burst.
			dt := min(now.Sub(last).Seconds(), 1)
			last = now

			ld := plog.NewLogs()
			g.appendNoise(ld, noisy.take(dt), now)
			td := ptrace.NewTraces()
			for i, tenant := range quietTenants {
				g.appendPayments(ld, tenant, payments[i].take(dt), now)
				g.appendCheckouts(td, tenant, checkouts[i].take(dt), now)
			}
			g.exportLogs(ctx, ld)
			g.exportTraces(ctx, td)
		case now := <-metricsTick.C:
			g.exportMetrics(ctx, now)
		case <-reportTick.C:
			g.report("sent")
		}
	}
}

// pacer converts a fractional events/sec rate into whole events per tick,
// carrying the remainder so e.g. 0.5/s still yields one event every 2s.
type pacer struct{ rate, carry float64 }

func (p *pacer) take(dt float64) int {
	p.carry += p.rate * dt
	n := int(p.carry)
	p.carry -= float64(n)
	return n
}

// counts is one reporting window's tally; totals accumulate across windows.
type counts struct {
	logs     map[string]int64 // per tenant
	spans    int64
	failures int64
}

func newCounts() counts { return counts{logs: map[string]int64{}} }

type generator struct {
	rnd *rand.Rand
	pii piiSource

	logsClient    plogotlp.GRPCClient
	tracesClient  ptraceotlp.GRPCClient
	metricsClient pmetricotlp.GRPCClient

	// Metrics are cumulative since start, as Prometheus' OTLP intake expects.
	start       pcommon.Timestamp
	paymentReqs map[string]int64
	cacheMisses int64

	window, total counts
	lastErr       string
}

func newGenerator(conn *grpc.ClientConn) *generator {
	rnd := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 0x9e3779b97f4a7c15))
	return &generator{
		rnd:           rnd,
		pii:           piiSource{rnd},
		logsClient:    plogotlp.NewGRPCClient(conn),
		tracesClient:  ptraceotlp.NewGRPCClient(conn),
		metricsClient: pmetricotlp.NewGRPCClient(conn),
		start:         pcommon.NewTimestampFromTime(time.Now()),
		paymentReqs:   map[string]int64{},
		window:        newCounts(),
		total:         newCounts(),
	}
}

func newResource(attrs pcommon.Map, service string) {
	attrs.PutStr("service.name", service)
	attrs.PutStr("service.instance.id", service+"-sandbox-0")
	attrs.PutStr("deployment.environment.name", "sandbox")
}

func (g *generator) appendNoise(ld plog.Logs, n int, now time.Time) {
	if n == 0 {
		return
	}
	rl := ld.ResourceLogs().AppendEmpty()
	newResource(rl.Resource().Attributes(), "search-indexer")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(scopeName)
	ts := pcommon.NewTimestampFromTime(now)
	for range n {
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(ts)
		lr.SetObservedTimestamp(ts)
		lr.SetSeverityNumber(plog.SeverityNumberDebug)
		lr.SetSeverityText("DEBUG")
		lr.Body().SetStr(g.pii.noiseBody())
		lr.Attributes().PutStr("tenant_id", noisyTenant)
		lr.Attributes().PutStr("job.name", "reindex-all")
	}
	g.cacheMisses += int64(n)
	g.countLogs(noisyTenant, n)
}

func (g *generator) appendPayments(ld plog.Logs, tenant string, n int, now time.Time) {
	if n == 0 {
		return
	}
	rl := ld.ResourceLogs().AppendEmpty()
	newResource(rl.Resource().Attributes(), "payments-api")
	sl := rl.ScopeLogs().AppendEmpty()
	sl.Scope().SetName(scopeName)
	ts := pcommon.NewTimestampFromTime(now)
	for range n {
		ev := g.pii.paymentEvent()
		lr := sl.LogRecords().AppendEmpty()
		lr.SetTimestamp(ts)
		lr.SetObservedTimestamp(ts)
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.SetSeverityText("INFO")
		lr.Body().SetStr(ev.body)
		lr.Attributes().PutStr("tenant_id", tenant)
		lr.Attributes().PutStr("event.name", ev.name)
		lr.Attributes().PutStr("user.email", ev.email)
	}
	g.paymentReqs[tenant] += int64(n)
	g.countLogs(tenant, n)
}

// appendCheckouts emits n three-span checkout traces: the HTTP server span
// plus two database client spans whose db.query.text leaks PII.
func (g *generator) appendCheckouts(td ptrace.Traces, tenant string, n int, now time.Time) {
	if n == 0 {
		return
	}
	rs := td.ResourceSpans().AppendEmpty()
	newResource(rs.Resource().Attributes(), "payments-api")
	ss := rs.ScopeSpans().AppendEmpty()
	ss.Scope().SetName(scopeName)

	for range n {
		traceID := g.traceID()
		email := g.pii.email()
		lookup, insert := g.pii.checkoutQueries(email)

		end := now
		start := end.Add(-time.Duration(40+g.rnd.IntN(160)) * time.Millisecond)
		root := ss.Spans().AppendEmpty()
		rootID := g.spanID()
		setSpan(root, traceID, rootID, pcommon.SpanID{}, "POST /v1/payments", ptrace.SpanKindServer, start, end)
		root.Attributes().PutStr("tenant_id", tenant)
		root.Attributes().PutStr("http.request.method", "POST")
		root.Attributes().PutStr("http.route", "/v1/payments")
		root.Attributes().PutInt("http.response.status_code", 200)

		cursor := start.Add(2 * time.Millisecond)
		for _, q := range []struct{ name, text string }{
			{"SELECT customers", lookup},
			{"INSERT payments", insert},
		} {
			d := time.Duration(3+g.rnd.IntN(15)) * time.Millisecond
			s := ss.Spans().AppendEmpty()
			setSpan(s, traceID, g.spanID(), rootID, q.name, ptrace.SpanKindClient, cursor, cursor.Add(d))
			s.Attributes().PutStr("tenant_id", tenant)
			s.Attributes().PutStr("db.system.name", "postgresql")
			s.Attributes().PutStr("db.query.text", q.text)
			cursor = cursor.Add(d + time.Millisecond)
		}
	}
	g.window.spans += int64(3 * n)
	g.total.spans += int64(3 * n)
}

func setSpan(s ptrace.Span, traceID pcommon.TraceID, id, parent pcommon.SpanID, name string, kind ptrace.SpanKind, start, end time.Time) {
	s.SetTraceID(traceID)
	s.SetSpanID(id)
	s.SetParentSpanID(parent)
	s.SetName(name)
	s.SetKind(kind)
	s.SetStartTimestamp(pcommon.NewTimestampFromTime(start))
	s.SetEndTimestamp(pcommon.NewTimestampFromTime(end))
	s.Status().SetCode(ptrace.StatusCodeOk)
}

func (g *generator) traceID() pcommon.TraceID {
	var id pcommon.TraceID
	binary.BigEndian.PutUint64(id[:8], g.rnd.Uint64())
	binary.BigEndian.PutUint64(id[8:], g.rnd.Uint64())
	return id
}

func (g *generator) spanID() pcommon.SpanID {
	var id pcommon.SpanID
	binary.BigEndian.PutUint64(id[:], g.rnd.Uint64()|1) // never the all-zero invalid ID
	return id
}

func (g *generator) exportMetrics(ctx context.Context, now time.Time) {
	md := pmetric.NewMetrics()
	ts := pcommon.NewTimestampFromTime(now)

	addSum := func(service, name, unit, desc string, points map[string]int64) {
		rm := md.ResourceMetrics().AppendEmpty()
		newResource(rm.Resource().Attributes(), service)
		sm := rm.ScopeMetrics().AppendEmpty()
		sm.Scope().SetName(scopeName)
		m := sm.Metrics().AppendEmpty()
		m.SetName(name)
		m.SetUnit(unit)
		m.SetDescription(desc)
		sum := m.SetEmptySum()
		sum.SetIsMonotonic(true)
		sum.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		for tenant, v := range points {
			dp := sum.DataPoints().AppendEmpty()
			dp.SetStartTimestamp(g.start)
			dp.SetTimestamp(ts)
			dp.SetIntValue(v)
			dp.Attributes().PutStr("tenant_id", tenant)
		}
	}
	addSum("payments-api", "payments.requests", "{request}", "Payment API requests handled, per tenant.", g.paymentReqs)
	addSum("search-indexer", "search.cache.misses", "{miss}", "Search index cache misses, per tenant.",
		map[string]int64{noisyTenant: g.cacheMisses})

	ctx, cancel := context.WithTimeout(ctx, exportTimeout)
	defer cancel()
	if _, err := g.metricsClient.Export(ctx, pmetricotlp.NewExportRequestFromMetrics(md)); err != nil {
		g.fail(err)
	}
}

func (g *generator) exportLogs(ctx context.Context, ld plog.Logs) {
	n := ld.LogRecordCount()
	if n == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, exportTimeout)
	defer cancel()
	resp, err := g.logsClient.Export(ctx, plogotlp.NewExportRequestFromLogs(ld))
	if err != nil {
		g.fail(err)
		return
	}
	if r := resp.PartialSuccess().RejectedLogRecords(); r > 0 {
		g.fail(fmt.Errorf("proxy rejected %d log records: %s", r, resp.PartialSuccess().ErrorMessage()))
	}
}

func (g *generator) exportTraces(ctx context.Context, td ptrace.Traces) {
	if td.SpanCount() == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, exportTimeout)
	defer cancel()
	resp, err := g.tracesClient.Export(ctx, ptraceotlp.NewExportRequestFromTraces(td))
	if err != nil {
		g.fail(err)
		return
	}
	if r := resp.PartialSuccess().RejectedSpans(); r > 0 {
		g.fail(fmt.Errorf("proxy rejected %d spans: %s", r, resp.PartialSuccess().ErrorMessage()))
	}
}

func (g *generator) countLogs(tenant string, n int) {
	g.window.logs[tenant] += int64(n)
	g.total.logs[tenant] += int64(n)
}

// fail records an export error. Messages are kept for the next report rather
// than logged per call: with the proxy down, every 100ms tick would fail.
func (g *generator) fail(err error) {
	g.window.failures++
	g.total.failures++
	g.lastErr = err.Error()
}

func (g *generator) report(label string) {
	c := g.window
	if label == "final" {
		c = g.total
	}
	var logs int64
	for _, v := range c.logs {
		logs += v
	}
	line := fmt.Sprintf("loadgen: %s logs=%d (%s=%d acme=%d initech=%d) spans=%d export_errors=%d",
		label, logs, noisyTenant, c.logs[noisyTenant], c.logs["acme"], c.logs["initech"], c.spans, c.failures)
	if c.failures > 0 {
		line += " last_error=" + strconv.Quote(g.lastErr)
	}
	log.Print(line)
	g.window = newCounts()
}

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil || f < 0 {
		log.Fatalf("loadgen: %s=%q is not a non-negative number", key, v)
	}
	return f
}
