package filterprocessor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// Virtual condition field for metrics: rules whose condition_field is this
// name match against the metric's own name (e.g. "http.server.duration")
// instead of an attribute. Namespaced under "metric." so it can never shadow
// a real attribute key by accident — the same convention as log.severity and
// log.body on the logs side.
const fieldMetricName = "metric.name"

// metricsProcessor filters metrics against the dynamic ruleset held by the
// shared ruleEngine — the SAME engine instance the traces and logs processors
// use, so one collector polls the policy endpoint once and sends one combined
// heartbeat no matter how many signal pipelines reference pulse_filter.
//
// Scope: DROP (Phase 6) and ROUTE (Phase 7), evaluated per Metric (the name +
// datapoints unit). REDACT and SAMPLE have no metrics semantics yet; see
// ruleAppliesMetrics. The hot-path rules from the other signals apply
// unchanged: no heap allocation per batch in the common path, snapshotRules
// holds the read lock only long enough to copy the slice header.
type metricsProcessor struct {
	cfg    *Config
	logger *zap.Logger
	next   consumer.Metrics
	engine *ruleEngine
}

// Compile-time guarantee that we satisfy the full processor contract:
// component.Component (Start/Shutdown) + consumer.Metrics (Capabilities/ConsumeMetrics).
var _ processor.Metrics = (*metricsProcessor)(nil)

func newMetricsProcessor(set processor.Settings, cfg *Config, engine *ruleEngine, next consumer.Metrics) *metricsProcessor {
	return &metricsProcessor{
		cfg:    cfg,
		logger: set.Logger,
		next:   next,
		engine: engine,
	}
}

// Start/Shutdown delegate to the refcounted engine: the first signal
// processor to start spawns the control-plane goroutines, the last one to
// shut down tears them down.
func (p *metricsProcessor) Start(_ context.Context, _ component.Host) error {
	p.engine.start()
	return nil
}

func (p *metricsProcessor) Shutdown(_ context.Context) error {
	p.engine.stop()
	p.logger.Info("pulse_filter metrics processor stopped")
	return nil
}

// Capabilities: we drop metrics from the batches we forward and stamp
// resource attributes in place (ROUTE), so downstream fan-out consumers must
// not assume the data is shared — MutatesData is true.
func (p *metricsProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// ConsumeMetrics evaluates the current ruleset against every metric and
// forwards whatever survives. Counts are whole Metric objects (one name +
// its datapoints), matching the granularity DROP operates at. Fully-dropped
// batches are consumed here (returning nil acknowledges the data) rather
// than forwarding an empty container.
func (p *metricsProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	rules := p.engine.snapshotRules()

	metricsIn := md.MetricCount()

	if len(rules) > 0 {
		p.applyRules(rules, md)
	}

	metricsOut := md.MetricCount()

	// Feeds both the stats heartbeat and the collector's self-metrics; safe on
	// every batch, concurrently across receivers.
	p.engine.observeMetrics(ctx, int64(metricsIn), int64(metricsIn-metricsOut))

	p.logger.Debug("pulse_filter processed metric batch",
		zap.Int("metrics_in", metricsIn),
		zap.Int("metrics_dropped", metricsIn-metricsOut),
		zap.Int("metrics_out", metricsOut),
	)

	if metricsOut == 0 {
		return nil
	}
	return p.next.ConsumeMetrics(ctx, md)
}

// applyRules removes every metric matched by an enforced DROP rule, then
// prunes scope/resource containers left empty so downstream components never
// see hollow envelopes.
func (p *metricsProcessor) applyRules(rules []compiledRule, md pmetric.Metrics) {
	md.ResourceMetrics().RemoveIf(func(rm pmetric.ResourceMetrics) bool {
		resAttrs := rm.Resource().Attributes()
		rm.ScopeMetrics().RemoveIf(func(sm pmetric.ScopeMetrics) bool {
			sm.Metrics().RemoveIf(func(metric pmetric.Metric) bool {
				return p.evaluateMetric(rules, metric, resAttrs)
			})
			return sm.Metrics().Len() == 0
		})
		return rm.ScopeMetrics().Len() == 0
	})
}

// evaluateMetric runs every enforced rule against one metric and reports
// whether the metric should be dropped. ROUTE rules stamp the enclosing
// resource as a side effect but never drop; a ROUTE tag does not shield the
// metric from a later DROP rule.
func (p *metricsProcessor) evaluateMetric(rules []compiledRule, metric pmetric.Metric, resAttrs pcommon.Map) bool {
	for i := range rules {
		rule := &rules[i]
		if !ruleAppliesMetrics(rule) {
			continue
		}
		if !metricConditionMatches(rule, metric, resAttrs) {
			continue
		}

		switch rule.ActionType {
		case ActionDrop:
			p.logger.Debug("pulse_filter dropping metric",
				zap.String("rule", rule.Name),
				zap.String("metric_name", metric.Name()),
			)
			return true
		case ActionRoute:
			// Stamp the RESOURCE, not the metric: the routing connector
			// matches at resource granularity (see attrRoutingDestination).
			// Every metric under this resource inherits the destination;
			// later ROUTE matches overwrite it, and later rules may still
			// drop the metric.
			resAttrs.PutStr(attrRoutingDestination, rule.TargetDestination)
			p.logger.Debug("pulse_filter tagged metric resource for routing",
				zap.String("rule", rule.Name),
				zap.String("destination", rule.TargetDestination),
				zap.String("metric_name", metric.Name()),
			)
		case ActionThrottle:
			// One token per Metric (name + datapoints unit) from the group's
			// bucket; the excess over the rule's rate is dropped — the same
			// granularity DROP operates at.
			if !rule.limiters.allow(throttleKeyMetric(rule, metric, resAttrs)) {
				p.logger.Debug("pulse_filter dropping metric (throttle rate exceeded)",
					zap.String("rule", rule.Name),
					zap.String("metric_name", metric.Name()),
				)
				return true
			}
			// Under the rate; later rules may still drop the metric.
		}
	}
	return false
}

// metricConditionMatches resolves the rule's condition field on a metric and
// applies the operator. Resolution order: the virtual metric.name field
// first, then the enclosing resource's attributes — so fleets can drop by
// metric name (the headline use case) or by resource identity (e.g.
// service.name EQUALS "staging-worker"). Datapoint-level attributes are out
// of scope until a phase defines whether a partial datapoint match drops the
// whole metric. A missing field never matches (including EXISTS, which is
// true exactly when the field is present and non-empty).
func metricConditionMatches(rule *compiledRule, metric pmetric.Metric, resAttrs pcommon.Map) bool {
	if rule.ConditionField == fieldMetricName {
		// Metric names are never empty in valid OTLP; guard anyway so EXISTS
		// cannot match a nameless metric from a buggy producer.
		name := metric.Name()
		return name != "" && matchConditionString(rule, name)
	}

	val, ok := resAttrs.Get(rule.ConditionField)
	if !ok {
		return false
	}
	return matchConditionValue(rule, val)
}

// throttleKeyMetric resolves the THROTTLE group key for one metric, honoring
// the same virtual field metricConditionMatches does: group_by "metric.name"
// gives every metric name its own bucket (the natural metrics partition — cap
// each series family, not the whole pipeline), anything else resolves on the
// enclosing resource. Missing values share the default bucket, so a nameless
// or attribute-less producer cannot dodge the limit.
func throttleKeyMetric(rule *compiledRule, metric pmetric.Metric, resAttrs pcommon.Map) string {
	switch rule.ThrottleGroupBy {
	case "":
		return throttleDefaultKey
	case fieldMetricName:
		if name := metric.Name(); name != "" {
			return name
		}
		return throttleDefaultKey
	}
	if v, ok := resAttrs.Get(rule.ThrottleGroupBy); ok {
		return v.AsString()
	}
	return throttleDefaultKey
}
