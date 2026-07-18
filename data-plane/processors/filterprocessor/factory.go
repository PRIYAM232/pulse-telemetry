package filterprocessor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.uber.org/zap"
)

// Type is the component type users reference in the collector YAML, e.g.
//
//	processors:
//	  pulse_filter:
//
// MustNewType panics on invalid identifiers at init time, which is exactly
// when we want to find out.
var Type = component.MustNewType("pulse_filter")

// Stability per signal. Development keeps us honest until the rule engine is
// real and benchmarked.
const (
	tracesStability  = component.StabilityLevelDevelopment
	logsStability    = component.StabilityLevelDevelopment
	metricsStability = component.StabilityLevelDevelopment
)

// sharedEngines hands every signal processor created under one component ID
// (e.g. `pulse_filter`, `pulse_filter/edge`) the same ruleEngine, so a
// collector running pulse_filter in both a traces and a logs pipeline polls
// the policy endpoint once and sends one combined heartbeat. Engines are
// kept across collector config reloads (refcount drops to zero, then climbs
// back) and adopt the freshly unmarshaled Config while idle.
type sharedEngines struct {
	mu      sync.Mutex
	engines map[component.ID]*ruleEngine
}

func (s *sharedEngines) getOrCreate(set processor.Settings, cfg *Config) *ruleEngine {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.engines[set.ID]; ok {
		e.adoptConfigIfIdle(cfg)
		return e
	}
	e := newRuleEngine(set.Logger, cfg)
	// Self-metrics registration is best-effort: losing the Prometheus counters
	// degrades observability, not the pipeline, so a registration failure must
	// never block component creation (same stance the engine takes toward a
	// flaky control plane).
	tel, err := newEngineTelemetry(set.TelemetrySettings, func() int64 {
		return int64(len(e.snapshotRules()))
	})
	if err != nil {
		set.Logger.Warn("pulse_filter self-metrics disabled: instrument registration failed", zap.Error(err))
	} else if tel != nil {
		e.telemetry = tel
	}
	s.engines[set.ID] = e
	return e
}

// NewFactory registers the Pulse filter processor with the collector.
// ocb's generated components.go calls this exactly once at startup.
func NewFactory() processor.Factory {
	shared := &sharedEngines{engines: make(map[component.ID]*ruleEngine)}
	return processor.NewFactory(
		Type,
		createDefaultConfig,
		processor.WithTraces(shared.createTraces, tracesStability),
		processor.WithLogs(shared.createLogs, logsStability),
		processor.WithMetrics(shared.createMetrics, metricsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		LogSpanDetails: false,
		// Sync and stats are opt-in: leaving both endpoints empty keeps the
		// processor in static pass-through mode. 30s is a sane default poll
		// cadence once an endpoint is configured; stats report faster so the
		// dashboard's savings numbers track close to real time.
		SyncInterval:  30 * time.Second,
		StatsInterval: 10 * time.Second,
	}
}

func (s *sharedEngines) createTraces(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Traces,
) (processor.Traces, error) {
	pCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid configuration type: expected *filterprocessor.Config, got %T", cfg)
	}
	return newTracesProcessor(set, pCfg, s.getOrCreate(set, pCfg), next), nil
}

func (s *sharedEngines) createLogs(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Logs,
) (processor.Logs, error) {
	pCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid configuration type: expected *filterprocessor.Config, got %T", cfg)
	}
	return newLogsProcessor(set, pCfg, s.getOrCreate(set, pCfg), next), nil
}

func (s *sharedEngines) createMetrics(
	_ context.Context,
	set processor.Settings,
	cfg component.Config,
	next consumer.Metrics,
) (processor.Metrics, error) {
	pCfg, ok := cfg.(*Config)
	if !ok {
		return nil, fmt.Errorf("invalid configuration type: expected *filterprocessor.Config, got %T", cfg)
	}
	return newMetricsProcessor(set, pCfg, s.getOrCreate(set, pCfg), next), nil
}
