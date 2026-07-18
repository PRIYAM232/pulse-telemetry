package filterprocessor

import (
	"sync"

	"golang.org/x/time/rate"
)

// Token-bucket state backing the THROTTLE action.
//
// Each enforced THROTTLE rule owns one limiterGroup: a bounded, thread-safe
// map from group key (the value of the rule's throttleGroupBy attribute, e.g.
// one tenant_id) to its *rate.Limiter. Groups are created by compileRules at
// ruleset-publication time, so — like the compiled regexes — the hot paths
// share them without recomputing anything per record.
//
// Note the consequence: a ruleset CHANGE recreates every group, so all
// buckets restart full. A momentary re-burst after an operator edits any rule
// is the price of keeping limiter state inside the immutable compiled rules
// instead of a cross-sync registry; at Phase 8 rates that is invisible.

const (
	// throttleDefaultKey is the bucket used when the record does not carry
	// the rule's throttleGroupBy attribute at all — and, for rules with no
	// throttleGroupBy, the single shared bucket that makes the rule a global
	// rate limit.
	throttleDefaultKey = "default"

	// maxThrottleKeysPerGeneration bounds one generation of a group's key
	// map; total retention is at most twice this (see limiterGroup). 4096
	// distinct group values per rule per generation covers any sane tenant
	// or service cardinality; beyond it we would rather recycle buckets than
	// grow without bound.
	maxThrottleKeysPerGeneration = 4096

	// maxThrottleKeyBytes truncates pathological group values (a group-by
	// pointed at a free-text attribute) so an evicted-and-recreated map can
	// never retain megabytes of key strings either.
	maxThrottleKeyBytes = 256
)

// limiterGroup is the per-rule bucket map. Memory is bounded by generational
// eviction rather than a classic LRU: lookups hit `cur` under the read lock
// only; a miss promotes from `prev` (or creates) under the write lock; and
// when `cur` fills up it becomes `prev` and the old `prev` — keys not touched
// for a full generation — is dropped wholesale.
//
// This trades exact recency order for a hot path with zero mutation: a true
// LRU must splice its recency list on EVERY Allow, which means every record
// takes the write lock — precisely the bottleneck the RWMutex pattern is
// supposed to avoid. Keys evicted here lose their token state and restart
// with a full burst if seen again; that only happens under group-key churn
// past thousands of distinct values, which is the memory-exhaustion case this
// structure exists to survive.
type limiterGroup struct {
	limit rate.Limit
	burst int

	mu   sync.RWMutex
	cur  map[string]*rate.Limiter
	prev map[string]*rate.Limiter
}

// newLimiterGroup builds the group for one THROTTLE rule. Burst equals the
// rate — one second's worth of headroom — so short spikes inside a healthy
// second pass while sustained overload clamps to eventsPerSec. Callers
// guarantee eventsPerSec > 0 (ruleApplies* skips rate-less rules).
func newLimiterGroup(eventsPerSec int) *limiterGroup {
	return &limiterGroup{
		limit: rate.Limit(eventsPerSec),
		burst: eventsPerSec,
		cur:   make(map[string]*rate.Limiter),
	}
}

// allow reports whether one event for the given group key fits under the
// rule's rate right now, consuming a token if so. Safe for concurrent use by
// every consume hot path: the common case (key already hot) is one RLock'd
// map read, and rate.Limiter itself is internally synchronized, so Allow
// runs after our locks are released.
func (g *limiterGroup) allow(key string) bool {
	if len(key) > maxThrottleKeyBytes {
		key = key[:maxThrottleKeyBytes]
	}

	g.mu.RLock()
	l := g.cur[key]
	g.mu.RUnlock()
	if l == nil {
		l = g.getOrCreate(key)
	}
	return l.Allow()
}

// getOrCreate is the slow path behind allow: first sighting of a key (per
// generation) takes the write lock once, then the key serves from the read
// path until evicted. Re-checks cur under the lock because two goroutines can
// race the same miss — both must end up with the SAME limiter or the group
// would briefly grant double the rate.
func (g *limiterGroup) getOrCreate(key string) *rate.Limiter {
	g.mu.Lock()
	defer g.mu.Unlock()

	if l, ok := g.cur[key]; ok {
		return l
	}

	// Promote from the previous generation if the key was recently active —
	// this carries the token state over, so a steadily-throttled group is not
	// handed a fresh burst just because the map rotated underneath it.
	l, ok := g.prev[key]
	if !ok {
		l = rate.NewLimiter(g.limit, g.burst)
	}

	if len(g.cur) >= maxThrottleKeysPerGeneration {
		g.prev = g.cur
		g.cur = make(map[string]*rate.Limiter)
	}
	g.cur[key] = l
	return l
}
