package crawler

import (
	"sync/atomic"
	"time"
)

// Metrics accumulates crawl statistics using lock-free atomic counters, so
// recording a result never contends with the visitedSet's lock.
type Metrics struct {
	pagesCrawled   atomic.Int64
	pagesFailed    atomic.Int64
	totalLatencyNs atomic.Int64
}

// Snapshot is a point-in-time, read-only view of Metrics.
type Snapshot struct {
	PagesCrawled   int64
	PagesFailed    int64
	AverageLatency time.Duration
}

// RecordSuccess records a successfully fetched page and how long it took.
func (m *Metrics) RecordSuccess(latency time.Duration) {
	m.pagesCrawled.Add(1)
	m.totalLatencyNs.Add(latency.Nanoseconds())
}

// RecordFailure records a page that could not be fetched or returned a
// non-2xx status.
func (m *Metrics) RecordFailure() {
	m.pagesFailed.Add(1)
}

// Snapshot returns the current metric values. AverageLatency is 0 when no
// pages have been crawled successfully yet.
func (m *Metrics) Snapshot() Snapshot {
	crawled := m.pagesCrawled.Load()

	var avg time.Duration
	if crawled > 0 {
		avg = time.Duration(m.totalLatencyNs.Load() / crawled)
	}

	return Snapshot{
		PagesCrawled:   crawled,
		PagesFailed:    m.pagesFailed.Load(),
		AverageLatency: avg,
	}
}
