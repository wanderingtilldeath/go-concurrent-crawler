package crawler

import (
	"testing"
	"time"
)

func TestMetrics_SnapshotWithNoData(t *testing.T) {
	m := &Metrics{}

	snap := m.Snapshot()
	if snap.PagesCrawled != 0 || snap.PagesFailed != 0 || snap.AverageLatency != 0 {
		t.Fatalf("Snapshot() on empty Metrics = %+v, want all zero", snap)
	}
}

func TestMetrics_RecordAndSnapshot(t *testing.T) {
	m := &Metrics{}

	m.RecordSuccess(100 * time.Millisecond)
	m.RecordSuccess(200 * time.Millisecond)
	m.RecordFailure()

	snap := m.Snapshot()

	if snap.PagesCrawled != 2 {
		t.Errorf("PagesCrawled = %d, want 2", snap.PagesCrawled)
	}
	if snap.PagesFailed != 1 {
		t.Errorf("PagesFailed = %d, want 1", snap.PagesFailed)
	}
	if want := 150 * time.Millisecond; snap.AverageLatency != want {
		t.Errorf("AverageLatency = %v, want %v", snap.AverageLatency, want)
	}
}
