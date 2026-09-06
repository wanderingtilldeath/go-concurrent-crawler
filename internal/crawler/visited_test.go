package crawler

import (
	"sync"
	"testing"
)

func TestVisitedSet_MarksOnlyOnce(t *testing.T) {
	v := newVisitedSet()

	if !v.markVisited("https://example.com") {
		t.Fatal("first markVisited() for a new URL should return true")
	}
	if v.markVisited("https://example.com") {
		t.Fatal("second markVisited() for the same URL should return false")
	}
	if v.count() != 1 {
		t.Fatalf("count() = %d, want 1", v.count())
	}
}

// TestVisitedSet_ConcurrentSameURL exercises the exact race the crawler
// relies on: many goroutines racing to claim the same URL must produce
// exactly one winner. Run with -race to also verify the map access itself
// is properly synchronized.
func TestVisitedSet_ConcurrentSameURL(t *testing.T) {
	v := newVisitedSet()

	const goroutines = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			if v.markVisited("https://example.com/same") {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if wins != 1 {
		t.Fatalf("exactly one goroutine should win markVisited(), got %d", wins)
	}
}
