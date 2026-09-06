package crawler

import "sync"

// visitedSet is a concurrency-safe set of URLs already claimed by a worker.
// It exists as its own tiny type so its lock only ever guards this map,
// independent of any lock metrics collection needs.
type visitedSet struct {
	mu   sync.Mutex
	seen map[string]bool
}

func newVisitedSet() *visitedSet {
	return &visitedSet{seen: make(map[string]bool)}
}

// markVisited atomically checks whether url has been seen before and, if
// not, marks it seen. It returns true only for the caller that wins the
// race to visit url first, so a URL is ever crawled exactly once.
func (v *visitedSet) markVisited(url string) bool {
	v.mu.Lock()
	defer v.mu.Unlock()

	if v.seen[url] {
		return false
	}

	v.seen[url] = true
	return true
}

func (v *visitedSet) count() int {
	v.mu.Lock()
	defer v.mu.Unlock()

	return len(v.seen)
}
