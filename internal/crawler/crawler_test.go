package crawler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/wanderingtilldeath/go-concurrent-crawler/internal/fetcher"
	"github.com/wanderingtilldeath/go-concurrent-crawler/internal/linkextractor"
)

func testConfig() Config {
	return Config{
		MaxDepth:  1,
		Workers:   4,
		RateLimit: time.Millisecond,
		QueueSize: 16,
	}
}

func newTestCrawler(cfg Config) *Crawler {
	return New(cfg, fetcher.New(2*time.Second), linkextractor.HTMLExtractor{}, discardLogger())
}

// linkGraphServer serves a tiny, fixed site: "/" links to "/a" and "/b",
// "/a" links to "/c" (two hops from "/"), "/b" has no outgoing links, and
// "/missing" (never linked to) returns 404. It also counts hits per path so
// tests can assert a page was fetched exactly once.
func linkGraphServer(t *testing.T) (*httptest.Server, *hitCounter) {
	t.Helper()

	hits := newHitCounter()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		hits.record("/")
		w.Write([]byte(`<a href="/a">a</a><a href="/b">b</a>`))
	})
	mux.HandleFunc("/a", func(w http.ResponseWriter, r *http.Request) {
		hits.record("/a")
		w.Write([]byte(`<a href="/c">c</a>`))
	})
	mux.HandleFunc("/b", func(w http.ResponseWriter, r *http.Request) {
		hits.record("/b")
		w.Write([]byte(`no links here`))
	})
	mux.HandleFunc("/c", func(w http.ResponseWriter, r *http.Request) {
		hits.record("/c")
		w.Write([]byte(`should never be fetched at depth 1`))
	})
	mux.HandleFunc("/missing", func(w http.ResponseWriter, r *http.Request) {
		hits.record("/missing")
		w.WriteHeader(http.StatusNotFound)
	})

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	return server, hits
}

func TestCrawler_RespectsMaxDepth(t *testing.T) {
	server, hits := linkGraphServer(t)

	c := newTestCrawler(testConfig()) // MaxDepth: 1
	c.Start(context.Background(), server.URL+"/")

	if hits.count("/") != 1 {
		t.Errorf("hits on / = %d, want 1", hits.count("/"))
	}
	if hits.count("/a") != 1 {
		t.Errorf("hits on /a = %d, want 1", hits.count("/a"))
	}
	if hits.count("/b") != 1 {
		t.Errorf("hits on /b = %d, want 1", hits.count("/b"))
	}
	if hits.count("/c") != 0 {
		t.Errorf("hits on /c = %d, want 0 (it is two hops from the seed, beyond MaxDepth)", hits.count("/c"))
	}
}

func TestCrawler_DeduplicatesCycles(t *testing.T) {
	hits := newHitCounter()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		hits.record("/")
		w.Write([]byte(`<a href="/loop">loop</a>`))
	})
	mux.HandleFunc("/loop", func(w http.ResponseWriter, r *http.Request) {
		hits.record("/loop")
		w.Write([]byte(`<a href="/">back to start</a>`))
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	cfg := testConfig()
	cfg.MaxDepth = 5 // generous depth; dedup, not depth, must stop the cycle
	c := newTestCrawler(cfg)
	c.Start(context.Background(), server.URL+"/")

	if hits.count("/") != 1 {
		t.Errorf("hits on / = %d, want 1 (cyclic link must not be re-fetched)", hits.count("/"))
	}
	if hits.count("/loop") != 1 {
		t.Errorf("hits on /loop = %d, want 1", hits.count("/loop"))
	}
}

func TestCrawler_RecordsFailureOnNon200(t *testing.T) {
	hits := newHitCounter()

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		hits.record("/")
		w.Write([]byte(`<a href="/missing">missing</a>`))
	})
	mux.HandleFunc("/missing", func(w http.ResponseWriter, r *http.Request) {
		hits.record("/missing")
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	c := newTestCrawler(testConfig())
	c.Start(context.Background(), server.URL+"/")

	snap := c.Metrics()
	if snap.PagesCrawled != 1 {
		t.Errorf("PagesCrawled = %d, want 1 (only the seed page succeeds)", snap.PagesCrawled)
	}
	if snap.PagesFailed != 1 {
		t.Errorf("PagesFailed = %d, want 1 (the 404 page)", snap.PagesFailed)
	}
}

func TestCrawler_ContextCancellationStopsPromptly(t *testing.T) {
	block := make(chan struct{})

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		<-block // hang until the test closes it, simulating a stuck request
	})
	server := httptest.NewServer(mux)
	// Cleanups run LIFO: unblock the handler first, only then let
	// server.Close() wait for the (now finishing) in-flight request.
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(block) })

	c := newTestCrawler(testConfig())

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		c.Start(ctx, server.URL+"/")
		close(done)
	}()

	time.Sleep(20 * time.Millisecond) // let the worker start the blocking request
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Start() did not return promptly after context cancellation")
	}
}

// --- test helpers ---

type hitCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func newHitCounter() *hitCounter {
	return &hitCounter{counts: make(map[string]int)}
}

func (h *hitCounter) record(path string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.counts[path]++
}

func (h *hitCounter) count(path string) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.counts[path]
}
