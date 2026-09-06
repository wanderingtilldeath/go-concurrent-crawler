// Package crawler implements a concurrent, depth-bounded web crawler built
// around a fixed worker pool and a job channel. It knows nothing about
// flags, signals, or process lifecycle - those are the caller's job.
package crawler

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/wanderingtilldeath/go-concurrent-crawler/internal/fetcher"
	"github.com/wanderingtilldeath/go-concurrent-crawler/internal/linkextractor"
)

// Config controls crawl behavior.
type Config struct {
	// MaxDepth is the maximum number of link hops to follow from the seed
	// URL. The seed itself is depth 0.
	MaxDepth int
	// Workers is the number of goroutines concurrently processing jobs.
	Workers int
	// RateLimit is the minimum interval between requests, shared across
	// all workers.
	RateLimit time.Duration
	// QueueSize is the buffered capacity of the job channel; it bounds how
	// many discovered-but-not-yet-processed links can queue up before
	// producers block (backpressure).
	QueueSize int
}

// Crawler crawls a site breadth-first up to Config.MaxDepth, using an
// injected Fetcher and Extractor so it can be exercised in tests without
// real network or HTML parsing.
type Crawler struct {
	cfg       Config
	fetcher   fetcher.Fetcher
	extractor linkextractor.Extractor
	logger    *slog.Logger

	visited *visitedSet
	metrics *Metrics

	jobs chan Job
	wg   sync.WaitGroup

	rateLimiter *time.Ticker
}

// New builds a Crawler. logger may be nil, in which case slog.Default() is
// used.
func New(cfg Config, f fetcher.Fetcher, e linkextractor.Extractor, logger *slog.Logger) *Crawler {
	if logger == nil {
		logger = slog.Default()
	}

	return &Crawler{
		cfg:         cfg,
		fetcher:     f,
		extractor:   e,
		logger:      logger,
		visited:     newVisitedSet(),
		metrics:     &Metrics{},
		jobs:        make(chan Job, cfg.QueueSize),
		rateLimiter: time.NewTicker(cfg.RateLimit),
	}
}

// Metrics returns a snapshot of the crawl's metrics so far.
func (c *Crawler) Metrics() Snapshot {
	return c.metrics.Snapshot()
}

// VisitedCount returns the number of distinct URLs claimed by a worker so
// far.
func (c *Crawler) VisitedCount() int {
	return c.visited.count()
}

// Start launches the worker pool, seeds it with startURL, and blocks until
// every discovered job (within MaxDepth) has been processed or ctx is
// cancelled.
func (c *Crawler) Start(ctx context.Context, startURL string) {
	defer c.rateLimiter.Stop()

	for i := 1; i <= c.cfg.Workers; i++ {
		go c.worker(ctx, i)
	}

	c.wg.Add(1)
	select {
	case <-ctx.Done():
		return
	case c.jobs <- Job{URL: startURL, Depth: 0}:
	}

	c.wg.Wait()
	close(c.jobs)
}

func (c *Crawler) worker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("worker stopping: crawl cancelled", "worker", id)
			return

		case job, ok := <-c.jobs:
			if !ok {
				return
			}

			c.crawl(ctx, job, id)
			c.wg.Done()
		}
	}
}

func (c *Crawler) crawl(ctx context.Context, job Job, workerID int) {
	if job.Depth > c.cfg.MaxDepth {
		return
	}

	if !c.visited.markVisited(job.URL) {
		return
	}

	select {
	case <-ctx.Done():
		return
	case <-c.rateLimiter.C:
	}

	c.logger.Info("crawling", "worker", workerID, "url", job.URL, "depth", job.Depth)

	start := time.Now()
	resp, err := c.fetcher.Fetch(ctx, job.URL)
	if err != nil {
		c.logger.Warn("fetch failed", "url", job.URL, "error", err)
		c.metrics.RecordFailure()
		return
	}
	defer resp.Body.Close()
	latency := time.Since(start)

	if resp.StatusCode != http.StatusOK {
		c.logger.Warn("unexpected status", "url", job.URL, "status", resp.StatusCode)
		c.metrics.RecordFailure()
		return
	}
	c.metrics.RecordSuccess(latency)

	links, err := c.extractor.Extract(resp.Body, job.URL)
	if err != nil {
		c.logger.Warn("failed to parse page", "url", job.URL, "error", err)
		return
	}

	c.logger.Info("found links", "url", job.URL, "count", len(links))

	if job.Depth+1 > c.cfg.MaxDepth {
		return
	}

	for _, link := range links {
		select {
		case <-ctx.Done():
			return
		default:
		}

		c.wg.Add(1)
		select {
		case <-ctx.Done():
			c.wg.Done()
			return
		case c.jobs <- Job{URL: link, Depth: job.Depth + 1}:
		}
	}
}
