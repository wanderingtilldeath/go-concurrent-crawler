// Command crawler runs a concurrent, depth-bounded web crawler starting
// from a single seed URL.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/wanderingtilldeath/go-concurrent-crawler/internal/crawler"
	"github.com/wanderingtilldeath/go-concurrent-crawler/internal/fetcher"
	"github.com/wanderingtilldeath/go-concurrent-crawler/internal/linkextractor"
)

func main() {
	var (
		startURL  = flag.String("url", "https://go.dev", "seed URL to start crawling from")
		maxDepth  = flag.Int("max-depth", 1, "maximum link depth to follow from the seed URL")
		workers   = flag.Int("workers", 5, "number of concurrent crawl workers")
		rateLimit = flag.Duration("rate-limit", 200*time.Millisecond, "minimum interval between requests")
		timeout   = flag.Duration("timeout", 15*time.Second, "HTTP request timeout")
	)
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := crawler.Config{
		MaxDepth:  *maxDepth,
		Workers:   *workers,
		RateLimit: *rateLimit,
		QueueSize: 100,
	}
	c := crawler.New(cfg, fetcher.New(*timeout), linkextractor.HTMLExtractor{}, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go listenForShutdown(cancel, logger)

	c.Start(ctx, *startURL)

	snap := c.Metrics()
	logger.Info("crawl finished",
		"visited", c.VisitedCount(),
		"pages_crawled", snap.PagesCrawled,
		"pages_failed", snap.PagesFailed,
		"average_latency", snap.AverageLatency,
	)
}

func listenForShutdown(cancel context.CancelFunc, logger *slog.Logger) {
	signalChannel := make(chan os.Signal, 1)
	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	<-signalChannel

	logger.Info("shutdown signal received, stopping crawler")
	cancel()
}
