// Package store persists crawled pages and serves full-text search over them.
package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"
)

// Page is one stored document.
type Page struct {
	URLHash      string
	CrawlID      int64
	URL          string
	StatusCode   int
	ContentType  string
	Title        string
	Description  string
	H1           string
	Snippet      string
	ContentHash  string
	ETag         string
	LastModified string
	SizeBytes    int
	Latency      time.Duration
}

// Hit is a search result.
type Hit struct {
	URL         string  `json:"url"`
	Title       string  `json:"title"`
	Description string  `json:"description"`
	Snippet     string  `json:"snippet"`
	Rank        float64 `json:"rank"`
}

// Validators are the cache validators saved from the last fetch of a URL.
type Validators struct {
	ETag         string
	LastModified string
}

// Store saves pages and searches them.
type Store interface {
	// Save durably stores p; it returns after the write is committed, so a
	// caller can safely mark the URL done afterwards.
	Save(ctx context.Context, p Page) error
	// Validators returns the validators from the last stored fetch of a URL.
	Validators(ctx context.Context, urlHash string) (Validators, bool, error)
	// Search returns up to limit pages matching q, best first.
	Search(ctx context.Context, q string, limit int) ([]Hit, error)
	// Close flushes and releases resources.
	Close() error
}

// ContentHash is the dedupe fingerprint of a page body.
func ContentHash(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
