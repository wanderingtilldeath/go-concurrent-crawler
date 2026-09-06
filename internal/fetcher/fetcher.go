// Package fetcher isolates the network boundary of the crawler: making an
// HTTP GET request for a page. Keeping it behind an interface lets the
// crawler's tests substitute a fake instead of hitting the network.
package fetcher

import (
	"context"
	"net/http"
	"time"
)

// Fetcher retrieves a page over HTTP.
type Fetcher interface {
	Fetch(ctx context.Context, rawURL string) (*http.Response, error)
}

// HTTPFetcher is the production Fetcher, backed by a real *http.Client.
type HTTPFetcher struct {
	Client *http.Client
}

// New returns an HTTPFetcher whose client times out after timeout.
func New(timeout time.Duration) *HTTPFetcher {
	return &HTTPFetcher{
		Client: &http.Client{Timeout: timeout},
	}
}

// Fetch issues a GET request for rawURL, honoring ctx for cancellation.
// The caller is responsible for closing the response body.
func (f *HTTPFetcher) Fetch(ctx context.Context, rawURL string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}

	return f.Client.Do(req)
}
