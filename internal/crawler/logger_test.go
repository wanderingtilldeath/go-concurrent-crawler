package crawler

import (
	"io"
	"log/slog"
)

// discardLogger returns a logger that swallows all output, keeping `go
// test` output focused on test results rather than crawl logs.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
