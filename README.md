# go-concurrent-crawler

A small, concurrent, depth-bounded web crawler written in Go. It starts from
one seed URL, fetches pages with a fixed pool of worker goroutines, follows
links up to a configurable depth, and reports basic crawl metrics — all with
graceful shutdown on `Ctrl+C`.

It's intentionally minimal: no database, no distributed queue, no external
frameworks beyond the standard library and `golang.org/x/net/html`. The point
is to be a clean, well-tested example of Go's concurrency primitives used for
a real (if small) purpose. See [`docs/CONCEPTS.md`](docs/CONCEPTS.md) for a
detailed walkthrough of every concept and pattern used here, aimed at
developers new to Go and preparing for SDE interviews.

## Project layout

```
cmd/crawler/            entrypoint: flags, logger, signal handling, wiring
internal/crawler/        worker pool, job queue, dedup, metrics, orchestration
internal/fetcher/         HTTP fetch, behind an interface for testability
internal/linkextractor/   HTML parsing -> absolute links, behind an interface
docs/CONCEPTS.md          concept & system design deep-dive
```

`internal/` packages are not importable outside this module — that's enforced
by the Go compiler, not just convention.

## Running it

```bash
go run ./cmd/crawler -url=https://go.dev -max-depth=1 -workers=5
```

Flags (all optional):

| Flag           | Default            | Meaning                                   |
|----------------|--------------------|--------------------------------------------|
| `-url`         | `https://go.dev`   | Seed URL to start crawling from            |
| `-max-depth`   | `1`                | Max link hops to follow from the seed      |
| `-workers`     | `5`                | Number of concurrent crawl workers         |
| `-rate-limit`  | `200ms`            | Minimum interval between requests          |
| `-timeout`     | `15s`              | Per-request HTTP timeout                   |

Press `Ctrl+C` to stop early; the crawler shuts down gracefully and still
prints the metrics gathered so far.

## Building and testing

```bash
go build ./...
go vet ./...
go test ./... -race -v
```

Tests run against in-process `httptest` servers (no real network calls), so
they're fast and deterministic.
