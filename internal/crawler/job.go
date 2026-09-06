package crawler

// Job is a single unit of crawl work: fetch URL, which was discovered at
// the given link Depth from the seed URL (the seed itself is Depth 0).
type Job struct {
	URL   string
	Depth int
}
