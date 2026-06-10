package main

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/html"
)

type CrawlJob struct {
	URL   string
	Depth int
}

type Metrics struct {
	PagesCrawled int
	PagesFailed  int
	TotalLatency time.Duration
}

type Crawler struct {
	MaxDepth int
	Workers  int

	Visited map[string]bool
	Mu      sync.Mutex

	Metrics Metrics

	Jobs chan CrawlJob
	Wg   sync.WaitGroup

	RateLimiter *time.Ticker

	Client *http.Client
}

func main() {
	crawler := &Crawler{
		MaxDepth: 1,
		Workers:  5,
		Visited: make(map[string]bool),
		Jobs:    make(chan CrawlJob, 100),

		RateLimiter: time.NewTicker(200 * time.Millisecond),

		Client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
	defer crawler.RateLimiter.Stop()

	startURL := "https://go.dev"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go listenForShutdown(cancel)

	crawler.Start(ctx, startURL)

	fmt.Println("Crawl finished")
	crawler.printStats()
}

func listenForShutdown(cancel context.CancelFunc) {
	signalChannel := make(chan os.Signal, 1)

	signal.Notify(signalChannel, os.Interrupt, syscall.SIGTERM)

	<-signalChannel

	fmt.Println()
	fmt.Println("Shutdown signal received. Stopping crawler...")

	cancel()
}

func (c *Crawler) Start(ctx context.Context, startURL string) {
	for i := 1; i <= c.Workers; i++ {
		go c.worker(ctx, i)
	}

	c.Wg.Add(1)

	select {
	case <-ctx.Done():
		return
	case c.Jobs <- CrawlJob{
		URL:   startURL,
		Depth: 0,
	}:
	}

	c.Wg.Wait()

	close(c.Jobs)
}

func (c *Crawler) worker(ctx context.Context, id int) {
	for {
		select {
		case <-ctx.Done():
			fmt.Println("Worker", id, "stopping because crawler was cancelled")
			return

		case job, ok := <-c.Jobs:
			if !ok {
				return
			}

			fmt.Println("Worker", id, "received job:", job.URL, "depth:", job.Depth)

			c.crawl(ctx, job, id)

			c.Wg.Done()
		}
	}
}

func (c *Crawler) crawl(ctx context.Context, job CrawlJob, workerID int) {
	if job.Depth > c.MaxDepth {
		return
	}

	if !c.markVisited(job.URL) {
		fmt.Println("Already visited, skipping:", job.URL)
		return
	}

	select {
	case <-ctx.Done():
		fmt.Println("Crawl cancelled before fetching:", job.URL)
		return
	case <-c.RateLimiter.C:
	}

	fmt.Println("Worker", workerID, "crawling:", job.URL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, job.URL, nil)
	if err != nil {
		fmt.Println("Error creating request:", err)
		c.recordFailure()
		return
	}

	startTime := time.Now()

	resp, err := c.Client.Do(req)
	if err != nil {
		fmt.Println("Error fetching page:", err)
		c.recordFailure()
		return
	}
	defer resp.Body.Close()

	latency := time.Since(startTime)

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Skipping page. Status code:", resp.StatusCode)
		c.recordFailure()
		return
	}

	c.recordSuccess(latency)

	links := extractLinks(resp, job.URL)

	fmt.Println("Found", len(links), "links on", job.URL)

	for _, link := range links {
		select {
		case <-ctx.Done():
			fmt.Println("Crawl cancelled while adding links")
			return
		default:
		}

		c.Mu.Lock()
		alreadyVisited := c.Visited[link]
		c.Mu.Unlock()

		if !alreadyVisited && job.Depth+1 <= c.MaxDepth {
			c.Wg.Add(1)

			select {
			case <-ctx.Done():
				c.Wg.Done()
				return

			case c.Jobs <- CrawlJob{
				URL:   link,
				Depth: job.Depth + 1,
			}:
			}
		}
	}
}

func (c *Crawler) markVisited(pageURL string) bool {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if c.Visited[pageURL] {
		return false
	}

	c.Visited[pageURL] = true
	return true
}

func (c *Crawler) recordSuccess(latency time.Duration) {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	c.Metrics.PagesCrawled++
	c.Metrics.TotalLatency += latency
}

func (c *Crawler) recordFailure() {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	c.Metrics.PagesFailed++
}

func (c *Crawler) printStats() {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	fmt.Println("Total unique pages visited:", len(c.Visited))
	fmt.Println("Pages crawled successfully:", c.Metrics.PagesCrawled)
	fmt.Println("Pages failed:", c.Metrics.PagesFailed)

	if c.Metrics.PagesCrawled > 0 {
		averageLatency := c.Metrics.TotalLatency / time.Duration(c.Metrics.PagesCrawled)
		fmt.Println("Average latency:", averageLatency)
	} else {
		fmt.Println("Average latency: N/A")
	}
}

func extractLinks(resp *http.Response, baseURL string) []string {
	var links []string

	doc, err := html.Parse(resp.Body)
	if err != nil {
		fmt.Println("Error parsing HTML:", err)
		return links
	}

	var visitNode func(*html.Node)

	visitNode = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					absoluteLink, ok := normalizeURL(attr.Val, baseURL)
					if ok {
						links = append(links, absoluteLink)
					}
				}
			}
		}

		for child := n.FirstChild; child != nil; child = child.NextSibling {
			visitNode(child)
		}
	}

	visitNode(doc)

	return links
}

func normalizeURL(link string, baseURL string) (string, bool) {
	parsedBaseURL, err := url.Parse(baseURL)
	if err != nil {
		return "", false
	}

	parsedLink, err := url.Parse(link)
	if err != nil {
		return "", false
	}

	absoluteURL := parsedBaseURL.ResolveReference(parsedLink)

	if absoluteURL.Scheme != "http" && absoluteURL.Scheme != "https" {
		return "", false
	}

	absoluteURL.Fragment = ""

	return absoluteURL.String(), true
}