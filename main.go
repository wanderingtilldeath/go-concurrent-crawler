package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
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

type RobotsRules struct {
	DisallowPaths []string
}

type PageResult struct {
	URL        string
	StatusCode int
	Title      string
	PageSize   int
	Latency    time.Duration
}

type Crawler struct {
	MaxDepth   int
	Workers    int
	MaxRetries int

	Visited     map[string]bool
	RobotsCache map[string]RobotsRules

	Mu      sync.Mutex
	Metrics Metrics

	Jobs chan CrawlJob
	Wg   sync.WaitGroup

	RateLimiter *time.Ticker
	Client      *http.Client
	DB          *pgxpool.Pool
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dbURL := "postgres://postgres:postgres@localhost:5432/crawlerdb?sslmode=disable"

	db, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		fmt.Println("Error connecting to PostgreSQL:", err)
		return
	}
	defer db.Close()

	if err := setupDatabase(ctx, db); err != nil {
		fmt.Println("Error setting up database:", err)
		return
	}

	crawler := &Crawler{
		MaxDepth:   1,
		Workers:    5,
		MaxRetries: 3,

		Visited:     make(map[string]bool),
		RobotsCache: make(map[string]RobotsRules),

		Jobs: make(chan CrawlJob, 100),

		RateLimiter: time.NewTicker(200 * time.Millisecond),

		Client: &http.Client{
			Timeout: 15 * time.Second,
		},

		DB: db,
	}
	defer crawler.RateLimiter.Stop()

	startURL := "https://go.dev"

	go listenForShutdown(cancel)

	crawler.Start(ctx, startURL)

	fmt.Println("Crawl finished")
	crawler.printStats()
}

func setupDatabase(ctx context.Context, db *pgxpool.Pool) error {
	query := `
	CREATE TABLE IF NOT EXISTS crawled_pages (
		id BIGSERIAL PRIMARY KEY,
		url TEXT NOT NULL UNIQUE,
		status_code INTEGER NOT NULL,
		title TEXT,
		page_size INTEGER NOT NULL,
		latency_ms INTEGER NOT NULL,
		crawled_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	`

	_, err := db.Exec(ctx, query)
	return err
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

	allowed, err := c.isAllowedByRobots(ctx, job.URL)
	if err != nil {
		fmt.Println("Could not check robots.txt, continuing:", err)
	} else if !allowed {
		fmt.Println("Blocked by robots.txt, skipping:", job.URL)
		return
	}

	fmt.Println("Worker", workerID, "preparing to crawl:", job.URL)

	resp, latency, err := c.fetchWithRetry(ctx, job.URL)
	if err != nil {
		fmt.Println("Final fetch failure:", err)
		c.recordFailure()
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Println("Skipping page. Status code:", resp.StatusCode)
		c.recordFailure()
		return
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading page body:", err)
		c.recordFailure()
		return
	}

	title := extractTitle(bytes.NewReader(body))
	links := extractLinks(bytes.NewReader(body), job.URL)

	result := PageResult{
		URL:        job.URL,
		StatusCode: resp.StatusCode,
		Title:      title,
		PageSize:   len(body),
		Latency:    latency,
	}

	if err := c.savePageResult(ctx, result); err != nil {
		fmt.Println("Error saving page result:", err)
	}

	c.recordSuccess(latency)

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

func (c *Crawler) savePageResult(ctx context.Context, result PageResult) error {
	query := `
	INSERT INTO crawled_pages (
		url,
		status_code,
		title,
		page_size,
		latency_ms
	)
	VALUES ($1, $2, $3, $4, $5)
	ON CONFLICT (url) DO UPDATE SET
		status_code = EXCLUDED.status_code,
		title = EXCLUDED.title,
		page_size = EXCLUDED.page_size,
		latency_ms = EXCLUDED.latency_ms,
		crawled_at = NOW();
	`

	_, err := c.DB.Exec(
		ctx,
		query,
		result.URL,
		result.StatusCode,
		result.Title,
		result.PageSize,
		int(result.Latency.Milliseconds()),
	)

	return err
}

func (c *Crawler) isAllowedByRobots(ctx context.Context, pageURL string) (bool, error) {
	parsedURL, err := url.Parse(pageURL)
	if err != nil {
		return false, err
	}

	domain := parsedURL.Scheme + "://" + parsedURL.Host

	rules, err := c.getRobotsRules(ctx, domain)
	if err != nil {
		return true, err
	}

	for _, disallowPath := range rules.DisallowPaths {
		if disallowPath == "" {
			continue
		}

		if strings.HasPrefix(parsedURL.Path, disallowPath) {
			return false, nil
		}
	}

	return true, nil
}

func (c *Crawler) getRobotsRules(ctx context.Context, domain string) (RobotsRules, error) {
	c.Mu.Lock()
	rules, exists := c.RobotsCache[domain]
	c.Mu.Unlock()

	if exists {
		return rules, nil
	}

	robotsURL := domain + "/robots.txt"

	fmt.Println("Fetching robots.txt:", robotsURL)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, robotsURL, nil)
	if err != nil {
		return RobotsRules{}, err
	}

	resp, err := c.Client.Do(req)
	if err != nil {
		return RobotsRules{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		emptyRules := RobotsRules{}

		c.Mu.Lock()
		c.RobotsCache[domain] = emptyRules
		c.Mu.Unlock()

		return emptyRules, nil
	}

	parsedRules := parseRobotsTxt(resp)

	c.Mu.Lock()
	c.RobotsCache[domain] = parsedRules
	c.Mu.Unlock()

	return parsedRules, nil
}

func parseRobotsTxt(resp *http.Response) RobotsRules {
	var rules RobotsRules

	scanner := bufio.NewScanner(resp.Body)

	appliesToAllCrawlers := false

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		lowerLine := strings.ToLower(line)

		if strings.HasPrefix(lowerLine, "user-agent:") {
			userAgent := strings.TrimSpace(line[len("user-agent:"):])
			appliesToAllCrawlers = userAgent == "*"
			continue
		}

		if appliesToAllCrawlers && strings.HasPrefix(lowerLine, "disallow:") {
			disallowPath := strings.TrimSpace(line[len("disallow:"):])
			rules.DisallowPaths = append(rules.DisallowPaths, disallowPath)
		}
	}

	return rules
}

func (c *Crawler) fetchWithRetry(ctx context.Context, pageURL string) (*http.Response, time.Duration, error) {
	var lastErr error
	var lastLatency time.Duration

	for attempt := 1; attempt <= c.MaxRetries+1; attempt++ {
		select {
		case <-ctx.Done():
			return nil, lastLatency, ctx.Err()
		case <-c.RateLimiter.C:
		}

		fmt.Println("Fetching:", pageURL, "attempt:", attempt)

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, pageURL, nil)
		if err != nil {
			return nil, lastLatency, err
		}

		startTime := time.Now()

		resp, err := c.Client.Do(req)

		lastLatency = time.Since(startTime)

		if err == nil && !shouldRetryStatus(resp.StatusCode) {
			return resp, lastLatency, nil
		}

		if err == nil {
			lastErr = fmt.Errorf("received retryable status code: %d", resp.StatusCode)
			resp.Body.Close()
		} else {
			lastErr = err
		}

		if attempt <= c.MaxRetries {
			backoff := calculateBackoff(attempt)

			fmt.Println("Retrying after:", backoff)

			select {
			case <-ctx.Done():
				return nil, lastLatency, ctx.Err()
			case <-time.After(backoff):
			}
		}
	}

	return nil, lastLatency, lastErr
}

func shouldRetryStatus(statusCode int) bool {
	return statusCode == http.StatusTooManyRequests ||
		statusCode == http.StatusInternalServerError ||
		statusCode == http.StatusBadGateway ||
		statusCode == http.StatusServiceUnavailable ||
		statusCode == http.StatusGatewayTimeout
}

func calculateBackoff(attempt int) time.Duration {
	baseDelay := 500 * time.Millisecond

	return baseDelay * time.Duration(1<<(attempt-1))
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

func extractLinks(reader io.Reader, baseURL string) []string {
	var links []string

	doc, err := html.Parse(reader)
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

func extractTitle(reader io.Reader) string {
	doc, err := html.Parse(reader)
	if err != nil {
		return ""
	}

	var title string

	var visitNode func(*html.Node)

	visitNode = func(n *html.Node) {
		if title != "" {
			return
		}

		if n.Type == html.ElementNode && n.Data == "title" {
			if n.FirstChild != nil {
				title = strings.TrimSpace(n.FirstChild.Data)
			}
			return
		}

		for child := n.FirstChild; child != nil; child = child.NextSibling {
			visitNode(child)
		}
	}

	visitNode(doc)

	return title
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