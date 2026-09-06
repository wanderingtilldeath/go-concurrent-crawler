// Package linkextractor parses HTML documents and resolves the hyperlinks
// they contain into absolute, crawlable URLs.
package linkextractor

import (
	"io"
	"net/url"

	"golang.org/x/net/html"
)

// Extractor pulls crawlable links out of an HTML document. It is an
// interface so callers (e.g. the crawler) can be tested without parsing
// real HTML.
type Extractor interface {
	Extract(body io.Reader, baseURL string) ([]string, error)
}

// HTMLExtractor is the production Extractor: it parses the document with
// golang.org/x/net/html and resolves every <a href> against baseURL.
type HTMLExtractor struct{}

// Extract returns the absolute URLs of every anchor tag in body, resolved
// against baseURL. Non-http(s) links (mailto:, javascript:, ...) and
// malformed URLs are silently skipped.
func (HTMLExtractor) Extract(body io.Reader, baseURL string) ([]string, error) {
	doc, err := html.Parse(body)
	if err != nil {
		return nil, err
	}

	var links []string

	var visitNode func(*html.Node)
	visitNode = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "a" {
			for _, attr := range n.Attr {
				if attr.Key == "href" {
					if absoluteLink, ok := NormalizeURL(attr.Val, baseURL); ok {
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

	return links, nil
}

// NormalizeURL resolves link against baseURL into an absolute URL, dropping
// any fragment. It reports ok=false for unparsable URLs or schemes other
// than http/https (mailto:, javascript:, tel:, ...).
func NormalizeURL(link string, baseURL string) (string, bool) {
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
