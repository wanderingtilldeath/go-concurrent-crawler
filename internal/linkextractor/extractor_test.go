package linkextractor

import (
	"strings"
	"testing"
)

func TestHTMLExtractor_Extract(t *testing.T) {
	tests := []struct {
		name    string
		html    string
		baseURL string
		want    []string
	}{
		{
			name:    "absolute and relative links resolved against base",
			html:    `<a href="/about">about</a><a href="https://other.example/page">other</a>`,
			baseURL: "https://example.com/",
			want:    []string{"https://example.com/about", "https://other.example/page"},
		},
		{
			name:    "fragment is stripped",
			html:    `<a href="/docs#section-2">docs</a>`,
			baseURL: "https://example.com/",
			want:    []string{"https://example.com/docs"},
		},
		{
			name:    "non-http schemes are filtered out",
			html:    `<a href="mailto:a@example.com">mail</a><a href="javascript:void(0)">js</a><a href="/ok">ok</a>`,
			baseURL: "https://example.com/",
			want:    []string{"https://example.com/ok"},
		},
		{
			name:    "no anchors yields no links",
			html:    `<p>nothing to see here</p>`,
			baseURL: "https://example.com/",
			want:    nil,
		},
	}

	var extractor HTMLExtractor

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractor.Extract(strings.NewReader(tt.html), tt.baseURL)
			if err != nil {
				t.Fatalf("Extract() returned error: %v", err)
			}

			if len(got) != len(tt.want) {
				t.Fatalf("Extract() = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("Extract() = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestNormalizeURL(t *testing.T) {
	tests := []struct {
		name    string
		link    string
		baseURL string
		want    string
		wantOK  bool
	}{
		{
			name:    "relative path resolved against base",
			link:    "/path",
			baseURL: "https://example.com/base/",
			want:    "https://example.com/path",
			wantOK:  true,
		},
		{
			name:    "fragment removed",
			link:    "/path#top",
			baseURL: "https://example.com/",
			want:    "https://example.com/path",
			wantOK:  true,
		},
		{
			name:    "non-http scheme rejected",
			link:    "mailto:someone@example.com",
			baseURL: "https://example.com/",
			wantOK:  false,
		},
		{
			name:    "invalid base URL rejected",
			link:    "/path",
			baseURL: "://not-a-url",
			wantOK:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := NormalizeURL(tt.link, tt.baseURL)
			if ok != tt.wantOK {
				t.Fatalf("NormalizeURL() ok = %v, want %v", ok, tt.wantOK)
			}
			if ok && got != tt.want {
				t.Fatalf("NormalizeURL() = %q, want %q", got, tt.want)
			}
		})
	}
}
