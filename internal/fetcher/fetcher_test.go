package fetcher

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func handlerOK() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("hello"))
	}
}

func TestHTTPFetcher_Fetch_Success(t *testing.T) {
	ts := httptest.NewServer(handlerOK())
	defer ts.Close()

	f := New(2 * time.Second)

	resp, err := f.Fetch(context.Background(), ts.URL)
	if err != nil {
		t.Fatalf("Fetch() returned error: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("StatusCode = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("failed to read body: %v", err)
	}
	if string(body) != "hello" {
		t.Fatalf("body = %q, want %q", body, "hello")
	}
}

func TestHTTPFetcher_Fetch_ContextCancelled(t *testing.T) {
	ts := httptest.NewServer(handlerOK())
	defer ts.Close()

	f := New(2 * time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := f.Fetch(ctx, ts.URL)
	if err == nil {
		t.Fatal("Fetch() with a cancelled context should return an error")
	}
}
