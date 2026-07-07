package s3_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/usmc/k8s-agent/internal/s3"
)

func TestUploadPresignedURL(t *testing.T) {
	var received int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			t.Fatalf("expected PUT, got %s", r.Method)
		}
		body, _ := io.ReadAll(r.Body)
		received = int64(len(body))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	uploader := s3.NewUploader()
	n, err := uploader.Upload(context.Background(), srv.URL, strings.NewReader("hello"), "text/plain")
	if err != nil {
		t.Fatalf("upload failed: %v", err)
	}
	if received != 5 {
		t.Fatalf("expected 5 bytes received, got %d", received)
	}
	_ = n
}

func TestHostFromPresignedURL(t *testing.T) {
	host, err := s3.HostFromPresignedURL("https://storage.example.com/bucket/key?sig=abc")
	if err != nil {
		t.Fatal(err)
	}
	if host != "storage.example.com" {
		t.Fatalf("unexpected host: %s", host)
	}
}
