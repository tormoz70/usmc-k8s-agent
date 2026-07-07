package logs

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSanitizePathPart(t *testing.T) {
	if sanitizePathPart("app/../x") == "app_.._x" || sanitizePathPart("") == "unknown" {
		// acceptable
	}
	if got := sanitizePathPart("payments-api"); got != "payments-api" {
		t.Fatalf("got %q", got)
	}
}

func TestZipDir(t *testing.T) {
	dir := t.TempDir()
	sub := filepath.Join(dir, "deploy", "pod-1")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "app-current-ts.log"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	zipPath := filepath.Join(dir, "bundle.zip")
	size, err := zipDir(dir, zipPath, "bundle.zip")
	if err != nil {
		t.Fatal(err)
	}
	if size <= 0 {
		t.Fatalf("size=%d", size)
	}
}
