package local_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/usmc/k8s-agent/internal/local"
)

func TestZipDirectory(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	if err := os.MkdirAll(filepath.Join(src, "ns"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "ns", "obj.yaml"), []byte("test"), 0o644); err != nil {
		t.Fatal(err)
	}
	zipPath := filepath.Join(base, "out.zip")
	if err := local.ZipDirectory(src, zipPath); err != nil {
		t.Fatalf("zip failed: %v", err)
	}
	info, err := os.Stat(zipPath)
	if err != nil || info.Size() == 0 {
		t.Fatal("expected non-empty zip")
	}
}

func TestTarGzDirectory(t *testing.T) {
	base := t.TempDir()
	src := filepath.Join(base, "src")
	if err := os.MkdirAll(filepath.Join(src, "ns"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "ns", "obj.yaml"), []byte("test"), 0o644); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(base, "out.tar.gz")
	if err := local.TarGzDirectory(src, archive); err != nil {
		t.Fatalf("tar.gz failed: %v", err)
	}
	info, err := os.Stat(archive)
	if err != nil || info.Size() == 0 {
		t.Fatal("expected non-empty archive")
	}
}
