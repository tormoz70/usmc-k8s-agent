package nodelocal

import (
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestReaderCurrentAndPrevious(t *testing.T) {
	root := t.TempDir()
	podDir := filepath.Join(root, "ns_mypod_uid123", "app")
	if err := os.MkdirAll(podDir, 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(name, content string) {
		if err := os.WriteFile(filepath.Join(podDir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("0.log", `{"log":"old\n","stream":"stdout","time":"2024-01-01T00:00:00.000000000Z"}`+"\n")
	write("1.log", `{"log":"new\n","stream":"stdout","time":"2024-01-02T00:00:00.000000000Z"}`+"\n")

	r := NewReader(root)
	rc, err := r.Open(t.Context(), ReadOptions{Namespace: "ns", Pod: "mypod", Container: "app"})
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b); got != "new\n" {
		t.Fatalf("current = %q", got)
	}

	rc2, err := r.Open(t.Context(), ReadOptions{Namespace: "ns", Pod: "mypod", Container: "app", Previous: true})
	if err != nil {
		t.Fatal(err)
	}
	defer rc2.Close()
	b, err = io.ReadAll(rc2)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(b); got != "old\n" {
		t.Fatalf("previous = %q", got)
	}
}

func TestReaderSinceTime(t *testing.T) {
	root := t.TempDir()
	podDir := filepath.Join(root, "ns_p_uid", "c")
	if err := os.MkdirAll(podDir, 0o755); err != nil {
		t.Fatal(err)
	}
	content := "" +
		`{"log":"a\n","stream":"stdout","time":"2024-01-01T00:00:00.000000000Z"}` + "\n" +
		`{"log":"b\n","stream":"stdout","time":"2024-01-03T00:00:00.000000000Z"}` + "\n"
	if err := os.WriteFile(filepath.Join(podDir, "0.log"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	since := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	r := NewReader(root)
	rc, err := r.Open(t.Context(), ReadOptions{Namespace: "ns", Pod: "p", Container: "c", SinceTime: &since})
	if err != nil {
		t.Fatal(err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "b\n" {
		t.Fatalf("got %q", b)
	}
}
