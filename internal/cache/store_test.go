package cache

import (
	"testing"
	"time"
)

func TestStorePutGetDelete(t *testing.T) {
	s := NewStore()
	s.Put("a/b", "enabled", 3600)
	entry, ok := s.Get("a/b")
	if !ok || entry.Value != "enabled" {
		t.Fatalf("expected entry, got ok=%v value=%q", ok, entry.Value)
	}
	if n := s.Delete([]string{"a/b", "missing"}); n != 1 {
		t.Fatalf("expected 1 deleted, got %d", n)
	}
	if _, ok := s.Get("a/b"); ok {
		t.Fatal("expected miss after delete")
	}
}

func TestStoreExpiry(t *testing.T) {
	s := NewStore()
	s.Put("temp", "x", 1)
	time.Sleep(1100 * time.Millisecond)
	if _, ok := s.Get("temp"); ok {
		t.Fatal("expected expired entry to be removed")
	}
}

func TestStoreClearPrefix(t *testing.T) {
	s := NewStore()
	s.Put("feature/a", "1", 0)
	s.Put("feature/b", "2", 0)
	s.Put("other/c", "3", 0)
	if n := s.ClearPrefix("feature/"); n != 2 {
		t.Fatalf("expected 2 cleared, got %d", n)
	}
	if s.Len() != 1 {
		t.Fatalf("expected 1 entry left, got %d", s.Len())
	}
}

func TestStoreListByPrefix(t *testing.T) {
	s := NewStore()
	s.Put("feature/a", "1", 0)
	s.Put("feature/b", "2", 0)
	s.Put("other/c", "3", 0)
	items := s.ListByPrefix("feature/")
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
}
