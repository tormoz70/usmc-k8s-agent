package loglevel

import (
	"testing"
	"time"
)

func TestLLCacheDedup(t *testing.T) {
	c := NewLLCache(time.Minute)
	if !c.UpdateNewRequest("a/b/c", "DEBUG") {
		t.Fatal("first should be new")
	}
	if c.UpdateNewRequest("a/b/c", "DEBUG") {
		t.Fatal("second identical should be skipped")
	}
	if !c.UpdateNewRequest("a/b/c", "INFO") {
		t.Fatal("different level should be new")
	}
}
