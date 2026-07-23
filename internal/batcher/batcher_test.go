package batcher

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestBatcherFlushBySize(t *testing.T) {
	var mu sync.Mutex
	var batches [][]int
	b := New(3, time.Hour, func(_ context.Context, items []int) error {
		mu.Lock()
		defer mu.Unlock()
		cp := append([]int(nil), items...)
		batches = append(batches, cp)
		return nil
	})
	for i := 1; i <= 3; i++ {
		if err := b.Add(context.Background(), i); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		n := len(batches)
		mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for flush")
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(batches[0]) != 3 {
		t.Fatalf("batch=%v", batches[0])
	}
	_ = b.Close(context.Background())
}

func TestBatcherFlushByTimeout(t *testing.T) {
	var mu sync.Mutex
	var got []int
	b := New(10, 30*time.Millisecond, func(_ context.Context, items []int) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, items...)
		return nil
	})
	if err := b.Add(context.Background(), 42); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		mu.Lock()
		n := len(got)
		mu.Unlock()
		if n >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("timeout waiting for flush")
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = b.Close(context.Background())
}
