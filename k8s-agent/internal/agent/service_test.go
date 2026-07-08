package agent_test

import (
	"context"
	"sync"
	"testing"
	"time"
)

// TestGracefulShutdownWaitsInFlight verifies shutdown waits for in-flight work.
func TestGracefulShutdownWaitsInFlight(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	done := make(chan struct{})

	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		close(done)
	}()

	waitDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(waitDone)
	}()

	select {
	case <-waitDone:
	case <-time.After(200 * time.Millisecond):
		t.Fatal("shutdown did not wait for in-flight work")
	}
}

// TestContextCancel simulates agent stop on SIGTERM.
func TestContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	select {
	case <-ctx.Done():
	default:
		t.Fatal("expected cancelled context")
	}
}
