package batcher

import (
	"context"
	"sync"
	"time"
)

// FlushFunc is called with a batch of items (size or timeout flush).
type FlushFunc[T any] func(ctx context.Context, items []T) error

// Batcher accumulates items and flushes by max size or timeout.
type Batcher[T any] struct {
	maxSize  int
	timeout  time.Duration
	flushFn  FlushFunc[T]
	mu       sync.Mutex
	buf      []T
	timer    *time.Timer
	closed   bool
	flushing sync.WaitGroup
}

func New[T any](maxSize int, timeout time.Duration, flush FlushFunc[T]) *Batcher[T] {
	if maxSize < 1 {
		maxSize = 1
	}
	return &Batcher[T]{
		maxSize: maxSize,
		timeout: timeout,
		flushFn: flush,
		buf:     make([]T, 0, maxSize),
	}
}

// Add appends an item; may trigger a flush.
func (b *Batcher[T]) Add(ctx context.Context, item T) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return context.Canceled
	}
	b.buf = append(b.buf, item)
	if len(b.buf) == 1 && b.timeout > 0 {
		b.resetTimerLocked()
	}
	if len(b.buf) >= b.maxSize {
		return b.flushLocked(ctx)
	}
	return nil
}

// Flush forces a flush of the current buffer.
func (b *Batcher[T]) Flush(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.flushLocked(ctx)
}

// Close flushes remaining items and stops the timer.
func (b *Batcher[T]) Close(ctx context.Context) error {
	b.mu.Lock()
	b.closed = true
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	err := b.flushLocked(ctx)
	b.mu.Unlock()
	b.flushing.Wait()
	return err
}

func (b *Batcher[T]) resetTimerLocked() {
	if b.timer != nil {
		b.timer.Stop()
	}
	b.timer = time.AfterFunc(b.timeout, func() {
		b.mu.Lock()
		defer b.mu.Unlock()
		if b.closed || len(b.buf) == 0 {
			return
		}
		_ = b.flushLocked(context.Background())
	})
}

func (b *Batcher[T]) flushLocked(ctx context.Context) error {
	if len(b.buf) == 0 {
		return nil
	}
	items := append([]T(nil), b.buf...)
	b.buf = b.buf[:0]
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
	b.flushing.Add(1)
	go func() {
		defer b.flushing.Done()
		_ = b.flushFn(ctx, items)
	}()
	return nil
}
