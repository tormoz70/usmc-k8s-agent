package keyedlock

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestSynchronizerSerializesSameKey(t *testing.T) {
	s := New()
	var concurrent int32
	var maxConcurrent int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = s.Do("ns/obj", func() error {
				cur := atomic.AddInt32(&concurrent, 1)
				for {
					old := atomic.LoadInt32(&maxConcurrent)
					if cur <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, cur) {
						break
					}
				}
				atomic.AddInt32(&concurrent, -1)
				return nil
			})
		}()
	}
	wg.Wait()
	if maxConcurrent != 1 {
		t.Fatalf("maxConcurrent=%d want 1", maxConcurrent)
	}
}
