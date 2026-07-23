package keyedlock

import "sync"

// Synchronizer provides per-key mutual exclusion (Java StringKeySynchronizer analogue).
type Synchronizer struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func New() *Synchronizer {
	return &Synchronizer{locks: make(map[string]*sync.Mutex)}
}

func (s *Synchronizer) lockFor(key string) *sync.Mutex {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, ok := s.locks[key]
	if !ok {
		m = &sync.Mutex{}
		s.locks[key] = m
	}
	return m
}

// Do runs fn while holding the lock for key.
func (s *Synchronizer) Do(key string, fn func() error) error {
	m := s.lockFor(key)
	m.Lock()
	defer m.Unlock()
	return fn()
}

// Lock acquires the lock for key. Caller must Unlock.
func (s *Synchronizer) Lock(key string) {
	s.lockFor(key).Lock()
}

// Unlock releases the lock for key.
func (s *Synchronizer) Unlock(key string) {
	s.lockFor(key).Unlock()
}
