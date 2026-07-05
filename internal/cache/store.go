package cache

import (
	"sync"
	"time"
)

const SchemaVersionV1 = "v1"

// Entry holds a cached key-value with optional expiry.
type Entry struct {
	Key       string
	Value     string
	UpdatedAt time.Time
	ExpiresAt *time.Time
}

// HTTPResponse is returned by GET /v1/cache/{key}.
type HTTPResponse struct {
	Key       string     `json:"key"`
	Value     string     `json:"value"`
	UpdatedAt time.Time  `json:"updated_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// Store is an in-memory TTL cache (leader-only).
type Store struct {
	mu      sync.RWMutex
	entries map[string]Entry
}

func NewStore() *Store {
	return &Store{entries: make(map[string]Entry)}
}

func (s *Store) Put(key, value string, ttlSeconds int) {
	now := time.Now().UTC()
	var expires *time.Time
	if ttlSeconds > 0 {
		t := now.Add(time.Duration(ttlSeconds) * time.Second)
		expires = &t
	}
	s.mu.Lock()
	s.entries[key] = Entry{
		Key: key, Value: value, UpdatedAt: now, ExpiresAt: expires,
	}
	s.mu.Unlock()
}

func (s *Store) Delete(keys []string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted := 0
	for _, key := range keys {
		if _, ok := s.entries[key]; ok {
			delete(s.entries, key)
			deleted++
		}
	}
	return deleted
}

func (s *Store) Get(key string) (Entry, bool) {
	s.mu.RLock()
	entry, ok := s.entries[key]
	s.mu.RUnlock()
	if !ok {
		return Entry{}, false
	}
	if entry.ExpiresAt != nil && time.Now().UTC().After(*entry.ExpiresAt) {
		s.mu.Lock()
		delete(s.entries, key)
		s.mu.Unlock()
		return Entry{}, false
	}
	return entry, true
}

func (s *Store) ListByPrefix(prefix string) []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now().UTC()
	out := make([]Entry, 0)
	for key, entry := range s.entries {
		if prefix != "" && !hasPrefix(key, prefix) {
			continue
		}
		if entry.ExpiresAt != nil && now.After(*entry.ExpiresAt) {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func (s *Store) Clear() {
	s.mu.Lock()
	s.entries = make(map[string]Entry)
	s.mu.Unlock()
}

func (s *Store) ClearPrefix(prefix string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	for key := range s.entries {
		if prefix == "" || hasPrefix(key, prefix) {
			delete(s.entries, key)
			removed++
		}
	}
	return removed
}

func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

func (e Entry) ToHTTPResponse() HTTPResponse {
	return HTTPResponse{
		Key: e.Key, Value: e.Value, UpdatedAt: e.UpdatedAt, ExpiresAt: e.ExpiresAt,
	}
}

func hasPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
