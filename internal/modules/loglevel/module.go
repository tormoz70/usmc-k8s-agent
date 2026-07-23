package loglevel

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/features"
	"github.com/usmc/usmc-k8s-agent/internal/keyedlock"
	"github.com/usmc/usmc-k8s-agent/internal/modules"
	"github.com/usmc/usmc-k8s-agent/internal/protoheaders"
	"github.com/usmc/usmc-k8s-agent/internal/protodispatch"
)

// LLCache is a TTL cache for log-level requests (Java LLCache analogue).
type LLCache struct {
	mu      sync.Mutex
	entries map[string]cacheEntry
	ttl     time.Duration
}

type cacheEntry struct {
	level     string
	expiresAt time.Time
}

func NewLLCache(ttl time.Duration) *LLCache {
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	return &LLCache{entries: make(map[string]cacheEntry), ttl: ttl}
}

// UpdateNewRequest returns false if a recent identical request exists (skip core call).
func (c *LLCache) UpdateNewRequest(key, level string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := time.Now()
	if e, ok := c.entries[key]; ok && e.level == level && now.Before(e.expiresAt) {
		return false
	}
	c.entries[key] = cacheEntry{level: level, expiresAt: now.Add(c.ttl)}
	return true
}

func (c *LLCache) Put(key, level string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = cacheEntry{level: level, expiresAt: time.Now().Add(c.ttl)}
}

func (c *LLCache) Get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || time.Now().After(e.expiresAt) {
		return "", false
	}
	return e.level, true
}

// Module handles Tengri-style log level updates via protobuf.
type Module struct {
	cfg    *config.Config
	cache  *LLCache
	locks  *keyedlock.Synchronizer
	log    *slog.Logger
	disp   *protodispatch.Dispatcher
}

var _ modules.Module = (*Module)(nil)

func New(cfg *config.Config, disp *protodispatch.Dispatcher, log *slog.Logger) *Module {
	if log == nil {
		log = slog.Default()
	}
	return &Module{
		cfg:   cfg,
		cache: NewLLCache(5 * time.Minute),
		locks: keyedlock.New(),
		log:   log,
		disp:  disp,
	}
}

func (m *Module) Name() string { return "loglevel" }

func (m *Module) Enabled(cfg *config.Config, _ *features.Registry) bool {
	if cfg == nil {
		return false
	}
	return cfg.Kafka.Mode == config.KafkaModeProtobuf || cfg.Kafka.Mode == config.KafkaModeDual
}

func (m *Module) Start(context.Context) error {
	if m.disp != nil {
		m.disp.Register("UpdateLLCache", m.handleUpdateCache)
		m.disp.Register("SetLogLevel", m.handleSetLevel)
	}
	m.log.Info("loglevel module started")
	return nil
}

func (m *Module) Stop(context.Context) error { return nil }

type setLevelPayload struct {
	Cluster   string `json:"cluster"`
	Namespace string `json:"namespace"`
	Object    string `json:"object"`
	Level     string `json:"level"`
}

func (m *Module) handleUpdateCache(_ context.Context, _ protoheaders.Headers, body []byte) error {
	var p setLevelPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return err
	}
	key := p.Cluster + "/" + p.Namespace + "/" + p.Object
	m.cache.Put(key, p.Level)
	m.log.Info("llcache updated", "key", key, "level", p.Level)
	return nil
}

func (m *Module) handleSetLevel(_ context.Context, _ protoheaders.Headers, body []byte) error {
	var p setLevelPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return err
	}
	key := p.Cluster + "/" + p.Namespace + "/" + p.Object
	return m.locks.Do(key, func() error {
		if !m.cache.UpdateNewRequest(key, p.Level) {
			m.log.Debug("set log level skipped (cache hit)", "key", key)
			return nil
		}
		m.cache.Put(key, p.Level)
		m.log.Info("set log level accepted", "key", key, "level", p.Level)
		return nil
	})
}

// Cache exposes LLCache for tests.
func (m *Module) Cache() *LLCache { return m.cache }
