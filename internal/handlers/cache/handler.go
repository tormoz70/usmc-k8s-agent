package cachehandler

import (
	"context"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/cache"
	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/observability"
	"github.com/usmc/usmc-k8s-agent/internal/policy"
	"github.com/usmc/usmc-k8s-agent/internal/result"
)

type cacheMetrics interface {
	SetCacheEntries(n int)
}

type metricsAdapter struct {
	m *observability.Metrics
}

func (a metricsAdapter) SetCacheEntries(n int) {
	if a.m != nil {
		a.m.CacheEntries.Set(float64(n))
	}
}

func syncCacheEntries(store *cache.Store, metrics cacheMetrics) {
	if metrics != nil {
		metrics.SetCacheEntries(store.Len())
	}
}

// PutHandler handles cache.put commands (leader-only).
type PutHandler struct {
	store   *cache.Store
	policy  *policy.Engine
	metrics cacheMetrics
}

func NewPutHandler(store *cache.Store, engine *policy.Engine, metrics *observability.Metrics) *PutHandler {
	return &PutHandler{store: store, policy: engine, metrics: metricsAdapter{metrics}}
}

func (h *PutHandler) Type() string {
	return command.TypeCachePut
}

func (h *PutHandler) Handle(_ context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error) {
	started := time.Now().UTC()
	if err := h.policy.AllowCommandType(command.TypeCachePut); err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "PolicyDenied", "FORBIDDEN_COMMAND", err.Error(), started, time.Now().UTC()), nil
	}
	payload, err := ParsePutPayload(cmd.Payload)
	if err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "ValidationFailed", "INVALID_PAYLOAD", err.Error(), started, time.Now().UTC()), nil
	}
	for _, entry := range payload.Entries {
		h.store.Put(entry.Key, entry.Value, entry.TTLSeconds)
	}
	syncCacheEntries(h.store, h.metrics)
	finished := time.Now().UTC()
	resp := result.Completed(cmd.CommandID, meta.CorrelationID, started, finished)
	resp.KeysWritten = len(payload.Entries)
	return resp, nil
}

// DeleteHandler handles cache.delete commands.
type DeleteHandler struct {
	store   *cache.Store
	policy  *policy.Engine
	metrics cacheMetrics
}

func NewDeleteHandler(store *cache.Store, engine *policy.Engine, metrics *observability.Metrics) *DeleteHandler {
	return &DeleteHandler{store: store, policy: engine, metrics: metricsAdapter{metrics}}
}

func (h *DeleteHandler) Type() string {
	return command.TypeCacheDelete
}

func (h *DeleteHandler) Handle(_ context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error) {
	started := time.Now().UTC()
	if err := h.policy.AllowCommandType(command.TypeCacheDelete); err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "PolicyDenied", "FORBIDDEN_COMMAND", err.Error(), started, time.Now().UTC()), nil
	}
	payload, err := ParseDeletePayload(cmd.Payload)
	if err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "ValidationFailed", "INVALID_PAYLOAD", err.Error(), started, time.Now().UTC()), nil
	}
	deleted := h.store.Delete(payload.Keys)
	syncCacheEntries(h.store, h.metrics)
	finished := time.Now().UTC()
	resp := result.Completed(cmd.CommandID, meta.CorrelationID, started, finished)
	resp.KeysDeleted = deleted
	return resp, nil
}

// ClearHandler handles cache.clear commands.
type ClearHandler struct {
	store   *cache.Store
	policy  *policy.Engine
	metrics cacheMetrics
}

func NewClearHandler(store *cache.Store, engine *policy.Engine, metrics *observability.Metrics) *ClearHandler {
	return &ClearHandler{store: store, policy: engine, metrics: metricsAdapter{metrics}}
}

func (h *ClearHandler) Type() string {
	return command.TypeCacheClear
}

func (h *ClearHandler) Handle(_ context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error) {
	started := time.Now().UTC()
	if err := h.policy.AllowCommandType(command.TypeCacheClear); err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "PolicyDenied", "FORBIDDEN_COMMAND", err.Error(), started, time.Now().UTC()), nil
	}
	payload, err := ParseClearPayload(cmd.Payload)
	if err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "ValidationFailed", "INVALID_PAYLOAD", err.Error(), started, time.Now().UTC()), nil
	}
	var cleared int
	if payload.Prefix == "" {
		cleared = h.store.Len()
		h.store.Clear()
	} else {
		cleared = h.store.ClearPrefix(payload.Prefix)
	}
	syncCacheEntries(h.store, h.metrics)
	finished := time.Now().UTC()
	resp := result.Completed(cmd.CommandID, meta.CorrelationID, started, finished)
	resp.KeysCleared = cleared
	return resp, nil
}
