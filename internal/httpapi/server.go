package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/usmc/usmc-k8s-agent/internal/cache"
	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/observability"
)

// Server exposes health, readiness, metrics and cache endpoints.
type Server struct {
	cfg   config.HTTPConfig
	state *observability.RuntimeState
	cache *cache.Store
	log   *slog.Logger
	srv   *http.Server
}

func NewServer(cfg config.HTTPConfig, state *observability.RuntimeState, cacheStore *cache.Store, log *slog.Logger, router *command.Router) *Server {
	if log == nil {
		log = slog.Default()
	}
	mux := http.NewServeMux()
	s := &Server{cfg: cfg, state: state, cache: cacheStore, log: log}
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/v1/cache/", s.handleCacheGet)
	mux.HandleFunc("/v1/cache", s.handleCacheList)
	if router != nil {
		MountInternalRoutes(mux, router, state, cfg.InternalBearerToken)
	}

	s.srv = &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

func (s *Server) Start() error {
	s.log.Info("http server listening", "addr", s.srv.Addr)
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("http server error", "error", err)
		}
	}()
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	body := map[string]interface{}{
		"kafka":     s.state.KafkaConnected(),
		"apiserver": s.state.APIServerOK(),
		"leader":    s.state.IsLeader(),
		"ready":     s.state.Ready(),
	}
	status := http.StatusOK
	if !s.state.Ready() {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, body)
}

func (s *Server) handleCacheGet(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if !s.authorizeCache(w, r) {
		return
	}
	key := strings.TrimPrefix(r.URL.Path, "/v1/cache/")
	key = strings.Trim(key, "/")
	if key == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "key is required"})
		return
	}
	entry, ok := s.cache.Get(key)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	writeJSON(w, http.StatusOK, entry.ToHTTPResponse())
}

func (s *Server) handleCacheList(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if !s.authorizeCache(w, r) {
		return
	}
	prefix := r.URL.Query().Get("prefix")
	items := s.cache.ListByPrefix(prefix)
	out := make([]cache.HTTPResponse, 0, len(items))
	for _, item := range items {
		out = append(out, item.ToHTTPResponse())
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{"entries": out})
}

func (s *Server) authorizeCache(w http.ResponseWriter, r *http.Request) bool {
	if s.cfg.BearerToken != "" {
		auth := r.Header.Get("Authorization")
		if !strings.HasPrefix(auth, "Bearer ") || strings.TrimPrefix(auth, "Bearer ") != s.cfg.BearerToken {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return false
		}
	}
	if !s.state.IsLeader() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not leader"})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, body interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
