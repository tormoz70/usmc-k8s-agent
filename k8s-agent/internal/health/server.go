package health

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

type Server struct {
	isLeader atomic.Bool
	ready    atomic.Bool
}

func New() *Server {
	s := &Server{}
	s.ready.Store(true)
	return s
}

func (s *Server) SetLeader(v bool) {
	s.isLeader.Store(v)
}

func (s *Server) SetReady(v bool) {
	s.ready.Store(v)
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.liveness)
	mux.HandleFunc("/readyz", s.readiness)
	return mux
}

func (s *Server) liveness(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

func (s *Server) readiness(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		http.Error(w, "not ready", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ready":  true,
		"leader": s.isLeader.Load(),
	})
}
