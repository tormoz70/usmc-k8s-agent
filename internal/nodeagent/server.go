// Package nodeagent serves per-node log reads for agent v2 DaemonSet pods.
package nodeagent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/nodelocal"
	"github.com/usmc/usmc-k8s-agent/internal/policy"
)

// FetchRequest is the JSON body for POST /internal/v1/logs/fetch.
type FetchRequest struct {
	Namespace  string     `json:"namespace"`
	Pod        string     `json:"pod"`
	PodUID     string     `json:"pod_uid,omitempty"`
	Container  string     `json:"container"`
	Previous   bool       `json:"previous"`
	SinceTime  *time.Time `json:"since_time,omitempty"`
	TailLines  *int64     `json:"tail_lines,omitempty"`
	LimitBytes *int64     `json:"limit_bytes,omitempty"`
}

// Server exposes node-local log fetch/stream endpoints.
type Server struct {
	reader   *nodelocal.Reader
	policy   *policy.Engine
	token    string
	nodeName string
	log      *slog.Logger
	srv      *http.Server
}

func NewServer(addr string, reader *nodelocal.Reader, engine *policy.Engine, token, nodeName string, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{reader: reader, policy: engine, token: token, nodeName: nodeName, log: log}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok", "node": s.nodeName})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ready", "node": s.nodeName})
	})
	mux.HandleFunc("/internal/v1/logs/fetch", s.handleFetch)
	mux.HandleFunc("/internal/v1/logs/stream", s.handleStream)
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return s
}

func (s *Server) Start() error {
	s.log.Info("logs-node agent listening", "addr", s.srv.Addr, "node", s.nodeName)
	go func() {
		if err := s.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			s.log.Error("logs-node http error", "error", err)
		}
	}()
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) authorize(w http.ResponseWriter, r *http.Request) bool {
	auth := r.Header.Get("Authorization")
	want := "Bearer " + s.token
	if s.token == "" || auth != want {
		http.Error(w, `{"error":"unauthorized"}`, http.StatusUnauthorized)
		return false
	}
	return true
}

func (s *Server) handleFetch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	if !s.authorize(w, r) {
		return
	}
	var req FetchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}
	if err := s.allowNS(req.Namespace); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusForbidden)
		return
	}
	rc, err := s.reader.Open(r.Context(), nodelocal.ReadOptions{
		Namespace:  req.Namespace,
		Pod:        req.Pod,
		PodUID:     req.PodUID,
		Container:  req.Container,
		Previous:   req.Previous,
		SinceTime:  req.SinceTime,
		TailLines:  req.TailLines,
		LimitBytes: req.LimitBytes,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusNotFound)
		return
	}
	defer rc.Close()
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = io.Copy(w, rc)
}

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	// Streaming from rotated files is best-effort: same as fetch with follow via re-open loop.
	// For v2 MVP, stream endpoint reuses fetch semantics with optional follow query.
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	if !s.authorize(w, r) {
		return
	}
	var req FetchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}
	if err := s.allowNS(req.Namespace); err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusForbidden)
		return
	}
	follow := r.URL.Query().Get("follow") == "true"
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":"streaming unsupported"}`, http.StatusInternalServerError)
		return
	}

	// Initial open before headers so we can return 404 cleanly.
	rc, err := s.reader.Open(r.Context(), nodelocal.ReadOptions{
		Namespace:  req.Namespace,
		Pod:        req.Pod,
		PodUID:     req.PodUID,
		Container:  req.Container,
		Previous:   req.Previous,
		SinceTime:  req.SinceTime,
		TailLines:  req.TailLines,
		LimitBytes: req.LimitBytes,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":%q}`, err.Error()), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	_, _ = io.Copy(w, rc)
	rc.Close()
	flusher.Flush()
	if !follow {
		return
	}
	now := time.Now().UTC()
	req.SinceTime = &now
	req.TailLines = nil
	for {
		select {
		case <-r.Context().Done():
			return
		default:
		}
		time.Sleep(time.Second)
		rc, err := s.reader.Open(r.Context(), nodelocal.ReadOptions{
			Namespace:  req.Namespace,
			Pod:        req.Pod,
			PodUID:     req.PodUID,
			Container:  req.Container,
			Previous:   req.Previous,
			SinceTime:  req.SinceTime,
			TailLines:  req.TailLines,
			LimitBytes: req.LimitBytes,
		})
		if err != nil {
			return
		}
		_, _ = io.Copy(w, rc)
		rc.Close()
		flusher.Flush()
		t := time.Now().UTC()
		req.SinceTime = &t
	}
}

func (s *Server) allowNS(ns string) error {
	if s.policy == nil {
		return nil
	}
	return s.policy.AllowNamespace(ns)
}
