package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/usmc/usmc-k8s-agent/hack/mockcorelib"
)

type server struct {
	cfg   config
	modes *agentModeController
}

func newServer(cfg config) *server {
	return &server{
		cfg:   cfg,
		modes: newAgentModeController(cfg),
	}
}

func (s *server) staticHandler() http.Handler {
	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(sub))
}

func (s *server) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *server) handleTopics(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"topics":       s.cfg.DefaultTopics,
		"reply_topic":  s.cfg.ReplyTopic,
		"request_topic": s.cfg.RequestTopic,
	})
}

type templateInfo struct {
	Name string          `json:"name"`
	Type string          `json:"type"`
	Body json.RawMessage `json:"body"`
}

func (s *server) handleTemplates(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	entries, err := os.ReadDir(s.cfg.FixturesDir)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	templates := make([]templateInfo, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(s.cfg.FixturesDir, e.Name()))
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		cmdType := extractCommandType(data)
		templates = append(templates, templateInfo{
			Name: strings.TrimSuffix(e.Name(), ".json"),
			Type: cmdType,
			Body: json.RawMessage(data),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"templates": templates})
}

func extractCommandType(data []byte) string {
	var partial struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(data, &partial)
	return partial.Type
}

type sendCommandRequest struct {
	Body       json.RawMessage `json:"body"`
	ReplyTopic string          `json:"reply_topic"`
}

func (s *server) handleCommands(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req sendCommandRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON body: %w", err))
		return
	}
	if len(req.Body) == 0 {
		writeError(w, http.StatusBadRequest, fmt.Errorf("body is required"))
		return
	}
	if !json.Valid(req.Body) {
		writeError(w, http.StatusBadRequest, fmt.Errorf("body must be valid JSON"))
		return
	}
	replyTopic := req.ReplyTopic
	if replyTopic == "" {
		replyTopic = s.cfg.ReplyTopic
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	result, err := mockcorelib.SendCommand(ctx, s.cfg.Brokers, s.cfg.RequestTopic, replyTopic, req.Body)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleMessageStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		topic = s.cfg.ReplyTopic
	}
	correlationID := r.URL.Query().Get("correlation_id")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ctx := r.Context()
	out := make(chan mockcorelib.Message, 8)
	errCh := make(chan error, 1)
	go func() {
		errCh <- mockcorelib.StreamMessages(ctx, s.cfg.Brokers, topic, correlationID, out)
	}()

	fmt.Fprintf(w, "event: connected\ndata: {\"topic\":%q}\n\n", topic)
	flusher.Flush()

	for {
		select {
		case <-ctx.Done():
			return
		case err := <-errCh:
			if err != nil && ctx.Err() == nil {
				slog.Error("message stream error", "error", err, "topic", topic)
				fmt.Fprintf(w, "event: error\ndata: %q\n\n", err.Error())
				flusher.Flush()
			}
			return
		case msg, ok := <-out:
			if !ok {
				return
			}
			data, err := json.Marshal(msg)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: message\ndata: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func (s *server) handleMessageHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		topic = s.cfg.ReplyTopic
	}
	correlationID := r.URL.Query().Get("correlation_id")
	limit := 50
	if v := strings.TrimSpace(r.URL.Query().Get("limit")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid limit"))
			return
		}
		limit = n
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	msgs, err := mockcorelib.ReadRecentMessages(ctx, s.cfg.Brokers, topic, limit, correlationID)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"topic":  topic,
		"limit":  limit,
		"count":  len(msgs),
		"messages": msgs,
	})
}

func (s *server) handleS3Head(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	bucket := r.URL.Query().Get("bucket")
	key := r.URL.Query().Get("key")
	if bucket == "" || key == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("bucket and key are required"))
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	info, err := s.headObject(ctx, bucket, key)
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}
	info.ConsoleURL = fmt.Sprintf("%s/browser/%s/%s", s.cfg.MinIOConsole, bucket, key)
	writeJSON(w, http.StatusOK, info)
}

func (s *server) handleAgentModes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"modes": s.modes.listPresets(),
	})
}

func (s *server) handleClusterInventory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	includeSystem := r.URL.Query().Get("include_system") == "1" ||
		strings.EqualFold(r.URL.Query().Get("include_system"), "true")
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	inv, err := s.modes.inventory(ctx, includeSystem)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, inv)
}

func (s *server) handleAgentMode(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		st, err := s.modes.status(ctx)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, st)
	case http.MethodPost:
		var req applyAgentModeRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON body: %w", err))
			return
		}
		if strings.TrimSpace(req.Mode) == "" {
			writeError(w, http.StatusBadRequest, fmt.Errorf("mode is required"))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
		defer cancel()
		result, err := s.modes.applyMode(ctx, req.Mode)
		if err != nil {
			writeError(w, http.StatusBadGateway, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}
