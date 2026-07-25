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
	cfg       config
	modes     *agentModeController
	targets   *targetRegistry
	resources *resourcesController
}

func newServer(cfg config) *server {
	fallback := agentTarget{
		ID:              "default",
		Label:           "Default agent",
		Implementation:  "v1",
		ClusterID:       "local",
		RequestTopic:    cfg.RequestTopic,
		ReplyTopic:      cfg.ReplyTopic,
		AgentNamespace:  cfg.AgentNamespace,
		AgentHTTPURL:    cfg.AgentHTTPURL,
		EventsTopic:     "cluster.events",
		LogsStreamTopic: "logs.stream",
		HealthTopic:     "cluster.health",
		LifecycleTopic:  "agent.lifecycle",
	}
	reg, err := loadTargets(cfg.TargetsFile, fallback)
	if err != nil {
		slog.Warn("targets load failed; using default", "error", err)
		reg, _ = loadTargets("", fallback)
	}
	modes := newAgentModeController(cfg)
	return &server{
		cfg:       cfg,
		modes:     modes,
		targets:   reg,
		resources: newResourcesController(cfg, modes),
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
	t, err := s.targets.Current()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	topics := t.MonitorTopics()
	if len(topics) == 0 {
		topics = s.cfg.DefaultTopics
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"topics":        topics,
		"reply_topic":   t.ReplyTopic,
		"request_topic": t.RequestTopic,
		"target_id":     t.ID,
	})
}

func (s *server) handleTargets(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	cur, _ := s.targets.Current()
	writeJSON(w, http.StatusOK, map[string]any{
		"targets":     s.targets.List(),
		"selected_id": cur.ID,
		"selected":    cur,
	})
}

func (s *server) handleTargetSelect(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON: %w", err))
		return
	}
	req.ID = strings.TrimSpace(req.ID)
	if req.ID == "" {
		writeError(w, http.StatusBadRequest, fmt.Errorf("id is required"))
		return
	}
	t, err := s.targets.Select(req.ID)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	// Keep Agent Mode / inventory scoped to the selected agent namespace.
	s.cfg.AgentNamespace = t.AgentNamespace
	if t.AgentHTTPURL != "" {
		s.cfg.AgentHTTPURL = t.AgentHTTPURL
	}
	s.modes.cfg.AgentNamespace = t.AgentNamespace
	s.resources.cfg.AgentNamespace = t.AgentNamespace
	s.resources.cfg.AgentHTTPURL = s.cfg.AgentHTTPURL
	slog.Info("agent target selected", "id", t.ID, "implementation", t.Implementation, "request_topic", t.RequestTopic, "namespace", t.AgentNamespace)
	writeJSON(w, http.StatusOK, map[string]any{"selected": t})
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
		if t, err := s.targets.Current(); err == nil && t.ReplyTopic != "" {
			replyTopic = t.ReplyTopic
		} else {
			replyTopic = s.cfg.ReplyTopic
		}
	}
	requestTopic := s.cfg.RequestTopic
	if t, err := s.targets.Current(); err == nil && t.RequestTopic != "" {
		requestTopic = t.RequestTopic
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	sentAt := time.Now().UTC()
	result, reply, waitErr := mockcorelib.SendAndWait(ctx, s.cfg.Brokers, requestTopic, replyTopic, req.Body, 45*time.Second)
	receivedAt := time.Now().UTC()
	if result.CorrelationID == "" && waitErr != nil {
		writeError(w, http.StatusBadGateway, waitErr)
		return
	}
	timing := map[string]any{
		"sent_at":     sentAt,
		"received_at": receivedAt,
		"duration_ms": receivedAt.Sub(sentAt).Milliseconds(),
	}
	if waitErr != nil {
		timing["timed_out"] = true
		timing["wait_error"] = waitErr.Error()
	}

	resp := map[string]any{
		"correlation_id": result.CorrelationID,
		"reply_topic":    result.ReplyTopic,
		"request_topic":  result.RequestTopic,
		"timing":         timing,
	}
	if t, err := s.targets.Current(); err == nil {
		resp["target_id"] = t.ID
		resp["implementation"] = t.Implementation
	}
	if waitErr == nil {
		var replyBody any
		if json.Unmarshal(reply.Body, &replyBody) == nil {
			resp["reply"] = map[string]any{
				"headers": reply.Headers,
				"body":    replyBody,
			}
		} else {
			resp["reply"] = map[string]any{
				"headers": reply.Headers,
				"body":    string(reply.Body),
			}
		}
	}
	writeJSON(w, http.StatusOK, resp)
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

func (s *server) handleResourcesProfiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"profiles": resourceProfiles()})
}

func (s *server) handleResourcesSnapshot(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ns := r.URL.Query().Get("namespace")
	targetID := ""
	httpURL := s.cfg.AgentHTTPURL
	if ns == "" {
		if t, err := s.targets.Current(); err == nil {
			ns = t.AgentNamespace
			targetID = t.ID
			if t.AgentHTTPURL != "" {
				httpURL = t.AgentHTTPURL
			}
		} else {
			ns = s.cfg.AgentNamespace
		}
	} else if t, err := s.targets.Current(); err == nil {
		targetID = t.ID
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	writeJSON(w, http.StatusOK, s.resources.snapshot(ctx, ns, targetID, httpURL))
}

func (s *server) handleResourcesCompare(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	type side struct {
		Target   agentTarget        `json:"target"`
		Snapshot *resourcesSnapshot `json:"snapshot"`
	}
	var sides []side
	for _, t := range s.targets.List() {
		url := t.AgentHTTPURL
		if url == "" {
			url = s.cfg.AgentHTTPURL
		}
		sides = append(sides, side{
			Target:   t,
			Snapshot: s.resources.snapshot(ctx, t.AgentNamespace, t.ID, url),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"sides": sides})
}

func (s *server) handleResourcesProfile(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, map[string]any{"profiles": resourceProfiles()})
	case http.MethodPost:
		var req applyResourceProfileRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON: %w", err))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
		defer cancel()
		result, err := s.resources.applyProfile(ctx, strings.TrimSpace(req.Profile))
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
