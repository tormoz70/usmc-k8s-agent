package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/usmc/usmc-k8s-agent/hack/mockcorelib"
)

// Scenario is a UI→agent stub: core is a black box; we only exercise agent Kafka/S3/REST.
type Scenario struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Description string `json:"description"`
	Channel     string `json:"channel"` // kafka | rest | flow
	Group       string `json:"group"`
	Fixture     string `json:"fixture,omitempty"`
	RESTPath    string `json:"rest_path,omitempty"`
	RESTMethod  string `json:"rest_method,omitempty"`
	ExpectS3    bool   `json:"expect_s3,omitempty"`
	WatchTopic  string `json:"watch_topic,omitempty"`
}

func (s *server) scenarios() []Scenario {
	return []Scenario{
		{
			ID:          "ui-list-namespaces",
			Title:       "UI: список Namespace",
			Description: "Как будто UI запросил inventory namespaces через core → Kafka k8s.api",
			Channel:     "kafka",
			Group:       "Kafka / inventory",
			Fixture:     "k8s-api-list-namespaces.json",
			WatchTopic:  s.cfg.ReplyTopic,
		},
		{
			ID:          "ui-list-pods",
			Title:       "UI: список Pod",
			Description: "Inventory pods (k8s.api GET)",
			Channel:     "kafka",
			Group:       "Kafka / inventory",
			Fixture:     "k8s-api-list-pods.json",
			WatchTopic:  s.cfg.ReplyTopic,
		},
		{
			ID:          "ui-list-deployments",
			Title:       "UI: список Deployment",
			Description: "Inventory deployments в test-namespace-1",
			Channel:     "kafka",
			Group:       "Kafka / inventory",
			Fixture:     "k8s-api-list-deployments.json",
			WatchTopic:  s.cfg.ReplyTopic,
		},
		{
			ID:          "ui-list-services",
			Title:       "UI: список Service",
			Description: "Inventory services",
			Channel:     "kafka",
			Group:       "Kafka / inventory",
			Fixture:     "k8s-api-list-services.json",
			WatchTopic:  s.cfg.ReplyTopic,
		},
		{
			ID:          "ui-watch-pods",
			Title:       "UI: подписка на Pod events",
			Description: "watch.subscribe → события в cluster.events",
			Channel:     "kafka",
			Group:       "Kafka / watch",
			Fixture:     "watch-subscribe-pods.json",
			WatchTopic:  "cluster.events",
		},
		{
			ID:          "ui-watch-pods-stop",
			Title:       "UI: отписка Pod events",
			Description: "watch.unsubscribe",
			Channel:     "kafka",
			Group:       "Kafka / watch",
			Fixture:     "watch-unsubscribe-pods.json",
			WatchTopic:  s.cfg.ReplyTopic,
		},
		{
			ID:          "ui-logs-collect",
			Title:       "UI: скачать логи в S3",
			Description: "logs.collect → zip в MinIO; потом проверьте S3 Check",
			Channel:     "kafka",
			Group:       "Kafka / logs + S3",
			Fixture:     "logs-collect.json",
			ExpectS3:    true,
			WatchTopic:  s.cfg.ReplyTopic,
		},
		{
			ID:          "ui-logs-stream-start",
			Title:       "UI: стрим логов (start)",
			Description: "logs.stream.start → строки в logs.stream",
			Channel:     "kafka",
			Group:       "Kafka / logs + S3",
			Fixture:     "logs-stream-start-logger-a.json",
			WatchTopic:  "logs.stream",
		},
		{
			ID:          "ui-logs-stream-stop",
			Title:       "UI: стрим логов (stop)",
			Description: "logs.stream.stop",
			Channel:     "kafka",
			Group:       "Kafka / logs + S3",
			Fixture:     "logs-stream-stop-logger-a.json",
			WatchTopic:  s.cfg.ReplyTopic,
		},
		{
			ID:          "ui-cache-put",
			Title:       "UI/core: положить в cache",
			Description: "cache.put по Kafka; значение читается REST GET /v1/cache",
			Channel:     "kafka",
			Group:       "Kafka / cache",
			Fixture:     "cache-put.json",
			WatchTopic:  s.cfg.ReplyTopic,
		},
		{
			ID:          "ui-cache-delete",
			Title:       "UI/core: удалить из cache",
			Description: "cache.delete",
			Channel:     "kafka",
			Group:       "Kafka / cache",
			Fixture:     "cache-delete.json",
			WatchTopic:  s.cfg.ReplyTopic,
		},
		{
			ID:          "ui-health-report-start",
			Title:       "UI: health report start",
			Description: "health.report.start → cluster.health",
			Channel:     "kafka",
			Group:       "Kafka / health",
			Fixture:     "health-report-start.json",
			WatchTopic:  "cluster.health",
		},
		{
			ID:          "ui-health-report-stop",
			Title:       "UI: health report stop",
			Description: "health.report.stop",
			Channel:     "kafka",
			Group:       "Kafka / health",
			Fixture:     "health-report-stop.json",
			WatchTopic:  s.cfg.ReplyTopic,
		},
		{
			ID:          "rest-healthz",
			Title:       "REST: agent /healthz",
			Description: "Прямой probe HTTP агента (как sidecar/ingress), без Kafka",
			Channel:     "rest",
			Group:       "REST agent",
			RESTPath:    "/healthz",
			RESTMethod:  http.MethodGet,
		},
		{
			ID:          "rest-readyz",
			Title:       "REST: agent /readyz",
			Description: "Readiness probe агента",
			Channel:     "rest",
			Group:       "REST agent",
			RESTPath:    "/readyz",
			RESTMethod:  http.MethodGet,
		},
		{
			ID:          "rest-metrics",
			Title:       "REST: agent /metrics",
			Description: "Prometheus metrics endpoint",
			Channel:     "rest",
			Group:       "REST agent",
			RESTPath:    "/metrics",
			RESTMethod:  http.MethodGet,
		},
		{
			ID:          "rest-cache-list",
			Title:       "REST: GET /v1/cache",
			Description: "Список ключей in-memory cache (после cache.put)",
			Channel:     "rest",
			Group:       "REST agent",
			RESTPath:    "/v1/cache",
			RESTMethod:  http.MethodGet,
		},
		{
			ID:          "flow-logs-to-s3",
			Title:       "Flow: logs.collect → проверить S3",
			Description: "Один клик: Kafka logs.collect, затем HEAD объекта в MinIO из ответа",
			Channel:     "flow",
			Group:       "Flows",
			Fixture:     "logs-collect.json",
			ExpectS3:    true,
			WatchTopic:  s.cfg.ReplyTopic,
		},
		{
			ID:          "flow-cache-put-get",
			Title:       "Flow: cache.put → REST GET",
			Description: "Kafka cache.put, затем REST чтение /v1/cache/{key}",
			Channel:     "flow",
			Group:       "Flows",
			Fixture:     "cache-put.json",
			WatchTopic:  s.cfg.ReplyTopic,
		},
	}
}

func (s *server) scenarioByID(id string) (*Scenario, error) {
	for i := range s.scenarios() {
		sc := s.scenarios()[i]
		if sc.ID == id {
			return &sc, nil
		}
	}
	return nil, fmt.Errorf("unknown scenario %q", id)
}

func (s *server) handleScenarios(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"scenarios":       s.scenarios(),
		"agent_http_url":  s.cfg.AgentHTTPURL,
		"request_topic":   s.cfg.RequestTopic,
		"reply_topic":     s.cfg.ReplyTopic,
		"core_is_blackbox": true,
		"note":            "Stubs simulate UI/core requests; only k8s-agent Kafka/S3/REST are exercised",
	})
}

type runScenarioRequest struct {
	ID         string `json:"id"`
	ReplyTopic string `json:"reply_topic,omitempty"`
}

type runScenarioResponse struct {
	ScenarioID    string          `json:"scenario_id"`
	Channel       string          `json:"channel"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	ReplyTopic    string          `json:"reply_topic,omitempty"`
	WatchTopic    string          `json:"watch_topic,omitempty"`
	KafkaResult   json.RawMessage `json:"kafka_result,omitempty"`
	REST          *restProbeResult `json:"rest,omitempty"`
	S3            *s3ObjectInfo   `json:"s3,omitempty"`
	Hints         []string        `json:"hints,omitempty"`
}

func (s *server) handleScenarioRun(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req runScenarioRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("invalid JSON: %w", err))
		return
	}
	sc, err := s.scenarioByID(strings.TrimSpace(req.ID))
	if err != nil {
		writeError(w, http.StatusNotFound, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()

	out := &runScenarioResponse{
		ScenarioID: sc.ID,
		Channel:    sc.Channel,
		WatchTopic: sc.WatchTopic,
		Hints:      []string{},
	}

	switch sc.Channel {
	case "rest":
		rest, err := s.probeAgentREST(ctx, sc.RESTMethod, sc.RESTPath)
		if err != nil {
			writeError(w, http.StatusBadGateway, err)
			return
		}
		out.REST = rest
	case "kafka":
		if err := s.runKafkaScenario(ctx, sc, req.ReplyTopic, out); err != nil {
			writeError(w, http.StatusBadGateway, err)
			return
		}
	case "flow":
		if err := s.runKafkaScenario(ctx, sc, req.ReplyTopic, out); err != nil {
			writeError(w, http.StatusBadGateway, err)
			return
		}
		switch sc.ID {
		case "flow-logs-to-s3":
			if bucket, key, ok := extractS3FromKafkaResult(out.KafkaResult); ok {
				info, err := s.headObject(ctx, bucket, key)
				if err != nil {
					out.Hints = append(out.Hints, "S3 object not ready yet: "+err.Error())
				} else {
					info.ConsoleURL = fmt.Sprintf("%s/browser/%s/%s", s.cfg.MinIOConsole, bucket, key)
					out.S3 = info
				}
			} else {
				out.Hints = append(out.Hints, "reply did not contain s3_bucket/s3_key — check Kafka Monitor")
			}
		case "flow-cache-put-get":
			key := extractCacheKeyFromFixture(sc.Fixture, s.cfg.FixturesDir)
			if key == "" {
				out.Hints = append(out.Hints, "could not detect cache key from fixture")
				break
			}
			rest, err := s.probeAgentREST(ctx, http.MethodGet, "/v1/cache/"+key)
			if err != nil {
				out.Hints = append(out.Hints, "REST cache get failed: "+err.Error())
			} else {
				out.REST = rest
			}
		}
	default:
		writeError(w, http.StatusBadRequest, fmt.Errorf("unsupported channel %q", sc.Channel))
		return
	}

	if sc.ExpectS3 && out.S3 == nil {
		out.Hints = append(out.Hints, "После ответа откройте вкладку S3 Check (bucket/key из reply)")
	}
	if sc.WatchTopic != "" && sc.WatchTopic != out.ReplyTopic {
		out.Hints = append(out.Hints, "Смотрите топик "+sc.WatchTopic+" во вкладке Kafka Monitor")
	}

	writeJSON(w, http.StatusOK, out)
}

func (s *server) runKafkaScenario(ctx context.Context, sc *Scenario, replyTopic string, out *runScenarioResponse) error {
	body, err := os.ReadFile(filepath.Join(s.cfg.FixturesDir, sc.Fixture))
	if err != nil {
		return fmt.Errorf("fixture %s: %w", sc.Fixture, err)
	}
	if replyTopic == "" {
		replyTopic = s.cfg.ReplyTopic
	}
	result, err := mockcorelib.SendCommand(ctx, s.cfg.Brokers, s.cfg.RequestTopic, replyTopic, body)
	if err != nil {
		return err
	}
	out.ReplyTopic = replyTopic
	out.CorrelationID = result.CorrelationID

	var requestBody any
	if json.Unmarshal(body, &requestBody) != nil {
		requestBody = string(body)
	}
	payload := map[string]any{
		"correlation_id": result.CorrelationID,
		"reply_topic":    result.ReplyTopic,
		"request_topic":  result.RequestTopic,
		"request": map[string]any{
			"topic":   result.RequestTopic,
			"fixture": sc.Fixture,
			"body":    requestBody,
		},
	}

	var reply mockcorelib.Message
	err = mockcorelib.ListenOnce(s.cfg.Brokers, replyTopic, result.CorrelationID, 45*time.Second, func(m mockcorelib.Message) {
		reply = m
	})
	if err != nil {
		payload["wait_error"] = err.Error()
		out.Hints = append(out.Hints, "reply timeout: "+err.Error())
	} else {
		var replyBody any
		if json.Unmarshal(reply.Body, &replyBody) == nil {
			payload["reply"] = map[string]any{
				"topic":          replyTopic,
				"correlation_id": reply.CorrelationID,
				"headers":        reply.Headers,
				"body":           replyBody,
			}
			payload["body"] = replyBody
		} else {
			payload["reply"] = map[string]any{
				"topic":          replyTopic,
				"correlation_id": reply.CorrelationID,
				"headers":        reply.Headers,
				"body":           string(reply.Body),
			}
			payload["body"] = json.RawMessage(reply.Body)
		}
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	out.KafkaResult = raw
	return nil
}

type restProbeResult struct {
	URL        string `json:"url"`
	Method     string `json:"method"`
	StatusCode int    `json:"status_code"`
	Body       string `json:"body"`
	Truncated  bool   `json:"truncated,omitempty"`
}

func (s *server) probeAgentREST(ctx context.Context, method, path string) (*restProbeResult, error) {
	base := strings.TrimRight(s.cfg.AgentHTTPURL, "/")
	if base == "" {
		return nil, fmt.Errorf("AGENT_HTTP_URL is not set (example: http://host.docker.internal:8080)")
	}
	if method == "" {
		method = http.MethodGet
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	url := base + path
	req, err := http.NewRequestWithContext(ctx, method, url, nil)
	if err != nil {
		return nil, err
	}
	if tok := s.cfg.AgentHTTPBearer; tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w — agent HTTP is ClusterIP; run: powershell -File hack/port-forward-agent-http.ps1 (exposes localhost:8080)", err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	out := &restProbeResult{
		URL:        url,
		Method:     method,
		StatusCode: resp.StatusCode,
		Body:       string(data),
	}
	if resp.ContentLength > 64<<10 || len(data) >= 64<<10 {
		out.Truncated = true
	}
	return out, nil
}

func (s *server) handleAgentRESTProbe(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	path := r.URL.Query().Get("path")
	if path == "" {
		path = "/healthz"
	}
	method := r.URL.Query().Get("method")
	if method == "" {
		method = http.MethodGet
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	res, err := s.probeAgentREST(ctx, method, path)
	if err != nil {
		writeError(w, http.StatusBadGateway, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func extractS3FromKafkaResult(raw json.RawMessage) (bucket, key string, ok bool) {
	if len(raw) == 0 {
		return "", "", false
	}
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return "", "", false
	}
	candidates := []json.RawMessage{raw}
	if b, found := envelope["body"]; found {
		candidates = append(candidates, b)
		var asStr string
		if json.Unmarshal(b, &asStr) == nil {
			candidates = append(candidates, json.RawMessage(asStr))
		}
	}
	if b, found := envelope["reply"]; found {
		var reply struct {
			Body json.RawMessage `json:"body"`
		}
		if json.Unmarshal(b, &reply) == nil && len(reply.Body) > 0 {
			candidates = append(candidates, reply.Body)
			var asStr string
			if json.Unmarshal(reply.Body, &asStr) == nil {
				candidates = append(candidates, json.RawMessage(asStr))
			}
		}
	}
	for _, c := range candidates {
		var probe struct {
			S3Bucket string `json:"s3_bucket"`
			S3Key    string `json:"s3_key"`
			Payload  struct {
				S3Bucket string `json:"s3_bucket"`
				S3Key    string `json:"s3_key"`
			} `json:"payload"`
			Result struct {
				S3Bucket string `json:"s3_bucket"`
				S3Key    string `json:"s3_key"`
			} `json:"result"`
		}
		if json.Unmarshal(c, &probe) != nil {
			continue
		}
		if probe.S3Bucket != "" && probe.S3Key != "" {
			return probe.S3Bucket, probe.S3Key, true
		}
		if probe.Payload.S3Bucket != "" && probe.Payload.S3Key != "" {
			return probe.Payload.S3Bucket, probe.Payload.S3Key, true
		}
		if probe.Result.S3Bucket != "" && probe.Result.S3Key != "" {
			return probe.Result.S3Bucket, probe.Result.S3Key, true
		}
	}
	return "", "", false
}

func extractCacheKeyFromFixture(name, dir string) string {
	data, err := os.ReadFile(filepath.Join(dir, name))
	if err != nil {
		return ""
	}
	var probe struct {
		Payload struct {
			Entries []struct {
				Key string `json:"key"`
			} `json:"entries"`
			Keys []string `json:"keys"`
		} `json:"payload"`
	}
	if json.Unmarshal(data, &probe) != nil {
		return ""
	}
	if len(probe.Payload.Entries) > 0 {
		return probe.Payload.Entries[0].Key
	}
	if len(probe.Payload.Keys) > 0 {
		return probe.Payload.Keys[0]
	}
	return ""
}
