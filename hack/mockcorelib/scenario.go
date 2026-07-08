package mockcorelib

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const DefaultScenariosFile = "test/scenarios/scenarios.yaml"

// ScenarioExpect defines assertions on the sync reply message body.
type ScenarioExpect struct {
	Status     string            `yaml:"status"`
	HTTPStatus int               `yaml:"http_status"`
	BodyContains []string        `yaml:"body_contains"`
	Fields     map[string]string `yaml:"fields"`
}

// ScenarioStream defines assertions on Kafka event/stream topic messages.
type ScenarioStream struct {
	Topic       string        `yaml:"topic"`
	Timeout     time.Duration `yaml:"timeout"`
	MinMessages int           `yaml:"min_messages"`
	BodyContains []string     `yaml:"body_contains"`
}

// ScenarioTriggerArgs holds optional trigger parameters from scenarios.yaml.
type ScenarioTriggerArgs struct {
	Namespace string `yaml:"namespace"`
	PodName   string `yaml:"pod_name"`
}

// Scenario describes one E2E mock-core test case.
type Scenario struct {
	ID          string              `yaml:"id"`
	Name        string              `yaml:"name"`
	Command     string              `yaml:"command"`
	Cleanup     string              `yaml:"cleanup"`
	ReplyTimeout time.Duration      `yaml:"reply_timeout"`
	Expect      ScenarioExpect        `yaml:"expect"`
	Stream      *ScenarioStream       `yaml:"stream"`
	Trigger     string                `yaml:"trigger"`
	TriggerArgs ScenarioTriggerArgs   `yaml:"trigger_args"`
}

// ScenarioCatalog is the root document in scenarios.yaml.
type ScenarioCatalog struct {
	Scenarios []Scenario `yaml:"scenarios"`
}

// LoadScenarios reads scenario definitions from a YAML file.
func LoadScenarios(path string) (*ScenarioCatalog, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read scenarios file: %w", err)
	}
	var catalog ScenarioCatalog
	if err := yaml.Unmarshal(data, &catalog); err != nil {
		return nil, fmt.Errorf("parse scenarios file: %w", err)
	}
	if len(catalog.Scenarios) == 0 {
		return nil, fmt.Errorf("no scenarios defined in %s", path)
	}
	for i := range catalog.Scenarios {
		if catalog.Scenarios[i].ReplyTimeout <= 0 {
			catalog.Scenarios[i].ReplyTimeout = 2 * time.Minute
		}
		if catalog.Scenarios[i].Stream != nil && catalog.Scenarios[i].Stream.Timeout <= 0 {
			catalog.Scenarios[i].Stream.Timeout = 60 * time.Second
		}
	}
	return &catalog, nil
}

// FindScenario returns a scenario by id.
func (c *ScenarioCatalog) FindScenario(id string) (*Scenario, error) {
	for i := range c.Scenarios {
		if c.Scenarios[i].ID == id {
			return &c.Scenarios[i], nil
		}
	}
	return nil, fmt.Errorf("scenario %q not found", id)
}

// RunScenario executes one scenario end-to-end against a live stack.
func RunScenario(ctx context.Context, brokers []string, requestTopic, replyTopic, repoRoot string, s *Scenario) error {
	cmdPath := resolvePath(repoRoot, s.Command)
	body, err := os.ReadFile(cmdPath)
	if err != nil {
		return fmt.Errorf("read command file: %w", err)
	}

	sendCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	result, err := SendCommand(sendCtx, brokers, requestTopic, replyTopic, body)
	if err != nil {
		return fmt.Errorf("send command: %w", err)
	}

	streamCtx, streamCancel := context.WithCancel(ctx)
	streamDone := make(chan error, 1)
	var streamMessages []Message
	if s.Stream != nil {
		go func() {
			msgs, err := collectStreamMessages(streamCtx, brokers, s.Stream.Topic, "", s.Stream.MinMessages, s.Stream.Timeout)
			streamMessages = msgs
			streamDone <- err
		}()
	}

	reply, err := waitForReply(brokers, replyTopic, result.CorrelationID, s.ReplyTimeout)
	if err != nil {
		streamCancel()
		return err
	}
	if err := assertExpectation(reply, s.Expect); err != nil {
		streamCancel()
		return fmt.Errorf("reply assertion: %w", err)
	}

	if s.Trigger != "" {
		if err := runTrigger(ctx, s); err != nil {
			streamCancel()
			return fmt.Errorf("trigger %q: %w", s.Trigger, err)
		}
	}

	if s.Stream != nil {
		subscriptionID := extractField(reply.Body, "subscription_id")
		streamCancel()
		if err := <-streamDone; err != nil {
			return fmt.Errorf("stream %q: %w", s.Stream.Topic, err)
		}
		if subscriptionID != "" {
			filtered := make([]Message, 0, len(streamMessages))
			for _, msg := range streamMessages {
				if strings.Contains(string(msg.Body), subscriptionID) {
					filtered = append(filtered, msg)
				}
			}
			streamMessages = filtered
		}
		if len(streamMessages) < s.Stream.MinMessages {
			return fmt.Errorf("stream %q: got %d messages, want >= %d", s.Stream.Topic, len(streamMessages), s.Stream.MinMessages)
		}
		for _, needle := range s.Stream.BodyContains {
			found := false
			for _, msg := range streamMessages {
				if strings.Contains(string(msg.Body), needle) {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("stream %q: no message contains %q", s.Stream.Topic, needle)
			}
		}
	}

	if s.Cleanup != "" {
		cleanupPath := resolvePath(repoRoot, s.Cleanup)
		cleanupBody, err := os.ReadFile(cleanupPath)
		if err != nil {
			return fmt.Errorf("read cleanup file: %w", err)
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(ctx, 30*time.Second)
		defer cleanupCancel()
		cleanupResult, err := SendCommand(cleanupCtx, brokers, requestTopic, replyTopic, cleanupBody)
		if err != nil {
			return fmt.Errorf("send cleanup: %w", err)
		}
		if _, err := waitForReply(brokers, replyTopic, cleanupResult.CorrelationID, s.ReplyTimeout); err != nil {
			return fmt.Errorf("cleanup reply: %w", err)
		}
	}
	return nil
}

// RunAllScenarios executes every scenario in the catalog.
func RunAllScenarios(ctx context.Context, brokers []string, requestTopic, replyTopic, repoRoot string, catalog *ScenarioCatalog) error {
	var failures []string
	for _, scenario := range catalog.Scenarios {
		sc := scenario
		fmt.Printf("== scenario %s: %s\n", sc.ID, sc.Name)
		if err := RunScenario(ctx, brokers, requestTopic, replyTopic, repoRoot, &sc); err != nil {
			failures = append(failures, fmt.Sprintf("%s: %v", sc.ID, err))
			fmt.Printf("FAIL %s: %v\n", sc.ID, err)
			continue
		}
		fmt.Printf("PASS %s\n", sc.ID)
	}
	if len(failures) > 0 {
		return fmt.Errorf("%d scenario(s) failed:\n  %s", len(failures), strings.Join(failures, "\n  "))
	}
	return nil
}

func resolvePath(repoRoot, rel string) string {
	if filepath.IsAbs(rel) {
		return rel
	}
	if repoRoot == "" {
		return rel
	}
	return filepath.Join(repoRoot, rel)
}

func waitForReply(brokers []string, replyTopic, corrID string, timeout time.Duration) (Message, error) {
	var reply Message
	err := ListenOnce(brokers, replyTopic, corrID, timeout, func(m Message) {
		reply = m
	})
	if err != nil {
		return Message{}, fmt.Errorf("wait reply: %w", err)
	}
	return reply, nil
}

func assertExpectation(reply Message, expect ScenarioExpect) error {
	bodyStr := string(reply.Body)
	if expect.Status != "" {
		status := extractField(reply.Body, "status")
		if status != expect.Status {
			return fmt.Errorf("status=%q want %q body=%s", status, expect.Status, bodyStr)
		}
	}
	if expect.HTTPStatus > 0 {
		var partial struct {
			HTTPStatus int `json:"http_status"`
		}
		_ = json.Unmarshal(reply.Body, &partial)
		if partial.HTTPStatus != expect.HTTPStatus {
			return fmt.Errorf("http_status=%d want %d body=%s", partial.HTTPStatus, expect.HTTPStatus, bodyStr)
		}
	}
	for key, want := range expect.Fields {
		got := extractField(reply.Body, key)
		if got != want {
			return fmt.Errorf("field %q=%q want %q body=%s", key, got, want, bodyStr)
		}
	}
	for _, needle := range expect.BodyContains {
		if !strings.Contains(bodyStr, needle) {
			return fmt.Errorf("reply body missing %q", needle)
		}
	}
	return nil
}

func extractField(body json.RawMessage, key string) string {
	var m map[string]any
	if err := json.Unmarshal(body, &m); err != nil {
		return ""
	}
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return fmt.Sprintf("%.0f", t)
	case json.Number:
		return t.String()
	default:
		return fmt.Sprint(v)
	}
}

func collectStreamMessages(ctx context.Context, brokers []string, topic, correlationID string, minMessages int, timeout time.Duration) ([]Message, error) {
	if minMessages < 1 {
		minMessages = 1
	}
	r := NewReader(brokers, topic)
	defer r.Close()
	deadline := time.After(timeout)
	var out []Message
	for len(out) < minMessages {
		select {
		case <-ctx.Done():
			return out, ctx.Err()
		case <-deadline:
			return out, fmt.Errorf("timeout after %s: got %d messages, want >= %d", timeout, len(out), minMessages)
		default:
		}
		readCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		msg, err := r.ReadMessage(readCtx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return out, ctx.Err()
			}
			continue
		}
		if correlationID != "" && HeaderValue(msg.Headers, "correlation_id") != correlationID {
			continue
		}
		out = append(out, FormatMessage(topic, msg))
	}
	return out, nil
}

func runTrigger(ctx context.Context, s *Scenario) error {
	switch s.Trigger {
	case "kubectl_pod_churn":
		return kubectlPodChurn(ctx, s.TriggerArgs.Namespace, s.TriggerArgs.PodName)
	default:
		return fmt.Errorf("unknown trigger %q", s.Trigger)
	}
}

func kubectlPodChurn(ctx context.Context, namespace, podName string) error {
	if namespace == "" || podName == "" {
		return fmt.Errorf("namespace and pod_name are required")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		return fmt.Errorf("kubectl not found in PATH")
	}
	_ = exec.CommandContext(ctx, "kubectl", "delete", "pod", podName, "-n", namespace, "--ignore-not-found", "--wait=false").Run()
	time.Sleep(2 * time.Second)
	cmd := exec.CommandContext(ctx, "kubectl", "run", podName,
		"--image=busybox:1.36",
		"--restart=Never",
		"-n", namespace,
		"--", "sleep", "120")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("kubectl run: %w: %s", err, strings.TrimSpace(string(out)))
	}
	time.Sleep(3 * time.Second)
	go func() {
		time.Sleep(2 * time.Minute)
		_ = exec.Command("kubectl", "delete", "pod", podName, "-n", namespace, "--ignore-not-found", "--wait=false").Run()
	}()
	return nil
}

// FindRepoRoot walks up from cwd looking for go.mod.
func FindRepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("go.mod not found from cwd")
		}
		dir = parent
	}
}

// KafkaReachable returns true when at least one broker accepts a TCP connection.
func KafkaReachable(brokers []string) bool {
	for _, broker := range brokers {
		conn, err := net.DialTimeout("tcp", broker, 2*time.Second)
		if err == nil {
			_ = conn.Close()
			return true
		}
	}
	return false
}
