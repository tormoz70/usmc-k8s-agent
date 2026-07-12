package logstream

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/observability"
	"github.com/usmc/usmc-k8s-agent/internal/policy"
)

const (
	SchemaVersionV1 = "v1"
	batchSize       = 100
)

// StreamPublisher publishes batched log lines to Kafka.
type StreamPublisher interface {
	PublishEvent(ctx context.Context, topic, key string, event any) error
}

type StartPayload struct {
	SubscriptionID string `json:"subscription_id"`
	Namespace      string `json:"namespace"`
	Pod            string `json:"pod"`
	Container      string `json:"container"`
	Follow         bool   `json:"follow"`
	TailLines      *int64 `json:"tail_lines"`
	SinceSeconds   *int64 `json:"since_seconds"`
	OutputTopic    string `json:"output_topic"`
	TTLSeconds     int    `json:"ttl_seconds"`
}

type StopPayload struct {
	SubscriptionID string `json:"subscription_id"`
}

type LogMessage struct {
	SchemaVersion  string    `json:"schema_version"`
	SubscriptionID string    `json:"subscription_id"`
	Namespace      string    `json:"namespace"`
	Pod            string    `json:"pod"`
	Container      string    `json:"container"`
	Timestamp      time.Time `json:"timestamp"`
	Lines          []string  `json:"lines"`
}

type subscription struct {
	payload *StartPayload
	cancel  context.CancelFunc
	podKey  string
}

// Manager owns log stream subscriptions.
type Manager struct {
	clusterID    string
	defaultTopic string
	kube         kubernetes.Interface
	policy       *policy.Engine
	publisher    StreamPublisher
	maxPerPod    int
	backlogMax   int
	metrics      *observability.Metrics
	log          *slog.Logger

	mu      sync.Mutex
	subs    map[string]*subscription
	podSubs map[string]string
}

func NewManager(clusterID, defaultTopic string, kube kubernetes.Interface, engine *policy.Engine, publisher StreamPublisher, maxPerPod, backlogMax int, metrics *observability.Metrics, log *slog.Logger) *Manager {
	if maxPerPod < 1 {
		maxPerPod = 1
	}
	if backlogMax < 1 {
		backlogMax = 1000
	}
	if log == nil {
		log = slog.Default()
	}
	return &Manager{
		clusterID:    clusterID,
		defaultTopic: defaultTopic,
		kube:         kube,
		policy:       engine,
		publisher:    publisher,
		maxPerPod:    maxPerPod,
		backlogMax:   backlogMax,
		metrics:      metrics,
		log:          log,
		subs:         make(map[string]*subscription),
		podSubs:      make(map[string]string),
	}
}

func ParseStartPayload(raw json.RawMessage) (*StartPayload, error) {
	var p StartPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	if p.SubscriptionID == "" {
		return nil, fmt.Errorf("subscription_id is required")
	}
	if p.Namespace == "" || p.Pod == "" {
		return nil, fmt.Errorf("namespace and pod are required")
	}
	if p.Container == "" {
		return nil, fmt.Errorf("container is required")
	}
	return &p, nil
}

func ParseStopPayload(raw json.RawMessage) (*StopPayload, error) {
	var p StopPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	if p.SubscriptionID == "" {
		return nil, fmt.Errorf("subscription_id is required")
	}
	return &p, nil
}

func (p *StartPayload) OutputTopicOr(defaultTopic string) string {
	if p.OutputTopic != "" {
		return p.OutputTopic
	}
	return defaultTopic
}

func (m *Manager) Start(_ context.Context, payload *StartPayload) error {
	if err := m.policy.AllowCommandType(command.TypeLogsStreamStart); err != nil {
		return err
	}
	if err := m.policy.AllowNamespace(payload.Namespace); err != nil {
		return err
	}

	podKey := payload.Namespace + "/" + payload.Pod
	m.mu.Lock()
	if _, exists := m.subs[payload.SubscriptionID]; exists {
		m.mu.Unlock()
		return fmt.Errorf("subscription %q already exists", payload.SubscriptionID)
	}
	if existing, ok := m.podSubs[podKey]; ok {
		m.mu.Unlock()
		return fmt.Errorf("pod %q already has an active stream subscription %q", podKey, existing)
	}

	// Do not derive from the command request context — it is cancelled when the reply is sent.
	subCtx, cancel := subscriptionContext(payload.TTLSeconds)
	sub := &subscription{payload: payload, cancel: cancel, podKey: podKey}
	m.subs[payload.SubscriptionID] = sub
	m.podSubs[podKey] = payload.SubscriptionID
	m.mu.Unlock()

	go m.run(subCtx, sub)
	return nil
}

func subscriptionContext(ttlSeconds int) (context.Context, context.CancelFunc) {
	if ttlSeconds > 0 {
		return context.WithTimeout(context.Background(), time.Duration(ttlSeconds)*time.Second)
	}
	return context.WithCancel(context.Background())
}

func (m *Manager) Stop(subscriptionID string) error {
	m.mu.Lock()
	sub, ok := m.subs[subscriptionID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("subscription %q not found", subscriptionID)
	}
	delete(m.subs, subscriptionID)
	if m.podSubs[sub.podKey] == subscriptionID {
		delete(m.podSubs, sub.podKey)
	}
	m.mu.Unlock()
	sub.cancel()
	return nil
}

func (m *Manager) StopAll() {
	m.mu.Lock()
	subs := m.subs
	m.subs = make(map[string]*subscription)
	m.podSubs = make(map[string]string)
	m.mu.Unlock()
	for _, sub := range subs {
		sub.cancel()
	}
}

func (m *Manager) ActiveCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.subs)
}

func (m *Manager) run(ctx context.Context, sub *subscription) {
	defer func() {
		m.mu.Lock()
		delete(m.subs, sub.payload.SubscriptionID)
		if m.podSubs[sub.podKey] == sub.payload.SubscriptionID {
			delete(m.podSubs, sub.podKey)
		}
		m.mu.Unlock()
		sub.cancel()
	}()

	payload := sub.payload
	opts := &corev1.PodLogOptions{
		Container: payload.Container,
		Follow:    payload.Follow,
	}
	if payload.TailLines != nil {
		opts.TailLines = payload.TailLines
	}
	if payload.SinceSeconds != nil {
		opts.SinceSeconds = payload.SinceSeconds
	}

	req := m.kube.CoreV1().Pods(payload.Namespace).GetLogs(payload.Pod, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		m.log.Warn("log stream open failed", "subscription_id", payload.SubscriptionID, "error", err)
		return
	}
	defer stream.Close()

	reader := bufio.NewReader(stream)
	topic := payload.OutputTopicOr(m.defaultTopic)
	key := fmt.Sprintf("%s/%s/v1/pod/%s/container/%s", m.clusterID, payload.Namespace, payload.Pod, payload.Container)

	var pending []string
	flush := func(force bool) {
		for len(pending) >= batchSize || (force && len(pending) > 0) {
			n := batchSize
			if len(pending) < n {
				n = len(pending)
			}
			if n == 0 {
				return
			}
			msg := LogMessage{
				SchemaVersion:  SchemaVersionV1,
				SubscriptionID: payload.SubscriptionID,
				Namespace:      payload.Namespace,
				Pod:            payload.Pod,
				Container:      payload.Container,
				Timestamp:      time.Now().UTC(),
				Lines:          append([]string(nil), pending[:n]...),
			}
			pending = pending[n:]
			pubCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := m.publisher.PublishEvent(pubCtx, topic, key, msg)
			cancel()
			if err != nil {
				m.log.Warn("log stream publish failed", "subscription_id", payload.SubscriptionID, "error", err)
			}
			if !force && len(pending) < batchSize {
				return
			}
		}
	}
	appendLine := func(line string) {
		pending = append(pending, line)
		for len(pending) > m.backlogMax {
			pending = pending[1:]
			if m.metrics != nil {
				m.metrics.LogStreamDroppedLines.Inc()
			}
		}
		if len(pending) >= batchSize {
			flush(false)
		}
	}

	lineCh := make(chan string, m.backlogMax)
	readErrCh := make(chan error, 1)
	go func() {
		defer close(lineCh)
		for {
			line, err := reader.ReadString('\n')
			if len(line) > 0 {
				select {
				case lineCh <- strings.TrimRight(line, "\r\n"):
				case <-ctx.Done():
					return
				}
			}
			if err != nil {
				if err != io.EOF {
					readErrCh <- err
				}
				return
			}
		}
	}()

	// Flush partial batches so follow streams publish sooner than batchSize lines.
	flushTicker := time.NewTicker(time.Second)
	defer flushTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			flush(true)
			return
		case <-flushTicker.C:
			flush(true)
		case err := <-readErrCh:
			if ctx.Err() == nil {
				m.log.Warn("log stream read failed", "subscription_id", payload.SubscriptionID, "error", err)
			}
			flush(true)
			return
		case line, ok := <-lineCh:
			if !ok {
				flush(true)
				return
			}
			appendLine(line)
		}
	}
}
