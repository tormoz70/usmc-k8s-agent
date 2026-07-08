package logstream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"sync"
	"time"

	"github.com/usmc/k8s-agent/internal/command"
	"github.com/usmc/k8s-agent/internal/k8s"
	"github.com/usmc/k8s-agent/internal/metrics"
	"github.com/usmc/k8s-agent/internal/result"
)

type Target struct {
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Container string `json:"container"`
	Previous  bool   `json:"previous"`
}

type SubscribePayload struct {
	SubscriptionID  string   `json:"subscription_id"`
	Targets         []Target `json:"targets"`
	Pattern         string   `json:"pattern"`
	PatternType     string   `json:"pattern_type"`
	CaseInsensitive bool     `json:"case_insensitive"`
	Follow          bool     `json:"follow"`
	SinceSeconds    int64    `json:"since_seconds"`
	TTLSeconds      int64    `json:"ttl_seconds"`
}

type streamHandle struct {
	cancel context.CancelFunc
}

type Manager struct {
	logReader *k8s.LogReader
	events    *result.EventPublisher
	logger    *slog.Logger
	mu        sync.RWMutex
	streams   map[string]*streamHandle
	isLeader  bool
}

func NewManager(logReader *k8s.LogReader, events *result.EventPublisher, logger *slog.Logger) *Manager {
	if logger == nil {
		logger = slog.Default()
	}
	return &Manager{
		logReader: logReader,
		events:    events,
		logger:    logger,
		streams:   make(map[string]*streamHandle),
	}
}

func (m *Manager) SetLeader(v bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isLeader = v
	if !v {
		for id, h := range m.streams {
			h.cancel()
			delete(m.streams, id)
		}
	}
}

func (m *Manager) ActiveCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.streams)
}

func (m *Manager) Subscribe(ctx context.Context, payload SubscribePayload) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isLeader {
		return fmt.Errorf("log stream subscriptions require leader")
	}
	if _, exists := m.streams[payload.SubscriptionID]; exists {
		return fmt.Errorf("subscription %s already exists", payload.SubscriptionID)
	}
	if payload.Pattern == "" {
		return fmt.Errorf("pattern is required")
	}

	re, err := compilePattern(payload)
	if err != nil {
		return err
	}

	subCtx, cancel := context.WithCancel(ctx)
	m.streams[payload.SubscriptionID] = &streamHandle{cancel: cancel}

	for _, t := range payload.Targets {
		target := t
		go m.runStream(subCtx, payload.SubscriptionID, target, re, payload)
	}
	return nil
}

func (m *Manager) Unsubscribe(subscriptionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	h, ok := m.streams[subscriptionID]
	if !ok {
		return fmt.Errorf("subscription %s not found", subscriptionID)
	}
	h.cancel()
	delete(m.streams, subscriptionID)
	return nil
}

func compilePattern(payload SubscribePayload) (*regexp.Regexp, error) {
	return CompilePattern(payload)
}

func CompilePattern(payload SubscribePayload) (*regexp.Regexp, error) {
	pattern := payload.Pattern
	if payload.PatternType != "regex" && payload.PatternType != "" {
		return nil, fmt.Errorf("unsupported pattern_type: %s", payload.PatternType)
	}
	if payload.CaseInsensitive {
		pattern = "(?i)" + pattern
	}
	return regexp.Compile(pattern)
}

func (m *Manager) runStream(ctx context.Context, subscriptionID string, target Target, re *regexp.Regexp, payload SubscribePayload) {
	backoff := time.Second
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		since := payload.SinceSeconds
		stream, err := m.logReader.FollowStream(ctx, k8s.LogTarget{
			Namespace: target.Namespace,
			Pod:       target.Pod,
			Container: target.Container,
			Previous:  target.Previous,
		}, since, true)
		if err != nil {
			m.logger.Warn("log follow failed", "error", err, "pod", target.Pod)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
				if backoff < 30*time.Second {
					backoff *= 2
				}
				continue
			}
		}
		backoff = time.Second
		m.scanStream(ctx, subscriptionID, target, re, stream)
		stream.Close()

		if !payload.Follow {
			return
		}
	}
}

func (m *Manager) scanStream(ctx context.Context, subscriptionID string, target Target, re *regexp.Regexp, r io.Reader) {
	scanner := bufio.NewScanner(r)
	scanner.Split(scanLines)
	var lineNum int64
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if !re.MatchString(line) {
			continue
		}
		match := re.FindString(line)
		details, _ := json.Marshal(map[string]any{
			"container":       target.Container,
			"line":            line,
			"line_number":     lineNum,
			"pattern_matched": match,
		})
		event := &command.ClusterEvent{
			SubscriptionID: subscriptionID,
			EventType:      command.EventLogLine,
			Resource: command.Target{
				Kind:      "Pod",
				Namespace: target.Namespace,
				Name:      target.Pod,
			},
			ObservedAt: time.Now().UTC(),
			Details:    details,
		}
		key := fmt.Sprintf("%s/%s/%s", target.Namespace, target.Pod, target.Container)
		if err := m.events.Publish(ctx, key, event); err != nil {
			m.logger.Error("publish log line", "error", err)
		} else {
			metrics.LogLinesMatched.Inc()
		}
	}
}

func scanLines(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, '\n'); i >= 0 {
		return i + 1, data[0:i], nil
	}
	if atEOF {
		return len(data), data, nil
	}
	const maxLine = 1024 * 1024
	if len(data) > maxLine {
		return maxLine, data[0:maxLine], nil
	}
	return 0, nil, nil
}
