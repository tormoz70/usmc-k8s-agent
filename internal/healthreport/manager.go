package healthreport

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/policy"
)

const SchemaVersionV1 = "v1"

type HealthPublisher interface {
	PublishEvent(ctx context.Context, topic, key string, event any) error
}

type StartPayload struct {
	SubscriptionID      string   `json:"subscription_id"`
	IntervalSeconds     int      `json:"interval_seconds"`
	Namespaces          []string `json:"namespaces"`
	LabelSelector       string   `json:"label_selector"`
	IncludeNotReadyOnly bool     `json:"include_not_ready_only"`
	OutputTopic         string   `json:"output_topic"`
	MaxPodsPerMessage   int      `json:"max_pods_per_message"`
}

type StopPayload struct {
	SubscriptionID string `json:"subscription_id"`
}

type Summary struct {
	Total     int `json:"total"`
	Running   int `json:"running"`
	Pending   int `json:"pending"`
	Failed    int `json:"failed"`
	Succeeded int `json:"succeeded"`
	Unknown   int `json:"unknown"`
	NotReady  int `json:"not_ready"`
}

type PodStatus struct {
	Namespace    string `json:"namespace"`
	Name         string `json:"name"`
	Phase        string `json:"phase"`
	Ready        bool   `json:"ready"`
	RestartCount int32  `json:"restart_count"`
	Reason       string `json:"reason"`
	Message      string `json:"message"`
}

type ReportMessage struct {
	SchemaVersion  string      `json:"schema_version"`
	SubscriptionID string      `json:"subscription_id"`
	ObservedAt     time.Time   `json:"observed_at"`
	Page           int         `json:"page,omitempty"`
	PageCount      int         `json:"page_count,omitempty"`
	Summary        Summary     `json:"summary"`
	Pods           []PodStatus `json:"pods"`
}

type subscription struct {
	payload *StartPayload
	cancel  context.CancelFunc
}

type Manager struct {
	clusterID    string
	defaultTopic string
	kube         kubernetes.Interface
	policy       *policy.Engine
	publisher    HealthPublisher
	defaultMax   int
	log          *slog.Logger

	mu   sync.Mutex
	subs map[string]*subscription
}

func NewManager(clusterID, defaultTopic string, kube kubernetes.Interface, engine *policy.Engine, publisher HealthPublisher, defaultMax int, log *slog.Logger) *Manager {
	if defaultMax < 1 {
		defaultMax = 500
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
		defaultMax:   defaultMax,
		log:          log,
		subs:         make(map[string]*subscription),
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
	if p.IntervalSeconds < 1 {
		return nil, fmt.Errorf("interval_seconds must be >= 1")
	}
	if p.MaxPodsPerMessage < 1 {
		p.MaxPodsPerMessage = 500
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

func (m *Manager) Start(parent context.Context, payload *StartPayload) error {
	if err := m.policy.AllowCommandType(command.TypeHealthReportStart); err != nil {
		return err
	}
	namespaces := payload.Namespaces
	if len(namespaces) == 0 {
		namespaces = m.policy.AllowedNamespaces()
	}
	for _, ns := range namespaces {
		if err := m.policy.AllowNamespace(ns); err != nil {
			return err
		}
	}
	payload.Namespaces = namespaces

	m.mu.Lock()
	if _, exists := m.subs[payload.SubscriptionID]; exists {
		m.mu.Unlock()
		return fmt.Errorf("subscription %q already exists", payload.SubscriptionID)
	}
	subCtx, cancel := context.WithCancel(parent)
	sub := &subscription{payload: payload, cancel: cancel}
	m.subs[payload.SubscriptionID] = sub
	m.mu.Unlock()

	go m.run(subCtx, sub)
	return nil
}

func (m *Manager) Stop(subscriptionID string) error {
	m.mu.Lock()
	sub, ok := m.subs[subscriptionID]
	if !ok {
		m.mu.Unlock()
		return fmt.Errorf("subscription %q not found", subscriptionID)
	}
	delete(m.subs, subscriptionID)
	m.mu.Unlock()
	sub.cancel()
	return nil
}

func (m *Manager) StopAll() {
	m.mu.Lock()
	subs := m.subs
	m.subs = make(map[string]*subscription)
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
		m.mu.Unlock()
		sub.cancel()
	}()

	payload := sub.payload
	ticker := time.NewTicker(time.Duration(payload.IntervalSeconds) * time.Second)
	defer ticker.Stop()

	m.publishOnce(ctx, payload)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.publishOnce(ctx, payload)
		}
	}
}

func (m *Manager) publishOnce(ctx context.Context, payload *StartPayload) {
	pods, summary, err := m.collectPods(ctx, payload)
	if err != nil {
		m.log.Warn("health report collect failed", "subscription_id", payload.SubscriptionID, "error", err)
		return
	}
	maxPerMsg := payload.MaxPodsPerMessage
	if maxPerMsg < 1 {
		maxPerMsg = m.defaultMax
	}
	pageCount := 1
	if len(pods) > 0 {
		pageCount = (len(pods) + maxPerMsg - 1) / maxPerMsg
	}
	topic := payload.OutputTopicOr(m.defaultTopic)
	for page := 0; page < pageCount; page++ {
		start := page * maxPerMsg
		end := start + maxPerMsg
		if end > len(pods) {
			end = len(pods)
		}
		msg := ReportMessage{
			SchemaVersion:  SchemaVersionV1,
			SubscriptionID: payload.SubscriptionID,
			ObservedAt:     time.Now().UTC(),
			Page:           page + 1,
			PageCount:      pageCount,
			Summary:        summary,
			Pods:           pods[start:end],
		}
		key := fmt.Sprintf("%s/health/%s/page-%d", m.clusterID, payload.SubscriptionID, page+1)
		pubCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := m.publisher.PublishEvent(pubCtx, topic, key, msg)
		cancel()
		if err != nil {
			m.log.Warn("health report publish failed", "subscription_id", payload.SubscriptionID, "error", err)
		}
	}
}

func (m *Manager) collectPods(ctx context.Context, payload *StartPayload) ([]PodStatus, Summary, error) {
	var all []PodStatus
	var summary Summary
	for _, ns := range payload.Namespaces {
		opts := metav1.ListOptions{}
		if payload.LabelSelector != "" {
			opts.LabelSelector = payload.LabelSelector
		}
		list, err := m.kube.CoreV1().Pods(ns).List(ctx, opts)
		if err != nil {
			return nil, summary, err
		}
		for _, pod := range list.Items {
			status := podStatusFromPod(pod)
			summary.Total++
			switch status.Phase {
			case string(corev1.PodRunning):
				summary.Running++
			case string(corev1.PodPending):
				summary.Pending++
			case string(corev1.PodFailed):
				summary.Failed++
			case string(corev1.PodSucceeded):
				summary.Succeeded++
			default:
				summary.Unknown++
			}
			if !status.Ready {
				summary.NotReady++
			}
			if payload.IncludeNotReadyOnly && status.Ready {
				continue
			}
			all = append(all, status)
		}
	}
	return all, summary, nil
}

func podStatusFromPod(pod corev1.Pod) PodStatus {
	ready := true
	restarts := int32(0)
	reason := pod.Status.Reason
	message := pod.Status.Message
	for _, cs := range pod.Status.ContainerStatuses {
		restarts += cs.RestartCount
		if !cs.Ready {
			ready = false
			if cs.State.Waiting != nil && cs.State.Waiting.Reason != "" {
				reason = cs.State.Waiting.Reason
				message = cs.State.Waiting.Message
			}
		}
	}
	return PodStatus{
		Namespace:    pod.Namespace,
		Name:         pod.Name,
		Phase:        string(pod.Status.Phase),
		Ready:        ready,
		RestartCount: restarts,
		Reason:       reason,
		Message:      message,
	}
}
