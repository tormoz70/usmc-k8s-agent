package watch

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/policy"
)

const (
	EventAdded    = "ADDED"
	EventModified = "MODIFIED"
	EventDeleted  = "DELETED"
	EventRestart  = "RESTART"
	EventK8sEvent = "K8S_EVENT"
)

// SubscribePayload is the watch.subscribe command payload.
type SubscribePayload struct {
	SubscriptionID string    `json:"subscription_id"`
	GVK            policy.GVK `json:"gvk"`
	Namespace      string    `json:"namespace"`
	LabelSelector  string    `json:"label_selector"`
	FieldSelector  string    `json:"field_selector"`
	EventFilter    []string  `json:"event_filter"`
	OutputTopic    string    `json:"output_topic"`
	TTLSeconds     int       `json:"ttl_seconds"`
}

// UnsubscribePayload is the watch.unsubscribe command payload.
type UnsubscribePayload struct {
	SubscriptionID string `json:"subscription_id"`
}

// ClusterEvent is published to cluster.events.
type ClusterEvent struct {
	SchemaVersion  string         `json:"schema_version"`
	SubscriptionID string         `json:"subscription_id"`
	EventType      string         `json:"event_type"`
	Resource       ResourceRef    `json:"resource"`
	ObservedAt     time.Time      `json:"observed_at"`
	Details        map[string]any `json:"details,omitempty"`
}

type ResourceRef struct {
	Group     string `json:"group"`
	Version   string `json:"version"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
}

func ParseSubscribePayload(raw json.RawMessage) (*SubscribePayload, error) {
	var p SubscribePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	if p.SubscriptionID == "" {
		return nil, fmt.Errorf("subscription_id is required")
	}
	if p.GVK.Kind == "" || p.GVK.Version == "" {
		return nil, fmt.Errorf("gvk.kind and gvk.version are required")
	}
	if p.Namespace == "" {
		return nil, fmt.Errorf("namespace is required")
	}
	if len(p.EventFilter) == 0 {
		p.EventFilter = []string{EventAdded, EventModified, EventDeleted}
	}
	return &p, nil
}

func ParseUnsubscribePayload(raw json.RawMessage) (*UnsubscribePayload, error) {
	var p UnsubscribePayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, fmt.Errorf("decode payload: %w", err)
	}
	if p.SubscriptionID == "" {
		return nil, fmt.Errorf("subscription_id is required")
	}
	return &p, nil
}

func (p *SubscribePayload) Allows(eventType string) bool {
	for _, f := range p.EventFilter {
		if strings.EqualFold(f, eventType) {
			return true
		}
	}
	return false
}

func (p *SubscribePayload) OutputTopicOr(defaultTopic string) string {
	if p.OutputTopic != "" {
		return p.OutputTopic
	}
	return defaultTopic
}
