package lifecycle

import (
	"context"
	"encoding/json"
	"time"
)

// Event is published to agent.lifecycle topic.
type Event struct {
	SchemaVersion        string    `json:"schema_version"`
	EventType            string    `json:"event_type"`
	ClusterID            string    `json:"cluster_id"`
	AgentInstanceID      string    `json:"agent_instance_id"`
	AgentImplementation  string    `json:"agent_implementation,omitempty"`
	LogsBackend          string    `json:"logs_backend,omitempty"`
	Leader               bool      `json:"leader"`
	ObservedAt           time.Time `json:"observed_at"`
}

const (
	EventTypeStarted       = "agent.started"
	EventTypeLeaderChanged = "agent.leader.changed"
)

// Publisher writes lifecycle events to Kafka.
type Publisher struct {
	publish func(ctx context.Context, topic, key string, headers map[string]string, value []byte) error
	topic   string
}

func NewPublisher(topic string, publish func(ctx context.Context, topic, key string, headers map[string]string, value []byte) error) *Publisher {
	return &Publisher{topic: topic, publish: publish}
}

func (p *Publisher) PublishStarted(ctx context.Context, clusterID, instanceID, implementation, logsBackend string, leader bool) error {
	return p.publishEvent(ctx, EventTypeStarted, clusterID, instanceID, implementation, logsBackend, leader)
}

func (p *Publisher) PublishLeaderLost(ctx context.Context, clusterID, instanceID, implementation, logsBackend string) error {
	return p.publishEvent(ctx, EventTypeLeaderChanged, clusterID, instanceID, implementation, logsBackend, false)
}

func (p *Publisher) publishEvent(ctx context.Context, eventType, clusterID, instanceID, implementation, logsBackend string, leader bool) error {
	if p == nil || p.publish == nil {
		return nil
	}
	ev := Event{
		SchemaVersion:       "v1",
		EventType:           eventType,
		ClusterID:           clusterID,
		AgentInstanceID:     instanceID,
		AgentImplementation: implementation,
		LogsBackend:         logsBackend,
		Leader:              leader,
		ObservedAt:          time.Now().UTC(),
	}
	body, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return p.publish(ctx, p.topic, clusterID, nil, body)
}
