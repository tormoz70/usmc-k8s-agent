package lifecycle

import (
	"context"
	"encoding/json"
	"time"
)

// Event is published to agent.lifecycle topic.
type Event struct {
	SchemaVersion   string    `json:"schema_version"`
	EventType       string    `json:"event_type"`
	ClusterID       string    `json:"cluster_id"`
	AgentInstanceID string    `json:"agent_instance_id"`
	Leader          bool      `json:"leader"`
	ObservedAt      time.Time `json:"observed_at"`
}

const (
	EventTypeStarted       = "agent.started"
	EventTypeLeaderChanged = "agent.leader.changed"
)

// Publisher writes lifecycle events to Kafka.
type Publisher struct {
	publish func(ctx context.Context, topic string, key string, value []byte) error
	topic   string
}

func NewPublisher(topic string, publish func(ctx context.Context, topic string, key string, value []byte) error) *Publisher {
	return &Publisher{topic: topic, publish: publish}
}

func (p *Publisher) PublishStarted(ctx context.Context, clusterID, instanceID string, leader bool) error {
	return p.publishEvent(ctx, EventTypeStarted, clusterID, instanceID, leader)
}

func (p *Publisher) PublishLeaderLost(ctx context.Context, clusterID, instanceID string) error {
	return p.publishEvent(ctx, EventTypeLeaderChanged, clusterID, instanceID, false)
}

func (p *Publisher) publishEvent(ctx context.Context, eventType, clusterID, instanceID string, leader bool) error {
	if p == nil || p.publish == nil {
		return nil
	}
	ev := Event{
		SchemaVersion:   "v1",
		EventType:       eventType,
		ClusterID:       clusterID,
		AgentInstanceID: instanceID,
		Leader:          leader,
		ObservedAt:      time.Now().UTC(),
	}
	body, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	return p.publish(ctx, p.topic, clusterID, body)
}
