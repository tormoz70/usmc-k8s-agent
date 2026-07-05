package watch

import (
	"context"

	"github.com/usmc/usmc-k8s-agent/internal/kafka"
)

// KafkaPublisher adapts kafka.Publisher to EventPublisher.
type KafkaPublisher struct {
	p *kafka.Publisher
}

func NewKafkaPublisher(p *kafka.Publisher) *KafkaPublisher {
	return &KafkaPublisher{p: p}
}

func (k *KafkaPublisher) PublishEvent(ctx context.Context, topic, key string, event *ClusterEvent) error {
	return k.p.PublishEvent(ctx, topic, key, event)
}
