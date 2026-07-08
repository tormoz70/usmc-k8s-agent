package mockcorelib

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// TopicSpec describes a topic to bootstrap for local testing.
type TopicSpec struct {
	Name       string
	Partitions int
}

// DefaultTopics are created by dev-up / kafka-init and ensured by mock-core before use.
var DefaultTopics = []TopicSpec{
	{Name: DefaultRequestTopic, Partitions: 1},
	{Name: DefaultReplyTopic, Partitions: 1},
	{Name: "cluster.events", Partitions: 1},
	{Name: "logs.stream", Partitions: 1},
	{Name: "cluster.health", Partitions: 1},
	{Name: "agent.lifecycle", Partitions: 1},
}

// EnsureTopics creates topics if they do not exist (idempotent for local Redpanda).
func EnsureTopics(brokers []string, specs ...TopicSpec) error {
	if len(brokers) == 0 {
		return fmt.Errorf("no kafka brokers configured")
	}
	if len(specs) == 0 {
		specs = DefaultTopics
	}
	conn, err := kafkago.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("dial kafka %s: %w", brokers[0], err)
	}
	defer conn.Close()

	configs := make([]kafkago.TopicConfig, 0, len(specs))
	for _, spec := range specs {
		if spec.Name == "" {
			continue
		}
		parts := spec.Partitions
		if parts < 1 {
			parts = 1
		}
		configs = append(configs, kafkago.TopicConfig{
			Topic:             spec.Name,
			NumPartitions:     parts,
			ReplicationFactor: 1,
		})
	}
	if len(configs) == 0 {
		return nil
	}
	if err := conn.CreateTopics(configs...); err != nil && !isTopicAlreadyExists(err) {
		return fmt.Errorf("create topics: %w", err)
	}
	return nil
}

// EnsureTopic creates a single topic if missing.
func EnsureTopic(brokers []string, topic string, partitions int) error {
	return EnsureTopics(brokers, TopicSpec{Name: topic, Partitions: partitions})
}

func isTopicAlreadyExists(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, kafkago.TopicAlreadyExists) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "topic already exists") ||
		strings.Contains(msg, "already been created") ||
		strings.Contains(msg, "topic_already_exists")
}

func isUnknownTopicOrPartition(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, kafkago.UnknownTopicOrPartition) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown topic or partition") ||
		strings.Contains(msg, "unknown topic")
}

func ensureTopicBeforeUse(brokers []string, topic string) {
	if topic == "" || len(brokers) == 0 {
		return
	}
	_ = EnsureTopic(brokers, topic, 1)
}

func readMessageWithTopicRetry(ctx context.Context, brokers []string, topic string, read func(context.Context) (kafkago.Message, error)) (kafkago.Message, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(2 * time.Minute)
	}
	for {
		msg, err := read(ctx)
		if err == nil {
			return msg, nil
		}
		if isUnknownTopicOrPartition(err) && time.Now().Before(deadline) {
			ensureTopicBeforeUse(brokers, topic)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		return msg, err
	}
}
