// Package mockcorelib provides Kafka helpers for local mock-core testing tools.
package mockcorelib

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

const DefaultRequestTopic = "k8s.commands.request"
const DefaultReplyTopic = "core-client.dev.responses"

// Message is a Kafka message formatted for API/UI consumption.
type Message struct {
	Topic         string            `json:"topic"`
	Partition     int               `json:"partition"`
	Offset        int64             `json:"offset"`
	Key           string            `json:"key"`
	Body          json.RawMessage   `json:"body"`
	Headers       map[string]string `json:"headers"`
	CorrelationID string            `json:"correlation_id,omitempty"`
	Timestamp     time.Time         `json:"timestamp"`
}

// SendResult holds metadata from a published command.
type SendResult struct {
	CorrelationID string `json:"correlation_id"`
	ReplyTopic    string `json:"reply_topic"`
	RequestTopic  string `json:"request_topic"`
}

// NewCorrelationID generates a unique correlation id.
func NewCorrelationID() string {
	return fmt.Sprintf("corr-%d", time.Now().UnixNano())
}

// SplitBrokers parses a comma-separated broker list.
func SplitBrokers(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// HeaderValue returns a header value by key.
func HeaderValue(headers []kafkago.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// HeadersMap converts kafka headers to a string map.
func HeadersMap(headers []kafkago.Header) map[string]string {
	out := make(map[string]string, len(headers))
	for _, h := range headers {
		out[h.Key] = string(h.Value)
	}
	return out
}

// FormatMessage converts a kafka message to Message.
func FormatMessage(topic string, msg kafkago.Message) Message {
	var body json.RawMessage
	if json.Valid(msg.Value) {
		body = msg.Value
	} else {
		body = json.RawMessage(fmt.Sprintf("%q", string(msg.Value)))
	}
	return Message{
		Topic:         topic,
		Partition:     msg.Partition,
		Offset:        msg.Offset,
		Key:           string(msg.Key),
		Body:          body,
		Headers:       HeadersMap(msg.Headers),
		CorrelationID: HeaderValue(msg.Headers, "correlation_id"),
		Timestamp:     msg.Time,
	}
}

// PrintMessage prints a message to stdout (CLI).
func PrintMessage(topic string, msg kafkago.Message) {
	formatted := FormatMessage(topic, msg)
	fmt.Printf("[%s] partition=%d offset=%d key=%q correlation_id=%q body=%s\n",
		formatted.Topic, formatted.Partition, formatted.Offset, formatted.Key,
		formatted.CorrelationID, string(formatted.Body))
}

// SendCommand publishes a command to the request topic.
func SendCommand(ctx context.Context, brokers []string, requestTopic, replyTopic string, body []byte) (SendResult, error) {
	if requestTopic == "" {
		requestTopic = DefaultRequestTopic
	}
	if replyTopic == "" {
		replyTopic = DefaultReplyTopic
	}
	corrID := NewCorrelationID()

	w := &kafkago.Writer{
		Addr:     kafkago.TCP(brokers...),
		Topic:    requestTopic,
		Balancer: &kafkago.LeastBytes{},
	}
	defer w.Close()

	err := w.WriteMessages(ctx, kafkago.Message{
		Value: body,
		Headers: []kafkago.Header{
			{Key: "correlation_id", Value: []byte(corrID)},
			{Key: "reply_topic", Value: []byte(replyTopic)},
		},
	})
	if err != nil {
		return SendResult{}, err
	}
	return SendResult{
		CorrelationID: corrID,
		ReplyTopic:    replyTopic,
		RequestTopic:  requestTopic,
	}, nil
}

// NewReader creates a reader with a unique consumer group.
func NewReader(brokers []string, topic string) *kafkago.Reader {
	return kafkago.NewReader(kafkago.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: fmt.Sprintf("mock-core-%d", time.Now().UnixNano()),
	})
}

// ListenTopic continuously reads messages and invokes fn for each.
func ListenTopic(brokers []string, topic string, fn func(Message)) error {
	r := NewReader(brokers, topic)
	defer r.Close()
	for {
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			return err
		}
		fn(FormatMessage(topic, msg))
	}
}

// ListenOnce waits for a reply matching corrID or until timeout.
func ListenOnce(brokers []string, replyTopic, corrID string, timeout time.Duration, onMatch func(Message)) error {
	r := NewReader(brokers, replyTopic)
	defer r.Close()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			return fmt.Errorf("timeout waiting for reply on %s", replyTopic)
		default:
		}
		msg, err := r.ReadMessage(context.Background())
		if err != nil {
			return err
		}
		if HeaderValue(msg.Headers, "correlation_id") != corrID {
			continue
		}
		onMatch(FormatMessage(replyTopic, msg))
		return nil
	}
}

// StreamMessages reads from a topic until ctx is cancelled, sending matching messages to out.
func StreamMessages(ctx context.Context, brokers []string, topic, correlationID string, out chan<- Message) error {
	r := NewReader(brokers, topic)
	defer r.Close()
	defer close(out)
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if correlationID != "" && HeaderValue(msg.Headers, "correlation_id") != correlationID {
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case out <- FormatMessage(topic, msg):
		}
	}
}
