package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/result"
)

const (
	headerCorrelationID = "correlation_id"
	headerReplyTopic    = "reply_topic"
)

// Publisher sends responses to Kafka reply topics.
type Publisher struct {
	writer *kafkago.Writer
	log    *slog.Logger
}

func NewPublisher(cfg config.KafkaConfig, log *slog.Logger) (*Publisher, error) {
	if err := ValidateSecurityConfig(cfg); err != nil {
		return nil, fmt.Errorf("kafka publisher: %w", err)
	}
	if log == nil {
		log = slog.Default()
	}
	transport, err := newTransport(cfg)
	if err != nil {
		return nil, err
	}
	return &Publisher{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(cfg.Brokers...),
			Balancer:     &kafkago.LeastBytes{},
			RequiredAcks: kafkago.RequireAll,
			Compression:  kafkago.Zstd,
			Transport:    transport,
		},
		log: log,
	}, nil
}

func (p *Publisher) PublishResponse(ctx context.Context, replyTopic, correlationID string, resp *result.Response) error {
	body, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	msg := kafkago.Message{
		Topic: replyTopic,
		Key:   []byte(correlationID),
		Value: body,
		Headers: []kafkago.Header{
			{Key: headerCorrelationID, Value: []byte(correlationID)},
		},
	}
	return p.writer.WriteMessages(ctx, msg)
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

// PublishEvent sends a cluster.events message with the given key for ordering.
func (p *Publisher) PublishEvent(ctx context.Context, topic, key string, event any) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return p.writer.WriteMessages(ctx, kafkago.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: body,
	})
}

// PublishRaw sends a message to an arbitrary topic (lifecycle, protobuf, events, etc.).
func (p *Publisher) PublishRaw(ctx context.Context, topic, key string, headers map[string]string, value []byte) error {
	msg := kafkago.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	}
	for k, v := range headers {
		msg.Headers = append(msg.Headers, kafkago.Header{Key: k, Value: []byte(v)})
	}
	return p.writer.WriteMessages(ctx, msg)
}

// Consumer reads commands from the request topic.
type Consumer struct {
	reader *kafkago.Reader
	cfg    config.KafkaConfig
	log    *slog.Logger
}

func NewConsumer(cfg config.KafkaConfig, log *slog.Logger) (*Consumer, error) {
	if err := ValidateSecurityConfig(cfg); err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	if log == nil {
		log = slog.Default()
	}
	d, err := newDialer(cfg)
	if err != nil {
		return nil, err
	}
	group := cfg.ConsumerGroup
	if cfg.ShadowConsumerGroup != "" {
		group = cfg.ShadowConsumerGroup
	}
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        group,
		Topic:          cfg.RequestTopic,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		CommitInterval: 0,
		StartOffset:    kafkago.LastOffset,
		Dialer:         d,
		MaxWait:        time.Second,
	})
	if cfg.ClientPrincipal != "" {
		log.Info("kafka consumer configured", "security_mode", ResolveSecurityMode(cfg), "client_principal", cfg.ClientPrincipal, "group", group, "topic", cfg.RequestTopic)
	} else if mode := ResolveSecurityMode(cfg); mode == SecurityMTLS {
		log.Info("kafka consumer configured", "security_mode", mode, "note", "set KAFKA_TLS_CLIENT_PRINCIPAL to match Kafka ACL principal (usually certificate CN)")
	}
	return &Consumer{reader: r, cfg: cfg, log: log}, nil
}

// FetchMessage reads the next Kafka message.
func (c *Consumer) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	return c.reader.FetchMessage(ctx)
}

// CommitMessage commits offset after successful processing.
func (c *Consumer) CommitMessage(ctx context.Context, msg kafkago.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

// ParseCommand decodes envelope and Kafka headers into command + meta.
func ParseCommand(msg kafkago.Message) (*command.Command, command.RequestMeta, error) {
	var cmd command.Command
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		return nil, command.RequestMeta{}, fmt.Errorf("decode command: %w", err)
	}
	meta := command.RequestMeta{
		CorrelationID: headerValue(msg.Headers, headerCorrelationID),
		ReplyTopic:    headerValue(msg.Headers, headerReplyTopic),
		Partition:     msg.Partition,
		Offset:        msg.Offset,
	}
	if meta.CorrelationID == "" || meta.ReplyTopic == "" {
		return nil, meta, fmt.Errorf("missing required headers correlation_id and reply_topic")
	}
	return &cmd, meta, nil
}

func headerValue(headers []kafkago.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func newDialer(cfg config.KafkaConfig) (*kafkago.Dialer, error) {
	d := &kafkago.Dialer{Timeout: 10 * time.Second, DualStack: true}
	mode := ResolveSecurityMode(cfg)
	if mode == SecurityPlaintext {
		return d, nil
	}
	tlsConfig, err := BuildTLSConfig(mode, cfg.TLS)
	if err != nil {
		return nil, err
	}
	d.TLS = tlsConfig
	return d, nil
}

func newTransport(cfg config.KafkaConfig) (*kafkago.Transport, error) {
	mode := ResolveSecurityMode(cfg)
	if mode == SecurityPlaintext {
		return &kafkago.Transport{}, nil
	}
	tlsConfig, err := BuildTLSConfig(mode, cfg.TLS)
	if err != nil {
		return nil, err
	}
	return &kafkago.Transport{TLS: tlsConfig}, nil
}
