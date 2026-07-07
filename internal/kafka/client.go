package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
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

func NewPublisher(cfg config.KafkaConfig, log *slog.Logger) *Publisher {
	if log == nil {
		log = slog.Default()
	}
	return &Publisher{
		writer: &kafkago.Writer{
			Addr:         kafkago.TCP(cfg.Brokers...),
			Balancer:     &kafkago.LeastBytes{},
			RequiredAcks: kafkago.RequireAll,
			Compression:  kafkago.Zstd,
			Transport:    transport(cfg.TLS),
		},
		log: log,
	}
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

// PublishRaw sends a message to an arbitrary topic (lifecycle, events, etc.).
func (p *Publisher) PublishRaw(ctx context.Context, topic, key string, value []byte) error {
	return p.writer.WriteMessages(ctx, kafkago.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
	})
}

// Consumer reads commands from the request topic.
type Consumer struct {
	reader *kafkago.Reader
	cfg    config.KafkaConfig
	log    *slog.Logger
}

func NewConsumer(cfg config.KafkaConfig, log *slog.Logger) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	r := kafkago.NewReader(kafkago.ReaderConfig{
		Brokers:        cfg.Brokers,
		GroupID:        cfg.ConsumerGroup,
		Topic:          cfg.RequestTopic,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		CommitInterval: 0,
		Dialer:         dialer(cfg.TLS),
	})
	return &Consumer{reader: r, cfg: cfg, log: log}
}

// FetchMessage reads the next Kafka message.
func (c *Consumer) FetchMessage(ctx context.Context) (kafkago.Message, error) {
	return c.reader.FetchMessage(ctx)
}

// CommitMessage commits offset (at-most-once: commit before processing when configured).
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

func dialer(tlsCfg config.TLSConfig) *kafkago.Dialer {
	d := &kafkago.Dialer{Timeout: 10 * time.Second, DualStack: true}
	if tlsCfg.Enabled {
		d.TLS = buildTLS(tlsCfg)
	}
	return d
}

func transport(tlsCfg config.TLSConfig) *kafkago.Transport {
	t := &kafkago.Transport{}
	if tlsCfg.Enabled {
		t.TLS = buildTLS(tlsCfg)
	}
	return t
}

func buildTLS(cfg config.TLSConfig) *tls.Config {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err == nil {
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(caPEM)
			tlsConfig.RootCAs = pool
		}
	}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err == nil {
			tlsConfig.Certificates = []tls.Certificate{cert}
		}
	}
	return tlsConfig
}
