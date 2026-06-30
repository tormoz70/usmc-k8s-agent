package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
	topic  string
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		topic: topic,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			Compression:  kafka.Lz4,
			BatchTimeout: 10 * time.Millisecond,
		},
	}
}

func (p *Producer) Publish(ctx context.Context, key string, value []byte) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: value,
		Time:  time.Now().UTC(),
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("publish to %s: %w", p.topic, err)
	}
	return nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

type MessageHandler func(ctx context.Context, msg kafka.Message) error

type Consumer struct {
	reader  *kafka.Reader
	handler MessageHandler
	logger  *slog.Logger
}

func NewConsumer(brokers []string, topic, group string, handler MessageHandler, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{
		handler: handler,
		logger:  logger,
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        brokers,
			GroupID:        group,
			Topic:          topic,
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: 0,
			StartOffset:    kafka.LastOffset,
		}),
	}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("fetch message: %w", err)
		}

		if err := c.handler(ctx, msg); err != nil {
			c.logger.Error("message handler failed", "error", err, "offset", msg.Offset)
			continue
		}

		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			return fmt.Errorf("commit message: %w", err)
		}
	}
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func PublishDLQ(ctx context.Context, producer *Producer, original []byte, reason string) error {
	payload := fmt.Sprintf(`{"reason":%q,"original":%s}`, reason, string(original))
	return producer.Publish(ctx, "dlq", []byte(payload))
}
