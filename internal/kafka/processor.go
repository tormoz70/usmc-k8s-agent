package kafka

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/observability"
	"github.com/usmc/usmc-k8s-agent/internal/result"
)

// CommandExecutor runs a decoded command.
type CommandExecutor interface {
	Handle(ctx context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error)
}

// Processor handles the consume → commit → route → publish loop sequentially.
type Processor struct {
	consumer    *Consumer
	publisher   *Publisher
	executor    CommandExecutor
	guard       *CommandGuard
	metrics     *observability.Metrics
	log         *slog.Logger
	commitFirst bool
}

func NewProcessor(consumer *Consumer, publisher *Publisher, executor CommandExecutor, guard *CommandGuard, commitOnReceive bool, metrics *observability.Metrics, log *slog.Logger) *Processor {
	if log == nil {
		log = slog.Default()
	}
	return &Processor{
		consumer:    consumer,
		publisher:   publisher,
		executor:    executor,
		guard:       guard,
		metrics:     metrics,
		log:         log,
		commitFirst: commitOnReceive,
	}
}

// Run processes messages until context cancellation.
func (p *Processor) Run(ctx context.Context) error {
	for {
		msg, err := p.consumer.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err := p.handleMessage(ctx, msg); err != nil {
			p.log.Error("handle message failed", "error", err, "offset", msg.Offset)
		}
	}
}

func (p *Processor) handleMessage(ctx context.Context, msg kafkago.Message) error {
	cmd, meta, err := ParseCommand(msg)
	if err != nil {
		p.log.Warn("invalid kafka message", "error", err)
		return p.commit(ctx, msg)
	}

	if p.guard != nil {
		if err := p.guard.Validate(cmd, meta); err != nil {
			p.log.Warn("command rejected by kafka guard", "error", err, "command_id", cmd.CommandID, "issuer", cmd.Issuer, "reply_topic", meta.ReplyTopic)
			if p.guard.CanPublishReply(meta.ReplyTopic) {
				started := time.Now().UTC()
				resp := result.Rejected(cmd.CommandID, meta.CorrelationID, "PolicyDenied", "KAFKA_GUARD", err.Error(), started, time.Now().UTC())
				if pubErr := p.publisher.PublishResponse(ctx, meta.ReplyTopic, meta.CorrelationID, resp); pubErr != nil {
					return pubErr
				}
			}
			return p.commit(ctx, msg)
		}
	}

	if p.commitFirst {
		if err := p.consumer.CommitMessage(ctx, msg); err != nil {
			return err
		}
	}

	started := time.Now()
	resp, err := p.executor.Handle(ctx, cmd, meta)
	if err != nil {
		// Always reply + commit so UI/core never hang waiting on a correlation_id.
		failed := result.Failed(cmd.CommandID, meta.CorrelationID, "ExecuteFailed", "AGENT_EXECUTE_ERROR", err.Error(), started.UTC(), time.Now().UTC())
		if pubErr := p.publisher.PublishResponse(ctx, meta.ReplyTopic, meta.CorrelationID, failed); pubErr != nil {
			return fmt.Errorf("execute: %w; publish failed reply: %v", err, pubErr)
		}
		p.recordCommand(cmd.Type, failed.Status, started)
		if commitErr := p.commit(ctx, msg); commitErr != nil {
			return fmt.Errorf("execute: %w; commit: %v", err, commitErr)
		}
		return err
	}
	if resp == nil {
		p.recordCommand(cmd.Type, "async", started)
		return p.commit(ctx, msg)
	}

	if err := p.publisher.PublishResponse(ctx, meta.ReplyTopic, meta.CorrelationID, resp); err != nil {
		return err
	}
	p.recordCommand(cmd.Type, resp.Status, started)
	return p.commit(ctx, msg)
}

func (p *Processor) commit(ctx context.Context, msg kafkago.Message) error {
	if p.commitFirst {
		return nil
	}
	return p.consumer.CommitMessage(ctx, msg)
}

func (p *Processor) recordCommand(commandType, status string, started time.Time) {
	if p.metrics != nil {
		p.metrics.RecordCommand(commandType, status, time.Since(started))
	}
}

// HandleOnce is useful in tests.
func (p *Processor) HandleOnce(ctx context.Context, msg kafkago.Message) (*result.Response, error) {
	cmd, meta, err := ParseCommand(msg)
	if err != nil {
		return nil, err
	}
	return p.executor.Handle(ctx, cmd, meta)
}
