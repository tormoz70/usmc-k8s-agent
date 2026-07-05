package kafka

import (
	"context"
	"log/slog"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/observability"
	"github.com/usmc/usmc-k8s-agent/internal/result"
)

// Processor handles the consume → commit → route → publish loop sequentially.
type Processor struct {
	consumer    *Consumer
	publisher   *Publisher
	router      *command.Router
	metrics     *observability.Metrics
	log         *slog.Logger
	commitFirst bool
}

func NewProcessor(consumer *Consumer, publisher *Publisher, router *command.Router, commitOnReceive bool, metrics *observability.Metrics, log *slog.Logger) *Processor {
	if log == nil {
		log = slog.Default()
	}
	return &Processor{
		consumer:    consumer,
		publisher:   publisher,
		router:      router,
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
	if p.commitFirst {
		if err := p.consumer.CommitMessage(ctx, msg); err != nil {
			return err
		}
	}

	cmd, meta, err := ParseCommand(msg)
	if err != nil {
		p.log.Warn("invalid kafka message", "error", err)
		return nil
	}

	started := time.Now()
	resp, err := p.router.Handle(ctx, cmd, meta)
	if err != nil {
		return err
	}
	if resp == nil {
		p.recordCommand(cmd.Type, "async", started)
		return nil
	}

	if err := p.publisher.PublishResponse(ctx, meta.ReplyTopic, meta.CorrelationID, resp); err != nil {
		return err
	}
	p.recordCommand(cmd.Type, resp.Status, started)

	if !p.commitFirst {
		return p.consumer.CommitMessage(ctx, msg)
	}
	return nil
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
	return p.router.Handle(ctx, cmd, meta)
}
