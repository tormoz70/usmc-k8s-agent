package result

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/usmc/k8s-agent/internal/command"
	"github.com/usmc/k8s-agent/internal/kafka"
)

type Publisher struct {
	producer *kafka.Producer
}

func NewPublisher(producer *kafka.Producer) *Publisher {
	return &Publisher{producer: producer}
}

func (p *Publisher) Publish(ctx context.Context, res *command.Result) error {
	if res == nil {
		return fmt.Errorf("result is nil")
	}
	data, err := json.Marshal(res)
	if err != nil {
		return fmt.Errorf("marshal result: %w", err)
	}
	key := res.CommandID
	if key == "" {
		key = res.IdempotencyKey
	}
	return p.producer.Publish(ctx, key, data)
}

func NewResult(cmd command.Command, status string) *command.Result {
	now := time.Now().UTC()
	return &command.Result{
		CommandID:      cmd.CommandID,
		IdempotencyKey: cmd.IdempotencyKey,
		Status:         status,
		StartedAt:      now,
	}
}

func Finish(res *command.Result, status string, details any, errDetail *command.ErrorDetail) *command.Result {
	now := time.Now().UTC()
	res.Status = status
	res.FinishedAt = &now
	if details != nil {
		b, _ := json.Marshal(details)
		res.Details = b
	}
	res.Error = errDetail
	return res
}

func Rejected(cmd command.Command, code, message string) *command.Result {
	now := time.Now().UTC()
	return &command.Result{
		CommandID:      cmd.CommandID,
		IdempotencyKey: cmd.IdempotencyKey,
		Status:         command.StatusRejected,
		StartedAt:      now,
		FinishedAt:     &now,
		Error:          &command.ErrorDetail{Code: code, Message: message},
	}
}

func Failed(cmd command.Command, code, message string) *command.Result {
	now := time.Now().UTC()
	return &command.Result{
		CommandID:      cmd.CommandID,
		IdempotencyKey: cmd.IdempotencyKey,
		Status:         command.StatusFailed,
		StartedAt:      now,
		FinishedAt:     &now,
		Error:          &command.ErrorDetail{Code: code, Message: message},
	}
}

type EventPublisher struct {
	producer *kafka.Producer
}

func NewEventPublisher(producer *kafka.Producer) *EventPublisher {
	return &EventPublisher{producer: producer}
}

func (p *EventPublisher) Publish(ctx context.Context, key string, event *command.ClusterEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}
	return p.producer.Publish(ctx, key, data)
}
