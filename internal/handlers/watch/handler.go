package watchhandler

import (
	"context"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/result"
	"github.com/usmc/usmc-k8s-agent/internal/watch"
)

// SubscribeHandler handles watch.subscribe commands.
type SubscribeHandler struct {
	manager *watch.Manager
}

func NewSubscribeHandler(manager *watch.Manager) *SubscribeHandler {
	return &SubscribeHandler{manager: manager}
}

func (h *SubscribeHandler) Type() string {
	return command.TypeWatchSubscribe
}

func (h *SubscribeHandler) Handle(ctx context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error) {
	started := time.Now().UTC()
	payload, err := watch.ParseSubscribePayload(cmd.Payload)
	if err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "ValidationFailed", "INVALID_PAYLOAD", err.Error(), started, time.Now().UTC()), nil
	}
	if err := h.manager.Subscribe(ctx, payload); err != nil {
		return result.Failed(cmd.CommandID, meta.CorrelationID, "SubscribeFailed", "WATCH_SUBSCRIBE_ERROR", err.Error(), started, time.Now().UTC()), nil
	}
	resp := result.Completed(cmd.CommandID, meta.CorrelationID, started, time.Now().UTC())
	return resp, nil
}

// UnsubscribeHandler handles watch.unsubscribe commands.
type UnsubscribeHandler struct {
	manager *watch.Manager
}

func NewUnsubscribeHandler(manager *watch.Manager) *UnsubscribeHandler {
	return &UnsubscribeHandler{manager: manager}
}

func (h *UnsubscribeHandler) Type() string {
	return command.TypeWatchUnsubscribe
}

func (h *UnsubscribeHandler) Handle(ctx context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error) {
	started := time.Now().UTC()
	payload, err := watch.ParseUnsubscribePayload(cmd.Payload)
	if err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "ValidationFailed", "INVALID_PAYLOAD", err.Error(), started, time.Now().UTC()), nil
	}
	if err := h.manager.Unsubscribe(payload.SubscriptionID); err != nil {
		return result.Failed(cmd.CommandID, meta.CorrelationID, "UnsubscribeFailed", "WATCH_UNSUBSCRIBE_ERROR", err.Error(), started, time.Now().UTC()), nil
	}
	return result.Completed(cmd.CommandID, meta.CorrelationID, started, time.Now().UTC()), nil
}
