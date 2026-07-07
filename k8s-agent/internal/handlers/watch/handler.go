package watch

import (
	"context"
	"encoding/json"

	"github.com/usmc/k8s-agent/internal/command"
	"github.com/usmc/k8s-agent/internal/policy"
	"github.com/usmc/k8s-agent/internal/result"
	"github.com/usmc/k8s-agent/internal/watchmanager"
)

type SubscribeHandler struct {
	manager *watchmanager.Manager
	policy  *policy.Engine
}

func NewSubscribeHandler(manager *watchmanager.Manager, pe *policy.Engine) *SubscribeHandler {
	return &SubscribeHandler{manager: manager, policy: pe}
}

func (h *SubscribeHandler) Type() string {
	return command.TypeWatchSubscribe
}

func (h *SubscribeHandler) Handle(ctx context.Context, cmd command.Command) (*command.Result, error) {
	var payload watchmanager.SubscribePayload
	if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
		return result.Rejected(cmd, "INVALID_PAYLOAD", err.Error()), nil
	}
	if payload.SubscriptionID == "" {
		return result.Rejected(cmd, "INVALID_PAYLOAD", "subscription_id required"), nil
	}
	if err := h.policy.CheckGVK(payload.GVK); err != nil {
		return result.Rejected(cmd, "POLICY_VIOLATION", err.Error()), nil
	}
	if payload.Namespace != "" {
		if err := h.policy.CheckNamespace(payload.Namespace); err != nil {
			return result.Rejected(cmd, "POLICY_VIOLATION", err.Error()), nil
		}
	}

	if err := h.manager.Subscribe(ctx, payload); err != nil {
		return result.Failed(cmd, "SUBSCRIBE_FAILED", err.Error()), nil
	}

	res := result.NewResult(cmd, command.StatusCompleted)
	now := res.StartedAt
	res.FinishedAt = &now
	details, _ := json.Marshal(map[string]any{"subscription_id": payload.SubscriptionID, "active": h.manager.ActiveCount()})
	res.Details = details
	return res, nil
}

type UnsubscribeHandler struct {
	manager *watchmanager.Manager
}

func NewUnsubscribeHandler(manager *watchmanager.Manager) *UnsubscribeHandler {
	return &UnsubscribeHandler{manager: manager}
}

func (h *UnsubscribeHandler) Type() string {
	return command.TypeWatchUnsubscribe
}

type unsubscribePayload struct {
	SubscriptionID string `json:"subscription_id"`
}

func (h *UnsubscribeHandler) Handle(ctx context.Context, cmd command.Command) (*command.Result, error) {
	var payload unsubscribePayload
	if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
		return result.Rejected(cmd, "INVALID_PAYLOAD", err.Error()), nil
	}
	if err := h.manager.Unsubscribe(payload.SubscriptionID); err != nil {
		return result.Failed(cmd, "UNSUBSCRIBE_FAILED", err.Error()), nil
	}
	res := result.NewResult(cmd, command.StatusCompleted)
	now := res.StartedAt
	res.FinishedAt = &now
	return res, nil
}
