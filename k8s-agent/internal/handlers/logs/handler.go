package logs

import (
	"context"
	"encoding/json"

	"github.com/usmc/k8s-agent/internal/command"
	"github.com/usmc/k8s-agent/internal/logstream"
	"github.com/usmc/k8s-agent/internal/policy"
	"github.com/usmc/k8s-agent/internal/result"
)

type SubscribeHandler struct {
	manager *logstream.Manager
	policy  *policy.Engine
}

func NewSubscribeHandler(manager *logstream.Manager, pe *policy.Engine) *SubscribeHandler {
	return &SubscribeHandler{manager: manager, policy: pe}
}

func (h *SubscribeHandler) Type() string {
	return command.TypeLogsStreamSubscribe
}

func (h *SubscribeHandler) Handle(ctx context.Context, cmd command.Command) (*command.Result, error) {
	var payload logstream.SubscribePayload
	if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
		return result.Rejected(cmd, "INVALID_PAYLOAD", err.Error()), nil
	}
	if payload.SubscriptionID == "" {
		return result.Rejected(cmd, "INVALID_PAYLOAD", "subscription_id required"), nil
	}
	if len(payload.Targets) == 0 {
		return result.Rejected(cmd, "INVALID_PAYLOAD", "targets required"), nil
	}
	maxPattern := h.policy.Policy().LogStream.MaxPatternLength
	if maxPattern > 0 && len(payload.Pattern) > maxPattern {
		return result.Rejected(cmd, "INVALID_PAYLOAD", "pattern too long"), nil
	}
	for _, t := range payload.Targets {
		if err := h.policy.CheckNamespace(t.Namespace); err != nil {
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
	manager *logstream.Manager
}

func NewUnsubscribeHandler(manager *logstream.Manager) *UnsubscribeHandler {
	return &UnsubscribeHandler{manager: manager}
}

func (h *UnsubscribeHandler) Type() string {
	return command.TypeLogsStreamUnsubscribe
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
