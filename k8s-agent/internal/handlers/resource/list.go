package resource

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/usmc/k8s-agent/internal/command"
	"github.com/usmc/k8s-agent/internal/k8s"
	"github.com/usmc/k8s-agent/internal/policy"
	"github.com/usmc/k8s-agent/internal/result"
)

type ListHandler struct {
	lister *k8s.Lister
	policy *policy.Engine
	maxInline int64
}

func NewListHandler(lister *k8s.Lister, pe *policy.Engine, maxInline int64) *ListHandler {
	return &ListHandler{lister: lister, policy: pe, maxInline: maxInline}
}

func (h *ListHandler) Type() string {
	return command.TypeResourceList
}

type listPayload struct {
	LabelSelector  string   `json:"labelSelector"`
	FieldSelector  string   `json:"fieldSelector"`
	OutputFormat   string   `json:"output_format"`
	Limit          int64    `json:"limit"`
	ContinueToken  string   `json:"continue_token"`
	StripStatus    bool     `json:"strip_status"`
	Namespaces     []string `json:"namespaces"`
}

func (h *ListHandler) Handle(ctx context.Context, cmd command.Command) (*command.Result, error) {
	res := result.NewResult(cmd, command.StatusExecuting)

	gvk := command.GVK{
		Group:   cmd.Target.Group,
		Version: cmd.Target.Version,
		Kind:    cmd.Target.Kind,
	}
	if err := h.policy.CheckGVK(gvk); err != nil {
		return result.Rejected(cmd, "POLICY_VIOLATION", err.Error()), nil
	}
	if cmd.Target.Namespace != "" {
		if err := h.policy.CheckNamespace(cmd.Target.Namespace); err != nil {
			return result.Rejected(cmd, "POLICY_VIOLATION", err.Error()), nil
		}
	}

	var payload listPayload
	if len(cmd.Payload) > 0 {
		if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
			return result.Rejected(cmd, "INVALID_PAYLOAD", err.Error()), nil
		}
	}
	if payload.OutputFormat == "" {
		payload.OutputFormat = "yaml"
	}

	out, err := h.lister.List(ctx, k8s.ListOptions{
		GVK:           gvk,
		Namespace:     cmd.Target.Namespace,
		LabelSelector: payload.LabelSelector,
		FieldSelector: payload.FieldSelector,
		Limit:         payload.Limit,
		ContinueToken: payload.ContinueToken,
		OutputFormat:  payload.OutputFormat,
		StripStatus:   payload.StripStatus,
		Namespaces:    payload.Namespaces,
	})
	if err != nil {
		return result.Failed(cmd, "LIST_FAILED", err.Error()), nil
	}

	var totalSize int64
	for _, item := range out.Items {
		totalSize += int64(len(item))
	}
	if h.maxInline > 0 && totalSize > h.maxInline {
		out.Truncated = true
		return result.Failed(cmd, "PAYLOAD_TOO_LARGE",
			fmt.Sprintf("list result %d bytes exceeds inline limit %d; use file.fetch resource_export", totalSize, h.maxInline)), nil
	}

	now := time.Now().UTC()
	res.Status = command.StatusCompleted
	res.FinishedAt = &now
	details, _ := json.Marshal(out)
	res.Details = details
	return res, nil
}
