package api

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/k8s"
	"github.com/usmc/usmc-k8s-agent/internal/policy"
	"github.com/usmc/usmc-k8s-agent/internal/result"
)

// Handler proxies type=k8s.api HTTP-over-Kafka requests to kube-apiserver.
type Handler struct {
	k8s    *k8s.Client
	policy *policy.Engine
	trim   k8s.TrimOptions
}

func NewHandler(client *k8s.Client, engine *policy.Engine, trim k8s.TrimOptions) *Handler {
	return &Handler{k8s: client, policy: engine, trim: trim}
}

func (h *Handler) Type() string {
	return command.TypeK8sAPI
}

func (h *Handler) Handle(ctx context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error) {
	started := time.Now().UTC()
	if cmd.HTTP == nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "ValidationFailed", "MISSING_HTTP",
			"http field is required for k8s.api", started, time.Now().UTC()), nil
	}
	if err := h.policy.AllowCommandType(command.TypeK8sAPI); err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "PolicyDenied", "FORBIDDEN_COMMAND", err.Error(), started, time.Now().UTC()), nil
	}
	if err := h.policy.AllowHTTP(cmd.HTTP.Method, cmd.HTTP.Path); err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "PolicyDenied", "FORBIDDEN_RESOURCE", err.Error(), started, time.Now().UTC()), nil
	}

	timeout := cmd.TimeoutDuration(30 * time.Second)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	status, body, err := h.k8s.ProxyRequest(ctx, cmd.HTTP.Method, cmd.HTTP.Path, cmd.HTTP.Query, cmd.HTTP.Headers, cmd.HTTP.Body)
	finished := time.Now().UTC()
	if err != nil {
		return result.Failed(cmd.CommandID, meta.CorrelationID, "ProxyError", "K8S_API_ERROR", err.Error(), started, finished), nil
	}

	trimmed, err := k8s.TrimResponse(body, h.trim)
	if err != nil {
		return result.Failed(cmd.CommandID, meta.CorrelationID, "TrimError", "TRIM_ERROR", err.Error(), started, finished), nil
	}

	resp := result.Completed(cmd.CommandID, meta.CorrelationID, started, finished)
	resp.HTTPStatus = status
	resp.HTTPBody = json.RawMessage(trimmed)
	resp.ResourceVersion = k8s.ExtractResourceVersion(trimmed)
	if status >= 400 {
		resp.Status = result.StatusFailed
		resp.Reason = "HTTPError"
		resp.Error = &result.ErrorDetail{
			Code:    fmt.Sprintf("HTTP_%d", status),
			Message: fmt.Sprintf("apiserver returned status %d", status),
		}
	}
	return resp, nil
}
