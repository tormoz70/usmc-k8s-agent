package logstreamhandler

import (
	"context"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/logstream"
	"github.com/usmc/usmc-k8s-agent/internal/result"
)

type StartHandler struct {
	manager      *logstream.Manager
	defaultTopic string
}

func NewStartHandler(manager *logstream.Manager, defaultTopic string) *StartHandler {
	return &StartHandler{manager: manager, defaultTopic: defaultTopic}
}

func (h *StartHandler) Type() string {
	return command.TypeLogsStreamStart
}

func (h *StartHandler) Handle(ctx context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error) {
	started := time.Now().UTC()
	payload, err := logstream.ParseStartPayload(cmd.Payload)
	if err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "ValidationFailed", "INVALID_PAYLOAD", err.Error(), started, time.Now().UTC()), nil
	}
	if err := h.manager.Start(ctx, payload); err != nil {
		return result.Failed(cmd.CommandID, meta.CorrelationID, "StreamStartFailed", "LOG_STREAM_START_ERROR", err.Error(), started, time.Now().UTC()), nil
	}
	finished := time.Now().UTC()
	resp := result.Completed(cmd.CommandID, meta.CorrelationID, started, finished)
	resp.SubscriptionID = payload.SubscriptionID
	resp.OutputTopic = payload.OutputTopicOr(h.defaultTopic)
	return resp, nil
}

type StopHandler struct {
	manager *logstream.Manager
}

func NewStopHandler(manager *logstream.Manager) *StopHandler {
	return &StopHandler{manager: manager}
}

func (h *StopHandler) Type() string {
	return command.TypeLogsStreamStop
}

func (h *StopHandler) Handle(_ context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error) {
	started := time.Now().UTC()
	payload, err := logstream.ParseStopPayload(cmd.Payload)
	if err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "ValidationFailed", "INVALID_PAYLOAD", err.Error(), started, time.Now().UTC()), nil
	}
	if err := h.manager.Stop(payload.SubscriptionID); err != nil {
		return result.Failed(cmd.CommandID, meta.CorrelationID, "StreamStopFailed", "LOG_STREAM_STOP_ERROR", err.Error(), started, time.Now().UTC()), nil
	}
	return result.Completed(cmd.CommandID, meta.CorrelationID, started, time.Now().UTC()), nil
}
