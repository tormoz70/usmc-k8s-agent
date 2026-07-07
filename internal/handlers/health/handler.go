package healthhandler

import (
	"context"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/healthreport"
	"github.com/usmc/usmc-k8s-agent/internal/result"
)

type StartHandler struct {
	manager      *healthreport.Manager
	defaultTopic string
}

func NewStartHandler(manager *healthreport.Manager, defaultTopic string) *StartHandler {
	return &StartHandler{manager: manager, defaultTopic: defaultTopic}
}

func (h *StartHandler) Type() string {
	return command.TypeHealthReportStart
}

func (h *StartHandler) Handle(ctx context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error) {
	started := time.Now().UTC()
	payload, err := healthreport.ParseStartPayload(cmd.Payload)
	if err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "ValidationFailed", "INVALID_PAYLOAD", err.Error(), started, time.Now().UTC()), nil
	}
	if err := h.manager.Start(ctx, payload); err != nil {
		return result.Failed(cmd.CommandID, meta.CorrelationID, "HealthStartFailed", "HEALTH_REPORT_START_ERROR", err.Error(), started, time.Now().UTC()), nil
	}
	finished := time.Now().UTC()
	resp := result.Completed(cmd.CommandID, meta.CorrelationID, started, finished)
	resp.SubscriptionID = payload.SubscriptionID
	resp.OutputTopic = payload.OutputTopicOr(h.defaultTopic)
	resp.IntervalSeconds = payload.IntervalSeconds
	return resp, nil
}

type StopHandler struct {
	manager *healthreport.Manager
}

func NewStopHandler(manager *healthreport.Manager) *StopHandler {
	return &StopHandler{manager: manager}
}

func (h *StopHandler) Type() string {
	return command.TypeHealthReportStop
}

func (h *StopHandler) Handle(_ context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error) {
	started := time.Now().UTC()
	payload, err := healthreport.ParseStopPayload(cmd.Payload)
	if err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "ValidationFailed", "INVALID_PAYLOAD", err.Error(), started, time.Now().UTC()), nil
	}
	if err := h.manager.Stop(payload.SubscriptionID); err != nil {
		return result.Failed(cmd.CommandID, meta.CorrelationID, "HealthStopFailed", "HEALTH_REPORT_STOP_ERROR", err.Error(), started, time.Now().UTC()), nil
	}
	return result.Completed(cmd.CommandID, meta.CorrelationID, started, time.Now().UTC()), nil
}
