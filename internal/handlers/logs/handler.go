package logs

import (
	"context"
	"log/slog"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/observability"
	"github.com/usmc/usmc-k8s-agent/internal/policy"
	"github.com/usmc/usmc-k8s-agent/internal/result"
)

// ReplyPublisher sends async command results to Kafka.
type ReplyPublisher interface {
	PublishResponse(ctx context.Context, replyTopic, correlationID string, resp *result.Response) error
}

// Handler runs logs.collect jobs asynchronously (pool size from config).
type Handler struct {
	collector *Collector
	policy    *policy.Engine
	publisher ReplyPublisher
	log       *slog.Logger
	metrics   *observability.Metrics
	jobs      chan struct{}
}

func NewHandler(collector *Collector, engine *policy.Engine, publisher ReplyPublisher, maxJobs int, log *slog.Logger) *Handler {
	if maxJobs < 1 {
		maxJobs = config.DefaultLogsCollectMaxJobs
	}
	if log == nil {
		log = slog.Default()
	}
	return &Handler{
		collector: collector,
		policy:    engine,
		publisher: publisher,
		jobs:      make(chan struct{}, maxJobs),
		log:       log,
	}
}

func (h *Handler) SetMetrics(metrics *observability.Metrics) {
	h.metrics = metrics
}

func (h *Handler) Type() string {
	return command.TypeLogsCollect
}

// Handle validates the command and starts an async collect job.
// Returns (nil, nil) when the job is scheduled; the handler publishes the final response.
func (h *Handler) Handle(ctx context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error) {
	started := time.Now().UTC()

	if err := h.policy.AllowCommandType(command.TypeLogsCollect); err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "PolicyDenied", "FORBIDDEN_COMMAND", err.Error(), started, time.Now().UTC()), nil
	}

	payload, err := ParseCollectPayload(cmd.Payload)
	if err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "ValidationFailed", "INVALID_PAYLOAD", err.Error(), started, time.Now().UTC()), nil
	}
	if err := h.policy.AllowNamespace(payload.Namespace); err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "PolicyDenied", "FORBIDDEN_NAMESPACE", err.Error(), started, time.Now().UTC()), nil
	}

	select {
	case h.jobs <- struct{}{}:
		h.syncActiveJobs(len(h.jobs))
	default:
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "TooManyJobs", "JOB_POOL_FULL",
			"logs.collect job pool is full", started, time.Now().UTC()), nil
	}

	timeout := cmd.TimeoutDuration(10 * time.Minute)
	go h.runJob(timeout, cmd, meta, payload, started)

	return nil, nil
}

func (h *Handler) runJob(timeout time.Duration, cmd *command.Command, meta command.RequestMeta, payload *CollectPayload, started time.Time) {
	defer func() {
		<-h.jobs
		h.syncActiveJobs(len(h.jobs))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	h.log.Info("logs.collect started", "command_id", cmd.CommandID, "namespace", payload.Namespace)

	collectResult, err := h.collector.Run(ctx, payload)
	finished := time.Now().UTC()
	if err != nil {
		resp := result.Failed(cmd.CommandID, meta.CorrelationID, "CollectFailed", "LOGS_COLLECT_ERROR", err.Error(), started, finished)
		h.publish(meta, resp)
		return
	}

	resp := result.Completed(cmd.CommandID, meta.CorrelationID, started, finished)
	resp.S3Bucket = collectResult.Bucket
	resp.S3Key = collectResult.Key
	resp.ByteSize = collectResult.ByteSize
	resp.FileCount = collectResult.FileCount
	resp.PartialErrors = collectResult.PartialErrors
	resp.Truncated = collectResult.Truncated
	if collectResult.Truncated {
		resp.Reason = "Truncated"
	}
	h.publish(meta, resp)
	h.log.Info("logs.collect finished", "command_id", cmd.CommandID, "bytes", collectResult.ByteSize, "truncated", collectResult.Truncated)
}

func (h *Handler) syncActiveJobs(n int) {
	if h.metrics != nil {
		h.metrics.LogsCollectJobsActive.Set(float64(n))
	}
}

func (h *Handler) publish(meta command.RequestMeta, resp *result.Response) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := h.publisher.PublishResponse(ctx, meta.ReplyTopic, meta.CorrelationID, resp); err != nil {
		h.log.Error("publish logs.collect response failed", "error", err, "command_id", resp.CommandID)
	}
}
