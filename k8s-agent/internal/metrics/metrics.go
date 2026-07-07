package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	CommandsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "k8s_agent_commands_total",
		Help: "Total commands processed by type and status",
	}, []string{"type", "status"})

	CommandDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "k8s_agent_command_duration_seconds",
		Help:    "Command execution duration",
		Buckets: prometheus.DefBuckets,
	}, []string{"type"})

	WatchSubscriptions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "k8s_agent_watch_subscriptions_active",
		Help: "Active watch subscriptions",
	})

	LogStreamSubscriptions = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "k8s_agent_log_stream_subscriptions_active",
		Help: "Active log stream subscriptions",
	})

	LogLinesMatched = promauto.NewCounter(prometheus.CounterOpts{
		Name: "k8s_agent_log_lines_matched_total",
		Help: "Total log lines matched and published",
	})

	FileUploadBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "k8s_agent_file_upload_bytes_total",
		Help: "Total bytes uploaded via presigned URL",
	})

	ErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "k8s_agent_errors_total",
		Help: "Total errors by component",
	}, []string{"component"})
)

func Handler() http.Handler {
	return promhttp.Handler()
}

func ObserveCommand(cmdType, status string, start time.Time) {
	CommandsTotal.WithLabelValues(cmdType, status).Inc()
	CommandDuration.WithLabelValues(cmdType).Observe(time.Since(start).Seconds())
}
