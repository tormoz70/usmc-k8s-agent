package observability

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "k8s_agent"

// Metrics holds Prometheus collectors for the agent.
type Metrics struct {
	Leader                  prometheus.Gauge
	KafkaConnected          prometheus.Gauge
	APIServerOK             prometheus.Gauge
	CommandsTotal           *prometheus.CounterVec
	CommandDuration         *prometheus.HistogramVec
	WatchSubscriptions      prometheus.Gauge
	LogStreamsActive        prometheus.Gauge
	HealthReportsActive     prometheus.Gauge
	CacheEntries            prometheus.Gauge
	LogStreamDroppedLines   prometheus.Counter
	LogsCollectJobsActive   prometheus.Gauge
}

// DefaultMetrics registers and returns the standard agent metric set.
func DefaultMetrics() *Metrics {
	defaultMetricsOnce.Do(func() {
		defaultMetrics = newMetrics()
	})
	return defaultMetrics
}

var (
	defaultMetricsOnce sync.Once
	defaultMetrics     *Metrics
)

func newMetrics() *Metrics {
	m := &Metrics{
		Leader: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "leader",
			Help:      "1 when this pod is the elected leader.",
		}),
		KafkaConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "kafka_connected",
			Help:      "1 when Kafka client is connected.",
		}),
		APIServerOK: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "apiserver_ok",
			Help:      "1 when kube-apiserver ping succeeded.",
		}),
		CommandsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "commands_total",
			Help:      "Kafka commands processed by type and status.",
		}, []string{"type", "status"}),
		CommandDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "command_duration_seconds",
			Help:      "Kafka command handler duration.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"type"}),
		WatchSubscriptions: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "watch_subscriptions_active",
			Help:      "Active watch.subscribe subscriptions.",
		}),
		LogStreamsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "log_streams_active",
			Help:      "Active logs.stream subscriptions.",
		}),
		HealthReportsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "health_reports_active",
			Help:      "Active health.report subscriptions.",
		}),
		CacheEntries: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "cache_entries",
			Help:      "In-memory cache entries on leader.",
		}),
		LogStreamDroppedLines: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "log_stream_dropped_lines_total",
			Help:      "Log lines dropped due to backpressure.",
		}),
		LogsCollectJobsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "logs_collect_jobs_active",
			Help:      "In-flight logs.collect async jobs.",
		}),
	}
	prometheus.MustRegister(
		m.Leader,
		m.KafkaConnected,
		m.APIServerOK,
		m.CommandsTotal,
		m.CommandDuration,
		m.WatchSubscriptions,
		m.LogStreamsActive,
		m.HealthReportsActive,
		m.CacheEntries,
		m.LogStreamDroppedLines,
		m.LogsCollectJobsActive,
	)
	return m
}

func (m *Metrics) RecordCommand(commandType, status string, duration time.Duration) {
	if m == nil {
		return
	}
	m.CommandsTotal.WithLabelValues(commandType, status).Inc()
	m.CommandDuration.WithLabelValues(commandType).Observe(duration.Seconds())
}

func (m *Metrics) SyncFromState(state *RuntimeState) {
	if m == nil || state == nil {
		return
	}
	if state.IsLeader() {
		m.Leader.Set(1)
	} else {
		m.Leader.Set(0)
	}
	if state.KafkaConnected() {
		m.KafkaConnected.Set(1)
	} else {
		m.KafkaConnected.Set(0)
	}
	if state.APIServerOK() {
		m.APIServerOK.Set(1)
	} else {
		m.APIServerOK.Set(0)
	}
}
