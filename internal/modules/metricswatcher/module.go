package metricswatcher

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/coreclient"
	"github.com/usmc/usmc-k8s-agent/internal/features"
	"github.com/usmc/usmc-k8s-agent/internal/modules"
	"github.com/usmc/usmc-k8s-agent/internal/protoheaders"
)

// Module periodically scrapes node/pod counts and publishes to metrics watcher topic.
type Module struct {
	cfg      *config.Config
	client   *coreclient.Client
	kube     kubernetes.Interface
	log      *slog.Logger
	interval time.Duration
	cancel   context.CancelFunc
}

var _ modules.Module = (*Module)(nil)

func New(cfg *config.Config, client *coreclient.Client, kube kubernetes.Interface, log *slog.Logger) *Module {
	if log == nil {
		log = slog.Default()
	}
	return &Module{
		cfg:      cfg,
		client:   client,
		kube:     kube,
		log:      log,
		interval: 60 * time.Second,
	}
}

func (m *Module) Name() string { return "metricswatcher" }

func (m *Module) Enabled(cfg *config.Config, _ *features.Registry) bool {
	if cfg == nil {
		return false
	}
	return cfg.Kafka.Mode == config.KafkaModeProtobuf || cfg.Kafka.Mode == config.KafkaModeDual
}

func (m *Module) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	go m.loop(runCtx)
	m.log.Info("metricswatcher started", "interval", m.interval, "topic", m.cfg.Kafka.MetricsWatcherTopic)
	return nil
}

func (m *Module) Stop(context.Context) error {
	if m.cancel != nil {
		m.cancel()
	}
	return nil
}

func (m *Module) loop(ctx context.Context) {
	t := time.NewTicker(m.interval)
	defer t.Stop()
	m.scrape(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.scrape(ctx)
		}
	}
}

type snapshot struct {
	ClusterID string `json:"cluster_id"`
	Nodes     int    `json:"nodes"`
	Pods      int    `json:"pods"`
	At        string `json:"observed_at"`
}

func (m *Module) scrape(ctx context.Context) {
	if m.kube == nil || m.client == nil {
		return
	}
	nodes, err := m.kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{Limit: 500})
	if err != nil {
		m.log.Warn("metricswatcher nodes list failed", "error", err)
		return
	}
	pods, err := m.kube.CoreV1().Pods("").List(ctx, metav1.ListOptions{Limit: 5000})
	if err != nil {
		m.log.Warn("metricswatcher pods list failed", "error", err)
		return
	}
	body, _ := json.Marshal(snapshot{
		ClusterID: m.cfg.ClusterID,
		Nodes:     len(nodes.Items),
		Pods:      len(pods.Items),
		At:        time.Now().UTC().Format(time.RFC3339),
	})
	h := protoheaders.NewRequest(
		m.cfg.ClusterID,
		"ClusterMetricsSnapshot",
		"METRICS_SNAPSHOT",
		m.cfg.Kafka.MetricsWatcherTopic,
		"",
	)
	h.Addressee = "uamc-metrics-watcher"
	if err := m.client.SendRequestVoid(ctx, h.ToMap(), body); err != nil {
		m.log.Warn("metricswatcher publish failed", "error", err)
	}
}
