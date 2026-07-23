package ottlogstrue

import (
	"context"
	"log/slog"
	"sync"
	"time"

	ottpb "github.com/usmc/usmc-k8s-agent/api/gen/ottlogstrue"
	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/coreclient"
	"github.com/usmc/usmc-k8s-agent/internal/features"
	"github.com/usmc/usmc-k8s-agent/internal/modules"
	"github.com/usmc/usmc-k8s-agent/internal/protoheaders"
)

// Module is the OttLogTrueWatcher vertical (scheduler + config + parsers).
// Full sidecar log streaming is enabled when config is received; parser/bucketizer are always tested.
type Module struct {
	cfg    *config.Config
	client *coreclient.Client
	log    *slog.Logger
	parser *Parser

	mu     sync.Mutex
	cancel context.CancelFunc
	cfgRsp *ottpb.OttLogTrueConfigResponse
}

var _ modules.Module = (*Module)(nil)

func New(cfg *config.Config, client *coreclient.Client, log *slog.Logger) *Module {
	if log == nil {
		log = slog.Default()
	}
	return &Module{cfg: cfg, client: client, log: log, parser: NewParser()}
}

func (m *Module) Name() string { return "ottlogstrue" }

func (m *Module) Enabled(cfg *config.Config, _ *features.Registry) bool {
	if cfg == nil {
		return false
	}
	return cfg.Kafka.Mode == config.KafkaModeProtobuf || cfg.Kafka.Mode == config.KafkaModeDual
}

func (m *Module) Start(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	m.cancel = cancel
	go m.scheduler(runCtx)
	m.log.Info("ottlogstrue module started")
	return nil
}

func (m *Module) Stop(context.Context) error {
	if m.cancel != nil {
		m.cancel()
	}
	return nil
}

func (m *Module) scheduler(ctx context.Context) {
	// Default scan every 5 minutes until config overrides (units: minutes per migration open Q).
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	m.refreshConfig(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.refreshConfig(ctx)
			m.emitHealthchecks(ctx)
		}
	}
}

func (m *Module) refreshConfig(ctx context.Context) {
	if m.client == nil {
		return
	}
	req := &ottpb.OttLogTrueConfigRequest{ClusterID: m.cfg.ClusterID}
	body, err := req.Marshal()
	if err != nil {
		return
	}
	respTopic := config.ResolveTopic(m.cfg.Kafka.OutResponseTopicTemplate, m.cfg.ClusterID)
	h := protoheaders.NewRequest(
		m.cfg.ClusterID,
		ottpb.MessageTypeConfigRequest,
		"OTT_LOG_TRUE_CONFIG",
		m.cfg.Kafka.OutRequestTopic,
		respTopic,
	)
	out, _, err := m.client.SendRequest(ctx, h.ToMap(), body)
	if err != nil {
		m.log.Debug("ottlogstrue config request failed", "error", err)
		return
	}
	var rsp ottpb.OttLogTrueConfigResponse
	if err := rsp.Unmarshal(out); err != nil {
		m.log.Warn("ottlogstrue config decode failed", "error", err)
		return
	}
	m.mu.Lock()
	m.cfgRsp = &rsp
	m.mu.Unlock()
	m.log.Info("ottlogstrue config loaded", "namespaces", len(rsp.NamespacesOfCluster), "reply_topic", rsp.ReplyTopicEvents)
}

func (m *Module) emitHealthchecks(ctx context.Context) {
	m.mu.Lock()
	cfg := m.cfgRsp
	m.mu.Unlock()
	if cfg == nil || cfg.ReplyTopicEvents == "" || m.client == nil {
		return
	}
	rec := m.parser.Parse(nil, true)
	rec.Received = time.Now().UnixMilli()
	ev := ottpb.OttLogTrueSidecarEvent{
		RequestResult:     rec.RequestResult,
		HealthcheckRecord: true,
		Received:          rec.Received,
	}
	sender := NewSender(m.client, cfg.ReplyTopicEvents, m.cfg.ClusterID, "_health", "_", "_")
	_ = sender.Add(ctx, ev)
	_ = sender.Close(ctx)
}

// Parser exposes parser for tests.
func (m *Module) Parser() *Parser { return m.parser }
