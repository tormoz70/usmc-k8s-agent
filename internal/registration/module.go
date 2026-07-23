package registration

import (
	"context"
	"log/slog"
	"time"

	registrationpb "github.com/usmc/usmc-k8s-agent/api/gen/registration"
	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/coreclient"
	"github.com/usmc/usmc-k8s-agent/internal/features"
	"github.com/usmc/usmc-k8s-agent/internal/modules"
	"github.com/usmc/usmc-k8s-agent/internal/protoheaders"
)

// Module performs self-registration with uamc-core on Start.
type Module struct {
	cfg    *config.Config
	client *coreclient.Client
	log    *slog.Logger
	names  []string
}

var _ modules.Module = (*Module)(nil)

func New(cfg *config.Config, client *coreclient.Client, moduleNames []string, log *slog.Logger) *Module {
	if log == nil {
		log = slog.Default()
	}
	return &Module{cfg: cfg, client: client, log: log, names: moduleNames}
}

func (m *Module) Name() string { return "registration" }

func (m *Module) Enabled(cfg *config.Config, _ *features.Registry) bool {
	if cfg == nil {
		return false
	}
	return cfg.Agent.RegistrationEnabled &&
		(cfg.Kafka.Mode == config.KafkaModeProtobuf || cfg.Kafka.Mode == config.KafkaModeDual)
}

func (m *Module) Start(ctx context.Context) error {
	if m.client == nil {
		m.log.Warn("registration skipped: no core client")
		return nil
	}
	delay := time.Duration(m.cfg.Agent.RegistrationDelayMs) * time.Millisecond
	if delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	req := &registrationpb.AgentRegistrationRequest{
		ClusterID:       m.cfg.ClusterID,
		AgentInstanceID: m.cfg.Agent.InstanceID,
		ClusterName:     m.cfg.ClusterID,
		Modules:         m.names,
	}
	body, err := req.Marshal()
	if err != nil {
		return err
	}
	respTopic := config.ResolveTopic(m.cfg.Kafka.OutResponseTopicTemplate, m.cfg.ClusterID)
	h := protoheaders.NewRequest(
		m.cfg.ClusterID,
		registrationpb.MessageTypeRequest,
		registrationpb.RequestTypeRegister,
		m.cfg.Kafka.OutRequestTopic,
		respTopic,
	)
	_, _, err = m.client.SendRequest(ctx, h.ToMap(), body)
	if err != nil {
		m.log.Warn("agent registration failed", "error", err)
		return nil // non-fatal: core may be absent in local json mode dual
	}
	m.log.Info("agent registered with uamc-core", "cluster_id", m.cfg.ClusterID)
	return nil
}

func (m *Module) Stop(context.Context) error { return nil }
