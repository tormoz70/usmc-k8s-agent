package ailogs

import (
	"context"
	"log/slog"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/features"
	"github.com/usmc/usmc-k8s-agent/internal/modules"
	"github.com/usmc/usmc-k8s-agent/internal/modules/cmdmods"
)

// Module aligns Java ailogscollector with existing logs.collect handler (feature alias).
type Module struct {
	inner *cmdmods.HandlerModule
	log   *slog.Logger
}

var _ modules.CommandProvider = (*Module)(nil)

func New(logsHandler command.Handler, log *slog.Logger) *Module {
	if log == nil {
		log = slog.Default()
	}
	inner := cmdmods.NewHandlerModule(
		"ailogscollector",
		[]string{"logs_collect"},
		[]string{command.TypeLogsCollect},
		logsHandler,
	)
	return &Module{inner: inner, log: log}
}

func (m *Module) Name() string { return "ailogscollector" }

func (m *Module) Enabled(cfg *config.Config, feat *features.Registry) bool {
	// Alias module: only advertise under protobuf/dual; handlers still come from logs module.
	if cfg == nil {
		return false
	}
	if cfg.Kafka.Mode != config.KafkaModeProtobuf && cfg.Kafka.Mode != config.KafkaModeDual {
		return false
	}
	return m.inner.Enabled(cfg, feat)
}

func (m *Module) Handlers() []command.Handler {
	// Avoid double-registering logs.collect; this module is lifecycle/metrics marker only.
	return nil
}

func (m *Module) Start(context.Context) error {
	m.log.Info("ailogscollector aligned with logs.collect")
	return nil
}

func (m *Module) Stop(context.Context) error { return nil }
