package cmdmods

import (
	"context"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/features"
	"github.com/usmc/usmc-k8s-agent/internal/modules"
)

// HandlerModule wraps a set of command handlers behind a named module.
type HandlerModule struct {
	name     string
	featKeys []string // feature names that enable this module (any)
	cmdTypes []string // command types; module enabled if any command is enabled
	handlers []command.Handler
}

var _ modules.CommandProvider = (*HandlerModule)(nil)

func NewHandlerModule(name string, featKeys, cmdTypes []string, handlers ...command.Handler) *HandlerModule {
	return &HandlerModule{
		name:     name,
		featKeys: featKeys,
		cmdTypes: cmdTypes,
		handlers: handlers,
	}
}

func (m *HandlerModule) Name() string { return m.name }

func (m *HandlerModule) Enabled(_ *config.Config, feat *features.Registry) bool {
	if feat == nil {
		return true
	}
	for _, f := range m.featKeys {
		if feat.Enabled(f) {
			return true
		}
	}
	for _, t := range m.cmdTypes {
		if feat.CommandEnabled(t) {
			return true
		}
	}
	return len(m.featKeys) == 0 && len(m.cmdTypes) == 0
}

func (m *HandlerModule) Handlers() []command.Handler {
	if m == nil {
		return nil
	}
	out := make([]command.Handler, 0, len(m.handlers))
	for _, h := range m.handlers {
		if h == nil {
			continue
		}
		out = append(out, h)
	}
	return out
}

func (m *HandlerModule) Start(context.Context) error { return nil }
func (m *HandlerModule) Stop(context.Context) error  { return nil }

// FilterHandlers keeps only handlers whose Type() is CommandEnabled.
func FilterHandlers(feat *features.Registry, handlers []command.Handler) []command.Handler {
	if feat == nil {
		return handlers
	}
	out := make([]command.Handler, 0, len(handlers))
	for _, h := range handlers {
		if feat.CommandEnabled(h.Type()) {
			out = append(out, h)
		}
	}
	return out
}
