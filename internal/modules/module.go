package modules

import (
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/features"
)

// Module is a lifecycle-managed agent capability (Spring profile analogue).
type Module interface {
	Name() string
	Enabled(cfg *config.Config, feat *features.Registry) bool
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

// CommandProvider optionally contributes Kafka command handlers.
type CommandProvider interface {
	Module
	Handlers() []command.Handler
}

// Registry starts/stops enabled modules and collects handlers.
type Registry struct {
	mu      sync.Mutex
	modules []Module
	started []Module
	log     *slog.Logger
}

func NewRegistry(log *slog.Logger, mods ...Module) *Registry {
	if log == nil {
		log = slog.Default()
	}
	return &Registry{modules: mods, log: log}
}

// Register appends a module before Start.
func (r *Registry) Register(m Module) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.modules = append(r.modules, m)
}

// Handlers returns handlers from enabled CommandProvider modules.
func (r *Registry) Handlers(cfg *config.Config, feat *features.Registry) []command.Handler {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []command.Handler
	for _, m := range r.modules {
		if !m.Enabled(cfg, feat) {
			continue
		}
		if cp, ok := m.(CommandProvider); ok {
			out = append(out, cp.Handlers()...)
		}
	}
	return out
}

// Start starts all enabled modules.
func (r *Registry) Start(ctx context.Context, cfg *config.Config, feat *features.Registry) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, m := range r.modules {
		if !m.Enabled(cfg, feat) {
			r.log.Info("module disabled", "module", m.Name())
			continue
		}
		if err := m.Start(ctx); err != nil {
			return fmt.Errorf("module %s start: %w", m.Name(), err)
		}
		r.started = append(r.started, m)
		r.log.Info("module started", "module", m.Name())
	}
	return nil
}

// Stop stops started modules in reverse order.
func (r *Registry) Stop(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for i := len(r.started) - 1; i >= 0; i-- {
		m := r.started[i]
		if err := m.Stop(ctx); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("module %s stop: %w", m.Name(), err)
		}
	}
	r.started = nil
	return firstErr
}

// Names returns registered module names (for tests/diagnostics).
func (r *Registry) Names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.modules))
	for i, m := range r.modules {
		out[i] = m.Name()
	}
	return out
}
