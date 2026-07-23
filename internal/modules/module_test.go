package modules

import (
	"context"
	"testing"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/features"
	"github.com/usmc/usmc-k8s-agent/internal/result"
)

type stubMod struct {
	name    string
	enabled bool
	started bool
	stopped bool
	types   []string
}

func (s *stubMod) Name() string { return s.name }
func (s *stubMod) Enabled(*config.Config, *features.Registry) bool {
	return s.enabled
}
func (s *stubMod) Start(context.Context) error { s.started = true; return nil }
func (s *stubMod) Stop(context.Context) error  { s.stopped = true; return nil }
func (s *stubMod) Handlers() []command.Handler {
	out := make([]command.Handler, 0, len(s.types))
	for _, t := range s.types {
		out = append(out, &stubHandler{t: t})
	}
	return out
}

type stubHandler struct{ t string }

func (h *stubHandler) Type() string { return h.t }
func (h *stubHandler) Handle(context.Context, *command.Command, command.RequestMeta) (*result.Response, error) {
	return nil, nil
}

func TestRegistryHandlersAndLifecycle(t *testing.T) {
	a := &stubMod{name: "a", enabled: true, types: []string{"cmd.a"}}
	b := &stubMod{name: "b", enabled: false, types: []string{"cmd.b"}}
	c := &stubMod{name: "c", enabled: true, types: []string{"cmd.c"}}
	reg := NewRegistry(nil, a, b, c)

	cfg := &config.Config{}
	handlers := reg.Handlers(cfg, nil)
	if len(handlers) != 2 {
		t.Fatalf("handlers=%d want 2", len(handlers))
	}

	if err := reg.Start(context.Background(), cfg, nil); err != nil {
		t.Fatal(err)
	}
	if !a.started || b.started || !c.started {
		t.Fatalf("start flags a=%v b=%v c=%v", a.started, b.started, c.started)
	}
	if err := reg.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !a.stopped || !c.stopped {
		t.Fatalf("stop flags a=%v c=%v", a.stopped, c.stopped)
	}
}
