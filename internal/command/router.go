package command

import (
	"context"
	"fmt"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/result"
)

// Handler executes one command type.
type Handler interface {
	Type() string
	Handle(ctx context.Context, cmd *Command, meta RequestMeta) (*result.Response, error)
}

// Router dispatches commands to registered handlers after validation.
type Router struct {
	handlers map[string]Handler
}

func NewRouter(handlers ...Handler) *Router {
	m := make(map[string]Handler, len(handlers))
	for _, h := range handlers {
		m[h.Type()] = h
	}
	return &Router{handlers: m}
}

func (r *Router) Handle(ctx context.Context, cmd *Command, meta RequestMeta) (*result.Response, error) {
	started := time.Now().UTC()
	if err := cmd.Validate(); err != nil {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "ValidationFailed", "INVALID_ENVELOPE", err.Error(), started, time.Now().UTC()), nil
	}
	h, ok := r.handlers[cmd.Type]
	if !ok {
		return result.Rejected(cmd.CommandID, meta.CorrelationID, "UnknownCommand", "UNKNOWN_TYPE",
			fmt.Sprintf("no handler for type %q", cmd.Type), started, time.Now().UTC()), nil
	}
	return h.Handle(ctx, cmd, meta)
}

// Register adds a handler (used in tests or phased wiring).
func (r *Router) Register(h Handler) {
	r.handlers[h.Type()] = h
}
