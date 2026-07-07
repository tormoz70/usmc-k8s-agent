package command

import (
	"context"
	"fmt"
)

type Handler interface {
	Type() string
	Handle(ctx context.Context, cmd Command) (*Result, error)
}

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

func (r *Router) Route(ctx context.Context, cmd Command) (*Result, error) {
	h, ok := r.handlers[cmd.Type]
	if !ok {
		return nil, fmt.Errorf("no handler for type %s", cmd.Type)
	}
	return h.Handle(ctx, cmd)
}

func (r *Router) Types() []string {
	out := make([]string, 0, len(r.handlers))
	for t := range r.handlers {
		out = append(out, t)
	}
	return out
}
