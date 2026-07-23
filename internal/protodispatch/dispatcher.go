package protodispatch

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/usmc/usmc-k8s-agent/internal/protoheaders"
	"github.com/usmc/usmc-k8s-agent/internal/transport"
)

// Handler handles one protobuf messageType.
type Handler func(ctx context.Context, headers protoheaders.Headers, body []byte) error

// Dispatcher routes by messageType.
type Dispatcher struct {
	handlers map[string]Handler
	log      *slog.Logger
}

func New(log *slog.Logger) *Dispatcher {
	if log == nil {
		log = slog.Default()
	}
	return &Dispatcher{handlers: make(map[string]Handler), log: log}
}

func (d *Dispatcher) Register(messageType string, h Handler) {
	d.handlers[messageType] = h
}

var _ transport.ProtoDispatcher = (*Dispatcher)(nil)

func (d *Dispatcher) HandleProto(ctx context.Context, headers map[string]string, body []byte) error {
	h := protoheaders.FromMap(headers)
	fn, ok := d.handlers[h.MessageType]
	if !ok {
		d.log.Warn("unknown protobuf messageType", "messageType", h.MessageType, "requestType", h.RequestType)
		return fmt.Errorf("unknown messageType %q", h.MessageType)
	}
	return fn(ctx, h, body)
}
