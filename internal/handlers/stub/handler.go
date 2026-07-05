package stub

import (
	"context"
	"fmt"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/result"
)

// NotImplementedHandler returns rejected responses for command types not yet implemented.
type NotImplementedHandler struct {
	commandType string
	phase       string
}

func NewNotImplemented(commandType, phase string) *NotImplementedHandler {
	return &NotImplementedHandler{commandType: commandType, phase: phase}
}

func (h *NotImplementedHandler) Type() string {
	return h.commandType
}

func (h *NotImplementedHandler) Handle(_ context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error) {
	now := time.Now().UTC()
	return result.Rejected(cmd.CommandID, meta.CorrelationID, "NotImplemented", "NOT_IMPLEMENTED",
		fmt.Sprintf("command type %q is planned for %s", h.commandType, h.phase), now, now), nil
}
