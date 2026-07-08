package command

import (
	"fmt"
	"strings"
)

var allowedTypes = map[string]struct{}{
	TypeResourceList:          {},
	TypeFileFetch:             {},
	TypeWatchSubscribe:        {},
	TypeWatchUnsubscribe:      {},
	TypeLogsStreamSubscribe:   {},
	TypeLogsStreamUnsubscribe: {},
}

func Validate(cmd *Command) error {
	if cmd == nil {
		return fmt.Errorf("command is nil")
	}
	if strings.TrimSpace(cmd.CommandID) == "" {
		return fmt.Errorf("command_id is required")
	}
	if strings.TrimSpace(cmd.IdempotencyKey) == "" {
		return fmt.Errorf("idempotency_key is required")
	}
	if strings.TrimSpace(cmd.Type) == "" {
		return fmt.Errorf("type is required")
	}
	if _, ok := allowedTypes[cmd.Type]; !ok {
		return fmt.Errorf("unsupported command type: %s", cmd.Type)
	}
	if strings.TrimSpace(cmd.IssuedBy) == "" {
		return fmt.Errorf("issued_by is required")
	}
	if cmd.TS.IsZero() {
		return fmt.Errorf("ts is required")
	}
	return nil
}
