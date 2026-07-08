package command_test

import (
	"testing"
	"time"

	"github.com/usmc/k8s-agent/internal/command"
)

func TestValidateOK(t *testing.T) {
	cmd := command.Command{
		CommandID:      "id-1",
		IdempotencyKey: "key-1",
		Type:           command.TypeResourceList,
		IssuedBy:       "core-prod",
		TS:             time.Now().UTC(),
	}
	if err := command.Validate(&cmd); err != nil {
		t.Fatalf("expected valid command: %v", err)
	}
}

func TestValidateMissingFields(t *testing.T) {
	err := command.Validate(&command.Command{})
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestValidateUnsupportedType(t *testing.T) {
	cmd := command.Command{
		CommandID:      "id-1",
		IdempotencyKey: "key-1",
		Type:           "unknown.type",
		IssuedBy:       "core-prod",
		TS:             time.Now().UTC(),
	}
	if err := command.Validate(&cmd); err == nil {
		t.Fatal("expected unsupported type error")
	}
}
