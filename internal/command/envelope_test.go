package command

import (
	"testing"
	"time"
)

func TestCommandValidate(t *testing.T) {
	cmd := &Command{
		SchemaVersion:  SchemaVersionV1,
		CommandID:      "cmd-1",
		Type:           TypeK8sAPI,
		IdempotencyKey: "key-1",
		IssuedAt:       time.Now().UTC(),
	}
	if err := cmd.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
}

func TestCommandValidateMissingFields(t *testing.T) {
	cmd := &Command{SchemaVersion: SchemaVersionV1}
	if err := cmd.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestTimeoutDuration(t *testing.T) {
	cmd := &Command{Timeout: "45s"}
	if d := cmd.TimeoutDuration(30 * time.Second); d != 45*time.Second {
		t.Fatalf("duration=%s", d)
	}
}
