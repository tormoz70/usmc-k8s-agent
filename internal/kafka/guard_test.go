package kafka

import (
	"testing"
	"time"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/policy"
)

func TestCommandGuardRejectsUnknownIssuer(t *testing.T) {
	engine, err := policy.NewEngine(policy.Config{
		AllowedIssuers:      []string{"core-client"},
		AllowedReplyTopics:  []string{"core-client.dev.responses"},
		AllowedCommandTypes: []string{"k8s.api"},
	})
	if err != nil {
		t.Fatal(err)
	}
	guard := NewCommandGuard(engine)
	cmd := &command.Command{
		SchemaVersion:  command.SchemaVersionV1,
		CommandID:      "cmd-1",
		Type:           "k8s.api",
		Issuer:         "attacker",
		IdempotencyKey: "k1",
		IssuedAt:       time.Now().UTC(),
	}
	meta := command.RequestMeta{CorrelationID: "corr-1", ReplyTopic: "core-client.dev.responses"}
	if err := guard.Validate(cmd, meta); err == nil {
		t.Fatal("expected guard rejection")
	}
}

func TestCommandGuardRejectsUnknownReplyTopic(t *testing.T) {
	engine, err := policy.NewEngine(policy.Config{
		AllowedIssuers:      []string{"mock-core"},
		AllowedReplyTopics:  []string{"core-client.dev.responses"},
		AllowedCommandTypes: []string{"k8s.api"},
	})
	if err != nil {
		t.Fatal(err)
	}
	guard := NewCommandGuard(engine)
	cmd := &command.Command{
		SchemaVersion:  command.SchemaVersionV1,
		CommandID:      "cmd-1",
		Type:           "k8s.api",
		Issuer:         "mock-core",
		IdempotencyKey: "k1",
		IssuedAt:       time.Now().UTC(),
	}
	meta := command.RequestMeta{CorrelationID: "corr-1", ReplyTopic: "attacker-topic"}
	if err := guard.Validate(cmd, meta); err == nil {
		t.Fatal("expected reply topic rejection")
	}
}

func TestParseCommandRequiresHeaders(t *testing.T) {
	_, _, err := ParseCommand(kafkago.Message{Value: []byte(`{"schema_version":"v1","command_id":"c1","type":"k8s.api","issuer":"mock-core","idempotency_key":"k","issued_at":"2026-07-06T00:00:00Z"}`)})
	if err == nil {
		t.Fatal("expected missing headers error")
	}
}
