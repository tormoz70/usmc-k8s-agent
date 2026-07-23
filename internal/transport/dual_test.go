package transport

import (
	"context"
	"testing"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/protoheaders"
	"github.com/usmc/usmc-k8s-agent/internal/result"
)

type stubExec struct{ called bool }

func (s *stubExec) Handle(context.Context, *command.Command, command.RequestMeta) (*result.Response, error) {
	s.called = true
	return &result.Response{Status: "ok"}, nil
}

type stubProto struct{ called bool }

func (s *stubProto) HandleProto(context.Context, map[string]string, []byte) error {
	s.called = true
	return nil
}

func TestDualAdapterJSON(t *testing.T) {
	ex := &stubExec{}
	d := NewDualAdapter(ModeJSON, ex, nil, nil, nil)
	msg := kafkago.Message{
		Value: []byte(`{"schema_version":"v1","command_id":"1","type":"k8s.api","issuer":"t","idempotency_key":"1","issued_at":"2026-01-01T00:00:00Z","payload":{}}`),
		Headers: []kafkago.Header{
			{Key: "correlation_id", Value: []byte("c")},
			{Key: "reply_topic", Value: []byte("r")},
		},
	}
	if _, err := d.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !ex.called {
		t.Fatal("executor not called")
	}
}

func TestDualAdapterProtobuf(t *testing.T) {
	p := &stubProto{}
	d := NewDualAdapter(ModeProtobuf, nil, p, nil, nil)
	msg := kafkago.Message{
		Value: []byte(`{}`),
		Headers: []kafkago.Header{
			{Key: protoheaders.KeyMessageType, Value: []byte("AgentRegistrationRequest")},
			{Key: protoheaders.KeyDirection, Value: []byte(protoheaders.DirectionRequest)},
		},
	}
	if _, err := d.HandleMessage(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
	if !p.called {
		t.Fatal("proto dispatcher not called")
	}
}
