package transport

import (
	"context"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/result"
)

// Mode selects Kafka wire format.
type Mode string

const (
	ModeJSON     Mode = "json"
	ModeProtobuf Mode = "protobuf"
	ModeDual     Mode = "dual"
)

// Publisher publishes replies and fire-and-forget events.
type Publisher interface {
	PublishResponse(ctx context.Context, replyTopic, correlationID string, resp *result.Response) error
	PublishEvent(ctx context.Context, topic, key string, event any) error
	PublishRaw(ctx context.Context, topic, key string, headers map[string]string, body []byte) error
	Close() error
}

// Executor handles a decoded inbound command (JSON path).
type Executor interface {
	Handle(ctx context.Context, cmd *command.Command, meta command.RequestMeta) (*result.Response, error)
}

// ProtoDispatcher handles protobuf inbound messages (uamc-core path).
type ProtoDispatcher interface {
	HandleProto(ctx context.Context, headers map[string]string, body []byte) error
}

// RequestReply is the CoreClient-style async request API.
type RequestReply interface {
	SendRequest(ctx context.Context, headers map[string]string, body []byte) ([]byte, map[string]string, error)
	SendRequestVoid(ctx context.Context, headers map[string]string, body []byte) error
}
