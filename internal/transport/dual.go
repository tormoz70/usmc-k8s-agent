package transport

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	kafkago "github.com/segmentio/kafka-go"

	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/protoheaders"
	"github.com/usmc/usmc-k8s-agent/internal/result"
)

// ResponseSink resolves pending CoreClient requests from RESPONSE messages.
type ResponseSink interface {
	HandleInboundResponse(headers map[string]string, body []byte) bool
}

// DualAdapter routes Kafka messages to JSON executor or protobuf dispatcher by mode + headers.
type DualAdapter struct {
	mode      Mode
	executor  Executor
	proto     ProtoDispatcher
	responses ResponseSink
	log       *slog.Logger
}

func NewDualAdapter(mode Mode, executor Executor, proto ProtoDispatcher, responses ResponseSink, log *slog.Logger) *DualAdapter {
	if log == nil {
		log = slog.Default()
	}
	return &DualAdapter{mode: mode, executor: executor, proto: proto, responses: responses, log: log}
}

// DetectModeFromConfig maps config string to Mode.
func DetectModeFromConfig(cfg config.KafkaConfig) Mode {
	switch strings.ToLower(cfg.Mode) {
	case string(ModeProtobuf):
		return ModeProtobuf
	case string(ModeDual):
		return ModeDual
	default:
		return ModeJSON
	}
}

// RequestTopicForMode returns the Kafka request topic for the active mode.
func RequestTopicForMode(cfg config.KafkaConfig, clusterID string) string {
	mode := DetectModeFromConfig(cfg)
	switch mode {
	case ModeProtobuf:
		return config.ResolveTopic(cfg.InRequestTopicTemplate, clusterID)
	case ModeDual:
		return cfg.RequestTopic
	default:
		return cfg.RequestTopic
	}
}

// ProtobufRequestTopic returns the uamc-core inbound topic.
func ProtobufRequestTopic(cfg config.KafkaConfig, clusterID string) string {
	return config.ResolveTopic(cfg.InRequestTopicTemplate, clusterID)
}

// HandleMessage routes one Kafka message.
func (d *DualAdapter) HandleMessage(ctx context.Context, msg kafkago.Message) (*result.Response, error) {
	headers := headerMap(msg.Headers)

	if d.responses != nil && protoheaders.FromMap(headers).Direction == protoheaders.DirectionResponse {
		if d.responses.HandleInboundResponse(headers, msg.Value) {
			return nil, nil
		}
	}

	switch d.mode {
	case ModeJSON:
		return d.handleJSON(ctx, msg)
	case ModeProtobuf:
		return d.handleProto(ctx, headers, msg.Value)
	default:
		if looksLikeJSONCommand(msg.Value, headers) {
			return d.handleJSON(ctx, msg)
		}
		if protoheaders.FromMap(headers).MessageType != "" || protoheaders.FromMap(headers).Direction != "" {
			return d.handleProto(ctx, headers, msg.Value)
		}
		return d.handleJSON(ctx, msg)
	}
}

func (d *DualAdapter) handleJSON(ctx context.Context, msg kafkago.Message) (*result.Response, error) {
	if d.executor == nil {
		return nil, nil
	}
	var cmd command.Command
	if err := json.Unmarshal(msg.Value, &cmd); err != nil {
		return nil, err
	}
	meta := command.RequestMeta{
		CorrelationID: headerValue(msg.Headers, "correlation_id"),
		ReplyTopic:    headerValue(msg.Headers, "reply_topic"),
		Partition:     msg.Partition,
		Offset:        msg.Offset,
	}
	return d.executor.Handle(ctx, &cmd, meta)
}

func (d *DualAdapter) handleProto(ctx context.Context, headers map[string]string, body []byte) (*result.Response, error) {
	if d.proto == nil {
		d.log.Warn("protobuf message received but no dispatcher configured",
			"messageType", protoheaders.FromMap(headers).MessageType)
		return nil, nil
	}
	return nil, d.proto.HandleProto(ctx, headers, body)
}

func looksLikeJSONCommand(body []byte, headers map[string]string) bool {
	if _, ok := headers["correlation_id"]; ok {
		if _, ok2 := headers["reply_topic"]; ok2 {
			var probe struct {
				Type string `json:"type"`
			}
			if json.Unmarshal(body, &probe) == nil && probe.Type != "" {
				return true
			}
		}
	}
	var probe struct {
		SchemaVersion string `json:"schema_version"`
		Type          string `json:"type"`
	}
	if json.Unmarshal(body, &probe) == nil && (probe.Type != "" || probe.SchemaVersion != "") {
		return true
	}
	return false
}

func headerMap(headers []kafkago.Header) map[string]string {
	m := make(map[string]string, len(headers))
	for _, h := range headers {
		m[h.Key] = string(h.Value)
	}
	return m
}

func headerValue(headers []kafkago.Header, key string) string {
	for _, h := range headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
