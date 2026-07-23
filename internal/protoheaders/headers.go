package protoheaders

import (
	"strconv"
	"strings"
	"time"
)

// Kafka header keys (Java CoreClient style).
const (
	KeyMessageID        = "messageId"
	KeyCorrelationID    = "correlationId"
	KeyTopic            = "topic"
	KeyTopicForResponse = "topicForResponse"
	KeyDirection        = "direction"
	KeyAddressee        = "addressee"
	KeySender           = "sender"
	KeyMessageType      = "messageType"
	KeyRequestType      = "requestType"
	KeyTimestamp        = "timestamp"
	KeyZipped           = "zipped"
)

const (
	DirectionRequest  = "REQUEST"
	DirectionResponse = "RESPONSE"
	AddresseeCore     = "uamc-core"
)

// Headers is the Go view of ProtoHeaders on the Kafka wire.
type Headers struct {
	MessageID        string
	CorrelationID    string
	Topic            string
	TopicForResponse string
	Direction        string
	Addressee        string
	Sender           string
	MessageType      string
	RequestType      string
	Timestamp        string
	Zipped           bool
}

// ToMap encodes headers for Kafka.
func (h Headers) ToMap() map[string]string {
	m := map[string]string{
		KeyMessageID:        h.MessageID,
		KeyCorrelationID:    h.CorrelationID,
		KeyTopic:            h.Topic,
		KeyTopicForResponse: h.TopicForResponse,
		KeyDirection:        h.Direction,
		KeyAddressee:        h.Addressee,
		KeySender:           h.Sender,
		KeyMessageType:      h.MessageType,
		KeyRequestType:      h.RequestType,
		KeyTimestamp:        h.Timestamp,
	}
	if h.Zipped {
		m[KeyZipped] = "true"
	} else {
		m[KeyZipped] = "false"
	}
	return m
}

// FromMap decodes Kafka headers (case-sensitive Java keys; also accepts snake variants).
func FromMap(m map[string]string) Headers {
	get := func(keys ...string) string {
		for _, k := range keys {
			if v, ok := m[k]; ok {
				return v
			}
			for mk, mv := range m {
				if strings.EqualFold(mk, k) {
					return mv
				}
			}
		}
		return ""
	}
	zipped := strings.EqualFold(get(KeyZipped, "zipped"), "true")
	return Headers{
		MessageID:        get(KeyMessageID, "message_id"),
		CorrelationID:    get(KeyCorrelationID, "correlation_id", "correlationId"),
		Topic:            get(KeyTopic),
		TopicForResponse: get(KeyTopicForResponse, "topic_for_response"),
		Direction:        get(KeyDirection),
		Addressee:        get(KeyAddressee),
		Sender:           get(KeySender),
		MessageType:      get(KeyMessageType, "message_type"),
		RequestType:      get(KeyRequestType, "request_type"),
		Timestamp:        get(KeyTimestamp),
		Zipped:           zipped,
	}
}

// NewRequest builds REQUEST headers for agent → core.
func NewRequest(sender, messageType, requestType, topic, topicForResponse string) Headers {
	return Headers{
		MessageID:        newUUID(),
		CorrelationID:    newUUID(),
		Topic:            topic,
		TopicForResponse: topicForResponse,
		Direction:        DirectionRequest,
		Addressee:        AddresseeCore,
		Sender:           sender,
		MessageType:      messageType,
		RequestType:      requestType,
		Timestamp:        time.Now().UTC().Format(time.RFC3339Nano),
	}
}

func newUUID() string {
	// lightweight unique id without extra deps
	return strconv.FormatInt(time.Now().UnixNano(), 36) + "-" + strconv.FormatInt(time.Now().UnixNano()^0x5deece66d, 36)
}
