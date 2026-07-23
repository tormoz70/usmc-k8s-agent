package protoheaders

import (
	"testing"
)

func TestHeadersRoundTrip(t *testing.T) {
	h := Headers{
		MessageID:        "m1",
		CorrelationID:    "c1",
		Topic:            "uamc-agent.ssl.request",
		TopicForResponse: "uamc-agent.ssl.response.cluster-uamc-agent",
		Direction:        DirectionRequest,
		Addressee:        AddresseeCore,
		Sender:           "cluster-a",
		MessageType:      "AgentRegistrationRequest",
		RequestType:      "REGISTER",
		Timestamp:        "2026-01-01T00:00:00Z",
		Zipped:           true,
	}
	got := FromMap(h.ToMap())
	if got.CorrelationID != h.CorrelationID || !got.Zipped || got.Addressee != AddresseeCore {
		t.Fatalf("got=%+v", got)
	}
}
