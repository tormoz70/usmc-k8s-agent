package coreclient

import (
	"context"
	"testing"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/protoheaders"
)

type memPub struct {
	lastTopic   string
	lastHeaders map[string]string
	lastBody    []byte
}

func (m *memPub) PublishRaw(_ context.Context, topic, _ string, headers map[string]string, body []byte) error {
	m.lastTopic = topic
	m.lastHeaders = headers
	m.lastBody = append([]byte(nil), body...)
	return nil
}

func TestSendRequestVoidAndResponse(t *testing.T) {
	pub := &memPub{}
	c := New(pub, "cluster-a", "uamc-agent.ssl.request", time.Second, nil)
	h := protoheaders.NewRequest("cluster-a", "AgentRegistrationRequest", "REGISTER", "uamc-agent.ssl.request", "uamc-agent.ssl.response.x")
	body := []byte(`{"ok":true}`)
	go func() {
		time.Sleep(20 * time.Millisecond)
		respH := h.ToMap()
		respH[protoheaders.KeyDirection] = protoheaders.DirectionResponse
		_ = c.HandleInboundResponse(respH, []byte(`{"accepted":true}`))
	}()
	out, _, err := c.SendRequest(context.Background(), h.ToMap(), body)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `{"accepted":true}` {
		t.Fatalf("out=%s", out)
	}
	if pub.lastTopic != "uamc-agent.ssl.request" {
		t.Fatalf("topic=%s", pub.lastTopic)
	}
}
