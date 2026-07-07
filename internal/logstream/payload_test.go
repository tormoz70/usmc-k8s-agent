package logstream

import (
	"encoding/json"
	"testing"
)

func TestParseStartPayload(t *testing.T) {
	raw := json.RawMessage(`{
		"subscription_id":"sub-1",
		"namespace":"default",
		"pod":"api-abc",
		"container":"app",
		"follow":true,
		"tail_lines":100
	}`)
	p, err := ParseStartPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	if p.SubscriptionID != "sub-1" || p.Container != "app" {
		t.Fatalf("unexpected payload: %+v", p)
	}
}

func TestParseStartPayloadRequiresFields(t *testing.T) {
	_, err := ParseStartPayload(json.RawMessage(`{"subscription_id":"sub-1"}`))
	if err == nil {
		t.Fatal("expected error")
	}
}
