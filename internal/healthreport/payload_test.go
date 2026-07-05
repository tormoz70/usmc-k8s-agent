package healthreport

import (
	"encoding/json"
	"testing"
)

func TestParseStartPayload(t *testing.T) {
	raw := json.RawMessage(`{
		"subscription_id":"health-sub-001",
		"interval_seconds":60,
		"namespaces":["default"]
	}`)
	p, err := ParseStartPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	if p.MaxPodsPerMessage != 500 {
		t.Fatalf("expected default max pods 500, got %d", p.MaxPodsPerMessage)
	}
}

func TestParseStartPayloadRejectsBadInterval(t *testing.T) {
	_, err := ParseStartPayload(json.RawMessage(`{"subscription_id":"x","interval_seconds":0}`))
	if err == nil {
		t.Fatal("expected error")
	}
}
