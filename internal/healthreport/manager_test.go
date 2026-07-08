package healthreport

import (
	"testing"
)

func TestParseStartPayloadRequiresInterval(t *testing.T) {
	_, err := ParseStartPayload([]byte(`{"subscription_id":"x","interval_seconds":0}`))
	if err == nil {
		t.Fatal("expected interval error")
	}
}

func TestParseStartPayloadAcceptsTTL(t *testing.T) {
	p, err := ParseStartPayload([]byte(`{"subscription_id":"x","interval_seconds":30,"ttl_seconds":600}`))
	if err != nil {
		t.Fatal(err)
	}
	if p.TTLSeconds != 600 {
		t.Fatalf("ttl=%d", p.TTLSeconds)
	}
}
