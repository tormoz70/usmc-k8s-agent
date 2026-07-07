package cachehandler

import (
	"encoding/json"
	"testing"
)

func TestParsePutPayload(t *testing.T) {
	raw := json.RawMessage(`{"entries":[{"key":"a","value":"b","ttl_seconds":60}]}`)
	p, err := ParsePutPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Entries) != 1 || p.Entries[0].Key != "a" {
		t.Fatalf("unexpected payload: %+v", p)
	}
}

func TestParsePutPayloadRejectsEmpty(t *testing.T) {
	_, err := ParsePutPayload(json.RawMessage(`{"entries":[]}`))
	if err == nil {
		t.Fatal("expected error")
	}
}
