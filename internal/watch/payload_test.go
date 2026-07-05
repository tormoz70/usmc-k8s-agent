package watch

import (
	"encoding/json"
	"testing"
)

func TestParseSubscribePayload(t *testing.T) {
	raw := json.RawMessage(`{
		"subscription_id": "sub-1",
		"gvk": {"group":"apps","version":"v1","kind":"Deployment"},
		"namespace": "default",
		"label_selector": "app=api"
	}`)
	p, err := ParseSubscribePayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	if p.SubscriptionID != "sub-1" {
		t.Fatalf("id=%q", p.SubscriptionID)
	}
	if len(p.EventFilter) != 3 {
		t.Fatalf("filters=%v", p.EventFilter)
	}
}

func TestAllowsRestart(t *testing.T) {
	p := &SubscribePayload{EventFilter: []string{"RESTART"}}
	if !p.Allows(EventRestart) {
		t.Fatal("expected restart allowed")
	}
}

func TestOutputTopicDefault(t *testing.T) {
	p := &SubscribePayload{}
	if p.OutputTopicOr("cluster.events") != "cluster.events" {
		t.Fatal("expected default topic")
	}
}
