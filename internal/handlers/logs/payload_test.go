package logs

import (
	"encoding/json"
	"testing"
	"time"
)

func TestParseCollectPayload(t *testing.T) {
	raw := json.RawMessage(`{
		"namespace": "default",
		"label_selector": "app=api",
		"include_current": true,
		"s3": {
			"bucket": "logs-bundles",
			"key": "logs/test.zip",
			"access_key_id": "key",
			"secret_access_key": "secret"
		}
	}`)
	p, err := ParseCollectPayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	if p.Namespace != "default" {
		t.Fatalf("namespace=%q", p.Namespace)
	}
}

func TestParseCollectPayloadMissingS3(t *testing.T) {
	raw := json.RawMessage(`{"namespace":"default"}`)
	_, err := ParseCollectPayload(raw)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestContainerNamesAll(t *testing.T) {
	p := &CollectPayload{Containers: "all"}
	names := p.ContainerNames([]string{"app", "sidecar"})
	if len(names) != 2 {
		t.Fatalf("names=%v", names)
	}
}

func TestContainerNamesList(t *testing.T) {
	p := &CollectPayload{Containers: []string{"app"}}
	names := p.ContainerNames([]string{"app", "sidecar"})
	if len(names) != 1 || names[0] != "app" {
		t.Fatalf("names=%v", names)
	}
}

func TestLogStatesDefault(t *testing.T) {
	p := &CollectPayload{IncludeCurrent: true, IncludePrevious: true}
	states := logStates(p)
	if len(states) != 2 {
		t.Fatalf("states=%v", states)
	}
	_ = time.Now()
}
