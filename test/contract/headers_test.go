package contract_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/usmc/usmc-k8s-agent/internal/protoheaders"
)

func TestRegistrationFixtureHeaders(t *testing.T) {
	_, file, _, _ := runtime.Caller(0)
	path := filepath.Join(filepath.Dir(file), "..", "..", "contracts", "fixtures", "registration-request.json")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Headers map[string]string   `json:"headers"`
		Body    json.RawMessage     `json:"body"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	h := protoheaders.FromMap(fixture.Headers)
	if h.MessageType != "AgentRegistrationRequest" || h.Addressee != protoheaders.AddresseeCore {
		t.Fatalf("headers=%+v", h)
	}
	round := protoheaders.FromMap(h.ToMap())
	if round.CorrelationID != h.CorrelationID {
		t.Fatalf("round-trip correlation mismatch")
	}
}
