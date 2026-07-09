package mockcorelib

import (
	"encoding/json"
	"testing"
)

func TestLoadScenarios(t *testing.T) {
	root, err := FindRepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := LoadScenarios(resolvePath(root, DefaultScenariosFile))
	if err != nil {
		t.Fatal(err)
	}
	if len(catalog.Scenarios) != 7 {
		t.Fatalf("expected 7 scenarios, got %d", len(catalog.Scenarios))
	}
	sc, err := catalog.FindScenario("02-list-pods")
	if err != nil {
		t.Fatal(err)
	}
	if sc.Command == "" {
		t.Fatal("expected command path")
	}
}

func TestAssertExpectation(t *testing.T) {
	body, err := json.Marshal(map[string]any{
		"status":          "completed",
		"http_status":     200,
		"subscription_id": "sub-1",
		"http_body":       map[string]any{"items": []any{}},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = assertExpectation(Message{Body: body}, ScenarioExpect{
		Status:     "completed",
		HTTPStatus: 200,
		Fields:     map[string]string{"subscription_id": "sub-1"},
		BodyContains: []string{"items"},
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestExtractFieldNumeric(t *testing.T) {
	body := json.RawMessage(`{"interval_seconds":30}`)
	if got := extractField(body, "interval_seconds"); got != "30" {
		t.Fatalf("got %q want 30", got)
	}
}
