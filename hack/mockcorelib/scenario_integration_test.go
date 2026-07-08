//go:build integration

package mockcorelib

import (
	"context"
	"os"
	"testing"
	"time"
)

func TestIntegrationScenarios(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("set RUN_INTEGRATION=1 to run live Kafka scenarios (requires make dev-up)")
	}
	brokers := SplitBrokers(envOr("KAFKA_BROKERS", "localhost:9092"))
	if !KafkaReachable(brokers) {
		t.Skip("Kafka not reachable at " + envOr("KAFKA_BROKERS", "localhost:9092"))
	}
	root, err := FindRepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := LoadScenarios(resolvePath(root, DefaultScenariosFile))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	if err := RunAllScenarios(ctx, brokers, DefaultRequestTopic, DefaultReplyTopic, root, catalog); err != nil {
		t.Fatal(err)
	}
}

func TestIntegrationScenarioByID(t *testing.T) {
	id := os.Getenv("SCENARIO_ID")
	if id == "" {
		t.Skip("set SCENARIO_ID to run a single scenario")
	}
	if os.Getenv("RUN_INTEGRATION") != "1" {
		t.Skip("set RUN_INTEGRATION=1 to run live Kafka scenarios")
	}
	brokers := SplitBrokers(envOr("KAFKA_BROKERS", "localhost:9092"))
	if !KafkaReachable(brokers) {
		t.Skip("Kafka not reachable")
	}
	root, err := FindRepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := LoadScenarios(resolvePath(root, DefaultScenariosFile))
	if err != nil {
		t.Fatal(err)
	}
	sc, err := catalog.FindScenario(id)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	if err := RunScenario(ctx, brokers, DefaultRequestTopic, DefaultReplyTopic, root, sc); err != nil {
		t.Fatal(err)
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
