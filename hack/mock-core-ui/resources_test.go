package main

import (
	"strings"
	"testing"
)

func TestParsePromGauges(t *testing.T) {
	raw := `# HELP go_goroutines
# TYPE go_goroutines gauge
go_goroutines 42
process_resident_memory_bytes 1048576
k8s_agent_logs_collect_jobs_active 3
k8s_agent_watch_subscriptions_active{foo="bar"} 7
`
	got := parsePromGauges(strings.NewReader(raw), []string{
		"go_goroutines",
		"process_resident_memory_bytes",
		"k8s_agent_logs_collect_jobs_active",
		"k8s_agent_watch_subscriptions_active",
	})
	if got["go_goroutines"] != 42 {
		t.Fatalf("goroutines=%v", got["go_goroutines"])
	}
	if got["process_resident_memory_bytes"] != 1048576 {
		t.Fatalf("rss=%v", got["process_resident_memory_bytes"])
	}
	if got["k8s_agent_logs_collect_jobs_active"] != 3 {
		t.Fatalf("jobs=%v", got["k8s_agent_logs_collect_jobs_active"])
	}
	if got["k8s_agent_watch_subscriptions_active"] != 7 {
		t.Fatalf("watch=%v", got["k8s_agent_watch_subscriptions_active"])
	}
}

func TestProfileByID(t *testing.T) {
	for _, id := range []string{"ha", "balanced", "lean"} {
		p, err := profileByID(id)
		if err != nil || p.ID != id {
			t.Fatalf("%s: %v %+v", id, err, p)
		}
	}
	if _, err := profileByID("nope"); err == nil {
		t.Fatal("expected error")
	}
}
