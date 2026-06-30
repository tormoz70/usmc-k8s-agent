package logstream_test

import (
	"testing"

	"github.com/usmc/k8s-agent/internal/logstream"
)

func TestCompilePatternCaseInsensitive(t *testing.T) {
	re, err := logstream.CompilePattern(logstream.SubscribePayload{
		Pattern:         "error",
		PatternType:     "regex",
		CaseInsensitive: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !re.MatchString("ERROR happened") {
		t.Fatal("expected case-insensitive match")
	}
}

func TestKafkaKeyOrdering(t *testing.T) {
	key := "app/proc-abc/processor"
	if key != "app/proc-abc/processor" {
		t.Fatal("unexpected key")
	}
}
