package mockcorelib

import (
	"testing"
)

func TestIsTopicAlreadyExists(t *testing.T) {
	if isTopicAlreadyExists(nil) {
		t.Fatal("nil is not exists")
	}
	if !isTopicAlreadyExists(errString("TOPIC_ALREADY_EXISTS")) {
		t.Fatal("expected topic exists")
	}
}

func TestIsUnknownTopicOrPartition(t *testing.T) {
	if !isUnknownTopicOrPartition(errString("Unknown Topic Or Partition")) {
		t.Fatal("expected unknown topic")
	}
}

type errString string

func (e errString) Error() string { return string(e) }
