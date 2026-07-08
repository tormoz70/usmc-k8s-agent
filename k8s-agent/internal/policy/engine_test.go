package policy_test

import (
	"testing"
	"time"

	"github.com/usmc/k8s-agent/internal/command"
	"github.com/usmc/k8s-agent/internal/policy"
)

func TestPolicyAllowsResourceList(t *testing.T) {
	engine := policy.NewEngine(policy.DefaultPolicy())
	cmd := command.Command{
		CommandID:      "c1",
		IdempotencyKey: "k1",
		Type:           command.TypeResourceList,
		IssuedBy:       "core-prod",
		TS:             time.Now().UTC(),
		Target: command.Target{
			Group:     "networking.istio.io",
			Version:   "v1beta1",
			Kind:      "VirtualService",
			Namespace: "app",
		},
	}
	if err := engine.CheckCommand(cmd); err != nil {
		t.Fatalf("expected allowed: %v", err)
	}
}

func TestPolicyRejectsSecret(t *testing.T) {
	engine := policy.NewEngine(policy.DefaultPolicy())
	cmd := command.Command{
		CommandID:      "c1",
		IdempotencyKey: "k1",
		Type:           command.TypeResourceList,
		IssuedBy:       "core-prod",
		TS:             time.Now().UTC(),
		Target: command.Target{
			Kind:      "Secret",
			Namespace: "app",
		},
	}
	if err := engine.CheckCommand(cmd); err == nil {
		t.Fatal("expected Secret rejection")
	}
}

func TestPolicyRejectsUnknownIssuer(t *testing.T) {
	engine := policy.NewEngine(policy.DefaultPolicy())
	cmd := command.Command{
		CommandID:      "c1",
		IdempotencyKey: "k1",
		Type:           command.TypeResourceList,
		IssuedBy:       "unknown",
		TS:             time.Now().UTC(),
		Target: command.Target{
			Group:     "networking.istio.io",
			Version:   "v1beta1",
			Kind:      "VirtualService",
			Namespace: "app",
		},
	}
	if err := engine.CheckCommand(cmd); err == nil {
		t.Fatal("expected issuer rejection")
	}
}
