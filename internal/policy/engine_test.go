package policy

import (
	"testing"
)

func TestEngineAllowHTTPDeployment(t *testing.T) {
	engine, err := NewEngine(Config{
		DenySecrets: true,
		AllowedGVK: []GVK{
			{Group: "apps", Version: "v1", Kind: "Deployment"},
		},
		AllowedNamespaces:   []string{"payments"},
		AllowedCommandTypes: []string{"k8s.api"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.AllowHTTP("GET", "/apis/apps/v1/namespaces/payments/deployments/api"); err != nil {
		t.Fatalf("expected allow, got %v", err)
	}
}

func TestEngineDenySecret(t *testing.T) {
	engine, err := NewEngine(Config{
		DenySecrets:         true,
		AllowedCommandTypes: []string{"k8s.api"},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = engine.AllowHTTP("GET", "/api/v1/namespaces/payments/secrets/db")
	if err == nil {
		t.Fatal("expected secret denial")
	}
}

func TestEngineDenyNamespace(t *testing.T) {
	engine, err := NewEngine(Config{
		AllowedNamespaces:   []string{"app"},
		AllowedCommandTypes: []string{"k8s.api"},
		AllowedGVK:          []GVK{{Group: "", Version: "v1", Kind: "Pod"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	err = engine.AllowHTTP("GET", "/api/v1/namespaces/payments/pods")
	if err == nil {
		t.Fatal("expected namespace denial")
	}
	if err := engine.AllowNamespace("payments"); err == nil {
		t.Fatal("expected AllowNamespace denial")
	}
}

func TestEngineAllowNamespaceList(t *testing.T) {
	engine, err := NewEngine(Config{
		AllowedCommandTypes: []string{"k8s.api"},
		AllowedGVK: []GVK{
			{Group: "", Version: "v1", Kind: "Namespace"},
		},
		AllowedNamespaces: []string{"app"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.AllowHTTP("GET", "/api/v1/namespaces"); err != nil {
		t.Fatalf("expected namespace list allow, got %v", err)
	}
}

func TestEngineDenyUndeterminedKind(t *testing.T) {
	engine, err := NewEngine(Config{
		AllowedCommandTypes: []string{"k8s.api"},
		AllowedGVK: []GVK{
			{Group: "", Version: "v1", Kind: "Pod"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.AllowHTTP("GET", "/api/v1"); err == nil {
		t.Fatal("expected denial when kind cannot be determined")
	}
}

func TestEngineAllowIssuer(t *testing.T) {
	engine, err := NewEngine(Config{
		AllowedIssuers: []string{"core-client", "mock-core"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.AllowIssuer("mock-core"); err != nil {
		t.Fatal(err)
	}
	if err := engine.AllowIssuer("attacker"); err == nil {
		t.Fatal("expected issuer denial")
	}
}

func TestEngineAllowReplyTopic(t *testing.T) {
	engine, err := NewEngine(Config{
		AllowedReplyTopics:        []string{"core-client.dev.responses"},
		AllowedReplyTopicPrefixes: []string{"core-client."},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.AllowReplyTopic("core-client.dev.responses"); err != nil {
		t.Fatal(err)
	}
	if err := engine.AllowReplyTopic("core-client.prod.responses"); err != nil {
		t.Fatal(err)
	}
	if err := engine.AllowReplyTopic("evil.responses"); err == nil {
		t.Fatal("expected reply topic denial")
	}
}

func TestEngineAllowGVK(t *testing.T) {
	engine, err := NewEngine(Config{
		AllowedGVK: []GVK{{Group: "apps", Version: "v1", Kind: "Deployment"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.AllowGVK(GVK{Group: "apps", Version: "v1", Kind: "Deployment"}); err != nil {
		t.Fatal(err)
	}
	if err := engine.AllowGVK(GVK{Group: "", Version: "v1", Kind: "Secret"}); err == nil {
		t.Fatal("expected denial")
	}
	if err := engine.AllowGVK(GVK{Group: "", Version: "v1", Kind: ""}); err == nil {
		t.Fatal("expected denial for empty kind")
	}
}

func TestEngineWildcardNamespaceRejected(t *testing.T) {
	_, err := NewEngine(Config{
		AllowedNamespaces: []string{"*"},
	})
	if err == nil {
		t.Fatal("expected wildcard rejection")
	}
}
