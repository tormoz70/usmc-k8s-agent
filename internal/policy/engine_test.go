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
}

func TestEngineWildcardNamespaceRejected(t *testing.T) {
	_, err := NewEngine(Config{
		AllowedNamespaces: []string{"*"},
	})
	if err == nil {
		t.Fatal("expected wildcard rejection")
	}
}
