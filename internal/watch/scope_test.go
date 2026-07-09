package watch

import "testing"

func TestClusterScopedKind(t *testing.T) {
	if !ClusterScopedKind("Namespace") {
		t.Fatal("Namespace should be cluster-scoped")
	}
	if ClusterScopedKind("Pod") {
		t.Fatal("Pod should not be cluster-scoped")
	}
}

func TestInformerNamespace(t *testing.T) {
	if got := InformerNamespace("Namespace", "ignored"); got != "" {
		t.Fatalf("got %q", got)
	}
	if got := InformerNamespace("Pod", "app"); got != "app" {
		t.Fatalf("got %q", got)
	}
}

func TestParseSubscribePayloadNamespaceWatch(t *testing.T) {
	raw := []byte(`{
		"subscription_id": "sub-ns",
		"gvk": {"group":"","version":"v1","kind":"Namespace"},
		"event_filter": ["ADDED"]
	}`)
	p, err := ParseSubscribePayload(raw)
	if err != nil {
		t.Fatal(err)
	}
	if p.Namespace != "" {
		t.Fatalf("namespace=%q", p.Namespace)
	}
}

func TestParseSubscribePayloadRequiresNamespaceForPod(t *testing.T) {
	raw := []byte(`{
		"subscription_id": "sub-1",
		"gvk": {"group":"","version":"v1","kind":"Pod"}
	}`)
	if _, err := ParseSubscribePayload(raw); err == nil {
		t.Fatal("expected namespace required for Pod watch")
	}
}
