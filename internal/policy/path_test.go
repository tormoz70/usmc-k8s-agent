package policy

import "testing"

func TestParseAPIPathDeployment(t *testing.T) {
	req, err := ParseAPIPath("GET", "/apis/apps/v1/namespaces/payments/deployments/api")
	if err != nil {
		t.Fatal(err)
	}
	if req.Group != "apps" || req.Version != "v1" || req.Kind != "Deployment" {
		t.Fatalf("unexpected gvk: %+v", req)
	}
	if req.Namespace != "payments" || req.Name != "api" {
		t.Fatalf("unexpected ns/name: %+v", req)
	}
	if req.Verb != "get" {
		t.Fatalf("verb=%q", req.Verb)
	}
}

func TestParseAPIPathIstio(t *testing.T) {
	req, err := ParseAPIPath("PATCH", "/apis/networking.istio.io/v1/namespaces/payments/virtualservices/api")
	if err != nil {
		t.Fatal(err)
	}
	if req.Kind != "VirtualService" {
		t.Fatalf("kind=%q", req.Kind)
	}
}

func TestParseAPIPathRoleBindings(t *testing.T) {
	req, err := ParseAPIPath("GET", "/apis/rbac.authorization.k8s.io/v1/namespaces/default/rolebindings")
	if err != nil {
		t.Fatal(err)
	}
	if req.Group != "rbac.authorization.k8s.io" || req.Version != "v1" || req.Kind != "RoleBinding" {
		t.Fatalf("unexpected gvk: %+v", req)
	}
	if req.Namespace != "default" {
		t.Fatalf("namespace=%q", req.Namespace)
	}

	req, err = ParseAPIPath("GET", "/apis/rbac.authorization.k8s.io/v1/rolebindings")
	if err != nil {
		t.Fatal(err)
	}
	if req.Kind != "RoleBinding" || req.Namespace != "" {
		t.Fatalf("unexpected cluster list: %+v", req)
	}
}
