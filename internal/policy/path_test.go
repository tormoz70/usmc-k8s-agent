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

func TestParseAPIPathDeploymentConfig(t *testing.T) {
	req, err := ParseAPIPath("GET", "/apis/apps.openshift.io/v1/namespaces/app/deploymentconfigs/processor")
	if err != nil {
		t.Fatal(err)
	}
	if req.Group != "apps.openshift.io" || req.Kind != "DeploymentConfig" {
		t.Fatalf("unexpected: %+v", req)
	}
}
