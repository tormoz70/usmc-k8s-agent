package k8s

import (
	"strings"
	"testing"
)

func TestTrimResponseRemovesManagedFields(t *testing.T) {
	in := []byte(`{
		"metadata": {
			"name": "api",
			"resourceVersion": "123",
			"managedFields": [{"manager":"kubectl"}],
			"annotations": {
				"kubectl.kubernetes.io/last-applied-configuration": "{}",
				"keep": "yes"
			}
		},
		"status": {"readyReplicas": 1}
	}`)
	out, err := TrimResponse(in, DefaultTrimOptions())
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	if strings.Contains(s, "managedFields") {
		t.Fatalf("managedFields not removed: %s", s)
	}
	if strings.Contains(s, "last-applied-configuration") {
		t.Fatalf("last-applied annotation not removed: %s", s)
	}
	if !strings.Contains(s, "keep") {
		t.Fatalf("expected keep annotation: %s", s)
	}
}
