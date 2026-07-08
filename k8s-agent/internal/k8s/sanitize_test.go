package k8s_test

import (
	"testing"

	"github.com/usmc/k8s-agent/internal/k8s"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestSanitizeRemovesManagedFields(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]interface{}{
			"name": "test",
			"managedFields": []interface{}{
				map[string]interface{}{"manager": "kubectl"},
			},
			"annotations": map[string]interface{}{
				"kubectl.kubernetes.io/last-applied-configuration": "{}",
				"keep": "yes",
			},
		},
	}}

	out := k8s.SanitizeObject(obj, k8s.SanitizeOptions{})
	meta := out.Object["metadata"].(map[string]interface{})
	if _, ok := meta["managedFields"]; ok {
		t.Fatal("managedFields should be removed")
	}
	ann := meta["annotations"].(map[string]interface{})
	if _, ok := ann["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		t.Fatal("last-applied-configuration should be removed")
	}
	if ann["keep"] != "yes" {
		t.Fatal("other annotations should remain")
	}
}

func TestSanitizeStripStatus(t *testing.T) {
	obj := &unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{"phase": "Running"},
	}}
	out := k8s.SanitizeObject(obj, k8s.SanitizeOptions{StripStatus: true})
	if _, ok := out.Object["status"]; ok {
		t.Fatal("status should be stripped")
	}
}
