package watch

import (
	"encoding/json"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func TestDetectRestartsHandlesNumericTypes(t *testing.T) {
	oldPod := &unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{
			"containerStatuses": []interface{}{
				map[string]interface{}{"name": "app", "restartCount": int64(1)},
				map[string]interface{}{"name": "sidecar", "restartCount": float64(2)},
			},
		},
	}}
	newPod := &unstructured.Unstructured{Object: map[string]interface{}{
		"status": map[string]interface{}{
			"containerStatuses": []interface{}{
				map[string]interface{}{"name": "app", "restartCount": float64(2)},
				map[string]interface{}{"name": "sidecar", "restartCount": json.Number("3")},
			},
		},
	}}

	got := detectRestarts(oldPod, newPod)
	if got["app"] != 2 {
		t.Fatalf("app restart_count=%v want 2", got["app"])
	}
	if got["sidecar"] != 3 {
		t.Fatalf("sidecar restart_count=%v want 3", got["sidecar"])
	}
}
