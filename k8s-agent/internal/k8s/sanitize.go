package k8s

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

const lastAppliedAnnotation = "kubectl.kubernetes.io/last-applied-configuration"

type SanitizeOptions struct {
	StripStatus bool
}

func SanitizeObject(obj *unstructured.Unstructured, opts SanitizeOptions) *unstructured.Unstructured {
	if obj == nil {
		return nil
	}
	copy := obj.DeepCopy()
	metadata, ok, _ := unstructured.NestedMap(copy.Object, "metadata")
	if ok {
		delete(metadata, "managedFields")
		if ann, ok := metadata["annotations"].(map[string]interface{}); ok {
			delete(ann, lastAppliedAnnotation)
			if len(ann) == 0 {
				delete(metadata, "annotations")
			} else {
				metadata["annotations"] = ann
			}
		}
		_ = unstructured.SetNestedMap(copy.Object, metadata, "metadata")
	}
	if opts.StripStatus {
		unstructured.RemoveNestedField(copy.Object, "status")
	}
	return copy
}

func ToYAML(obj *unstructured.Unstructured) ([]byte, error) {
	sanitized := SanitizeObject(obj, SanitizeOptions{})
	return json.Marshal(sanitized.Object)
}

func ToJSON(obj *unstructured.Unstructured) ([]byte, error) {
	sanitized := SanitizeObject(obj, SanitizeOptions{})
	return json.Marshal(sanitized.Object)
}
