package k8s

import (
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TrimOptions controls response shaping before publishing to Kafka.
type TrimOptions struct {
	StripStatus bool
}

// TrimResponse removes heavy fields from Kubernetes JSON responses.
func TrimResponse(body []byte, opts TrimOptions) ([]byte, error) {
	if len(body) == 0 {
		return body, nil
	}

	var obj map[string]interface{}
	if err := json.Unmarshal(body, &obj); err != nil {
		return body, nil
	}

	trimObjectMap(obj, opts)

	out, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("marshal trimmed body: %w", err)
	}
	return out, nil
}

func trimObjectMap(obj map[string]interface{}, opts TrimOptions) {
	if md, ok := obj["metadata"].(map[string]interface{}); ok {
		delete(md, "managedFields")
		if ann, ok := md["annotations"].(map[string]interface{}); ok {
			delete(ann, "kubectl.kubernetes.io/last-applied-configuration")
			if len(ann) == 0 {
				delete(md, "annotations")
			}
		}
	}
	if opts.StripStatus {
		delete(obj, "status")
	}

	if items, ok := obj["items"].([]interface{}); ok {
		for _, item := range items {
			if m, ok := item.(map[string]interface{}); ok {
				trimObjectMap(m, opts)
			}
		}
	}
}

// DefaultTrimOptions matches MVP plan defaults.
func DefaultTrimOptions() TrimOptions {
	return TrimOptions{StripStatus: false}
}

// ExtractResourceVersion reads metadata.resourceVersion from a JSON object.
func ExtractResourceVersion(body []byte) string {
	var obj struct {
		Metadata metav1.ObjectMeta `json:"metadata"`
	}
	if err := json.Unmarshal(body, &obj); err != nil {
		return ""
	}
	return obj.Metadata.ResourceVersion
}
