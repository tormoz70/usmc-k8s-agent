package watch

import "strings"

// ClusterScopedKind reports GVK kinds watched at cluster scope (no namespace in payload).
func ClusterScopedKind(kind string) bool {
	switch strings.TrimSpace(kind) {
	case "Namespace", "Node", "PersistentVolume", "StorageClass", "ClusterRole", "ClusterRoleBinding":
		return true
	default:
		return false
	}
}

// InformerNamespace returns the namespace argument for dynamic informer factory.
func InformerNamespace(kind, payloadNamespace string) string {
	if ClusterScopedKind(kind) {
		return ""
	}
	return payloadNamespace
}
