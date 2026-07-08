package httpapi

import (
	"net/http"
	"strings"
)

func authorizeBearer(w http.ResponseWriter, r *http.Request, expectedToken string) bool {
	if expectedToken == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "internal API is disabled"})
		return false
	}
	auth := r.Header.Get("Authorization")
	if !strings.HasPrefix(auth, "Bearer ") || strings.TrimPrefix(auth, "Bearer ") != expectedToken {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return false
	}
	return true
}

// ingressProxyAllowed reports whether ingress may forward the path to agent-service.
func ingressProxyAllowed(path string) bool {
	if strings.HasPrefix(path, "/internal/") {
		return false
	}
	switch {
	case path == "/metrics":
		return true
	case path == "/v1/cache":
		return true
	case strings.HasPrefix(path, "/v1/cache/"):
		return true
	default:
		return false
	}
}
