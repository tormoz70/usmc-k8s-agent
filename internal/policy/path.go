package policy

import (
	"fmt"
	"net/http"
	"strings"
)

// APIRequest describes a parsed kube-apiserver HTTP call.
type APIRequest struct {
	Method    string
	Path      string
	Verb      string
	Group     string
	Version   string
	Kind      string
	Namespace string
	Name      string
	IsSecret  bool
}

// ParseAPIPath extracts verb, GVK, namespace and name from an apiserver path.
func ParseAPIPath(method, path string) (*APIRequest, error) {
	method = strings.ToUpper(strings.TrimSpace(method))
	path = strings.TrimSpace(path)
	if method == "" || path == "" {
		return nil, fmt.Errorf("method and path are required")
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	req := &APIRequest{Method: method, Path: path, Verb: httpMethodVerb(method)}

	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) < 2 {
		return nil, fmt.Errorf("unsupported path %q", path)
	}

	switch parts[0] {
	case "api":
		req.Group = ""
		req.Version = parts[1]
		return parseCoreResources(req, parts[2:])
	case "apis":
		if len(parts) < 3 {
			return nil, fmt.Errorf("unsupported apis path %q", path)
		}
		req.Group = parts[1]
		req.Version = parts[2]
		return parseCoreResources(req, parts[3:])
	default:
		return nil, fmt.Errorf("unsupported path prefix %q", parts[0])
	}
}

func parseCoreResources(req *APIRequest, parts []string) (*APIRequest, error) {
	if len(parts) == 0 {
		return req, nil
	}

	if parts[0] == "namespaces" {
		if len(parts) < 2 {
			return req, nil
		}
		req.Namespace = parts[1]
		if len(parts) >= 3 {
			subResource := parts[2]
			req.Kind = resourceToKind(subResource)
			if subResource == "secrets" {
				req.IsSecret = true
			}
			if len(parts) >= 4 {
				req.Name = parts[3]
			}
		}
		return req, nil
	}

	resource := parts[0]
	req.Kind = resourceToKind(resource)
	if resource == "secrets" || strings.HasSuffix(resource, "/secrets") {
		req.IsSecret = true
	}

	if len(parts) >= 2 && parts[1] == "namespaces" {
		if len(parts) < 3 {
			return req, nil
		}
		req.Namespace = parts[2]
		if len(parts) >= 4 {
			subResource := parts[3]
			req.Kind = resourceToKind(subResource)
			if subResource == "secrets" {
				req.IsSecret = true
			}
			if len(parts) >= 5 {
				req.Name = parts[4]
			}
		}
		return req, nil
	}

	if len(parts) >= 2 {
		req.Name = parts[1]
	}
	return req, nil
}

func httpMethodVerb(method string) string {
	switch method {
	case http.MethodGet, http.MethodHead:
		return "get"
	case http.MethodPost:
		return "create"
	case http.MethodPut:
		return "update"
	case http.MethodPatch:
		return "patch"
	case http.MethodDelete:
		return "delete"
	default:
		return strings.ToLower(method)
	}
}

func resourceToKind(resource string) string {
	switch resource {
	case "pods", "pod":
		return "Pod"
	case "services", "service":
		return "Service"
	case "configmaps", "configmap":
		return "ConfigMap"
	case "secrets", "secret":
		return "Secret"
	case "events", "event":
		return "Event"
	case "deployments", "deployment":
		return "Deployment"
	case "deploymentconfigs", "deploymentconfig":
		return "DeploymentConfig"
	case "virtualservices", "virtualservice":
		return "VirtualService"
	case "destinationrules", "destinationrule":
		return "DestinationRule"
	case "gateways", "gateway":
		return "Gateway"
	case "authorizationpolicies", "authorizationpolicy":
		return "AuthorizationPolicy"
	default:
		if resource == "" {
			return ""
		}
		r := strings.TrimSuffix(resource, "s")
		return strings.ToUpper(r[:1]) + r[1:]
	}
}
