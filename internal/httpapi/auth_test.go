package httpapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestIngressProxyAllowed(t *testing.T) {
	allowed := []string{
		"/metrics",
		"/v1/cache",
		"/v1/cache/feature/foo",
	}
	for _, path := range allowed {
		if !ingressProxyAllowed(path) {
			t.Fatalf("expected allow for %q", path)
		}
	}

	denied := []string{
		"/",
		"/internal/v1/commands",
		"/healthz",
		"/readyz",
		"/v1/admin",
	}
	for _, path := range denied {
		if ingressProxyAllowed(path) {
			t.Fatalf("expected deny for %q", path)
		}
	}
}

func TestAuthorizeBearer(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/internal/v1/commands", nil)
	req.Header.Set("Authorization", "Bearer secret")

	rec := httptest.NewRecorder()
	if !authorizeBearer(rec, req, "secret") {
		t.Fatal("expected authorized request")
	}

	rec = httptest.NewRecorder()
	if authorizeBearer(rec, req, "other") {
		t.Fatal("expected unauthorized request")
	}

	rec = httptest.NewRecorder()
	if authorizeBearer(rec, req, "") {
		t.Fatal("expected disabled internal API")
	}
}

func TestProxyRejectsInternalPath(t *testing.T) {
	proxy, err := NewProxyServer(0, "http://127.0.0.1:1", nil)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/internal/v1/commands", nil)
	rec := httptest.NewRecorder()
	proxy.srv.Handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}
