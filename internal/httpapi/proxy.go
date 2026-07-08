package httpapi

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"
)

// ProxyServer forwards public HTTP traffic to agent-service leader.
type ProxyServer struct {
	log *slog.Logger
	srv *http.Server
}

func NewProxyServer(port int, upstreamURL string, log *slog.Logger) (*ProxyServer, error) {
	if log == nil {
		log = slog.Default()
	}
	target, err := url.Parse(upstreamURL)
	if err != nil {
		return nil, fmt.Errorf("AGENT_SERVICE_URL: %w", err)
	}

	proxy := httputil.NewSingleHostReverseProxy(target)
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Warn("ingress proxy error", "error", err, "path", r.URL.Path)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "upstream unavailable"})
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if !ingressProxyAllowed(r.URL.Path) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
			return
		}
		proxy.ServeHTTP(w, r)
	})

	return &ProxyServer{
		log: log,
		srv: &http.Server{
			Addr:              fmt.Sprintf(":%d", port),
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
		},
	}, nil
}

func (p *ProxyServer) Start() error {
	p.log.Info("ingress proxy listening", "addr", p.srv.Addr)
	go func() {
		if err := p.srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			p.log.Error("ingress proxy error", "error", err)
		}
	}()
	return nil
}

func (p *ProxyServer) Shutdown(ctx context.Context) error {
	return p.srv.Shutdown(ctx)
}
