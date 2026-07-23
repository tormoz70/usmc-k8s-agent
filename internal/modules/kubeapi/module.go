package kubeapi

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strings"
	"time"

	"k8s.io/client-go/rest"

	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/features"
	"github.com/usmc/usmc-k8s-agent/internal/modules"
)

// Module exposes a REST reverse-proxy to the Kubernetes API server (Java kubeapi analogue).
type Module struct {
	cfg        *config.Config
	restConfig *rest.Config
	log        *slog.Logger
	server     *http.Server
	listenAddr string
}

var _ modules.Module = (*Module)(nil)

func New(cfg *config.Config, restConfig *rest.Config, listenAddr string, log *slog.Logger) *Module {
	if log == nil {
		log = slog.Default()
	}
	if listenAddr == "" {
		listenAddr = ":8082"
	}
	return &Module{cfg: cfg, restConfig: restConfig, log: log, listenAddr: listenAddr}
}

func (m *Module) Name() string { return "kubeapi" }

func (m *Module) Enabled(cfg *config.Config, feat *features.Registry) bool {
	if cfg == nil {
		return false
	}
	if strings.EqualFold(os.Getenv("KUBEAPI_REST_ENABLED"), "true") {
		return true
	}
	if feat != nil && (feat.Enabled("cluster_inventory") || feat.CommandEnabled("k8s.api")) {
		return cfg.Kafka.Mode == config.KafkaModeProtobuf || cfg.Kafka.Mode == config.KafkaModeDual
	}
	return false
}

func (m *Module) Start(ctx context.Context) error {
	if m.restConfig == nil {
		m.log.Warn("kubeapi skipped: no rest config")
		return nil
	}
	host := m.restConfig.Host
	target, err := url.Parse(host)
	if err != nil {
		return err
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	defaultDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		defaultDirector(req)
		req.Host = target.Host
		if m.restConfig.BearerToken != "" {
			req.Header.Set("Authorization", "Bearer "+m.restConfig.BearerToken)
		}
	}
	proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		m.log.Warn("kubeapi proxy error", "error", err, "path", r.URL.Path)
		http.Error(w, "kubeapi proxy error", http.StatusBadGateway)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok")
	})
	mux.Handle("/", proxy)

	m.server = &http.Server{
		Addr:              m.listenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		m.log.Info("kubeapi REST proxy listening", "addr", m.listenAddr)
		if err := m.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			m.log.Error("kubeapi server failed", "error", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = m.server.Shutdown(shutdownCtx)
	}()
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	if m.server == nil {
		return nil
	}
	return m.server.Shutdown(ctx)
}
