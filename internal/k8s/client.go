package k8s

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// Client wraps rest.Config and typed clients for apiserver calls.
type Client struct {
	restConfig *rest.Config
	http       *http.Client
}

// NewInCluster builds a client from pod ServiceAccount credentials.
func NewInCluster() (*Client, error) {
	return NewInClusterWithRateLimit(50, 100)
}

// NewInClusterWithRateLimit builds an in-cluster client with rate limits.
func NewInClusterWithRateLimit(qps float32, burst int) (*Client, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return NewFromRestConfigWithRateLimit(cfg, qps, burst)
}

// NewFromRestConfig builds a client from an existing rest config (tests, dev).
func NewFromRestConfig(cfg *rest.Config) (*Client, error) {
	return NewFromRestConfigWithRateLimit(cfg, 50, 100)
}

// NewFromRestConfigWithRateLimit builds a client with explicit QPS/burst limits.
func NewFromRestConfigWithRateLimit(cfg *rest.Config, qps float32, burst int) (*Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("rest config is nil")
	}
	if qps <= 0 {
		qps = 50
	}
	if burst <= 0 {
		burst = 100
	}
	cfg = rest.CopyConfig(cfg)
	cfg.QPS = qps
	cfg.Burst = burst
	transport, err := rest.TransportFor(cfg)
	if err != nil {
		return nil, err
	}
	return &Client{
		restConfig: cfg,
		http: &http.Client{
			Transport: transport,
			Timeout:   0,
		},
	}, nil
}

// Kubernetes returns a typed clientset (for leader election, later phases).
func (c *Client) Kubernetes() (*kubernetes.Clientset, error) {
	return kubernetes.NewForConfig(c.restConfig)
}

// RestConfig returns a copy of the underlying config.
func (c *Client) RestConfig() *rest.Config {
	return rest.CopyConfig(c.restConfig)
}

// ProxyRequest executes an HTTP request against kube-apiserver.
func (c *Client) ProxyRequest(ctx context.Context, method, path, query string, headers map[string]string, body []byte) (int, []byte, error) {
	method = strings.ToUpper(method)
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	url := c.restConfig.Host + path
	if query != "" {
		url += "?" + strings.TrimPrefix(query, "?")
	}

	var bodyReader io.Reader
	if len(body) > 0 {
		bodyReader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return 0, nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json")
	}
	if bodyReader != nil && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if err != nil {
		return resp.StatusCode, nil, err
	}
	return resp.StatusCode, respBody, nil
}

// Ping checks apiserver connectivity with a short timeout.
func (c *Client) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	code, _, err := c.ProxyRequest(ctx, http.MethodGet, "/readyz", "", nil, nil)
	if err != nil {
		return err
	}
	if code >= 500 {
		return fmt.Errorf("apiserver /readyz returned %d", code)
	}
	return nil
}
