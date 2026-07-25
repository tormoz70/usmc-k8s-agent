package nodelocal

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// NodeClient fetches logs from logs-node-agent DaemonSet pods.
type NodeClient struct {
	Kube      kubernetes.Interface
	Namespace string
	Selector  string
	Port      int
	Token     string
	HTTP      *http.Client
}

// FetchRequest mirrors nodeagent.FetchRequest for the leader → DS call.
type FetchRequest struct {
	Namespace  string     `json:"namespace"`
	Pod        string     `json:"pod"`
	PodUID     string     `json:"pod_uid,omitempty"`
	Container  string     `json:"container"`
	Previous   bool       `json:"previous"`
	SinceTime  *time.Time `json:"since_time,omitempty"`
	TailLines  *int64     `json:"tail_lines,omitempty"`
	LimitBytes *int64     `json:"limit_bytes,omitempty"`
}

func (c *NodeClient) httpClient() *http.Client {
	if c.HTTP != nil {
		return c.HTTP
	}
	return &http.Client{Timeout: 2 * time.Minute}
}

// ResolveAgentURL finds the logs-node-agent pod on the given node and returns its base URL.
func (c *NodeClient) ResolveAgentURL(ctx context.Context, nodeName string) (string, error) {
	if nodeName == "" {
		return "", fmt.Errorf("node name is empty")
	}
	list, err := c.Kube.CoreV1().Pods(c.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: c.Selector,
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		return "", err
	}
	for _, p := range list.Items {
		if p.Status.PodIP == "" {
			continue
		}
		phase := p.Status.Phase
		if phase != "" && phase != "Running" {
			continue
		}
		return fmt.Sprintf("http://%s:%d", p.Status.PodIP, c.Port), nil
	}
	return "", fmt.Errorf("no logs-node-agent pod on node %s", nodeName)
}

// Fetch opens a stream of plain log text from the node agent on nodeName.
func (c *NodeClient) Fetch(ctx context.Context, nodeName string, req FetchRequest) (io.ReadCloser, error) {
	base, err := c.ResolveAgentURL(ctx, nodeName)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/internal/v1/logs/fetch", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.Token)
	resp, err := c.httpClient().Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("node agent %s: status %d: %s", nodeName, resp.StatusCode, string(b))
	}
	return resp.Body, nil
}

// Stream opens a follow stream from the node agent.
func (c *NodeClient) Stream(ctx context.Context, nodeName string, req FetchRequest) (io.ReadCloser, error) {
	base, err := c.ResolveAgentURL(ctx, nodeName)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/internal/v1/logs/stream?follow=true", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.Token)
	// No overall timeout for follow streams.
	client := &http.Client{Timeout: 0}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer resp.Body.Close()
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return nil, fmt.Errorf("node agent stream %s: status %d: %s", nodeName, resp.StatusCode, string(b))
	}
	return resp.Body, nil
}
