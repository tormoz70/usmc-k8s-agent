package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

type resourceProfile struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description"`
	Replicas    int32             `json:"replicas"`
	Env         map[string]string `json:"env"`
	FeaturesMode string           `json:"features_mode,omitempty"` // full | minimal | ""
	AgentMemLimit string          `json:"agent_mem_limit,omitempty"`
	EmptyDirLimit string          `json:"empty_dir_limit,omitempty"`
}

type podResourceRow struct {
	Name           string `json:"name"`
	Component      string `json:"component"`
	Phase          string `json:"phase"`
	Node           string `json:"node,omitempty"`
	CPUUsageMilli  int64  `json:"cpu_usage_milli,omitempty"`
	MemUsageBytes  int64  `json:"mem_usage_bytes,omitempty"`
	CPURequestMilli int64 `json:"cpu_request_milli"`
	MemRequestBytes int64 `json:"mem_request_bytes"`
	CPULimitMilli   int64 `json:"cpu_limit_milli,omitempty"`
	MemLimitBytes   int64 `json:"mem_limit_bytes,omitempty"`
	Kind           string `json:"kind"` // Pod | DaemonSet-pod
}

type resourceTotals struct {
	CPUUsageMilli   int64 `json:"cpu_usage_milli"`
	MemUsageBytes   int64 `json:"mem_usage_bytes"`
	CPURequestMilli int64 `json:"cpu_request_milli"`
	MemRequestBytes int64 `json:"mem_request_bytes"`
	PodCount        int   `json:"pod_count"`
}

type agentRuntimeMetrics struct {
	ProcessRSSBytes       float64 `json:"process_rss_bytes,omitempty"`
	GoAllocBytes          float64 `json:"go_alloc_bytes,omitempty"`
	GoGoroutines          float64 `json:"go_goroutines,omitempty"`
	LogsCollectJobsActive float64 `json:"logs_collect_jobs_active,omitempty"`
	WatchSubscriptions    float64 `json:"watch_subscriptions_active,omitempty"`
	LogStreamsActive      float64 `json:"log_streams_active,omitempty"`
	HealthReportsActive   float64 `json:"health_reports_active,omitempty"`
	CacheEntries          float64 `json:"cache_entries,omitempty"`
	RawError              string  `json:"error,omitempty"`
}

type resourcesSnapshot struct {
	Namespace      string               `json:"namespace"`
	TargetID       string               `json:"target_id,omitempty"`
	ObservedAt     time.Time            `json:"observed_at"`
	K8sAvailable   bool                 `json:"k8s_available"`
	K8sError       string               `json:"k8s_error,omitempty"`
	MetricsError   string               `json:"metrics_error,omitempty"` // metrics-server
	Pods           []podResourceRow     `json:"pods"`
	Totals         resourceTotals       `json:"totals"`
	AgentMetrics   agentRuntimeMetrics  `json:"agent_metrics"`
	AgentHTTPURL   string               `json:"agent_http_url,omitempty"`
}

type applyResourceProfileRequest struct {
	Profile string `json:"profile"`
}

type applyResourceProfileResponse struct {
	Profile   string   `json:"profile"`
	Namespace string   `json:"namespace"`
	Patched   []string `json:"patched"`
	Restarted []string `json:"restarted"`
	Features  string   `json:"features_mode,omitempty"`
	Hints     []string `json:"hints,omitempty"`
}

func resourceProfiles() []resourceProfile {
	return []resourceProfile{
		{
			ID:          "ha",
			Name:        "HA",
			Description: "PROD HA: replicas=2, QPS 50/100, full logs pool, emptyDir 2Gi",
			Replicas:    2,
			Env: map[string]string{
				"K8S_API_QPS":            "50",
				"K8S_API_BURST":          "100",
				"LOGS_COLLECT_MAX_JOBS":  "20",
				"LOGS_COLLECT_MAX_BYTES": "524288000",
			},
			FeaturesMode:  "full",
			AgentMemLimit: "512Mi",
			EmptyDirLimit: "2Gi",
		},
		{
			ID:          "balanced",
			Name:        "Balanced",
			Description: "replicas=1, QPS 30/60, logs jobs=5 / 100Mi, agent mem 768Mi",
			Replicas:    1,
			Env: map[string]string{
				"K8S_API_QPS":            "30",
				"K8S_API_BURST":          "60",
				"LOGS_COLLECT_MAX_JOBS":  "5",
				"LOGS_COLLECT_MAX_BYTES": "104857600",
			},
			FeaturesMode:  "full",
			AgentMemLimit: "768Mi",
			EmptyDirLimit: "2Gi",
		},
		{
			ID:          "lean",
			Name:        "Lean",
			Description: "replicas=1, features-minimal, QPS 20/40, logs jobs=2 / 50Mi, emptyDir 512Mi",
			Replicas:    1,
			Env: map[string]string{
				"K8S_API_QPS":            "20",
				"K8S_API_BURST":          "40",
				"LOGS_COLLECT_MAX_JOBS":  "2",
				"LOGS_COLLECT_MAX_BYTES": "52428800",
			},
			FeaturesMode:  "minimal",
			AgentMemLimit: "512Mi",
			EmptyDirLimit: "512Mi",
		},
	}
}

func profileByID(id string) (*resourceProfile, error) {
	for i := range resourceProfiles() {
		p := resourceProfiles()[i]
		if p.ID == id {
			return &p, nil
		}
	}
	return nil, fmt.Errorf("unknown profile %q (want ha|balanced|lean)", id)
}

type resourcesController struct {
	cfg   config
	modes *agentModeController
}

func newResourcesController(cfg config, modes *agentModeController) *resourcesController {
	return &resourcesController{cfg: cfg, modes: modes}
}

func (c *resourcesController) snapshot(ctx context.Context, namespace, targetID, agentHTTP string) *resourcesSnapshot {
	out := &resourcesSnapshot{
		Namespace:    namespace,
		TargetID:     targetID,
		ObservedAt:   time.Now().UTC(),
		AgentHTTPURL: agentHTTP,
		Pods:         []podResourceRow{},
	}
	if c.modes == nil || c.modes.k8s == nil {
		out.K8sAvailable = false
		out.K8sError = "kubernetes client not available (set KUBECONFIG)"
		out.AgentMetrics = c.fetchAgentMetrics(ctx, namespace, agentHTTP)
		return out
	}
	out.K8sAvailable = true

	pods, err := c.modes.k8s.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: "app.kubernetes.io/part-of=uamc-agent",
	})
	if err != nil {
		out.K8sError = err.Error()
		out.AgentMetrics = c.fetchAgentMetrics(ctx, namespace, agentHTTP)
		return out
	}

	usageByPod, metricsErr := c.podMetrics(ctx, namespace)
	if metricsErr != nil {
		out.MetricsError = metricsErr.Error()
	}

	for _, p := range pods.Items {
		row := podResourceRow{
			Name:      p.Name,
			Component: p.Labels["app.kubernetes.io/component"],
			Phase:     string(p.Status.Phase),
			Node:      p.Spec.NodeName,
			Kind:      "Pod",
		}
		if row.Component == "logs-node-agent" {
			row.Kind = "DaemonSet-pod"
		}
		reqCPU, reqMem, limCPU, limMem := sumPodResources(&p)
		row.CPURequestMilli = reqCPU
		row.MemRequestBytes = reqMem
		row.CPULimitMilli = limCPU
		row.MemLimitBytes = limMem
		if u, ok := usageByPod[p.Name]; ok {
			row.CPUUsageMilli = u.cpuMilli
			row.MemUsageBytes = u.memBytes
		}
		out.Pods = append(out.Pods, row)
		out.Totals.PodCount++
		out.Totals.CPURequestMilli += row.CPURequestMilli
		out.Totals.MemRequestBytes += row.MemRequestBytes
		out.Totals.CPUUsageMilli += row.CPUUsageMilli
		out.Totals.MemUsageBytes += row.MemUsageBytes
	}

	out.AgentMetrics = c.fetchAgentMetrics(ctx, namespace, agentHTTP)
	return out
}

type podUsage struct {
	cpuMilli int64
	memBytes int64
}

func (c *resourcesController) podMetrics(ctx context.Context, namespace string) (map[string]podUsage, error) {
	if c.modes.restConfig == nil {
		return nil, fmt.Errorf("no rest config")
	}
	dyn, err := dynamic.NewForConfig(c.modes.restConfig)
	if err != nil {
		return nil, err
	}
	gvr := schema.GroupVersionResource{Group: "metrics.k8s.io", Version: "v1beta1", Resource: "pods"}
	list, err := dyn.Resource(gvr).Namespace(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("metrics.k8s.io unavailable (install metrics-server): %w", err)
	}
	out := map[string]podUsage{}
	for _, item := range list.Items {
		name := item.GetName()
		containers, found, _ := unstructured.NestedSlice(item.Object, "containers")
		if !found {
			continue
		}
		var cpuMilli, memBytes int64
		for _, raw := range containers {
			cm, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			usage, _, _ := unstructured.NestedMap(cm, "usage")
			if usage == nil {
				continue
			}
			if s, _ := usage["cpu"].(string); s != "" {
				if q, err := resource.ParseQuantity(s); err == nil {
					cpuMilli += q.MilliValue()
				}
			}
			if s, _ := usage["memory"].(string); s != "" {
				if q, err := resource.ParseQuantity(s); err == nil {
					memBytes += q.Value()
				}
			}
		}
		out[name] = podUsage{cpuMilli: cpuMilli, memBytes: memBytes}
	}
	return out, nil
}

func sumPodResources(p *corev1.Pod) (reqCPU, reqMem, limCPU, limMem int64) {
	for _, c := range p.Spec.Containers {
		if q, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
			reqCPU += q.MilliValue()
		}
		if q, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
			reqMem += q.Value()
		}
		if q, ok := c.Resources.Limits[corev1.ResourceCPU]; ok {
			limCPU += q.MilliValue()
		}
		if q, ok := c.Resources.Limits[corev1.ResourceMemory]; ok {
			limMem += q.Value()
		}
	}
	return
}

func (c *resourcesController) fetchAgentMetrics(ctx context.Context, namespace, agentHTTP string) agentRuntimeMetrics {
	out := agentRuntimeMetrics{}
	body, _, err := c.readAgentMetrics(ctx, namespace, agentHTTP)
	if err != nil {
		out.RawError = err.Error()
		return out
	}
	vals := parsePromGauges(strings.NewReader(string(body)), []string{
		"process_resident_memory_bytes",
		"go_memstats_alloc_bytes",
		"go_goroutines",
		"k8s_agent_logs_collect_jobs_active",
		"k8s_agent_watch_subscriptions_active",
		"k8s_agent_log_streams_active",
		"k8s_agent_health_reports_active",
		"k8s_agent_cache_entries",
	})
	out.ProcessRSSBytes = vals["process_resident_memory_bytes"]
	out.GoAllocBytes = vals["go_memstats_alloc_bytes"]
	out.GoGoroutines = vals["go_goroutines"]
	out.LogsCollectJobsActive = vals["k8s_agent_logs_collect_jobs_active"]
	out.WatchSubscriptions = vals["k8s_agent_watch_subscriptions_active"]
	out.LogStreamsActive = vals["k8s_agent_log_streams_active"]
	out.HealthReportsActive = vals["k8s_agent_health_reports_active"]
	out.CacheEntries = vals["k8s_agent_cache_entries"]
	return out
}

// readAgentMetrics prefers in-cluster Service proxy (no port-forward), then AGENT_HTTP_URL.
func (c *resourcesController) readAgentMetrics(ctx context.Context, namespace, agentHTTP string) ([]byte, string, error) {
	var kubeErr error
	if c.modes != nil && c.modes.k8s != nil && namespace != "" {
		raw, err := c.modes.k8s.CoreV1().Services(namespace).ProxyGet("http", "k8s-agent-http", "http", "metrics", nil).DoRaw(ctx)
		if err == nil {
			return raw, "kube-proxy:" + namespace + "/services/k8s-agent-http", nil
		}
		kubeErr = err
	}

	base := strings.TrimRight(agentHTTP, "/")
	if base == "" {
		if kubeErr != nil {
			return nil, "", fmt.Errorf("agent /metrics via kube proxy failed: %w", kubeErr)
		}
		return nil, "", fmt.Errorf("agent HTTP URL not set")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/metrics", nil)
	if err != nil {
		return nil, "", err
	}
	if tok := c.cfg.AgentHTTPBearer; tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		msg := err.Error()
		if kubeErr != nil {
			msg = fmt.Sprintf("%s; kube proxy: %v", msg, kubeErr)
		}
		return nil, "", fmt.Errorf("%s — run: powershell -File hack/port-forward-agent-http.ps1 (or rely on kubeconfig Service proxy)", msg)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode >= 300 {
		return nil, "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(b))
	}
	return b, base, nil
}

func parsePromGauges(r io.Reader, names []string) map[string]float64 {
	want := map[string]struct{}{}
	for _, n := range names {
		want[n] = struct{}{}
	}
	out := map[string]float64{}
	sc := bufio.NewScanner(r)
	buf := make([]byte, 0, 64*1024)
	sc.Buffer(buf, 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// metric{labels} value OR metric value
		name := line
		if i := strings.IndexByte(line, '{'); i >= 0 {
			name = line[:i]
		} else if i := strings.IndexByte(line, ' '); i >= 0 {
			name = line[:i]
		}
		if _, ok := want[name]; !ok {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		v, err := strconv.ParseFloat(parts[len(parts)-1], 64)
		if err != nil {
			continue
		}
		// Prefer unlabeled sample if multiple; otherwise keep first.
		if _, exists := out[name]; !exists || !strings.Contains(line, "{") {
			out[name] = v
		}
	}
	return out
}

func (c *resourcesController) applyProfile(ctx context.Context, profileID string) (*applyResourceProfileResponse, error) {
	if c.modes == nil || c.modes.k8s == nil {
		return nil, fmt.Errorf("kubernetes client not available (set KUBECONFIG)")
	}
	p, err := profileByID(profileID)
	if err != nil {
		return nil, err
	}
	ns := c.cfg.AgentNamespace
	resp := &applyResourceProfileResponse{
		Profile:   p.ID,
		Namespace: ns,
		Hints:     []string{},
	}

	deps := []string{"ingress", "egress", "agent-service"}
	for _, name := range deps {
		if err := c.patchDeploymentProfile(ctx, ns, name, p); err != nil {
			return nil, fmt.Errorf("patch %s: %w", name, err)
		}
		resp.Patched = append(resp.Patched, name)
		if err := c.modes.restartDeployment(ctx, name); err != nil {
			return nil, fmt.Errorf("restart %s: %w", name, err)
		}
		resp.Restarted = append(resp.Restarted, name)
	}

	if p.FeaturesMode == "minimal" {
		if _, err := c.modes.applyMode(ctx, "minimal"); err != nil {
			resp.Hints = append(resp.Hints, "features-minimal apply failed: "+err.Error())
		} else {
			resp.Features = "minimal"
		}
	} else if p.FeaturesMode == "full" {
		if _, err := c.modes.applyMode(ctx, "full"); err != nil {
			resp.Hints = append(resp.Hints, "features full apply failed: "+err.Error())
		} else {
			resp.Features = "full"
		}
	}

	resp.Hints = append(resp.Hints,
		"DaemonSet logs-node-agent (v2) is not resized by this profile — check snapshot DS pods separately",
	)
	return resp, nil
}

func (c *resourcesController) patchDeploymentProfile(ctx context.Context, ns, name string, p *resourceProfile) error {
	deploy, err := c.modes.k8s.AppsV1().Deployments(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	replicas := p.Replicas
	deploy.Spec.Replicas = &replicas

	if len(deploy.Spec.Template.Spec.Containers) == 0 {
		return fmt.Errorf("no containers")
	}
	// Patch env on every container (agent / gateway).
	for i := range deploy.Spec.Template.Spec.Containers {
		ctr := &deploy.Spec.Template.Spec.Containers[i]
		for k, v := range p.Env {
			setEnvVar(&ctr.Env, k, v)
		}
		if name == "agent-service" && p.AgentMemLimit != "" {
			if ctr.Resources.Limits == nil {
				ctr.Resources.Limits = corev1.ResourceList{}
			}
			q := resource.MustParse(p.AgentMemLimit)
			ctr.Resources.Limits[corev1.ResourceMemory] = q
		}
	}

	if name == "agent-service" && p.EmptyDirLimit != "" {
		for i := range deploy.Spec.Template.Spec.Volumes {
			vol := &deploy.Spec.Template.Spec.Volumes[i]
			if vol.Name == "logs-temp" && vol.EmptyDir != nil {
				sz := resource.MustParse(p.EmptyDirLimit)
				vol.EmptyDir.SizeLimit = &sz
			}
		}
	}

	_, err = c.modes.k8s.AppsV1().Deployments(ns).Update(ctx, deploy, metav1.UpdateOptions{})
	return err
}

func setEnvVar(env *[]corev1.EnvVar, name, value string) {
	for i := range *env {
		if (*env)[i].Name == name {
			(*env)[i].Value = value
			(*env)[i].ValueFrom = nil
			return
		}
	}
	*env = append(*env, corev1.EnvVar{Name: name, Value: value})
}
