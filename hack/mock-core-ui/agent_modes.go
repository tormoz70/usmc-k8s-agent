package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/usmc/usmc-k8s-agent/internal/features"
)

type agentModePreset struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	File        string `json:"file"`
}

type agentModeFeature struct {
	ID          string `json:"id"`
	Enabled     bool   `json:"enabled"`
	Description string `json:"description,omitempty"`
	RBACRole    string `json:"rbac_role,omitempty"`
	WatchGVK    []string `json:"watch_gvk,omitempty"`
}

type agentModeStatus struct {
	K8sAvailable    bool               `json:"k8s_available"`
	K8sError        string             `json:"k8s_error,omitempty"`
	CurrentMode     string             `json:"current_mode"`
	CurrentModeName string             `json:"current_mode_name"`
	EnabledFeatures []string           `json:"enabled_features"`
	Features        []agentModeFeature `json:"features"`
}

type applyAgentModeRequest struct {
	Mode string `json:"mode"`
}

type applyAgentModeResponse struct {
	Mode            string   `json:"mode"`
	Restarted       []string `json:"restarted"`
	EnabledFeatures []string `json:"enabled_features"`
}

type agentModeController struct {
	cfg        config
	presets    []agentModePreset
	k8s        kubernetes.Interface
	restConfig *rest.Config
}

func newAgentModeController(cfg config) *agentModeController {
	c := &agentModeController{
		cfg: cfg,
		presets: []agentModePreset{
			{
				ID:          "full",
				Name:        "Full",
				Description: "All capabilities: inventory, workloads, Istio, logs, watch, health, cache",
				File:        "features.yaml",
			},
			{
				ID:          "minimal",
				Name:        "Observability",
				Description: "Read inventory + watch + health only (no writes, logs export, cache)",
				File:        "features-minimal.yaml",
			},
		},
	}
	clientset, restCfg, err := newKubeClients(cfg.Kubeconfig)
	if err != nil {
		c.k8s = nil
	} else {
		c.k8s = clientset
		c.restConfig = restCfg
	}
	return c
}

func newKubeClients(kubeconfig string) (kubernetes.Interface, *rest.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	if kubeconfig != "" {
		loadingRules.ExplicitPath = kubeconfig
	}
	overrides := &clientcmd.ConfigOverrides{}
	clientCfg := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, overrides)
	restCfg, err := clientCfg.ClientConfig()
	if err != nil {
		return nil, nil, err
	}
	clientset, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, nil, err
	}
	return clientset, restCfg, nil
}

func (c *agentModeController) listPresets() []agentModePreset {
	return c.presets
}

func (c *agentModeController) presetByID(id string) (*agentModePreset, error) {
	for i := range c.presets {
		if c.presets[i].ID == id {
			return &c.presets[i], nil
		}
	}
	return nil, fmt.Errorf("unknown mode %q", id)
}

func (c *agentModeController) readPresetYAML(modeID string) ([]byte, *features.Config, error) {
	preset, err := c.presetByID(modeID)
	if err != nil {
		return nil, nil, err
	}
	path := filepath.Join(c.cfg.FeaturesDir, preset.File)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read preset %s: %w", preset.File, err)
	}
	featCfg, err := features.Load(path)
	if err != nil {
		return nil, nil, err
	}
	if featCfg == nil {
		return nil, nil, fmt.Errorf("preset %s is empty", preset.File)
	}
	return data, featCfg, nil
}

func (c *agentModeController) status(ctx context.Context) (*agentModeStatus, error) {
	st := &agentModeStatus{
		K8sAvailable: c.k8s != nil,
		CurrentMode:  "unknown",
	}
	if !st.K8sAvailable {
		st.K8sError = "KUBECONFIG not configured or cluster unreachable"
		return st, nil
	}

	cm, err := c.k8s.CoreV1().ConfigMaps(c.cfg.AgentNamespace).Get(ctx, c.cfg.AgentConfigMap, metav1.GetOptions{})
	if err != nil {
		st.K8sError = err.Error()
		return st, nil
	}
	currentYAML := cm.Data["features.yaml"]
	if currentYAML == "" {
		st.K8sError = "configmap missing features.yaml key"
		return st, nil
	}

	currentCfg, err := parseFeaturesYAML([]byte(currentYAML))
	if err != nil {
		st.K8sError = err.Error()
		return st, nil
	}

	st.CurrentMode, st.CurrentModeName = c.detectMode(currentYAML)
	st.EnabledFeatures = enabledFeatureNames(currentCfg)
	st.Features = summarizeFeatures(currentCfg)
	return st, nil
}

func parseFeaturesYAML(data []byte) (*features.Config, error) {
	var cfg features.Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse features.yaml: %w", err)
	}
	return &cfg, nil
}

func enabledFeatureNames(cfg *features.Config) []string {
	if cfg == nil {
		return nil
	}
	out := make([]string, 0, len(cfg.Features))
	for name, feat := range cfg.Features {
		if feat.Enabled {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

func summarizeFeatures(cfg *features.Config) []agentModeFeature {
	if cfg == nil {
		return nil
	}
	names := make([]string, 0, len(cfg.Features))
	for name := range cfg.Features {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]agentModeFeature, 0, len(names))
	for _, name := range names {
		feat := cfg.Features[name]
		item := agentModeFeature{
			ID:          name,
			Enabled:     feat.Enabled,
			Description: feat.Description,
			RBACRole:    feat.RBACRole,
		}
		if name == "watch_events" && len(feat.AllowedGVK) > 0 {
			for _, g := range feat.AllowedGVK {
				item.WatchGVK = append(item.WatchGVK, gvkLabel(g))
			}
		}
		out = append(out, item)
	}
	return out
}

func gvkLabel(g features.GVK) string {
	if g.Group == "" {
		return g.Version + "/" + g.Kind
	}
	return g.Group + "/" + g.Version + "/" + g.Kind
}

func normalizeYAML(data []byte) (string, error) {
	var v any
	if err := yaml.Unmarshal(data, &v); err != nil {
		return "", err
	}
	out, err := yaml.Marshal(v)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(out)), nil
}

func (c *agentModeController) detectMode(currentYAML string) (id, name string) {
	currentNorm, err := normalizeYAML([]byte(currentYAML))
	if err != nil {
		return "custom", "Custom"
	}
	for _, preset := range c.presets {
		data, err := os.ReadFile(filepath.Join(c.cfg.FeaturesDir, preset.File))
		if err != nil {
			continue
		}
		presetNorm, err := normalizeYAML(data)
		if err != nil {
			continue
		}
		if presetNorm == currentNorm {
			return preset.ID, preset.Name
		}
	}
	return "custom", "Custom"
}

func (c *agentModeController) applyMode(ctx context.Context, modeID string) (*applyAgentModeResponse, error) {
	if c.k8s == nil {
		return nil, fmt.Errorf("kubernetes client not available (set KUBECONFIG)")
	}
	preset, err := c.presetByID(modeID)
	if err != nil {
		return nil, err
	}
	data, featCfg, err := c.readPresetYAML(modeID)
	if err != nil {
		return nil, err
	}
	if _, err := features.Apply(featCfg); err != nil {
		return nil, fmt.Errorf("invalid preset %s: %w", preset.File, err)
	}

	cm, err := c.k8s.CoreV1().ConfigMaps(c.cfg.AgentNamespace).Get(ctx, c.cfg.AgentConfigMap, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("get configmap: %w", err)
	}
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	cm.Data["features.yaml"] = string(data)
	if _, err := c.k8s.CoreV1().ConfigMaps(c.cfg.AgentNamespace).Update(ctx, cm, metav1.UpdateOptions{}); err != nil {
		return nil, fmt.Errorf("update configmap: %w", err)
	}

	restarted := make([]string, 0, len(c.cfg.AgentDeployments))
	for _, depName := range c.cfg.AgentDeployments {
		if err := c.restartDeployment(ctx, depName); err != nil {
			return nil, fmt.Errorf("restart %s: %w", depName, err)
		}
		restarted = append(restarted, depName)
	}

	return &applyAgentModeResponse{
		Mode:            modeID,
		Restarted:       restarted,
		EnabledFeatures: enabledFeatureNames(featCfg),
	}, nil
}

func (c *agentModeController) restartDeployment(ctx context.Context, name string) error {
	deploy, err := c.k8s.AppsV1().Deployments(c.cfg.AgentNamespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if deploy.Spec.Template.Annotations == nil {
		deploy.Spec.Template.Annotations = map[string]string{}
	}
	deploy.Spec.Template.Annotations["kubectl.kubernetes.io/restartedAt"] = time.Now().UTC().Format(time.RFC3339)
	_, err = c.k8s.AppsV1().Deployments(c.cfg.AgentNamespace).Update(ctx, deploy, metav1.UpdateOptions{})
	return err
}
