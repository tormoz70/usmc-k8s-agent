package features

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/usmc/usmc-k8s-agent/internal/command"
)

// GVK identifies a Kubernetes resource type in features.yaml.
type GVK struct {
	Group   string `yaml:"group" json:"group"`
	Version string `yaml:"version" json:"version"`
	Kind    string `yaml:"kind" json:"kind"`
}

// Feature toggles a capability group (maps to RBAC role + Kafka command types).
type Feature struct {
	Enabled      bool     `yaml:"enabled"`
	Description  string   `yaml:"description,omitempty"`
	RBACRole     string   `yaml:"rbac_role,omitempty"`
	CommandTypes []string `yaml:"command_types"`
	AllowedGVK   []GVK    `yaml:"allowed_gvk,omitempty"`
}

// Config is loaded from features.yaml.
type Config struct {
	Features map[string]Feature `yaml:"features"`
}

// Registry answers enablement queries after policy merge.
type Registry struct {
	enabled map[string]struct{}
	byCmd   map[string]struct{}
}

// Load reads features.yaml. Missing file means all features enabled (no file = full profile).
func Load(path string) (*Config, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read features file: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse features file: %w", err)
	}
	if len(cfg.Features) == 0 {
		return nil, fmt.Errorf("features file %s: no features defined", path)
	}
	return &cfg, nil
}

// ApplyResult holds merged policy fields from enabled features.
type ApplyResult struct {
	CommandTypes []string
	AllowedGVK   []GVK
	Registry     *Registry
}

// Apply merges enabled features into command/GVK allow-lists.
func Apply(featCfg *Config) (*ApplyResult, error) {
	if featCfg == nil || len(featCfg.Features) == 0 {
		return &ApplyResult{Registry: allEnabledRegistry()}, nil
	}

	cmdSet := map[string]struct{}{}
	gvkSet := map[GVK]struct{}{}
	enabledFeatures := map[string]struct{}{}

	for name, feat := range featCfg.Features {
		if !feat.Enabled {
			continue
		}
		if len(feat.CommandTypes) == 0 {
			return nil, fmt.Errorf("feature %q: command_types is required when enabled", name)
		}
		enabledFeatures[name] = struct{}{}
		for _, cmd := range feat.CommandTypes {
			cmdSet[strings.TrimSpace(cmd)] = struct{}{}
		}
		for _, g := range feat.AllowedGVK {
			if g.Kind == "" {
				continue
			}
			gvkSet[g] = struct{}{}
		}
	}

	if len(cmdSet) == 0 {
		return nil, fmt.Errorf("features: at least one feature must be enabled")
	}

	outCmds := make([]string, 0, len(cmdSet))
	for cmd := range cmdSet {
		outCmds = append(outCmds, cmd)
	}

	usesK8sAPI := false
	usesWatch := false
	for cmd := range cmdSet {
		switch cmd {
		case command.TypeK8sAPI:
			usesK8sAPI = true
		case command.TypeWatchSubscribe:
			usesWatch = true
		}
	}

	outGVK := make([]GVK, 0, len(gvkSet))
	for g := range gvkSet {
		outGVK = append(outGVK, g)
	}
	if usesK8sAPI && len(outGVK) == 0 {
		return nil, fmt.Errorf("features: enabled k8s.api requires allowed_gvk on at least one feature")
	}
	if usesWatch && len(outGVK) == 0 {
		return nil, fmt.Errorf("features: enabled watch requires allowed_gvk on watch_events or another feature")
	}

	reg := &Registry{
		enabled: enabledFeatures,
		byCmd:   cmdSet,
	}
	return &ApplyResult{
		CommandTypes: outCmds,
		AllowedGVK:   outGVK,
		Registry:     reg,
	}, nil
}

func allEnabledRegistry() *Registry {
	all := []string{
		command.TypeK8sAPI,
		command.TypeLogsCollect,
		command.TypeWatchSubscribe,
		command.TypeWatchUnsubscribe,
		command.TypeCachePut,
		command.TypeCacheDelete,
		command.TypeCacheClear,
		command.TypeLogsStreamStart,
		command.TypeLogsStreamStop,
		command.TypeHealthReportStart,
		command.TypeHealthReportStop,
	}
	byCmd := make(map[string]struct{}, len(all))
	for _, cmd := range all {
		byCmd[cmd] = struct{}{}
	}
	return &Registry{enabled: nil, byCmd: byCmd}
}

// Enabled reports whether a feature group is active (nil registry = all on).
func (r *Registry) Enabled(name string) bool {
	if r == nil || r.enabled == nil {
		return true
	}
	_, ok := r.enabled[name]
	return ok
}

// CommandEnabled reports whether a command type should be wired and accepted.
func (r *Registry) CommandEnabled(commandType string) bool {
	if r == nil || r.byCmd == nil {
		return true
	}
	_, ok := r.byCmd[commandType]
	return ok
}

// EnabledFeatures returns sorted feature names (empty if all-default).
func (r *Registry) EnabledFeatures(featCfg *Config) []string {
	if featCfg == nil {
		return nil
	}
	out := make([]string, 0, len(featCfg.Features))
	for name, feat := range featCfg.Features {
		if feat.Enabled {
			out = append(out, name)
		}
	}
	return out
}
