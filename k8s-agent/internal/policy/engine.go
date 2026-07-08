package policy

import (
	"fmt"
	"os"
	"strings"

	"github.com/usmc/k8s-agent/internal/command"
	"gopkg.in/yaml.v3"
)

type GVKRule struct {
	Group   string `yaml:"group"`
	Version string `yaml:"version"`
	Kind    string `yaml:"kind"`
}

type IssuerRule struct {
	Issuer     string   `yaml:"issuer"`
	Namespaces []string `yaml:"namespaces"`
}

type FileFetchPolicy struct {
	MaxBytes               int64    `yaml:"max_bytes"`
	MaxConcurrentTargets   int      `yaml:"max_concurrent_targets"`
	AllowedPresignedHosts  []string `yaml:"allowed_domains"`
}

type LogStreamPolicy struct {
	MaxSubscriptions int `yaml:"max_subscriptions"`
	MaxPatternLength int `yaml:"max_pattern_length"`
}

type Policy struct {
	AllowedGVK           []GVKRule    `yaml:"allowed_gvk"`
	AllowedNamespaces    []string     `yaml:"allowed_namespaces"`
	AllowedCommandTypes  []string     `yaml:"allowed_command_types"`
	IssuerRules          []IssuerRule `yaml:"issuer_rules"`
	FileFetch            FileFetchPolicy `yaml:"file_fetch"`
	LogStream            LogStreamPolicy `yaml:"log_stream"`
	ExcludeSecretKind    bool         `yaml:"exclude_secret_kind"`
}

func Load(path string) (*Policy, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return defaultPolicy(), nil
	}
	var p Policy
	if err := yaml.Unmarshal(data, &p); err != nil {
		return nil, fmt.Errorf("parse policy: %w", err)
	}
	if p.ExcludeSecretKind == false && len(p.AllowedGVK) == 0 {
		p = *defaultPolicy()
	}
	return &p, nil
}

func DefaultPolicy() *Policy {
	return defaultPolicy()
}

func defaultPolicy() *Policy {
	return &Policy{
		AllowedGVK: []GVKRule{
			{Group: "networking.istio.io", Version: "v1beta1", Kind: "VirtualService"},
			{Group: "networking.istio.io", Version: "v1beta1", Kind: "DestinationRule"},
			{Group: "networking.istio.io", Version: "v1beta1", Kind: "Gateway"},
			{Group: "apps", Version: "v1", Kind: "Deployment"},
			{Group: "", Version: "v1", Kind: "Pod"},
		},
		AllowedNamespaces: []string{"app", "istio-system", "k8s-agent"},
		AllowedCommandTypes: []string{
			command.TypeResourceList,
			command.TypeFileFetch,
			command.TypeWatchSubscribe,
			command.TypeWatchUnsubscribe,
			command.TypeLogsStreamSubscribe,
			command.TypeLogsStreamUnsubscribe,
		},
		IssuerRules: []IssuerRule{
			{Issuer: "core-prod", Namespaces: []string{"app", "istio-system"}},
		},
		FileFetch: FileFetchPolicy{
			MaxBytes:              1 << 30,
			MaxConcurrentTargets:  50,
			AllowedPresignedHosts: []string{},
		},
		LogStream: LogStreamPolicy{
			MaxSubscriptions: 100,
			MaxPatternLength: 512,
		},
		ExcludeSecretKind: true,
	}
}

type Engine struct {
	p *Policy
}

func NewEngine(p *Policy) *Engine {
	if p == nil {
		p = defaultPolicy()
	}
	return &Engine{p: p}
}

func (e *Engine) CheckCommand(cmd command.Command) error {
	if !e.allowedCommandType(cmd.Type) {
		return fmt.Errorf("command type %s not allowed", cmd.Type)
	}
	if e.p.ExcludeSecretKind && strings.EqualFold(cmd.Target.Kind, "Secret") {
		return fmt.Errorf("Secret resources are forbidden")
	}
	if cmd.Target.Kind != "" {
		if !e.allowedGVK(cmd.Target.Group, cmd.Target.Version, cmd.Target.Kind) {
			return fmt.Errorf("GVK %s/%s/%s not allowed", cmd.Target.Group, cmd.Target.Version, cmd.Target.Kind)
		}
	}
	if cmd.Target.Namespace != "" && !e.allowedNamespace(cmd.Target.Namespace) {
		return fmt.Errorf("namespace %s not allowed", cmd.Target.Namespace)
	}
	if !e.allowedIssuer(cmd.IssuedBy, cmd.Target.Namespace) {
		return fmt.Errorf("issuer %s not allowed for namespace", cmd.IssuedBy)
	}
	return nil
}

func (e *Engine) CheckGVK(g command.GVK) error {
	if e.p.ExcludeSecretKind && strings.EqualFold(g.Kind, "Secret") {
		return fmt.Errorf("Secret resources are forbidden")
	}
	if !e.allowedGVK(g.Group, g.Version, g.Kind) {
		return fmt.Errorf("GVK not allowed")
	}
	return nil
}

func (e *Engine) CheckNamespace(ns string) error {
	if !e.allowedNamespace(ns) {
		return fmt.Errorf("namespace %s not allowed", ns)
	}
	return nil
}

func (e *Engine) CheckPresignedHost(host string) error {
	if len(e.p.FileFetch.AllowedPresignedHosts) == 0 {
		return nil
	}
	for _, h := range e.p.FileFetch.AllowedPresignedHosts {
		if strings.EqualFold(h, host) {
			return nil
		}
	}
	return fmt.Errorf("presigned URL host %s not allowed", host)
}

func (e *Engine) Policy() *Policy {
	return e.p
}

func (e *Engine) allowedCommandType(t string) bool {
	for _, ct := range e.p.AllowedCommandTypes {
		if ct == t {
			return true
		}
	}
	return false
}

func (e *Engine) allowedGVK(group, version, kind string) bool {
	for _, g := range e.p.AllowedGVK {
		if g.Group == group && g.Version == version && g.Kind == kind {
			return true
		}
	}
	return false
}

func (e *Engine) allowedNamespace(ns string) bool {
	if ns == "" {
		return true
	}
	for _, n := range e.p.AllowedNamespaces {
		if n == ns || n == "*" {
			return true
		}
	}
	return false
}

func (e *Engine) allowedIssuer(issuer, ns string) bool {
	if len(e.p.IssuerRules) == 0 {
		return true
	}
	for _, rule := range e.p.IssuerRules {
		if rule.Issuer != issuer {
			continue
		}
		if ns == "" {
			return true
		}
		for _, allowed := range rule.Namespaces {
			if allowed == ns || allowed == "*" {
				return true
			}
		}
		return false
	}
	return false
}
