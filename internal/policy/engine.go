package policy

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/usmc/usmc-k8s-agent/internal/features"
)

// GVK identifies a Kubernetes or CRD resource type.
type GVK struct {
	Group   string `yaml:"group" json:"group"`
	Version string `yaml:"version" json:"version"`
	Kind    string `yaml:"kind" json:"kind"`
}

// Config is the allow-list loaded from policy files.
type Config struct {
	AllowedGVK                []GVK    `yaml:"allowed_gvk"`
	AllowedVerbs              []string `yaml:"allowed_verbs"`
	AllowedNamespaces         []string `yaml:"allowed_namespaces"`
	AllowedCommandTypes       []string `yaml:"allowed_command_types"`
	AllowedIssuers            []string `yaml:"allowed_issuers"`
	AllowedReplyTopics        []string `yaml:"allowed_reply_topics"`
	AllowedReplyTopicPrefixes []string `yaml:"allowed_reply_topic_prefixes"`
	DenySecrets               bool     `yaml:"deny_secrets"`
}

// Engine evaluates requests against the allow-list.
type Engine struct {
	cfg    Config
	ns     map[string]struct{}
	gvk    map[gvkKey]struct{}
	verb   map[string]struct{}
	cmd    map[string]struct{}
	issuer map[string]struct{}
	reply  map[string]struct{}
	replyPrefix []string
}

type gvkKey struct {
	group   string
	version string
	kind    string
}

// LoadFromFiles reads policy YAML and optional namespaces list file.
func LoadFromFiles(policyFile, namespacesFile string) (*Engine, error) {
	data, err := os.ReadFile(policyFile)
	if err != nil {
		return nil, fmt.Errorf("read policy file: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse policy file: %w", err)
	}
	if namespacesFile != "" {
		nsData, err := os.ReadFile(namespacesFile)
		if err != nil {
			return nil, fmt.Errorf("read namespaces file: %w", err)
		}
		var nsList struct {
			Namespaces []string `yaml:"namespaces"`
		}
		if err := yaml.Unmarshal(nsData, &nsList); err != nil {
			return nil, fmt.Errorf("parse namespaces file: %w", err)
		}
		if len(nsList.Namespaces) > 0 {
			cfg.AllowedNamespaces = nsList.Namespaces
		}
	}
	return NewEngine(cfg)
}

// LoadFromFilesWithFeatures loads policy, optional namespaces list, and optional features toggles.
// When featuresFile is set and readable, enabled groups override allowed_command_types and allowed_gvk.
func LoadFromFilesWithFeatures(policyFile, namespacesFile, featuresFile string) (*Engine, *features.Registry, error) {
	data, err := os.ReadFile(policyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read policy file: %w", err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, nil, fmt.Errorf("parse policy file: %w", err)
	}
	if namespacesFile != "" {
		nsData, err := os.ReadFile(namespacesFile)
		if err != nil {
			return nil, nil, fmt.Errorf("read namespaces file: %w", err)
		}
		var nsList struct {
			Namespaces []string `yaml:"namespaces"`
		}
		if err := yaml.Unmarshal(nsData, &nsList); err != nil {
			return nil, nil, fmt.Errorf("parse namespaces file: %w", err)
		}
		if len(nsList.Namespaces) > 0 {
			cfg.AllowedNamespaces = nsList.Namespaces
		}
	}
	featCfg, err := features.Load(featuresFile)
	if err != nil {
		return nil, nil, err
	}
	featRes, err := features.Apply(featCfg)
	if err != nil {
		return nil, nil, err
	}
	if featCfg != nil {
		cfg.AllowedCommandTypes = featRes.CommandTypes
		if len(featRes.AllowedGVK) > 0 {
			cfg.AllowedGVK = make([]GVK, len(featRes.AllowedGVK))
			for i, g := range featRes.AllowedGVK {
				cfg.AllowedGVK[i] = GVK{Group: g.Group, Version: g.Version, Kind: g.Kind}
			}
		}
	}
	engine, err := NewEngine(cfg)
	if err != nil {
		return nil, nil, err
	}
	return engine, featRes.Registry, nil
}

// NewEngine builds an Engine from Config with defaults applied.
func NewEngine(cfg Config) (*Engine, error) {
	cfg.DenySecrets = true
	if len(cfg.AllowedVerbs) == 0 {
		cfg.AllowedVerbs = []string{"get", "list", "watch", "create", "update", "patch", "delete", "apply"}
	}
	if len(cfg.AllowedCommandTypes) == 0 {
		cfg.AllowedCommandTypes = []string{"k8s.api"}
	}

	e := &Engine{
		cfg:         cfg,
		ns:          make(map[string]struct{}, len(cfg.AllowedNamespaces)),
		gvk:         make(map[gvkKey]struct{}, len(cfg.AllowedGVK)),
		verb:        make(map[string]struct{}, len(cfg.AllowedVerbs)),
		cmd:         make(map[string]struct{}, len(cfg.AllowedCommandTypes)),
		issuer:      make(map[string]struct{}, len(cfg.AllowedIssuers)),
		reply:       make(map[string]struct{}, len(cfg.AllowedReplyTopics)),
		replyPrefix: append([]string(nil), cfg.AllowedReplyTopicPrefixes...),
	}

	for _, ns := range cfg.AllowedNamespaces {
		ns = strings.TrimSpace(ns)
		if ns == "" || ns == "*" {
			return nil, fmt.Errorf("wildcard namespace is not allowed")
		}
		e.ns[ns] = struct{}{}
	}
	for _, g := range cfg.AllowedGVK {
		e.gvk[gvkKey{group: g.Group, version: g.Version, kind: g.Kind}] = struct{}{}
	}
	for _, v := range cfg.AllowedVerbs {
		e.verb[strings.ToLower(v)] = struct{}{}
	}
	for _, c := range cfg.AllowedCommandTypes {
		e.cmd[c] = struct{}{}
	}
	for _, issuer := range cfg.AllowedIssuers {
		issuer = strings.TrimSpace(issuer)
		if issuer != "" {
			e.issuer[issuer] = struct{}{}
		}
	}
	for _, topic := range cfg.AllowedReplyTopics {
		topic = strings.TrimSpace(topic)
		if topic != "" {
			e.reply[topic] = struct{}{}
		}
	}
	return e, nil
}

// AllowCommandType checks typed command routing.
func (e *Engine) AllowCommandType(commandType string) error {
	if _, ok := e.cmd[commandType]; ok {
		return nil
	}
	return fmt.Errorf("command type %q is not allowed", commandType)
}

// AllowHTTP checks k8s.api proxy requests.
func (e *Engine) AllowHTTP(method, path string) error {
	req, err := ParseAPIPath(method, path)
	if err != nil {
		return err
	}
	if e.cfg.DenySecrets && req.IsSecret {
		return fmt.Errorf("secret access is denied")
	}
	if req.Namespace != "" {
		if err := e.allowNamespace(req.Namespace); err != nil {
			return err
		}
	}
	if req.Verb != "" {
		if _, ok := e.verb[strings.ToLower(req.Verb)]; !ok {
			return fmt.Errorf("verb %q is not allowed", req.Verb)
		}
	}
	if len(e.gvk) > 0 {
		if req.Kind == "" {
			return fmt.Errorf("resource kind could not be determined for path %q", path)
		}
		key := gvkKey{group: req.Group, version: req.Version, kind: req.Kind}
		if _, ok := e.gvk[key]; !ok {
			return fmt.Errorf("resource %s/%s/%s is not allowed", req.Group, req.Version, req.Kind)
		}
	}
	return nil
}

// AllowIssuer checks command envelope issuer against allow-list.
func (e *Engine) AllowIssuer(issuer string) error {
	if len(e.issuer) == 0 {
		return nil
	}
	issuer = strings.TrimSpace(issuer)
	if _, ok := e.issuer[issuer]; ok {
		return nil
	}
	return fmt.Errorf("issuer %q is not allowed", issuer)
}

// AllowReplyTopic checks Kafka reply topic header against allow-list.
func (e *Engine) AllowReplyTopic(topic string) error {
	if len(e.reply) == 0 && len(e.replyPrefix) == 0 {
		return nil
	}
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return fmt.Errorf("reply_topic is required")
	}
	if _, ok := e.reply[topic]; ok {
		return nil
	}
	for _, prefix := range e.replyPrefix {
		if prefix != "" && strings.HasPrefix(topic, prefix) {
			return nil
		}
	}
	return fmt.Errorf("reply topic %q is not allowed", topic)
}

// AllowNamespace checks namespace for typed commands (logs.collect, watch, etc.).
func (e *Engine) AllowNamespace(ns string) error {
	return e.allowNamespace(ns)
}

func (e *Engine) allowNamespace(ns string) error {
	if len(e.ns) == 0 {
		return nil
	}
	if _, ok := e.ns[ns]; ok {
		return nil
	}
	return fmt.Errorf("namespace %q is not allowed", ns)
}

// AllowGVK checks typed command target resource types (watch, etc.).
func (e *Engine) AllowGVK(gvk GVK) error {
	if len(e.gvk) == 0 {
		return nil
	}
	if gvk.Kind == "" {
		return fmt.Errorf("resource kind is required")
	}
	key := gvkKey{group: gvk.Group, version: gvk.Version, kind: gvk.Kind}
	if _, ok := e.gvk[key]; ok {
		return nil
	}
	return fmt.Errorf("resource %s/%s/%s is not allowed", gvk.Group, gvk.Version, gvk.Kind)
}

// AllowedNamespaces returns a copy for health/watch handlers (later phases).
func (e *Engine) AllowedNamespaces() []string {
	out := make([]string, len(e.cfg.AllowedNamespaces))
	copy(out, e.cfg.AllowedNamespaces)
	return out
}
