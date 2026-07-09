package features

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/usmc/usmc-k8s-agent/internal/command"
)

func TestApplyMinimalProfile(t *testing.T) {
	featCfg, err := Load(filepath.Join("..", "..", "deploy", "base", "policy", "features-minimal.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	res, err := Apply(featCfg)
	if err != nil {
		t.Fatal(err)
	}
	reg := res.Registry
	if reg.CommandEnabled(command.TypeLogsCollect) {
		t.Fatal("logs.collect should be disabled in minimal profile")
	}
	if !reg.CommandEnabled(command.TypeWatchSubscribe) {
		t.Fatal("watch should be enabled")
	}
	if !reg.Enabled("cluster_inventory") {
		t.Fatal("cluster_inventory should be enabled")
	}
	if reg.Enabled("istio_manage") {
		t.Fatal("istio should be disabled")
	}
	hasNamespaceWatch := false
	for _, g := range res.AllowedGVK {
		if g.Kind == "Namespace" {
			hasNamespaceWatch = true
		}
	}
	if !hasNamespaceWatch {
		t.Fatal("Namespace GVK should be allowed for watch in minimal profile")
	}
}

func TestApplyRequiresEnabledFeature(t *testing.T) {
	featCfg := &Config{
		Features: map[string]Feature{
			"all_off": {Enabled: false, CommandTypes: []string{"k8s.api"}},
		},
	}
	_, err := Apply(featCfg)
	if err == nil {
		t.Fatal("expected error when all features disabled")
	}
}

func TestLoadMissingFile(t *testing.T) {
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg != nil {
		t.Fatal("expected nil config for missing file")
	}
}

func TestLoadFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "features.yaml")
	if err := os.WriteFile(path, []byte(`
features:
  cache:
    enabled: true
    command_types: [cache.put]
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Features["cache"].Enabled {
		t.Fatal("cache should be enabled")
	}
}

func TestCommandEnabledNilRegistry(t *testing.T) {
	var reg *Registry
	if !reg.CommandEnabled(command.TypeK8sAPI) {
		t.Fatal("nil registry should allow all commands")
	}
}
