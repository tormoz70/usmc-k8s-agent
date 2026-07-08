package kafka

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/usmc/usmc-k8s-agent/internal/config"
)

func TestValidateSecurityConfigPlaintext(t *testing.T) {
	cfg := config.KafkaConfig{SecurityMode: SecurityPlaintext}
	if err := ValidateSecurityConfig(cfg); err != nil {
		t.Fatal(err)
	}
}

func TestValidateSecurityConfigTLSRequiredOnPlaintext(t *testing.T) {
	cfg := config.KafkaConfig{
		SecurityMode: SecurityPlaintext,
		TLSRequired:  true,
	}
	if err := ValidateSecurityConfig(cfg); err == nil {
		t.Fatal("expected tls required error")
	}
}

func TestBuildTLSConfigMissingCA(t *testing.T) {
	_, err := BuildTLSConfig(SecurityTLS, config.TLSConfig{Enabled: true})
	if err == nil {
		t.Fatal("expected missing CA error")
	}
}

func TestBuildTLSConfigMTLSRequiresClientCert(t *testing.T) {
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(ca, []byte("not-a-pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := BuildTLSConfig(SecurityMTLS, config.TLSConfig{
		Enabled: true,
		CAFile:  ca,
	})
	if err == nil {
		t.Fatal("expected error for invalid/missing client cert")
	}
}
