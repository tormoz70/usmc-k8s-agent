package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/usmc/usmc-k8s-agent/internal/config"
)

const (
	SecurityPlaintext = "plaintext"
	SecurityTLS       = "tls"
	SecurityMTLS      = "mtls"
)

// ResolveSecurityMode returns the effective Kafka security mode.
func ResolveSecurityMode(cfg config.KafkaConfig) string {
	if cfg.SecurityMode != "" {
		return cfg.SecurityMode
	}
	if !cfg.TLS.Enabled {
		return SecurityPlaintext
	}
	if cfg.TLS.CertFile != "" && cfg.TLS.KeyFile != "" {
		return SecurityMTLS
	}
	return SecurityTLS
}

// ValidateSecurityConfig checks TLS/mTLS settings before connecting to Kafka.
func ValidateSecurityConfig(cfg config.KafkaConfig) error {
	mode := ResolveSecurityMode(cfg)
	switch mode {
	case SecurityPlaintext:
		if cfg.TLSRequired {
			return fmt.Errorf("KAFKA_TLS_REQUIRED is set but security mode is plaintext")
		}
		return nil
	case SecurityTLS, SecurityMTLS:
		if _, err := BuildTLSConfig(mode, cfg.TLS); err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unsupported KAFKA_SECURITY_MODE %q (use plaintext, tls, or mtls)", mode)
	}
}

// BuildTLSConfig loads TLS settings and fails if required material is missing or invalid.
func BuildTLSConfig(mode string, cfg config.TLSConfig) (*tls.Config, error) {
	if !cfg.Enabled && mode != SecurityPlaintext {
		return nil, fmt.Errorf("kafka TLS is not enabled")
	}
	if cfg.CAFile == "" {
		return nil, fmt.Errorf("KAFKA_TLS_CA_FILE is required for %s", mode)
	}
	caPEM, err := os.ReadFile(cfg.CAFile)
	if err != nil {
		return nil, fmt.Errorf("read KAFKA_TLS_CA_FILE: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("KAFKA_TLS_CA_FILE does not contain valid PEM certificates")
	}

	tlsConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
	}

	if mode == SecurityMTLS {
		if cfg.CertFile == "" || cfg.KeyFile == "" {
			return nil, fmt.Errorf("KAFKA_TLS_CERT_FILE and KAFKA_TLS_KEY_FILE are required for mtls")
		}
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}
