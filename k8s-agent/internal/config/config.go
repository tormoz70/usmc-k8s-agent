package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	KafkaBrokers       []string
	TopicCommandsIn    string
	TopicCommandsOut   string
	TopicClusterEvents string
	TopicDLQ           string
	ConsumerGroup      string

	HTTPAddr string

	PolicyFile string

	LeaderElectionEnabled bool
	LeaderLeaseName       string
	LeaderLeaseNamespace  string

	TempDir string

	MaxRetries     int
	RetryBaseDelay time.Duration

	InlineListMaxBytes int64
}

func LoadFromEnv() (*Config, error) {
	brokers := envOr("KAFKA_BROKERS", "localhost:9092")
	cfg := &Config{
		KafkaBrokers:          splitCSV(brokers),
		TopicCommandsIn:       envOr("KAFKA_TOPIC_COMMANDS_IN", "commands.in"),
		TopicCommandsOut:      envOr("KAFKA_TOPIC_COMMANDS_OUT", "commands.results"),
		TopicClusterEvents:    envOr("KAFKA_TOPIC_CLUSTER_EVENTS", "cluster.events"),
		TopicDLQ:              envOr("KAFKA_TOPIC_DLQ", "commands.dlq"),
		ConsumerGroup:         envOr("KAFKA_CONSUMER_GROUP", "k8s-agent"),
		HTTPAddr:              envOr("HTTP_ADDR", ":8080"),
		PolicyFile:            envOr("POLICY_FILE", "/etc/k8s-agent/policy.yaml"),
		LeaderElectionEnabled: envBool("LEADER_ELECTION_ENABLED", true),
		LeaderLeaseName:       envOr("LEADER_LEASE_NAME", "k8s-agent-leader"),
		LeaderLeaseNamespace:  envOr("LEADER_LEASE_NAMESPACE", "k8s-agent"),
		TempDir:               envOr("TEMP_DIR", "/tmp/k8s-agent"),
		MaxRetries:            envInt("MAX_RETRIES", 5),
		RetryBaseDelay:        time.Duration(envInt("RETRY_BASE_DELAY_MS", 200)) * time.Millisecond,
		InlineListMaxBytes:    int64(envInt("INLINE_LIST_MAX_BYTES", 5*1024*1024)),
	}
	if cfg.MaxRetries < 1 {
		return nil, fmt.Errorf("MAX_RETRIES must be >= 1")
	}
	return cfg, nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func envBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func splitCSV(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			part := trim(s[start:i])
			if part != "" {
				out = append(out, part)
			}
			start = i + 1
		}
	}
	if len(out) == 0 {
		return []string{"localhost:9092"}
	}
	return out
}

func trim(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
