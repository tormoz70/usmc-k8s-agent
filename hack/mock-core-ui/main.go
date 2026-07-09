// mock-core-ui is a web UI for local E2E testing (mock Java core-client).
package main

import (
	"embed"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"

	"github.com/usmc/usmc-k8s-agent/hack/mockcorelib"
)

//go:embed static/*
var staticFS embed.FS

type config struct {
	Addr             string
	Brokers          []string
	RequestTopic     string
	ReplyTopic       string
	FixturesDir      string
	FeaturesDir      string
	Kubeconfig       string
	AgentNamespace   string
	AgentConfigMap   string
	AgentDeployments []string
	S3Endpoint       string
	S3Region         string
	S3AccessKey      string
	S3SecretKey      string
	S3PathStyle      bool
	MinIOConsole     string
	DefaultTopics    []string
}

func main() {
	addr := flag.String("addr", env("MOCK_CORE_UI_ADDR", ":8090"), "listen address")
	brokers := flag.String("brokers", env("KAFKA_BROKERS", "localhost:9092"), "Kafka brokers")
	requestTopic := flag.String("request-topic", env("KAFKA_REQUEST_TOPIC", mockcorelib.DefaultRequestTopic), "request topic")
	replyTopic := flag.String("reply-topic", env("KAFKA_REPLY_TOPIC", mockcorelib.DefaultReplyTopic), "default reply topic")
	fixturesDir := flag.String("fixtures", env("FIXTURES_DIR", "test/fixtures"), "command template directory")
	featuresDir := flag.String("features-dir", env("FEATURES_DIR", "deploy/base/policy"), "agent feature preset directory")
	kubeconfig := flag.String("kubeconfig", env("KUBECONFIG", ""), "kubeconfig for agent mode switching")
	agentNamespace := flag.String("agent-namespace", env("AGENT_NAMESPACE", "uamc-agent"), "agent namespace")
	agentConfigMap := flag.String("agent-configmap", env("AGENT_CONFIGMAP", "k8s-agent-policy"), "policy configmap name")
	s3Endpoint := flag.String("s3-endpoint", env("S3_ENDPOINT", "http://localhost:9000"), "S3 endpoint")
	s3Region := flag.String("s3-region", env("S3_REGION", "us-east-1"), "S3 region")
	s3AccessKey := flag.String("s3-access-key", env("S3_ACCESS_KEY", "minioadmin"), "S3 access key")
	s3SecretKey := flag.String("s3-secret-key", env("S3_SECRET_KEY", "minioadmin"), "S3 secret key")
	minioConsole := flag.String("minio-console", env("MINIO_CONSOLE_URL", "http://localhost:9001"), "MinIO console URL for links")
	flag.Parse()

	cfg := config{
		Addr:           *addr,
		Brokers:        mockcorelib.SplitBrokers(*brokers),
		RequestTopic:   *requestTopic,
		ReplyTopic:     *replyTopic,
		FixturesDir:    *fixturesDir,
		FeaturesDir:    *featuresDir,
		Kubeconfig:     *kubeconfig,
		AgentNamespace: *agentNamespace,
		AgentConfigMap: *agentConfigMap,
		AgentDeployments: []string{"agent-service", "egress"},
		S3Endpoint:     *s3Endpoint,
		S3Region:     *s3Region,
		S3AccessKey:  *s3AccessKey,
		S3SecretKey:  *s3SecretKey,
		S3PathStyle:  envBool("S3_FORCE_PATH_STYLE", true),
		MinIOConsole: strings.TrimRight(*minioConsole, "/"),
		DefaultTopics: []string{
			mockcorelib.DefaultRequestTopic,
			mockcorelib.DefaultReplyTopic,
			"cluster.events",
			"cluster.health",
			"logs.stream",
			"agent.lifecycle",
		},
	}

	srv := newServer(cfg)
	mux := http.NewServeMux()
	mux.Handle("/", srv.staticHandler())
	mux.HandleFunc("/api/templates", srv.handleTemplates)
	mux.HandleFunc("/api/topics", srv.handleTopics)
	mux.HandleFunc("/api/commands", srv.handleCommands)
	mux.HandleFunc("/api/messages/stream", srv.handleMessageStream)
	mux.HandleFunc("/api/s3/head", srv.handleS3Head)
	mux.HandleFunc("/api/agent/modes", srv.handleAgentModes)
	mux.HandleFunc("/api/agent/mode", srv.handleAgentMode)
	mux.HandleFunc("/api/health", srv.handleHealth)

	slog.Info("mock-core-ui listening", "addr", cfg.Addr, "brokers", cfg.Brokers, "fixtures", cfg.FixturesDir)
	if err := http.ListenAndServe(cfg.Addr, mux); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envBool(k string, d bool) bool {
	v := os.Getenv(k)
	if v == "" {
		return d
	}
	switch strings.ToLower(v) {
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		return d
	}
}
