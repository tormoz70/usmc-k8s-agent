package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/usmc/k8s-agent/internal/agent"
	"github.com/usmc/k8s-agent/internal/config"
	"github.com/usmc/k8s-agent/internal/k8s"
	"github.com/usmc/k8s-agent/internal/metrics"
	"github.com/usmc/k8s-agent/internal/policy"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg, err := config.LoadFromEnv()
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}

	p, err := policy.Load(cfg.PolicyFile)
	if err != nil {
		logger.Error("load policy", "error", err)
		os.Exit(1)
	}
	pe := policy.NewEngine(p)

	clients, err := k8s.NewInCluster()
	if err != nil {
		logger.Warn("in-cluster config failed, trying kubeconfig", "error", err)
		clients, err = k8s.NewFromKubeconfig(os.Getenv("KUBECONFIG"))
		if err != nil {
			logger.Error("kubernetes client", "error", err)
			os.Exit(1)
		}
	}

	svc, err := agent.New(agent.Options{
		Config:  cfg,
		Logger:  logger,
		Clients: clients,
		Policy:  pe,
	})
	if err != nil {
		logger.Error("create agent", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/", svc.Health().Handler())

	go func() {
		logger.Info("http server listening", "addr", cfg.HTTPAddr)
		if err := http.ListenAndServe(cfg.HTTPAddr, mux); err != nil && err != http.ErrServerClosed {
			logger.Error("http server", "error", err)
			os.Exit(1)
		}
	}()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	logger.Info("k8s-agent starting",
		"commands_in", cfg.TopicCommandsIn,
		"commands_out", cfg.TopicCommandsOut,
		"cluster_events", cfg.TopicClusterEvents,
	)
	if err := svc.Run(ctx); err != nil {
		logger.Error("agent stopped", "error", err)
		os.Exit(1)
	}
	logger.Info("k8s-agent stopped")
}
