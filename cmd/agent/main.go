package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/usmc/usmc-k8s-agent/internal/agent"
)

func main() {
	devNoLeader := flag.Bool("dev-no-leader-election", false, "run command processor without Kubernetes leader election")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	app, err := agent.New(agent.Options{Logger: log})
	if err != nil {
		log.Error("failed to initialize agent", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := app.Run(ctx, *devNoLeader); err != nil {
		log.Error("agent stopped with error", "error", err)
		os.Exit(1)
	}
}
