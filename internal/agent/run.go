package agent

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/usmc/usmc-k8s-agent/internal/bridge"
	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/httpapi"
	"github.com/usmc/usmc-k8s-agent/internal/kafka"
	"github.com/usmc/usmc-k8s-agent/internal/leaderelection"
	"github.com/usmc/usmc-k8s-agent/internal/lifecycle"
	"github.com/usmc/usmc-k8s-agent/internal/protodispatch"
	"github.com/usmc/usmc-k8s-agent/internal/transport"
)

func (a *App) runAll(ctx context.Context, devNoLeader bool) error {
	if err := a.http.Start(); err != nil {
		return err
	}
	a.state.SetKafkaConnected(true)
	a.metrics.SyncFromState(a.state)

	if err := a.k8sClient.Ping(ctx); err != nil {
		a.log.Warn("apiserver ping failed", "error", err)
	} else {
		a.state.SetAPIServerOK(true)
		a.metrics.SyncFromState(a.state)
	}

	return a.runWithLeaderElection(ctx, devNoLeader, "", a.commandLoop())
}

func (a *App) runAgentService(ctx context.Context, devNoLeader bool) error {
	internalCfg := a.cfg.HTTP
	internalCfg.Port = a.cfg.Agent.InternalHTTPPort
	a.http = httpapi.NewServer(internalCfg, a.state, a.cacheStore, a.log, a.router)

	if err := a.http.Start(); err != nil {
		return err
	}
	a.state.SetKafkaConnected(true)
	a.metrics.SyncFromState(a.state)

	if err := a.k8sClient.Ping(ctx); err != nil {
		a.log.Warn("apiserver ping failed", "error", err)
	} else {
		a.state.SetAPIServerOK(true)
		a.metrics.SyncFromState(a.state)
	}

	runLeader := func(ctx context.Context) {
		a.state.SetLeader(true)
		a.metrics.SyncFromState(a.state)
		defer func() {
			a.state.SetLeader(false)
			a.metrics.SyncFromState(a.state)
		}()
		defer a.stopSubscriptions()

		a.patchLeaderLabel(ctx, true)
		defer a.patchLeaderLabel(context.Background(), false)

		lc := lifecycle.NewPublisher(a.cfg.Kafka.LifecycleTopic, a.publisher.PublishRaw)
		if err := lc.PublishStarted(ctx, a.cfg.ClusterID, a.cfg.Agent.InstanceID, true); err != nil {
			a.log.Warn("publish agent.lifecycle failed", "error", err)
		}
		defer func() {
			pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := lc.PublishLeaderLost(pubCtx, a.cfg.ClusterID, a.cfg.Agent.InstanceID); err != nil {
				a.log.Warn("publish agent.lifecycle leader lost failed", "error", err)
			}
		}()

		a.log.Info("agent-service leader ready")
		<-ctx.Done()
	}

	leaseName := a.cfg.Agent.LeaderLeaseName
	if leaseName == "" {
		leaseName = "k8s-agent-leader"
	}
	return a.runWithLeaderElection(ctx, devNoLeader, leaseName, runLeader)
}

func (a *App) runEgress(ctx context.Context, devNoLeader bool) error {
	if err := a.startProbeServer(); err != nil {
		return err
	}
	a.state.SetKafkaConnected(true)
	a.metrics.SyncFromState(a.state)

	leaseName := a.cfg.Agent.LeaderLeaseName
	if leaseName == "" {
		leaseName = "k8s-agent-egress-leader"
	}
	return a.runWithLeaderElection(ctx, devNoLeader, leaseName, a.commandLoop())
}

func (a *App) runIngress(ctx context.Context) error {
	proxy, err := httpapi.NewProxyServer(a.cfg.HTTP.Port, a.cfg.Agent.AgentServiceURL, a.log)
	if err != nil {
		return err
	}
	if err := proxy.Start(); err != nil {
		return err
	}
	a.log.Info("ingress gateway ready", "upstream", a.cfg.Agent.AgentServiceURL)
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownGracePeriod())
	defer cancel()
	return proxy.Shutdown(shutdownCtx)
}

func (a *App) commandLoop() func(context.Context) {
	return func(ctx context.Context) {
		a.state.SetLeader(true)
		a.metrics.SyncFromState(a.state)
		defer func() {
			a.state.SetLeader(false)
			a.metrics.SyncFromState(a.state)
		}()
		defer a.stopSubscriptions()

		if a.cfg.Agent.Component == config.ComponentAll {
			a.patchLeaderLabel(ctx, true)
			defer a.patchLeaderLabel(context.Background(), false)
		}

		// Join the Kafka consumer group only while leading. Standby replicas must not hold partitions.
		// Retry on broker disconnect so a transient Kafka outage does not leave egress "Running" but idle.
		backoff := time.Second
		const maxBackoff = 30 * time.Second
		for {
			if ctx.Err() != nil {
				return
			}
			kcfg := a.cfg.Kafka
			kcfg.RequestTopic = transport.RequestTopicForMode(kcfg, a.cfg.ClusterID)
			consumer, err := kafka.NewConsumer(kcfg, a.log)
			if err != nil {
				a.log.Error("kafka consumer create failed", "error", err, "retry_in", backoff.String())
				if !sleepCtx(ctx, backoff) {
					return
				}
				backoff = nextBackoff(backoff, maxBackoff)
				continue
			}
			a.consumer = consumer

			var executor kafka.CommandExecutor = a.router
			if a.cfg.Agent.Component == config.ComponentEgress {
				executor = bridge.NewRemoteExecutor(a.cfg.Agent.AgentServiceURL, a.cfg.HTTP.InternalBearerToken)
			}
			a.processor = kafka.NewProcessor(
				consumer,
				a.publisher,
				executor,
				a.commandGuard,
				a.cfg.Kafka.CommitOnReceive,
				a.metrics,
				a.log,
			)

			mode := transport.DetectModeFromConfig(a.cfg.Kafka)
			if mode != transport.ModeJSON {
				var protoDisp transport.ProtoDispatcher
				if a.protoDispatch != nil {
					protoDisp = a.protoDispatch
				} else {
					protoDisp = protodispatch.New(a.log)
				}
				dual := transport.NewDualAdapter(mode, executor, protoDisp, a.coreClient, a.log)
				a.processor.SetDualAdapter(dual)
			}

			lc := lifecycle.NewPublisher(a.cfg.Kafka.LifecycleTopic, a.publisher.PublishRaw)
			if err := lc.PublishStarted(ctx, a.cfg.ClusterID, a.cfg.Agent.InstanceID, true); err != nil {
				a.log.Warn("publish agent.lifecycle failed", "error", err)
			}

			a.log.Info("command processor started", "topic", kcfg.RequestTopic, "kafka_mode", mode)
			runErr := a.processor.Run(ctx)
			_ = consumer.Close()
			if a.consumer == consumer {
				a.consumer = nil
			}
			pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := lc.PublishLeaderLost(pubCtx, a.cfg.ClusterID, a.cfg.Agent.InstanceID); err != nil {
				a.log.Warn("publish agent.lifecycle leader lost failed", "error", err)
			}
			cancel()

			if ctx.Err() != nil {
				return
			}
			a.log.Error("command processor stopped", "error", runErr, "retry_in", backoff.String())
			if !sleepCtx(ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, maxBackoff)
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	next := cur * 2
	if next > max {
		return max
	}
	if next < cur {
		return max
	}
	return next
}

func (a *App) runWithLeaderElection(ctx context.Context, devNoLeader bool, leaseName string, fn func(context.Context)) error {
	if devNoLeader || !a.cfg.Agent.LeaderElection {
		go fn(ctx)
		<-ctx.Done()
		return a.shutdown(context.Background())
	}

	kube, err := a.k8sClient.Kubernetes()
	if err != nil {
		return err
	}

	leaseCfg := leaderelection.Config{
		Identity:  a.cfg.Agent.InstanceID,
		LeaseName: leaseName,
	}

	err = leaderelection.Run(ctx, kube, leaseCfg, func(ctx context.Context) {
		a.state.SetLeader(true)
		a.metrics.SyncFromState(a.state)
	}, func(context.Context) {
		a.state.SetLeader(false)
		a.metrics.SyncFromState(a.state)
	}, fn, a.log)

	shutdownErr := a.shutdown(context.Background())
	if err != nil {
		return err
	}
	return shutdownErr
}

func (a *App) startProbeServer() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	// Readiness must NOT require leadership: leader-only /readyz blocks RollingUpdate
	// (new pods never become Ready while the old leader holds the lease).
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !a.state.KafkaConnected() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"ready":false,"kafka":false}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ready":true,"leader":` + boolJSON(a.state.IsLeader()) + `}`))
	})

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", a.cfg.HTTP.Port),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			a.log.Error("probe server error", "error", err)
		}
	}()
	return nil
}

func boolJSON(v bool) string {
	if v {
		return "true"
	}
	return "false"
}
