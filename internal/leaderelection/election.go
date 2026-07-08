package leaderelection

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

const (
	defaultLeaseName      = "k8s-agent-leader"
	defaultLeaseNamespace = "uamc-agent"
	defaultLeaseDuration  = 15 * time.Second
	defaultRenewDeadline  = 10 * time.Second
	defaultRetryPeriod    = 2 * time.Second
)

// Config configures Kubernetes Lease-based leader election.
type Config struct {
	LeaseName      string
	LeaseNamespace string
	Identity       string
}

// Run executes fn while holding the leader lease; blocks until context done.
func Run(ctx context.Context, client kubernetes.Interface, cfg Config, onStartedLeading, onStoppedLeading func(context.Context), fn func(context.Context), log *slog.Logger) error {
	if client == nil {
		return fmt.Errorf("kubernetes client is nil")
	}
	if log == nil {
		log = slog.Default()
	}
	if cfg.LeaseName == "" {
		cfg.LeaseName = defaultLeaseName
	}
	if cfg.LeaseNamespace == "" {
		cfg.LeaseNamespace = env("POD_NAMESPACE", defaultLeaseNamespace)
	}
	if cfg.Identity == "" {
		cfg.Identity = env("HOSTNAME", "k8s-agent")
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      cfg.LeaseName,
			Namespace: cfg.LeaseNamespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: cfg.Identity,
		},
	}

	lec := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		LeaseDuration:   defaultLeaseDuration,
		RenewDeadline:   defaultRenewDeadline,
		RetryPeriod:     defaultRetryPeriod,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				log.Info("became leader", "identity", cfg.Identity)
				if onStartedLeading != nil {
					onStartedLeading(ctx)
				}
				fn(ctx)
			},
			OnStoppedLeading: func() {
				log.Info("lost leadership", "identity", cfg.Identity)
				if onStoppedLeading != nil {
					onStoppedLeading(context.Background())
				}
			},
			OnNewLeader: func(identity string) {
				if identity == cfg.Identity {
					return
				}
				log.Info("new leader elected", "leader", identity)
			},
		},
	}

	leaderelection.RunOrDie(ctx, lec)
	return nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
