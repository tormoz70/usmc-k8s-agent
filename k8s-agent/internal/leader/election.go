package leader

import (
	"context"
	"log/slog"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

type Callbacks struct {
	OnStartedLeading func(ctx context.Context)
	OnStoppedLeading func()
}

func Run(ctx context.Context, kube kubernetes.Interface, namespace, name string, cb Callbacks, logger *slog.Logger) error {
	if logger == nil {
		logger = slog.Default()
	}
	id, err := os.Hostname()
	if err != nil {
		id = "k8s-agent"
	}

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Client: kube.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: id,
		},
	}

	cfg := leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				logger.Info("became leader", "identity", id)
				if cb.OnStartedLeading != nil {
					cb.OnStartedLeading(ctx)
				}
			},
			OnStoppedLeading: func() {
				logger.Info("lost leadership", "identity", id)
				if cb.OnStoppedLeading != nil {
					cb.OnStoppedLeading()
				}
			},
		},
	}

	leaderelection.RunOrDie(ctx, cfg)
	return nil
}
