package k8s

import (
	"context"
	"fmt"
	"io"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

type LogTarget struct {
	Namespace string `json:"namespace"`
	Pod       string `json:"pod"`
	Container string `json:"container"`
	Previous  bool   `json:"previous"`
}

type LogFetchOptions struct {
	SinceSeconds *int64
	TailLines    *int64
	LimitBytes   *int64
}

type LogReader struct {
	clients *Clients
}

func NewLogReader(clients *Clients) *LogReader {
	return &LogReader{clients: clients}
}

func (r *LogReader) Fetch(ctx context.Context, target LogTarget, opts LogFetchOptions) ([]byte, error) {
	req := r.clients.Kube.CoreV1().Pods(target.Namespace).GetLogs(target.Pod, &corev1.PodLogOptions{
		Container:  target.Container,
		Previous:   target.Previous,
		SinceSeconds: opts.SinceSeconds,
		TailLines:    opts.TailLines,
		LimitBytes:   opts.LimitBytes,
	})
	stream, err := req.Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("stream logs %s/%s/%s: %w", target.Namespace, target.Pod, target.Container, err)
	}
	defer stream.Close()
	return io.ReadAll(stream)
}

func (r *LogReader) FollowStream(ctx context.Context, target LogTarget, sinceSeconds int64, timestamps bool) (io.ReadCloser, error) {
	opts := &corev1.PodLogOptions{
		Container:    target.Container,
		Previous:     target.Previous,
		Follow:       true,
		Timestamps:   timestamps,
		SinceSeconds: &sinceSeconds,
	}
	req := r.clients.Kube.CoreV1().Pods(target.Namespace).GetLogs(target.Pod, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("follow logs %s/%s/%s: %w", target.Namespace, target.Pod, target.Container, err)
	}
	return stream, nil
}

func (r *LogReader) ResolveDeploymentName(ctx context.Context, ns, podName string) string {
	pod, err := r.clients.Kube.CoreV1().Pods(ns).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return "unknown"
	}
	for _, owner := range pod.OwnerReferences {
		if owner.Kind == "ReplicaSet" {
			rs, err := r.clients.Kube.AppsV1().ReplicaSets(ns).Get(ctx, owner.Name, metav1.GetOptions{})
			if err != nil {
				continue
			}
			for _, rsOwner := range rs.OwnerReferences {
				if rsOwner.Kind == "Deployment" {
					return rsOwner.Name
				}
			}
		}
	}
	return "standalone"
}

func TimestampForFilename() string {
	return time.Now().UTC().Format("20060102T150405Z")
}
