package logs

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/usmc/usmc-k8s-agent/internal/config"
	"github.com/usmc/usmc-k8s-agent/internal/nodelocal"
	"github.com/usmc/usmc-k8s-agent/internal/result"
	s3client "github.com/usmc/usmc-k8s-agent/internal/s3"
)

// Collector gathers pod logs, builds a zip bundle and uploads to S3.
type Collector struct {
	kube       kubernetes.Interface
	s3         *s3client.Client
	s3Defaults config.S3Config
	maxBytes   int64
	tempRoot   string
	backend    string
	nodeClient *nodelocal.NodeClient
}

func NewCollector(kube kubernetes.Interface, s3 *s3client.Client, s3Defaults config.S3Config, maxBytes int64, tempRoot string) *Collector {
	if tempRoot == "" {
		tempRoot = os.TempDir()
	}
	return &Collector{
		kube:       kube,
		s3:         s3,
		s3Defaults: s3Defaults,
		maxBytes:   maxBytes,
		tempRoot:   tempRoot,
		backend:    config.LogsBackendAPI,
	}
}

// WithNodeLocal enables fan-out to logs-node-agent DaemonSet pods.
func (c *Collector) WithNodeLocal(client *nodelocal.NodeClient) *Collector {
	c.backend = config.LogsBackendNodeLocal
	c.nodeClient = client
	return c
}

type CollectResult struct {
	Bucket        string
	Key           string
	ByteSize      int64
	FileCount     int
	PartialErrors []result.PartialError
	Truncated     bool
}

func (c *Collector) Run(ctx context.Context, payload *CollectPayload) (*CollectResult, error) {
	workDir, err := os.MkdirTemp(c.tempRoot, "logs-collect-*")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(workDir)

	pods, err := c.listPods(ctx, payload)
	if err != nil {
		return nil, err
	}

	var partial []result.PartialError
	var totalBytes int64
	var truncated bool
	fileCount := 0
	ts := time.Now().UTC().Format("20060102T150405Z")

	for _, pod := range pods {
		if truncated {
			break
		}
		deployName, _ := c.deploymentName(ctx, pod)
		containers := payload.ContainerNames(containerNames(pod))

		for _, container := range containers {
			if truncated {
				break
			}
			states := logStates(payload)
			for _, state := range states {
				if truncated {
					break
				}
				relPath := filepath.Join(deployName, pod.Name, fmt.Sprintf("%s-%s-%s.log", container, state.name, ts))
				fullPath := filepath.Join(workDir, relPath)
				if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
					return nil, err
				}

				written, err := c.writePodLogs(ctx, pod, container, state.previous, payload, fullPath)
				if err != nil {
					partial = append(partial, result.PartialError{
						Pod:       pod.Name,
						Container: container,
						Reason:    state.name + "LogError",
						Message:   err.Error(),
					})
					continue
				}
				if written == 0 {
					continue
				}
				totalBytes += written
				fileCount++
				if totalBytes > c.maxBytes {
					truncated = true
					partial = append(partial, result.PartialError{
						Reason:  "SizeLimitExceeded",
						Message: fmt.Sprintf("Bundle reached %d byte hard limit", c.maxBytes),
					})
					break
				}
			}
		}
	}

	if fileCount == 0 {
		return nil, fmt.Errorf("no log files collected")
	}

	zipPath := filepath.Join(workDir, "bundle.zip")
	zipSize, err := zipDir(workDir, zipPath, "bundle.zip")
	if err != nil {
		return nil, err
	}

	f, err := os.Open(zipPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	err = c.s3.Upload(ctx, s3client.UploadInput{
		Endpoint:        c.s3Defaults.Endpoint,
		Region:          c.s3Defaults.Region,
		ForcePathStyle:  c.s3Defaults.ForcePathStyle,
		Bucket:          payload.S3.Bucket,
		Key:             payload.S3.Key,
		AccessKeyID:     payload.S3.AccessKeyID,
		SecretAccessKey: payload.S3.SecretAccessKey,
		Body:            f,
		ContentLength:   zipSize,
		ContentType:     "application/zip",
	})
	if err != nil {
		return nil, err
	}

	return &CollectResult{
		Bucket:        payload.S3.Bucket,
		Key:           payload.S3.Key,
		ByteSize:      zipSize,
		FileCount:     fileCount,
		PartialErrors: partial,
		Truncated:     truncated,
	}, nil
}

type logState struct {
	name     string
	previous bool
}

func logStates(p *CollectPayload) []logState {
	var out []logState
	if p.IncludeCurrent {
		out = append(out, logState{name: "current", previous: false})
	}
	if p.IncludePrevious {
		out = append(out, logState{name: "previous", previous: true})
	}
	return out
}

func (c *Collector) listPods(ctx context.Context, payload *CollectPayload) ([]corev1.Pod, error) {
	if len(payload.Pods) > 0 {
		out := make([]corev1.Pod, 0, len(payload.Pods))
		for _, name := range payload.Pods {
			pod, err := c.kube.CoreV1().Pods(payload.Namespace).Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return nil, fmt.Errorf("get pod %s: %w", name, err)
			}
			out = append(out, *pod)
		}
		return out, nil
	}

	opts := metav1.ListOptions{}
	if payload.LabelSelector != "" {
		opts.LabelSelector = payload.LabelSelector
	}
	list, err := c.kube.CoreV1().Pods(payload.Namespace).List(ctx, opts)
	if err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, fmt.Errorf("no pods matched in namespace %s", payload.Namespace)
	}
	return list.Items, nil
}

func (c *Collector) writePodLogs(ctx context.Context, pod corev1.Pod, container string, previous bool, payload *CollectPayload, dest string) (int64, error) {
	if c.backend == config.LogsBackendNodeLocal && c.nodeClient != nil {
		return c.writePodLogsNodeLocal(ctx, pod, container, previous, payload, dest)
	}
	return c.writePodLogsAPI(ctx, pod.Namespace, pod.Name, container, previous, payload, dest)
}

func (c *Collector) writePodLogsNodeLocal(ctx context.Context, pod corev1.Pod, container string, previous bool, payload *CollectPayload, dest string) (int64, error) {
	node := pod.Spec.NodeName
	if node == "" {
		return 0, fmt.Errorf("pod %s has no nodeName", pod.Name)
	}
	stream, err := c.nodeClient.Fetch(ctx, node, nodelocal.FetchRequest{
		Namespace:  pod.Namespace,
		Pod:        pod.Name,
		PodUID:     string(pod.UID),
		Container:  container,
		Previous:   previous,
		SinceTime:  payload.SinceTime,
		TailLines:  payload.TailLines,
		LimitBytes: payload.LimitBytes,
	})
	if err != nil {
		return 0, err
	}
	defer stream.Close()

	f, err := os.Create(dest)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	return io.Copy(f, stream)
}

func (c *Collector) writePodLogsAPI(ctx context.Context, namespace, pod, container string, previous bool, payload *CollectPayload, dest string) (int64, error) {
	opts := &corev1.PodLogOptions{Container: container, Previous: previous}
	if payload.SinceTime != nil {
		t := payload.SinceTime.UTC()
		opts.SinceTime = &metav1.Time{Time: t}
	}
	if payload.TailLines != nil {
		opts.TailLines = payload.TailLines
	}
	if payload.LimitBytes != nil {
		opts.LimitBytes = payload.LimitBytes
	}

	req := c.kube.CoreV1().Pods(namespace).GetLogs(pod, opts)
	stream, err := req.Stream(ctx)
	if err != nil {
		return 0, err
	}
	defer stream.Close()

	f, err := os.Create(dest)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	n, err := io.Copy(f, stream)
	return n, err
}

func (c *Collector) deploymentName(ctx context.Context, pod corev1.Pod) (string, error) {
	for _, ref := range pod.OwnerReferences {
		if ref.Kind != "ReplicaSet" {
			continue
		}
		rs, err := c.kube.AppsV1().ReplicaSets(pod.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			return sanitizePathPart(pod.Name), err
		}
		for _, rsRef := range rs.OwnerReferences {
			if rsRef.Kind == "Deployment" {
				return sanitizePathPart(rsRef.Name), nil
			}
		}
		return sanitizePathPart(ref.Name), nil
	}
	return sanitizePathPart(pod.Name), nil
}

func containerNames(pod corev1.Pod) []string {
	names := make([]string, 0, len(pod.Spec.Containers))
	for _, c := range pod.Spec.Containers {
		names = append(names, c.Name)
	}
	return names
}

func sanitizePathPart(s string) string {
	s = strings.ReplaceAll(s, string(os.PathSeparator), "_")
	s = strings.ReplaceAll(s, "..", "_")
	if s == "" {
		return "unknown"
	}
	return s
}

func zipDir(srcDir, zipPath, skipBase string) (int64, error) {
	out, err := os.Create(zipPath)
	if err != nil {
		return 0, err
	}
	defer out.Close()

	zw := zip.NewWriter(out)
	defer zw.Close()

	var total int64
	err = filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if filepath.Base(path) == skipBase {
			return nil
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)

		fh, err := zw.Create(rel)
		if err != nil {
			return err
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		n, copyErr := io.Copy(fh, in)
		in.Close()
		total += n
		return copyErr
	})
	if err != nil {
		return 0, err
	}
	if err := zw.Close(); err != nil {
		return 0, err
	}
	info, err := os.Stat(zipPath)
	if err != nil {
		return total, err
	}
	return info.Size(), nil
}
