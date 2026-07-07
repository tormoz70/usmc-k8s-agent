package file

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/usmc/k8s-agent/internal/command"
	"github.com/usmc/k8s-agent/internal/config"
	"github.com/usmc/k8s-agent/internal/k8s"
	"github.com/usmc/k8s-agent/internal/local"
	"github.com/usmc/k8s-agent/internal/policy"
	"github.com/usmc/k8s-agent/internal/result"
	"github.com/usmc/k8s-agent/internal/s3"
	"sigs.k8s.io/yaml"
)

type FetchHandler struct {
	lister     *k8s.Lister
	logReader  *k8s.LogReader
	policy     *policy.Engine
	uploader   *s3.Uploader
	cfg        *config.Config
	progressFn func(*command.Result) error
}

func NewFetchHandler(lister *k8s.Lister, logReader *k8s.LogReader, pe *policy.Engine, uploader *s3.Uploader, cfg *config.Config, progressFn func(*command.Result) error) *FetchHandler {
	return &FetchHandler{
		lister:     lister,
		logReader:  logReader,
		policy:     pe,
		uploader:   uploader,
		cfg:        cfg,
		progressFn: progressFn,
	}
}

func (h *FetchHandler) Type() string {
	return command.TypeFileFetch
}

type fetchPayload struct {
	Source          string          `json:"source"`
	SourceParams    json.RawMessage `json:"source_params"`
	Destination     destination     `json:"destination"`
	LocalProcessing []string        `json:"local_processing"`
}

type destination struct {
	PresignedPutURL string `json:"presigned_put_url"`
	ContentType     string `json:"content_type"`
	ObjectKey       string `json:"object_key"`
	S3URI           string `json:"s3_uri"`
}

type exportParams struct {
	GVK           command.GVK `json:"gvk"`
	Namespace     string      `json:"namespace"`
	LabelSelector string      `json:"labelSelector"`
	Namespaces    []string    `json:"namespaces"`
}

type logsBatchParams struct {
	Targets      []k8s.LogTarget    `json:"targets"`
	SinceSeconds *int64             `json:"since_seconds"`
	TailLines    *int64             `json:"tail_lines"`
	LimitBytes   *int64             `json:"limit_bytes"`
}

type partialError struct {
	Namespace string `json:"namespace,omitempty"`
	Pod       string `json:"pod,omitempty"`
	Container string `json:"container,omitempty"`
	Error     string `json:"error"`
}

func (h *FetchHandler) Handle(ctx context.Context, cmd command.Command) (*command.Result, error) {
	var payload fetchPayload
	if err := json.Unmarshal(cmd.Payload, &payload); err != nil {
		return result.Rejected(cmd, "INVALID_PAYLOAD", err.Error()), nil
	}
	if payload.Destination.PresignedPutURL == "" {
		return result.Rejected(cmd, "INVALID_PAYLOAD", "presigned_put_url is required"), nil
	}

	host, err := s3.HostFromPresignedURL(payload.Destination.PresignedPutURL)
	if err != nil {
		return result.Rejected(cmd, "INVALID_PAYLOAD", err.Error()), nil
	}
	if err := h.policy.CheckPresignedHost(host); err != nil {
		return result.Rejected(cmd, "POLICY_VIOLATION", err.Error()), nil
	}

	switch payload.Source {
	case "resource_export":
		return h.exportResources(ctx, cmd, payload)
	case "pod_logs_batch":
		return h.batchLogs(ctx, cmd, payload)
	default:
		return result.Rejected(cmd, "INVALID_PAYLOAD", fmt.Sprintf("unsupported source: %s", payload.Source)), nil
	}
}

func (h *FetchHandler) exportResources(ctx context.Context, cmd command.Command, payload fetchPayload) (*command.Result, error) {
	var params exportParams
	if err := json.Unmarshal(payload.SourceParams, &params); err != nil {
		return result.Rejected(cmd, "INVALID_PAYLOAD", err.Error()), nil
	}
	if err := h.policy.CheckGVK(params.GVK); err != nil {
		return result.Rejected(cmd, "POLICY_VIOLATION", err.Error()), nil
	}

	tmp, err := local.NewTempDir(h.cfg.TempDir)
	if err != nil {
		return result.Failed(cmd, "TEMP_DIR", err.Error()), nil
	}
	defer tmp.Cleanup()

	items, err := h.lister.ListAllPages(ctx, k8s.ListOptions{
		GVK:           params.GVK,
		Namespace:     params.Namespace,
		LabelSelector: params.LabelSelector,
		Namespaces:    params.Namespaces,
		StripStatus:   true,
	})
	if err != nil {
		return result.Failed(cmd, "LIST_FAILED", err.Error()), nil
	}

	dataDir := filepath.Join(tmp.Path, "data")
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return result.Failed(cmd, "TEMP_DIR", err.Error()), nil
	}

	for i, obj := range items {
		ns := obj.GetNamespace()
		if ns == "" {
			ns = "_cluster"
		}
		rel := filepath.ToSlash(filepath.Join(ns, obj.GetName()+".yaml"))
		b, err := yaml.Marshal(k8s.SanitizeObject(&obj, k8s.SanitizeOptions{StripStatus: true}).Object)
		if err != nil {
			return result.Failed(cmd, "MARSHAL_FAILED", err.Error()), nil
		}
		if err := local.WriteFile(dataDir, rel, b); err != nil {
			return result.Failed(cmd, "WRITE_FAILED", err.Error()), nil
		}
		if h.progressFn != nil && len(items) > 0 {
			res := result.NewResult(cmd, command.StatusExecuting)
			res.Phase = command.StatusExecuting
			res.Progress = (i + 1) * 100 / len(items)
			_ = h.progressFn(res)
		}
	}

	archivePath := filepath.Join(tmp.Path, "export.tar.gz")
	if err := local.TarGzDirectory(dataDir, archivePath); err != nil {
		return result.Failed(cmd, "ARCHIVE_FAILED", err.Error()), nil
	}

	size, err := local.FileSize(archivePath)
	if err != nil {
		return result.Failed(cmd, "STAT_FAILED", err.Error()), nil
	}
	if h.policy.Policy().FileFetch.MaxBytes > 0 && size > h.policy.Policy().FileFetch.MaxBytes {
		return result.Failed(cmd, "SIZE_LIMIT", fmt.Sprintf("export size %d exceeds limit", size)), nil
	}

	f, err := local.OpenFile(archivePath)
	if err != nil {
		return result.Failed(cmd, "OPEN_FAILED", err.Error()), nil
	}
	defer f.Close()

	contentType := payload.Destination.ContentType
	if contentType == "" {
		contentType = "application/gzip"
	}
	_, err = s3.UploadWithRetry(ctx, h.uploader, payload.Destination.PresignedPutURL, contentType, f, h.cfg.MaxRetries, h.cfg.RetryBaseDelay)
	if err != nil {
		code := "UPLOAD_FAILED"
		if strings.Contains(err.Error(), "PRESIGNED_URL_EXPIRED") {
			code = "PRESIGNED_URL_EXPIRED"
		}
		return result.Failed(cmd, code, err.Error()), nil
	}

	details := map[string]any{
		"s3_uri":         payload.Destination.S3URI,
		"object_key":     payload.Destination.ObjectKey,
		"bytes":          size,
		"resource_count": len(items),
	}
	now := time.Now().UTC()
	res := result.NewResult(cmd, command.StatusCompleted)
	res.FinishedAt = &now
	b, _ := json.Marshal(details)
	res.Details = b
	return res, nil
}

func (h *FetchHandler) batchLogs(ctx context.Context, cmd command.Command, payload fetchPayload) (*command.Result, error) {
	var params logsBatchParams
	if err := json.Unmarshal(payload.SourceParams, &params); err != nil {
		return result.Rejected(cmd, "INVALID_PAYLOAD", err.Error()), nil
	}
	if len(params.Targets) == 0 {
		return result.Rejected(cmd, "INVALID_PAYLOAD", "targets required"), nil
	}

	maxConc := h.policy.Policy().FileFetch.MaxConcurrentTargets
	if maxConc <= 0 {
		maxConc = 10
	}

	tmp, err := local.NewTempDir(h.cfg.TempDir)
	if err != nil {
		return result.Failed(cmd, "TEMP_DIR", err.Error()), nil
	}
	defer tmp.Cleanup()

	logsDir := filepath.Join(tmp.Path, "logs")
	if err := os.MkdirAll(logsDir, 0o755); err != nil {
		return result.Failed(cmd, "TEMP_DIR", err.Error()), nil
	}

	var (
		mu            sync.Mutex
		partialErrors []partialError
		fileCount     int
	)

	sem := make(chan struct{}, maxConc)
	var wg sync.WaitGroup
	opts := k8s.LogFetchOptions{
		SinceSeconds: params.SinceSeconds,
		TailLines:    params.TailLines,
		LimitBytes:   params.LimitBytes,
	}

	for _, target := range params.Targets {
		if err := h.policy.CheckNamespace(target.Namespace); err != nil {
			return result.Rejected(cmd, "POLICY_VIOLATION", err.Error()), nil
		}
		wg.Add(1)
		go func(t k8s.LogTarget) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			data, err := h.logReader.Fetch(ctx, t, opts)
			if err != nil {
				mu.Lock()
				partialErrors = append(partialErrors, partialError{
					Namespace: t.Namespace,
					Pod:       t.Pod,
					Container: t.Container,
					Error:     err.Error(),
				})
				mu.Unlock()
				return
			}

			deployment := h.logReader.ResolveDeploymentName(ctx, t.Namespace, t.Pod)
			state := "current"
			if t.Previous {
				state = "previous"
			}
			ts := k8s.TimestampForFilename()
			rel := filepath.ToSlash(filepath.Join(deployment, t.Pod, fmt.Sprintf("%s-%s-%s.log", t.Container, state, ts)))
			if err := local.WriteFile(logsDir, rel, data); err != nil {
				mu.Lock()
				partialErrors = append(partialErrors, partialError{Pod: t.Pod, Container: t.Container, Error: err.Error()})
				mu.Unlock()
				return
			}
			mu.Lock()
			fileCount++
			mu.Unlock()
		}(target)
	}
	wg.Wait()

	zipPath := filepath.Join(tmp.Path, "logs.zip")
	if err := local.ZipDirectory(logsDir, zipPath); err != nil {
		return result.Failed(cmd, "ARCHIVE_FAILED", err.Error()), nil
	}

	size, err := local.FileSize(zipPath)
	if err != nil {
		return result.Failed(cmd, "STAT_FAILED", err.Error()), nil
	}

	f, err := os.Open(zipPath)
	if err != nil {
		return result.Failed(cmd, "OPEN_FAILED", err.Error()), nil
	}
	defer f.Close()

	contentType := payload.Destination.ContentType
	if contentType == "" {
		contentType = "application/zip"
	}
	_, err = s3.UploadWithRetry(ctx, h.uploader, payload.Destination.PresignedPutURL, contentType, f, h.cfg.MaxRetries, h.cfg.RetryBaseDelay)
	if err != nil {
		code := "UPLOAD_FAILED"
		if strings.Contains(err.Error(), "PRESIGNED_URL_EXPIRED") {
			code = "PRESIGNED_URL_EXPIRED"
		}
		return result.Failed(cmd, code, err.Error()), nil
	}

	details := map[string]any{
		"s3_uri":         payload.Destination.S3URI,
		"object_key":     payload.Destination.ObjectKey,
		"bytes":          size,
		"file_count":     fileCount,
		"partial_errors": partialErrors,
	}
	now := time.Now().UTC()
	res := result.NewResult(cmd, command.StatusCompleted)
	res.FinishedAt = &now
	b, _ := json.Marshal(details)
	res.Details = b
	return res, nil
}
