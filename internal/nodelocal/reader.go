// Package nodelocal reads container stdout/stderr from kubelet host paths.
package nodelocal

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

// ReadOptions control which slice of a container log file to return.
type ReadOptions struct {
	Namespace string
	Pod       string
	PodUID    string // optional; empty = match by ns_pod prefix
	Container string
	Previous  bool
	SinceTime *time.Time
	TailLines *int64
	LimitBytes *int64
}

// Reader resolves and reads CRI log files under PodLogsRoot (default /var/log/pods).
type Reader struct {
	Root string
}

func NewReader(root string) *Reader {
	if root == "" {
		root = "/var/log/pods"
	}
	return &Reader{Root: root}
}

var numberedLog = regexp.MustCompile(`^(\d+)\.log$`)

// Open returns a ReadCloser of decoded plain log lines for the requested container.
func (r *Reader) Open(ctx context.Context, opts ReadOptions) (io.ReadCloser, error) {
	path, err := r.resolveLogFile(opts)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	pr, pw := io.Pipe()
	go func() {
		defer f.Close()
		err := r.decodeTo(ctx, f, pw, opts)
		_ = pw.CloseWithError(err)
	}()
	return pr, nil
}

func (r *Reader) resolveLogFile(opts ReadOptions) (string, error) {
	if opts.Namespace == "" || opts.Pod == "" || opts.Container == "" {
		return "", fmt.Errorf("namespace, pod and container are required")
	}
	podDir, err := r.findPodDir(opts.Namespace, opts.Pod, opts.PodUID)
	if err != nil {
		return "", err
	}
	containerDir := filepath.Join(podDir, opts.Container)
	entries, err := os.ReadDir(containerDir)
	if err != nil {
		return "", fmt.Errorf("container logs %s: %w", opts.Container, err)
	}
	var nums []int
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := numberedLog.FindStringSubmatch(e.Name())
		if m == nil {
			continue
		}
		n, _ := strconv.Atoi(m[1])
		nums = append(nums, n)
	}
	if len(nums) == 0 {
		return "", fmt.Errorf("no log files for container %s in %s/%s", opts.Container, opts.Namespace, opts.Pod)
	}
	sort.Ints(nums)
	idx := len(nums) - 1
	if opts.Previous {
		if len(nums) < 2 {
			return "", fmt.Errorf("no previous log for container %s", opts.Container)
		}
		idx = len(nums) - 2
	}
	return filepath.Join(containerDir, fmt.Sprintf("%d.log", nums[idx])), nil
}

func (r *Reader) findPodDir(namespace, pod, uid string) (string, error) {
	if uid != "" {
		dir := filepath.Join(r.Root, fmt.Sprintf("%s_%s_%s", namespace, pod, uid))
		if st, err := os.Stat(dir); err == nil && st.IsDir() {
			return dir, nil
		}
		return "", fmt.Errorf("pod log dir not found for uid %s", uid)
	}
	prefix := fmt.Sprintf("%s_%s_", namespace, pod)
	entries, err := os.ReadDir(r.Root)
	if err != nil {
		return "", fmt.Errorf("read pod logs root: %w", err)
	}
	var matches []string
	for _, e := range entries {
		if e.IsDir() && strings.HasPrefix(e.Name(), prefix) {
			matches = append(matches, filepath.Join(r.Root, e.Name()))
		}
	}
	switch len(matches) {
	case 0:
		return "", fmt.Errorf("pod log dir not found for %s/%s", namespace, pod)
	case 1:
		return matches[0], nil
	default:
		// Prefer the most recently modified directory (likely current pod instance).
		sort.Slice(matches, func(i, j int) bool {
			si, _ := os.Stat(matches[i])
			sj, _ := os.Stat(matches[j])
			if si == nil || sj == nil {
				return matches[i] < matches[j]
			}
			return si.ModTime().After(sj.ModTime())
		})
		return matches[0], nil
	}
}

type criLine struct {
	Log    string `json:"log"`
	Stream string `json:"stream"`
	Time   string `json:"time"`
}

func (r *Reader) decodeTo(ctx context.Context, in io.Reader, out io.Writer, opts ReadOptions) error {
	scanner := bufio.NewScanner(in)
	// CRI lines can be large; raise buffer.
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	var lines []string
	var totalBytes int64
	for scanner.Scan() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		raw := scanner.Bytes()
		msg, ts, ok := parseCRILine(raw)
		if !ok {
			msg = string(raw)
		}
		if opts.SinceTime != nil && ts != nil && ts.Before(opts.SinceTime.UTC()) {
			continue
		}
		lines = append(lines, msg)
		totalBytes += int64(len(msg) + 1)
		if opts.LimitBytes != nil && totalBytes > *opts.LimitBytes {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if opts.TailLines != nil && *opts.TailLines >= 0 && int64(len(lines)) > *opts.TailLines {
		lines = lines[int64(len(lines))-*opts.TailLines:]
	}
	for _, line := range lines {
		if _, err := io.WriteString(out, line+"\n"); err != nil {
			return err
		}
	}
	return nil
}

func parseCRILine(raw []byte) (string, *time.Time, bool) {
	var c criLine
	if err := json.Unmarshal(raw, &c); err != nil {
		return "", nil, false
	}
	msg := strings.TrimRight(c.Log, "\r\n")
	if c.Time == "" {
		return msg, nil, true
	}
	t, err := time.Parse(time.RFC3339Nano, c.Time)
	if err != nil {
		return msg, nil, true
	}
	return msg, &t, true
}
