package k8s

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/discovery/cached/memory"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/restmapper"
	"k8s.io/client-go/tools/clientcmd"
)

type Clients struct {
	Dynamic    dynamic.Interface
	Kube       kubernetes.Interface
	Discovery  discovery.DiscoveryInterface
	RESTMapper meta.RESTMapper
	restConfig *rest.Config
	mapperMu   sync.RWMutex
}

func NewInCluster() (*Clients, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("in-cluster config: %w", err)
	}
	return newFromConfig(cfg)
}

func NewFromKubeconfig(path string) (*Clients, error) {
	if path == "" {
		home, _ := os.UserHomeDir()
		path = filepath.Join(home, ".kube", "config")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		return nil, fmt.Errorf("kubeconfig: %w", err)
	}
	return newFromConfig(cfg)
}

func newFromConfig(cfg *rest.Config) (*Clients, error) {
	dyn, err := dynamic.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("dynamic client: %w", err)
	}
	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("kubernetes client: %w", err)
	}
	disc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("discovery client: %w", err)
	}
	c := &Clients{
		Dynamic:    dyn,
		Kube:       kube,
		Discovery:  disc,
		restConfig: cfg,
	}
	c.resetMapper()
	return c, nil
}

func (c *Clients) resetMapper() {
	c.mapperMu.Lock()
	defer c.mapperMu.Unlock()
	cached := memory.NewMemCacheClient(c.Discovery)
	c.RESTMapper = restmapper.NewDeferredDiscoveryRESTMapper(cached)
}

func (c *Clients) RESTConfig() *rest.Config {
	return c.restConfig
}

func (c *Clients) ResetMapper() {
	c.resetMapper()
}

func (c *Clients) GetRESTMapper() meta.RESTMapper {
	c.mapperMu.RLock()
	defer c.mapperMu.RUnlock()
	return c.RESTMapper
}
