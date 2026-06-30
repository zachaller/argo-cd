package clusters

import (
	"context"
	"fmt"
	"sync"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	versioned "github.com/argoproj/argo-cd/v3/pkg/client/clientset/versioned"
	"github.com/argoproj/argo-cd/v3/util/db"
)

// Registry tracks the set of downstream clusters the aggregator fans out to, lazily builds
// and caches a typed argoproj clientset per cluster, and maintains the authoritative
// synthetic-namespace reverse map used to route reads and writes.
type Registry struct {
	db db.ArgoDB

	mu      sync.Mutex
	clients map[string]versioned.Interface // keyed by downstream server URL

	reverse sync.Map // synthetic namespace -> Target

	// newClient is overridable in tests so a fake clientset can be injected per cluster.
	newClient func(cluster *appv1.Cluster) (versioned.Interface, error)
}

// NewRegistry returns a cluster registry backed by the given Argo database.
func NewRegistry(argoDB db.ArgoDB) *Registry {
	return &Registry{
		db:        argoDB,
		clients:   map[string]versioned.Interface{},
		newClient: defaultNewClient,
	}
}

// WithClientFactory overrides how downstream clientsets are built. It is primarily intended
// for tests (to inject fake clientsets) but also allows customizing the downstream transport.
func (r *Registry) WithClientFactory(factory func(cluster *appv1.Cluster) (versioned.Interface, error)) *Registry {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.newClient = factory
	r.clients = map[string]versioned.Interface{}
	return r
}

// defaultNewClient builds a typed argoproj clientset from a downstream cluster's credentials.
func defaultNewClient(cluster *appv1.Cluster) (versioned.Interface, error) {
	restConfig, err := cluster.RESTConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to build rest config for cluster %q: %w", cluster.Name, err)
	}
	return versioned.NewForConfig(restConfig)
}

// Clusters returns the current set of downstream clusters from the Argo cluster secrets.
func (r *Registry) Clusters(ctx context.Context) ([]appv1.Cluster, error) {
	list, err := r.db.ListClusters(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list clusters: %w", err)
	}
	return list.Items, nil
}

// ClientForCluster returns a cached clientset for the given cluster, creating it on first use.
func (r *Registry) ClientForCluster(cluster *appv1.Cluster) (versioned.Interface, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if client, ok := r.clients[cluster.Server]; ok {
		return client, nil
	}
	client, err := r.newClient(cluster)
	if err != nil {
		return nil, err
	}
	r.clients[cluster.Server] = client
	return client, nil
}

// ClientForServer returns a cached clientset for the cluster identified by its server URL.
func (r *Registry) ClientForServer(ctx context.Context, server string) (versioned.Interface, error) {
	r.mu.Lock()
	if client, ok := r.clients[server]; ok {
		r.mu.Unlock()
		return client, nil
	}
	r.mu.Unlock()
	cluster, err := r.db.GetCluster(ctx, server)
	if err != nil {
		return nil, fmt.Errorf("failed to get cluster %q: %w", server, err)
	}
	return r.ClientForCluster(cluster)
}

// serverForCluster resolves a downstream cluster name to its API server URL.
func (r *Registry) serverForCluster(ctx context.Context, name string) (string, error) {
	servers, err := r.db.GetClusterServersByName(ctx, name)
	if err != nil {
		return "", fmt.Errorf("failed to resolve cluster name %q: %w", name, err)
	}
	if len(servers) == 0 {
		return "", fmt.Errorf("no downstream cluster registered with name %q", name)
	}
	return servers[0], nil
}

// Invalidate drops the cached clientset for a server URL so it is rebuilt on next use, e.g.
// after a cluster secret rotation.
func (r *Registry) Invalidate(server string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, server)
}
