package registry

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/request"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	versioned "github.com/argoproj/argo-cd/v3/pkg/client/clientset/versioned"
	appfake "github.com/argoproj/argo-cd/v3/pkg/client/clientset/versioned/fake"
	"github.com/argoproj/argo-cd/v3/server/aggregator/clusters"
	"github.com/argoproj/argo-cd/v3/util/db"
)

// fakeDB embeds the ArgoDB interface and implements just the cluster lookups the registry uses.
type fakeDB struct {
	db.ArgoDB
	clusters []appv1.Cluster
}

func (f *fakeDB) ListClusters(context.Context) (*appv1.ClusterList, error) {
	return &appv1.ClusterList{Items: f.clusters}, nil
}

func (f *fakeDB) GetCluster(_ context.Context, server string) (*appv1.Cluster, error) {
	for i := range f.clusters {
		if f.clusters[i].Server == server {
			return &f.clusters[i], nil
		}
	}
	return nil, fmt.Errorf("cluster %q not found", server)
}

func (f *fakeDB) GetClusterServersByName(_ context.Context, name string) ([]string, error) {
	for _, c := range f.clusters {
		if c.Name == name {
			return []string{c.Server}, nil
		}
	}
	return nil, nil
}

func app(namespace, name string) *appv1.Application {
	return &appv1.Application{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
}

// testRegistry wires a registry over two fake clusters with the given seeded clientsets.
func testRegistry(prod, staging versioned.Interface) *clusters.Registry {
	fdb := &fakeDB{clusters: []appv1.Cluster{
		{Name: "prod", Server: "https://prod"},
		{Name: "staging", Server: "https://staging"},
	}}
	clients := map[string]versioned.Interface{"https://prod": prod, "https://staging": staging}
	return clusters.NewRegistry(fdb).WithClientFactory(func(c *appv1.Cluster) (versioned.Interface, error) {
		client, ok := clients[c.Server]
		if !ok {
			return nil, fmt.Errorf("no fake client for %q", c.Server)
		}
		return client, nil
	})
}

func TestStoreListFanOut(t *testing.T) {
	prod := appfake.NewSimpleClientset(app("argocd", "guestbook"), app("team-a", "web"))
	staging := appfake.NewSimpleClientset(app("argocd", "api"))
	store := NewStore(applicationResource{}, testRegistry(prod, staging))

	ctx := request.WithNamespace(context.Background(), metav1.NamespaceAll)
	obj, err := store.List(ctx, &metainternalversion.ListOptions{})
	require.NoError(t, err)

	list, ok := obj.(*appv1.ApplicationList)
	require.True(t, ok)
	require.Len(t, list.Items, 3)

	// Every aggregated item is rewritten into a synthetic namespace and carries source labels.
	byName := map[string]appv1.Application{}
	for _, item := range list.Items {
		byName[item.Name] = item
		assert.NotEmpty(t, item.Labels[clusters.LabelSourceCluster])
		assert.NotEmpty(t, item.Labels[clusters.LabelSourceNamespace])
		assert.Equal(t, clusters.SyntheticNamespace(item.Labels[clusters.LabelSourceCluster], item.Labels[clusters.LabelSourceNamespace]), item.Namespace)
	}
	assert.Equal(t, "prod", byName["web"].Labels[clusters.LabelSourceCluster])
	assert.Equal(t, "team-a", byName["web"].Labels[clusters.LabelSourceNamespace])
	assert.Equal(t, "staging", byName["api"].Labels[clusters.LabelSourceCluster])
}

func TestStoreGetAfterList(t *testing.T) {
	prod := appfake.NewSimpleClientset(app("team-a", "web"))
	staging := appfake.NewSimpleClientset()
	registry := testRegistry(prod, staging)
	store := NewStore(applicationResource{}, registry)

	// List first to populate the reverse map.
	_, err := store.List(request.WithNamespace(context.Background(), metav1.NamespaceAll), &metainternalversion.ListOptions{})
	require.NoError(t, err)

	synthetic := clusters.SyntheticNamespace("prod", "team-a")
	got, err := store.Get(request.WithNamespace(context.Background(), synthetic), "web", &metav1.GetOptions{})
	require.NoError(t, err)
	gotApp := got.(*appv1.Application)
	assert.Equal(t, "web", gotApp.Name)
	assert.Equal(t, synthetic, gotApp.Namespace)
	assert.Equal(t, "prod", gotApp.Labels[clusters.LabelSourceCluster])
}

func TestStoreCreateRoutesDownstream(t *testing.T) {
	prod := appfake.NewSimpleClientset()
	staging := appfake.NewSimpleClientset()
	store := NewStore(applicationResource{}, testRegistry(prod, staging))

	synthetic := clusters.SyntheticNamespace("prod", "team-a")
	newApp := &appv1.Application{ObjectMeta: metav1.ObjectMeta{
		Namespace: synthetic,
		Name:      "newapp",
		Labels: map[string]string{
			clusters.LabelSourceCluster:   "prod",
			clusters.LabelSourceNamespace: "team-a",
		},
	}}

	ctx := request.WithNamespace(context.Background(), synthetic)
	out, err := store.Create(ctx, newApp, nilValidate, &metav1.CreateOptions{})
	require.NoError(t, err)
	assert.Equal(t, synthetic, out.(*appv1.Application).Namespace)

	// The object actually landed on the prod cluster in the real downstream namespace.
	down, err := prod.ArgoprojV1alpha1().Applications("team-a").Get(context.Background(), "newapp", metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, "newapp", down.Name)

	// And not on staging.
	_, err = staging.ArgoprojV1alpha1().Applications("team-a").Get(context.Background(), "newapp", metav1.GetOptions{})
	require.Error(t, err)
}

func TestStoreDeleteRoutesDownstream(t *testing.T) {
	prod := appfake.NewSimpleClientset(app("team-a", "web"))
	staging := appfake.NewSimpleClientset()
	store := NewStore(applicationResource{}, testRegistry(prod, staging))

	// Populate the reverse map.
	_, err := store.List(request.WithNamespace(context.Background(), metav1.NamespaceAll), &metainternalversion.ListOptions{})
	require.NoError(t, err)

	synthetic := clusters.SyntheticNamespace("prod", "team-a")
	_, deleted, err := store.Delete(request.WithNamespace(context.Background(), synthetic), "web", nil, &metav1.DeleteOptions{})
	require.NoError(t, err)
	assert.True(t, deleted)

	_, err = prod.ArgoprojV1alpha1().Applications("team-a").Get(context.Background(), "web", metav1.GetOptions{})
	require.Error(t, err)
}

func nilValidate(_ context.Context, _ runtime.Object) error { return nil }
