package clusters

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/argoproj/argo-cd/v3/util/db"
)

// fakeClusterDB embeds the ArgoDB interface (leaving all methods unimplemented) and overrides
// only the cluster-name lookup used by ResolveForWrite.
type fakeClusterDB struct {
	db.ArgoDB
	servers map[string][]string
}

func (f *fakeClusterDB) GetClusterServersByName(_ context.Context, name string) ([]string, error) {
	return f.servers[name], nil
}

func TestSyntheticNamespace(t *testing.T) {
	t.Run("deterministic", func(t *testing.T) {
		assert.Equal(t, SyntheticNamespace("prod", "argocd"), SyntheticNamespace("prod", "argocd"))
	})

	t.Run("distinguishes cluster and namespace", func(t *testing.T) {
		assert.NotEqual(t, SyntheticNamespace("prod", "argocd"), SyntheticNamespace("prod", "team-a"))
		assert.NotEqual(t, SyntheticNamespace("prod", "argocd"), SyntheticNamespace("staging", "argocd"))
	})

	t.Run("keeps cluster prefix readable", func(t *testing.T) {
		assert.True(t, strings.HasPrefix(SyntheticNamespace("prod", "argocd"), "prod-"))
	})

	t.Run("always a valid RFC1123 namespace even for long inputs", func(t *testing.T) {
		longCluster := strings.Repeat("a", 200)
		longNamespace := strings.Repeat("b", 200)
		got := SyntheticNamespace(longCluster, longNamespace)
		assert.LessOrEqual(t, len(got), validation.DNS1123LabelMaxLength)
		assert.Empty(t, validation.IsDNS1123Label(got))
	})

	t.Run("sanitizes invalid characters in cluster name", func(t *testing.T) {
		got := SyntheticNamespace("https://API.Example.com:6443", "argocd")
		assert.Empty(t, validation.IsDNS1123Label(got))
	})
}

func TestTagUntagRoundTrip(t *testing.T) {
	r := &Registry{}
	target := Target{Cluster: "prod", Server: "https://prod", Namespace: "team-a"}

	app := &appv1.Application{ObjectMeta: metav1.ObjectMeta{Name: "guestbook", Namespace: "team-a"}}
	r.Tag(app, target)

	synthetic := SyntheticNamespace("prod", "team-a")
	assert.Equal(t, synthetic, app.Namespace)
	assert.Equal(t, "prod", app.Labels[LabelSourceCluster])
	assert.Equal(t, "team-a", app.Labels[LabelSourceNamespace])

	// The reverse map resolves the synthetic namespace back to the downstream target.
	resolved, ok := r.Resolve(synthetic)
	require.True(t, ok)
	assert.Equal(t, target, resolved)

	Untag(app, target)
	assert.Equal(t, "team-a", app.Namespace)
	assert.NotContains(t, app.Labels, LabelSourceCluster)
	assert.NotContains(t, app.Labels, LabelSourceNamespace)
}

func TestResolveForWriteFromLabels(t *testing.T) {
	r := &Registry{}
	r.newClient = nil // unused

	app := &appv1.Application{ObjectMeta: metav1.ObjectMeta{
		Name:      "guestbook",
		Namespace: "prod-abcdef12",
		Labels: map[string]string{
			LabelSourceCluster:   "prod",
			LabelSourceNamespace: "team-a",
		},
	}}

	// Labels are authoritative; the server URL is resolved via the registry.
	r.db = &fakeClusterDB{servers: map[string][]string{"prod": {"https://prod"}}}
	target, err := r.ResolveForWrite(context.Background(), app.Namespace, app)
	require.NoError(t, err)
	assert.Equal(t, Target{Cluster: "prod", Server: "https://prod", Namespace: "team-a"}, target)
}

func TestResolveForWriteFallsBackToReverseMap(t *testing.T) {
	r := &Registry{}
	synthetic := SyntheticNamespace("prod", "team-a")
	r.reverse.Store(synthetic, Target{Cluster: "prod", Server: "https://prod", Namespace: "team-a"})

	app := &appv1.Application{ObjectMeta: metav1.ObjectMeta{Name: "guestbook", Namespace: synthetic}}
	target, err := r.ResolveForWrite(context.Background(), synthetic, app)
	require.NoError(t, err)
	assert.Equal(t, "team-a", target.Namespace)
}

func TestResolveForWriteUnknown(t *testing.T) {
	r := &Registry{}
	app := &appv1.Application{ObjectMeta: metav1.ObjectMeta{Name: "guestbook", Namespace: "unknown-12345678"}}
	_, err := r.ResolveForWrite(context.Background(), app.Namespace, app)
	require.Error(t, err)
}
