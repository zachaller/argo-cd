package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	apps "github.com/argoproj/argo-cd/v3/pkg/client/clientset/versioned/fake"
)

func cachedKeys(informer cache.SharedIndexInformer) []string {
	return informer.GetStore().ListKeys()
}

func Test_newApplicationInformers(t *testing.T) {
	t.Parallel()

	app := func(namespace, name string) *v1alpha1.Application {
		return &v1alpha1.Application{Namespace: namespace, Name: name}
	}
	appSet := func(namespace, name string) *v1alpha1.ApplicationSet {
		return &v1alpha1.ApplicationSet{Namespace: namespace, Name: name}
	}

	// Objects from namespaces that are not enabled must stay out of the caches, whether they exist
	// when the informers start or show up later.
	t.Run("caches only enabled namespaces when watching cluster-wide", func(t *testing.T) {
		t.Parallel()
		client := apps.NewSimpleClientset(
			app("argocd", "control-plane"), app("team-a", "exact"), app("dev-1", "pattern"), app("team-b", "not-enabled"),
			appSet("argocd", "control-plane"), appSet("team-a", "exact"), appSet("team-b", "not-enabled"),
		)
		appInformer, appsetInformer := newApplicationInformers(client, "argocd", []string{"team-a", "dev-*"})

		go appInformer.Run(t.Context().Done())
		go appsetInformer.Run(t.Context().Done())
		require.True(t, cache.WaitForCacheSync(t.Context().Done(), appInformer.HasSynced, appsetInformer.HasSynced))

		assert.ElementsMatch(t, []string{"argocd/control-plane", "team-a/exact", "dev-1/pattern"}, cachedKeys(appInformer))
		assert.ElementsMatch(t, []string{"argocd/control-plane", "team-a/exact"}, cachedKeys(appsetInformer))

		// The object from the namespace that is not enabled is created first, so it has been through
		// the informer by the time the enabled one shows up.
		_, err := client.ArgoprojV1alpha1().Applications("team-b").Create(t.Context(), app("team-b", "not-enabled-later"), metav1.CreateOptions{})
		require.NoError(t, err)
		_, err = client.ArgoprojV1alpha1().Applications("dev-2").Create(t.Context(), app("dev-2", "later"), metav1.CreateOptions{})
		require.NoError(t, err)
		_, err = client.ArgoprojV1alpha1().ApplicationSets("team-b").Create(t.Context(), appSet("team-b", "not-enabled-later"), metav1.CreateOptions{})
		require.NoError(t, err)
		_, err = client.ArgoprojV1alpha1().ApplicationSets("argocd").Create(t.Context(), appSet("argocd", "later"), metav1.CreateOptions{})
		require.NoError(t, err)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			assert.Contains(c, cachedKeys(appInformer), "dev-2/later")
			assert.Contains(c, cachedKeys(appsetInformer), "argocd/later")
		}, 5*time.Second, 10*time.Millisecond)
		assert.ElementsMatch(t, []string{"argocd/control-plane", "team-a/exact", "dev-1/pattern", "dev-2/later"}, cachedKeys(appInformer))
		assert.ElementsMatch(t, []string{"argocd/control-plane", "team-a/exact", "argocd/later"}, cachedKeys(appsetInformer))
	})

	t.Run("watches only the server namespace without application namespaces", func(t *testing.T) {
		t.Parallel()
		client := apps.NewSimpleClientset(
			app("argocd", "control-plane"), app("team-a", "elsewhere"),
			appSet("argocd", "control-plane"), appSet("team-a", "elsewhere"),
		)
		appInformer, appsetInformer := newApplicationInformers(client, "argocd", nil)

		go appInformer.Run(t.Context().Done())
		go appsetInformer.Run(t.Context().Done())
		require.True(t, cache.WaitForCacheSync(t.Context().Done(), appInformer.HasSynced, appsetInformer.HasSynced))

		assert.Equal(t, []string{"argocd/control-plane"}, cachedKeys(appInformer))
		assert.Equal(t, []string{"argocd/control-plane"}, cachedKeys(appsetInformer))
	})
}
