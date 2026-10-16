package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"

	. "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/argoproj/argo-cd/v3/test/e2e/fixture"
	. "github.com/argoproj/argo-cd/v3/test/e2e/fixture/app"
	"github.com/argoproj/argo-cd/v3/util/errors"
)

var (
	applicationTreesGVR = schema.GroupVersionResource{Group: "views.argoproj.io", Version: "v1alpha1", Resource: "applicationtrees"}
	applicationDiffsGVR = schema.GroupVersionResource{Group: "views.argoproj.io", Version: "v1alpha1", Resource: "applicationdiffs"}
)

// skipWithoutAggregatedAPI skips tests which need argocd-aggregated-apiserver (manifests/aggregated-api) to be installed
// and registered as an APIService of the test cluster.
func skipWithoutAggregatedAPI(t *testing.T) {
	t.Helper()
	if os.Getenv("ARGOCD_E2E_AGGREGATED_API") != "true" {
		t.Skip("requires argocd-aggregated-apiserver, set ARGOCD_E2E_AGGREGATED_API=true to run")
	}
}

func readyCondition(t *testing.T, obj *unstructured.Unstructured) string {
	t.Helper()
	conditions, _, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	require.NoError(t, err)
	for _, condition := range conditions {
		c := condition.(map[string]any)
		if c["type"] == "Ready" {
			return c["status"].(string)
		}
	}
	return ""
}

func TestAggregatedAPIViews(t *testing.T) {
	skipWithoutAggregatedAPI(t)
	// The test runs as a Kubernetes cluster admin, which Kubernetes RBAC allows. Allow it in Argo CD RBAC too.
	require.NoError(t, fixture.SetParamInRBACConfigMap("policy.csv", "g, k8s:system:authenticated, role:readonly"))

	ctx := Given(t)
	ctx.Path(guestbookPath).
		When().
		CreateApp().
		Sync().
		Then().
		Expect(SyncStatusIs(SyncStatusCodeSynced)).
		And(func(app *Application) {
			trees := fixture.DynamicClientset.Resource(applicationTreesGVR).Namespace(app.Namespace)
			diffs := fixture.DynamicClientset.Resource(applicationDiffsGVR).Namespace(app.Namespace)

			var tree *unstructured.Unstructured
			require.Eventually(t, func() bool {
				var err error
				tree, err = trees.Get(t.Context(), app.Name, metav1.GetOptions{})
				return err == nil && readyCondition(t, tree) == "True"
			}, time.Minute, time.Second)
			nodes, _, err := unstructured.NestedSlice(tree.Object, "tree", "nodes")
			require.NoError(t, err)
			kinds := map[string]bool{}
			for _, node := range nodes {
				kinds[node.(map[string]any)["kind"].(string)] = true
			}
			assert.True(t, kinds["Deployment"], "the tree must contain the guestbook Deployment")
			assert.True(t, kinds["Service"], "the tree must contain the guestbook Service")
			assert.Equal(t, app.UID, tree.GetUID())

			diff, err := diffs.Get(t.Context(), app.Name, metav1.GetOptions{})
			require.NoError(t, err)
			resources, _, err := unstructured.NestedSlice(diff.Object, "resources")
			require.NoError(t, err)
			assert.Len(t, resources, 2)

			_, err = trees.Get(t.Context(), "does-not-exist", metav1.GetOptions{})
			assert.True(t, apierrors.IsNotFound(err), "unexpected error: %v", err)

			// The projections are never part of an Application's resource tree.
			latest, err := fixture.AppClientset.ArgoprojV1alpha1().Applications(app.Namespace).Get(t.Context(), app.Name, metav1.GetOptions{})
			require.NoError(t, err)
			for _, res := range latest.Status.Resources {
				assert.NotEqual(t, "views.argoproj.io", res.Group)
			}
		})
}

func TestAggregatedAPIViewsWatch(t *testing.T) {
	skipWithoutAggregatedAPI(t)
	require.NoError(t, fixture.SetParamInRBACConfigMap("policy.csv", "g, k8s:system:authenticated, role:readonly"))

	ctx := Given(t)
	ctx.Path(guestbookPath).
		When().
		CreateApp().
		Sync().
		Then().
		Expect(SyncStatusIs(SyncStatusCodeSynced)).
		And(func(app *Application) {
			watchCtx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
			defer cancel()
			w, err := fixture.DynamicClientset.Resource(applicationDiffsGVR).Namespace(app.Namespace).Watch(watchCtx, metav1.ListOptions{
				FieldSelector: "metadata.name=" + app.Name,
			})
			require.NoError(t, err)
			defer w.Stop()

			// Drift the live state: the diff must report the Deployment as modified.
			errors.NewHandler(t).FailOnErr(fixture.KubeClientset.AppsV1().Deployments(ctx.DeploymentNamespace()).Patch(t.Context(),
				"guestbook-ui", types.MergePatchType, []byte(`{"spec": {"revisionHistoryLimit": 0}}`), metav1.PatchOptions{}))

			for {
				select {
				case <-watchCtx.Done():
					t.Fatal("timed out waiting for the modified diff")
				case ev, ok := <-w.ResultChan():
					require.True(t, ok, "watch closed unexpectedly")
					if ev.Type != watch.Added && ev.Type != watch.Modified {
						continue
					}
					obj := ev.Object.(*unstructured.Unstructured)
					resources, _, err := unstructured.NestedSlice(obj.Object, "resources")
					require.NoError(t, err)
					for _, res := range resources {
						r := res.(map[string]any)
						if modified, _ := r["modified"].(bool); r["kind"] == "Deployment" && modified {
							return
						}
					}
				}
			}
		})
}
