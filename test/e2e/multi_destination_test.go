package e2e

import (
	"fmt"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	. "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/argoproj/argo-cd/v3/test/e2e/fixture"
	. "github.com/argoproj/argo-cd/v3/test/e2e/fixture/app"
	clusterFixture "github.com/argoproj/argo-cd/v3/test/e2e/fixture/cluster"
	"github.com/argoproj/argo-cd/v3/util/errors"
)

const (
	multiDestinationPath           = "multi-destination"
	multiDestinationUndeclaredPath = "multi-destination-undeclared"
	// The namespace in the second cluster. It is deliberately not the deployment namespace of the
	// primary destination: a test that used the same name in both clusters could not tell a
	// resource that landed in the right place from one that landed in the wrong cluster.
	secondDestinationNamespace = "argocd-e2e-second-dest"
)

// enableMultiDestination turns the feature on for the duration of the test. It is off by default, so
// an Application declaring spec.destinations is rejected until an operator opts in.
func enableMultiDestination(t *testing.T) {
	t.Helper()
	errors.NewHandler(t).FailOnErr(nil, fixture.SetParamInSettingConfigMap("application.destinations.enabled", "true"))
	t.Cleanup(func() {
		_ = fixture.SetParamInSettingConfigMap("application.destinations.enabled", "false")
	})
}

// TestMultiDestinationPlacement is the test the whole feature exists for: a manifest annotated for a
// named destination has to land in that destination's cluster, and an unannotated one in the
// primary. Asserting it needs two genuinely separate clusters, because Argo CD reads live objects
// once per cluster -- two destinations backed by the same API server would each see the other's
// resources.
func TestMultiDestinationPlacement(t *testing.T) {
	// Order matters: Given empties argocd-cm as part of cleaning state, so the feature gate has to
	// be set after it, and the second cluster registered without cleaning state again.
	ctx := Given(t)
	enableMultiDestination(t)
	second := clusterFixture.EnsureSecondCluster(t, ctx)
	second.RecreateNamespace(t, secondDestinationNamespace)

	ctx.
		Path(multiDestinationPath).
		When().
		CreateApp("--dest", fmt.Sprintf("name=second,server=%s,namespace=%s", second.Server, secondDestinationNamespace)).
		Then().
		And(func(app *Application) {
			require.Len(t, app.Spec.Destinations, 1)
			assert.Equal(t, "second", app.Spec.Destinations[0].Name)
			assert.Equal(t, second.Server, app.Spec.Destinations[0].Server)
		}).
		When().
		Sync().
		Then().
		Expect(SyncStatusIs(SyncStatusCodeSynced)).
		Expect(HealthIs(health.HealthStatusHealthy)).
		And(func(_ *Application) {
			// The annotated ConfigMap is in the second cluster and nowhere else.
			assert.True(t, second.HasConfigMap(t, secondDestinationNamespace, "second-cm"),
				"the annotated manifest must land in the second destination's cluster")
			assert.False(t, second.HasConfigMap(t, secondDestinationNamespace, "primary-cm"),
				"an unannotated manifest must not land in the second destination")
		}).
		And(func(_ *Application) {
			// The unannotated ConfigMap is in the primary destination.
			_, err := fixture.KubeClientset.CoreV1().ConfigMaps(ctx.DeploymentNamespace()).Get(t.Context(), "primary-cm", metav1.GetOptions{})
			require.NoError(t, err, "the unannotated manifest must land in the primary destination")
		})
}

// TestMultiDestinationStatusIsPerDestination checks that the resources an Application reports carry
// the destination they belong to, which is what the UI and CLI group on.
func TestMultiDestinationStatusIsPerDestination(t *testing.T) {
	// Order matters: Given empties argocd-cm as part of cleaning state, so the feature gate has to
	// be set after it, and the second cluster registered without cleaning state again.
	ctx := Given(t)
	enableMultiDestination(t)
	second := clusterFixture.EnsureSecondCluster(t, ctx)
	second.RecreateNamespace(t, secondDestinationNamespace)

	ctx.
		Path(multiDestinationPath).
		When().
		CreateApp("--dest", fmt.Sprintf("name=second,server=%s,namespace=%s", second.Server, secondDestinationNamespace)).
		Sync().
		Then().
		Expect(SyncStatusIs(SyncStatusCodeSynced)).
		And(func(app *Application) {
			byName := map[string]string{}
			for _, res := range app.Status.Resources {
				byName[res.Name] = res.Destination
			}
			assert.Equal(t, "second", byName["second-cm"], "the annotated resource belongs to the named destination")
			assert.Empty(t, byName["primary-cm"], "an unannotated resource belongs to the primary destination")
		})
}

// TestMultiDestinationUndeclaredDestinationFailsApp checks that a manifest naming a destination the
// Application does not declare fails the Application rather than being deployed somewhere. Routing
// it to the primary destination would put a resource in a cluster nobody asked for.
func TestMultiDestinationUndeclaredDestinationFailsApp(t *testing.T) {
	ctx := Given(t)
	enableMultiDestination(t)
	second := clusterFixture.EnsureSecondCluster(t, ctx)

	ctx.
		Path(multiDestinationUndeclaredPath).
		When().
		IgnoreErrors().
		CreateApp("--dest", fmt.Sprintf("name=second,server=%s,namespace=%s", second.Server, secondDestinationNamespace)).
		Then().
		Expect(Condition(ApplicationConditionInvalidSpecError, "nowhere"))
}

// TestMultiDestinationSameClusterRejected checks the distinctness rule. Two destinations on one
// cluster cannot be told apart when live objects are read, so each would treat the other's resources
// as extras. This needs only one cluster, so it runs in any e2e environment.
func TestMultiDestinationSameClusterRejected(t *testing.T) {
	ctx := Given(t)
	enableMultiDestination(t)

	ctx.
		Path(guestbookPath).
		When().
		IgnoreErrors().
		CreateApp("--dest", fmt.Sprintf("name=same,server=%s,namespace=%s", KubernetesInternalAPIServerAddr, secondDestinationNamespace)).
		Then().
		Expect(Condition(ApplicationConditionInvalidSpecError, "must be a different cluster"))
}

// TestMultiDestinationDisabledByDefault checks the feature gate: without the argocd-cm opt in, an
// Application may not declare named destinations at all. This needs only one cluster.
func TestMultiDestinationDisabledByDefault(t *testing.T) {
	Given(t).
		Path(guestbookPath).
		When().
		IgnoreErrors().
		CreateApp("--dest", fmt.Sprintf("name=second,server=%s,namespace=%s", KubernetesInternalAPIServerAddr, secondDestinationNamespace)).
		Then().
		Expect(Condition(ApplicationConditionInvalidSpecError, "application.destinations.enabled"))
}
