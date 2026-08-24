package e2e

import (
	"fmt"
	"testing"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/health"
	. "github.com/argoproj/argo-cd/gitops-engine/v3/pkg/sync/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/argoproj/argo-cd/v3/common"
	. "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/argoproj/argo-cd/v3/test/e2e/fixture"
	. "github.com/argoproj/argo-cd/v3/test/e2e/fixture/app"
	clusterFixture "github.com/argoproj/argo-cd/v3/test/e2e/fixture/cluster"
	"github.com/argoproj/argo-cd/v3/util/errors"
)

const (
	multiDestinationPath           = "multi-destination"
	multiDestinationUndeclaredPath = "multi-destination-undeclared"
	// Both manifests here are named shared-cm. Routed to different destinations but into the same
	// namespace name, they differ only by destination -- the case a resource-level request cannot
	// resolve without being told which one it means.
	multiDestinationDuplicatePath = "multi-destination-duplicate"
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
		// Wait for the sync operation itself to finish, but not for the Application to report
		// Synced. And() runs immediately while Expect() polls, so asserting placement without this
		// would check for resources the sync had not applied yet.
		Expect(OperationPhaseIs(OperationSucceeded)).
		// Where the resources landed is then asserted before convergence, and read from each
		// cluster's own API rather than from what Argo CD reports about itself. It is the fact this
		// test exists to establish, and putting it after an expectation that waits for convergence
		// would lose it entirely whenever convergence is what is broken.
		// Every diagnostic below runs before anything can fail the test. An earlier version put a
		// require.NoError among the placement assertions, which aborted the test at the first
		// disagreement and threw away the logging that would have explained it.
		And(func(app *Application) {
			// Where each manifest actually is, in both clusters. Reading only the cluster a
			// resource was meant for cannot distinguish "never applied" from "applied and then
			// removed" from "applied to the wrong cluster", and the sync log shows each ConfigMap
			// being created and then pruned within one operation.
			t.Logf("second cluster  %s/second-cm:  %s", secondDestinationNamespace, second.DescribeConfigMap(t, secondDestinationNamespace, "second-cm"))
			t.Logf("second cluster  %s/primary-cm: %s", secondDestinationNamespace, second.DescribeConfigMap(t, secondDestinationNamespace, "primary-cm"))
			t.Logf("primary cluster %s/primary-cm: %s", ctx.DeploymentNamespace(), describePrimaryConfigMap(t, ctx.DeploymentNamespace(), "primary-cm"))
			t.Logf("primary cluster %s/second-cm:  %s", secondDestinationNamespace, describePrimaryConfigMap(t, secondDestinationNamespace, "second-cm"))

			// What the comparison attributes to which destination.
			for _, res := range app.Status.Resources {
				t.Logf("compared %s/%s destination=%q status=%s health=%v",
					res.Namespace, res.Name, res.Destination, res.Status, res.Health)
			}
			// What the operation did, per destination. A resource that appears twice here -- once
			// synced and once pruned -- is the shape of the failure being chased.
			if opState := app.Status.OperationState; opState != nil && opState.SyncResult != nil {
				for _, res := range opState.SyncResult.Resources {
					t.Logf("synced %s/%s destination=%q status=%s hookPhase=%s message=%q",
						res.Namespace, res.Name, res.Destination, res.Status, res.HookPhase, res.Message)
				}
			}
			for _, cond := range app.Status.Conditions {
				t.Logf("condition %s: %s", cond.Type, cond.Message)
			}
		}).
		And(func(_ *Application) {
			assert.True(t, second.HasConfigMap(t, secondDestinationNamespace, "second-cm"),
				"the annotated manifest must land in the second destination's cluster")
			assert.False(t, second.HasConfigMap(t, secondDestinationNamespace, "primary-cm"),
				"an unannotated manifest must not land in the second destination")

			_, err := fixture.KubeClientset.CoreV1().ConfigMaps(ctx.DeploymentNamespace()).Get(t.Context(), "primary-cm", metav1.GetOptions{})
			assert.NoError(t, err, "the unannotated manifest must land in the primary destination")
		}).
		Expect(SyncStatusIs(SyncStatusCodeSynced)).
		Expect(HealthIs(health.HealthStatusHealthy))
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
		// The API server refuses the create outright, so there is no Application to carry a
		// condition -- the rejection is only visible in what the CLI reports.
		AndCLIOutput(func(_ string, err error) {
			require.Error(t, err)
			assert.Contains(t, err.Error(), "must be a different cluster")
		})
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
		AndCLIOutput(func(_ string, err error) {
			require.Error(t, err)
			assert.Contains(t, err.Error(), "application.destinations.enabled")
		})
}

// describePrimaryConfigMap reports whether a ConfigMap exists in the cluster Argo CD runs in and,
// if it does, the resource-tracking annotation on it. It is the primary-cluster counterpart of
// SecondCluster.DescribeConfigMap, so a test can say which cluster a resource reached rather than
// only whether the cluster it was meant for has it.
func describePrimaryConfigMap(t *testing.T, namespace, name string) string {
	t.Helper()
	cm, err := fixture.KubeClientset.CoreV1().ConfigMaps(namespace).Get(t.Context(), name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return "absent"
	}
	if err != nil {
		return "error: " + err.Error()
	}
	return fmt.Sprintf("present tracking-id=%q label-instance=%q",
		cm.Annotations[common.AnnotationKeyAppInstance], cm.Labels[common.LabelKeyAppInstance])
}

// TestMultiDestinationAddressDuplicateResource covers the case the destination selector exists for:
// the same group, kind, namespace and name in two of an Application's destinations. Reaching either
// copy needs the request to name a destination, because nothing else distinguishes them.
//
// The two destinations deliberately share a namespace NAME here. Everywhere else in this file they
// differ, which keeps those tests honest about placement but means a lookup by namespace alone is
// already unambiguous -- so none of them exercise this.
func TestMultiDestinationAddressDuplicateResource(t *testing.T) {
	// Order matters: Given empties argocd-cm as part of cleaning state, so the feature gate has to
	// be set after it, and the second cluster registered without cleaning state again.
	ctx := Given(t)
	enableMultiDestination(t)
	second := clusterFixture.EnsureSecondCluster(t, ctx)
	// The same namespace name in the second cluster as the Application deploys to in the primary.
	sharedNamespace := ctx.DeploymentNamespace()
	second.RecreateNamespace(t, sharedNamespace)

	ctx.
		Path(multiDestinationDuplicatePath).
		When().
		CreateApp("--dest", fmt.Sprintf("name=second,server=%s,namespace=%s", second.Server, sharedNamespace)).
		Sync().
		Then().
		Expect(OperationPhaseIs(OperationSucceeded)).
		Expect(SyncStatusIs(SyncStatusCodeSynced)).
		And(func(_ *Application) {
			// Both copies exist, in different clusters, under the same namespace and name.
			primary, err := fixture.KubeClientset.CoreV1().ConfigMaps(sharedNamespace).Get(t.Context(), "shared-cm", metav1.GetOptions{})
			require.NoError(t, err, "the unannotated manifest belongs in the primary destination")
			assert.Equal(t, "primary", primary.Data["destination"])

			other, err := second.Client.CoreV1().ConfigMaps(sharedNamespace).Get(t.Context(), "shared-cm", metav1.GetOptions{})
			require.NoError(t, err, "the annotated manifest belongs in the second destination")
			assert.Equal(t, "second", other.Data["destination"])
		}).
		And(func(app *Application) {
			appName := app.Name

			// Reading without a destination returns every copy: the command walks the resource tree
			// and asks for each node by its own destination, so there is nothing to disambiguate.
			out, err := fixture.RunCli("app", "get-resource", appName, "--kind", "ConfigMap", "--resource-name", "shared-cm", "-o", "yaml")
			require.NoError(t, err)
			assert.Contains(t, out, "destination: primary")
			assert.Contains(t, out, "destination: second")

			// Naming one narrows it to that destination.
			out, err = fixture.RunCli("app", "get-resource", appName, "--kind", "ConfigMap", "--resource-name", "shared-cm", "--destination", "second", "-o", "yaml")
			require.NoError(t, err)
			assert.Contains(t, out, "destination: second")
			assert.NotContains(t, out, "destination: primary")

			// The primary destination has no name of its own, so it is addressed by the selector.
			out, err = fixture.RunCli("app", "get-resource", appName, "--kind", "ConfigMap", "--resource-name", "shared-cm", "--destination", PrimaryDestinationSelector, "-o", "yaml")
			require.NoError(t, err)
			assert.Contains(t, out, "destination: primary")
			assert.NotContains(t, out, "destination: second")
		}).
		And(func(app *Application) {
			appName := app.Name

			// Writing is where the ambiguity has to be refused. Without a destination the command
			// cannot choose, and --all is not the answer: it would patch the copy in both clusters.
			_, err := fixture.RunCli("app", "patch-resource", appName, "--kind", "ConfigMap", "--resource-name", "shared-cm",
				"--patch", `{"data":{"patched":"yes"}}`, "--patch-type", "application/merge-patch+json")
			require.Error(t, err, "two copies match, so the command must not pick one")
			assert.Contains(t, err.Error(), "multiple resources match")

			// With a destination it patches exactly that copy.
			_, err = fixture.RunCli("app", "patch-resource", appName, "--kind", "ConfigMap", "--resource-name", "shared-cm",
				"--destination", "second",
				"--patch", `{"data":{"patched":"yes"}}`, "--patch-type", "application/merge-patch+json")
			require.NoError(t, err)

			other, err := second.Client.CoreV1().ConfigMaps(sharedNamespace).Get(t.Context(), "shared-cm", metav1.GetOptions{})
			require.NoError(t, err)
			assert.Equal(t, "yes", other.Data["patched"], "the named destination's copy is the one that changed")

			primary, err := fixture.KubeClientset.CoreV1().ConfigMaps(sharedNamespace).Get(t.Context(), "shared-cm", metav1.GetOptions{})
			require.NoError(t, err)
			assert.NotContains(t, primary.Data, "patched", "the other cluster's copy must be untouched")
		})
}
