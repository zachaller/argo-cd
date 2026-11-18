package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic/fake"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	kubetesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"

	"github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
)

func TestIsAppSyncStatusRefreshed(t *testing.T) {
	logger, _ := test.NewNullLogger()
	logEntry := logger.WithField("", "")

	tests := []struct {
		name          string
		app           *unstructured.Unstructured
		expectedValue bool
	}{
		{
			name: "No OperationState",
			app: &unstructured.Unstructured{
				Object: map[string]any{
					"status": map[string]any{},
				},
			},
			expectedValue: true,
		},
		{
			name: "No FinishedAt, Completed Phase",
			app: &unstructured.Unstructured{
				Object: map[string]any{
					"status": map[string]any{
						"operationState": map[string]any{
							"phase": "Succeeded",
						},
					},
				},
			},
			expectedValue: false,
		},
		{
			name: "FinishedAt After ReconciledAt & ObservedAt",
			app: &unstructured.Unstructured{
				Object: map[string]any{
					"status": map[string]any{
						"operationState": map[string]any{
							"finishedAt": "2021-01-01T01:05:00Z",
							"phase":      "Succeeded",
						},
						"reconciledAt": "2021-01-01T01:02:00Z",
						"observedAt":   "2021-01-01T01:04:00Z",
					},
				},
			},
			expectedValue: false,
		},
		{
			name: "FinishedAt Before ReconciledAt & ObservedAt",
			app: &unstructured.Unstructured{
				Object: map[string]any{
					"status": map[string]any{
						"operationState": map[string]any{
							"finishedAt": "2021-01-01T01:02:00Z",
							"phase":      "Succeeded",
						},
						"reconciledAt": "2021-01-01T01:04:00Z",
						"observedAt":   "2021-01-01T01:06:00Z",
					},
				},
			},
			expectedValue: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actualValue := isAppSyncStatusRefreshed(test.app, logEntry)
			assert.Equal(t, test.expectedValue, actualValue)
		})
	}
}

func TestGetAppProj_invalidProjectNestedString(t *testing.T) {
	app := &unstructured.Unstructured{
		Object: map[string]any{
			"spec": map[string]any{},
		},
	}
	informer := cache.NewSharedIndexInformer(nil, nil, 0, nil)
	proj := getAppProj(app, informer, "argocd")

	assert.Nil(t, proj)
}

func TestGetAppProj_appInDifferentNamespace(t *testing.T) {
	// AppProject lives in the controller namespace...
	proj := &unstructured.Unstructured{}
	proj.SetGroupVersionKind(v1alpha1.AppProjectSchemaGroupVersionKind)
	proj.SetName("my-proj")
	proj.SetNamespace("argocd")

	informer := cache.NewSharedIndexInformer(&cache.ListWatch{}, &unstructured.Unstructured{}, 0,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, informer.GetIndexer().Add(proj))

	// ...while the application lives in a different (app) namespace. Keying on the
	// app namespace would miss the cache; the lookup must use the controller namespace.
	app := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{"name": "my-app", "namespace": "some-app-ns"},
			"spec":     map[string]any{"project": "my-proj"},
		},
	}

	result := getAppProj(app, informer, "argocd")
	require.NotNil(t, result)
	assert.Equal(t, "my-proj", result.GetName())
	assert.Equal(t, "argocd", result.GetNamespace())
}

func TestGetAppProj_defaultsToDefaultProject(t *testing.T) {
	// The 'default' project lives in the controller namespace.
	proj := &unstructured.Unstructured{}
	proj.SetGroupVersionKind(v1alpha1.AppProjectSchemaGroupVersionKind)
	proj.SetName("default")
	proj.SetNamespace("argocd")

	informer := cache.NewSharedIndexInformer(&cache.ListWatch{}, &unstructured.Unstructured{}, 0,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, informer.GetIndexer().Add(proj))

	// An app with no spec.project belongs to the 'default' project.
	app := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{"name": "my-app", "namespace": "argocd"},
			"spec":     map[string]any{},
		},
	}

	result := getAppProj(app, informer, "argocd")
	require.NotNil(t, result)
	assert.Equal(t, "default", result.GetName())
}

func TestGetAppProj_doesNotMutateCachedObject(t *testing.T) {
	// Informer cache objects are shared among goroutines; getAppProj must
	// return a copy so callers can't mutate the indexed AppProject.
	proj := &unstructured.Unstructured{}
	proj.SetGroupVersionKind(v1alpha1.AppProjectSchemaGroupVersionKind)
	proj.SetName("my-proj")
	proj.SetNamespace("argocd")

	informer := cache.NewSharedIndexInformer(&cache.ListWatch{}, &unstructured.Unstructured{}, 0,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	require.NoError(t, informer.GetIndexer().Add(proj))

	app := &unstructured.Unstructured{
		Object: map[string]any{
			"metadata": map[string]any{"name": "my-app", "namespace": "argocd"},
			"spec":     map[string]any{"project": "my-proj"},
		},
	}

	result := getAppProj(app, informer, "argocd")
	require.NotNil(t, result)
	assert.NotSame(t, proj, result)

	// Mutating the returned object must not leak into the indexed object.
	result.SetAnnotations(map[string]string{"mutated": "true"})

	cached, ok, err := informer.GetIndexer().GetByKey("argocd/my-proj")
	require.NoError(t, err)
	require.True(t, ok)
	assert.Nil(t, cached.(*unstructured.Unstructured).GetAnnotations(), "indexed AppProject must remain unchanged")
}

func TestInit(t *testing.T) {
	scheme := runtime.NewScheme()
	err := v1alpha1.SchemeBuilder.AddToScheme(scheme)
	require.NoErrorf(t, err, "Error registering the resource")
	dynamicClient := fake.NewSimpleDynamicClient(scheme)
	k8sClient := k8sfake.NewSimpleClientset()
	appLabelSelector := "app=test"

	selfServiceNotificationEnabledFlags := []bool{false, true}
	for _, selfServiceNotificationEnabled := range selfServiceNotificationEnabledFlags {
		nc := NewController(
			k8sClient,
			dynamicClient,
			nil,
			"default",
			[]string{},
			appLabelSelector,
			nil,
			"my-secret",
			"my-configmap",
			selfServiceNotificationEnabled,
		)

		assert.NotNil(t, nc)

		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()

		err = nc.Init(ctx)

		require.NoError(t, err)
	}
}

func TestInitTimeout(t *testing.T) {
	scheme := runtime.NewScheme()
	err := v1alpha1.SchemeBuilder.AddToScheme(scheme)
	require.NoErrorf(t, err, "Error registering the resource")
	dynamicClient := fake.NewSimpleDynamicClient(scheme)
	k8sClient := k8sfake.NewSimpleClientset()
	appLabelSelector := "app=test"

	nc := NewController(
		k8sClient,
		dynamicClient,
		nil,
		"default",
		[]string{},
		appLabelSelector,
		nil,
		"my-secret",
		"my-configmap",
		false,
	)

	assert.NotNil(t, nc)

	// Fail every list so the informers never sync and Init can only time out.
	// Otherwise the fake informers can sync before WaitForCacheSync first checks them.
	failList := func(_ kubetesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("list failed")
	}
	dynamicClient.PrependReactor("list", "*", failList)
	k8sClient.PrependReactor("list", "*", failList)

	// Use a short timeout to simulate a timeout during cache synchronization
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Millisecond)
	defer cancel()

	err = nc.Init(ctx)

	// Expect an error & add assertion for the error message
	assert.EqualError(t, err, "timed out waiting for caches to sync")
}

func TestCheckAppNotInAdditionalNamespaces(t *testing.T) {
	app := &unstructured.Unstructured{
		Object: map[string]any{
			"spec": map[string]any{},
		},
	}
	namespace := "argocd"
	var applicationNamespaces []string
	applicationNamespaces = append(applicationNamespaces, "namespace1")
	applicationNamespaces = append(applicationNamespaces, "namespace2")

	// app is in same namespace as controller's namespace
	app.SetNamespace(namespace)
	assert.False(t, checkAppNotInAdditionalNamespaces(app, namespace, applicationNamespaces))

	// app is not in the namespace as controller's namespace, but it is in one of the applicationNamespaces
	app.SetNamespace("namespace2")
	assert.False(t, checkAppNotInAdditionalNamespaces(app, "", applicationNamespaces))

	// app is not in the namespace as controller's namespace, and it is not in any of the applicationNamespaces
	app.SetNamespace("namespace3")
	assert.True(t, checkAppNotInAdditionalNamespaces(app, "", applicationNamespaces))
}

// Applications from namespaces that are not enabled must stay out of the informer cache, whether they
// exist when the controller starts or are created later.
func TestNewController_AppInformerCachesOnlyEnabledNamespaces(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.SchemeBuilder.AddToScheme(scheme))
	app := func(namespace, name string) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{}
		obj.SetAPIVersion("argoproj.io/v1alpha1")
		obj.SetKind("Application")
		obj.SetNamespace(namespace)
		obj.SetName(name)
		return obj
	}
	dynamicClient := fake.NewSimpleDynamicClient(scheme, app("argocd", "control-plane"), app("team-a", "enabled"), app("team-b", "not-enabled"))
	nc := NewController(k8sfake.NewSimpleClientset(), dynamicClient, nil, "argocd", []string{"team-a"}, "", nil, "my-secret", "my-configmap", false)

	require.NoError(t, nc.Init(t.Context()))
	assert.ElementsMatch(t, []string{"argocd/control-plane", "team-a/enabled"}, nc.appInformer.GetStore().ListKeys())

	// The application from the namespace that is not enabled is created first, so it has been through
	// the informer by the time the enabled one shows up.
	_, err := dynamicClient.Resource(applications).Namespace("team-b").Create(t.Context(), app("team-b", "not-enabled-later"), metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = dynamicClient.Resource(applications).Namespace("team-a").Create(t.Context(), app("team-a", "enabled-later"), metav1.CreateOptions{})
	require.NoError(t, err)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Contains(c, nc.appInformer.GetStore().ListKeys(), "team-a/enabled-later")
	}, 5*time.Second, 10*time.Millisecond)
	assert.ElementsMatch(t, []string{"argocd/control-plane", "team-a/enabled", "team-a/enabled-later"}, nc.appInformer.GetStore().ListKeys())
}

// Without application namespaces the controller only watches its own namespace.
func TestNewController_AppInformerWatchesOwnNamespaceByDefault(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1alpha1.SchemeBuilder.AddToScheme(scheme))
	app := func(namespace string) *unstructured.Unstructured {
		obj := &unstructured.Unstructured{}
		obj.SetAPIVersion("argoproj.io/v1alpha1")
		obj.SetKind("Application")
		obj.SetNamespace(namespace)
		obj.SetName("app")
		return obj
	}
	dynamicClient := fake.NewSimpleDynamicClient(scheme, app("argocd"), app("team-a"))
	nc := NewController(k8sfake.NewSimpleClientset(), dynamicClient, nil, "argocd", []string{}, "", nil, "my-secret", "my-configmap", false)

	require.NoError(t, nc.Init(t.Context()))
	assert.Equal(t, []string{"argocd/app"}, nc.appInformer.GetStore().ListKeys())
}
