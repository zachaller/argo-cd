package aggregated

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apiserver/pkg/authentication/user"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	views "github.com/argoproj/argo-cd/v3/pkg/apis/views/v1alpha1"
)

func requestContext(namespace string, u user.Info) context.Context {
	ctx := genericapirequest.WithNamespace(context.Background(), namespace)
	return genericapirequest.WithUser(ctx, u)
}

func TestGetTree(t *testing.T) {
	guestbook := newApp(testNamespace, "guestbook", "default", map[string]string{"team": "web", trackingLabelKey: "parent"})
	f := newFixture(t, guestbook)
	f.setTree(t, guestbook, "cm-1", "cm-2")
	trees := newREST(treeKind, f.proj, f.authz, f.hub)

	obj, err := trees.Get(requestContext(testNamespace, alice), "guestbook", &metav1.GetOptions{})
	require.NoError(t, err)
	tree := obj.(*views.ApplicationTree)

	assert.Equal(t, "guestbook", tree.Name)
	assert.Equal(t, testNamespace, tree.Namespace)
	assert.Equal(t, guestbook.UID, tree.UID)
	assert.Equal(t, map[string]string{"team": "web"}, tree.Labels, "the tracking label must not be copied")
	assert.Empty(t, tree.Annotations, "annotations must not be copied")
	assert.Empty(t, tree.OwnerReferences)
	assert.NotEmpty(t, tree.ResourceVersion)
	assert.Len(t, tree.Tree.Nodes, 2)
	assert.Equal(t, views.ProjectionStatus{
		Project:    "default",
		Conditions: []views.ProjectionCondition{{Type: views.ConditionTypeReady, Status: metav1.ConditionTrue, Reason: views.ReasonReconciled}},
	}, tree.Status)

	t.Run("resourceVersion is a content hash", func(t *testing.T) {
		again, err := trees.Get(requestContext(testNamespace, alice), "guestbook", &metav1.GetOptions{})
		require.NoError(t, err)
		assert.Equal(t, tree.ResourceVersion, again.(*views.ApplicationTree).ResourceVersion)

		f.setTree(t, guestbook, "cm-1")
		changed, err := trees.Get(requestContext(testNamespace, alice), "guestbook", &metav1.GetOptions{})
		require.NoError(t, err)
		assert.NotEqual(t, tree.ResourceVersion, changed.(*views.ApplicationTree).ResourceVersion)
	})
}

func TestGetNotReconciled(t *testing.T) {
	guestbook := newApp(testNamespace, "guestbook", "default", nil)
	broken := newApp(testNamespace, "broken", "default", nil)
	broken.Status.Conditions = []appv1.ApplicationCondition{{Type: appv1.ApplicationConditionComparisonError, Message: "repository not found"}}
	f := newFixture(t, guestbook, broken)
	trees := newREST(treeKind, f.proj, f.authz, f.hub)

	obj, err := trees.Get(requestContext(testNamespace, alice), "guestbook", &metav1.GetOptions{})
	require.NoError(t, err)
	tree := obj.(*views.ApplicationTree)
	assert.Empty(t, tree.Tree.Nodes)
	require.Len(t, tree.Status.Conditions, 1)
	assert.Equal(t, metav1.ConditionFalse, tree.Status.Conditions[0].Status)
	assert.Equal(t, views.ReasonNotReconciled, tree.Status.Conditions[0].Reason)

	obj, err = trees.Get(requestContext(testNamespace, alice), "broken", &metav1.GetOptions{})
	require.NoError(t, err)
	tree = obj.(*views.ApplicationTree)
	require.Len(t, tree.Status.Conditions, 1)
	assert.Equal(t, views.ReasonComparisonError, tree.Status.Conditions[0].Reason)
	assert.Contains(t, tree.Status.Conditions[0].Message, "repository not found")
}

type failingCache struct{}

func (failingCache) GetAppResourcesTree(string, *appv1.ApplicationTree) error {
	return errors.New("redis is down")
}

func (failingCache) GetAppManagedResources(string, *[]*appv1.ResourceDiff) error {
	return errors.New("redis is down")
}

func TestGetCacheUnavailable(t *testing.T) {
	f := newFixture(t, newApp(testNamespace, "guestbook", "default", nil))
	f.proj.cache = failingCache{}
	_, err := newREST(treeKind, f.proj, f.authz, f.hub).Get(requestContext(testNamespace, alice), "guestbook", &metav1.GetOptions{})
	assert.True(t, apierrors.IsServiceUnavailable(err), "unexpected error: %v", err)
}

func TestGetAccess(t *testing.T) {
	f := newFixture(t,
		newApp(testNamespace, "guestbook", "default", nil),
		newApp(testNamespace, "vault", "secret", nil),
		newApp(otherNamespace, "team-app", "default", nil),
		newApp("not-enabled", "other", "default", nil),
	)
	trees := newREST(treeKind, f.proj, f.authz, f.hub)

	tests := []struct {
		name      string
		namespace string
		app       string
		user      user.Info
		found     bool
	}{
		{"allowed by user policy", testNamespace, "guestbook", alice, true},
		{"denied by project", testNamespace, "vault", alice, false},
		{"allowed by group", testNamespace, "vault", bob, true},
		{"denied without policy", testNamespace, "guestbook", mallet, false},
		{"anonymous", testNamespace, "guestbook", nil, false},
		{"application namespace", otherNamespace, "team-app", alice, true},
		{"namespace not enabled", "not-enabled", "other", bob, false},
		{"missing application", testNamespace, "missing", bob, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := genericapirequest.WithNamespace(context.Background(), tt.namespace)
			if tt.user != nil {
				ctx = genericapirequest.WithUser(ctx, tt.user)
			}
			obj, err := trees.Get(ctx, tt.app, &metav1.GetOptions{})
			if tt.found {
				require.NoError(t, err)
				assert.Equal(t, tt.app, obj.(*views.ApplicationTree).Name)
			} else {
				// Denied and missing applications are indistinguishable.
				assert.True(t, apierrors.IsNotFound(err), "unexpected error: %v", err)
			}
		})
	}
}

func TestGetDiff(t *testing.T) {
	guestbook := newApp(testNamespace, "guestbook", "default", nil)
	f := newFixture(t, guestbook)
	f.setManagedResources(t, guestbook,
		&appv1.ResourceDiff{Kind: "ConfigMap", Name: "cm-1", Modified: true},
		&appv1.ResourceDiff{Kind: "Job", Name: "pre-sync", Hook: true},
		&appv1.ResourceDiff{Kind: "ConfigMap", Name: "cm-2"},
	)
	diffs := newREST(diffKind, f.proj, f.authz, f.hub)

	obj, err := diffs.Get(requestContext(testNamespace, alice), "guestbook", &metav1.GetOptions{})
	require.NoError(t, err)
	diff := obj.(*views.ApplicationDiff)
	require.Len(t, diff.Resources, 2, "hooks must be omitted")
	assert.Equal(t, "cm-1", diff.Resources[0].Name)
	assert.True(t, diff.Resources[0].Modified)
	assert.Equal(t, "cm-2", diff.Resources[1].Name)
	assert.Equal(t, views.ReasonReconciled, diff.Status.Conditions[0].Reason)

	table, err := diffs.ConvertToTable(context.Background(), diff, nil)
	require.NoError(t, err)
	require.Len(t, table.Rows, 1)
	assert.Equal(t, []any{"guestbook", "default", int64(2), int64(1), "True", "60m"}, table.Rows[0].Cells)
}

func TestList(t *testing.T) {
	guestbook := newApp(testNamespace, "guestbook", "default", map[string]string{"team": "web", trackingLabelKey: "parent"})
	vault := newApp(testNamespace, "vault", "secret", map[string]string{"team": "security"})
	teamApp := newApp(otherNamespace, "team-app", "default", map[string]string{"team": "web"})
	f := newFixture(t, guestbook, vault, teamApp, newApp("not-enabled", "other", "default", nil))
	for _, app := range []*appv1.Application{guestbook, vault, teamApp} {
		f.setTree(t, app, "cm")
	}
	trees := newREST(treeKind, f.proj, f.authz, f.hub)

	list := func(t *testing.T, namespace string, u user.Info, options *metainternalversion.ListOptions) []string {
		t.Helper()
		obj, err := trees.List(requestContext(namespace, u), options)
		require.NoError(t, err)
		var names []string
		for _, item := range obj.(*views.ApplicationTreeList).Items {
			names = append(names, item.Namespace+"/"+item.Name)
		}
		return names
	}

	assert.Equal(t, []string{"argocd/guestbook", "argocd/vault", "team-a/team-app"}, list(t, "", bob, nil))
	assert.Equal(t, []string{"argocd/guestbook", "team-a/team-app"}, list(t, "", alice, nil), "applications denied by Argo CD RBAC are omitted")
	assert.Equal(t, []string{"argocd/guestbook", "argocd/vault"}, list(t, testNamespace, bob, nil))
	assert.Empty(t, list(t, "", mallet, nil))
	assert.Empty(t, list(t, "not-enabled", bob, nil))

	webSelector := labels.SelectorFromSet(labels.Set{"team": "web"})
	assert.Equal(t, []string{"argocd/guestbook", "team-a/team-app"}, list(t, "", bob, &metainternalversion.ListOptions{LabelSelector: webSelector}))
	trackingSelector := labels.SelectorFromSet(labels.Set{trackingLabelKey: "parent"})
	assert.Empty(t, list(t, "", bob, &metainternalversion.ListOptions{LabelSelector: trackingSelector}), "the tracking label is not part of the projection")
	nameSelector := fields.OneTermEqualSelector("metadata.name", "vault")
	assert.Equal(t, []string{"argocd/vault"}, list(t, "", bob, &metainternalversion.ListOptions{FieldSelector: nameSelector}))

	_, err := trees.List(requestContext("", bob), &metainternalversion.ListOptions{FieldSelector: fields.OneTermEqualSelector("spec.project", "default")})
	assert.True(t, apierrors.IsBadRequest(err), "unexpected error: %v", err)

	t.Run("list resourceVersion is remembered for watches", func(t *testing.T) {
		obj, err := trees.List(requestContext("", bob), nil)
		require.NoError(t, err)
		treeList := obj.(*views.ApplicationTreeList)
		snapshot, ok := f.hub.snapshot(treeKind, treeList.ResourceVersion)
		require.True(t, ok)
		assert.Len(t, snapshot, 3)
		assert.Equal(t, treeList.Items[0].ResourceVersion, snapshot["argocd/guestbook"])

		table, err := trees.ConvertToTable(context.Background(), treeList, nil)
		require.NoError(t, err)
		assert.Len(t, table.Rows, 3)
		assert.Equal(t, treeList.ResourceVersion, table.ResourceVersion)
		assert.Equal(t, []any{"guestbook", "default", int64(1), int64(0), "True", "60m"}, table.Rows[0].Cells)
	})
}

func TestArgoRBACAuthorizerClaims(t *testing.T) {
	f := newFixture(t)
	app := newApp(testNamespace, "guestbook", "default", nil)

	t.Run("Kubernetes users never match Argo CD local users", func(t *testing.T) {
		require.NoError(t, f.enf.SetUserPolicy(testPolicy+"\ng, admin, role:admin\np, role:admin, applications, get, */*, allow"))
		assert.False(t, f.authz.canGet(&user.DefaultInfo{Name: "admin"}, app))
		assert.False(t, f.authz.canGet(&user.DefaultInfo{Name: "mallet", Groups: []string{"admin"}}, app))
	})

	t.Run("project token subjects are rejected", func(t *testing.T) {
		unprefixed := &argoRBACAuthorizer{enf: f.enf, namespace: testNamespace}
		_, ok := unprefixed.claimsFor(&user.DefaultInfo{Name: "proj:default:ci"})
		assert.False(t, ok)
	})

	t.Run("prefix is applied to users and groups", func(t *testing.T) {
		claims, ok := f.authz.claimsFor(&user.DefaultInfo{Name: "system:serviceaccount:ci:deployer", Groups: []string{"system:authenticated"}})
		require.True(t, ok)
		assert.Equal(t, "k8s:system:serviceaccount:ci:deployer", claims["sub"])
		assert.Equal(t, []any{"k8s:system:authenticated"}, claims["groups"])
	})
}

func TestAppKeyFromInstanceName(t *testing.T) {
	assert.Equal(t, "argocd/guestbook", appKeyFromInstanceName("guestbook", testNamespace))
	assert.Equal(t, "team-a/guestbook", appKeyFromInstanceName("team-a_guestbook", testNamespace))
}
