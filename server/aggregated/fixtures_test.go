package aggregated

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	"github.com/argoproj/argo-cd/v3/common"
	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	applisters "github.com/argoproj/argo-cd/v3/pkg/client/listers/application/v1alpha1"
	servercache "github.com/argoproj/argo-cd/v3/server/cache"
	"github.com/argoproj/argo-cd/v3/server/rbacpolicy"
	cacheutil "github.com/argoproj/argo-cd/v3/util/cache"
	appstatecache "github.com/argoproj/argo-cd/v3/util/cache/appstate"
	"github.com/argoproj/argo-cd/v3/util/rbac"
)

const (
	testNamespace    = "argocd"
	otherNamespace   = "team-a"
	trackingLabelKey = "app.kubernetes.io/instance"
)

// testPolicy grants alice access to the default project, the devs group read access to everything, and nothing to
// anybody else.
const testPolicy = `
p, k8s:alice, applications, get, default/*, allow
g, k8s:devs, role:readonly
`

var (
	alice  = &user.DefaultInfo{Name: "alice"}
	bob    = &user.DefaultInfo{Name: "bob", Groups: []string{"devs"}}
	mallet = &user.DefaultInfo{Name: "mallet"}
)

type fixture struct {
	appIndexer cache.Indexer
	stateCache *appstatecache.Cache
	cache      *servercache.Cache
	enf        *rbac.Enforcer
	proj       *projector
	authz      *argoRBACAuthorizer
	hub        *hub
}

func newFixture(t *testing.T, apps ...*appv1.Application) *fixture {
	t.Helper()
	kubeclientset := fake.NewClientset(&corev1.ConfigMap{
		Name: common.ArgoCDRBACConfigMapName, Namespace: testNamespace,
	})
	projIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	require.NoError(t, projIndexer.Add(&appv1.AppProject{
		Name: "default", Namespace: testNamespace,
		Spec: appv1.AppProjectSpec{SourceNamespaces: []string{"*"}},
	}))
	require.NoError(t, projIndexer.Add(&appv1.AppProject{
		Name: "secret", Namespace: testNamespace,
		Spec: appv1.AppProjectSpec{SourceNamespaces: []string{"*"}},
	}))
	enf := rbac.NewEnforcer(kubeclientset, testNamespace, common.ArgoCDRBACConfigMapName, nil)
	require.NoError(t, enf.SetBuiltinPolicy(`p, role:readonly, applications, get, */*, allow`))
	require.NoError(t, enf.SetUserPolicy(testPolicy))
	enf.SetDefaultRole("")
	enf.SetClaimsEnforcerFunc(rbacpolicy.NewRBACPolicyEnforcer(enf, applisters.NewAppProjectLister(projIndexer).AppProjects(testNamespace)).EnforceClaims)

	appIndexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc})
	for _, app := range apps {
		require.NoError(t, appIndexer.Add(app))
	}
	stateCache := appstatecache.NewCache(cacheutil.NewCache(cacheutil.NewInMemoryCache(time.Hour)), time.Hour)
	serverCache := servercache.NewCache(stateCache, time.Hour, time.Hour)

	proj := &projector{
		appLister:        applisters.NewApplicationLister(appIndexer),
		cache:            serverCache,
		namespace:        testNamespace,
		appNamespaces:    []string{otherNamespace},
		trackingLabelKey: func() string { return trackingLabelKey },
	}
	authz := &argoRBACAuthorizer{enf: enf, namespace: testNamespace, subjectPrefix: DefaultSubjectPrefix}
	return &fixture{
		appIndexer: appIndexer,
		stateCache: stateCache,
		cache:      serverCache,
		enf:        enf,
		proj:       proj,
		authz:      authz,
		hub:        newHub(proj, authz),
	}
}

func newApp(namespace, name, project string, labels map[string]string) *appv1.Application {
	return &appv1.Application{
		Name:              name,
		Namespace:         namespace,
		UID:               types.UID("uid-" + name),
		Labels:            labels,
		Annotations:       map[string]string{"argocd.argoproj.io/tracking-id": "parent:argoproj.io/Application:argocd/" + name},
		CreationTimestamp: metav1.NewTime(time.Now().Add(-time.Hour)),
		Spec:              appv1.ApplicationSpec{Project: project},
	}
}

func (f *fixture) setTree(t *testing.T, app *appv1.Application, nodeNames ...string) {
	t.Helper()
	tree := &appv1.ApplicationTree{}
	for _, name := range nodeNames {
		tree.Nodes = append(tree.Nodes, appv1.ResourceNode{Version: "v1", Kind: "ConfigMap", Namespace: "default", Name: name})
	}
	require.NoError(t, f.stateCache.SetAppResourcesTree(app.InstanceName(testNamespace), tree))
}

func (f *fixture) setManagedResources(t *testing.T, app *appv1.Application, diffs ...*appv1.ResourceDiff) {
	t.Helper()
	require.NoError(t, f.stateCache.SetAppManagedResources(app.InstanceName(testNamespace), diffs))
}
