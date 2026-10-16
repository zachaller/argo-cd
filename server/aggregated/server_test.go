package aggregated

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/authentication/authenticator"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/authorization/authorizerfactory"
	"k8s.io/client-go/rest"

	views "github.com/argoproj/argo-cd/v3/pkg/apis/views/v1alpha1"
)

const (
	testUserHeader  = "X-Test-User"
	testGroupHeader = "X-Test-Group"
)

// newTestServer serves the aggregated API over plain HTTP. Requests are authenticated from test headers instead of
// the front proxy headers set by kube-apiserver, and Kubernetes authorization allows everything, so that the tests
// exercise the Argo CD RBAC layer.
func newTestServer(t *testing.T, f *fixture) *httptest.Server {
	t.Helper()
	config := NewConfig()
	config.ExternalAddress = "127.0.0.1:443"
	config.LoopbackClientConfig = &rest.Config{Host: "127.0.0.1:443"}
	config.Authentication.Authenticator = authenticator.RequestFunc(func(req *http.Request) (*authenticator.Response, bool, error) {
		name := req.Header.Get(testUserHeader)
		if name == "" {
			return nil, false, nil
		}
		return &authenticator.Response{User: &user.DefaultInfo{Name: name, Groups: req.Header.Values(testGroupHeader)}}, true, nil
	})
	config.Authorization.Authorizer = authorizerfactory.NewAlwaysAllowAuthorizer()

	server, err := New(config, Dependencies{
		Namespace:             testNamespace,
		ApplicationNamespaces: []string{otherNamespace},
		AppLister:             f.proj.appLister,
		Cache:                 f.cache,
		Enforcer:              f.enf,
		SubjectPrefix:         DefaultSubjectPrefix,
		TrackingLabelKey:      f.proj.trackingLabelKey,
		HasSynced:             func() bool { return true },
	})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go server.RunWatchHub(ctx)
	// PrepareRun installs the OpenAPI and health endpoints.
	server.generic.PrepareRun()
	ts := httptest.NewServer(server.Handler())
	t.Cleanup(ts.Close)
	return ts
}

func get(t *testing.T, ts *httptest.Server, path, userName, accept string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+path, http.NoBody)
	require.NoError(t, err)
	if userName != "" {
		req.Header.Set(testUserHeader, userName)
	}
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	resp, err := ts.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, body
}

func TestServerHTTP(t *testing.T) {
	guestbook := newApp(testNamespace, "guestbook", "default", map[string]string{"team": "web"})
	f := newFixture(t, guestbook, newApp(testNamespace, "vault", "secret", nil))
	f.setTree(t, guestbook, "cm-1", "cm-2")
	ts := newTestServer(t, f)
	const base = "/apis/views.argoproj.io/v1alpha1"

	t.Run("discovery", func(t *testing.T) {
		code, body := get(t, ts, base, "alice", "")
		require.Equal(t, http.StatusOK, code, string(body))
		var resources metav1.APIResourceList
		require.NoError(t, json.Unmarshal(body, &resources))
		require.Len(t, resources.APIResources, 2)
		byName := map[string]metav1.APIResource{}
		for _, resource := range resources.APIResources {
			byName[resource.Name] = resource
		}
		tree := byName[views.ApplicationTreeResource]
		assert.Equal(t, views.ApplicationTreeKind, tree.Kind)
		assert.True(t, tree.Namespaced)
		assert.Equal(t, views.ApplicationTreeSingular, tree.SingularName)
		assert.Equal(t, []string{views.ApplicationTreeShort}, tree.ShortNames)
		assert.ElementsMatch(t, []string{"get", "list", "watch"}, []string(tree.Verbs))
		assert.Equal(t, views.ApplicationDiffKind, byName[views.ApplicationDiffResource].Kind)
	})

	t.Run("get", func(t *testing.T) {
		code, body := get(t, ts, base+"/namespaces/argocd/applicationtrees/guestbook", "alice", "")
		require.Equal(t, http.StatusOK, code, string(body))
		var tree views.ApplicationTree
		require.NoError(t, json.Unmarshal(body, &tree))
		assert.Equal(t, views.ApplicationTreeKind, tree.Kind)
		assert.Equal(t, "views.argoproj.io/v1alpha1", tree.APIVersion)
		assert.Len(t, tree.Tree.Nodes, 2)
		assert.Equal(t, "default", tree.Status.Project)
	})

	t.Run("get denied by Argo CD RBAC", func(t *testing.T) {
		code, body := get(t, ts, base+"/namespaces/argocd/applicationtrees/vault", "alice", "")
		assert.Equal(t, http.StatusNotFound, code, string(body))
	})

	t.Run("unauthenticated", func(t *testing.T) {
		code, _ := get(t, ts, base+"/namespaces/argocd/applicationtrees/guestbook", "", "")
		assert.Equal(t, http.StatusUnauthorized, code)
	})

	t.Run("list as table", func(t *testing.T) {
		code, body := get(t, ts, base+"/applicationtrees", "alice", "application/json;as=Table;v=v1;g=meta.k8s.io")
		require.Equal(t, http.StatusOK, code, string(body))
		var table metav1.Table
		require.NoError(t, json.Unmarshal(body, &table))
		require.Len(t, table.Rows, 1)
		assert.Equal(t, "guestbook", table.Rows[0].Cells[0])
		assert.Equal(t, "Nodes", table.ColumnDefinitions[2].Name)
	})

	t.Run("list with selectors", func(t *testing.T) {
		code, body := get(t, ts, base+"/namespaces/argocd/applicationdiffs?labelSelector=team%3Dweb&fieldSelector=metadata.name%3Dguestbook", "alice", "")
		require.Equal(t, http.StatusOK, code, string(body))
		var list views.ApplicationDiffList
		require.NoError(t, json.Unmarshal(body, &list))
		require.Len(t, list.Items, 1)
		assert.Equal(t, "guestbook", list.Items[0].Name)
		assert.Equal(t, views.ReasonNotReconciled, list.Items[0].Status.Conditions[0].Reason)
	})

	t.Run("watch", func(t *testing.T) {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, ts.URL+base+"/namespaces/argocd/applicationtrees?watch=true&fieldSelector=metadata.name%3Dguestbook", http.NoBody)
		require.NoError(t, err)
		req.Header.Set(testUserHeader, "alice")
		resp, err := ts.Client().Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		decoder := json.NewDecoder(bufio.NewReader(resp.Body))
		var event struct {
			Type   string                `json:"type"`
			Object views.ApplicationTree `json:"object"`
		}
		require.NoError(t, decoder.Decode(&event))
		assert.Equal(t, "ADDED", event.Type)
		assert.Equal(t, "guestbook", event.Object.Name)
		assert.Equal(t, views.ApplicationTreeKind, event.Object.Kind)

		// Publish trees until the hub, which subscribes asynchronously, delivers one.
		done := make(chan struct{})
		defer close(done)
		go func() {
			for i := 3; ; i++ {
				select {
				case <-done:
					return
				default:
				}
				f.setTree(t, guestbook, make([]string, i)...)
				select {
				case <-done:
					return
				case <-t.Context().Done():
					return
				case <-time.After(50 * time.Millisecond):
				}
			}
		}()
		require.NoError(t, decoder.Decode(&event))
		assert.Equal(t, "MODIFIED", event.Type)
		assert.GreaterOrEqual(t, len(event.Object.Tree.Nodes), 3)
	})

	t.Run("openapi v2", func(t *testing.T) {
		code, body := get(t, ts, "/openapi/v2", "alice", "application/json")
		require.Equal(t, http.StatusOK, code, string(body))
		var doc struct {
			Definitions map[string]struct {
				GVK []map[string]string `json:"x-kubernetes-group-version-kind"`
			} `json:"definitions"`
		}
		require.NoError(t, json.Unmarshal(body, &doc))
		tree, ok := doc.Definitions["io.argoproj.views.v1alpha1.ApplicationTree"]
		require.True(t, ok)
		assert.Equal(t, []map[string]string{{"group": views.Group, "version": views.Version, "kind": views.ApplicationTreeKind}}, tree.GVK)
		for name := range doc.Definitions {
			assert.NotContains(t, name, "/", "definition names must be REST friendly")
		}
	})

	t.Run("openapi v3", func(t *testing.T) {
		code, body := get(t, ts, "/openapi/v3/apis/views.argoproj.io/v1alpha1", "alice", "application/json")
		require.Equal(t, http.StatusOK, code, string(body))
		assert.Contains(t, string(body), "io.argoproj.views.v1alpha1.ApplicationDiff")
	})
}
