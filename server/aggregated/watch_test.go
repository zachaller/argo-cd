package aggregated

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/watch"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	views "github.com/argoproj/argo-cd/v3/pkg/apis/views/v1alpha1"
)

type receivedEvent struct {
	eventType watch.EventType
	name      string
	nodes     int
	meta      metav1.Object
}

func nextEvent(t *testing.T, w watch.Interface) receivedEvent {
	t.Helper()
	select {
	case ev, ok := <-w.ResultChan():
		require.True(t, ok, "watch closed unexpectedly")
		received := receivedEvent{eventType: ev.Type}
		if tree, ok := ev.Object.(*views.ApplicationTree); ok {
			received.name = tree.Namespace + "/" + tree.Name
			received.nodes = len(tree.Tree.Nodes)
			received.meta = tree
		}
		return received
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watch event")
	}
	return receivedEvent{}
}

func expectNoEvent(t *testing.T, w watch.Interface) {
	t.Helper()
	select {
	case ev := <-w.ResultChan():
		t.Fatalf("unexpected watch event: %s %v", ev.Type, ev.Object)
	case <-time.After(100 * time.Millisecond):
	}
}

// startHub runs the hub of f with its in-memory cache notifications and returns the tree storage.
func startHub(t *testing.T, f *fixture) *REST {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go f.hub.run(ctx, f.cache)
	return newREST(treeKind, f.proj, f.authz, f.hub)
}

func startWatch(t *testing.T, trees *REST, namespace string, options *metainternalversion.ListOptions) watch.Interface {
	t.Helper()
	w, err := trees.Watch(requestContext(namespace, bob), options)
	require.NoError(t, err)
	t.Cleanup(w.Stop)
	return w
}

// waitForSubscription publishes a tree until the hub delivers it to w, i.e. until the hub subscribed to the cache.
func waitForSubscription(t *testing.T, f *fixture, w watch.Interface, app *appv1.Application) {
	t.Helper()
	for i := 1; ; i++ {
		f.setTree(t, app, make([]string, i)...)
		select {
		case ev := <-w.ResultChan():
			require.Equal(t, watch.Modified, ev.Type)
			return
		case <-time.After(50 * time.Millisecond):
			require.Less(t, i, 100, "hub did not subscribe to the cache")
		}
	}
}

func TestWatchDeliversChanges(t *testing.T) {
	guestbook := newApp(testNamespace, "guestbook", "default", nil)
	vault := newApp(testNamespace, "vault", "default", nil)
	f := newFixture(t, guestbook, vault)
	f.setTree(t, guestbook, "cm")
	trees := startHub(t, f)

	w := startWatch(t, trees, testNamespace, &metainternalversion.ListOptions{})
	first, second := nextEvent(t, w), nextEvent(t, w)
	assert.Equal(t, receivedEvent{eventType: watch.Added, name: "argocd/guestbook", nodes: 1, meta: first.meta}, first)
	assert.Equal(t, watch.Added, second.eventType)
	assert.Equal(t, "argocd/vault", second.name)
	expectNoEvent(t, w)

	waitForSubscription(t, f, w, guestbook)

	t.Run("unchanged state is not delivered again", func(t *testing.T) {
		require.NoError(t, f.cache.GetCache().NotifyUpdated("app|managed-resources|guestbook"))
		f.hub.queue.Add("argocd/guestbook")
		expectNoEvent(t, w)
	})

	t.Run("changed tree is delivered", func(t *testing.T) {
		f.setTree(t, vault, "a", "b", "c")
		ev := nextEvent(t, w)
		assert.Equal(t, watch.Modified, ev.eventType)
		assert.Equal(t, "argocd/vault", ev.name)
		assert.Equal(t, 3, ev.nodes)
	})

	t.Run("deleted application is delivered", func(t *testing.T) {
		require.NoError(t, f.appIndexer.Delete(vault))
		f.hub.eventHandler().OnDelete(vault)
		ev := nextEvent(t, w)
		assert.Equal(t, watch.Deleted, ev.eventType)
		assert.Equal(t, "argocd/vault", ev.name)
	})

	t.Run("new application is delivered", func(t *testing.T) {
		app := newApp(testNamespace, "new", "default", nil)
		require.NoError(t, f.appIndexer.Add(app))
		f.hub.eventHandler().OnAdd(app, false)
		ev := nextEvent(t, w)
		assert.Equal(t, watch.Added, ev.eventType)
		assert.Equal(t, "argocd/new", ev.name)
	})

	t.Run("applications in other namespaces are not delivered", func(t *testing.T) {
		app := newApp(otherNamespace, "elsewhere", "default", nil)
		require.NoError(t, f.appIndexer.Add(app))
		f.hub.eventHandler().OnAdd(app, false)
		expectNoEvent(t, w)
	})
}

func TestWatchFollowsVisibility(t *testing.T) {
	guestbook := newApp(testNamespace, "guestbook", "default", map[string]string{"team": "web"})
	f := newFixture(t, guestbook)
	trees := startHub(t, f)

	w, err := trees.Watch(requestContext(testNamespace, alice), &metainternalversion.ListOptions{})
	require.NoError(t, err)
	t.Cleanup(w.Stop)
	assert.Equal(t, watch.Added, nextEvent(t, w).eventType)

	t.Run("moving the application to a project the user may not read deletes it", func(t *testing.T) {
		updated := guestbook.DeepCopy()
		updated.Spec.Project = "secret"
		require.NoError(t, f.appIndexer.Update(updated))
		f.hub.eventHandler().OnUpdate(guestbook, updated)
		ev := nextEvent(t, w)
		assert.Equal(t, watch.Deleted, ev.eventType)
		assert.Equal(t, "argocd/guestbook", ev.name)

		require.NoError(t, f.appIndexer.Update(guestbook))
		f.hub.eventHandler().OnUpdate(updated, guestbook)
		assert.Equal(t, watch.Added, nextEvent(t, w).eventType)
	})

	t.Run("a policy change takes effect on the next change", func(t *testing.T) {
		require.NoError(t, f.enf.SetUserPolicy(""))
		f.hub.queue.Add("argocd/guestbook")
		assert.Equal(t, watch.Deleted, nextEvent(t, w).eventType)
	})
}

func TestWatchLabelSelector(t *testing.T) {
	guestbook := newApp(testNamespace, "guestbook", "default", map[string]string{"team": "web"})
	f := newFixture(t, guestbook)
	trees := startHub(t, f)

	selector := labels.SelectorFromSet(labels.Set{"team": "web"})
	w := startWatch(t, trees, "", &metainternalversion.ListOptions{LabelSelector: selector})
	assert.Equal(t, watch.Added, nextEvent(t, w).eventType)

	updated := guestbook.DeepCopy()
	updated.Labels = map[string]string{"team": "ops"}
	require.NoError(t, f.appIndexer.Update(updated))
	f.hub.eventHandler().OnUpdate(guestbook, updated)
	assert.Equal(t, watch.Deleted, nextEvent(t, w).eventType)
}

func TestWatchSendInitialEvents(t *testing.T) {
	f := newFixture(t, newApp(testNamespace, "a", "default", nil), newApp(testNamespace, "b", "default", nil))
	trees := startHub(t, f)

	sendInitialEvents := true
	w := startWatch(t, trees, "", &metainternalversion.ListOptions{SendInitialEvents: &sendInitialEvents, AllowWatchBookmarks: true})
	assert.Equal(t, "argocd/a", nextEvent(t, w).name)
	assert.Equal(t, "argocd/b", nextEvent(t, w).name)
	bookmark := nextEvent(t, w)
	assert.Equal(t, watch.Bookmark, bookmark.eventType)
	assert.Equal(t, "true", bookmark.meta.GetAnnotations()[metav1.InitialEventsAnnotationKey])

	t.Run("the bookmark resourceVersion resumes the watch", func(t *testing.T) {
		resumed := startWatch(t, trees, "", &metainternalversion.ListOptions{ResourceVersion: bookmark.meta.GetResourceVersion()})
		expectNoEvent(t, resumed)
	})
}

func TestWatchResume(t *testing.T) {
	a := newApp(testNamespace, "a", "default", nil)
	b := newApp(testNamespace, "b", "default", nil)
	c := newApp(testNamespace, "c", "default", nil)
	f := newFixture(t, a, b, c)
	trees := startHub(t, f)

	listObj, err := trees.List(requestContext("", bob), nil)
	require.NoError(t, err)
	list := listObj.(*views.ApplicationTreeList)
	require.Len(t, list.Items, 3)

	// Changes made between the list and the watch.
	f.setTree(t, a, "cm")
	require.NoError(t, f.appIndexer.Delete(b))
	d := newApp(testNamespace, "d", "default", nil)
	require.NoError(t, f.appIndexer.Add(d))

	t.Run("from a remembered list resourceVersion only the changes are sent", func(t *testing.T) {
		w := startWatch(t, trees, "", &metainternalversion.ListOptions{ResourceVersion: list.ResourceVersion})
		events := map[string]watch.EventType{}
		for range 3 {
			ev := nextEvent(t, w)
			events[ev.name] = ev.eventType
		}
		assert.Equal(t, map[string]watch.EventType{"argocd/a": watch.Modified, "argocd/b": watch.Deleted, "argocd/d": watch.Added}, events)
		expectNoEvent(t, w)
	})

	t.Run("from an unknown resourceVersion the current state is replayed", func(t *testing.T) {
		w := startWatch(t, trees, "", &metainternalversion.ListOptions{ResourceVersion: "unknown"})
		for _, name := range []string{"argocd/a", "argocd/c", "argocd/d"} {
			ev := nextEvent(t, w)
			assert.Equal(t, watch.Modified, ev.eventType)
			assert.Equal(t, name, ev.name)
		}
		expectNoEvent(t, w)
	})
}

func TestWatchSlowConsumer(t *testing.T) {
	f := newFixture(t, newApp(testNamespace, "a", "default", nil), newApp(testNamespace, "b", "default", nil), newApp(testNamespace, "c", "default", nil))
	f.hub.bufferSize = 2
	trees := newREST(treeKind, f.proj, f.authz, f.hub)

	w, err := trees.Watch(requestContext("", bob), &metainternalversion.ListOptions{})
	require.NoError(t, err)
	defer w.Stop()
	var events []watch.Event
	for ev := range w.ResultChan() {
		events = append(events, ev)
	}
	require.Len(t, events, 3)
	assert.Equal(t, watch.Added, events[0].Type)
	assert.Equal(t, watch.Added, events[1].Type)
	assert.Equal(t, watch.Error, events[2].Type)
	status := events[2].Object.(*metav1.Status)
	assert.Equal(t, int32(http.StatusGone), status.Code)
	assert.Equal(t, metav1.StatusReasonExpired, status.Reason)
	assert.False(t, f.hub.hasWatchers())
}

func TestWatchPeriodicBookmarks(t *testing.T) {
	f := newFixture(t, newApp(testNamespace, "a", "default", nil))
	f.hub.bookmarkInterval = 20 * time.Millisecond
	trees := startHub(t, f)

	w := startWatch(t, trees, "", &metainternalversion.ListOptions{AllowWatchBookmarks: true})
	assert.Equal(t, watch.Added, nextEvent(t, w).eventType)
	bookmark := nextEvent(t, w)
	assert.Equal(t, watch.Bookmark, bookmark.eventType)
	snapshot, ok := f.hub.snapshot(treeKind, bookmark.meta.GetResourceVersion())
	require.True(t, ok)
	assert.Contains(t, snapshot, "argocd/a")
}

func TestWatchStopsWithContext(t *testing.T) {
	f := newFixture(t, newApp(testNamespace, "a", "default", nil))
	trees := newREST(treeKind, f.proj, f.authz, f.hub)
	ctx, cancel := context.WithCancel(requestContext("", bob))
	w, err := trees.Watch(ctx, &metainternalversion.ListOptions{})
	require.NoError(t, err)
	assert.True(t, f.hub.hasWatchers())
	cancel()
	require.Eventually(t, func() bool { return !f.hub.hasWatchers() }, 5*time.Second, 10*time.Millisecond)
	ev, ok := <-w.ResultChan()
	assert.True(t, ok)
	assert.Equal(t, watch.Added, ev.Type)
	_, ok = <-w.ResultChan()
	assert.False(t, ok, "the result channel must be closed")
	w.Stop()
}

func TestHubIgnoresIrrelevantUpdates(t *testing.T) {
	app := newApp(testNamespace, "a", "default", nil)
	f := newFixture(t, app)
	updated := app.DeepCopy()
	updated.Status.ReconciledAt = &metav1.Time{Time: time.Now()}
	updated.Status.Sync.Status = appv1.SyncStatusCodeSynced
	f.hub.eventHandler().OnUpdate(app, updated)
	assert.Equal(t, 0, f.hub.queue.Len())

	updated.Labels = map[string]string{"new": "label"}
	f.hub.eventHandler().OnUpdate(app, updated)
	assert.Equal(t, 1, f.hub.queue.Len())
}
