package informer

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

func configMap(namespace, name string) *corev1.ConfigMap {
	return &corev1.ConfigMap{Namespace: namespace, Name: name, ResourceVersion: "1"}
}

func keepNamespaces(namespaces ...string) KeepFunc {
	return func(obj metav1.Object) bool {
		for _, ns := range namespaces {
			if obj.GetNamespace() == ns {
				return true
			}
		}
		return false
	}
}

// fakeAPI serves ConfigMaps the way an API server does: a list returns the current objects, and a watch
// asked for initial events streams them followed by the bookmark that ends them.
type fakeAPI struct {
	mu          sync.Mutex
	objects     []*corev1.ConfigMap
	lists       int
	streamLists int
	watcher     *watch.RaceFreeFakeWatcher
}

func (f *fakeAPI) listWatch() *cache.ListWatch {
	return &cache.ListWatch{
		ListWithContextFunc: func(_ context.Context, _ metav1.ListOptions) (runtime.Object, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.lists++
			list := &corev1.ConfigMapList{ResourceVersion: "10"}
			for _, obj := range f.objects {
				list.Items = append(list.Items, *obj)
			}
			return list, nil
		},
		WatchFuncWithContext: func(_ context.Context, options metav1.ListOptions) (watch.Interface, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			w := watch.NewRaceFreeFake()
			if options.SendInitialEvents != nil && *options.SendInitialEvents {
				f.streamLists++
				for _, obj := range f.objects {
					w.Add(obj.DeepCopy())
				}
				w.Action(watch.Bookmark, &corev1.ConfigMap{
					ResourceVersion: "10",
					Annotations:     map[string]string{metav1.InitialEventsAnnotationKey: "true"},
				})
			}
			f.watcher = w
			return w, nil
		},
	}
}

func (f *fakeAPI) send(eventType watch.EventType, obj runtime.Object) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.watcher.Action(eventType, obj)
}

func (f *fakeAPI) calls() (lists, streamLists int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lists, f.streamLists
}

func storeKeys(informer cache.SharedIndexInformer) []string {
	return informer.GetStore().ListKeys()
}

// The filter has to hold on every path objects reach the reflector by: a regular list, the initial
// events of a streamed list, and the watch that follows either.
func TestNewListerWatcher_InformerCachesOnlyKeptObjects(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)

	tests := []struct {
		name string
		// client decides whether the reflector may stream the list.
		client       any
		wantStreamed bool
	}{
		{name: "regular list", client: kubefake.NewClientset(), wantStreamed: false},
		{name: "streamed list", client: nil, wantStreamed: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			api := &fakeAPI{objects: []*corev1.ConfigMap{
				configMap("argocd", "kept"),
				configMap("other", "dropped"),
				configMap("team-a", "kept-too"),
			}}
			informer := cache.NewSharedIndexInformer(
				NewListerWatcher(api.listWatch(), tc.client, keepNamespaces("argocd", "team-a")),
				&corev1.ConfigMap{}, 0, cache.Indexers{})

			var mu sync.Mutex
			var added []string
			_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
				AddFunc: func(obj any) {
					mu.Lock()
					defer mu.Unlock()
					key, _ := cache.MetaNamespaceKeyFunc(obj)
					added = append(added, key)
				},
			})
			require.NoError(t, err)

			go informer.Run(t.Context().Done())
			require.True(t, cache.WaitForCacheSync(t.Context().Done(), informer.HasSynced))

			assert.ElementsMatch(t, []string{"argocd/kept", "team-a/kept-too"}, storeKeys(informer))
			lists, streamLists := api.calls()
			if tc.wantStreamed {
				assert.Zero(t, lists, "a streamed list must not fall back to a regular list")
				assert.Equal(t, 1, streamLists)
			} else {
				assert.Equal(t, 1, lists)
				assert.Zero(t, streamLists)
			}

			// The rejected objects come first, so they have been through the filter by the time the
			// kept one shows up in the store.
			api.send(watch.Added, configMap("other", "dropped-later"))
			api.send(watch.Modified, configMap("other", "dropped"))
			api.send(watch.Deleted, configMap("other", "dropped"))
			api.send(watch.Added, configMap("team-a", "kept-later"))
			require.EventuallyWithT(t, func(c *assert.CollectT) {
				assert.Contains(c, storeKeys(informer), "team-a/kept-later")
			}, 5*time.Second, 10*time.Millisecond)
			assert.ElementsMatch(t, []string{"argocd/kept", "team-a/kept-too", "team-a/kept-later"}, storeKeys(informer))

			require.EventuallyWithT(t, func(c *assert.CollectT) {
				mu.Lock()
				defer mu.Unlock()
				assert.ElementsMatch(c, []string{"argocd/kept", "team-a/kept-too", "team-a/kept-later"}, added)
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}

func TestNewListerWatcher_WatchListSemantics(t *testing.T) {
	type unSupportedWatchListSemantics interface {
		IsWatchListSemanticsUnSupported() bool
	}

	fakeClient, ok := NewListerWatcher(&cache.ListWatch{}, kubefake.NewClientset(), nil).(unSupportedWatchListSemantics)
	require.True(t, ok)
	assert.True(t, fakeClient.IsWatchListSemanticsUnSupported(), "a fake clientset can't stream a list")

	realClient, ok := NewListerWatcher(&cache.ListWatch{}, struct{}{}, nil).(unSupportedWatchListSemantics)
	require.True(t, ok)
	assert.False(t, realClient.IsWatchListSemanticsUnSupported())
}

func TestListerWatcher_List(t *testing.T) {
	newList := func() *corev1.ConfigMapList {
		return &corev1.ConfigMapList{
			ResourceVersion: "10",
			Items:           []corev1.ConfigMap{*configMap("argocd", "a"), *configMap("other", "b"), *configMap("argocd", "c")},
		}
	}
	// list runs a list that returns the given result through a lister-watcher with the given filter.
	list := func(t *testing.T, result runtime.Object, resultErr error, keep KeepFunc) (runtime.Object, error) {
		t.Helper()
		lw := NewListerWatcher(&cache.ListWatch{ListWithContextFunc: func(_ context.Context, _ metav1.ListOptions) (runtime.Object, error) {
			return result, resultErr
		}}, nil, keep)
		return cache.ToListerWatcherWithContext(lw).ListWithContext(t.Context(), metav1.ListOptions{})
	}

	t.Run("drops rejected items and keeps the list metadata", func(t *testing.T) {
		got, err := list(t, newList(), nil, keepNamespaces("argocd"))
		require.NoError(t, err)
		filtered := got.(*corev1.ConfigMapList)
		assert.Equal(t, "10", filtered.ResourceVersion)
		require.Len(t, filtered.Items, 2)
		assert.Equal(t, "a", filtered.Items[0].Name)
		assert.Equal(t, "c", filtered.Items[1].Name)
	})

	t.Run("leaves the list alone when nothing is rejected", func(t *testing.T) {
		all := newList()
		first := &all.Items[0]
		got, err := list(t, all, nil, keepNamespaces("argocd", "other"))
		require.NoError(t, err)
		require.Len(t, got.(*corev1.ConfigMapList).Items, 3)
		assert.Same(t, first, &got.(*corev1.ConfigMapList).Items[0], "items must not be copied")
	})

	t.Run("returns everything without a filter", func(t *testing.T) {
		got, err := list(t, newList(), nil, nil)
		require.NoError(t, err)
		assert.Len(t, got.(*corev1.ConfigMapList).Items, 3)
	})

	t.Run("filters unstructured lists", func(t *testing.T) {
		item := func(namespace, name string) unstructured.Unstructured {
			obj := unstructured.Unstructured{}
			obj.SetNamespace(namespace)
			obj.SetName(name)
			return obj
		}
		all := &unstructured.UnstructuredList{Items: []unstructured.Unstructured{item("other", "a"), item("argocd", "b")}}
		got, err := list(t, all, nil, keepNamespaces("argocd"))
		require.NoError(t, err)
		items := got.(*unstructured.UnstructuredList).Items
		require.Len(t, items, 1)
		assert.Equal(t, "b", items[0].GetName())
	})

	t.Run("passes list errors through", func(t *testing.T) {
		listErr := errors.New("list failed")
		_, err := list(t, nil, listErr, keepNamespaces("argocd"))
		require.ErrorIs(t, err, listErr)
	})
}

func TestListerWatcher_Watch(t *testing.T) {
	newWatch := func(t *testing.T, keep KeepFunc) (*watch.RaceFreeFakeWatcher, watch.Interface) {
		t.Helper()
		incoming := watch.NewRaceFreeFake()
		lw := NewListerWatcher(&cache.ListWatch{
			WatchFuncWithContext: func(_ context.Context, _ metav1.ListOptions) (watch.Interface, error) {
				return incoming, nil
			},
		}, nil, keep)
		w, err := cache.ToListerWatcherWithContext(lw).WatchWithContext(t.Context(), metav1.ListOptions{})
		require.NoError(t, err)
		return incoming, w
	}
	next := func(t *testing.T, w watch.Interface) watch.Event {
		t.Helper()
		select {
		case event, ok := <-w.ResultChan():
			require.True(t, ok, "watch closed")
			return event
		case <-time.After(5 * time.Second):
			require.FailNow(t, "timed out waiting for a watch event")
			return watch.Event{}
		}
	}

	t.Run("drops events for rejected objects", func(t *testing.T) {
		incoming, w := newWatch(t, keepNamespaces("argocd"))
		defer w.Stop()

		incoming.Add(configMap("other", "a"))
		incoming.Modify(configMap("other", "a"))
		incoming.Delete(configMap("other", "a"))
		incoming.Add(configMap("argocd", "b"))
		incoming.Modify(configMap("argocd", "b"))
		incoming.Delete(configMap("argocd", "b"))

		for _, want := range []watch.EventType{watch.Added, watch.Modified, watch.Deleted} {
			event := next(t, w)
			assert.Equal(t, want, event.Type)
			assert.Equal(t, "b", event.Object.(*corev1.ConfigMap).Name)
		}
	})

	// A bookmark has no namespace, and a filter that swallowed it would leave a streamed list waiting
	// forever for its end. An error is a metav1.Status the reflector has to see.
	t.Run("passes bookmarks and errors", func(t *testing.T) {
		incoming, w := newWatch(t, keepNamespaces("argocd"))
		defer w.Stop()

		incoming.Action(watch.Bookmark, &corev1.ConfigMap{ResourceVersion: "10"})
		incoming.Error(&metav1.Status{Status: metav1.StatusFailure, Reason: metav1.StatusReasonExpired})

		assert.Equal(t, watch.Bookmark, next(t, w).Type)
		assert.Equal(t, watch.Error, next(t, w).Type)
	})

	t.Run("returns the watch as is without a filter", func(t *testing.T) {
		incoming, w := newWatch(t, nil)
		defer w.Stop()
		assert.Same(t, incoming, w)
	})

	t.Run("closes when the incoming watch closes", func(t *testing.T) {
		incoming, w := newWatch(t, keepNamespaces("argocd"))
		incoming.Stop()
		select {
		case _, ok := <-w.ResultChan():
			assert.False(t, ok)
		case <-time.After(5 * time.Second):
			require.FailNow(t, "the filtered watch did not close")
		}
	})

	// A reflector stops a watch and walks away without draining it.
	t.Run("stops with an undelivered event", func(t *testing.T) {
		incoming, w := newWatch(t, keepNamespaces("argocd"))
		incoming.Add(configMap("argocd", "a"))
		incoming.Add(configMap("argocd", "b"))

		w.Stop()
		w.Stop()

		assert.True(t, incoming.IsStopped())
		// At most the event that was already being handed over is delivered before the close.
		deadline := time.After(5 * time.Second)
		for {
			select {
			case _, ok := <-w.ResultChan():
				if !ok {
					return
				}
			case <-deadline:
				require.FailNow(t, "the filtered watch did not close after Stop")
			}
		}
	})

	t.Run("passes watch errors through", func(t *testing.T) {
		watchErr := errors.New("watch failed")
		lw := NewListerWatcher(&cache.ListWatch{
			WatchFuncWithContext: func(_ context.Context, _ metav1.ListOptions) (watch.Interface, error) {
				return nil, watchErr
			},
		}, nil, keepNamespaces("argocd"))
		_, err := cache.ToListerWatcherWithContext(lw).WatchWithContext(t.Context(), metav1.ListOptions{})
		require.ErrorIs(t, err, watchErr)
	})
}
