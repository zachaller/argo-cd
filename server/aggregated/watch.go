package aggregated

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"reflect"
	"slices"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/utils/lru"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	cacheutil "github.com/argoproj/argo-cd/v3/util/cache"
)

const (
	defaultBookmarkInterval = time.Minute
	defaultWatchBufferSize  = 100
	defaultResyncPeriod     = 3 * time.Minute
	// snapshotCacheSize is the number of list and bookmark resourceVersions remembered for resuming watches.
	snapshotCacheSize = 1024
)

// appChangeNotifier delivers a notification whenever the application controller updates the state of an application.
type appChangeNotifier interface {
	OnAnyAppResourcesTreeChanged(ctx context.Context, callback func(appInstanceName string) error) error
}

// hub fans out changes of the projections to watchers. A single hub serves all watchers of a server replica: it
// subscribes once to the application controller's cache notifications and once to the Application informer, computes
// each changed projection at most once per kind, and delivers it to every watcher allowed to see it.
type hub struct {
	proj             *projector
	authz            appAuthorizer
	bookmarkInterval time.Duration
	bufferSize       int
	resyncPeriod     time.Duration

	queue workqueue.TypedInterface[string]

	// processLock serializes the processing of changes with the registration of new watchers, so that a watcher never
	// receives an older version of an object after a newer one.
	processLock sync.Mutex

	watchersLock sync.RWMutex
	watchers     map[*watcher]struct{}

	// snapshots maps "<resource>/<resourceVersion>" of lists and bookmarks to the resourceVersions of their items.
	// It allows resuming a watch with exactly the events missed since the list or bookmark.
	snapshots *lru.Cache
}

func newHub(proj *projector, authz appAuthorizer) *hub {
	return &hub{
		proj:             proj,
		authz:            authz,
		bookmarkInterval: defaultBookmarkInterval,
		bufferSize:       defaultWatchBufferSize,
		resyncPeriod:     defaultResyncPeriod,
		queue:            workqueue.NewTyped[string](),
		watchers:         map[*watcher]struct{}{},
		snapshots:        lru.New(snapshotCacheSize),
	}
}

// eventHandler returns the Application informer event handler feeding the hub.
func (h *hub) eventHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if app, ok := obj.(*appv1.Application); ok {
				h.queue.Add(appKey(app.Namespace, app.Name))
			}
		},
		UpdateFunc: func(oldObj, newObj any) {
			oldApp, ok1 := oldObj.(*appv1.Application)
			newApp, ok2 := newObj.(*appv1.Application)
			if !ok1 || !ok2 {
				return
			}
			// The application controller's cache notifications cover changes of the tree and the diff. Of the
			// Application itself, only the fields copied to the projections matter.
			if !reflect.DeepEqual(oldApp.Labels, newApp.Labels) ||
				oldApp.Spec.GetProject() != newApp.Spec.GetProject() ||
				!reflect.DeepEqual(oldApp.Status.Conditions, newApp.Status.Conditions) {
				h.queue.Add(appKey(newApp.Namespace, newApp.Name))
			}
		},
		DeleteFunc: func(obj any) {
			if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
				obj = tombstone.Obj
			}
			if app, ok := obj.(*appv1.Application); ok {
				h.queue.Add(appKey(app.Namespace, app.Name))
			}
		},
	}
}

// run processes changes until ctx is done. It subscribes to the change notifications of notifier and periodically
// re-checks all Applications, which catches notifications lost by Redis pub/sub.
func (h *hub) run(ctx context.Context, notifier appChangeNotifier) {
	go func() {
		<-ctx.Done()
		h.queue.ShutDown()
	}()
	if notifier != nil {
		go h.subscribe(ctx, notifier)
	}
	go wait.Until(h.resync, h.resyncPeriod, ctx.Done())
	for h.processNextItem() {
	}
}

func (h *hub) subscribe(ctx context.Context, notifier appChangeNotifier) {
	wait.UntilWithContext(ctx, func(ctx context.Context) {
		err := notifier.OnAnyAppResourcesTreeChanged(ctx, func(appInstanceName string) error {
			h.queue.Add(appKeyFromInstanceName(appInstanceName, h.proj.namespace))
			return nil
		})
		if errors.Is(err, cacheutil.ErrPatternSubscriptionUnsupported) {
			log.Warnf("The cache does not support pattern subscriptions, watches are refreshed every %s only", h.resyncPeriod)
			<-ctx.Done()
			return
		}
		if err != nil && ctx.Err() == nil {
			log.WithError(err).Warn("Application state change subscription failed, resubscribing")
		}
	}, 5*time.Second)
}

func (h *hub) resync() {
	if !h.hasWatchers() {
		return
	}
	apps, err := h.proj.listApps("")
	if err != nil {
		log.WithError(err).Warn("Failed to list applications for watch resync")
		return
	}
	for _, app := range apps {
		h.queue.Add(appKey(app.Namespace, app.Name))
	}
}

func (h *hub) processNextItem() bool {
	key, shutdown := h.queue.Get()
	if shutdown {
		return false
	}
	defer h.queue.Done(key)
	h.process(key)
	return true
}

func (h *hub) hasWatchers() bool {
	h.watchersLock.RLock()
	defer h.watchersLock.RUnlock()
	return len(h.watchers) > 0
}

func (h *hub) currentWatchers() []*watcher {
	h.watchersLock.RLock()
	defer h.watchersLock.RUnlock()
	watchers := make([]*watcher, 0, len(h.watchers))
	for w := range h.watchers {
		watchers = append(watchers, w)
	}
	return watchers
}

// process delivers the current state of the projections of the Application with the given key to all watchers.
func (h *hub) process(key string) {
	h.processLock.Lock()
	defer h.processLock.Unlock()

	watchers := h.currentWatchers()
	if len(watchers) == 0 {
		return
	}
	app := h.lookupApp(key)
	projections := newProjectionCache(h.proj, app)
	for _, w := range watchers {
		w.update(key, app, projections)
	}
}

// lookupApp returns the Application with the given key, or nil if it does not exist or is not served.
func (h *hub) lookupApp(key string) *appv1.Application {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil || !h.proj.namespaceEnabled(namespace) {
		return nil
	}
	app, err := h.proj.appLister.Applications(namespace).Get(name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			log.WithError(err).Warnf("Failed to get application %s", key)
		}
		return nil
	}
	return app
}

// projectionCache computes the projection of an Application at most once per kind.
type projectionCache struct {
	proj    *projector
	app     *appv1.Application
	results map[*kind]projectionResult
}

type projectionResult struct {
	obj runtime.Object
	err error
}

func newProjectionCache(proj *projector, app *appv1.Application) *projectionCache {
	return &projectionCache{proj: proj, app: app, results: map[*kind]projectionResult{}}
}

func (c *projectionCache) get(k *kind) (runtime.Object, error) {
	if result, ok := c.results[k]; ok {
		return result.obj, result.err
	}
	obj, err := k.project(c.proj, c.app)
	c.results[k] = projectionResult{obj: obj, err: err}
	return obj, err
}

// watch registers a new watcher and sends it the initial events requested by options.
func (h *hub) watch(ctx context.Context, k *kind, u user.Info, sel *selection, options *metainternalversion.ListOptions) (watch.Interface, error) {
	w := &watcher{
		hub:    h,
		kind:   k,
		user:   u,
		sel:    sel,
		sent:   map[string]string{},
		result: make(chan watch.Event, h.bufferSize+1),
		done:   make(chan struct{}),
	}
	if options == nil {
		options = &metainternalversion.ListOptions{}
	}
	if err := h.start(w, options); err != nil {
		w.Stop()
		return nil, err
	}
	go func() {
		select {
		case <-ctx.Done():
			w.Stop()
		case <-w.done:
		}
	}()
	if options.AllowWatchBookmarks && h.bookmarkInterval > 0 {
		go w.sendBookmarks(h.bookmarkInterval)
	}
	return w, nil
}

// start registers w and sends the initial events:
//   - with sendInitialEvents=true, or without a resourceVersion: an ADDED event per object (then, for
//     sendInitialEvents with allowWatchBookmarks, a bookmark marking the end of the initial events);
//   - with the resourceVersion of a list or bookmark remembered by this replica: exactly the changes since then;
//   - with any other resourceVersion: the current state of every object as MODIFIED events. Clients may therefore
//     receive an event for an object they already have, but never miss a change.
func (h *hub) start(w *watcher, options *metainternalversion.ListOptions) error {
	h.processLock.Lock()
	defer h.processLock.Unlock()

	apps, err := h.proj.listApps(w.sel.namespace)
	if err != nil {
		return err
	}
	current := make(map[string]runtime.Object, len(apps))
	for _, app := range apps {
		if !w.sel.matches(app, h.proj.labels(app)) || !h.authz.canGet(w.user, app) {
			continue
		}
		obj, err := w.kind.project(h.proj, app)
		if err != nil {
			return err
		}
		current[appKey(app.Namespace, app.Name)] = obj
	}

	sendInitialEvents := options.SendInitialEvents != nil && *options.SendInitialEvents
	resourceVersion := options.ResourceVersion
	replayAll := sendInitialEvents || resourceVersion == "" || resourceVersion == "0"
	var known map[string]string
	resume := false
	if !replayAll {
		if known, resume = h.snapshot(w.kind, resourceVersion); !resume {
			log.Debugf("Unknown resourceVersion %q for %s watch, replaying current state", resourceVersion, w.kind.resource)
		}
	}

	w.lock.Lock()
	defer w.lock.Unlock()
	for _, key := range sortedKeys(current) {
		obj := current[key]
		rv := obj.(metav1.Object).GetResourceVersion()
		eventType := watch.Added
		switch {
		case replayAll:
		case !resume:
			eventType = watch.Modified
		default:
			previous, existed := known[key]
			if existed && previous == rv {
				w.sent[key] = rv
				continue
			}
			if existed {
				eventType = watch.Modified
			}
		}
		w.sendLocked(watch.Event{Type: eventType, Object: obj})
		w.sent[key] = rv
	}
	for key, previous := range known {
		if _, exists := current[key]; !exists {
			w.sendDeletedLocked(key, previous)
		}
	}
	// Clients receive bookmarks only when they ask for them; kube-apiserver defaults sendInitialEvents to true for
	// watches without resourceVersion, without asking for bookmarks.
	if sendInitialEvents && options.AllowWatchBookmarks {
		w.sendBookmarkLocked(true)
	}
	if w.stopped {
		// The client could not even keep up with the initial events.
		return nil
	}

	h.watchersLock.Lock()
	h.watchers[w] = struct{}{}
	h.watchersLock.Unlock()
	return nil
}

func (h *hub) remove(w *watcher) {
	h.watchersLock.Lock()
	delete(h.watchers, w)
	h.watchersLock.Unlock()
}

func snapshotKey(k *kind, resourceVersion string) string {
	return k.resource + "/" + resourceVersion
}

// rememberSnapshot remembers the resourceVersions of the items of a list or bookmark, so that a watch started from
// resourceVersion receives exactly the changes made since.
func (h *hub) rememberSnapshot(k *kind, resourceVersion string, itemVersions map[string]string) {
	h.snapshots.Add(snapshotKey(k, resourceVersion), maps.Clone(itemVersions))
}

func (h *hub) snapshot(k *kind, resourceVersion string) (map[string]string, bool) {
	value, ok := h.snapshots.Get(snapshotKey(k, resourceVersion))
	if !ok {
		return nil, false
	}
	return value.(map[string]string), true
}

// watcher is a single watch request. It implements watch.Interface.
type watcher struct {
	hub  *hub
	kind *kind
	user user.Info
	sel  *selection

	lock sync.Mutex
	// sent holds the resourceVersion of the last event sent for each object the watcher has seen.
	sent    map[string]string
	result  chan watch.Event
	stopped bool
	done    chan struct{}
}

func (w *watcher) ResultChan() <-chan watch.Event {
	return w.result
}

func (w *watcher) Stop() {
	w.hub.remove(w)
	w.lock.Lock()
	defer w.lock.Unlock()
	w.stopLocked()
}

func (w *watcher) stopLocked() {
	if !w.stopped {
		w.stopped = true
		close(w.result)
		close(w.done)
	}
}

// update delivers the current state of the projection of the Application with the given key. app is nil if the
// Application does not exist anymore.
func (w *watcher) update(key string, app *appv1.Application, projections *projectionCache) {
	visible := app != nil && w.sel.matches(app, w.hub.proj.labels(app)) && w.hub.authz.canGet(w.user, app)
	if !visible {
		w.lock.Lock()
		defer w.lock.Unlock()
		if previous, ok := w.sent[key]; ok {
			w.sendDeletedLocked(key, previous)
		}
		return
	}
	obj, err := projections.get(w.kind)
	if err != nil {
		// Transient errors (e.g. Redis being unavailable) are retried on the next notification or resync.
		log.WithError(err).Warnf("Failed to compute %s of application %s", w.kind.singular, key)
		return
	}
	rv := obj.(metav1.Object).GetResourceVersion()

	w.lock.Lock()
	defer w.lock.Unlock()
	previous, ok := w.sent[key]
	if ok && previous == rv {
		return
	}
	eventType := watch.Modified
	if !ok {
		eventType = watch.Added
	}
	w.sendLocked(watch.Event{Type: eventType, Object: obj.DeepCopyObject()})
	w.sent[key] = rv
}

func (w *watcher) sendDeletedLocked(key, resourceVersion string) {
	namespace, name, _ := cache.SplitMetaNamespaceKey(key)
	w.sendLocked(watch.Event{Type: watch.Deleted, Object: w.kind.newObject(metav1.ObjectMeta{
		Namespace:       namespace,
		Name:            name,
		ResourceVersion: resourceVersion,
	})})
	delete(w.sent, key)
}

// sendBookmarkLocked sends a bookmark whose resourceVersion identifies the objects sent so far.
func (w *watcher) sendBookmarkLocked(initialEventsEnd bool) {
	resourceVersion := listHash(w.sent)
	w.hub.rememberSnapshot(w.kind, resourceVersion, w.sent)
	meta := metav1.ObjectMeta{ResourceVersion: resourceVersion}
	if initialEventsEnd {
		meta.Annotations = map[string]string{metav1.InitialEventsAnnotationKey: "true"}
	}
	w.sendLocked(watch.Event{Type: watch.Bookmark, Object: w.kind.newObject(meta)})
}

func (w *watcher) sendBookmarks(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-w.done:
			return
		case <-ticker.C:
			w.lock.Lock()
			// Bookmarks are best effort: skip them while the client is not keeping up.
			if !w.stopped && len(w.result) < cap(w.result)/2 {
				w.sendBookmarkLocked(false)
			}
			w.lock.Unlock()
		}
	}
}

// sendLocked sends ev without blocking. When the client does not keep up, the watch is terminated with an
// "Expired" error, so that the client starts over with a new list instead of stalling the hub.
func (w *watcher) sendLocked(ev watch.Event) {
	if w.stopped {
		return
	}
	if len(w.result) >= cap(w.result)-1 {
		w.result <- watch.Event{Type: watch.Error, Object: &metav1.Status{
			Status:  metav1.StatusFailure,
			Code:    http.StatusGone,
			Reason:  metav1.StatusReasonExpired,
			Message: "the watch fell behind, please restart it",
		}}
		w.hub.remove(w)
		w.stopLocked()
		return
	}
	w.result <- ev
}

func sortedKeys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}
