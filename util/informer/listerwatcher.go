// Package informer holds helpers for building client-go informers.
package informer

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/watchlist"
)

// KeepFunc reports whether an object may enter an informer's cache.
type KeepFunc func(obj metav1.Object) bool

// NewListerWatcher returns a cache.ListerWatcher for a hand-built cache.ListWatch.
//
// client is the clientset lw talks to. A reflector streams its initial state through the watch
// (WatchList) unless the client says it can't serve that, which the fake clientsets do. A bare
// cache.ListWatch carries no such signal, so a reflector on a fake clientset would wait forever
// for a bookmark the fake never sends.
//
// When keep is non-nil, objects it rejects are removed from list results and from the watch stream,
// so they never reach the reflector. An informer transform can't do that: it has to return an object,
// and the informer's queue stores whatever comes back. Filtering list results alone isn't enough
// either, because a WatchList reflector doesn't call List at all.
//
// keep must give the same answer for an object over its whole lifetime. An object that stopped
// passing would stay in the cache for good, because its later events, deletion included, are dropped.
func NewListerWatcher(lw *cache.ListWatch, client any, keep KeepFunc) cache.ListerWatcher {
	return &listerWatcher{
		lw:                            lw,
		keep:                          keep,
		unsupportedWatchListSemantics: watchlist.DoesClientNotSupportWatchListSemantics(client),
	}
}

type listerWatcher struct {
	lw                            *cache.ListWatch
	keep                          KeepFunc
	unsupportedWatchListSemantics bool
}

var _ cache.ListerWatcherWithContext = &listerWatcher{}

// IsWatchListSemanticsUnSupported tells the reflector to fall back to a regular list.
func (l *listerWatcher) IsWatchListSemanticsUnSupported() bool {
	return l.unsupportedWatchListSemantics
}

func (l *listerWatcher) List(options metav1.ListOptions) (runtime.Object, error) {
	return l.ListWithContext(context.Background(), options)
}

func (l *listerWatcher) ListWithContext(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
	list, err := l.lw.ListWithContext(ctx, options)
	if err != nil || l.keep == nil {
		return list, err
	}
	return list, filterList(list, l.keep)
}

func (l *listerWatcher) Watch(options metav1.ListOptions) (watch.Interface, error) {
	return l.WatchWithContext(context.Background(), options)
}

func (l *listerWatcher) WatchWithContext(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	w, err := l.lw.WatchWithContext(ctx, options)
	if err != nil || l.keep == nil {
		return w, err
	}
	fw := &filteredWatch{
		incoming: w,
		keep:     l.keep,
		result:   make(chan watch.Event),
		done:     make(chan struct{}),
	}
	go fw.run()
	return fw, nil
}

// filterList drops the items keep rejects from list. The list is left untouched when nothing is dropped.
func filterList(list runtime.Object, keep KeepFunc) error {
	items, err := meta.ExtractList(list)
	if err != nil {
		return err
	}
	kept := make([]runtime.Object, 0, len(items))
	for _, item := range items {
		if keepObject(item, keep) {
			kept = append(kept, item)
		}
	}
	if len(kept) == len(items) {
		return nil
	}
	return meta.SetList(list, kept)
}

// keepObject keeps anything that isn't an API object, so that the reflector sees and reports it.
func keepObject(obj runtime.Object, keep KeepFunc) bool {
	accessor, err := meta.Accessor(obj)
	if err != nil {
		return true
	}
	return keep(accessor)
}

// filteredWatch forwards the events of incoming whose object passes keep.
//
// It isn't watch.Filter because that one blocks forever on its result channel when the consumer
// stops reading, which is what a reflector does after it calls Stop.
type filteredWatch struct {
	incoming watch.Interface
	keep     KeepFunc
	result   chan watch.Event
	done     chan struct{}
	stopOnce sync.Once
}

func (fw *filteredWatch) ResultChan() <-chan watch.Event {
	return fw.result
}

func (fw *filteredWatch) Stop() {
	fw.stopOnce.Do(func() { close(fw.done) })
	fw.incoming.Stop()
}

func (fw *filteredWatch) run() {
	defer close(fw.result)
	for {
		select {
		case <-fw.done:
			return
		case event, ok := <-fw.incoming.ResultChan():
			if !ok {
				return
			}
			if !fw.keepEvent(event) {
				continue
			}
			select {
			case fw.result <- event:
			case <-fw.done:
				return
			}
		}
	}
}

// keepEvent always passes bookmarks and errors. A bookmark carries the resource version the reflector
// resumes from and marks the end of a WatchList's initial events; it has no namespace or labels to match.
func (fw *filteredWatch) keepEvent(event watch.Event) bool {
	switch event.Type {
	case watch.Added, watch.Modified, watch.Deleted:
		return keepObject(event.Object, fw.keep)
	default:
		return true
	}
}
