package registry

import (
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/endpoints/request"

	"github.com/argoproj/argo-cd/v3/server/aggregator/clusters"
)

// Watch aggregates downstream watches. With a synthetic namespace it watches a single
// downstream (cluster, namespace); with no namespace it fans in one watch per cluster and
// multiplexes their events onto a single stream, re-tagging every object into its synthetic
// namespace.
func (s *Store) Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	namespace, _ := request.NamespaceFrom(ctx)
	v1opts := toV1ListOptions(options)
	v1opts.Watch = true

	if namespace != "" {
		target, ok := s.registry.Resolve(namespace)
		if !ok {
			return watch.NewEmptyWatch(), nil
		}
		client, err := s.registry.ClientForServer(ctx, target.Server)
		if err != nil {
			return nil, err
		}
		w, err := s.resource.Watch(ctx, client, target.Namespace, v1opts)
		if err != nil {
			return nil, err
		}
		return s.taggedWatch(w, target.Cluster, target.Server), nil
	}

	clusterList, err := s.registry.Clusters(ctx)
	if err != nil {
		return nil, err
	}
	agg := newAggregatedWatch()
	for i := range clusterList {
		c := clusterList[i]
		client, err := s.registry.ClientForCluster(&c)
		if err != nil {
			continue
		}
		w, err := s.resource.Watch(ctx, client, metav1.NamespaceAll, v1opts)
		if err != nil {
			continue
		}
		agg.forward(s.taggedWatch(w, c.Name, c.Server))
	}
	return agg, nil
}

// taggedWatch wraps a downstream watch so that every emitted object is rewritten into its
// synthetic namespace before reaching the client.
func (s *Store) taggedWatch(w watch.Interface, clusterName, server string) watch.Interface {
	return watch.Filter(w, func(in watch.Event) (watch.Event, bool) {
		if in.Type != watch.Error && in.Object != nil {
			if acc, err := meta.Accessor(in.Object); err == nil {
				s.registry.Tag(acc, clusters.Target{Cluster: clusterName, Server: server, Namespace: acc.GetNamespace()})
			}
		}
		return in, true
	})
}

// aggregatedWatch multiplexes the event streams of several downstream watches onto a single
// channel, satisfying watch.Interface for the aggregated, all-namespaces case.
type aggregatedWatch struct {
	result   chan watch.Event
	done     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	mu       sync.Mutex
	children []watch.Interface
}

func newAggregatedWatch() *aggregatedWatch {
	return &aggregatedWatch{
		result: make(chan watch.Event),
		done:   make(chan struct{}),
	}
}

// forward pumps events from a child watch onto the aggregated result channel until either the
// child closes or the aggregate is stopped.
func (a *aggregatedWatch) forward(child watch.Interface) {
	a.mu.Lock()
	a.children = append(a.children, child)
	a.mu.Unlock()

	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		for {
			select {
			case <-a.done:
				return
			case ev, ok := <-child.ResultChan():
				if !ok {
					return
				}
				select {
				case a.result <- ev:
				case <-a.done:
					return
				}
			}
		}
	}()
}

func (a *aggregatedWatch) ResultChan() <-chan watch.Event {
	return a.result
}

func (a *aggregatedWatch) Stop() {
	a.stopOnce.Do(func() {
		close(a.done)
		a.mu.Lock()
		children := a.children
		a.mu.Unlock()
		for _, c := range children {
			c.Stop()
		}
		// Close the result channel only after all forwarders have exited so we never send on
		// a closed channel.
		go func() {
			a.wg.Wait()
			close(a.result)
		}()
	})
}
