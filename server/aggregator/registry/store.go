package registry

import (
	"context"
	"sync"

	"golang.org/x/sync/errgroup"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"

	"github.com/argoproj/argo-cd/v3/server/aggregator/clusters"
)

// Store is a custom REST storage that proxies a single argoproj resource across every
// downstream cluster known to the aggregator. Reads fan out and aggregate; writes are routed
// to the originating downstream cluster via the synthetic-namespace translation in the
// cluster registry.
type Store struct {
	resource Resource
	registry *clusters.Registry
	rest.TableConvertor
}

// Ensure Store satisfies the storage interfaces the aggregator advertises.
var (
	_ rest.Storage              = &Store{}
	_ rest.Scoper               = &Store{}
	_ rest.Lister               = &Store{}
	_ rest.Getter               = &Store{}
	_ rest.Watcher              = &Store{}
	_ rest.Creater              = &Store{}
	_ rest.Updater              = &Store{}
	_ rest.GracefulDeleter      = &Store{}
	_ rest.CollectionDeleter    = &Store{}
	_ rest.SingularNameProvider = &Store{}
)

// NewStore returns a Store for the given resource backed by the cluster registry.
func NewStore(resource Resource, registry *clusters.Registry) *Store {
	return &Store{
		resource:       resource,
		registry:       registry,
		TableConvertor: rest.NewDefaultTableConvertor(resource.GVR().GroupResource()),
	}
}

func (s *Store) New() runtime.Object     { return s.resource.NewEmpty() }
func (s *Store) NewList() runtime.Object { return s.resource.NewList() }
func (s *Store) Destroy()                {}
func (s *Store) NamespaceScoped() bool   { return true }
func (s *Store) GetSingularName() string { return s.resource.Singular() }
func (s *Store) notFound(name string) error {
	return apierrors.NewNotFound(s.resource.GVR().GroupResource(), name)
}

// List aggregates the resource across clusters. With no namespace it fans out to every
// cluster in parallel; with a synthetic namespace it resolves to a single downstream
// (cluster, namespace) and lists there.
func (s *Store) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	namespace, _ := request.NamespaceFrom(ctx)
	v1opts := toV1ListOptions(options)
	list := s.resource.NewList()

	if namespace != "" {
		target, ok := s.registry.Resolve(namespace)
		if !ok {
			// Unknown synthetic namespace: nothing to list yet.
			return list, nil
		}
		client, err := s.registry.ClientForServer(ctx, target.Server)
		if err != nil {
			return nil, err
		}
		down, err := s.resource.List(ctx, client, target.Namespace, v1opts)
		if err != nil {
			return nil, err
		}
		items, err := s.tagItems(down, target.Cluster, target.Server)
		if err != nil {
			return nil, err
		}
		if err := meta.SetList(list, items); err != nil {
			return nil, err
		}
		return list, nil
	}

	clusterList, err := s.registry.Clusters(ctx)
	if err != nil {
		return nil, err
	}
	var (
		mu  sync.Mutex
		all []runtime.Object
	)
	g, gctx := errgroup.WithContext(ctx)
	for i := range clusterList {
		c := clusterList[i]
		g.Go(func() error {
			client, err := s.registry.ClientForCluster(&c)
			if err != nil {
				// Skip clusters we cannot build a client for rather than failing the whole list.
				return nil
			}
			down, err := s.resource.List(gctx, client, metav1.NamespaceAll, v1opts)
			if err != nil {
				// Skip transiently unreachable clusters.
				return nil
			}
			items, err := s.tagItems(down, c.Name, c.Server)
			if err != nil {
				return err
			}
			mu.Lock()
			all = append(all, items...)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if err := meta.SetList(list, all); err != nil {
		return nil, err
	}
	return list, nil
}

// Get resolves the synthetic namespace to a downstream target and fetches the object.
func (s *Store) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	namespace, _ := request.NamespaceFrom(ctx)
	target, ok := s.registry.Resolve(namespace)
	if !ok {
		return nil, s.notFound(name)
	}
	client, err := s.registry.ClientForServer(ctx, target.Server)
	if err != nil {
		return nil, err
	}
	obj, err := s.resource.Get(ctx, client, target.Namespace, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	if err := s.tagOne(obj, target); err != nil {
		return nil, err
	}
	return obj, nil
}

// Create forwards a new object to the downstream cluster identified by its source labels.
func (s *Store) Create(ctx context.Context, obj runtime.Object, createValidation rest.ValidateObjectFunc, options *metav1.CreateOptions) (runtime.Object, error) {
	namespace, _ := request.NamespaceFrom(ctx)
	acc, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	target, err := s.registry.ResolveForWrite(ctx, namespace, acc)
	if err != nil {
		return nil, apierrors.NewBadRequest(err.Error())
	}
	if createValidation != nil {
		if err := createValidation(ctx, obj); err != nil {
			return nil, err
		}
	}
	client, err := s.registry.ClientForServer(ctx, target.Server)
	if err != nil {
		return nil, err
	}
	clusters.Untag(acc, target)
	acc.SetResourceVersion("")
	out, err := s.resource.Create(ctx, client, target.Namespace, obj, *options)
	if err != nil {
		return nil, err
	}
	if err := s.tagOne(out, target); err != nil {
		return nil, err
	}
	return out, nil
}

// Update fetches the current downstream object, applies the requested change and forwards it.
// When the object does not exist and forceAllowCreate is set, it creates it instead.
func (s *Store) Update(ctx context.Context, name string, objInfo rest.UpdatedObjectInfo, createValidation rest.ValidateObjectFunc, updateValidation rest.ValidateObjectUpdateFunc, forceAllowCreate bool, options *metav1.UpdateOptions) (runtime.Object, bool, error) {
	namespace, _ := request.NamespaceFrom(ctx)

	current, getErr := s.Get(ctx, name, &metav1.GetOptions{})
	if getErr != nil && !apierrors.IsNotFound(getErr) {
		return nil, false, getErr
	}

	var updated runtime.Object
	var err error
	if getErr == nil {
		updated, err = objInfo.UpdatedObject(ctx, current)
	} else {
		updated, err = objInfo.UpdatedObject(ctx, nil)
	}
	if err != nil {
		return nil, false, err
	}
	acc, err := meta.Accessor(updated)
	if err != nil {
		return nil, false, err
	}
	target, err := s.registry.ResolveForWrite(ctx, namespace, acc)
	if err != nil {
		return nil, false, apierrors.NewBadRequest(err.Error())
	}
	client, err := s.registry.ClientForServer(ctx, target.Server)
	if err != nil {
		return nil, false, err
	}

	if getErr != nil {
		// Create-on-update path.
		if !forceAllowCreate {
			return nil, false, getErr
		}
		if createValidation != nil {
			if err := createValidation(ctx, updated); err != nil {
				return nil, false, err
			}
		}
		clusters.Untag(acc, target)
		acc.SetResourceVersion("")
		out, err := s.resource.Create(ctx, client, target.Namespace, updated, metav1.CreateOptions{})
		if err != nil {
			return nil, false, err
		}
		if err := s.tagOne(out, target); err != nil {
			return nil, false, err
		}
		return out, true, nil
	}

	if updateValidation != nil {
		if err := updateValidation(ctx, updated, current); err != nil {
			return nil, false, err
		}
	}
	clusters.Untag(acc, target)
	out, err := s.resource.Update(ctx, client, target.Namespace, updated, *options)
	if err != nil {
		return nil, false, err
	}
	if err := s.tagOne(out, target); err != nil {
		return nil, false, err
	}
	return out, false, nil
}

// Delete resolves the synthetic namespace and forwards the deletion downstream.
func (s *Store) Delete(ctx context.Context, name string, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions) (runtime.Object, bool, error) {
	namespace, _ := request.NamespaceFrom(ctx)
	target, ok := s.registry.Resolve(namespace)
	if !ok {
		return nil, false, s.notFound(name)
	}
	client, err := s.registry.ClientForServer(ctx, target.Server)
	if err != nil {
		return nil, false, err
	}
	obj, err := s.resource.Get(ctx, client, target.Namespace, name, metav1.GetOptions{})
	if err != nil {
		return nil, false, err
	}
	if deleteValidation != nil {
		if err := deleteValidation(ctx, obj); err != nil {
			return nil, false, err
		}
	}
	if err := s.resource.Delete(ctx, client, target.Namespace, name, *options); err != nil {
		return nil, false, err
	}
	if err := s.tagOne(obj, target); err != nil {
		return nil, false, err
	}
	return obj, true, nil
}

// DeleteCollection forwards a collection deletion to the resolved cluster, or to every
// cluster when no namespace is specified.
func (s *Store) DeleteCollection(ctx context.Context, deleteValidation rest.ValidateObjectFunc, options *metav1.DeleteOptions, listOptions *metainternalversion.ListOptions) (runtime.Object, error) {
	namespace, _ := request.NamespaceFrom(ctx)
	v1listOpts := toV1ListOptions(listOptions)

	if namespace != "" {
		target, ok := s.registry.Resolve(namespace)
		if !ok {
			return s.resource.NewList(), nil
		}
		client, err := s.registry.ClientForServer(ctx, target.Server)
		if err != nil {
			return nil, err
		}
		if err := s.resource.DeleteCollection(ctx, client, target.Namespace, *options, v1listOpts); err != nil {
			return nil, err
		}
		return s.resource.NewList(), nil
	}

	clusterList, err := s.registry.Clusters(ctx)
	if err != nil {
		return nil, err
	}
	for i := range clusterList {
		c := clusterList[i]
		client, err := s.registry.ClientForCluster(&c)
		if err != nil {
			continue
		}
		_ = s.resource.DeleteCollection(ctx, client, metav1.NamespaceAll, *options, v1listOpts)
	}
	return s.resource.NewList(), nil
}

// tagItems extracts the items from a downstream list and rewrites each into the synthetic
// namespace, returning them as a slice for re-assembly into the aggregated list.
func (s *Store) tagItems(list runtime.Object, clusterName, server string) ([]runtime.Object, error) {
	items, err := meta.ExtractList(list)
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		acc, err := meta.Accessor(item)
		if err != nil {
			return nil, err
		}
		s.registry.Tag(acc, clusters.Target{Cluster: clusterName, Server: server, Namespace: acc.GetNamespace()})
	}
	return items, nil
}

// tagOne rewrites a single downstream object into the synthetic namespace.
func (s *Store) tagOne(obj runtime.Object, target clusters.Target) error {
	acc, err := meta.Accessor(obj)
	if err != nil {
		return err
	}
	s.registry.Tag(acc, target)
	return nil
}

// toV1ListOptions projects the internal list options onto the metav1 options understood by
// the downstream typed client. Continue tokens and resourceVersion are per-cluster concepts
// and are passed through unchanged; callers should not rely on a global ordering.
func toV1ListOptions(o *metainternalversion.ListOptions) metav1.ListOptions {
	out := metav1.ListOptions{}
	if o == nil {
		return out
	}
	if o.LabelSelector != nil {
		out.LabelSelector = o.LabelSelector.String()
	}
	if o.FieldSelector != nil {
		out.FieldSelector = o.FieldSelector.String()
	}
	out.ResourceVersion = o.ResourceVersion
	out.ResourceVersionMatch = o.ResourceVersionMatch
	out.TimeoutSeconds = o.TimeoutSeconds
	out.Limit = o.Limit
	out.Continue = o.Continue
	out.AllowWatchBookmarks = o.AllowWatchBookmarks
	return out
}
