package server

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"

	"github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	appclientset "github.com/argoproj/argo-cd/v3/pkg/client/clientset/versioned"
	appinformer "github.com/argoproj/argo-cd/v3/pkg/client/informers/externalversions"
	informerutil "github.com/argoproj/argo-cd/v3/util/informer"
	"github.com/argoproj/argo-cd/v3/util/security"
)

// newApplicationInformers returns the Application and ApplicationSet informers.
//
// With application namespaces configured the informers watch cluster-wide. Objects from namespaces
// that are not enabled are then kept out of the caches: the server does not manage them, and caching
// them costs memory in multi-tenant clusters.
func newApplicationInformers(clientset appclientset.Interface, namespace string, applicationNamespaces []string) (appInformer, appsetInformer cache.SharedIndexInformer) {
	if len(applicationNamespaces) == 0 {
		factory := appinformer.NewSharedInformerFactoryWithOptions(clientset, 0, appinformer.WithNamespace(namespace), appinformer.WithTweakListOptions(func(_ *metav1.ListOptions) {}))
		return factory.Argoproj().V1alpha1().Applications().Informer(), factory.Argoproj().V1alpha1().ApplicationSets().Informer()
	}
	keep := func(obj metav1.Object) bool {
		return security.IsNamespaceEnabled(obj.GetNamespace(), namespace, applicationNamespaces)
	}
	appInformer = newFilteredInformer(clientset, clientset.ArgoprojV1alpha1().Applications(metav1.NamespaceAll), &v1alpha1.Application{}, keep)
	appsetInformer = newFilteredInformer(clientset, clientset.ArgoprojV1alpha1().ApplicationSets(metav1.NamespaceAll), &v1alpha1.ApplicationSet{}, keep)
	return appInformer, appsetInformer
}

// listWatchClient is the part of a typed resource client an informer needs.
type listWatchClient[L apiruntime.Object] interface {
	List(ctx context.Context, opts metav1.ListOptions) (L, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
}

// newFilteredInformer returns an informer on client, a resource client of clientset, that only
// caches the objects keep accepts.
func newFilteredInformer[L apiruntime.Object](clientset appclientset.Interface, client listWatchClient[L], objType apiruntime.Object, keep informerutil.KeepFunc) cache.SharedIndexInformer {
	return cache.NewSharedIndexInformer(
		informerutil.NewListerWatcher(&cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (apiruntime.Object, error) {
				list, err := client.List(ctx, options)
				if err != nil {
					return nil, err
				}
				return list, nil
			},
			WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
				return client.Watch(ctx, options)
			},
		}, clientset, keep),
		objType,
		0,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
	)
}
