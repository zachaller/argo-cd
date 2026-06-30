package registry

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"

	"github.com/argoproj/argo-cd/v3/pkg/apis/application"
	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	versioned "github.com/argoproj/argo-cd/v3/pkg/client/clientset/versioned"
)

// Resource abstracts the per-kind typed-client calls the aggregator needs so that the
// generic Store can fan out, translate and proxy without knowing which argoproj resource it
// is serving. Each implementation simply delegates to the generated namespaced client.
type Resource interface {
	GVR() schema.GroupVersionResource
	Kind() string
	Singular() string
	NewEmpty() runtime.Object
	NewList() runtime.Object

	List(ctx context.Context, c versioned.Interface, namespace string, opts metav1.ListOptions) (runtime.Object, error)
	Get(ctx context.Context, c versioned.Interface, namespace, name string, opts metav1.GetOptions) (runtime.Object, error)
	Create(ctx context.Context, c versioned.Interface, namespace string, obj runtime.Object, opts metav1.CreateOptions) (runtime.Object, error)
	Update(ctx context.Context, c versioned.Interface, namespace string, obj runtime.Object, opts metav1.UpdateOptions) (runtime.Object, error)
	Delete(ctx context.Context, c versioned.Interface, namespace, name string, opts metav1.DeleteOptions) error
	DeleteCollection(ctx context.Context, c versioned.Interface, namespace string, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error
	Watch(ctx context.Context, c versioned.Interface, namespace string, opts metav1.ListOptions) (watch.Interface, error)
}

// AllResources returns the Resource adapters for every argoproj kind the aggregator serves.
func AllResources() []Resource {
	return []Resource{applicationResource{}, appProjectResource{}, applicationSetResource{}}
}

// --- Application ---

type applicationResource struct{}

func (applicationResource) GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: application.Group, Version: "v1alpha1", Resource: application.ApplicationPlural}
}
func (applicationResource) Kind() string             { return application.ApplicationKind }
func (applicationResource) Singular() string         { return application.ApplicationSingular }
func (applicationResource) NewEmpty() runtime.Object { return &appv1.Application{} }
func (applicationResource) NewList() runtime.Object  { return &appv1.ApplicationList{} }

func (applicationResource) List(ctx context.Context, c versioned.Interface, namespace string, opts metav1.ListOptions) (runtime.Object, error) {
	return c.ArgoprojV1alpha1().Applications(namespace).List(ctx, opts)
}
func (applicationResource) Get(ctx context.Context, c versioned.Interface, namespace, name string, opts metav1.GetOptions) (runtime.Object, error) {
	return c.ArgoprojV1alpha1().Applications(namespace).Get(ctx, name, opts)
}
func (applicationResource) Create(ctx context.Context, c versioned.Interface, namespace string, obj runtime.Object, opts metav1.CreateOptions) (runtime.Object, error) {
	app, ok := obj.(*appv1.Application)
	if !ok {
		return nil, fmt.Errorf("expected *Application, got %T", obj)
	}
	return c.ArgoprojV1alpha1().Applications(namespace).Create(ctx, app, opts)
}
func (applicationResource) Update(ctx context.Context, c versioned.Interface, namespace string, obj runtime.Object, opts metav1.UpdateOptions) (runtime.Object, error) {
	app, ok := obj.(*appv1.Application)
	if !ok {
		return nil, fmt.Errorf("expected *Application, got %T", obj)
	}
	return c.ArgoprojV1alpha1().Applications(namespace).Update(ctx, app, opts)
}
func (applicationResource) Delete(ctx context.Context, c versioned.Interface, namespace, name string, opts metav1.DeleteOptions) error {
	return c.ArgoprojV1alpha1().Applications(namespace).Delete(ctx, name, opts)
}
func (applicationResource) DeleteCollection(ctx context.Context, c versioned.Interface, namespace string, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return c.ArgoprojV1alpha1().Applications(namespace).DeleteCollection(ctx, opts, listOpts)
}
func (applicationResource) Watch(ctx context.Context, c versioned.Interface, namespace string, opts metav1.ListOptions) (watch.Interface, error) {
	return c.ArgoprojV1alpha1().Applications(namespace).Watch(ctx, opts)
}

// --- AppProject ---

type appProjectResource struct{}

func (appProjectResource) GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: application.Group, Version: "v1alpha1", Resource: application.AppProjectPlural}
}
func (appProjectResource) Kind() string             { return application.AppProjectKind }
func (appProjectResource) Singular() string         { return application.AppProjectSingular }
func (appProjectResource) NewEmpty() runtime.Object { return &appv1.AppProject{} }
func (appProjectResource) NewList() runtime.Object  { return &appv1.AppProjectList{} }

func (appProjectResource) List(ctx context.Context, c versioned.Interface, namespace string, opts metav1.ListOptions) (runtime.Object, error) {
	return c.ArgoprojV1alpha1().AppProjects(namespace).List(ctx, opts)
}
func (appProjectResource) Get(ctx context.Context, c versioned.Interface, namespace, name string, opts metav1.GetOptions) (runtime.Object, error) {
	return c.ArgoprojV1alpha1().AppProjects(namespace).Get(ctx, name, opts)
}
func (appProjectResource) Create(ctx context.Context, c versioned.Interface, namespace string, obj runtime.Object, opts metav1.CreateOptions) (runtime.Object, error) {
	proj, ok := obj.(*appv1.AppProject)
	if !ok {
		return nil, fmt.Errorf("expected *AppProject, got %T", obj)
	}
	return c.ArgoprojV1alpha1().AppProjects(namespace).Create(ctx, proj, opts)
}
func (appProjectResource) Update(ctx context.Context, c versioned.Interface, namespace string, obj runtime.Object, opts metav1.UpdateOptions) (runtime.Object, error) {
	proj, ok := obj.(*appv1.AppProject)
	if !ok {
		return nil, fmt.Errorf("expected *AppProject, got %T", obj)
	}
	return c.ArgoprojV1alpha1().AppProjects(namespace).Update(ctx, proj, opts)
}
func (appProjectResource) Delete(ctx context.Context, c versioned.Interface, namespace, name string, opts metav1.DeleteOptions) error {
	return c.ArgoprojV1alpha1().AppProjects(namespace).Delete(ctx, name, opts)
}
func (appProjectResource) DeleteCollection(ctx context.Context, c versioned.Interface, namespace string, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return c.ArgoprojV1alpha1().AppProjects(namespace).DeleteCollection(ctx, opts, listOpts)
}
func (appProjectResource) Watch(ctx context.Context, c versioned.Interface, namespace string, opts metav1.ListOptions) (watch.Interface, error) {
	return c.ArgoprojV1alpha1().AppProjects(namespace).Watch(ctx, opts)
}

// --- ApplicationSet ---

type applicationSetResource struct{}

func (applicationSetResource) GVR() schema.GroupVersionResource {
	return schema.GroupVersionResource{Group: application.Group, Version: "v1alpha1", Resource: application.ApplicationSetPlural}
}
func (applicationSetResource) Kind() string             { return application.ApplicationSetKind }
func (applicationSetResource) Singular() string         { return application.ApplicationSetSingular }
func (applicationSetResource) NewEmpty() runtime.Object { return &appv1.ApplicationSet{} }
func (applicationSetResource) NewList() runtime.Object  { return &appv1.ApplicationSetList{} }

func (applicationSetResource) List(ctx context.Context, c versioned.Interface, namespace string, opts metav1.ListOptions) (runtime.Object, error) {
	return c.ArgoprojV1alpha1().ApplicationSets(namespace).List(ctx, opts)
}
func (applicationSetResource) Get(ctx context.Context, c versioned.Interface, namespace, name string, opts metav1.GetOptions) (runtime.Object, error) {
	return c.ArgoprojV1alpha1().ApplicationSets(namespace).Get(ctx, name, opts)
}
func (applicationSetResource) Create(ctx context.Context, c versioned.Interface, namespace string, obj runtime.Object, opts metav1.CreateOptions) (runtime.Object, error) {
	appset, ok := obj.(*appv1.ApplicationSet)
	if !ok {
		return nil, fmt.Errorf("expected *ApplicationSet, got %T", obj)
	}
	return c.ArgoprojV1alpha1().ApplicationSets(namespace).Create(ctx, appset, opts)
}
func (applicationSetResource) Update(ctx context.Context, c versioned.Interface, namespace string, obj runtime.Object, opts metav1.UpdateOptions) (runtime.Object, error) {
	appset, ok := obj.(*appv1.ApplicationSet)
	if !ok {
		return nil, fmt.Errorf("expected *ApplicationSet, got %T", obj)
	}
	return c.ArgoprojV1alpha1().ApplicationSets(namespace).Update(ctx, appset, opts)
}
func (applicationSetResource) Delete(ctx context.Context, c versioned.Interface, namespace, name string, opts metav1.DeleteOptions) error {
	return c.ArgoprojV1alpha1().ApplicationSets(namespace).Delete(ctx, name, opts)
}
func (applicationSetResource) DeleteCollection(ctx context.Context, c versioned.Interface, namespace string, opts metav1.DeleteOptions, listOpts metav1.ListOptions) error {
	return c.ArgoprojV1alpha1().ApplicationSets(namespace).DeleteCollection(ctx, opts, listOpts)
}
func (applicationSetResource) Watch(ctx context.Context, c versioned.Interface, namespace string, opts metav1.ListOptions) (watch.Interface, error) {
	return c.ArgoprojV1alpha1().ApplicationSets(namespace).Watch(ctx, opts)
}
