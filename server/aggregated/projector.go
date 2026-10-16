package aggregated

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"sort"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	views "github.com/argoproj/argo-cd/v3/pkg/apis/views/v1alpha1"
	applisters "github.com/argoproj/argo-cd/v3/pkg/client/listers/application/v1alpha1"
	servercache "github.com/argoproj/argo-cd/v3/server/cache"
	"github.com/argoproj/argo-cd/v3/util/argo"
	"github.com/argoproj/argo-cd/v3/util/security"
)

// appStateCache is the subset of the API server cache holding the state computed by the application controller.
type appStateCache interface {
	GetAppResourcesTree(appInstanceName string, res *appv1.ApplicationTree) error
	GetAppManagedResources(appInstanceName string, res *[]*appv1.ResourceDiff) error
}

// projector builds the views of Applications from the Application informer and the application controller's cache.
type projector struct {
	appLister applisters.ApplicationLister
	cache     appStateCache
	// namespace is the Argo CD control plane namespace.
	namespace string
	// appNamespaces are the additional namespaces Applications may live in (glob or regexp patterns).
	appNamespaces []string
	// trackingLabelKey returns the label used by Argo CD to track resources. It is not copied to projections, so that a
	// projection can never be mistaken for a resource managed by a parent Application.
	trackingLabelKey func() string
}

func (p *projector) namespaceEnabled(namespace string) bool {
	return security.IsNamespaceEnabled(namespace, p.namespace, p.appNamespaces)
}

// getApp returns the Application with the given namespace and name, or a NotFound error for the given resource.
func (p *projector) getApp(resource, namespace, name string) (*appv1.Application, error) {
	if !p.namespaceEnabled(namespace) {
		return nil, apierrors.NewNotFound(views.Resource(resource), name)
	}
	app, err := p.appLister.Applications(namespace).Get(name)
	if apierrors.IsNotFound(err) {
		return nil, apierrors.NewNotFound(views.Resource(resource), name)
	}
	if err != nil {
		return nil, apierrors.NewInternalError(err)
	}
	return app, nil
}

// listApps returns the Applications in namespace ("" for all namespaces), sorted by namespace and name.
func (p *projector) listApps(namespace string) ([]*appv1.Application, error) {
	var apps []*appv1.Application
	var err error
	if namespace == "" {
		apps, err = p.appLister.List(labels.Everything())
	} else {
		apps, err = p.appLister.Applications(namespace).List(labels.Everything())
	}
	if err != nil {
		return nil, apierrors.NewInternalError(err)
	}
	result := make([]*appv1.Application, 0, len(apps))
	for _, app := range apps {
		if p.namespaceEnabled(app.Namespace) {
			result = append(result, app)
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return appKey(result[i].Namespace, result[i].Name) < appKey(result[j].Namespace, result[j].Name)
	})
	return result, nil
}

// labels returns the labels of the projections of app: the labels of the Application, except for the tracking label.
func (p *projector) labels(app *appv1.Application) map[string]string {
	if len(app.Labels) == 0 {
		return nil
	}
	lbls := maps.Clone(app.Labels)
	if p.trackingLabelKey != nil {
		delete(lbls, p.trackingLabelKey())
	}
	if len(lbls) == 0 {
		return nil
	}
	return lbls
}

// objectMeta returns the metadata of the projection of app. Annotations and owner references are deliberately not
// copied: they would make Argo CD consider the projection part of a parent Application.
func (p *projector) objectMeta(app *appv1.Application) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:              app.Name,
		Namespace:         app.Namespace,
		UID:               app.UID,
		CreationTimestamp: app.CreationTimestamp,
		Labels:            p.labels(app),
	}
}

// status returns the status of a projection of app, given the error returned when reading its state from the cache.
// Errors other than cache misses are returned as is.
func (p *projector) status(app *appv1.Application, cacheErr error) (views.ProjectionStatus, error) {
	status := views.ProjectionStatus{Project: app.Spec.GetProject()}
	switch {
	case cacheErr == nil:
		status.Conditions = []views.ProjectionCondition{{
			Type:   views.ConditionTypeReady,
			Status: metav1.ConditionTrue,
			Reason: views.ReasonReconciled,
		}}
	case errors.Is(cacheErr, servercache.ErrCacheMiss):
		condition := views.ProjectionCondition{
			Type:    views.ConditionTypeReady,
			Status:  metav1.ConditionFalse,
			Reason:  views.ReasonNotReconciled,
			Message: "The application controller has not computed the state of this application yet",
		}
		errConditions := app.Status.GetConditions(map[appv1.ApplicationConditionType]bool{
			appv1.ApplicationConditionComparisonError:  true,
			appv1.ApplicationConditionInvalidSpecError: true,
		})
		if len(errConditions) > 0 {
			condition.Reason = views.ReasonComparisonError
			condition.Message = argo.FormatAppConditions(errConditions)
		}
		status.Conditions = []views.ProjectionCondition{condition}
	default:
		return status, apierrors.NewServiceUnavailable(fmt.Sprintf("failed to read the state of application %s/%s: %v", app.Namespace, app.Name, cacheErr))
	}
	return status, nil
}

// tree returns the ApplicationTree projection of app.
func (p *projector) tree(app *appv1.Application) (*views.ApplicationTree, error) {
	obj := &views.ApplicationTree{ObjectMeta: p.objectMeta(app)}
	err := p.cache.GetAppResourcesTree(app.InstanceName(p.namespace), &obj.Tree)
	if err != nil {
		obj.Tree = appv1.ApplicationTree{}
	}
	if obj.Status, err = p.status(app, err); err != nil {
		return nil, err
	}
	obj.ResourceVersion, err = contentHash(obj)
	if err != nil {
		return nil, apierrors.NewInternalError(err)
	}
	return obj, nil
}

// diff returns the ApplicationDiff projection of app. Hook resources are omitted, like in the ManagedResources API.
func (p *projector) diff(app *appv1.Application) (*views.ApplicationDiff, error) {
	obj := &views.ApplicationDiff{ObjectMeta: p.objectMeta(app)}
	var items []*appv1.ResourceDiff
	err := p.cache.GetAppManagedResources(app.InstanceName(p.namespace), &items)
	obj.Resources = []appv1.ResourceDiff{}
	if err == nil {
		for _, item := range items {
			if item != nil && !item.Hook {
				obj.Resources = append(obj.Resources, *item)
			}
		}
	}
	if obj.Status, err = p.status(app, err); err != nil {
		return nil, err
	}
	obj.ResourceVersion, err = contentHash(obj)
	if err != nil {
		return nil, apierrors.NewInternalError(err)
	}
	return obj, nil
}

// contentHash returns an opaque resourceVersion for obj, derived from its content. The resourceVersion is identical
// on every replica of the server, so clients may resume watches against any replica. obj must not have a
// resourceVersion yet.
func contentHash(obj runtime.Object) (string, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return "", fmt.Errorf("failed to marshal %T: %w", obj, err)
	}
	h := fnv.New64a()
	_, _ = h.Write(data)
	return strconv.FormatUint(h.Sum64(), 16), nil
}

// listHash returns an opaque resourceVersion for a list, derived from the resourceVersions of its items.
func listHash(itemVersions map[string]string) string {
	keys := make([]string, 0, len(itemVersions))
	for key := range itemVersions {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	h := fnv.New64a()
	for _, key := range keys {
		_, _ = h.Write([]byte(key))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(itemVersions[key]))
		_, _ = h.Write([]byte{0})
	}
	return "l" + strconv.FormatUint(h.Sum64(), 16)
}

func appKey(namespace, name string) string {
	return namespace + "/" + name
}

// appKeyFromInstanceName converts an Application instance name (see Application.InstanceName) to a namespace/name key.
// Neither namespaces nor Application names may contain underscores, so the conversion is unambiguous.
func appKeyFromInstanceName(instanceName, controlPlaneNamespace string) string {
	if namespace, name, ok := strings.Cut(instanceName, "_"); ok {
		return appKey(namespace, name)
	}
	return appKey(controlPlaneNamespace, instanceName)
}
