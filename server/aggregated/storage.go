package aggregated

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/registry/rest"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	views "github.com/argoproj/argo-cd/v3/pkg/apis/views/v1alpha1"
)

// REST serves one kind of projection. It supports the get, list and watch verbs only.
type REST struct {
	kind  *kind
	proj  *projector
	authz appAuthorizer
	hub   *hub
}

var (
	_ rest.Storage              = &REST{}
	_ rest.Scoper               = &REST{}
	_ rest.Getter               = &REST{}
	_ rest.Lister               = &REST{}
	_ rest.Watcher              = &REST{}
	_ rest.SingularNameProvider = &REST{}
	_ rest.ShortNamesProvider   = &REST{}
)

func newREST(k *kind, proj *projector, authz appAuthorizer, h *hub) *REST {
	return &REST{kind: k, proj: proj, authz: authz, hub: h}
}

func (r *REST) New() runtime.Object {
	return r.kind.newObject(metav1.ObjectMeta{})
}

func (r *REST) NewList() runtime.Object {
	return r.kind.newList(nil, "")
}

func (r *REST) Destroy() {}

func (r *REST) NamespaceScoped() bool {
	return true
}

func (r *REST) GetSingularName() string {
	return r.kind.singular
}

func (r *REST) ShortNames() []string {
	return r.kind.shortNames
}

// Get returns the projection of the named Application. Applications the user may not read according to the Argo CD
// RBAC policy are reported as not found, so that their existence is not revealed.
func (r *REST) Get(ctx context.Context, name string, _ *metav1.GetOptions) (runtime.Object, error) {
	namespace, ok := genericapirequest.NamespaceFrom(ctx)
	if !ok || namespace == "" {
		return nil, apierrors.NewBadRequest("namespace is required")
	}
	app, err := r.proj.getApp(r.kind.resource, namespace, name)
	if err != nil {
		return nil, err
	}
	u, _ := genericapirequest.UserFrom(ctx)
	if !r.authz.canGet(u, app) {
		return nil, apierrors.NewNotFound(views.Resource(r.kind.resource), name)
	}
	return r.kind.project(r.proj, app)
}

// List returns the projections of the Applications in the request namespace (or all namespaces) which the user may
// read and which match the label and field selectors.
func (r *REST) List(ctx context.Context, options *metainternalversion.ListOptions) (runtime.Object, error) {
	sel, err := newSelection(genericapirequest.NamespaceValue(ctx), options)
	if err != nil {
		return nil, err
	}
	u, _ := genericapirequest.UserFrom(ctx)
	apps, err := r.proj.listApps(sel.namespace)
	if err != nil {
		return nil, err
	}
	items := make([]runtime.Object, 0, len(apps))
	versions := make(map[string]string, len(apps))
	for _, app := range apps {
		if !sel.matches(app, r.proj.labels(app)) || !r.authz.canGet(u, app) {
			continue
		}
		obj, err := r.kind.project(r.proj, app)
		if err != nil {
			return nil, err
		}
		items = append(items, obj)
		versions[appKey(app.Namespace, app.Name)] = obj.(metav1.Object).GetResourceVersion()
	}
	resourceVersion := listHash(versions)
	r.hub.rememberSnapshot(r.kind, resourceVersion, versions)
	return r.kind.newList(items, resourceVersion), nil
}

func (r *REST) Watch(ctx context.Context, options *metainternalversion.ListOptions) (watch.Interface, error) {
	sel, err := newSelection(genericapirequest.NamespaceValue(ctx), options)
	if err != nil {
		return nil, err
	}
	u, _ := genericapirequest.UserFrom(ctx)
	return r.hub.watch(ctx, r.kind, u, sel, options)
}

func (r *REST) ConvertToTable(_ context.Context, object runtime.Object, _ runtime.Object) (*metav1.Table, error) {
	table := &metav1.Table{ColumnDefinitions: r.kind.columns}
	var items []runtime.Object
	if meta.IsListType(object) {
		items = r.kind.extractItems(object)
		if listAccessor, err := meta.ListAccessor(object); err == nil {
			table.ResourceVersion = listAccessor.GetResourceVersion()
		}
	} else {
		items = []runtime.Object{object}
		if accessor, err := meta.Accessor(object); err == nil {
			table.ResourceVersion = accessor.GetResourceVersion()
		}
	}
	for _, item := range items {
		table.Rows = append(table.Rows, metav1.TableRow{
			Cells:  r.kind.cells(item),
			Object: runtime.RawExtension{Object: item},
		})
	}
	return table, nil
}

// selection holds the namespace, label selector and field selector of a list or watch request.
type selection struct {
	namespace string
	labels    labels.Selector
	fields    fields.Selector
}

// supportedFieldSelectors are the fields which list and watch requests may select on.
var supportedFieldSelectors = map[string]bool{"metadata.name": true, "metadata.namespace": true}

func newSelection(namespace string, options *metainternalversion.ListOptions) (*selection, error) {
	sel := &selection{namespace: namespace, labels: labels.Everything(), fields: fields.Everything()}
	if options == nil {
		return sel, nil
	}
	if options.LabelSelector != nil {
		sel.labels = options.LabelSelector
	}
	if options.FieldSelector != nil {
		for _, requirement := range options.FieldSelector.Requirements() {
			if !supportedFieldSelectors[requirement.Field] {
				return nil, apierrors.NewBadRequest(fmt.Sprintf("field selector %q is not supported, only metadata.name and metadata.namespace are", requirement.Field))
			}
		}
		sel.fields = options.FieldSelector
	}
	return sel, nil
}

// matches returns whether app is in the selected namespace and whether app and the labels of its projection match the
// label and field selectors.
func (s *selection) matches(app *appv1.Application, projectedLabels map[string]string) bool {
	if s.namespace != "" && app.Namespace != s.namespace {
		return false
	}
	return s.labels.Matches(labels.Set(projectedLabels)) &&
		s.fields.Matches(fields.Set{"metadata.name": app.Name, "metadata.namespace": app.Namespace})
}
