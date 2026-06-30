package apiserver

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"

	"github.com/argoproj/argo-cd/v3/pkg/apis/application"
	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/argoproj/argo-cd/v3/server/aggregator/clusters"
	aggregatorregistry "github.com/argoproj/argo-cd/v3/server/aggregator/registry"
)

var (
	// Scheme is the runtime scheme shared by the aggregated apiserver. It reuses the existing
	// argoproj.io/v1alpha1 types (and their generated DeepCopy) so no new API code is needed.
	Scheme = runtime.NewScheme()
	// Codecs provides serializers for the registered types.
	Codecs = serializer.NewCodecFactory(Scheme)
	// ParameterCodec decodes URL query parameters (list/watch options) into typed objects.
	ParameterCodec = runtime.NewParameterCodec(Scheme)
)

func init() {
	utilruntime.Must(appv1.AddToScheme(Scheme))
	metav1.AddToGroupVersion(Scheme, schema.GroupVersion{Version: "v1"})

	// The generic apiserver expects the meta types to be registered under the unversioned
	// legacy group so it can serve discovery and status responses.
	unversioned := schema.GroupVersion{Group: "", Version: "v1"}
	Scheme.AddUnversionedTypes(unversioned,
		&metav1.Status{},
		&metav1.APIVersions{},
		&metav1.APIGroupList{},
		&metav1.APIGroup{},
		&metav1.APIResourceList{},
	)

	// v1alpha1 is the single served version of the group; mark it as the preferred version so
	// InstallAPIGroup has a prioritized version to advertise.
	utilruntime.Must(Scheme.SetVersionPriority(appv1.SchemeGroupVersion))
}

// ExtraConfig holds aggregator-specific configuration beyond the generic apiserver config.
type ExtraConfig struct {
	// Registry tracks downstream clusters and routes/aggregates resources across them.
	Registry *clusters.Registry
}

// Config defines the config for the aggregator apiserver.
type Config struct {
	GenericConfig *genericapiserver.RecommendedConfig
	ExtraConfig   ExtraConfig
}

// AggregatorServer is the running aggregated apiserver.
type AggregatorServer struct {
	GenericAPIServer *genericapiserver.GenericAPIServer
}

type completedConfig struct {
	GenericConfig genericapiserver.CompletedConfig
	ExtraConfig   *ExtraConfig
}

// CompletedConfig wraps a completed config so it cannot be constructed outside this package.
type CompletedConfig struct {
	*completedConfig
}

// Complete fills in defaults and returns a completed config.
func (cfg *Config) Complete() CompletedConfig {
	c := completedConfig{
		cfg.GenericConfig.Complete(),
		&cfg.ExtraConfig,
	}
	return CompletedConfig{&c}
}

// New builds the aggregated apiserver, installing the argoproj.io group backed by the
// fan-out REST storage for each served resource.
func (c completedConfig) New() (*AggregatorServer, error) {
	genericServer, err := c.GenericConfig.New("argocd-aggregator", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, err
	}

	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(application.Group, Scheme, ParameterCodec, Codecs)

	storage := map[string]rest.Storage{}
	for _, resource := range aggregatorregistry.AllResources() {
		storage[resource.GVR().Resource] = aggregatorregistry.NewStore(resource, c.ExtraConfig.Registry)
	}
	apiGroupInfo.VersionedResourcesStorageMap[appv1.SchemeGroupVersion.Version] = storage

	if err := genericServer.InstallAPIGroup(&apiGroupInfo); err != nil {
		return nil, err
	}

	return &AggregatorServer{GenericAPIServer: genericServer}, nil
}
