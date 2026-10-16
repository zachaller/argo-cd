package aggregated

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/registry/rest"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/healthz"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	"k8s.io/client-go/tools/cache"
	"k8s.io/component-base/compatibility"
	baseversion "k8s.io/component-base/version"

	views "github.com/argoproj/argo-cd/v3/pkg/apis/views/v1alpha1"
	applisters "github.com/argoproj/argo-cd/v3/pkg/client/listers/application/v1alpha1"
	"github.com/argoproj/argo-cd/v3/util/rbac"
)

// DefaultSubjectPrefix is prepended to Kubernetes user and group names before they are evaluated against the Argo CD
// RBAC policy.
const DefaultSubjectPrefix = "k8s:"

// Options holds the serving, authentication and authorization options of the aggregated API server. Requests are
// authenticated and authorized by delegating to kube-apiserver, like every aggregated API server does.
type Options struct {
	SecureServing  *genericoptions.SecureServingOptionsWithLoopback
	Authentication *genericoptions.DelegatingAuthenticationOptions
	Authorization  *genericoptions.DelegatingAuthorizationOptions
}

// NewOptions returns the default options.
func NewOptions() *Options {
	o := &Options{
		SecureServing:  genericoptions.NewSecureServingOptions().WithLoopback(),
		Authentication: genericoptions.NewDelegatingAuthenticationOptions(),
		Authorization:  genericoptions.NewDelegatingAuthorizationOptions(),
	}
	o.SecureServing.BindPort = 8443
	// The self-signed certificate, generated when no certificate is provided, must be written to a writable location.
	o.SecureServing.ServerCert.CertDirectory = "/tmp/argocd-aggregated-apiserver"
	o.SecureServing.ServerCert.PairName = "apiserver"
	return o
}

// AddFlags adds the flags of the options to fs.
func (o *Options) AddFlags(fs *pflag.FlagSet) {
	o.SecureServing.AddFlags(fs)
	o.Authentication.AddFlags(fs)
	o.Authorization.AddFlags(fs)
}

// Config returns the generic API server configuration for the options. If no serving certificate is configured, a
// self-signed certificate is generated.
func (o *Options) Config() (*genericapiserver.RecommendedConfig, error) {
	if err := o.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{net.ParseIP("127.0.0.1")}); err != nil {
		return nil, fmt.Errorf("error creating self-signed certificates: %w", err)
	}
	config := NewConfig()
	if err := o.SecureServing.ApplyTo(&config.SecureServing, &config.LoopbackClientConfig); err != nil {
		return nil, err
	}
	if err := o.Authentication.ApplyTo(&config.Authentication, config.SecureServing, config.OpenAPIConfig); err != nil {
		return nil, err
	}
	if err := o.Authorization.ApplyTo(&config.Authorization); err != nil {
		return nil, err
	}
	return config, nil
}

// NewConfig returns a generic API server configuration without serving, authentication and authorization settings.
func NewConfig() *genericapiserver.RecommendedConfig {
	config := genericapiserver.NewRecommendedConfig(Codecs)
	config.EffectiveVersion = compatibility.NewEffectiveVersionFromString(baseversion.DefaultKubeBinaryVersion, "", "")

	namer := newDefinitionNamer()
	config.OpenAPIConfig = genericapiserver.DefaultOpenAPIConfig(getOpenAPIDefinitions, namer.namer)
	config.OpenAPIConfig.GetDefinitionName = namer.GetDefinitionName
	config.OpenAPIConfig.Info.Title = "Argo CD views"
	config.OpenAPIV3Config = genericapiserver.DefaultOpenAPIV3Config(getOpenAPIDefinitions, namer.namer)
	config.OpenAPIV3Config.GetDefinitionName = namer.GetDefinitionName
	config.OpenAPIV3Config.Info.Title = "Argo CD views"
	// The API is read-only: there is nothing to admit.
	config.AdmissionControl = nil
	return config
}

// Cache is the subset of the API server cache used by the aggregated API server.
type Cache interface {
	appStateCache
	appChangeNotifier
}

// Dependencies are the Argo CD components the aggregated API server reads from.
type Dependencies struct {
	// Namespace is the Argo CD control plane namespace.
	Namespace string
	// ApplicationNamespaces are the additional namespaces Applications may live in.
	ApplicationNamespaces []string
	AppLister             applisters.ApplicationLister
	AppInformer           cache.SharedIndexInformer
	Cache                 Cache
	// Enforcer enforces the Argo CD RBAC policy. Its claims enforcer must be set (see rbacpolicy.RBACPolicyEnforcer).
	Enforcer *rbac.Enforcer
	// SubjectPrefix is prepended to Kubernetes user and group names before they are evaluated against the Argo CD
	// RBAC policy.
	SubjectPrefix string
	// TrackingLabelKey returns the label used by Argo CD to track resources.
	TrackingLabelKey func() string
	// HasSynced reports whether the informers have synced. The server is not ready before.
	HasSynced func() bool

	BookmarkInterval time.Duration
	WatchBufferSize  int
	ResyncPeriod     time.Duration
}

// Server is the aggregated API server serving the views.argoproj.io API group.
type Server struct {
	generic *genericapiserver.GenericAPIServer
	hub     *hub
	cache   Cache
}

// New creates the aggregated API server and installs the views.argoproj.io API group.
func New(config *genericapiserver.RecommendedConfig, deps Dependencies) (*Server, error) {
	if deps.AppLister == nil || deps.Cache == nil || deps.Enforcer == nil {
		return nil, errors.New("application lister, cache and RBAC enforcer are required")
	}
	proj := &projector{
		appLister:        deps.AppLister,
		cache:            deps.Cache,
		namespace:        deps.Namespace,
		appNamespaces:    deps.ApplicationNamespaces,
		trackingLabelKey: deps.TrackingLabelKey,
	}
	authz := &argoRBACAuthorizer{enf: deps.Enforcer, namespace: deps.Namespace, subjectPrefix: deps.SubjectPrefix}
	h := newHub(proj, authz)
	if deps.BookmarkInterval > 0 {
		h.bookmarkInterval = deps.BookmarkInterval
	}
	if deps.WatchBufferSize > 0 {
		h.bufferSize = deps.WatchBufferSize
	}
	if deps.ResyncPeriod > 0 {
		h.resyncPeriod = deps.ResyncPeriod
	}
	if deps.AppInformer != nil {
		if _, err := deps.AppInformer.AddEventHandler(h.eventHandler()); err != nil {
			return nil, fmt.Errorf("error adding application event handler: %w", err)
		}
	}

	generic, err := config.Complete().New("argocd-aggregated-apiserver", genericapiserver.NewEmptyDelegate())
	if err != nil {
		return nil, fmt.Errorf("error creating generic API server: %w", err)
	}
	if deps.HasSynced != nil {
		if err := generic.AddReadyzChecks(healthz.NamedCheck("informer-sync", func(_ *http.Request) error {
			if !deps.HasSynced() {
				return errors.New("informers not synced")
			}
			return nil
		})); err != nil {
			return nil, err
		}
	}

	apiGroupInfo := genericapiserver.NewDefaultAPIGroupInfo(views.Group, Scheme, metav1.ParameterCodec, Codecs)
	apiGroupInfo.VersionedResourcesStorageMap[views.Version] = map[string]rest.Storage{
		views.ApplicationTreeResource: newREST(treeKind, proj, authz, h),
		views.ApplicationDiffResource: newREST(diffKind, proj, authz, h),
	}
	if err := generic.InstallAPIGroup(&apiGroupInfo); err != nil {
		return nil, fmt.Errorf("error installing %s API group: %w", views.Group, err)
	}
	return &Server{generic: generic, hub: h, cache: deps.Cache}, nil
}

// Handler returns the HTTP handler of the server, including authentication and authorization.
func (s *Server) Handler() http.Handler {
	return s.generic.Handler
}

// RunWatchHub runs the watch hub until ctx is done. Run calls it; it is exported for tests which serve Handler only.
func (s *Server) RunWatchHub(ctx context.Context) {
	s.hub.run(ctx, s.cache)
}

// Run serves the API until ctx is done.
func (s *Server) Run(ctx context.Context) error {
	go s.RunWatchHub(ctx)
	return s.generic.PrepareRun().RunWithContext(ctx)
}
