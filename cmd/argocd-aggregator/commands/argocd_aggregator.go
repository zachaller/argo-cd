package commands

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/spf13/cobra"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	genericapiserver "k8s.io/apiserver/pkg/server"
	genericoptions "k8s.io/apiserver/pkg/server/options"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	basecompatibility "k8s.io/component-base/compatibility"
	baseversion "k8s.io/component-base/version"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/argoproj/argo-cd/v3/server/aggregator/apiserver"
	"github.com/argoproj/argo-cd/v3/server/aggregator/clusters"
	"github.com/argoproj/argo-cd/v3/util/db"
	"github.com/argoproj/argo-cd/v3/util/settings"
)

// options holds the state for the aggregator apiserver command.
type options struct {
	RecommendedOptions *genericoptions.RecommendedOptions
	// Namespace is the Argo CD control-plane namespace where downstream cluster secrets live.
	Namespace string

	registry *clusters.Registry
}

func newOptions() *options {
	o := &options{
		RecommendedOptions: genericoptions.NewRecommendedOptions("", apiserver.Codecs.LegacyCodec(appv1.SchemeGroupVersion)),
		Namespace:          defaultNamespace(),
	}
	// The aggregator never persists objects itself: every resource is proxied to a downstream
	// cluster, so there is no etcd backend.
	o.RecommendedOptions.Etcd = nil
	return o
}

// NewCommand returns the cobra command that runs the Argo CD multi-cluster aggregated API
// server.
func NewCommand() *cobra.Command {
	o := newOptions()
	cmd := &cobra.Command{
		Use:   "argocd-aggregator",
		Short: "Run the Argo CD multi-cluster aggregated API server",
		Long: "argocd-aggregator is a Kubernetes aggregated API server that presents a unified, " +
			"read/write view of argoproj.io resources (Applications, ApplicationSets, AppProjects) " +
			"across every cluster registered in Argo CD, for UI centralization.",
		RunE: func(c *cobra.Command, _ []string) error {
			if err := o.Complete(); err != nil {
				return err
			}
			if err := o.Validate(); err != nil {
				return err
			}
			return o.Run(c.Context())
		},
	}

	flags := cmd.Flags()
	o.RecommendedOptions.AddFlags(flags)
	flags.StringVar(&o.Namespace, "argocd-namespace", o.Namespace, "Namespace where Argo CD cluster secrets are stored")
	return cmd
}

// Complete wires up the downstream cluster registry from the central cluster's Argo CD
// configuration.
func (o *options) Complete() error {
	restConfig, err := loadRESTConfig()
	if err != nil {
		return fmt.Errorf("failed to load kube config: %w", err)
	}
	kubeClient, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return fmt.Errorf("failed to build kube client: %w", err)
	}
	settingsMgr := settings.NewSettingsManager(context.Background(), kubeClient, o.Namespace)
	argoDB := db.NewDB(o.Namespace, settingsMgr, kubeClient)
	o.registry = clusters.NewRegistry(argoDB)
	return nil
}

// Validate validates the apiserver options.
func (o *options) Validate() error {
	return utilerrors.NewAggregate(o.RecommendedOptions.Validate())
}

// Config assembles the generic apiserver config with delegated authn/authz and the
// aggregator extra config.
func (o *options) Config() (*apiserver.Config, error) {
	if err := o.RecommendedOptions.SecureServing.MaybeDefaultWithSelfSignedCerts("localhost", nil, []net.IP{net.ParseIP("127.0.0.1")}); err != nil {
		return nil, fmt.Errorf("error creating self-signed certificates: %w", err)
	}

	serverConfig := genericapiserver.NewRecommendedConfig(apiserver.Codecs)
	serverConfig.EffectiveVersion = basecompatibility.NewEffectiveVersionFromString(baseversion.DefaultKubeBinaryVersion, "", "")
	serverConfig.FeatureGate = utilfeature.DefaultFeatureGate

	if err := o.RecommendedOptions.ApplyTo(serverConfig); err != nil {
		return nil, err
	}

	return &apiserver.Config{
		GenericConfig: serverConfig,
		ExtraConfig:   apiserver.ExtraConfig{Registry: o.registry},
	}, nil
}

// Run starts the aggregated apiserver and blocks until the context is cancelled.
func (o *options) Run(ctx context.Context) error {
	config, err := o.Config()
	if err != nil {
		return err
	}
	server, err := config.Complete().New()
	if err != nil {
		return err
	}
	return server.GenericAPIServer.PrepareRun().RunWithContext(ctx)
}

// loadRESTConfig loads the kube config for the central cluster, falling back to in-cluster
// configuration when running as a pod.
func loadRESTConfig() (*restclient.Config, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	clientConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, &clientcmd.ConfigOverrides{})
	return clientConfig.ClientConfig()
}

// defaultNamespace resolves the Argo CD namespace from the environment, defaulting to
// "argocd".
func defaultNamespace() string {
	if ns := os.Getenv("ARGOCD_NAMESPACE"); ns != "" {
		return ns
	}
	if ns := os.Getenv("POD_NAMESPACE"); ns != "" {
		return ns
	}
	return "argocd"
}
