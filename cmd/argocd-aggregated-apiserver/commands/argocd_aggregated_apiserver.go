package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	informersv1 "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	cmdutil "github.com/argoproj/argo-cd/v3/cmd/util"
	"github.com/argoproj/argo-cd/v3/common"
	"github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	appclientset "github.com/argoproj/argo-cd/v3/pkg/client/clientset/versioned"
	appinformer "github.com/argoproj/argo-cd/v3/pkg/client/informers/externalversions"
	"github.com/argoproj/argo-cd/v3/server/aggregated"
	servercache "github.com/argoproj/argo-cd/v3/server/cache"
	"github.com/argoproj/argo-cd/v3/server/rbacpolicy"
	"github.com/argoproj/argo-cd/v3/util/assets"
	appstatecache "github.com/argoproj/argo-cd/v3/util/cache/appstate"
	"github.com/argoproj/argo-cd/v3/util/cli"
	"github.com/argoproj/argo-cd/v3/util/env"
	"github.com/argoproj/argo-cd/v3/util/errors"
	"github.com/argoproj/argo-cd/v3/util/rbac"
	"github.com/argoproj/argo-cd/v3/util/security"
)

// appInstanceLabelKeySetting is the argocd-cm key configuring the label used to track resources.
const appInstanceLabelKeySetting = "application.instanceLabelKey"

// NewCommand returns the argocd-aggregated-apiserver command
func NewCommand() *cobra.Command {
	var (
		clientConfig          clientcmd.ClientConfig
		cacheSrc              func() (*appstatecache.Cache, error)
		glogLevel             int
		applicationNamespaces []string
		subjectPrefix         string
		bookmarkInterval      time.Duration
		watchBufferSize       int
		resyncPeriod          time.Duration
		options               = aggregated.NewOptions()
	)
	command := &cobra.Command{
		Use:               common.CommandAggregatedAPIServer,
		Short:             "Run the Argo CD aggregated API server",
		Long:              "The Argo CD aggregated API server serves read-only views of the state of Applications, such as their resource trees and diffs, as Kubernetes resources of the views.argoproj.io API group.",
		DisableAutoGenTag: true,
		RunE: func(c *cobra.Command, _ []string) error {
			ctx, cancel := signal.NotifyContext(c.Context(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			vers := common.GetVersion()
			namespace, _, err := clientConfig.Namespace()
			errors.CheckError(err)
			vers.LogStartupInfo("Argo CD Aggregated API Server", map[string]any{
				"namespace": namespace,
				"port":      options.SecureServing.BindPort,
			})

			cli.SetLogFormat(cmdutil.LogFormat)
			cli.SetLogLevel(cmdutil.LogLevel)
			cli.SetGLogLevel(glogLevel)

			config, err := clientConfig.ClientConfig()
			errors.CheckError(err)
			errors.CheckError(v1alpha1.SetK8SConfigDefaults(config))
			config.UserAgent = fmt.Sprintf("%s/%s (%s)", common.CommandAggregatedAPIServer, vers.Version, vers.Platform)
			kubeclientset := kubernetes.NewForConfigOrDie(config)
			appClientset := appclientset.NewForConfigOrDie(config)

			stateCache, err := cacheSrc()
			errors.CheckError(err)

			appInformerNs := namespace
			if len(applicationNamespaces) > 0 {
				appInformerNs = ""
			}
			projFactory := appinformer.NewSharedInformerFactoryWithOptions(appClientset, 0, appinformer.WithNamespace(namespace))
			appFactory := appinformer.NewSharedInformerFactoryWithOptions(appClientset, 0, appinformer.WithNamespace(appInformerNs))
			projInformer := projFactory.Argoproj().V1alpha1().AppProjects().Informer()
			projLister := projFactory.Argoproj().V1alpha1().AppProjects().Lister().AppProjects(namespace)
			appInformer := appFactory.Argoproj().V1alpha1().Applications().Informer()
			appLister := appFactory.Argoproj().V1alpha1().Applications().Lister()
			if len(applicationNamespaces) > 0 {
				// Do not cache Applications of namespaces which are not served.
				errors.CheckError(appInformer.SetTransform(func(obj any) (any, error) {
					if app, ok := obj.(*v1alpha1.Application); ok && !security.IsNamespaceEnabled(app.Namespace, namespace, applicationNamespaces) {
						return &v1alpha1.Application{Name: app.Name, Namespace: app.Namespace}, nil
					}
					return obj, nil
				}))
			}

			enf := rbac.NewEnforcer(kubeclientset, namespace, common.ArgoCDRBACConfigMapName, nil)
			errors.CheckError(enf.SetBuiltinPolicy(assets.BuiltinPolicyCSV))
			enf.EnableLog(os.Getenv(common.EnvVarRBACDebug) == "1")
			policyEnf := rbacpolicy.NewRBACPolicyEnforcer(enf, projLister)
			// The claims are built from Kubernetes users by the aggregated API server: groups are always in "groups".
			policyEnf.SetScopes([]string{"groups"})
			enf.SetClaimsEnforcerFunc(policyEnf.EnforceClaims)

			// Only argocd-cm is read (instead of using the settings manager), so that no access to Secrets is needed.
			cmInformer := informersv1.NewFilteredConfigMapInformer(kubeclientset, namespace, 0, cache.Indexers{}, func(options *metav1.ListOptions) {
				options.FieldSelector = fields.OneTermEqualSelector("metadata.name", common.ArgoCDConfigMapName).String()
			})
			trackingLabelKey := func() string {
				obj, exists, err := cmInformer.GetStore().GetByKey(namespace + "/" + common.ArgoCDConfigMapName)
				if err == nil && exists {
					if key := obj.(*corev1.ConfigMap).Data[appInstanceLabelKeySetting]; key != "" {
						return key
					}
				}
				return common.LabelKeyAppInstance
			}

			serverConfig, err := options.Config()
			errors.CheckError(err)
			server, err := aggregated.New(serverConfig, aggregated.Dependencies{
				Namespace:             namespace,
				ApplicationNamespaces: applicationNamespaces,
				AppLister:             appLister,
				AppInformer:           appInformer,
				Cache:                 servercache.NewCache(stateCache, 0, 0),
				Enforcer:              enf,
				SubjectPrefix:         subjectPrefix,
				TrackingLabelKey:      trackingLabelKey,
				HasSynced: func() bool {
					return appInformer.HasSynced() && projInformer.HasSynced() && cmInformer.HasSynced()
				},
				BookmarkInterval: bookmarkInterval,
				WatchBufferSize:  watchBufferSize,
				ResyncPeriod:     resyncPeriod,
			})
			errors.CheckError(err)

			go cmInformer.Run(ctx.Done())
			go projInformer.Run(ctx.Done())
			go appInformer.Run(ctx.Done())
			go func() {
				errors.CheckError(enf.RunPolicyLoader(ctx, func(_ *corev1.ConfigMap) error { return nil }))
			}()
			if !cache.WaitForCacheSync(ctx.Done(), appInformer.HasSynced, projInformer.HasSynced, cmInformer.HasSynced) {
				return context.Cause(ctx)
			}
			log.Infof("Serving %s", "views.argoproj.io/v1alpha1")
			return server.Run(ctx)
		},
	}

	clientConfig = cli.AddKubectlFlagsToCmd(command)
	options.AddFlags(command.Flags())
	command.Flags().StringVar(&cmdutil.LogFormat, "logformat", env.StringFromEnv("ARGOCD_AGGREGATED_APISERVER_LOGFORMAT", "json"), "Set the logging format. One of: json|text")
	command.Flags().StringVar(&cmdutil.LogLevel, "loglevel", env.StringFromEnv("ARGOCD_AGGREGATED_APISERVER_LOGLEVEL", "info"), "Set the logging level. One of: debug|info|warn|error")
	command.Flags().IntVar(&glogLevel, "gloglevel", 0, "Set the glog logging level")
	command.Flags().StringSliceVar(&applicationNamespaces, "application-namespaces", env.StringsFromEnv("ARGOCD_APPLICATION_NAMESPACES", []string{}, ","), "List of additional namespaces whose Applications are served")
	command.Flags().StringVar(&subjectPrefix, "rbac-subject-prefix", env.StringFromEnv("ARGOCD_AGGREGATED_APISERVER_RBAC_SUBJECT_PREFIX", aggregated.DefaultSubjectPrefix), "Prefix added to Kubernetes user and group names before they are evaluated against the Argo CD RBAC policy")
	command.Flags().DurationVar(&bookmarkInterval, "watch-bookmark-interval", env.ParseDurationFromEnv("ARGOCD_AGGREGATED_APISERVER_WATCH_BOOKMARK_INTERVAL", time.Minute, 0, time.Hour), "Interval between watch bookmarks, for clients requesting them")
	command.Flags().IntVar(&watchBufferSize, "watch-buffer-size", env.ParseNumFromEnv("ARGOCD_AGGREGATED_APISERVER_WATCH_BUFFER_SIZE", 100, 1, 100000), "Number of events buffered per watch before a slow client is disconnected")
	command.Flags().DurationVar(&resyncPeriod, "watch-resync-period", env.ParseDurationFromEnv("ARGOCD_AGGREGATED_APISERVER_WATCH_RESYNC_PERIOD", 3*time.Minute, time.Second, 24*time.Hour), "Interval at which watched projections are recomputed, to catch lost change notifications")
	cacheSrc = appstatecache.AddCacheFlagsToCmd(command)
	return command
}
