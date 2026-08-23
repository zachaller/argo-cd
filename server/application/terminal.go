package application

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"time"

	"github.com/argoproj/argo-cd/gitops-engine/v3/pkg/utils/kube"
	log "github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	cmdutil "k8s.io/kubectl/pkg/cmd/util"
	"k8s.io/streaming/pkg/httpstream"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	applisters "github.com/argoproj/argo-cd/v3/pkg/client/listers/application/v1alpha1"
	"github.com/argoproj/argo-cd/v3/util/argo"
	"github.com/argoproj/argo-cd/v3/util/db"
	"github.com/argoproj/argo-cd/v3/util/rbac"
	"github.com/argoproj/argo-cd/v3/util/security"
	util_session "github.com/argoproj/argo-cd/v3/util/session"
	"github.com/argoproj/argo-cd/v3/util/settings"
)

type terminalHandler struct {
	appLister         applisters.ApplicationLister
	db                db.ArgoDB
	appResourceTreeFn func(ctx context.Context, app *appv1.Application) (*appv1.ApplicationTree, error)
	allowedShells     []string
	namespace         string
	enabledNamespaces []string
	sessionManager    *util_session.SessionManager
	terminalOptions   *TerminalOptions
	// settingsMgr reads the destinations RBAC gates. Without it this path could not take part in
	// the destinations authorization axis at all.
	settingsMgr DestinationRBACGates
}

type TerminalOptions struct {
	DisableAuth bool
	Enf         *rbac.Enforcer
}

// NewHandler returns a new terminal handler.
func NewHandler(appLister applisters.ApplicationLister, namespace string, enabledNamespaces []string, db db.ArgoDB, appResourceTree AppResourceTreeFn, allowedShells []string, sessionManager *util_session.SessionManager, terminalOptions *TerminalOptions, settingsMgr DestinationRBACGates) *terminalHandler {
	return &terminalHandler{
		appLister:         appLister,
		db:                db,
		appResourceTreeFn: appResourceTree,
		allowedShells:     allowedShells,
		namespace:         namespace,
		enabledNamespaces: enabledNamespaces,
		sessionManager:    sessionManager,
		terminalOptions:   terminalOptions,
		settingsMgr:       settingsMgr,
	}
}

// getApplicationClusterRawConfig returns the raw REST config of the destination the pod is in.
// destName is the name of one of the Application's destinations, or the empty string for the
// primary one, so a terminal opens in the cluster the pod actually runs in.
func (s *terminalHandler) getApplicationClusterRawConfig(ctx context.Context, a *appv1.Application, destName string) (*rest.Config, error) {
	destination, err := a.Spec.GetDestination(destName)
	if err != nil {
		return nil, err
	}
	destCluster, err := argo.GetDestinationCluster(ctx, destination, s.db)
	if err != nil {
		return nil, err
	}
	rawConfig, err := destCluster.RawRestConfig()
	if err != nil {
		return nil, err
	}
	return rawConfig, nil
}

type GetSettingsFunc func() (*settings.ArgoCDSettings, error)

// WithFeatureFlagMiddleware is an HTTP middleware to verify if the terminal
// feature is enabled before invoking the main handler
func (s *terminalHandler) WithFeatureFlagMiddleware(getSettings GetSettingsFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		argocdSettings, err := getSettings()
		if err != nil {
			log.Errorf("error executing WithFeatureFlagMiddleware: error getting settings: %s", err)
			http.Error(w, "Failed to get settings", http.StatusBadRequest)
			return
		}
		if !argocdSettings.ExecEnabled {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		s.ServeHTTP(w, r)
	})
}

func (s *terminalHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	podName := q.Get("pod")
	container := q.Get("container")
	app := q.Get("appName")
	project := q.Get("projectName")
	namespace := q.Get("namespace")

	if podName == "" || container == "" || app == "" || project == "" || namespace == "" {
		http.Error(w, "Missing required parameters", http.StatusBadRequest)
		return
	}

	appNamespace := q.Get("appNamespace")
	// Which of the Application's destinations the pod is in. Empty means the client did not say,
	// which is what every client that predates multiple destinations sends; the pod is then looked
	// up across all of them and the request is rejected if that is ambiguous.
	destination := q.Get("destination")

	if !argo.IsValidDestinationSelector(destination) {
		http.Error(w, "Destination name is not valid", http.StatusBadRequest)
		return
	}
	if !argo.IsValidPodName(podName) {
		http.Error(w, "Pod name is not valid", http.StatusBadRequest)
		return
	}
	if !argo.IsValidContainerName(container) {
		http.Error(w, "Container name is not valid", http.StatusBadRequest)
		return
	}
	if !argo.IsValidAppName(app) {
		http.Error(w, "App name is not valid", http.StatusBadRequest)
		return
	}
	if !argo.IsValidProjectName(project) {
		http.Error(w, "Project name is not valid", http.StatusBadRequest)
		return
	}
	if !argo.IsValidNamespaceName(namespace) {
		http.Error(w, "Namespace name is not valid", http.StatusBadRequest)
		return
	}
	if !argo.IsValidNamespaceName(appNamespace) {
		http.Error(w, "App namespace name is not valid", http.StatusBadRequest)
		return
	}

	ns := appNamespace
	if ns == "" {
		ns = s.namespace
	}

	if !security.IsNamespaceEnabled(ns, s.namespace, s.enabledNamespaces) {
		http.Error(w, security.NamespaceNotPermittedError(ns).Error(), http.StatusForbidden)
		return
	}

	shell := q.Get("shell") // No need to validate. Will only be used if it's in the allow-list.

	ctx := r.Context()

	appRBACName := security.RBACName(s.namespace, project, appNamespace, app)
	if err := s.terminalOptions.Enf.EnforceErr(ctx.Value("claims"), rbac.ResourceApplications, rbac.ActionGet, appRBACName); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	if err := s.terminalOptions.Enf.EnforceErr(ctx.Value("claims"), rbac.ResourceExec, rbac.ActionCreate, appRBACName); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	fieldLog := log.WithFields(log.Fields{
		"application": app, "userName": util_session.Username(ctx), "container": container,
		"podName": podName, "namespace": namespace, "project": project, "appNamespace": appNamespace,
	})

	a, err := s.appLister.Applications(ns).Get(app)
	if err != nil {
		if apierrors.IsNotFound(err) {
			http.Error(w, "App not found", http.StatusNotFound)
			return
		}
		fieldLog.Errorf("Error when getting app %q when launching a terminal: %s", app, err)
		http.Error(w, "Cannot get app", http.StatusInternalServerError)
		return
	}

	if a.Spec.Project != project {
		fieldLog.Warnf("The wrong project (%q) was specified for the app %q when launching a terminal", project, app)
		http.Error(w, "The wrong project was specified for the app", http.StatusBadRequest)
		return
	}

	resourceTree, err := s.appResourceTreeFn(ctx, a)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// From the tree find the pod which matches the given pod, in the destination it names. The pod's
	// own destination is what everything below is done against: the cluster the terminal opens in,
	// and the destination the request is authorized against.
	podNode, err := findPodNode(resourceTree.Nodes, podName, namespace, destination)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if podNode == nil {
		http.Error(w, "Pod doesn't belong to specified app", http.StatusBadRequest)
		return
	}

	// The destination the pod is actually in, now that the lookup has established which one it is
	// unambiguously. A destinations policy is written against the same verb as the exec check.
	if err := enforceDestinations(ctx, s.terminalOptions.Enf, s.settingsMgr, project, destinationsNamed(a, podNode.Destination), rbac.ActionCreate); err != nil {
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}

	config, err := s.getApplicationClusterRawConfig(ctx, a, podNode.Destination)
	if err != nil {
		http.Error(w, "Cannot get raw cluster config", http.StatusBadRequest)
		return
	}

	kubeClientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		http.Error(w, "Cannot initialize kubeclient", http.StatusBadRequest)
		return
	}

	pod, err := kubeClientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		fieldLog.Errorf("error retrieving pod: %s", err)
		http.Error(w, "Cannot find pod", http.StatusBadRequest)
		return
	}

	if !containerRunning(pod, container) {
		fieldLog.Warn("terminal container not running")
		http.Error(w, "container find running", http.StatusBadRequest)
		return
	}

	fieldLog.Info("terminal session starting")

	session, err := newTerminalSession(ctx, w, r, nil, s.sessionManager, appRBACName, s.terminalOptions)
	if err != nil {
		http.Error(w, "Failed to start terminal session", http.StatusBadRequest)
		return
	}
	defer session.Done()

	// send pings across the WebSocket channel at regular intervals to keep it alive through
	// load balancers which may close an idle connection after some period of time
	go session.StartKeepalives(time.Second * 5)

	if slices.Contains(s.allowedShells, shell) {
		cmd := []string{shell}
		err = startProcess(ctx, kubeClientset, config, namespace, podName, container, cmd, session)
	} else {
		// No shell given or the given shell was not allowed: try the configured shells until one succeeds or all fail.
		for _, testShell := range s.allowedShells {
			cmd := []string{testShell}
			if err = startProcess(ctx, kubeClientset, config, namespace, podName, container, cmd, session); err == nil {
				break
			}
		}
	}

	if err != nil {
		http.Error(w, "Failed to exec container", http.StatusBadRequest)
		session.Close()
		return
	}

	session.Close()
}

// findPodNode returns the pod's node in the application's resource tree, or nil when the pod is not
// part of the application. destinationSelector narrows the search to one destination; an empty
// selector matches any, so a client that does not send one still finds a pod that exists in only one
// destination. A pod that exists in several is rejected rather than resolved to whichever came
// first: opening a shell in the wrong cluster is worse than refusing to open one.
func findPodNode(treeNodes []appv1.ResourceNode, podName, namespace, destinationSelector string) (*appv1.ResourceNode, error) {
	var found *appv1.ResourceNode
	for i := range treeNodes {
		treeNode := &treeNodes[i]
		if treeNode.Kind != kube.PodKind || treeNode.Group != "" || treeNode.UID == "" ||
			treeNode.Name != podName || treeNode.Namespace != namespace {
			continue
		}
		if !appv1.DestinationSelectorMatches(destinationSelector, treeNode.Destination) {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("pod %s/%s exists in more than one destination; specify which one", namespace, podName)
		}
		found = treeNode
	}
	return found, nil
}

func containerRunning(pod *corev1.Pod, containerName string) bool {
	return containerStatusRunning(pod.Status.ContainerStatuses, containerName) ||
		containerStatusRunning(pod.Status.InitContainerStatuses, containerName)
}

func containerStatusRunning(statuses []corev1.ContainerStatus, containerName string) bool {
	for i := range statuses {
		if statuses[i].Name == containerName {
			return statuses[i].State.Running != nil
		}
	}
	return false
}

const EndOfTransmission = "\u0004"

// PtyHandler is what remotecommand expects from a pty
type PtyHandler interface {
	io.Reader
	io.Writer
	remotecommand.TerminalSizeQueue
}

// TerminalMessage is the struct for websocket message.
type TerminalMessage struct {
	Operation string `json:"operation"`
	Data      string `json:"data"`
	Rows      uint16 `json:"rows"`
	Cols      uint16 `json:"cols"`
}

// TerminalCommand is the struct for websocket commands,For example you need ask client to reconnect
type TerminalCommand struct {
	Code int
}

// startProcess executes specified commands in the container and connects it up with the ptyHandler (a session)
func startProcess(ctx context.Context, k8sClient kubernetes.Interface, cfg *rest.Config, namespace, podName, containerName string, cmd []string, ptyHandler PtyHandler) error {
	req := k8sClient.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec")

	req.VersionedParams(&corev1.PodExecOptions{
		Container: containerName,
		Command:   cmd,
		Stdin:     true,
		Stdout:    true,
		Stderr:    true,
		TTY:       true,
	}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(cfg, "POST", req.URL())
	if err != nil {
		return err
	}

	// Fallback executor is default, unless feature flag is explicitly disabled.
	// Reuse environment variable for kubectl to disable the feature flag, default is enabled.
	if !cmdutil.RemoteCommandWebsockets.IsDisabled() {
		// WebSocketExecutor must be "GET" method as described in RFC 6455 Sec. 4.1 (page 17).
		websocketExec, err := remotecommand.NewWebSocketExecutor(cfg, "GET", req.URL().String())
		if err != nil {
			return err
		}
		exec, err = remotecommand.NewFallbackExecutor(websocketExec, exec, httpstream.IsUpgradeFailure)
		if err != nil {
			return err
		}
	}
	return exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdin:             ptyHandler,
		Stdout:            ptyHandler,
		Stderr:            ptyHandler,
		TerminalSizeQueue: ptyHandler,
		Tty:               true,
	})
}
