package cluster

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/argoproj/argo-cd/v3/common"
	"github.com/argoproj/argo-cd/v3/util/clusterauth"
)

// SecondClusterKubeconfigEnvVar names a kubeconfig for a second, genuinely separate Kubernetes
// cluster.
//
// A multi-destination Application needs one: Argo CD reads live objects once per cluster and
// attributes them to an Application by an annotation that names the Application rather than the
// destination, so two destinations backed by the same API server would each see the other's
// resources as extras. Registering one API server twice under two URLs does not help for the same
// reason -- the two caches watch the same objects.
const SecondClusterKubeconfigEnvVar = "ARGOCD_E2E_SECOND_CLUSTER_KUBECONFIG"

// SecondCluster is a second cluster registered with Argo CD, for tests that need an Application to
// span more than one.
type SecondCluster struct {
	// Server is the API server URL Argo CD knows the cluster by.
	Server string
	// Client talks to the second cluster directly, so a test can assert where a resource landed
	// rather than trusting Argo CD's own reporting.
	Client kubernetes.Interface
}

// SkipUnlessSecondCluster skips the test unless a second cluster is configured. The second cluster
// is optional so that `make start-e2e` keeps working with one cluster; CI provides it.
func SkipUnlessSecondCluster(t *testing.T) {
	t.Helper()
	if os.Getenv(SecondClusterKubeconfigEnvVar) == "" {
		t.Skipf("no second cluster: set %s to a kubeconfig for one", SecondClusterKubeconfigEnvVar)
	}
}

// EnsureSecondCluster registers the second cluster with Argo CD and returns a handle to it. It is
// idempotent: the cluster is upserted, so tests may call it freely.
func EnsureSecondCluster(t *testing.T) *SecondCluster {
	t.Helper()
	SkipUnlessSecondCluster(t)

	kubeconfig := os.Getenv(SecondClusterKubeconfigEnvVar)
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		t.Fatalf("could not read second cluster kubeconfig %q: %v", kubeconfig, err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		t.Fatalf("could not build a client for the second cluster: %v", err)
	}

	// A bearer token for a service account with cluster-manager permissions, which is how Argo CD
	// reaches any cluster that is not the one it runs in.
	token, err := clusterauth.InstallClusterManagerRBAC(client, "kube-system", []string{}, common.BearerTokenTimeout)
	if err != nil {
		t.Fatalf("could not install cluster manager RBAC on the second cluster: %v", err)
	}

	Given(t).
		Name("second-cluster").
		Server(config.Host).
		BearerToken(token).
		Upsert(true).
		When().
		Create()

	return &SecondCluster{Server: config.Host, Client: client}
}

// RecreateNamespace gives the test an empty namespace in the second cluster. EnsureCleanState only
// cleans the cluster Argo CD runs in, so anything a previous test left here would otherwise be
// mistaken for the current test's own resources.
func (c *SecondCluster) RecreateNamespace(t *testing.T, namespace string) {
	t.Helper()
	ctx := context.Background()

	err := c.Client.CoreV1().Namespaces().Delete(ctx, namespace, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		t.Fatalf("could not delete namespace %q in the second cluster: %v", namespace, err)
	}
	// Wait for the delete to finish before recreating; a namespace cannot be created while an
	// earlier one of the same name is still terminating.
	if err == nil {
		if err := waitForNamespaceGone(ctx, c.Client, namespace); err != nil {
			t.Fatalf("namespace %q in the second cluster did not go away: %v", namespace, err)
		}
	}

	_, err = c.Client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: namespace},
	}, metav1.CreateOptions{})
	if err != nil && !errors.IsAlreadyExists(err) {
		t.Fatalf("could not create namespace %q in the second cluster: %v", namespace, err)
	}
}

// HasConfigMap reports whether the named ConfigMap exists in the second cluster. It is how a test
// proves a manifest was routed to this cluster rather than to the primary destination.
func (c *SecondCluster) HasConfigMap(t *testing.T, namespace, name string) bool {
	t.Helper()
	_, err := c.Client.CoreV1().ConfigMaps(namespace).Get(context.Background(), name, metav1.GetOptions{})
	if err == nil {
		return true
	}
	if !errors.IsNotFound(err) {
		t.Fatalf("could not read ConfigMap %s/%s in the second cluster: %v", namespace, name, err)
	}
	return false
}

func waitForNamespaceGone(ctx context.Context, client kubernetes.Interface, namespace string) error {
	for range 60 {
		_, err := client.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
		if errors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		time.Sleep(time.Second)
	}
	return fmt.Errorf("namespace %q still present after waiting", namespace)
}
