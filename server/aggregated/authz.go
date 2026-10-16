package aggregated

import (
	"strings"

	"github.com/golang-jwt/jwt/v5"
	log "github.com/sirupsen/logrus"
	"k8s.io/apiserver/pkg/authentication/user"

	appv1 "github.com/argoproj/argo-cd/v3/pkg/apis/application/v1alpha1"
	"github.com/argoproj/argo-cd/v3/util/rbac"
)

// projectSubjectPrefix is the prefix of the subjects of Argo CD project tokens (proj:<project>:<role>).
const projectSubjectPrefix = "proj:"

// appAuthorizer decides whether a Kubernetes user may read the projections of an Application.
type appAuthorizer interface {
	canGet(u user.Info, app *appv1.Application) bool
}

// argoRBACAuthorizer enforces the Argo CD RBAC policy (argocd-rbac-cm and AppProject roles) on top of the Kubernetes
// RBAC which kube-apiserver already enforced. The Kubernetes user name and groups become the Argo CD subject and
// groups, prefixed with subjectPrefix so that Kubernetes identities can never be mistaken for Argo CD local users
// (such as "admin") or SSO groups.
type argoRBACAuthorizer struct {
	enf *rbac.Enforcer
	// namespace is the Argo CD control plane namespace.
	namespace     string
	subjectPrefix string
}

// claimsFor converts a Kubernetes user to the claims evaluated by the Argo CD RBAC policy. The groups are always
// stored in the "groups" claim, which the enforcer is configured to read.
func (a *argoRBACAuthorizer) claimsFor(u user.Info) (jwt.MapClaims, bool) {
	if u == nil || u.GetName() == "" {
		return nil, false
	}
	subject := a.subjectPrefix + u.GetName()
	// Project token subjects are enforced against the project's JWT tokens, never against a Kubernetes user.
	if strings.HasPrefix(subject, projectSubjectPrefix) {
		return nil, false
	}
	groups := make([]any, 0, len(u.GetGroups()))
	for _, group := range u.GetGroups() {
		groups = append(groups, a.subjectPrefix+group)
	}
	return jwt.MapClaims{"sub": subject, "groups": groups}, true
}

func (a *argoRBACAuthorizer) canGet(u user.Info, app *appv1.Application) bool {
	claims, ok := a.claimsFor(u)
	if !ok {
		return false
	}
	allowed := a.enf.Enforce(claims, rbac.ResourceApplications, rbac.ActionGet, app.RBACName(a.namespace))
	if !allowed {
		log.WithFields(log.Fields{"user": claims["sub"], "application": app.Name, "namespace": app.Namespace}).
			Debug("Argo CD RBAC denied access to application projection")
	}
	return allowed
}
