# `argocd-aggregated-apiserver` Command Reference

## argocd-aggregated-apiserver

Run the Argo CD aggregated API server

### Synopsis

The Argo CD aggregated API server serves read-only views of the state of Applications, such as their resource trees and diffs, as Kubernetes resources of the views.argoproj.io API group.

```
argocd-aggregated-apiserver [flags]
```

### Options

```
      --app-state-cache-expiration duration                     Cache expiration for app state (default 1h0m0s)
      --application-namespaces strings                          List of additional namespaces whose Applications are served
      --as string                                               Username to impersonate for the operation
      --as-group stringArray                                    Group to impersonate for the operation, this flag can be repeated to specify multiple groups.
      --as-uid string                                           UID to impersonate for the operation
      --authentication-kubeconfig string                        kubeconfig file pointing at the 'core' kubernetes server with enough rights to create tokenreviews.authentication.k8s.io.
      --authentication-skip-lookup                              If false, the authentication-kubeconfig will be used to lookup missing authentication configuration from the cluster.
      --authentication-token-webhook-cache-ttl duration         The duration to cache responses from the webhook token authenticator. (default 10s)
      --authentication-tolerate-lookup-failure                  If true, failures to look up missing authentication configuration from the cluster are not considered fatal. Note that this can result in authentication that treats all requests as anonymous.
      --authorization-always-allow-paths strings                A list of HTTP paths to skip during authorization, i.e. these are authorized without contacting the 'core' kubernetes server. (default [/healthz,/readyz,/livez])
      --authorization-kubeconfig string                         kubeconfig file pointing at the 'core' kubernetes server with enough rights to create subjectaccessreviews.authorization.k8s.io.
      --authorization-webhook-cache-authorized-ttl duration     The duration to cache 'authorized' responses from the webhook authorizer. (default 10s)
      --authorization-webhook-cache-unauthorized-ttl duration   The duration to cache 'unauthorized' responses from the webhook authorizer. (default 10s)
      --bind-address ip                                         The IP address on which to listen for the --secure-port port. The associated interface(s) must be reachable by the rest of the cluster, and by CLI/web clients. If blank or an unspecified address (0.0.0.0 or ::), all interfaces and IP address families will be used. (default 0.0.0.0)
      --cert-dir string                                         The directory where the TLS certs are located. If --tls-cert-file and --tls-private-key-file are provided, this flag will be ignored. (default "/tmp/argocd-aggregated-apiserver")
      --certificate-authority string                            Path to a cert file for the certificate authority
      --client-ca-file string                                   If set, any request presenting a client certificate signed by one of the authorities in the client-ca-file is authenticated with an identity corresponding to the CommonName of the client certificate.
      --client-certificate string                               Path to a client certificate file for TLS
      --client-key string                                       Path to a client key file for TLS
      --cluster string                                          The name of the kubeconfig cluster to use
      --context string                                          The name of the kubeconfig context to use
      --default-cache-expiration duration                       Cache expiration default (default 24h0m0s)
      --disable-compression                                     If true, opt-out of response compression for all requests to the server
      --disable-http2-serving                                   If true, HTTP2 serving will be disabled [default=false]
      --gloglevel int                                           Set the glog logging level
  -h, --help                                                    help for argocd-aggregated-apiserver
      --http2-max-streams-per-connection int                    The limit that the server gives to clients for the maximum number of streams in an HTTP/2 connection. Zero means to use golang's default.
      --insecure-skip-tls-verify                                If true, the server's certificate will not be checked for validity. This will make your HTTPS connections insecure
      --kubeconfig string                                       Path to a kube config. Only required if out-of-cluster
      --logformat string                                        Set the logging format. One of: json|text (default "json")
      --loglevel string                                         Set the logging level. One of: debug|info|warn|error (default "info")
  -n, --namespace string                                        If present, the namespace scope for this CLI request
      --password string                                         Password for basic authentication to the API server
      --permit-address-sharing                                  If true, SO_REUSEADDR will be used when binding the port. This allows binding to wildcard IPs like 0.0.0.0 and specific IPs in parallel, and it avoids waiting for the kernel to release sockets in TIME_WAIT state. [default=false]
      --permit-port-sharing                                     If true, SO_REUSEPORT will be used when binding the port, which allows more than one instance to bind on the same address and port. [default=false]
      --proxy-url string                                        If provided, this URL will be used to connect via proxy
      --rbac-subject-prefix string                              Prefix added to Kubernetes user and group names before they are evaluated against the Argo CD RBAC policy (default "k8s:")
      --redis string                                            Redis server hostname and port (e.g. argocd-redis:6379). 
      --redis-ca-certificate string                             Path to Redis server CA certificate (e.g. /etc/certs/redis/ca.crt). If not specified, system trusted CAs will be used for server certificate validation.
      --redis-client-certificate string                         Path to Redis client certificate (e.g. /etc/certs/redis/client.crt).
      --redis-client-key string                                 Path to Redis client key (e.g. /etc/certs/redis/client.crt).
      --redis-compress string                                   Enable compression for data sent to Redis with the required compression algorithm. (possible values: gzip, none) (default "gzip")
      --redis-insecure-skip-tls-verify                          Skip Redis server certificate validation.
      --redis-use-tls                                           Use TLS when connecting to Redis. 
      --redisdb int                                             Redis database.
      --request-timeout string                                  The length of time to wait before giving up on a single server request. Non-zero values should contain a corresponding time unit (e.g. 1s, 2m, 3h). A value of zero means don't timeout requests. (default "0")
      --requestheader-allowed-names strings                     List of client certificate common names to allow to provide usernames in headers specified by --requestheader-username-headers. If empty, any client certificate validated by the authorities in --requestheader-client-ca-file is allowed.
      --requestheader-client-ca-file string                     Root certificate bundle to use to verify client certificates on incoming requests before trusting usernames in headers specified by --requestheader-username-headers. WARNING: generally do not depend on authorization being already done for incoming requests.
      --requestheader-extra-headers-prefix strings              List of request header prefixes to inspect. X-Remote-Extra- is suggested. (default [x-remote-extra-])
      --requestheader-group-headers strings                     List of request headers to inspect for groups. X-Remote-Group is suggested. (default [x-remote-group])
      --requestheader-uid-headers strings                       List of request headers to inspect for UIDs. X-Remote-Uid is suggested. Requires the RemoteRequestHeaderUID feature to be enabled.
      --requestheader-username-headers strings                  List of request headers to inspect for usernames. X-Remote-User is common. (default [x-remote-user])
      --secure-port int                                         The port on which to serve HTTPS with authentication and authorization. If 0, don't serve HTTPS at all. (default 8443)
      --sentinel stringArray                                    Redis sentinel hostname and port (e.g. argocd-redis-ha-announce-0:6379). 
      --sentinelmaster string                                   Redis sentinel master group name. (default "master")
      --server string                                           The address and port of the Kubernetes API server
      --tls-cert-file string                                    File containing the default x509 Certificate for HTTPS. (CA cert, if any, concatenated after server cert). If HTTPS serving is enabled, and --tls-cert-file and --tls-private-key-file are not provided, a self-signed certificate and key are generated for the public address and saved to the directory specified by --cert-dir.
      --tls-cipher-suites strings                               Comma-separated list of cipher suites for the server. If omitted, the default Go cipher suites will be used. 
                                                                Preferred values: TLS_AES_128_GCM_SHA256, TLS_AES_256_GCM_SHA384, TLS_CHACHA20_POLY1305_SHA256, TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA, TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256, TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA, TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384, TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305, TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256, TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA, TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256, TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA, TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384, TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305, TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256. 
                                                                Insecure values: TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256, TLS_ECDHE_ECDSA_WITH_RC4_128_SHA, TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA, TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256, TLS_ECDHE_RSA_WITH_RC4_128_SHA, TLS_RSA_WITH_3DES_EDE_CBC_SHA, TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_128_CBC_SHA256, TLS_RSA_WITH_AES_128_GCM_SHA256, TLS_RSA_WITH_AES_256_CBC_SHA, TLS_RSA_WITH_AES_256_GCM_SHA384, TLS_RSA_WITH_RC4_128_SHA.
      --tls-curve-preferences int32Slice                        Comma-separated list of numeric Go crypto/tls CurveID values, as the allowed key exchange mechanisms for the server. The supported values depend on the Go version used. See https://pkg.go.dev/crypto/tls#CurveID for values supported for each Go version. The order of the list is ignored, and key exchange mechanisms are chosen by Go from this list using an internal preference order. If omitted, the default Go curves will be used. (default [])
      --tls-min-version string                                  Minimum TLS version supported. Possible values: VersionTLS10, VersionTLS11, VersionTLS12, VersionTLS13
      --tls-private-key-file string                             File containing the default x509 private key matching --tls-cert-file.
      --tls-server-name string                                  If provided, this name will be used to validate server certificate. If this is not provided, hostname used to contact the server is used.
      --tls-sni-cert-key namedCertKey                           A pair of x509 certificate and private key file paths, optionally suffixed with a list of domain patterns which are fully qualified domain names, possibly with prefixed wildcard segments. The domain patterns also allow IP addresses, but IPs should only be used if the apiserver has visibility to the IP address requested by a client. If no domain patterns are provided, the names of the certificate are extracted. Non-wildcard matches trump over wildcard matches, explicit domain patterns trump over extracted names. For multiple key/certificate pairs, use the --tls-sni-cert-key multiple times. Examples: "example.crt,example.key" or "foo.crt,foo.key:*.foo.com,foo.com". (default [])
      --token string                                            Bearer token for authentication to the API server
      --user string                                             The name of the kubeconfig user to use
      --username string                                         Username for basic authentication to the API server
      --watch-bookmark-interval duration                        Interval between watch bookmarks, for clients requesting them (default 1m0s)
      --watch-buffer-size int                                   Number of events buffered per watch before a slow client is disconnected (default 100)
      --watch-resync-period duration                            Interval at which watched projections are recomputed, to catch lost change notifications (default 3m0s)
```

