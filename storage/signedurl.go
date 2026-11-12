package storage

import (
	"cmp"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	defaultSigningHost = "storage.googleapis.com"
	maxSignedURLExpiry = 7 * 24 * time.Hour
)

// SigningScheme exists for source compatibility with the GCP SDK; only V4
// signing is implemented and it is the default.
type SigningScheme int

const (
	SigningSchemeDefault SigningScheme = iota
	SigningSchemeV4
)

type URLStyle interface {
	host(hostname, bucket string) string
	path(bucket, object string) string
}

type pathStyle struct{}

func (pathStyle) host(hostname, _ string) string { return hostname }
func (pathStyle) path(bucket, object string) string {
	if object == "" {
		return "/" + bucket
	}
	return "/" + bucket + "/" + object
}

type virtualHostedStyle struct{}

func (virtualHostedStyle) host(hostname, bucket string) string { return bucket + "." + hostname }
func (virtualHostedStyle) path(_, object string) string        { return "/" + object }

type bucketBoundHostname struct{ hostname string }

func (b bucketBoundHostname) host(_, _ string) string    { return b.hostname }
func (bucketBoundHostname) path(_, object string) string { return "/" + object }

func PathStyle() URLStyle                          { return pathStyle{} }
func VirtualHostedStyle() URLStyle                 { return virtualHostedStyle{} }
func BucketBoundHostname(hostname string) URLStyle { return bucketBoundHostname{hostname: hostname} }

type SignedURLOptions struct {
	GoogleAccessID  string
	PrivateKey      []byte
	SignBytes       func([]byte) ([]byte, error)
	Method          string
	Expires         time.Time
	ContentType     string
	Headers         []string
	QueryParameters url.Values
	MD5             string
	Style           URLStyle
	Insecure        bool
	Scheme          SigningScheme
	Hostname        string
}

func SignedURL(bucket, object string, opts *SignedURLOptions) (string, error) {
	return signURL(bucket, object, opts, "", time.Now())
}

// SignedURL fills GoogleAccessID and the signing key from the client's
// service account when they are not set, and signs for the client endpoint.
func (b *BucketHandle) SignedURL(object string, opts *SignedURLOptions) (string, error) {
	if opts == nil {
		return "", errors.New("storage: missing required SignedURLOptions")
	}
	o := *opts
	if b.c.key == nil && (o.GoogleAccessID == "" || (o.SignBytes == nil && len(o.PrivateKey) == 0)) {
		return "", errors.New("storage: client credentials have no service account key: set GoogleAccessID and PrivateKey or SignBytes")
	}
	if o.GoogleAccessID == "" {
		o.GoogleAccessID = b.c.email
	}
	if o.SignBytes == nil && len(o.PrivateKey) == 0 && o.GoogleAccessID == b.c.email {
		key := b.c.key
		o.SignBytes = func(p []byte) ([]byte, error) { return signRSA(key, p) }
	}
	return signURL(b.name, object, &o, b.c.endpoint, time.Now())
}

func signRSA(key *rsa.PrivateKey, p []byte) ([]byte, error) {
	sum := sha256.Sum256(p)
	return rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
}

func signURL(bucket, object string, opts *SignedURLOptions, endpoint string, now time.Time) (string, error) {
	if opts == nil {
		return "", errors.New("storage: missing required SignedURLOptions")
	}
	if bucket == "" {
		return "", errors.New("storage: bucket name is empty")
	}
	if opts.GoogleAccessID == "" {
		return "", errors.New("storage: missing required GoogleAccessID")
	}
	if opts.Scheme != SigningSchemeDefault && opts.Scheme != SigningSchemeV4 {
		return "", errors.New("storage: only V4 signing is supported")
	}
	sign, err := signer(opts)
	if err != nil {
		return "", err
	}

	method := strings.ToUpper(opts.Method)
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodPut, http.MethodPost, http.MethodDelete:
	default:
		return "", fmt.Errorf("storage: invalid Method %q", opts.Method)
	}

	now = now.UTC()
	if opts.Expires.IsZero() {
		return "", errors.New("storage: missing required Expires")
	}
	ttl := opts.Expires.Sub(now)
	if ttl < time.Second {
		return "", errors.New("storage: Expires must be in the future")
	}
	if ttl > maxSignedURLExpiry {
		return "", errors.New("storage: Expires must be within 7 days")
	}

	scheme, hostname := "https", defaultSigningHost
	if opts.Hostname != "" {
		hostname = opts.Hostname
	} else if u, err := url.Parse(endpoint); endpoint != "" && err == nil && u.Host != "" {
		scheme, hostname = u.Scheme, u.Host
	}
	if opts.Insecure {
		scheme = "http"
	}
	style := opts.Style
	if style == nil {
		style = PathStyle()
	}
	host := style.host(hostname, bucket)
	path := style.path(bucket, escapePath(object))

	headers := map[string]string{}
	add := func(k, v string) {
		k = strings.ToLower(strings.TrimSpace(k))
		v = strings.Join(strings.Fields(v), " ")
		if old, ok := headers[k]; ok {
			v = old + "," + v
		}
		headers[k] = v
	}
	for _, h := range opts.Headers {
		k, v, ok := strings.Cut(h, ":")
		if !ok || strings.TrimSpace(k) == "" {
			return "", fmt.Errorf("storage: invalid header %q, want \"Name:Value\"", h)
		}
		add(k, v)
	}
	if opts.ContentType != "" {
		add("content-type", opts.ContentType)
	}
	if opts.MD5 != "" {
		add("content-md5", opts.MD5)
	}
	headers["host"] = stripPort(host)

	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	slices.Sort(names)
	var canonicalHeaders strings.Builder
	for _, k := range names {
		canonicalHeaders.WriteString(k + ":" + headers[k] + "\n")
	}
	signedHeaders := strings.Join(names, ";")

	timestamp := now.Format("20060102T150405Z")
	scope := now.Format("20060102") + "/auto/storage/goog4_request"

	q := url.Values{}
	for k, vs := range opts.QueryParameters {
		q[k] = slices.Clone(vs)
	}
	q.Set("X-Goog-Algorithm", "GOOG4-RSA-SHA256")
	q.Set("X-Goog-Credential", opts.GoogleAccessID+"/"+scope)
	q.Set("X-Goog-Date", timestamp)
	q.Set("X-Goog-Expires", strconv.FormatInt(int64(ttl/time.Second), 10))
	q.Set("X-Goog-SignedHeaders", signedHeaders)
	query := canonicalQuery(q)

	payload := "UNSIGNED-PAYLOAD"
	if v, ok := headers["x-goog-content-sha256"]; ok {
		payload = v
	}

	canonicalRequest := strings.Join([]string{method, path, query, canonicalHeaders.String(), signedHeaders, payload}, "\n")
	sum := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := "GOOG4-RSA-SHA256\n" + timestamp + "\n" + scope + "\n" + hex.EncodeToString(sum[:])

	sig, err := sign([]byte(stringToSign))
	if err != nil {
		return "", fmt.Errorf("storage: sign url: %w", err)
	}
	return scheme + "://" + host + path + "?" + query + "&X-Goog-Signature=" + hex.EncodeToString(sig), nil
}

func signer(opts *SignedURLOptions) (func([]byte) ([]byte, error), error) {
	if (opts.SignBytes == nil) == (len(opts.PrivateKey) == 0) {
		return nil, errors.New("storage: exactly one of PrivateKey or SignBytes must be set")
	}
	if opts.SignBytes != nil {
		return opts.SignBytes, nil
	}
	block, _ := pem.Decode(opts.PrivateKey)
	if block == nil {
		return nil, errors.New("storage: PrivateKey is not valid PEM")
	}
	key, err := parseRSAKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	return func(p []byte) ([]byte, error) { return signRSA(key, p) }, nil
}

// escapeV4 percent-encodes everything except RFC 3986 unreserved characters.
func escapeV4(s string) string {
	return strings.ReplaceAll(url.QueryEscape(s), "+", "%20")
}

func escapePath(object string) string {
	segs := strings.Split(object, "/")
	for i, s := range segs {
		segs[i] = escapeV4(s)
	}
	return strings.Join(segs, "/")
}

func canonicalQuery(q url.Values) string {
	type pair struct{ k, v string }
	var pairs []pair
	for k, vs := range q {
		for _, v := range vs {
			pairs = append(pairs, pair{escapeV4(k), escapeV4(v)})
		}
	}
	slices.SortFunc(pairs, func(a, b pair) int {
		return cmp.Or(strings.Compare(a.k, b.k), strings.Compare(a.v, b.v))
	})
	parts := make([]string, len(pairs))
	for i, p := range pairs {
		parts[i] = p.k + "=" + p.v
	}
	return strings.Join(parts, "&")
}

func stripPort(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}
	return host
}
