package storage_test

import (
	"bytes"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/eeertekin/gcp/storage"
	"github.com/eeertekin/gcp/storage/storagetest"
)

func do(t *testing.T, method, u string, body []byte, header ...string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, u, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

func testCreds(t *testing.T, s *storagetest.Server) (email string, pemKey []byte) {
	t.Helper()
	var sa struct {
		ClientEmail string `json:"client_email"`
		PrivateKey  string `json:"private_key"`
	}
	if err := json.Unmarshal(s.Credentials(), &sa); err != nil {
		t.Fatal(err)
	}
	return sa.ClientEmail, []byte(sa.PrivateKey)
}

func TestSignedURLPutThenGet(t *testing.T) {
	s, c := storagetest.NewServer(t)
	b := c.Bucket(storagetest.Bucket)
	name := "dir/hello world&más.txt"

	putURL, err := b.SignedURL(name, &storage.SignedURLOptions{
		Method:      http.MethodPut,
		Expires:     time.Now().Add(15 * time.Minute),
		ContentType: "text/plain",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(putURL, s.URL()+"/"+storagetest.Bucket+"/dir/hello%20world%26m%C3%A1s.txt?") {
		t.Fatalf("url = %s", putURL)
	}
	if code, body := do(t, http.MethodPut, putURL, []byte("hi there"), "Content-Type", "text/plain"); code != http.StatusOK {
		t.Fatalf("PUT: %d %s", code, body)
	}
	assertObject(t, s, name, []byte("hi there"), "text/plain")

	getURL, err := b.SignedURL(name, &storage.SignedURLOptions{
		Method:  http.MethodGet,
		Expires: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	code, body := do(t, http.MethodGet, getURL, nil)
	if code != http.StatusOK || string(body) != "hi there" {
		t.Fatalf("GET: %d %q", code, body)
	}
}

func TestSignedURLMissingObject(t *testing.T) {
	_, c := storagetest.NewServer(t)
	u, err := c.Bucket(storagetest.Bucket).SignedURL("missing", &storage.SignedURLOptions{
		Method:  http.MethodGet,
		Expires: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if code, body := do(t, http.MethodGet, u, nil); code != http.StatusNotFound {
		t.Fatalf("GET: %d %s, want 404", code, body)
	}
}

func TestSignedURLContentTypeEnforced(t *testing.T) {
	s, c := storagetest.NewServer(t)
	u, err := c.Bucket(storagetest.Bucket).SignedURL("o", &storage.SignedURLOptions{
		Method:      http.MethodPut,
		Expires:     time.Now().Add(time.Minute),
		ContentType: "image/png",
	})
	if err != nil {
		t.Fatal(err)
	}
	if code, _ := do(t, http.MethodPut, u, []byte("x"), "Content-Type", "text/html"); code != http.StatusForbidden {
		t.Fatalf("PUT with wrong content type: %d, want 403", code)
	}
	if _, ok := s.Object(storagetest.Bucket, "o"); ok {
		t.Fatal("object stored despite signature mismatch")
	}
}

func TestSignedURLTamperingRejected(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.AddObject(storagetest.Bucket, "a", []byte("A"))
	s.AddObject(storagetest.Bucket, "b", []byte("B"))
	u, err := c.Bucket(storagetest.Bucket).SignedURL("a", &storage.SignedURLOptions{
		Method:  http.MethodGet,
		Expires: time.Now().Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if code, body := do(t, http.MethodGet, u, nil); code != http.StatusOK || string(body) != "A" {
		t.Fatalf("untampered GET: %d %q", code, body)
	}

	tampered := map[string]string{
		"other object":   strings.Replace(u, "/"+storagetest.Bucket+"/a?", "/"+storagetest.Bucket+"/b?", 1),
		"longer expiry":  regexp.MustCompile(`X-Goog-Expires=\d+`).ReplaceAllString(u, "X-Goog-Expires=600"),
		"extra param":    u + "&generation=1",
		"bad signature":  u[:len(u)-4] + "0000",
		"no signature":   u[:strings.Index(u, "&X-Goog-Signature=")],
		"different date": strings.Replace(u, "X-Goog-Date=2", "X-Goog-Date=1", 1),
	}
	for name, tu := range tampered {
		if tu == u {
			t.Fatalf("%s: tampering did not change the url", name)
		}
		if code, _ := do(t, http.MethodGet, tu, nil); code < 400 {
			t.Errorf("%s: status %d, want rejection", name, code)
		}
	}
	if code, _ := do(t, http.MethodPut, u, []byte("x")); code != http.StatusForbidden {
		t.Errorf("PUT with GET url: %d, want 403", code)
	}
	if obj, _ := s.Object(storagetest.Bucket, "a"); string(obj.Data) != "A" {
		t.Fatal("object modified via tampered url")
	}
}

func TestSignedURLExpires(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.AddObject(storagetest.Bucket, "o", []byte("x"))
	u, err := c.Bucket(storagetest.Bucket).SignedURL("o", &storage.SignedURLOptions{
		Method:  http.MethodGet,
		Expires: time.Now().Add(1100 * time.Millisecond),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(u, "X-Goog-Expires=1&") {
		t.Fatalf("url = %s", u)
	}
	if code, _ := do(t, http.MethodGet, u, nil); code != http.StatusOK {
		t.Fatalf("fresh url: %d", code)
	}
	time.Sleep(2100 * time.Millisecond)
	if code, body := do(t, http.MethodGet, u, nil); code != http.StatusBadRequest || !strings.Contains(string(body), "ExpiredToken") {
		t.Fatalf("expired url: %d %s", code, body)
	}
}

func TestSignedURLSignedHeadersAndQuery(t *testing.T) {
	s, c := storagetest.NewServer(t)
	u, err := c.Bucket(storagetest.Bucket).SignedURL("o", &storage.SignedURLOptions{
		Method:          http.MethodPut,
		Expires:         time.Now().Add(time.Minute),
		Headers:         []string{"X-Goog-Meta-Owner:  alice  ", "x-goog-if-generation-match:0"},
		QueryParameters: url.Values{"response-cache-control": {"no-store"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(u, "X-Goog-SignedHeaders=host%3Bx-goog-if-generation-match%3Bx-goog-meta-owner&") ||
		!strings.Contains(u, "response-cache-control=no-store") {
		t.Fatalf("url = %s", u)
	}

	if code, _ := do(t, http.MethodPut, u, []byte("x"), "X-Goog-Meta-Owner", "alice"); code != http.StatusForbidden {
		t.Fatalf("missing signed header: %d, want 403", code)
	}
	if code, _ := do(t, http.MethodPut, strings.Replace(u, "response-cache-control=no-store&", "", 1), []byte("x"),
		"X-Goog-Meta-Owner", "alice", "X-Goog-If-Generation-Match", "0"); code != http.StatusForbidden {
		t.Fatalf("dropped signed query param: %d, want 403", code)
	}
	if code, body := do(t, http.MethodPut, u, []byte("x"), "X-Goog-Meta-Owner", "alice", "X-Goog-If-Generation-Match", "0"); code != http.StatusOK {
		t.Fatalf("PUT: %d %s", code, body)
	}
	if _, ok := s.Object(storagetest.Bucket, "o"); !ok {
		t.Fatal("object not stored")
	}
}

func TestSignedURLExplicitCredentials(t *testing.T) {
	s, _ := storagetest.NewServer(t)
	s.AddObject(storagetest.Bucket, "o", []byte("explicit"))
	email, key := testCreds(t, s)

	u, err := storage.SignedURL(storagetest.Bucket, "o", &storage.SignedURLOptions{
		GoogleAccessID: email,
		PrivateKey:     key,
		Method:         "get",
		Expires:        time.Now().Add(time.Minute),
		Hostname:       strings.TrimPrefix(s.URL(), "http://"),
		Insecure:       true,
		Scheme:         storage.SigningSchemeV4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if code, body := do(t, http.MethodGet, u, nil); code != http.StatusOK || string(body) != "explicit" {
		t.Fatalf("GET: %d %q", code, body)
	}
}

func TestSignedURLSignBytes(t *testing.T) {
	s, _ := storagetest.NewServer(t)
	s.AddObject(storagetest.Bucket, "o", []byte("signer"))
	email, pemKey := testCreds(t, s)
	block, _ := pem.Decode(pemKey)
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	key := parsed.(*rsa.PrivateKey)

	calls := 0
	u, err := storage.SignedURL(storagetest.Bucket, "o", &storage.SignedURLOptions{
		GoogleAccessID: email,
		SignBytes: func(p []byte) ([]byte, error) {
			calls++
			if !strings.HasPrefix(string(p), "GOOG4-RSA-SHA256\n") {
				t.Errorf("unexpected string to sign %q", p)
			}
			sum := sha256.Sum256(p)
			return rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
		},
		Method:   http.MethodGet,
		Expires:  time.Now().Add(time.Minute),
		Hostname: strings.TrimPrefix(s.URL(), "http://"),
		Insecure: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("SignBytes called %d times", calls)
	}
	if code, body := do(t, http.MethodGet, u, nil); code != http.StatusOK || string(body) != "signer" {
		t.Fatalf("GET: %d %q", code, body)
	}
}

func TestSignedURLMaxExpiry(t *testing.T) {
	_, c := storagetest.NewServer(t)
	u, err := c.Bucket(storagetest.Bucket).SignedURL("o", &storage.SignedURLOptions{
		Method:  http.MethodGet,
		Expires: time.Now().Add(7*24*time.Hour - time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(u, "X-Goog-Expires=60479") {
		t.Fatalf("url = %s", u)
	}
}

func TestSignedURLDoesNotMutateOptions(t *testing.T) {
	_, c := storagetest.NewServer(t)
	opts := &storage.SignedURLOptions{Method: http.MethodGet, Expires: time.Now().Add(time.Minute)}
	if _, err := c.Bucket(storagetest.Bucket).SignedURL("o", opts); err != nil {
		t.Fatal(err)
	}
	if opts.GoogleAccessID != "" || opts.SignBytes != nil || opts.PrivateKey != nil {
		t.Fatalf("options mutated: %+v", opts)
	}
}

func TestSignedURLValidation(t *testing.T) {
	s, c := storagetest.NewServer(t)
	email, key := testCreds(t, s)
	sign := func([]byte) ([]byte, error) { return []byte("sig"), nil }
	future := time.Now().Add(time.Minute)

	bucketCases := map[string]*storage.SignedURLOptions{
		"nil options":       nil,
		"missing method":    {Expires: future},
		"invalid method":    {Method: "PATCH", Expires: future},
		"missing expires":   {Method: "GET"},
		"expires in past":   {Method: "GET", Expires: time.Now().Add(-time.Minute)},
		"expires too far":   {Method: "GET", Expires: time.Now().Add(7*24*time.Hour + time.Minute)},
		"header no colon":   {Method: "GET", Expires: future, Headers: []string{"x-goog-meta-a"}},
		"header empty name": {Method: "GET", Expires: future, Headers: []string{":value"}},
		"other account without key": {
			Method: "GET", Expires: future, GoogleAccessID: "someone@else.iam.gserviceaccount.com",
		},
	}
	for name, opts := range bucketCases {
		if _, err := c.Bucket(storagetest.Bucket).SignedURL("o", opts); err == nil {
			t.Errorf("bucket %s: expected error", name)
		}
	}
	if _, err := c.Bucket("").SignedURL("o", &storage.SignedURLOptions{Method: "GET", Expires: future}); err == nil {
		t.Error("empty bucket: expected error")
	}

	pkgCases := map[string]*storage.SignedURLOptions{
		"missing access id": {PrivateKey: key, Method: "GET", Expires: future},
		"no key or signer":  {GoogleAccessID: email, Method: "GET", Expires: future},
		"key and signer":    {GoogleAccessID: email, PrivateKey: key, SignBytes: sign, Method: "GET", Expires: future},
		"invalid pem":       {GoogleAccessID: email, PrivateKey: []byte("nope"), Method: "GET", Expires: future},
	}
	for name, opts := range pkgCases {
		if _, err := storage.SignedURL(storagetest.Bucket, "o", opts); err == nil {
			t.Errorf("package %s: expected error", name)
		}
	}
	if n := s.Attempts(); n != 0 {
		t.Fatalf("signing made %d API calls", n)
	}
}

func hmacOptions(s *storagetest.Server, secret string, expires time.Time) *storage.SignedURLOptions {
	id, _ := s.HMACKey()
	return &storage.SignedURLOptions{
		GoogleAccessID: id,
		HMACSecret:     secret,
		Method:         http.MethodGet,
		Expires:        expires,
		Hostname:       strings.TrimPrefix(s.URL(), "http://"),
		Insecure:       true,
	}
}

func TestSignedURLHMAC(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.AddObject(storagetest.Bucket, "a", []byte("A"))
	s.AddObject(storagetest.Bucket, "b", []byte("B"))
	_, secret := s.HMACKey()
	future := time.Now().Add(time.Minute)

	u, err := storage.SignedURL(storagetest.Bucket, "a", hmacOptions(s, secret, future))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(u, "X-Goog-Algorithm=GOOG4-HMAC-SHA256&X-Goog-Credential="+storagetest.HMACAccessID+"%2F") {
		t.Fatalf("url = %s", u)
	}
	if code, body := do(t, http.MethodGet, u, nil); code != http.StatusOK || string(body) != "A" {
		t.Fatalf("GET: %d %q", code, body)
	}

	wrong, err := storage.SignedURL(storagetest.Bucket, "a", hmacOptions(s, secret+"x", future))
	if err != nil {
		t.Fatal(err)
	}
	if code, body := do(t, http.MethodGet, wrong, nil); code != http.StatusForbidden {
		t.Errorf("wrong secret: %d %s, want 403", code, body)
	}

	hourAgo := time.Now().Add(-time.Hour)
	expired, err := storage.SignedURLAt(storagetest.Bucket, "a", hmacOptions(s, secret, hourAgo.Add(time.Minute)), hourAgo)
	if err != nil {
		t.Fatal(err)
	}
	if code, body := do(t, http.MethodGet, expired, nil); code != http.StatusBadRequest || !strings.Contains(string(body), "ExpiredToken") {
		t.Errorf("expired: %d %s", code, body)
	}

	tampered := strings.Replace(u, "/"+storagetest.Bucket+"/a?", "/"+storagetest.Bucket+"/b?", 1)
	if tampered == u {
		t.Fatal("tampering did not change the url")
	}
	if code, body := do(t, http.MethodGet, tampered, nil); code != http.StatusForbidden {
		t.Errorf("tampered path: %d %s, want 403", code, body)
	}

	bu, err := c.Bucket(storagetest.Bucket).SignedURL("b", &storage.SignedURLOptions{
		GoogleAccessID: storagetest.HMACAccessID,
		HMACSecret:     secret,
		Method:         http.MethodGet,
		Expires:        future,
	})
	if err != nil {
		t.Fatal(err)
	}
	if code, body := do(t, http.MethodGet, bu, nil); code != http.StatusOK || string(body) != "B" {
		t.Fatalf("bucket GET: %d %q", code, body)
	}
}

func TestSignedURLHMACWithMetadataCredentials(t *testing.T) {
	isolateADC(t)
	s, _ := storagetest.NewServer(t)
	t.Setenv("GCE_METADATA_HOST", s.Host())
	s.AddObject(storagetest.Bucket, "o", []byte("local"))
	c := adcClient(t, s)
	storage.SetIAMEndpoint(c, s.URL())
	id, secret := s.HMACKey()
	b := c.Bucket(storagetest.Bucket)

	if _, err := b.SignedURL("o", &storage.SignedURLOptions{HMACSecret: secret, Method: http.MethodGet, Expires: time.Now().Add(time.Minute)}); err == nil {
		t.Fatal("HMACSecret without GoogleAccessID must fail, not use the client email")
	}
	u, err := b.SignedURL("o", &storage.SignedURLOptions{GoogleAccessID: id, HMACSecret: secret, Method: http.MethodGet, Expires: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if code, body := do(t, http.MethodGet, u, nil); code != http.StatusOK || string(body) != "local" {
		t.Fatalf("signed GET = %d %q", code, body)
	}
	for _, call := range s.Calls() {
		if strings.HasPrefix(call, "SIGNBLOB") {
			t.Fatalf("HMAC signing called signBlob: %q", s.Calls())
		}
	}
}

func TestSignedURLHMACValidation(t *testing.T) {
	s, _ := storagetest.NewServer(t)
	email, key := testCreds(t, s)
	id, secret := s.HMACKey()
	sign := func([]byte) ([]byte, error) { return []byte("sig"), nil }
	future := time.Now().Add(time.Minute)

	cases := map[string]*storage.SignedURLOptions{
		"missing access id": {HMACSecret: secret, Method: "GET", Expires: future},
		"secret and key":    {GoogleAccessID: id, HMACSecret: secret, PrivateKey: key, Method: "GET", Expires: future},
		"secret and signer": {GoogleAccessID: email, HMACSecret: secret, SignBytes: sign, Method: "GET", Expires: future},
		"invalid method":    {GoogleAccessID: id, HMACSecret: secret, Method: "PATCH", Expires: future},
		"expires too far":   {GoogleAccessID: id, HMACSecret: secret, Method: "GET", Expires: time.Now().Add(8 * 24 * time.Hour)},
	}
	for name, opts := range cases {
		if _, err := storage.SignedURL(storagetest.Bucket, "o", opts); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
	_, err := storage.SignedURL(storagetest.Bucket, "o", &storage.SignedURLOptions{GoogleAccessID: id, HMACSecret: secret, PrivateKey: key, Method: "GET", Expires: future})
	if err == nil || !strings.Contains(err.Error(), "HMACSecret") {
		t.Fatalf("error must name HMACSecret: %v", err)
	}
}
