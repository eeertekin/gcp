package bigquery

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/eeertekin/gcp/option"
)

func TestNewClientCredentials(t *testing.T) {
	ctx := context.Background()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pkcs1 := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))

	creds := func(mut func(m map[string]string)) []byte {
		m := map[string]string{
			"type":         "service_account",
			"project_id":   "p1",
			"private_key":  pkcs1,
			"client_email": "sa@p1.iam.gserviceaccount.com",
		}
		mut(m)
		b, _ := json.Marshal(m)
		return b
	}
	valid := creds(func(map[string]string) {})

	c, err := NewClient(ctx, "", option.WithAuthCredentialsJSON(option.ServiceAccount, valid))
	if err != nil {
		t.Fatalf("PKCS1 key: %s", err)
	}
	if c.Project() != "p1" || c.tokens.tokenURI != defaultTokenURI || c.endpoint != defaultEndpoint {
		t.Fatalf("project=%q tokenURI=%q endpoint=%q", c.Project(), c.tokens.tokenURI, c.endpoint)
	}

	c, err = NewClient(ctx, DetectProjectID, option.WithAuthCredentialsJSON(option.ServiceAccount, valid))
	if err != nil || c.Project() != "p1" {
		t.Fatalf("detect project: %v %v", c, err)
	}

	c, err = NewClient(ctx, "override", option.WithAuthCredentialsJSON(option.ServiceAccount, valid), option.WithEndpoint("http://localhost:1/v2/"))
	if err != nil || c.Project() != "override" || c.endpoint != "http://localhost:1/v2" {
		t.Fatalf("project override / endpoint: %+v %v", c, err)
	}

	c, err = NewClient(ctx, "p", option.WithAuthCredentialsJSON(option.ServiceAccount, valid), option.WithEndpoint("bigquery.googleapis.com/bigquery/v2"))
	if err != nil || c.endpoint != "https://bigquery.googleapis.com/bigquery/v2" {
		t.Fatalf("endpoint without scheme: %+v %v", c, err)
	}

	hc := &http.Client{}
	if c, err = NewClient(ctx, "p", option.WithAuthCredentialsJSON(option.ServiceAccount, valid), option.WithHTTPClient(hc)); err != nil || c.http != hc || c.tokens.http != hc {
		t.Fatalf("WithHTTPClient not used for API and token calls: %v", err)
	}

	path := filepath.Join(t.TempDir(), "sa.json")
	if err := os.WriteFile(path, valid, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewClient(ctx, "", option.WithAuthCredentialsFile(option.ServiceAccount, path)); err != nil {
		t.Fatalf("credentials file: %s", err)
	}

	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_CONFIG", t.TempDir())
	t.Setenv("GCE_METADATA_HOST", "127.0.0.1:1")

	bad := map[string][]option.ClientOption{
		"no credentials":   nil,
		"missing file":     {option.WithAuthCredentialsFile(option.ServiceAccount, filepath.Join(t.TempDir(), "nope.json"))},
		"type mismatch":    {option.WithAuthCredentialsJSON(option.AuthorizedUser, valid)},
		"invalid json":     {option.WithAuthCredentialsJSON(option.ServiceAccount, []byte("{"))},
		"unsupported type": {option.WithAuthCredentialsJSON("", creds(func(m map[string]string) { m["type"] = "external_account" }))},
		"user without refresh token": {option.WithAuthCredentialsJSON(option.AuthorizedUser, []byte(
			`{"type":"authorized_user","client_id":"id","client_secret":"secret","quota_project_id":"p1"}`))},
		"bad pem": {option.WithAuthCredentialsJSON(option.ServiceAccount, creds(func(m map[string]string) { m["private_key"] = "not a key" }))},
		"bad der": {option.WithAuthCredentialsJSON(option.ServiceAccount, creds(func(m map[string]string) {
			m["private_key"] = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"
		}))},
		"no email":      {option.WithAuthCredentialsJSON(option.ServiceAccount, creds(func(m map[string]string) { delete(m, "client_email") }))},
		"no project id": {option.WithAuthCredentialsJSON(option.ServiceAccount, creds(func(m map[string]string) { delete(m, "project_id") }))},
	}
	for name, opts := range bad {
		if _, err := NewClient(ctx, "", opts...); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}
