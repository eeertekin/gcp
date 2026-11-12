package storage

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
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_CONFIG", t.TempDir())
	t.Setenv("GCE_METADATA_HOST", "127.0.0.1:1")
	ctx := context.Background()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pkcs1 := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))

	creds := func(mut func(m map[string]string)) []byte {
		m := map[string]string{
			"type":         "service_account",
			"private_key":  pkcs1,
			"client_email": "sa@p1.iam.gserviceaccount.com",
		}
		mut(m)
		b, _ := json.Marshal(m)
		return b
	}
	valid := creds(func(map[string]string) {})

	c, err := NewClient(ctx, option.WithAuthCredentialsJSON(option.ServiceAccount, valid))
	if err != nil {
		t.Fatalf("PKCS1 key: %s", err)
	}
	if c.tokens.tokenURI != defaultTokenURI || c.endpoint != defaultEndpoint {
		t.Fatalf("tokenURI=%q endpoint=%q", c.tokens.tokenURI, c.endpoint)
	}

	c, err = NewClient(ctx, option.WithAuthCredentialsJSON(option.ServiceAccount, valid), option.WithEndpoint("http://localhost:1/"))
	if err != nil || c.endpoint != "http://localhost:1" {
		t.Fatalf("endpoint override: %+v %v", c, err)
	}

	hc := &http.Client{}
	if c, err = NewClient(ctx, option.WithAuthCredentialsJSON(option.ServiceAccount, valid), option.WithHTTPClient(hc)); err != nil || c.http != hc || c.tokens.http != hc {
		t.Fatalf("WithHTTPClient not used for API and token calls: %v", err)
	}

	c, err = NewClient(ctx, option.WithAuthCredentialsJSON(option.ServiceAccount, valid), option.WithEndpoint("storage.googleapis.com:443"))
	if err != nil || c.endpoint != "https://storage.googleapis.com:443" {
		t.Fatalf("endpoint without scheme: %+v %v", c, err)
	}

	path := filepath.Join(t.TempDir(), "sa.json")
	if err := os.WriteFile(path, valid, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewClient(ctx, option.WithAuthCredentialsFile(option.ServiceAccount, path)); err != nil {
		t.Fatalf("credentials file: %s", err)
	}

	bad := map[string][]option.ClientOption{
		"no credentials":   nil,
		"missing file":     {option.WithAuthCredentialsFile(option.ServiceAccount, filepath.Join(t.TempDir(), "nope.json"))},
		"type mismatch":    {option.WithAuthCredentialsJSON("authorized_user", valid)},
		"invalid json":     {option.WithAuthCredentialsJSON(option.ServiceAccount, []byte("{"))},
		"unsupported type": {option.WithAuthCredentialsJSON("", creds(func(m map[string]string) { m["type"] = "external_account" }))},
		"incomplete user":  {option.WithAuthCredentialsJSON(option.AuthorizedUser, creds(func(m map[string]string) { m["type"] = "authorized_user" }))},
		"bad pem":          {option.WithAuthCredentialsJSON(option.ServiceAccount, creds(func(m map[string]string) { m["private_key"] = "not a key" }))},
		"bad der": {option.WithAuthCredentialsJSON(option.ServiceAccount, creds(func(m map[string]string) {
			m["private_key"] = "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"
		}))},
		"no email": {option.WithAuthCredentialsJSON(option.ServiceAccount, creds(func(m map[string]string) { delete(m, "client_email") }))},
	}
	for name, opts := range bad {
		if _, err := NewClient(ctx, opts...); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestPersistedOffset(t *testing.T) {
	cases := map[string]int64{
		"":               0,
		"bytes=0-0":      1,
		"bytes=0-262143": 262144,
	}
	for in, want := range cases {
		got, err := persistedOffset(in)
		if err != nil || got != want {
			t.Errorf("persistedOffset(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"bytes=0-", "bytes=abc", "garbage"} {
		if _, err := persistedOffset(in); err == nil {
			t.Errorf("persistedOffset(%q): expected error", in)
		}
	}
}

func TestChunkSizeAlignment(t *testing.T) {
	cases := map[int]int{
		0:                    defaultChunkSize,
		-1:                   defaultChunkSize,
		1:                    minChunkSize,
		minChunkSize:         minChunkSize,
		minChunkSize + 1:     2 * minChunkSize,
		3*minChunkSize - 100: 3 * minChunkSize,
	}
	for in, want := range cases {
		w := &Writer{ChunkSize: in}
		if got := w.chunkSize(); got != want {
			t.Errorf("chunkSize(%d) = %d, want %d", in, got, want)
		}
	}
}
