package pubsub

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

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
			"project_id":   "p1",
			"private_key":  pkcs1,
			"client_email": "sa@p1.iam.gserviceaccount.com",
		}
		mut(m)
		b, _ := json.Marshal(m)
		return b
	}
	valid := creds(func(map[string]string) {})
	withCreds := option.WithAuthCredentialsJSON(option.ServiceAccount, valid)

	c, err := NewClient(ctx, DetectProjectID, withCreds)
	if err != nil {
		t.Fatal(err)
	}
	if c.Project() != "p1" || c.endpoint != defaultEndpoint || c.tokens.tokenURI != defaultTokenURI {
		t.Fatalf("project=%q endpoint=%q tokenURI=%q", c.Project(), c.endpoint, c.tokens.tokenURI)
	}

	c, err = NewClient(ctx, "other", withCreds, option.WithEndpoint("localhost:8085/"))
	if err != nil || c.Project() != "other" || c.endpoint != "https://localhost:8085" {
		t.Fatalf("explicit project / endpoint: %+v %v", c, err)
	}

	hc := &http.Client{}
	if c, err = NewClient(ctx, "p", withCreds, option.WithHTTPClient(hc)); err != nil || c.http != hc || c.tokens.http != hc {
		t.Fatalf("WithHTTPClient not used for API and token calls: %v", err)
	}

	if _, err := NewClient(ctx, "", withCreds); !errors.Is(err, ErrEmptyProjectID) {
		t.Fatalf("empty project: %v", err)
	}
	if _, err := NewClient(ctx, DetectProjectID, option.WithAuthCredentialsJSON(option.ServiceAccount,
		creds(func(m map[string]string) { delete(m, "project_id") }))); !errors.Is(err, ErrEmptyProjectID) {
		t.Fatalf("undetectable project: %v", err)
	}

	path := filepath.Join(t.TempDir(), "sa.json")
	if err := os.WriteFile(path, valid, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewClient(ctx, DetectProjectID, option.WithAuthCredentialsFile(option.ServiceAccount, path)); err != nil {
		t.Fatalf("credentials file: %v", err)
	}

	bad := map[string][]option.ClientOption{
		"no credentials":  nil,
		"missing file":    {option.WithAuthCredentialsFile(option.ServiceAccount, filepath.Join(t.TempDir(), "nope.json"))},
		"type mismatch":   {option.WithAuthCredentialsJSON("authorized_user", valid)},
		"invalid json":    {option.WithAuthCredentialsJSON(option.ServiceAccount, []byte("{"))},
		"bad pem":         {option.WithAuthCredentialsJSON(option.ServiceAccount, creds(func(m map[string]string) { m["private_key"] = "x" }))},
		"unsupported":     {option.WithAuthCredentialsJSON("", creds(func(m map[string]string) { m["type"] = "external_account" }))},
		"incomplete user": {option.WithAuthCredentialsJSON(option.AuthorizedUser, creds(func(m map[string]string) { m["type"] = "authorized_user" }))},
		"no email":        {option.WithAuthCredentialsJSON(option.ServiceAccount, creds(func(m map[string]string) { delete(m, "client_email") }))},
	}
	for name, opts := range bad {
		if _, err := NewClient(ctx, "p", opts...); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

func TestMessageSizeMatchesProtobufEncoding(t *testing.T) {
	cases := []struct {
		msg  Message
		want int
	}{
		{Message{}, 0},
		{Message{Data: []byte("hello")}, 7},
		{Message{Data: make([]byte, 200)}, 203},
		{Message{Attributes: map[string]string{"k": "v"}}, 8},
		{Message{Data: []byte("x"), OrderingKey: "key"}, 8},
	}
	for _, tc := range cases {
		if got := messageSize(&tc.msg); got != tc.want {
			t.Errorf("messageSize(%+v) = %d, want %d", tc.msg, got, tc.want)
		}
	}
}

func TestLeaseDuration(t *testing.T) {
	s := &Subscriber{}
	cases := []struct {
		rs   ReceiveSettings
		want time.Duration
	}{
		{ReceiveSettings{}, defaultLease},
		{ReceiveSettings{MaxDurationPerAckExtension: 20 * time.Second}, 20 * time.Second},
		{ReceiveSettings{MaxDurationPerAckExtension: time.Second}, minLease},
		{ReceiveSettings{MinDurationPerAckExtension: 120 * time.Second}, 120 * time.Second},
		{ReceiveSettings{MinDurationPerAckExtension: time.Hour}, maxLease},
	}
	for _, tc := range cases {
		if got := s.leaseDuration(tc.rs); got != tc.want {
			t.Errorf("leaseDuration(%+v) = %s, want %s", tc.rs, got, tc.want)
		}
	}
}
