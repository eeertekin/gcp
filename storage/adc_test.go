package storage_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/eeertekin/gcp/option"
	"github.com/eeertekin/gcp/storage"
	"github.com/eeertekin/gcp/storage/storagetest"
)

func isolateADC(t *testing.T) string {
	t.Helper()
	gcloud := t.TempDir()
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")
	t.Setenv("CLOUDSDK_CONFIG", gcloud)
	t.Setenv("GCE_METADATA_HOST", "127.0.0.1:1")
	return gcloud
}

func writeFile(t *testing.T, path string, data []byte) {
	t.Helper()
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func adcClient(t *testing.T, s *storagetest.Server, opts ...option.ClientOption) *storage.Client {
	t.Helper()
	c, err := storage.NewClient(context.Background(), append(opts, option.WithEndpoint(s.URL()))...)
	if err != nil {
		t.Fatal(err)
	}
	c.Retry = storage.RetryPolicy{MaxAttempts: 3, Initial: time.Millisecond, Max: time.Millisecond}
	t.Cleanup(func() { c.Close() })
	return c
}

func deleteObject(t *testing.T, s *storagetest.Server, c *storage.Client, name string) {
	t.Helper()
	s.AddObject(storagetest.Bucket, name, nil)
	if err := c.Bucket(storagetest.Bucket).Object(name).Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Object(storagetest.Bucket, name); ok {
		t.Fatalf("object %q still stored", name)
	}
}

func TestADCEnvironmentFile(t *testing.T) {
	gcloud := isolateADC(t)
	s, _ := storagetest.NewServer(t)
	writeFile(t, filepath.Join(gcloud, "application_default_credentials.json"), s.AuthorizedUserJSON())
	path := filepath.Join(t.TempDir(), "sa.json")
	writeFile(t, path, s.Credentials())
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)
	before := s.TokenCalls()

	c := adcClient(t, s)
	deleteObject(t, s, c, "a")
	if q := s.QuotaProject(); q != "" {
		t.Fatalf("quota project %q: GOOGLE_APPLICATION_CREDENTIALS must win over the gcloud user file", q)
	}
	if n := s.TokenCalls() - before; n != 1 {
		t.Fatalf("token calls %d", n)
	}
	u, err := c.Bucket(storagetest.Bucket).SignedURL("a", &storage.SignedURLOptions{Method: http.MethodGet, Expires: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatalf("service account from GOOGLE_APPLICATION_CREDENTIALS must sign URLs: %v", err)
	}
	if !strings.Contains(u, "X-Goog-Credential="+strings.ReplaceAll(storagetest.ClientEmail, "@", "%40")) {
		t.Fatalf("signed URL credential: %s", u)
	}

	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing.json"))
	if _, err := storage.NewClient(context.Background()); err == nil || !strings.Contains(err.Error(), "GOOGLE_APPLICATION_CREDENTIALS") {
		t.Fatalf("missing env file: %v", err)
	}
	c = adcClient(t, s, option.WithAuthCredentialsJSON(option.ServiceAccount, s.Credentials()))
	deleteObject(t, s, c, "b")
}

func TestADCGcloudUserCredentials(t *testing.T) {
	gcloud := isolateADC(t)
	s, _ := storagetest.NewServer(t)
	writeFile(t, filepath.Join(gcloud, "application_default_credentials.json"), s.AuthorizedUserJSON())
	before := s.TokenCalls()

	c := adcClient(t, s)
	deleteObject(t, s, c, "a")
	if q := s.QuotaProject(); q != storagetest.ProjectID {
		t.Fatalf("X-Goog-User-Project %q, want the quota_project_id of the user credentials", q)
	}

	c = adcClient(t, s, option.WithAuthCredentialsJSON(option.AuthorizedUser, s.AuthorizedUserJSON()))
	deleteObject(t, s, c, "b")
	if n := s.TokenCalls() - before; n != 2 {
		t.Fatalf("token calls %d, want one refresh per client", n)
	}
}

func TestSignedURLWithoutServiceAccountKey(t *testing.T) {
	gcloud := isolateADC(t)
	s, _ := storagetest.NewServer(t)
	writeFile(t, filepath.Join(gcloud, "application_default_credentials.json"), s.AuthorizedUserJSON())
	s.AddObject(storagetest.Bucket, "o", []byte("hi"))
	c := adcClient(t, s)
	bucket := c.Bucket(storagetest.Bucket)
	expires := time.Now().Add(time.Minute)

	_, err := bucket.SignedURL("o", &storage.SignedURLOptions{Method: http.MethodGet, Expires: expires})
	if err == nil || !strings.Contains(err.Error(), "no service account key") {
		t.Fatalf("user credentials cannot sign: %v", err)
	}
	email, key := testCreds(t, s)
	if _, err := bucket.SignedURL("o", &storage.SignedURLOptions{PrivateKey: key, Method: http.MethodGet, Expires: expires}); err == nil {
		t.Fatal("PrivateKey without GoogleAccessID must fail when the client has no service account")
	}

	u, err := bucket.SignedURL("o", &storage.SignedURLOptions{GoogleAccessID: email, PrivateKey: key, Method: http.MethodGet, Expires: expires})
	if err != nil {
		t.Fatal(err)
	}
	if code, body := do(t, http.MethodGet, u, nil); code != http.StatusOK || string(body) != "hi" {
		t.Fatalf("signed GET = %d %q", code, body)
	}
}

func TestADCMetadataServer(t *testing.T) {
	isolateADC(t)
	s, _ := storagetest.NewServer(t)
	t.Setenv("GCE_METADATA_HOST", s.Host())
	before := s.TokenCalls()

	c := adcClient(t, s)
	deleteObject(t, s, c, "a")
	deleteObject(t, s, c, "b")
	if n := s.TokenCalls() - before; n != 1 {
		t.Fatalf("metadata token fetched %d times, want 1 (cached)", n)
	}
	if q := s.QuotaProject(); q != "" {
		t.Fatalf("quota project %q sent for metadata credentials", q)
	}
}

func TestSignedURLWithMetadataCredentials(t *testing.T) {
	isolateADC(t)
	s, _ := storagetest.NewServer(t)
	t.Setenv("GCE_METADATA_HOST", s.Host())
	s.AddObject(storagetest.Bucket, "o", []byte("signed by iam"))
	c := adcClient(t, s)
	storage.SetIAMEndpoint(c, s.URL())

	u, err := c.Bucket(storagetest.Bucket).SignedURL("o", &storage.SignedURLOptions{Method: http.MethodGet, Expires: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if code, body := do(t, http.MethodGet, u, nil); code != http.StatusOK || string(body) != "signed by iam" {
		t.Fatalf("signed GET = %d %q", code, body)
	}
	if !slices.Contains(s.Calls(), "SIGNBLOB "+storagetest.ClientEmail+":signBlob") {
		t.Fatalf("no signBlob call: %q", s.Calls())
	}
	if _, err := c.Bucket(storagetest.Bucket).SignedURL("o", &storage.SignedURLOptions{GoogleAccessID: "other@x.iam.gserviceaccount.com", Method: http.MethodGet, Expires: time.Now().Add(time.Minute)}); err == nil {
		t.Fatal("signBlob for an unknown service account succeeded")
	}
}

func TestADCNoCredentials(t *testing.T) {
	isolateADC(t)
	_, err := storage.NewClient(context.Background())
	if err == nil || !strings.HasPrefix(err.Error(), "storage: ") || !strings.Contains(err.Error(), "GOOGLE_APPLICATION_CREDENTIALS") {
		t.Fatalf("err %v", err)
	}
}
