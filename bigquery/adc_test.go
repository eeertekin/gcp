package bigquery_test

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eeertekin/gcp/bigquery"
	"github.com/eeertekin/gcp/bigquery/bigquerytest"
	"github.com/eeertekin/gcp/option"
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

func adcClient(t *testing.T, s *bigquerytest.Server, opts ...option.ClientOption) *bigquery.Client {
	t.Helper()
	c, err := bigquery.NewClient(context.Background(), bigquery.DetectProjectID, append(opts, option.WithEndpoint(s.URL()))...)
	if err != nil {
		t.Fatal(err)
	}
	c.Retry = bigquery.RetryPolicy{MaxAttempts: 3, Initial: time.Millisecond, Max: time.Millisecond}
	if c.Project() != bigquerytest.ProjectID {
		t.Fatalf("detected project %q", c.Project())
	}
	return c
}

func TestADCEnvironmentFile(t *testing.T) {
	gcloud := isolateADC(t)
	s, _ := bigquerytest.NewServer(t)
	s.AddDataset("d")
	writeFile(t, filepath.Join(gcloud, "application_default_credentials.json"), s.AuthorizedUserJSON())
	path := filepath.Join(t.TempDir(), "sa.json")
	writeFile(t, path, s.ServiceAccountJSON())
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)

	c := adcClient(t, s)
	if _, err := c.Dataset("d").Metadata(context.Background()); err != nil {
		t.Fatal(err)
	}
	if q := s.QuotaProject(); q != "" {
		t.Fatalf("quota project %q: GOOGLE_APPLICATION_CREDENTIALS must win over the gcloud user file", q)
	}
	if n := s.TokenCalls(); n != 1 {
		t.Fatalf("token calls %d", n)
	}

	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing.json"))
	if _, err := bigquery.NewClient(context.Background(), "p"); err == nil || !strings.Contains(err.Error(), "GOOGLE_APPLICATION_CREDENTIALS") {
		t.Fatalf("missing env file: %v", err)
	}
	c = adcClient(t, s, option.WithAuthCredentialsJSON(option.ServiceAccount, s.ServiceAccountJSON()))
	if _, err := c.Dataset("d").Metadata(context.Background()); err != nil {
		t.Fatalf("explicit credentials must bypass GOOGLE_APPLICATION_CREDENTIALS: %v", err)
	}
}

func TestADCGcloudUserCredentials(t *testing.T) {
	gcloud := isolateADC(t)
	s, _ := bigquerytest.NewServer(t)
	s.AddDataset("d")
	writeFile(t, filepath.Join(gcloud, "application_default_credentials.json"), s.AuthorizedUserJSON())

	c := adcClient(t, s)
	if _, err := c.Dataset("d").Metadata(context.Background()); err != nil {
		t.Fatal(err)
	}
	if q := s.QuotaProject(); q != bigquerytest.ProjectID {
		t.Fatalf("X-Goog-User-Project %q, want the quota_project_id of the user credentials", q)
	}

	c = adcClient(t, s, option.WithAuthCredentialsJSON(option.AuthorizedUser, s.AuthorizedUserJSON()))
	if _, err := c.Dataset("d").Metadata(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := s.TokenCalls(); n != 2 {
		t.Fatalf("token calls %d, want one refresh per client", n)
	}
}

func TestADCMetadataServer(t *testing.T) {
	isolateADC(t)
	s, _ := bigquerytest.NewServer(t)
	s.AddDataset("d")
	t.Setenv("GCE_METADATA_HOST", s.Host())

	c := adcClient(t, s)
	for range 2 {
		if _, err := c.Dataset("d").Metadata(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if n := s.TokenCalls(); n != 1 {
		t.Fatalf("metadata token fetched %d times, want 1 (cached)", n)
	}
	if q := s.QuotaProject(); q != "" {
		t.Fatalf("quota project %q sent for metadata credentials", q)
	}
}

func TestADCNoCredentials(t *testing.T) {
	isolateADC(t)
	_, err := bigquery.NewClient(context.Background(), "p")
	if err == nil || !strings.HasPrefix(err.Error(), "bigquery: ") || !strings.Contains(err.Error(), "GOOGLE_APPLICATION_CREDENTIALS") {
		t.Fatalf("err %v", err)
	}
}

type countingTransport struct {
	n atomic.Int32
}

func (c *countingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	c.n.Add(1)
	return http.DefaultTransport.RoundTrip(r)
}

func TestWithHTTPClient(t *testing.T) {
	s, _ := bigquerytest.NewServer(t)
	s.AddDataset("d")
	tr := &countingTransport{}
	c := adcClient(t, s,
		option.WithAuthCredentialsJSON(option.ServiceAccount, s.ServiceAccountJSON()),
		option.WithHTTPClient(&http.Client{Transport: tr}),
	)
	if _, err := c.Dataset("d").Metadata(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := tr.n.Load(); n != 2 {
		t.Fatalf("custom transport saw %d requests, want token exchange + API call", n)
	}
}
