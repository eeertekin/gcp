package pubsub_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/eeertekin/gcp/option"
	"github.com/eeertekin/gcp/pubsub"
	"github.com/eeertekin/gcp/pubsub/pubsubtest"
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

func adcClient(t *testing.T, s *pubsubtest.Server, opts ...option.ClientOption) *pubsub.Client {
	t.Helper()
	c, err := pubsub.NewClient(context.Background(), pubsub.DetectProjectID, append(opts, option.WithEndpoint(s.URL()))...)
	if err != nil {
		t.Fatal(err)
	}
	c.Retry = pubsub.RetryPolicy{MaxAttempts: 3, Initial: time.Millisecond, Max: time.Millisecond}
	t.Cleanup(func() { c.Close() })
	if c.Project() != pubsubtest.ProjectID {
		t.Fatalf("detected project %q", c.Project())
	}
	return c
}

func publishOne(t *testing.T, s *pubsubtest.Server, c *pubsub.Client, data string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	p := c.Publisher("adc")
	defer p.Stop()
	before := len(s.Batches("adc"))
	if _, err := p.Publish(ctx, &pubsub.Message{Data: []byte(data)}).Get(ctx); err != nil {
		t.Fatal(err)
	}
	batches := s.Batches("adc")
	if len(batches) != before+1 || string(batches[before][0].Data) != data {
		t.Fatalf("stored batches %+v", batches)
	}
}

func TestADCEnvironmentFile(t *testing.T) {
	gcloud := isolateADC(t)
	s, _ := pubsubtest.NewServer(t)
	s.CreateTopic("adc")
	writeFile(t, filepath.Join(gcloud, "application_default_credentials.json"), s.AuthorizedUserJSON())
	path := filepath.Join(t.TempDir(), "sa.json")
	writeFile(t, path, s.ServiceAccountJSON())
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", path)
	before := s.TokenCalls()

	publishOne(t, s, adcClient(t, s), "env")
	if q := s.QuotaProject(); q != "" {
		t.Fatalf("quota project %q: GOOGLE_APPLICATION_CREDENTIALS must win over the gcloud user file", q)
	}
	if n := s.TokenCalls() - before; n != 1 {
		t.Fatalf("token calls %d", n)
	}

	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing.json"))
	if _, err := pubsub.NewClient(context.Background(), "p"); err == nil || !strings.Contains(err.Error(), "GOOGLE_APPLICATION_CREDENTIALS") {
		t.Fatalf("missing env file: %v", err)
	}
	publishOne(t, s, adcClient(t, s, option.WithAuthCredentialsJSON(option.ServiceAccount, s.ServiceAccountJSON())), "explicit")
}

func TestADCGcloudUserCredentials(t *testing.T) {
	gcloud := isolateADC(t)
	s, _ := pubsubtest.NewServer(t)
	s.CreateTopic("adc")
	writeFile(t, filepath.Join(gcloud, "application_default_credentials.json"), s.AuthorizedUserJSON())
	before := s.TokenCalls()

	publishOne(t, s, adcClient(t, s), "gcloud")
	if q := s.QuotaProject(); q != pubsubtest.ProjectID {
		t.Fatalf("X-Goog-User-Project %q, want the quota_project_id of the user credentials", q)
	}

	publishOne(t, s, adcClient(t, s, option.WithAuthCredentialsJSON(option.AuthorizedUser, s.AuthorizedUserJSON())), "explicit")
	if n := s.TokenCalls() - before; n != 2 {
		t.Fatalf("token calls %d, want one refresh per client", n)
	}
}

func TestADCMetadataServer(t *testing.T) {
	isolateADC(t)
	s, _ := pubsubtest.NewServer(t)
	s.CreateTopic("adc")
	t.Setenv("GCE_METADATA_HOST", s.Host())
	before := s.TokenCalls()

	c := adcClient(t, s)
	publishOne(t, s, c, "one")
	publishOne(t, s, c, "two")
	if n := s.TokenCalls() - before; n != 1 {
		t.Fatalf("metadata token fetched %d times, want 1 (cached)", n)
	}
	if q := s.QuotaProject(); q != "" {
		t.Fatalf("quota project %q sent for metadata credentials", q)
	}
}

func TestADCNoCredentials(t *testing.T) {
	isolateADC(t)
	_, err := pubsub.NewClient(context.Background(), "p")
	if err == nil || !strings.HasPrefix(err.Error(), "pubsub: ") || !strings.Contains(err.Error(), "GOOGLE_APPLICATION_CREDENTIALS") {
		t.Fatalf("err %v", err)
	}
	if _, err := pubsub.NewClient(context.Background(), ""); !errors.Is(err, pubsub.ErrEmptyProjectID) {
		t.Fatalf("empty project: %v", err)
	}
}
