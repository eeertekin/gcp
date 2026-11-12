package bigquery_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/eeertekin/gcp/bigquery"
	"github.com/eeertekin/gcp/bigquery/bigquerytest"
	"github.com/eeertekin/gcp/googleapi"
)

func TestRetryTransientStatus(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "t")
	s.InjectFaults(
		bigquerytest.Fault{Status: 503},
		bigquerytest.Fault{Status: 500, Reason: "backendError"},
		bigquerytest.Fault{Status: 429},
		bigquerytest.Fault{Status: 403, Reason: "rateLimitExceeded"},
	)

	if err := c.Dataset("ds").Table("t").Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := s.Attempts(); n != 5 {
		t.Fatalf("attempts = %d, want 5", n)
	}
	if s.HasTable("ds", "t") {
		t.Fatal("table not deleted")
	}
}

func TestNoRetryOnPermanentErrors(t *testing.T) {
	cases := []bigquerytest.Fault{
		{Status: 400, Reason: "invalid"},
		{Status: 403, Reason: "accessDenied"},
		{Status: 403, Reason: "quotaExceeded"},
		{Status: 404, Reason: "notFound"},
	}
	for _, f := range cases {
		s, c := bigquerytest.NewServer(t)
		s.AddTable("ds", "t")
		s.InjectFaults(f)

		err := c.Dataset("ds").Table("t").Delete(context.Background())
		var apiErr *googleapi.Error
		if !errors.As(err, &apiErr) || apiErr.Code != f.Status || len(apiErr.Errors) == 0 || apiErr.Errors[0].Reason != f.Reason {
			t.Errorf("%+v: err = %v", f, err)
		}
		if n := s.Attempts(); n != 1 {
			t.Errorf("%+v: retried permanent error, attempts = %d", f, n)
		}
	}
}

func TestRetryGivesUp(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "t")
	c.Retry.MaxAttempts = 3
	for range 10 {
		s.InjectFaults(bigquerytest.Fault{Status: 503})
	}

	err := c.Dataset("ds").Table("t").Delete(context.Background())
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != http.StatusServiceUnavailable {
		t.Fatalf("err = %v, want 503", err)
	}
	if n := s.Attempts(); n != 3 {
		t.Fatalf("attempts = %d, want 3", n)
	}
	if !s.HasTable("ds", "t") {
		t.Fatal("table deleted despite all attempts failing")
	}
}

func TestRetryDisabled(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "t")
	c.Retry = bigquery.RetryPolicy{}
	s.InjectFaults(bigquerytest.Fault{Status: 503})

	if err := c.Dataset("ds").Table("t").Delete(context.Background()); err == nil {
		t.Fatal("expected error with retries disabled")
	}
	if n := s.Attempts(); n != 1 {
		t.Fatalf("attempts = %d, want 1", n)
	}
}

func TestRetryRespectsContext(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "t")
	c.Retry = bigquery.RetryPolicy{MaxAttempts: 10, Initial: 2 * time.Second, Max: 2 * time.Second}
	for range 10 {
		s.InjectFaults(bigquerytest.Fault{Status: 503})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := c.Dataset("ds").Table("t").Delete(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("backoff ignored context: took %s", elapsed)
	}
}

func TestRetryBackoffGrows(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "t")
	c.Retry = bigquery.RetryPolicy{MaxAttempts: 4, Initial: 40 * time.Millisecond, Max: time.Second}
	s.InjectFaults(bigquerytest.Fault{Status: 503}, bigquerytest.Fault{Status: 503}, bigquerytest.Fault{Status: 503})

	start := time.Now()
	if err := c.Dataset("ds").Table("t").Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	// jittered delays are at least half of 40ms + 80ms + 160ms
	if elapsed := time.Since(start); elapsed < 140*time.Millisecond {
		t.Fatalf("retries did not back off: %s", elapsed)
	}
}

func TestRetryConnectionDrop(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "src")
	s.InjectFaults(bigquerytest.Fault{Drop: true})
	ds := c.Dataset("ds")

	copier := ds.Table("dst").CopierFrom(ds.Table("src"))
	copier.JobID = "copy-1"
	job, err := copier.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if job.ID() != "copy-1" || len(s.Jobs()) != 1 {
		t.Fatalf("job = %q jobs = %+v", job.ID(), s.Jobs())
	}
}

func TestInsertJobRecoversWhenResponseLost(t *testing.T) {
	for _, f := range []bigquerytest.Fault{
		{Status: 503, AfterApply: true},
		{Drop: true, AfterApply: true},
	} {
		s, c := bigquerytest.NewServer(t)
		s.AddTable("ds", "t")
		s.InjectFaults(f)
		ctx := context.Background()

		loader := c.Dataset("ds").Table("t").LoaderFrom(bigquery.NewGCSReference("gs://bucket/a.json"))
		loader.JobID = "load-1"
		job, err := loader.Run(ctx)
		if err != nil {
			t.Fatalf("%+v: insert not recovered: %v", f, err)
		}
		if job.ID() != "load-1" || job.Location() != bigquerytest.Location {
			t.Fatalf("%+v: job id=%q location=%q", f, job.ID(), job.Location())
		}
		if n := len(s.Jobs()); n != 1 {
			t.Fatalf("%+v: %d jobs created, want 1", f, n)
		}
		status, err := job.Wait(ctx)
		if err != nil || status.Err() != nil {
			t.Fatalf("%+v: wait err=%v status err=%v", f, err, status.Err())
		}
	}
}

func TestInsertJobConflictNotMaskedWithoutPriorApply(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "src")
	ds := c.Dataset("ds")

	first := ds.Table("dst").CopierFrom(ds.Table("src"))
	first.JobID = "dup"
	if _, err := first.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	s.InjectFaults(bigquerytest.Fault{Status: 429})
	second := ds.Table("dst").CopierFrom(ds.Table("src"))
	second.JobID = "dup"
	_, err := second.Run(context.Background())
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != http.StatusConflict {
		t.Fatalf("err = %v, want 409 (a rate-limited attempt is never applied)", err)
	}
}

func TestCreateTableRecoversWhenResponseLost(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.InjectFaults(bigquerytest.Fault{Status: 502, AfterApply: true})

	err := c.Dataset("ds").Table("t").Create(context.Background(), &bigquery.TableMetadata{
		Schema: bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}},
	})
	if err != nil {
		t.Fatalf("create not recovered: %v", err)
	}
	if !s.HasTable("ds", "t") || s.Attempts() != 2 {
		t.Fatalf("table=%v attempts=%d", s.HasTable("ds", "t"), s.Attempts())
	}
}

func TestCreateTableConflictNotMaskedWithoutPriorApply(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "t")
	s.InjectFaults(bigquerytest.Fault{Status: 429})

	err := c.Dataset("ds").Table("t").Create(context.Background(), nil)
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != http.StatusConflict {
		t.Fatalf("err = %v, want 409", err)
	}
}

func TestDeleteTableRecoversWhenResponseLost(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "t")
	s.InjectFaults(bigquerytest.Fault{Status: 500, AfterApply: true})

	if err := c.Dataset("ds").Table("t").Delete(context.Background()); err != nil {
		t.Fatalf("delete not recovered: %v", err)
	}
	if s.HasTable("ds", "t") {
		t.Fatal("table still exists")
	}
}

func TestDeleteTableNotFoundAfterRateLimitStillReported(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.InjectFaults(bigquerytest.Fault{Status: 429})

	err := c.Dataset("ds").Table("missing").Delete(context.Background())
	if !bigquery.IsNotFound(err) {
		t.Fatalf("err = %v, want 404", err)
	}
}

func TestWaitRetriesTransientPollFailures(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "src")
	ds := c.Dataset("ds")
	ctx := context.Background()

	job, err := ds.Table("dst").CopierFrom(ds.Table("src")).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	s.InjectFaults(bigquerytest.Fault{Status: 503}, bigquerytest.Fault{Status: 504})

	status, err := job.Wait(ctx)
	if err != nil || status.Err() != nil || !status.Done() {
		t.Fatalf("wait err=%v status=%+v", err, status)
	}
	if !s.HasTable("ds", "dst") {
		t.Fatal("copy did not complete")
	}
}
