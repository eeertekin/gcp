package storage_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/eeertekin/gcp/storage"
	"github.com/eeertekin/gcp/storage/storagetest"
)

func TestRetryTransientStatus(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.AddObject(storagetest.Bucket, "o", nil)
	s.InjectFaults(
		storagetest.Fault{Status: 503},
		storagetest.Fault{Status: 500},
		storagetest.Fault{Status: 429},
		storagetest.Fault{Status: 408},
	)

	if err := c.Bucket(storagetest.Bucket).Object("o").Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := s.Attempts(); n != 5 {
		t.Fatalf("attempts = %d, want 5", n)
	}
	if _, ok := s.Object(storagetest.Bucket, "o"); ok {
		t.Fatal("object not deleted")
	}
}

func TestNoRetryOnPermanentErrors(t *testing.T) {
	for _, code := range []int{400, 401, 403, 412} {
		s, c := storagetest.NewServer(t)
		s.AddObject(storagetest.Bucket, "o", nil)
		s.InjectFaults(storagetest.Fault{Status: code})

		err := c.Bucket(storagetest.Bucket).Object("o").Delete(context.Background())
		var apiErr *storage.APIError
		if !errors.As(err, &apiErr) || apiErr.Code != code || apiErr.Reason != "injected" {
			t.Errorf("%d: err = %v", code, err)
		}
		if n := s.Attempts(); n != 1 {
			t.Errorf("%d: retried permanent error, attempts = %d", code, n)
		}
	}
}

func TestRetryGivesUp(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.AddObject(storagetest.Bucket, "o", nil)
	c.Retry.MaxAttempts = 3
	for range 10 {
		s.InjectFaults(storagetest.Fault{Status: 503})
	}

	err := c.Bucket(storagetest.Bucket).Object("o").Delete(context.Background())
	var apiErr *storage.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != http.StatusServiceUnavailable {
		t.Fatalf("err = %v, want 503", err)
	}
	if n := s.Attempts(); n != 3 {
		t.Fatalf("attempts = %d, want 3", n)
	}
	if _, ok := s.Object(storagetest.Bucket, "o"); !ok {
		t.Fatal("object deleted despite all attempts failing")
	}
}

func TestRetryDisabled(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.AddObject(storagetest.Bucket, "o", nil)
	c.Retry = storage.RetryPolicy{}
	s.InjectFaults(storagetest.Fault{Status: 503})

	if err := c.Bucket(storagetest.Bucket).Object("o").Delete(context.Background()); err == nil {
		t.Fatal("expected error with retries disabled")
	}
	if n := s.Attempts(); n != 1 {
		t.Fatalf("attempts = %d, want 1", n)
	}
}

func TestRetryRespectsContext(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.AddObject(storagetest.Bucket, "o", nil)
	c.Retry = storage.RetryPolicy{MaxAttempts: 10, Initial: 2 * time.Second, Max: 2 * time.Second}
	for range 10 {
		s.InjectFaults(storagetest.Fault{Status: 503})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := c.Bucket(storagetest.Bucket).Object("o").Delete(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("backoff ignored context: took %s", elapsed)
	}
}

func TestRetryBackoffGrows(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.AddObject(storagetest.Bucket, "o", nil)
	c.Retry = storage.RetryPolicy{MaxAttempts: 4, Initial: 40 * time.Millisecond, Max: time.Second}
	s.InjectFaults(storagetest.Fault{Status: 503}, storagetest.Fault{Status: 503}, storagetest.Fault{Status: 503})

	start := time.Now()
	if err := c.Bucket(storagetest.Bucket).Object("o").Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	// jittered delays are at least half of 40ms + 80ms + 160ms
	if elapsed := time.Since(start); elapsed < 140*time.Millisecond {
		t.Fatalf("retries did not back off: %s", elapsed)
	}
}
