package storage_test

import (
	"context"
	"strings"
	"testing"

	"github.com/eeertekin/gcp/storage/storagetest"
)

func TestTokenSignedAndCached(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.AddObject(storagetest.Bucket, "a", nil)
	s.AddObject(storagetest.Bucket, "b", nil)
	ctx := context.Background()

	if err := c.Bucket(storagetest.Bucket).Object("a").Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Bucket(storagetest.Bucket).Object("b").Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if n := s.TokenCalls(); n != 1 {
		t.Fatalf("token endpoint called %d times, want 1", n)
	}
}

func TestTokenRefreshedNearExpiry(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.SetTokenTTL(30)
	s.AddObject(storagetest.Bucket, "a", nil)
	s.AddObject(storagetest.Bucket, "b", nil)
	ctx := context.Background()

	if err := c.Bucket(storagetest.Bucket).Object("a").Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Bucket(storagetest.Bucket).Object("b").Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if n := s.TokenCalls(); n != 2 {
		t.Fatalf("token endpoint called %d times, want 2 (token expiring within a minute must be refreshed)", n)
	}
}

func TestTokenExchangeRejected(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.RejectTokens()

	err := c.Bucket(storagetest.Bucket).Object("o").Delete(context.Background())
	if err == nil || !strings.Contains(err.Error(), "token exchange failed") {
		t.Fatalf("err = %v, want token exchange failure", err)
	}
	if calls := s.Calls(); len(calls) != 0 {
		t.Fatalf("API called without token: %v", calls)
	}
	if n := s.TokenCalls(); n != 1 {
		t.Fatalf("rejected credentials retried: %d token calls", n)
	}
}

func TestTokenTransientFailureRetried(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.AddObject(storagetest.Bucket, "o", nil)
	s.FailTokens(503, 429)

	if err := c.Bucket(storagetest.Bucket).Object("o").Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := s.TokenCalls(); n != 3 {
		t.Fatalf("token calls = %d, want 3", n)
	}
	if n := s.Attempts(); n != 1 {
		t.Fatalf("API attempts = %d, want 1", n)
	}
}
