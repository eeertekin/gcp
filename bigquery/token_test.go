package bigquery_test

import (
	"context"
	"strings"
	"testing"

	"github.com/eeertekin/gcp/bigquery/bigquerytest"
)

func TestTokenSignedAndCached(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "a")
	s.AddTable("ds", "b")
	ctx := context.Background()

	if err := c.Dataset("ds").Table("a").Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Dataset("ds").Table("b").Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if n := s.TokenCalls(); n != 1 {
		t.Fatalf("token endpoint called %d times, want 1", n)
	}
}

func TestTokenRefreshedNearExpiry(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.SetTokenTTL(30)
	s.AddTable("ds", "a")
	s.AddTable("ds", "b")
	ctx := context.Background()

	if err := c.Dataset("ds").Table("a").Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if err := c.Dataset("ds").Table("b").Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if n := s.TokenCalls(); n != 2 {
		t.Fatalf("token endpoint called %d times, want 2 (token expiring within a minute must be refreshed)", n)
	}
}

func TestTokenExchangeRejected(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.RejectTokens()

	err := c.Dataset("ds").Table("t").Delete(context.Background())
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
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "t")
	s.FailTokens(503, 429)

	if err := c.Dataset("ds").Table("t").Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if n := s.TokenCalls(); n != 3 {
		t.Fatalf("token calls = %d, want 3", n)
	}
	if n := s.Attempts(); n != 1 {
		t.Fatalf("API attempts = %d, want 1", n)
	}
}
