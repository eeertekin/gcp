package storage_test

import (
	"context"
	"errors"
	"testing"

	"github.com/eeertekin/gcp/storage"
	"github.com/eeertekin/gcp/storage/storagetest"
)

func TestDelete(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.AddObject(storagetest.Bucket, "dir/file.txt", []byte("x"))

	if err := c.Bucket(storagetest.Bucket).Object("dir/file.txt").Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Object(storagetest.Bucket, "dir/file.txt"); ok {
		t.Fatal("object not deleted")
	}
	if calls := s.Calls(); len(calls) != 1 || calls[0] != "DELETE "+storagetest.Bucket+"/dir/file.txt" {
		t.Fatalf("calls = %v", calls)
	}
}

func TestDeleteEscapesObjectName(t *testing.T) {
	s, c := storagetest.NewServer(t)
	names := []string{"a b/c?d#e%f", "ünï/cødé+plus", "../dots"}
	for _, n := range names {
		s.AddObject(storagetest.Bucket, n, nil)
	}
	for _, n := range names {
		if err := c.Bucket(storagetest.Bucket).Object(n).Delete(context.Background()); err != nil {
			t.Errorf("%q: %v", n, err)
		}
		if _, ok := s.Object(storagetest.Bucket, n); ok {
			t.Errorf("%q: not deleted", n)
		}
	}
}

func TestDeleteNotExist(t *testing.T) {
	_, c := storagetest.NewServer(t)

	err := c.Bucket(storagetest.Bucket).Object("missing").Delete(context.Background())
	if !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("err = %v, want ErrObjectNotExist", err)
	}
}

func TestDeleteRejectsEmptyNames(t *testing.T) {
	s, c := storagetest.NewServer(t)
	ctx := context.Background()

	if err := c.Bucket("").Object("o").Delete(ctx); err == nil {
		t.Error("empty bucket: expected error")
	}
	if err := c.Bucket(storagetest.Bucket).Object("").Delete(ctx); err == nil {
		t.Error("empty object: expected error")
	}
	if n := s.Attempts(); n != 0 {
		t.Fatalf("invalid names reached the API: %d attempts", n)
	}
}

func TestDeleteRecoversWhenResponseLost(t *testing.T) {
	for _, f := range []storagetest.Fault{
		{Status: 500, AfterApply: true},
		{Drop: true, AfterApply: true},
	} {
		s, c := storagetest.NewServer(t)
		s.AddObject(storagetest.Bucket, "o", nil)
		s.InjectFaults(f)

		if err := c.Bucket(storagetest.Bucket).Object("o").Delete(context.Background()); err != nil {
			t.Fatalf("%+v: delete not recovered: %v", f, err)
		}
		if _, ok := s.Object(storagetest.Bucket, "o"); ok {
			t.Fatalf("%+v: object still exists", f)
		}
	}
}

func TestDeleteNotFoundAfterRateLimitStillReported(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.InjectFaults(storagetest.Fault{Status: 429})

	err := c.Bucket(storagetest.Bucket).Object("missing").Delete(context.Background())
	if !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("err = %v, want ErrObjectNotExist (a rate-limited attempt is never applied)", err)
	}
}
