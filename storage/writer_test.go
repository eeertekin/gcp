package storage_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/eeertekin/gcp/storage"
	"github.com/eeertekin/gcp/storage/storagetest"
)

const chunk = 256 << 10

func payload(n int) []byte {
	b := make([]byte, n)
	r := rand.New(rand.NewPCG(uint64(n), 42))
	for i := range b {
		b[i] = byte(r.Uint32())
	}
	return b
}

func upload(t *testing.T, w *storage.Writer, data []byte, piece int) {
	t.Helper()
	for len(data) > 0 {
		n := min(piece, len(data))
		if _, err := w.Write(data[:n]); err != nil {
			t.Fatalf("write: %v", err)
		}
		data = data[n:]
	}
}

func assertObject(t *testing.T, s *storagetest.Server, name string, want []byte, contentType string) storagetest.Object {
	t.Helper()
	obj, ok := s.Object(storagetest.Bucket, name)
	if !ok {
		t.Fatalf("object %q not stored", name)
	}
	if !bytes.Equal(obj.Data, want) {
		t.Fatalf("object %q: stored %d bytes, want %d (content differs)", name, len(obj.Data), len(want))
	}
	if obj.ContentType != contentType {
		t.Fatalf("object %q: content type %q, want %q", name, obj.ContentType, contentType)
	}
	return obj
}

func TestWriterSmallUpload(t *testing.T) {
	s, c := storagetest.NewServer(t)
	w := c.Bucket(storagetest.Bucket).Object("dir/hello.txt").NewWriter(context.Background())
	w.ContentType = "text/plain"

	upload(t, w, []byte("hello world"), 3)
	if w.Attrs() != nil {
		t.Fatal("attrs available before Close")
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	obj := assertObject(t, s, "dir/hello.txt", []byte("hello world"), "text/plain")
	if calls := s.Calls(); !slices.Equal(calls, []string{"UPLOAD " + storagetest.Bucket + "/dir/hello.txt"}) {
		t.Fatalf("calls = %v", calls)
	}
	a := w.Attrs()
	if a == nil || a.Bucket != storagetest.Bucket || a.Name != "dir/hello.txt" || a.Size != 11 ||
		a.ContentType != "text/plain" || a.Generation != obj.Generation {
		t.Fatalf("attrs = %+v", a)
	}
}

func TestWriterSniffsContentType(t *testing.T) {
	s, c := storagetest.NewServer(t)
	data := []byte("<!DOCTYPE html><html><body>hi</body></html>")

	w := c.Bucket(storagetest.Bucket).Object("index.html").NewWriter(context.Background())
	upload(t, w, data, len(data))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	assertObject(t, s, "index.html", data, "text/html; charset=utf-8")
}

func TestWriterEmptyObject(t *testing.T) {
	s, c := storagetest.NewServer(t)
	w := c.Bucket(storagetest.Bucket).Object("empty").NewWriter(context.Background())
	w.ContentType = "application/octet-stream"
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	assertObject(t, s, "empty", nil, "application/octet-stream")
	if a := w.Attrs(); a == nil || a.Size != 0 {
		t.Fatalf("attrs = %+v", a)
	}
}

func TestWriterExactChunkIsSingleRequest(t *testing.T) {
	s, c := storagetest.NewServer(t)
	data := payload(chunk)

	w := c.Bucket(storagetest.Bucket).Object("o").NewWriter(context.Background())
	w.ChunkSize = chunk
	upload(t, w, data, 10_000)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	assertObject(t, s, "o", data, "application/octet-stream")
	if calls := s.Calls(); len(calls) != 1 || s.Sessions() != 0 {
		t.Fatalf("calls = %v sessions = %d, want one single-shot upload", calls, s.Sessions())
	}
}

func TestWriterResumableUpload(t *testing.T) {
	s, c := storagetest.NewServer(t)
	data := payload(2*chunk + chunk/2)

	w := c.Bucket(storagetest.Bucket).Object("big.bin").NewWriter(context.Background())
	w.ChunkSize = chunk
	w.ContentType = "application/x-test"
	upload(t, w, data, 7_000)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	assertObject(t, s, "big.bin", data, "application/x-test")
	want := []string{
		"SESSION " + storagetest.Bucket + "/big.bin",
		"CHUNK bytes 0-262143/*",
		"CHUNK bytes 262144-524287/*",
		"CHUNK bytes 524288-655359/655360",
	}
	if calls := s.Calls(); !slices.Equal(calls, want) {
		t.Fatalf("calls =\n%v\nwant\n%v", calls, want)
	}
	if a := w.Attrs(); a == nil || a.Size != int64(len(data)) || a.Name != "big.bin" {
		t.Fatalf("attrs = %+v", a)
	}
}

func TestWriterResumableExactMultiple(t *testing.T) {
	s, c := storagetest.NewServer(t)
	data := payload(3 * chunk)

	w := c.Bucket(storagetest.Bucket).Object("o").NewWriter(context.Background())
	w.ChunkSize = chunk
	upload(t, w, data, len(data))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	assertObject(t, s, "o", data, "application/octet-stream")
	if calls := s.Calls(); calls[len(calls)-1] != "CHUNK bytes 524288-786431/786432" {
		t.Fatalf("final chunk must carry data and total: %v", calls)
	}
}

func TestWriterChunkSizeRoundedUp(t *testing.T) {
	s, c := storagetest.NewServer(t)
	data := payload(3 * chunk)

	w := c.Bucket(storagetest.Bucket).Object("o").NewWriter(context.Background())
	w.ChunkSize = chunk + 1
	upload(t, w, data, len(data))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	assertObject(t, s, "o", data, "application/octet-stream")
	if calls := s.Calls(); !slices.Contains(calls, "CHUNK bytes 0-524287/*") {
		t.Fatalf("chunk size not rounded to 256 KiB multiple: %v", calls)
	}
}

// startResumable sends the session request and the first chunk so faults
// injected afterwards hit the following chunk.
func startResumable(t *testing.T, c *storage.Client, name string, data []byte, chunkSize int) *storage.Writer {
	t.Helper()
	w := c.Bucket(storagetest.Bucket).Object(name).NewWriter(context.Background())
	w.ChunkSize = chunkSize
	upload(t, w, data[:chunkSize+1], chunkSize+1)
	return w
}

func TestWriterResumesAfterChunkFailure(t *testing.T) {
	cases := map[string]storagetest.Fault{
		"applied, 503":        {Status: 503, AfterApply: true},
		"applied, dropped":    {Drop: true, AfterApply: true},
		"not applied, 503":    {Status: 503},
		"not applied, drop":   {Drop: true},
		"not applied, 429":    {Status: 429},
		"persisted none, 308": {Partial: true},
	}
	for name, f := range cases {
		t.Run(name, func(t *testing.T) {
			s, c := storagetest.NewServer(t)
			data := payload(3*chunk + 100)

			w := startResumable(t, c, "o", data, chunk)
			s.InjectFaults(f)
			upload(t, w, data[chunk+1:], 50_000)
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			assertObject(t, s, "o", data, "application/octet-stream")
			if s.Sessions() != 1 {
				t.Fatalf("sessions = %d, want 1", s.Sessions())
			}
		})
	}
}

func TestWriterResumesAfterPartialPersist(t *testing.T) {
	s, c := storagetest.NewServer(t)
	data := payload(5*chunk + 10)

	w := startResumable(t, c, "o", data, 2*chunk)
	s.InjectFaults(storagetest.Fault{Partial: true})
	upload(t, w, data[2*chunk+1:], len(data))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	assertObject(t, s, "o", data, "application/octet-stream")
	want := []string{
		"SESSION " + storagetest.Bucket + "/o",
		"CHUNK bytes 0-524287/*",
		"CHUNK bytes 524288-1048575/*",
		"CHUNK bytes 786432-1048575/*",
		"CHUNK bytes 1048576-1310729/1310730",
	}
	if calls := s.Calls(); !slices.Equal(calls, want) {
		t.Fatalf("calls =\n%v\nwant\n%v", calls, want)
	}
}

func TestWriterFinalChunkResponseLost(t *testing.T) {
	for _, f := range []storagetest.Fault{
		{Status: 503, AfterApply: true},
		{Drop: true, AfterApply: true},
		{Drop: true},
	} {
		s, c := storagetest.NewServer(t)
		data := payload(chunk + 500)

		w := startResumable(t, c, "o", data, chunk)
		upload(t, w, data[chunk+1:], len(data))
		s.InjectFaults(f)
		if err := w.Close(); err != nil {
			t.Fatalf("%+v: %v", f, err)
		}
		obj := assertObject(t, s, "o", data, "application/octet-stream")
		if a := w.Attrs(); a == nil || a.Generation != obj.Generation || a.Size != int64(len(data)) {
			t.Fatalf("%+v: attrs = %+v, object generation %d", f, a, obj.Generation)
		}
	}
}

func TestWriterSessionGoneNotRetried(t *testing.T) {
	s, c := storagetest.NewServer(t)
	data := payload(3 * chunk)

	w := startResumable(t, c, "o", data, chunk)
	before := s.Attempts()
	s.InjectFaults(storagetest.Fault{Status: 404})

	_, err := w.Write(data[chunk+1:])
	var apiErr *storage.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != http.StatusNotFound {
		t.Fatalf("write err = %v, want 404", err)
	}
	if n := s.Attempts() - before; n != 1 {
		t.Fatalf("expired session retried: %d attempts", n)
	}
	if _, err2 := w.Write([]byte("x")); err2 != err {
		t.Fatalf("write after failure = %v, want sticky %v", err2, err)
	}
	if err2 := w.Close(); err2 != err {
		t.Fatalf("close after failure = %v, want sticky %v", err2, err)
	}
	if _, ok := s.Object(storagetest.Bucket, "o"); ok {
		t.Fatal("object created despite failed upload")
	}
}

func TestWriterChunkRetryGivesUp(t *testing.T) {
	s, c := storagetest.NewServer(t)
	c.Retry.MaxAttempts = 3
	data := payload(3 * chunk)

	w := startResumable(t, c, "o", data, chunk)
	for range 20 {
		s.InjectFaults(storagetest.Fault{Status: 503})
	}
	before := s.Attempts()

	_, err := w.Write(data[chunk+1:])
	var apiErr *storage.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != http.StatusServiceUnavailable {
		t.Fatalf("err = %v, want 503", err)
	}
	if n := s.Attempts() - before; n != 3 {
		t.Fatalf("attempts = %d, want 3", n)
	}
	if err := w.Close(); err == nil {
		t.Fatal("close succeeded after failed write")
	}
	if _, ok := s.Object(storagetest.Bucket, "o"); ok {
		t.Fatal("object created despite failed upload")
	}
}

func TestWriterSingleUploadRetried(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.InjectFaults(storagetest.Fault{Status: 503}, storagetest.Fault{Drop: true, AfterApply: true})

	w := c.Bucket(storagetest.Bucket).Object("o").NewWriter(context.Background())
	w.ContentType = "text/plain"
	upload(t, w, []byte("retry me"), 100)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	assertObject(t, s, "o", []byte("retry me"), "text/plain")
	if n := s.Attempts(); n != 3 {
		t.Fatalf("attempts = %d, want 3", n)
	}
}

func TestWriterMissingBucket(t *testing.T) {
	s, c := storagetest.NewServer(t)

	w := c.Bucket("nope").Object("o").NewWriter(context.Background())
	upload(t, w, []byte("x"), 1)
	var apiErr *storage.APIError
	if err := w.Close(); !errors.As(err, &apiErr) || apiErr.Code != http.StatusNotFound {
		t.Fatalf("single upload err = %v, want 404", err)
	}

	w = c.Bucket("nope").Object("o").NewWriter(context.Background())
	w.ChunkSize = chunk
	if _, err := w.Write(payload(chunk + 1)); !errors.As(err, &apiErr) || apiErr.Code != http.StatusNotFound {
		t.Fatalf("resumable upload err = %v, want 404", err)
	}
	if s.Sessions() != 0 {
		t.Fatal("session created for missing bucket")
	}
}

func TestWriterRejectsEmptyObjectName(t *testing.T) {
	s, c := storagetest.NewServer(t)
	w := c.Bucket(storagetest.Bucket).Object("").NewWriter(context.Background())
	upload(t, w, []byte("x"), 1)
	if err := w.Close(); err == nil {
		t.Fatal("expected error for empty object name")
	}
	if n := s.Attempts(); n != 0 {
		t.Fatalf("invalid name reached the API: %d attempts", n)
	}
}

func TestWriterObjectNameEscaping(t *testing.T) {
	names := []string{"a b/c+d", "q?x=1&y=2#frag", "ünï/cødé%20", "../dots"}
	for _, size := range []int{10, 2*chunk + 3} {
		s, c := storagetest.NewServer(t)
		for _, n := range names {
			data := payload(size)
			w := c.Bucket(storagetest.Bucket).Object(n).NewWriter(context.Background())
			w.ChunkSize = chunk
			upload(t, w, data, len(data))
			if err := w.Close(); err != nil {
				t.Fatalf("%q (%d bytes): %v", n, size, err)
			}
			assertObject(t, s, n, data, "application/octet-stream")
		}
	}
}

func TestWriterAfterClose(t *testing.T) {
	s, c := storagetest.NewServer(t)
	w := c.Bucket(storagetest.Bucket).Object("o").NewWriter(context.Background())
	upload(t, w, []byte("x"), 1)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("y")); err == nil {
		t.Fatal("write after close succeeded")
	}
	if err := w.Close(); err != nil {
		t.Fatalf("second close = %v", err)
	}
	if n := s.Attempts(); n != 1 {
		t.Fatalf("attempts = %d, want 1", n)
	}
	assertObject(t, s, "o", []byte("x"), "text/plain; charset=utf-8")
}

func TestWriterContextCanceled(t *testing.T) {
	s, c := storagetest.NewServer(t)
	ctx, cancel := context.WithCancel(context.Background())

	w := c.Bucket(storagetest.Bucket).Object("o").NewWriter(ctx)
	w.ChunkSize = chunk
	upload(t, w, payload(chunk+1), chunk+1)
	cancel()

	if _, err := w.Write([]byte("more")); !errors.Is(err, context.Canceled) {
		t.Fatalf("write err = %v, want canceled", err)
	}
	if err := w.Close(); !errors.Is(err, context.Canceled) {
		t.Fatalf("close err = %v, want canceled", err)
	}
	if _, ok := s.Object(storagetest.Bucket, "o"); ok {
		t.Fatal("object created after cancel")
	}
}

func TestWriterFromFile(t *testing.T) {
	s, c := storagetest.NewServer(t)
	for _, size := range []int{0, 1000, 3*chunk + 17} {
		data := payload(size)
		path := filepath.Join(t.TempDir(), "f.bin")
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		f, err := os.Open(path)
		if err != nil {
			t.Fatal(err)
		}

		name := fmt.Sprintf("files/%d.bin", size)
		w := c.Bucket(storagetest.Bucket).Object(name).NewWriter(context.Background())
		w.ChunkSize = chunk
		w.ContentType = "application/octet-stream"
		if _, err := io.Copy(w, f); err != nil {
			t.Fatal(err)
		}
		f.Close()
		if err := w.Close(); err != nil {
			t.Fatal(err)
		}
		assertObject(t, s, name, data, "application/octet-stream")
	}
}
