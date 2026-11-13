package storage_test

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/eeertekin/gcp/iterator"
	"github.com/eeertekin/gcp/storage"
	"github.com/eeertekin/gcp/storage/storagetest"
)

func jsonSetup(t *testing.T) (*storagetest.Server, *storage.BucketHandle) {
	s, c := storagetest.NewServer(t)
	return s, c.Bucket(storagetest.Bucket)
}

func put(t *testing.T, o *storage.ObjectHandle, data string, meta map[string]string) (*storage.ObjectAttrs, error) {
	t.Helper()
	w := o.NewWriter(context.Background())
	w.ContentType = "text/plain"
	w.Metadata = meta
	w.ChunkSize = chunk
	if _, err := w.Write([]byte(data)); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return w.Attrs(), nil
}

func readAll(t *testing.T, o *storage.ObjectHandle, off, n int64) string {
	t.Helper()
	r, err := o.NewRangeReader(context.Background(), off, n)
	if err != nil {
		t.Fatalf("reader %d+%d: %v", off, n, err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestJSONAttrs(t *testing.T) {
	_, b := jsonSetup(t)
	before := time.Now().Add(-time.Second)
	w, err := put(t, b.Object("a/b"), "hello", map[string]string{"seq": "3"})
	if err != nil {
		t.Fatal(err)
	}
	a, err := b.Object("a/b").Attrs(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if a.Name != "a/b" || a.Size != 5 || a.Generation != w.Generation || a.ContentType != "text/plain" || a.Metadata["seq"] != "3" ||
		a.Created.Before(before) || a.Finalized.IsZero() {
		t.Fatalf("attrs = %+v", a)
	}
	if _, err := b.Object("missing").Attrs(context.Background()); !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("missing attrs = %v", err)
	}
	if _, err := b.Object("a/b").Generation(w.Generation + 1).Attrs(context.Background()); !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("wrong generation attrs = %v", err)
	}
}

func TestJSONResumableMetadata(t *testing.T) {
	s, b := jsonSetup(t)
	data := strings.Repeat("x", 2*chunk+5)
	if _, err := put(t, b.Object("big"), data, map[string]string{"k": "v"}); err != nil {
		t.Fatal(err)
	}
	o, _ := s.Object(storagetest.Bucket, "big")
	if len(o.Data) != len(data) || o.Metadata["k"] != "v" || o.ContentType != "text/plain" {
		t.Fatalf("object = %d bytes meta=%v ct=%q", len(o.Data), o.Metadata, o.ContentType)
	}
}

func TestJSONConditions(t *testing.T) {
	_, b := jsonSetup(t)
	o := b.Object("lease")
	a, err := put(t, o.If(storage.Conditions{DoesNotExist: true}), "1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := put(t, o.If(storage.Conditions{DoesNotExist: true}), "2", nil); !isPrecondition(err) {
		t.Fatalf("second create = %v", err)
	}
	a2, err := put(t, o.If(storage.Conditions{GenerationMatch: a.Generation}), "3", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := put(t, o.If(storage.Conditions{GenerationMatch: a.Generation}), "4", nil); !isPrecondition(err) {
		t.Fatalf("stale cas = %v", err)
	}
	big := strings.Repeat("y", 2*chunk+1)
	if _, err := put(t, o.If(storage.Conditions{GenerationMatch: a.Generation}), big, nil); !isPrecondition(err) {
		t.Fatalf("stale resumable cas = %v", err)
	}
	if err := o.If(storage.Conditions{GenerationMatch: a.Generation}).Delete(context.Background()); !isPrecondition(err) {
		t.Fatalf("stale delete = %v", err)
	}
	if err := o.If(storage.Conditions{GenerationMatch: a2.Generation}).Delete(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := o.If(storage.Conditions{DoesNotExist: true, GenerationMatch: 1}).Delete(context.Background()); err == nil {
		t.Fatal("contradictory conditions accepted")
	}
}

func TestJSONRangeReader(t *testing.T) {
	s, b := jsonSetup(t)
	s.AddObject(storagetest.Bucket, "obj", []byte("0123456789"))
	o := b.Object("obj")
	for _, tc := range []struct {
		off, n int64
		want   string
	}{{0, -1, "0123456789"}, {3, 4, "3456"}, {7, 100, "789"}, {4, -1, "456789"}, {4, 0, ""}, {10, 5, ""}} {
		if got := readAll(t, o, tc.off, tc.n); got != tc.want {
			t.Errorf("read %d+%d = %q, want %q", tc.off, tc.n, got, tc.want)
		}
	}
	if _, err := b.Object("missing").NewReader(context.Background()); !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("missing reader = %v", err)
	}
	if _, err := o.NewRangeReader(context.Background(), -1, 2); err == nil {
		t.Fatal("negative offset accepted")
	}
}

func TestJSONReaderRetry(t *testing.T) {
	s, b := jsonSetup(t)
	s.AddObject(storagetest.Bucket, "obj", []byte("data"))
	s.InjectFaults(storagetest.Fault{Status: 503})
	if got := readAll(t, b.Object("obj"), 0, -1); got != "data" {
		t.Fatalf("read = %q", got)
	}
}

func TestJSONList(t *testing.T) {
	s, b := jsonSetup(t)
	for _, n := range []string{"t/db1/LEASE", "t/db1/log-1", "t/db2/HEAD", "t/top", "u/x"} {
		s.AddObject(storagetest.Bucket, n, []byte("x"))
	}
	s.SetPageSize(2)
	list := func(q *storage.Query) []string {
		it := b.Objects(context.Background(), q)
		var out []string
		for {
			a, err := it.Next()
			if errors.Is(err, iterator.Done) {
				return out
			}
			if err != nil {
				t.Fatal(err)
			}
			if a.Prefix != "" {
				out = append(out, "prefix "+a.Prefix)
			} else {
				out = append(out, a.Name)
			}
		}
	}
	if got, want := list(&storage.Query{Prefix: "t/"}), []string{"t/db1/LEASE", "t/db1/log-1", "t/db2/HEAD", "t/top"}; !slices.Equal(got, want) {
		t.Fatalf("list = %q, want %q", got, want)
	}
	got := list(&storage.Query{Prefix: "t/", Delimiter: "/"})
	slices.Sort(got)
	if want := []string{"prefix t/db1/", "prefix t/db2/", "t/top"}; !slices.Equal(got, want) {
		t.Fatalf("delimited list = %q, want %q", got, want)
	}
	if got := list(nil); len(got) != 5 {
		t.Fatalf("list all = %q", got)
	}
}

func TestJSONCompose(t *testing.T) {
	s, b := jsonSetup(t)
	s.AddObject(storagetest.Bucket, "c1", []byte("ab"))
	s.AddObject(storagetest.Bucket, "c2", []byte("cd"))
	dst := b.Object("dst").If(storage.Conditions{DoesNotExist: true})
	c := dst.ComposerFrom(b.Object("c1"), b.Object("c2"))
	c.ContentType = "application/octet-stream"
	a, err := c.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if a.Size != 4 || a.ContentType != "application/octet-stream" {
		t.Fatalf("attrs = %+v", a)
	}
	if got := readAll(t, b.Object("dst"), 0, -1); got != "abcd" {
		t.Fatalf("composed = %q", got)
	}
	if _, err := dst.ComposerFrom(b.Object("c1")).Run(context.Background()); !isPrecondition(err) {
		t.Fatalf("compose over existing = %v", err)
	}
	if _, err := b.Object("d2").ComposerFrom(b.Object("nope")).Run(context.Background()); err == nil {
		t.Fatal("compose of missing source succeeded")
	}
}

func TestJSONCopy(t *testing.T) {
	s, b := jsonSetup(t)
	if _, err := put(t, b.Object("src"), "payload", map[string]string{"m": "1"}); err != nil {
		t.Fatal(err)
	}
	dst := b.Object("dst").If(storage.Conditions{DoesNotExist: true})
	a, err := dst.CopierFrom(b.Object("src")).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if a.Name != "dst" || a.Size != 7 || a.Metadata["m"] != "1" {
		t.Fatalf("attrs = %+v", a)
	}
	if o, _ := s.Object(storagetest.Bucket, "dst"); !bytes.Equal(o.Data, []byte("payload")) {
		t.Fatalf("copy = %q", o.Data)
	}
	if _, err := dst.CopierFrom(b.Object("src")).Run(context.Background()); !isPrecondition(err) {
		t.Fatalf("copy over existing = %v", err)
	}
	if _, err := b.Object("x").CopierFrom(b.Object("missing")).Run(context.Background()); !errors.Is(err, storage.ErrObjectNotExist) {
		t.Fatalf("copy of missing = %v", err)
	}
	if n := strings.Count(strings.Join(s.Calls(), "\n"), "REWRITE "+storagetest.Bucket+"/src "+storagetest.Bucket+"/dst step-1"); n != 1 {
		t.Fatalf("rewrite continuation calls = %d\n%q", n, s.Calls())
	}
}

func TestJSONContentEncodingTranscoded(t *testing.T) {
	s, b := jsonSetup(t)
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	zw.Write([]byte("sqlite bytes"))
	zw.Close()
	w := b.Object("snap").NewWriter(context.Background())
	w.ContentType = "application/x-sqlite3"
	w.ContentEncoding = "gzip"
	w.Write(gz.Bytes())
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if o, _ := s.Object(storagetest.Bucket, "snap"); o.ContentEncoding != "gzip" || !bytes.Equal(o.Data, gz.Bytes()) {
		t.Fatalf("stored encoding %q, %d bytes", o.ContentEncoding, len(o.Data))
	}
	if a, err := b.Object("snap").Attrs(context.Background()); err != nil || a.ContentEncoding != "gzip" {
		t.Fatalf("attrs = %+v, %v", a, err)
	}
	if got := readAll(t, b.Object("snap"), 0, -1); got != "sqlite bytes" {
		t.Fatalf("read = %q", got)
	}
}

func TestPutObjectAndCreateBucket(t *testing.T) {
	s, c := storagetest.NewServer(t)
	s.CreateBucket("other")
	created := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	s.PutObject("other", "x", storagetest.Object{Data: []byte("abc"), Metadata: map[string]string{"k": "v"}, Created: created})
	a, err := c.Bucket("other").Object("x").Attrs(context.Background())
	if err != nil || a.Size != 3 || !a.Created.Equal(created) || !a.Finalized.Equal(created) || a.Metadata["k"] != "v" {
		t.Fatalf("attrs = %+v, %v", a, err)
	}
}

func TestJSONContentDisposition(t *testing.T) {
	const cd = `attachment; filename="report.csv"`
	for _, tc := range []struct {
		name string
		size int
	}{{"single", 5}, {"resumable", 2*chunk + 5}} {
		t.Run(tc.name, func(t *testing.T) {
			s, b := jsonSetup(t)
			w := b.Object("r.csv").NewWriter(context.Background())
			w.ContentType = "text/csv"
			w.ContentDisposition = cd
			w.ChunkSize = chunk
			w.Write([]byte(strings.Repeat("x", tc.size)))
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}
			if w.Attrs().ContentDisposition != cd {
				t.Fatalf("writer attrs disposition = %q", w.Attrs().ContentDisposition)
			}
			if o, _ := s.Object(storagetest.Bucket, "r.csv"); o.ContentDisposition != cd || o.ContentType != "text/csv" || len(o.Data) != tc.size {
				t.Fatalf("stored disposition=%q ct=%q %d bytes", o.ContentDisposition, o.ContentType, len(o.Data))
			}
			if a, err := b.Object("r.csv").Attrs(context.Background()); err != nil || a.ContentDisposition != cd {
				t.Fatalf("attrs = %+v, %v", a, err)
			}
		})
	}
}

func TestJSONCopyAttrs(t *testing.T) {
	const cd = `attachment; filename="a.pdf"`
	s, b := jsonSetup(t)
	w := b.Object("tmp").NewWriter(context.Background())
	w.ContentType = "application/octet-stream"
	w.ContentDisposition = "inline"
	w.Metadata = map[string]string{"m": "1"}
	w.Write([]byte("%PDF"))
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	c := b.Object("kept").CopierFrom(b.Object("tmp"))
	if _, err := c.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if o, _ := s.Object(storagetest.Bucket, "kept"); o.ContentType != "application/octet-stream" || o.ContentDisposition != "inline" || o.Metadata["m"] != "1" {
		t.Fatalf("inherited = %+v", o)
	}

	c = b.Object("a.pdf").CopierFrom(b.Object("tmp"))
	c.ContentType = "application/pdf"
	c.ContentDisposition = cd
	a, err := c.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if a.ContentType != "application/pdf" || a.ContentDisposition != cd || a.Metadata != nil || a.Size != 4 {
		t.Fatalf("attrs = %+v", a)
	}
	o, _ := s.Object(storagetest.Bucket, "a.pdf")
	if o.ContentType != "application/pdf" || o.ContentDisposition != cd || o.Metadata != nil || string(o.Data) != "%PDF" {
		t.Fatalf("stored = %+v", o)
	}
	if n := strings.Count(strings.Join(s.Calls(), "\n"), "REWRITE "+storagetest.Bucket+"/tmp "+storagetest.Bucket+"/a.pdf step-1"); n != 1 {
		t.Fatalf("rewrite continuation calls = %d\n%q", n, s.Calls())
	}

	c = b.Object("tagged").CopierFrom(b.Object("tmp"))
	c.Metadata = map[string]string{"n": "2"}
	c.ContentEncoding = "identity"
	if _, err := c.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if o, _ := s.Object(storagetest.Bucket, "tagged"); o.Metadata["n"] != "2" || o.Metadata["m"] != "" || o.ContentEncoding != "identity" || o.ContentDisposition != "" {
		t.Fatalf("replaced = %+v", o)
	}
}
