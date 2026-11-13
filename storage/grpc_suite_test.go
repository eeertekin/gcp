package storage_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/eeertekin/gcp/iterator"
	"github.com/eeertekin/gcp/option"
	"github.com/eeertekin/gcp/storage"
	"github.com/eeertekin/gcp/storage/storagetest"
)

func TestGRPCConformance(t *testing.T) {
	endpoint := os.Getenv("STORAGE_GRPC_TEST_ENDPOINT")
	if endpoint == "" {
		t.Skip("set STORAGE_GRPC_TEST_ENDPOINT and STORAGE_GRPC_TEST_BUCKET")
	}
	tokens, _ := storagetest.NewServer(t)
	c, err := storage.NewGRPCClient(context.Background(),
		option.WithAuthCredentialsJSON(option.ServiceAccount, tokens.Credentials()),
		option.WithEndpoint(endpoint))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	runAppendableSuite(t, c, os.Getenv("STORAGE_GRPC_TEST_BUCKET"), true)
}

func TestGRPCFake(t *testing.T) {
	s, _ := storagetest.NewServer(t)
	runAppendableSuite(t, s.GRPCClient(), storagetest.Bucket, false)
}

type suite struct {
	t      *testing.T
	c      *storage.Client
	bucket string
	prefix string
}

func runAppendableSuite(t *testing.T, c *storage.Client, bucket string, external bool) {
	prefix := fmt.Sprintf("suite-%d/", time.Now().UnixNano())
	if external {
		t.Cleanup(func() {
			it := c.Bucket(bucket).Objects(context.Background(), &storage.Query{Prefix: prefix})
			for {
				a, err := it.Next()
				if err != nil {
					return
				}
				c.Bucket(bucket).Object(a.Name).Delete(context.Background())
			}
		})
	}
	for _, tc := range []struct {
		name string
		fn   func(*suite)
	}{
		{"AppendFlushRead", testAppendFlushRead},
		{"TakeoverAppendFinalize", testTakeoverAppendFinalize},
		{"TakeoverFinalizeWithoutWrite", testTakeoverFinalizeWithoutWrite},
		{"EmptyFinalizedSegment", testEmptyFinalizedSegment},
		{"FinalizeOnCloseHonouredAtClose", testFinalizeOnCloseHonouredAtClose},
		{"FinalizeAfterFlushes", testFinalizeAfterFlushes},
		{"CreateExistingFails", testCreateExistingFails},
		{"FencedWriterFails", testFencedWriterFails},
		{"SmallObjectCAS", testSmallObjectCAS},
		{"LargeAppendable", testLargeAppendable},
		{"PlainWriterRejected", testPlainWriterRejected},
		{"List", testList},
		{"ListDelimiter", testListDelimiter},
		{"NotFound", testNotFound},
		{"Delete", testDelete},
		{"RangeEdges", testRangeEdges},
		{"Metadata", testMetadata},
		{"OpenWriterVisibility", testOpenWriterVisibility},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if external && tc.name == "Metadata" && os.Getenv("STORAGE_GRPC_TEST_METADATA") == "" {
				t.Skip("set STORAGE_GRPC_TEST_METADATA when the server keeps custom metadata")
			}
			tc.fn(&suite{t: t, c: c, bucket: bucket, prefix: prefix + tc.name + "/"})
		})
	}
}

func (s *suite) object(name string) *storage.ObjectHandle {
	return s.c.Bucket(s.bucket).Object(s.prefix + name)
}

func newAppendable(o *storage.ObjectHandle, finalize bool) *storage.Writer {
	w := o.NewWriter(context.Background())
	w.Append = true
	w.FinalizeOnClose = finalize
	w.ChunkSize = 256 * 1024
	w.ContentType = "application/octet-stream"
	return w
}

func (s *suite) writeFlush(w *storage.Writer, p string) int64 {
	s.t.Helper()
	if _, err := w.Write([]byte(p)); err != nil {
		s.t.Fatalf("write %q: %v", p, err)
	}
	n, err := w.Flush()
	if err != nil {
		s.t.Fatalf("flush after %q: %v", p, err)
	}
	return n
}

func (s *suite) read(o *storage.ObjectHandle, off, n int64) string {
	s.t.Helper()
	r, err := o.NewRangeReader(context.Background(), off, n)
	if err != nil {
		s.t.Fatalf("range reader %d+%d: %v", off, n, err)
	}
	defer r.Close()
	b, err := io.ReadAll(r)
	if err != nil {
		s.t.Fatalf("read %d+%d: %v", off, n, err)
	}
	return string(b)
}

func (s *suite) attrs(o *storage.ObjectHandle) *storage.ObjectAttrs {
	s.t.Helper()
	a, err := o.Attrs(context.Background())
	if err != nil {
		s.t.Fatalf("attrs: %v", err)
	}
	return a
}

func isPrecondition(err error) bool {
	var e *storage.APIError
	return errors.As(err, &e) && e.Code == http.StatusPreconditionFailed
}

func (s *suite) putSmall(o *storage.ObjectHandle, conds *storage.Conditions, body string) (int64, error) {
	if conds != nil {
		o = o.If(*conds)
	}
	w := o.NewWriter(context.Background())
	w.Append = true
	w.FinalizeOnClose = true
	w.ContentType = "application/json"
	if _, err := w.Write([]byte(body)); err != nil {
		w.Close()
		return 0, err
	}
	if err := w.Close(); err != nil {
		return 0, err
	}
	return w.Attrs().Generation, nil
}

func (s *suite) takeover(o *storage.ObjectHandle, finalize bool) (*storage.Writer, int64) {
	s.t.Helper()
	w, off, err := o.Generation(s.attrs(o).Generation).NewWriterFromAppendableObject(context.Background(), &storage.AppendableWriterOpts{FinalizeOnClose: finalize})
	if err != nil {
		s.t.Fatalf("takeover: %v", err)
	}
	return w, off
}

func testAppendFlushRead(s *suite) {
	o := s.object("log")
	w := newAppendable(o.If(storage.Conditions{DoesNotExist: true}), false)
	if n, err := w.Flush(); err != nil || n != 0 {
		s.t.Fatalf("initial flush = %d, %v; want 0, nil", n, err)
	}
	if n := s.writeFlush(w, "hello"); n != 5 {
		s.t.Fatalf("flush = %d, want 5", n)
	}
	a := s.attrs(o)
	if !a.Finalized.IsZero() || a.Generation == 0 || a.Created.IsZero() {
		s.t.Fatalf("attrs after first flush: finalized=%v gen=%d created=%v", a.Finalized, a.Generation, a.Created)
	}
	if got := s.read(o, 0, 5); got != "hello" {
		s.t.Fatalf("read = %q", got)
	}
	if n := s.writeFlush(w, " world"); n != 11 {
		s.t.Fatalf("flush = %d, want 11", n)
	}
	if got := s.read(o, 5, 6); got != " world" {
		s.t.Fatalf("read tail = %q", got)
	}
	if got := s.read(o, 0, -1); got != "hello world" {
		s.t.Fatalf("read all = %q", got)
	}
	if err := w.Close(); err != nil {
		s.t.Fatalf("close: %v", err)
	}
	b := s.attrs(o)
	if b.Size != 11 || !b.Finalized.IsZero() || b.Generation != a.Generation || b.ContentType != "application/octet-stream" {
		s.t.Fatalf("attrs after close: size=%d finalized=%v gen=%d (was %d) ct=%q", b.Size, b.Finalized, b.Generation, a.Generation, b.ContentType)
	}
}

func testTakeoverAppendFinalize(s *suite) {
	o := s.object("log")
	w := newAppendable(o.If(storage.Conditions{DoesNotExist: true}), false)
	s.writeFlush(w, "abc")
	if err := w.Close(); err != nil {
		s.t.Fatalf("close: %v", err)
	}
	gen := s.attrs(o).Generation
	tw, off := s.takeover(o, true)
	if off != 3 {
		s.t.Fatalf("takeover offset = %d, want 3", off)
	}
	if n := s.writeFlush(tw, "def"); n != 6 {
		s.t.Fatalf("flush = %d, want 6", n)
	}
	if err := tw.Close(); err != nil {
		s.t.Fatalf("close+finalize: %v", err)
	}
	if tw.Attrs().Size != 6 || tw.Attrs().Finalized.IsZero() {
		s.t.Fatalf("writer attrs: size=%d finalized=%v", tw.Attrs().Size, tw.Attrs().Finalized)
	}
	a := s.attrs(o)
	if a.Finalized.IsZero() || a.Size != 6 || a.Generation != gen {
		s.t.Fatalf("attrs after finalize: size=%d finalized=%v gen=%d (want %d)", a.Size, a.Finalized, a.Generation, gen)
	}
	if got := s.read(o, 0, -1); got != "abcdef" {
		s.t.Fatalf("read = %q", got)
	}
	_, _, err := o.Generation(gen).NewWriterFromAppendableObject(context.Background(), &storage.AppendableWriterOpts{FinalizeOnClose: true})
	if !isPrecondition(err) {
		s.t.Fatalf("takeover of finalized object = %v, want 412", err)
	}
}

func testTakeoverFinalizeWithoutWrite(s *suite) {
	o := s.object("log")
	w := newAppendable(o.If(storage.Conditions{DoesNotExist: true}), false)
	s.writeFlush(w, "xyz")
	if err := w.Close(); err != nil {
		s.t.Fatalf("close: %v", err)
	}
	tw, _ := s.takeover(o, true)
	if err := tw.Close(); err != nil {
		s.t.Fatalf("close: %v", err)
	}
	if a := s.attrs(o); a.Finalized.IsZero() || a.Size != 3 {
		s.t.Fatalf("attrs: size=%d finalized=%v", a.Size, a.Finalized)
	}
}

func testEmptyFinalizedSegment(s *suite) {
	o := s.object("log")
	w := newAppendable(o.If(storage.Conditions{DoesNotExist: true}), true)
	if _, err := w.Flush(); err != nil {
		s.t.Fatalf("flush: %v", err)
	}
	if err := w.Close(); err != nil {
		s.t.Fatalf("close: %v", err)
	}
	if a := s.attrs(o); a.Finalized.IsZero() || a.Size != 0 {
		s.t.Fatalf("attrs: size=%d finalized=%v", a.Size, a.Finalized)
	}
}

func testFinalizeOnCloseHonouredAtClose(s *suite) {
	o := s.object("log")
	w := newAppendable(o.If(storage.Conditions{DoesNotExist: true}), false)
	w.Flush()
	s.writeFlush(w, "frame")
	w.FinalizeOnClose = true
	if err := w.Close(); err != nil {
		s.t.Fatalf("close: %v", err)
	}
	if a := s.attrs(o); a.Finalized.IsZero() || a.Size != 5 {
		s.t.Fatalf("attrs: size=%d finalized=%v; want 5, finalized", a.Size, a.Finalized)
	}
}

func testFinalizeAfterFlushes(s *suite) {
	o := s.object("log")
	w := newAppendable(o.If(storage.Conditions{DoesNotExist: true}), true)
	w.Flush()
	s.writeFlush(w, "fra")
	s.writeFlush(w, "me")
	if err := w.Close(); err != nil {
		s.t.Fatalf("close: %v", err)
	}
	if a := s.attrs(o); a.Finalized.IsZero() || a.Size != 5 {
		s.t.Fatalf("attrs: size=%d finalized=%v", a.Size, a.Finalized)
	}
	if got := s.read(o, 0, -1); got != "frame" {
		s.t.Fatalf("read = %q", got)
	}
}

func testCreateExistingFails(s *suite) {
	o := s.object("log")
	w := newAppendable(o.If(storage.Conditions{DoesNotExist: true}), false)
	s.writeFlush(w, "a")
	if err := w.Close(); err != nil {
		s.t.Fatal(err)
	}
	w2 := newAppendable(o.If(storage.Conditions{DoesNotExist: true}), false)
	if _, err := w2.Flush(); !isPrecondition(err) {
		s.t.Fatalf("flush on existing object = %v, want 412", err)
	}
	w2.Close()
	if a := s.attrs(o); a.Size != 1 {
		s.t.Fatalf("size = %d, want 1", a.Size)
	}
}

func testFencedWriterFails(s *suite) {
	o := s.object("log")
	w := newAppendable(o.If(storage.Conditions{DoesNotExist: true}), false)
	s.writeFlush(w, "old")
	tw, off := s.takeover(o, true)
	if off != 3 {
		s.t.Fatalf("takeover offset = %d", off)
	}
	if err := tw.Close(); err != nil {
		s.t.Fatalf("finalize: %v", err)
	}
	_, err := w.Write([]byte("late"))
	if err == nil {
		_, err = w.Flush()
	}
	if !isPrecondition(err) {
		s.t.Fatalf("fenced writer = %v, want 412", err)
	}
	w.Close()
	if got := s.read(o, 0, -1); got != "old" {
		s.t.Fatalf("content = %q, want old", got)
	}
}

func testSmallObjectCAS(s *suite) {
	o := s.object("LEASE")
	g1, err := s.putSmall(o, &storage.Conditions{DoesNotExist: true}, `{"o":"a"}`)
	if err != nil {
		s.t.Fatalf("create: %v", err)
	}
	if _, err := s.putSmall(o, &storage.Conditions{DoesNotExist: true}, `{"o":"x"}`); !isPrecondition(err) {
		s.t.Fatalf("second create = %v, want 412", err)
	}
	g2, err := s.putSmall(o, &storage.Conditions{GenerationMatch: g1}, `{"o":"b"}`)
	if err != nil {
		s.t.Fatalf("cas g1: %v", err)
	}
	if g2 == g1 {
		s.t.Fatalf("generation did not change: %d", g2)
	}
	if _, err := s.putSmall(o, &storage.Conditions{GenerationMatch: g1}, `{"o":"c"}`); !isPrecondition(err) {
		s.t.Fatalf("stale cas = %v, want 412", err)
	}
	if _, err := s.putSmall(o, nil, `{"o":"d"}`); err != nil {
		s.t.Fatalf("unconditional overwrite: %v", err)
	}
	if a := s.attrs(o); a.Finalized.IsZero() || a.ContentType != "application/json" || a.Size != 9 {
		s.t.Fatalf("attrs: finalized=%v ct=%q size=%d", a.Finalized, a.ContentType, a.Size)
	}
	if got := s.read(o, 0, -1); got != `{"o":"d"}` {
		s.t.Fatalf("body = %s", got)
	}
}

func testLargeAppendable(s *suite) {
	o := s.object("snap")
	data := make([]byte, 9*256*1024+17)
	rand.Read(data)
	w := newAppendable(o.If(storage.Conditions{DoesNotExist: true}), true)
	if _, err := w.Write(data); err != nil {
		s.t.Fatalf("write: %v", err)
	}
	if err := w.Close(); err != nil {
		s.t.Fatalf("close: %v", err)
	}
	if w.Attrs().Size != int64(len(data)) {
		s.t.Fatalf("writer attrs size = %d", w.Attrs().Size)
	}
	if got := s.read(o, 0, -1); !bytes.Equal([]byte(got), data) {
		s.t.Fatalf("read %d bytes, mismatch with %d written", len(got), len(data))
	}
	if got := s.read(o, 256*1024-3, 6); got != string(data[256*1024-3:256*1024+3]) {
		s.t.Fatalf("range across chunk boundary mismatch")
	}
}

func testPlainWriterRejected(s *suite) {
	w := s.object("plain").NewWriter(context.Background())
	w.Write([]byte("x"))
	if err := w.Close(); err == nil {
		s.t.Fatal("plain upload succeeded")
	}
}

func (s *suite) list(q *storage.Query) []*storage.ObjectAttrs {
	s.t.Helper()
	it := s.c.Bucket(s.bucket).Objects(context.Background(), q)
	var out []*storage.ObjectAttrs
	for {
		a, err := it.Next()
		if errors.Is(err, iterator.Done) {
			return out
		}
		if err != nil {
			s.t.Fatalf("list: %v", err)
		}
		out = append(out, a)
	}
}

func testList(s *suite) {
	if _, err := s.putSmall(s.object("a/1"), nil, "one"); err != nil {
		s.t.Fatal(err)
	}
	w := newAppendable(s.object("a/2").If(storage.Conditions{DoesNotExist: true}), false)
	s.writeFlush(w, "two!")
	defer w.Close()
	if _, err := s.putSmall(s.object("b/3"), nil, "three"); err != nil {
		s.t.Fatal(err)
	}
	prefix := s.prefix + "a/"
	var got []string
	for _, a := range s.list(&storage.Query{Prefix: prefix}) {
		got = append(got, fmt.Sprintf("%s finalized=%v", a.Name[len(prefix):], !a.Finalized.IsZero()))
	}
	if want := []string{"1 finalized=true", "2 finalized=false"}; !slices.Equal(got, want) {
		s.t.Fatalf("list = %q, want %q", got, want)
	}
}

func testListDelimiter(s *suite) {
	for _, name := range []string{"t/db1/LEASE", "t/db1/log-1", "t/db2/HEAD", "t/top"} {
		if _, err := s.putSmall(s.object(name), nil, "x"); err != nil {
			s.t.Fatal(err)
		}
	}
	prefix := s.prefix + "t/"
	var got []string
	for _, a := range s.list(&storage.Query{Prefix: prefix, Delimiter: "/"}) {
		if a.Prefix != "" {
			got = append(got, "prefix "+a.Prefix[len(prefix):])
		} else {
			got = append(got, "object "+a.Name[len(prefix):])
		}
	}
	slices.Sort(got)
	if want := []string{"object top", "prefix db1/", "prefix db2/"}; !slices.Equal(got, want) {
		s.t.Fatalf("list = %q, want %q", got, want)
	}
}

func testNotFound(s *suite) {
	ctx := context.Background()
	o := s.object("missing")
	if _, err := o.Attrs(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
		s.t.Fatalf("attrs = %v", err)
	}
	if _, err := o.NewReader(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
		s.t.Fatalf("reader = %v", err)
	}
	if _, err := o.NewRangeReader(ctx, 3, 4); !errors.Is(err, storage.ErrObjectNotExist) {
		s.t.Fatalf("range reader = %v", err)
	}
	if err := o.Delete(ctx); !errors.Is(err, storage.ErrObjectNotExist) {
		s.t.Fatalf("delete = %v", err)
	}
	if _, _, err := o.Generation(1).NewWriterFromAppendableObject(ctx, &storage.AppendableWriterOpts{}); !errors.Is(err, storage.ErrObjectNotExist) {
		s.t.Fatalf("takeover of missing object = %v", err)
	}
}

func testDelete(s *suite) {
	f := s.object("finalized")
	if _, err := s.putSmall(f, nil, "x"); err != nil {
		s.t.Fatal(err)
	}
	u := s.object("unfinalized")
	w := newAppendable(u.If(storage.Conditions{DoesNotExist: true}), false)
	s.writeFlush(w, "y")
	w.Close()
	for _, o := range []*storage.ObjectHandle{f, u} {
		if err := o.Delete(context.Background()); err != nil {
			s.t.Fatalf("delete: %v", err)
		}
		if _, err := o.Attrs(context.Background()); !errors.Is(err, storage.ErrObjectNotExist) {
			s.t.Fatalf("attrs after delete = %v", err)
		}
	}
	w2 := newAppendable(u.If(storage.Conditions{DoesNotExist: true}), false)
	if n := s.writeFlush(w2, "again"); n != 5 {
		s.t.Fatalf("recreate flush = %d", n)
	}
	w2.Close()
}

func testRangeEdges(s *suite) {
	o := s.object("obj")
	if _, err := s.putSmall(o, nil, "0123456789"); err != nil {
		s.t.Fatal(err)
	}
	if got := s.read(o, 7, 100); got != "789" {
		s.t.Fatalf("read past end = %q", got)
	}
	if got := s.read(o, 4, 0); got != "" {
		s.t.Fatalf("zero length = %q", got)
	}
	if got := s.read(o, 10, 5); got != "" {
		s.t.Fatalf("read at end = %q", got)
	}
	var e *storage.APIError
	if _, err := o.NewRangeReader(context.Background(), 11, 5); !errors.As(err, &e) || e.Code != http.StatusRequestedRangeNotSatisfiable {
		s.t.Fatalf("read past end = %v, want 416", err)
	}
}

func testMetadata(s *suite) {
	o := s.object("meta")
	w := newAppendable(o.If(storage.Conditions{DoesNotExist: true}), true)
	w.Metadata = map[string]string{"seq": "7"}
	w.Write([]byte("x"))
	if err := w.Close(); err != nil {
		s.t.Fatal(err)
	}
	if a := s.attrs(o); a.Metadata["seq"] != "7" {
		s.t.Fatalf("metadata = %v", a.Metadata)
	}
}

func testOpenWriterVisibility(s *suite) {
	o := s.object("log")
	w := newAppendable(o.If(storage.Conditions{DoesNotExist: true}), false)
	w.Flush()
	sizes := func(step string, want int64) {
		s.t.Helper()
		a := s.attrs(o)
		l := s.list(&storage.Query{Prefix: s.prefix + "log"})
		if len(l) != 1 || a.Size != want || l[0].Size != want {
			s.t.Fatalf("%s: attrs.size=%d list=%v, want %d", step, a.Size, l, want)
		}
	}
	s.writeFlush(w, "hello")
	sizes("flushed 5", 0)
	if got := s.read(o, 0, -1); got != "hello" {
		s.t.Fatalf("read while open = %q", got)
	}
	if err := w.Close(); err != nil {
		s.t.Fatal(err)
	}
	sizes("closed", 5)
	tw, off := s.takeover(o, false)
	if off != 5 {
		s.t.Fatalf("takeover offset %d, want 5", off)
	}
	s.writeFlush(tw, "!")
	sizes("takeover flushed", 5)
	tw.Close()
	sizes("takeover closed", 6)
}
