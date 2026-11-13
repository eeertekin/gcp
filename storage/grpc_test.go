package storage_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/eeertekin/gcp/storage"
	"github.com/eeertekin/gcp/storage/storagetest"
)

const (
	codeInvalidArgument = 3
	codeUnavailable     = 14
)

func grpcSetup(t *testing.T) (*storagetest.Server, *storage.Client, *storage.BucketHandle) {
	s, _ := storagetest.NewServer(t)
	c := s.GRPCClient()
	return s, c, c.Bucket(storagetest.Bucket)
}

func appendWriter(o *storage.ObjectHandle) *storage.Writer {
	w := o.NewWriter(context.Background())
	w.Append = true
	return w
}

func mustFlush(t *testing.T, w *storage.Writer, p string) int64 {
	t.Helper()
	if _, err := w.Write([]byte(p)); err != nil {
		t.Fatalf("write: %v", err)
	}
	n, err := w.Flush()
	if err != nil {
		t.Fatalf("flush: %v", err)
	}
	return n
}

func TestGRPCRedirectOnCreate(t *testing.T) {
	s, _, b := grpcSetup(t)
	s.InjectGRPCFaults(storagetest.GRPCFault{Method: "BidiWriteObject", RoutingToken: "zone-a"})
	w := appendWriter(b.Object("log").If(storage.Conditions{DoesNotExist: true}))
	w.FinalizeOnClose = true
	if n := mustFlush(t, w, "abc"); n != 3 {
		t.Fatalf("flush = %d", n)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if got := s.RoutingTokens(); !slices.Equal(got, []string{"zone-a"}) {
		t.Fatalf("routing tokens = %q", got)
	}
	if o, _ := s.Object(storagetest.Bucket, "log"); string(o.Data) != "abc" || o.Finalized.IsZero() {
		t.Fatalf("object = %q finalized=%v", o.Data, o.Finalized)
	}
}

func TestGRPCRedirectMidStreamResendsUnpersisted(t *testing.T) {
	s, _, b := grpcSetup(t)
	w := appendWriter(b.Object("log").If(storage.Conditions{DoesNotExist: true}))
	mustFlush(t, w, "one,")
	s.InjectGRPCFaults(storagetest.GRPCFault{Method: "BidiWriteObject", AtMessage: 3, RoutingToken: "zone-b"})
	if n := mustFlush(t, w, "two,"); n != 8 {
		t.Fatalf("flush through redirect = %d, want 8", n)
	}
	if n := mustFlush(t, w, "three"); n != 13 {
		t.Fatalf("flush after redirect = %d, want 13", n)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if o, _ := s.Object(storagetest.Bucket, "log"); string(o.Data) != "one,two,three" {
		t.Fatalf("object = %q", o.Data)
	}
	calls := strings.Join(s.Calls(), "\n")
	if !strings.Contains(calls, "GRPC TAKEOVER "+storagetest.Bucket+"/log resumed=true") {
		t.Fatalf("no takeover after redirect:\n%s", calls)
	}
	if got := s.RoutingTokens(); !slices.Equal(got, []string{"zone-b"}) {
		t.Fatalf("routing tokens = %q", got)
	}
}

func TestGRPCTakeoverFencesOtherWriter(t *testing.T) {
	s, _, b := grpcSetup(t)
	o := b.Object("log")
	w := appendWriter(o.If(storage.Conditions{DoesNotExist: true}))
	mustFlush(t, w, "a")
	a, _ := o.Attrs(context.Background())
	tw, off, err := o.Generation(a.Generation).NewWriterFromAppendableObject(context.Background(), nil)
	if err != nil || off != 1 {
		t.Fatalf("takeover = %d, %v", off, err)
	}
	if !strings.Contains(strings.Join(s.Calls(), "\n"), "resumed=false") {
		t.Fatalf("takeover without handle resumed: %q", s.Calls())
	}
	mustFlush(t, tw, "b")
	_, err = w.Write([]byte("late"))
	if err == nil {
		_, err = w.Flush()
	}
	if !isPrecondition(err) {
		t.Fatalf("fenced writer = %v, want 412", err)
	}
	w.Close()
	tw.Close()
}

func TestGRPCUnaryRetry(t *testing.T) {
	s, _, b := grpcSetup(t)
	s.AddObject(storagetest.Bucket, "obj", []byte("x"))
	s.InjectGRPCFaults(storagetest.GRPCFault{Method: "GetObject", Code: codeUnavailable})
	a, err := b.Object("obj").Attrs(context.Background())
	if err != nil || a.Size != 1 {
		t.Fatalf("attrs = %+v, %v", a, err)
	}
	s.InjectGRPCFaults(storagetest.GRPCFault{Method: "GetObject", Code: codeInvalidArgument})
	_, err = b.Object("obj").Attrs(context.Background())
	var e *storage.APIError
	if !errors.As(err, &e) || e.Code != http.StatusBadRequest || e.Reason != "InvalidArgument" {
		t.Fatalf("attrs = %v, want InvalidArgument", err)
	}
}

func TestGRPCReadRetry(t *testing.T) {
	s, _, b := grpcSetup(t)
	s.AddObject(storagetest.Bucket, "obj", []byte("0123456789"))
	s.InjectGRPCFaults(storagetest.GRPCFault{Method: "ReadObject", Code: codeUnavailable})
	r, err := b.Object("obj").NewRangeReader(context.Background(), 2, 3)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || string(got) != "234" {
		t.Fatalf("read = %q, %v", got, err)
	}
}

func TestGRPCListPaging(t *testing.T) {
	s, _, b := grpcSetup(t)
	for _, n := range []string{"p/a", "p/b", "p/c", "p/d", "p/e", "q"} {
		s.AddObject(storagetest.Bucket, n, []byte("x"))
	}
	s.SetPageSize(2)
	it := b.Objects(context.Background(), &storage.Query{Prefix: "p/"})
	var got []string
	for {
		a, err := it.Next()
		if err != nil {
			break
		}
		got = append(got, a.Name)
	}
	if want := []string{"p/a", "p/b", "p/c", "p/d", "p/e"}; !slices.Equal(got, want) {
		t.Fatalf("list = %q, want %q", got, want)
	}
}

func TestGRPCWriterNeedsAppend(t *testing.T) {
	_, _, b := grpcSetup(t)
	w := b.Object("x").NewWriter(context.Background())
	if _, err := w.Flush(); err == nil || !strings.Contains(err.Error(), "appendable") {
		t.Fatalf("flush = %v", err)
	}
}

func TestGRPCComposeUnsupported(t *testing.T) {
	_, _, b := grpcSetup(t)
	if _, err := b.Object("d").ComposerFrom(b.Object("a")).Run(context.Background()); err == nil {
		t.Fatal("compose over gRPC succeeded")
	}
}

func TestJSONFlushNeedsGRPC(t *testing.T) {
	_, c := storagetest.NewServer(t)
	w := c.Bucket(storagetest.Bucket).Object("x").NewWriter(context.Background())
	if _, err := w.Flush(); err == nil {
		t.Fatal("flush on JSON writer succeeded")
	}
}
