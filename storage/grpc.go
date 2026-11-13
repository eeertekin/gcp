package storage

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/eeertekin/gcp/storage/internal/wire"
)

const (
	grpcService     = "/google.storage.v2.Storage/"
	maxGRPCMessage  = 64 << 20
	redirectTypeURL = "type.googleapis.com/google.storage.v2.BidiWriteObjectRedirectedError"
)

var grpcCodes = [...]struct {
	name string
	http int
}{
	{"OK", http.StatusOK},
	{"Canceled", 499},
	{"Unknown", http.StatusInternalServerError},
	{"InvalidArgument", http.StatusBadRequest},
	{"DeadlineExceeded", http.StatusGatewayTimeout},
	{"NotFound", http.StatusNotFound},
	{"AlreadyExists", http.StatusConflict},
	{"PermissionDenied", http.StatusForbidden},
	{"ResourceExhausted", http.StatusTooManyRequests},
	{"FailedPrecondition", http.StatusPreconditionFailed},
	{"Aborted", http.StatusConflict},
	{"OutOfRange", http.StatusRequestedRangeNotSatisfiable},
	{"Unimplemented", http.StatusNotImplemented},
	{"Internal", http.StatusInternalServerError},
	{"Unavailable", http.StatusServiceUnavailable},
	{"DataLoss", http.StatusInternalServerError},
	{"Unauthenticated", http.StatusUnauthorized},
}

type writeRedirect struct {
	routingToken string
	handle       []byte
	generation   int64
	err          error
}

func (e *writeRedirect) Error() string { return e.err.Error() }
func (e *writeRedirect) Unwrap() error { return e.err }

func grpcError(code int, msg string, details []byte) error {
	e := &APIError{Code: http.StatusInternalServerError, Reason: "Code(" + strconv.Itoa(code) + ")", Message: msg}
	if code >= 0 && code < len(grpcCodes) {
		e.Code, e.Reason = grpcCodes[code].http, grpcCodes[code].name
	}
	var redirect *writeRedirect
	wire.Fields(details, func(f wire.Field) error {
		if f.Num != 3 || f.Wire != wire.Bytes {
			return nil
		}
		var typeURL string
		var value []byte
		wire.Fields(f.Data, func(a wire.Field) error {
			switch a.Num {
			case 1:
				typeURL = string(a.Data)
			case 2:
				value = a.Data
			}
			return nil
		})
		if typeURL != redirectTypeURL {
			return nil
		}
		redirect = &writeRedirect{err: e}
		return wire.Fields(value, func(r wire.Field) error {
			switch r.Num {
			case 1:
				redirect.routingToken = string(r.Data)
			case 2:
				redirect.handle = parseHandle(r.Data)
			case 3:
				redirect.generation = int64(r.U)
			}
			return nil
		})
	})
	if redirect != nil {
		return redirect
	}
	return e
}

func parseHandle(b []byte) []byte {
	var h []byte
	wire.Fields(b, func(f wire.Field) error {
		if f.Num == 1 {
			h = bytes.Clone(f.Data)
		}
		return nil
	})
	return h
}

func statusFromHeader(h http.Header) (error, bool) {
	s := h.Get("Grpc-Status")
	if s == "" {
		return nil, false
	}
	code, err := strconv.Atoi(s)
	if err != nil {
		return fmt.Errorf("storage: invalid grpc-status %q", s), true
	}
	if code == 0 {
		return nil, true
	}
	msg, _ := url.PathUnescape(h.Get("Grpc-Message"))
	details, _ := base64.RawStdEncoding.DecodeString(strings.TrimRight(h.Get("Grpc-Status-Details-Bin"), "="))
	return grpcError(code, msg, details), true
}

type grpcClient struct {
	c    *Client
	base string
	http *http.Client
}

func newGRPCHTTPClient(plaintext bool) *http.Client {
	tr := &http.Transport{
		Proxy:               http.ProxyFromEnvironment,
		TLSHandshakeTimeout: 10 * time.Second,
		IdleConnTimeout:     90 * time.Second,
		Protocols:           new(http.Protocols),
		HTTP2:               &http.HTTP2Config{SendPingTimeout: 30 * time.Second, PingTimeout: 15 * time.Second},
	}
	if plaintext {
		tr.Protocols.SetUnencryptedHTTP2(true)
	} else {
		tr.Protocols.SetHTTP2(true)
	}
	return &http.Client{Transport: tr}
}

func bucketParams(bucket string, routingToken string) string {
	p := "bucket=" + url.QueryEscape("projects/_/buckets/"+bucket)
	if routingToken != "" {
		p += "&routing_token=" + url.QueryEscape(routingToken)
	}
	return p
}

type grpcStream struct {
	cancel context.CancelFunc
	pw     *io.PipeWriter
	respc  chan struct{}
	resp   *http.Response
	err    error
	hdr    [5]byte
}

func (g *grpcClient) open(ctx context.Context, method, params string) (*grpcStream, error) {
	ctx, cancel := context.WithCancel(ctx)
	pr, pw := io.Pipe()
	header := http.Header{
		"Content-Type":          {"application/grpc"},
		"Te":                    {"trailers"},
		"X-Goog-Request-Params": {params},
	}
	req, err := g.c.newRequest(ctx, http.MethodPost, g.base+grpcService+method, header, pr)
	if err != nil {
		cancel()
		return nil, err
	}
	s := &grpcStream{cancel: cancel, pw: pw, respc: make(chan struct{})}
	go func() {
		defer close(s.respc)
		resp, err := g.http.Do(req)
		if err != nil {
			pr.CloseWithError(err)
			s.err = &transportError{err: err}
			return
		}
		s.resp = resp
	}()
	return s, nil
}

func (s *grpcStream) send(msg wire.Buf) error {
	frame := make([]byte, 5, 5+len(msg))
	binary.BigEndian.PutUint32(frame[1:], uint32(len(msg)))
	if _, err := s.pw.Write(append(frame, msg...)); err != nil {
		return &transportError{err: err}
	}
	return nil
}

func (s *grpcStream) status() error {
	for {
		if _, err := s.recv(); err != nil {
			if err == io.EOF {
				return &transportError{err: errors.New("storage: gRPC stream closed by the server")}
			}
			return err
		}
	}
}

func (s *grpcStream) closeSend() error {
	return s.pw.Close()
}

func (s *grpcStream) wait() error {
	<-s.respc
	if s.err != nil {
		return s.err
	}
	if s.resp.StatusCode != http.StatusOK {
		data, _ := io.ReadAll(io.LimitReader(s.resp.Body, 4096))
		s.err = &APIError{Code: s.resp.StatusCode, Message: strings.TrimSpace(string(data))}
		if s.resp.StatusCode >= 500 || s.resp.StatusCode == http.StatusTooManyRequests {
			s.err = &transportError{err: s.err}
		}
		return s.err
	}
	if err, ok := statusFromHeader(s.resp.Header); ok {
		if err == nil {
			err = io.EOF
		}
		s.err = err
		return err
	}
	if ct := s.resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/grpc") {
		s.err = fmt.Errorf("storage: unexpected gRPC content type %q", ct)
		return s.err
	}
	return nil
}

func (s *grpcStream) recv() ([]byte, error) {
	if err := s.wait(); err != nil {
		return nil, err
	}
	if _, err := io.ReadFull(s.resp.Body, s.hdr[:]); err != nil {
		if err == io.EOF {
			if serr, ok := statusFromHeader(s.resp.Trailer); ok {
				if serr == nil {
					serr = io.EOF
				}
				s.err = serr
				return nil, serr
			}
			s.err = &transportError{err: errors.New("storage: gRPC stream ended without status")}
			return nil, s.err
		}
		s.err = &transportError{err: err}
		return nil, s.err
	}
	if s.hdr[0] != 0 {
		s.err = errors.New("storage: compressed gRPC messages are not supported")
		return nil, s.err
	}
	n := binary.BigEndian.Uint32(s.hdr[1:])
	if n > maxGRPCMessage {
		s.err = fmt.Errorf("storage: gRPC message of %d bytes is too large", n)
		return nil, s.err
	}
	msg := make([]byte, n)
	if _, err := io.ReadFull(s.resp.Body, msg); err != nil {
		s.err = &transportError{err: err}
		return nil, s.err
	}
	return msg, nil
}

func (s *grpcStream) close() {
	s.pw.CloseWithError(context.Canceled)
	s.cancel()
	<-s.respc
	if s.resp != nil {
		s.resp.Body.Close()
	}
}

func (g *grpcClient) unaryOnce(ctx context.Context, method, params string, req wire.Buf) ([]byte, error) {
	s, err := g.open(ctx, method, params)
	if err != nil {
		return nil, err
	}
	defer s.close()
	if err := s.send(req); err != nil {
		return nil, s.status()
	}
	s.closeSend()
	msg, err := s.recv()
	if err == io.EOF {
		return nil, &transportError{err: errors.New("storage: gRPC call returned no message")}
	}
	if err != nil {
		return nil, err
	}
	if _, err := s.recv(); err != io.EOF {
		if err == nil {
			err = errors.New("storage: gRPC unary call returned more than one message")
		}
		return nil, err
	}
	return msg, nil
}

func (g *grpcClient) unary(ctx context.Context, method, params string, req wire.Buf) ([]byte, error) {
	retry := g.c.Retry
	delay := retry.Initial
	for attempt := 1; ; attempt++ {
		msg, err := g.unaryOnce(ctx, method, params, req)
		if err == nil {
			return msg, nil
		}
		if ok, _ := retryable(err); !ok || attempt >= retry.MaxAttempts || ctx.Err() != nil {
			return nil, err
		}
		if err := sleep(ctx, jitter(delay)); err != nil {
			return nil, err
		}
		delay = retry.next(delay)
	}
}
