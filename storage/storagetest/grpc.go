package storagetest

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/eeertekin/gcp/storage/internal/wire"
)

const (
	bucketPrefix        = "projects/_/buckets/"
	maxReadChunk        = 2 << 20
	codeInvalidArgument = 3
	codeNotFound        = 5
	codeFailedPrecond   = 9
	codeAborted         = 10
	codeOutOfRange      = 11
	codeUnimplemented   = 12
	codeUnauthenticated = 16
	writeRedirectURL    = "type.googleapis.com/google.storage.v2.BidiWriteObjectRedirectedError"
	readRedirectURL     = "type.googleapis.com/google.storage.v2.BidiReadObjectRedirectedError"
)

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

type GRPCFault struct {
	Method       string
	AtMessage    int
	Code         int
	RoutingToken string
}

func (s *Server) InjectGRPCFaults(faults ...GRPCFault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.grpcFaults = append(s.grpcFaults, faults...)
}

func (s *Server) RoutingTokens() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.routing...)
}

type grpcErr struct {
	code    int
	msg     string
	details []byte
}

func (e *grpcErr) Error() string { return fmt.Sprintf("grpc code %d: %s", e.code, e.msg) }

func grpcError(code int, format string, args ...any) error {
	return &grpcErr{code: code, msg: fmt.Sprintf(format, args...)}
}

func errPrecondition() error {
	return grpcError(codeFailedPrecond, "At least one of the pre-conditions you specified did not hold.")
}

type grpcStream struct {
	w   http.ResponseWriter
	r   *http.Request
	hdr [5]byte
}

func (st *grpcStream) recv() ([]byte, error) {
	if _, err := io.ReadFull(st.r.Body, st.hdr[:]); err != nil {
		return nil, err
	}
	if st.hdr[0] != 0 {
		return nil, grpcError(codeUnimplemented, "compressed messages are not supported")
	}
	msg := make([]byte, binary.BigEndian.Uint32(st.hdr[1:]))
	_, err := io.ReadFull(st.r.Body, msg)
	return msg, err
}

func (st *grpcStream) send(msg wire.Buf) error {
	frame := make([]byte, 5, 5+len(msg))
	binary.BigEndian.PutUint32(frame[1:], uint32(len(msg)))
	if _, err := st.w.Write(append(frame, msg...)); err != nil {
		return err
	}
	return http.NewResponseController(st.w).Flush()
}

func (s *Server) handleGRPC(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor != 2 || !strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc") || r.Header.Get("Te") != "trailers" {
		s.t.Errorf("storagetest: gRPC request %s %s with Content-Type %q, Te %q", r.Proto, r.URL.Path, r.Header.Get("Content-Type"), r.Header.Get("Te"))
		http.Error(w, "not a gRPC request", http.StatusUnsupportedMediaType)
		return
	}
	w.Header().Set("Content-Type", "application/grpc")
	w.WriteHeader(http.StatusOK)
	st := &grpcStream{w: w, r: r}

	err := s.serveGRPC(r.PathValue("method"), st)
	code, msg, details := 0, "", []byte(nil)
	var ge *grpcErr
	switch {
	case errors.As(err, &ge):
		code, msg, details = ge.code, ge.msg, ge.details
	case err != nil:
		code, msg = 13, err.Error()
	}
	w.Header().Set(http.TrailerPrefix+"Grpc-Status", strconv.Itoa(code))
	if msg != "" {
		w.Header().Set(http.TrailerPrefix+"Grpc-Message", url.PathEscape(msg))
	}
	if details != nil {
		w.Header().Set(http.TrailerPrefix+"Grpc-Status-Details-Bin", base64.RawStdEncoding.EncodeToString(details))
	}
}

func (s *Server) serveGRPC(method string, st *grpcStream) error {
	s.mu.Lock()
	s.attempts++
	ok := s.lastToken != "" && st.r.Header.Get("Authorization") == "Bearer "+s.lastToken
	s.quotaProject = st.r.Header.Get("X-Goog-User-Project")
	s.mu.Unlock()
	if !ok {
		return grpcError(codeUnauthenticated, "bad token %q", st.r.Header.Get("Authorization"))
	}
	params, err := url.ParseQuery(st.r.Header.Get("X-Goog-Request-Params"))
	if err != nil {
		return grpcError(codeInvalidArgument, "bad x-goog-request-params: %v", err)
	}
	switch method {
	case "BidiWriteObject":
		return s.bidiWrite(st, params)
	case "BidiReadObject":
		return s.bidiRead(st, params)
	}

	req, err := st.recv()
	if err != nil {
		return grpcError(codeInvalidArgument, "read request: %v", err)
	}
	if err := s.grpcFault(method, 1, nil); err != nil {
		return err
	}
	var bucket string
	wire.Fields(req, func(f wire.Field) error {
		if f.Num == 1 {
			bucket = string(f.Data)
		}
		return nil
	})
	if params.Get("bucket") != bucket {
		return grpcError(codeInvalidArgument, "x-goog-request-params bucket %q does not match request bucket %q", params.Get("bucket"), bucket)
	}
	b, ok := strings.CutPrefix(bucket, bucketPrefix)
	if !ok {
		return grpcError(codeInvalidArgument, "invalid bucket name %q", bucket)
	}

	switch method {
	case "GetObject":
		return s.grpcGet(st, b, req)
	case "DeleteObject":
		return s.grpcDelete(st, b, req)
	case "ListObjects":
		return s.grpcList(st, b, req)
	}
	return grpcError(codeUnimplemented, "method %s is not implemented", method)
}

func (s *Server) grpcFault(method string, n int, redirect func(token string) (string, wire.Buf)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.grpcFaults) == 0 {
		return nil
	}
	f := s.grpcFaults[0]
	at := max(f.AtMessage, 1)
	if f.Method != method || at != n {
		return nil
	}
	s.grpcFaults = s.grpcFaults[1:]
	s.calls = append(s.calls, fmt.Sprintf("GRPC FAULT %s %d", method, n))
	if f.RoutingToken == "" {
		return grpcError(f.Code, "injected fault")
	}
	if redirect == nil {
		return grpcError(codeInvalidArgument, "storagetest: %s cannot be redirected", method)
	}
	typeURL, detail := redirect(f.RoutingToken)
	any := wire.Buf(nil).Str(1, typeURL).Bytes(2, detail)
	return &grpcErr{code: codeAborted, msg: "redirected", details: wire.Buf(nil).Int(1, codeAborted).Str(2, "redirected").Msg(3, any)}
}

type objectRequest struct {
	name       string
	generation int64
	ifGen      *int64
	offset     int64
	limit      int64
}

func parseObjectRequest(req []byte, genField, ifGenField, offsetField, limitField int) (objectRequest, error) {
	var o objectRequest
	err := wire.Fields(req, func(f wire.Field) error {
		switch f.Num {
		case 2:
			o.name = string(f.Data)
		case genField:
			o.generation = int64(f.U)
		case ifGenField:
			v := int64(f.U)
			o.ifGen = &v
		case offsetField:
			o.offset = int64(f.U)
		case limitField:
			o.limit = int64(f.U)
		}
		return nil
	})
	return o, err
}

func (s *Server) lookup(bucket string, o objectRequest) (*Object, error) {
	obj := s.objects[bucket+"/"+o.name]
	if obj == nil || o.generation != 0 && obj.Generation != o.generation {
		return nil, grpcError(codeNotFound, "No such object: %s/%s", bucket, o.name)
	}
	if o.ifGen != nil && *o.ifGen != obj.Generation {
		return nil, errPrecondition()
	}
	return obj, nil
}

func objectProto(bucket, name string, o *Object, live bool) wire.Buf {
	size := o.size()
	if live {
		size = int64(len(o.Data))
	}
	b := wire.Buf(nil).Str(1, name).Str(2, bucketPrefix+bucket).Int(3, o.Generation).Int(4, 1).
		Str(5, "RAPID").Int(6, size).Str(7, o.ContentEncoding).Str(8, o.ContentDisposition).Str(13, o.ContentType).Msg(14, wire.Timestamp(o.Created))
	for k, v := range o.Metadata {
		b = b.Msg(22, wire.Buf(nil).Str(1, k).Str(2, v))
	}
	if !o.Finalized.IsZero() {
		b = b.Msg(36, wire.Timestamp(o.Finalized))
	}
	return b
}

func (s *Server) grpcGet(st *grpcStream, bucket string, req []byte) error {
	o, err := parseObjectRequest(req, 3, 4, -1, -1)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.calls = append(s.calls, "GRPC GET "+bucket+"/"+o.name)
	obj, err := s.lookup(bucket, o)
	var resp wire.Buf
	if err == nil {
		resp = objectProto(bucket, o.name, obj, false)
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return st.send(resp)
}

func (s *Server) grpcDelete(st *grpcStream, bucket string, req []byte) error {
	o, err := parseObjectRequest(req, 4, 5, -1, -1)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.calls = append(s.calls, "GRPC DELETE "+bucket+"/"+o.name)
	if _, err = s.lookup(bucket, o); err == nil {
		delete(s.objects, bucket+"/"+o.name)
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}
	return st.send(nil)
}

func (s *Server) grpcList(st *grpcStream, bucket string, req []byte) error {
	var token, delimiter, prefix string
	err := wire.Fields(req, func(f wire.Field) error {
		switch f.Num {
		case 3:
			token = string(f.Data)
		case 4:
			delimiter = string(f.Data)
		case 6:
			prefix = string(f.Data)
		case 5, 7, 10, 11, 12, 14, 15:
			return grpcError(codeInvalidArgument, "ListObjects field %d is not supported", f.Num)
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.calls = append(s.calls, "GRPC LIST "+bucket+"/"+prefix)
	if !s.buckets[bucket] {
		s.mu.Unlock()
		return grpcError(codeNotFound, "bucket %q does not exist", bucket)
	}
	entries, next := s.listPage(bucket, prefix, delimiter, token)
	var resp wire.Buf
	for _, e := range entries {
		if e.obj != nil {
			resp = resp.Msg(1, objectProto(bucket, e.name, e.obj, false))
		} else {
			resp = resp.Str(2, e.prefix)
		}
	}
	s.mu.Unlock()
	return st.send(resp.Str(3, next))
}

func (s *Server) bidiRead(st *grpcStream, params url.Values) error {
	msg, err := st.recv()
	if err != nil {
		return grpcError(codeInvalidArgument, "read request: %v", err)
	}
	err = s.grpcFault("BidiReadObject", 1, func(token string) (string, wire.Buf) {
		return readRedirectURL, wire.Buf(nil).Str(2, token)
	})
	if err != nil {
		return err
	}
	var spec []byte
	var ranges [][3]int64
	err = wire.Fields(msg, func(f wire.Field) error {
		switch f.Num {
		case 1:
			spec = f.Data
		case 8:
			var rr [3]int64
			wire.Fields(f.Data, func(r wire.Field) error {
				if r.Num >= 1 && r.Num <= 3 {
					rr[r.Num-1] = int64(r.U)
				}
				return nil
			})
			ranges = append(ranges, rr)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if spec == nil {
		return grpcError(codeInvalidArgument, "the first message must carry read_object_spec")
	}
	var bucket, token string
	wire.Fields(spec, func(f wire.Field) error {
		switch f.Num {
		case 1:
			bucket = string(f.Data)
		case 14:
			token = string(f.Data)
		}
		return nil
	})
	if params.Get("bucket") != bucket {
		return grpcError(codeInvalidArgument, "x-goog-request-params bucket %q does not match request bucket %q", params.Get("bucket"), bucket)
	}
	if token != params.Get("routing_token") {
		return grpcError(codeInvalidArgument, "routing_token %q is not in x-goog-request-params", token)
	}
	o, err := parseObjectRequest(spec, 3, 4, -1, -1)
	if err != nil {
		return err
	}
	b := strings.TrimPrefix(bucket, bucketPrefix)
	s.mu.Lock()
	if token != "" {
		s.routing = append(s.routing, token)
	}
	s.calls = append(s.calls, fmt.Sprintf("GRPC READ %s/%s %v", b, o.name, ranges))
	obj, err := s.lookup(b, o)
	var data []byte
	var meta wire.Buf
	if err == nil {
		meta = objectProto(b, o.name, obj, true)
		data = obj.Data
	}
	s.mu.Unlock()
	if err != nil {
		return err
	}

	pending := wire.Buf(nil).Msg(4, meta).Msg(7, wire.Buf(nil).Bytes(1, epochHandle(uint64(obj.Generation))))
	for _, rr := range ranges {
		offset, length, id := rr[0], rr[1], rr[2]
		if offset < 0 || length < 0 {
			return grpcError(codeInvalidArgument, "negative read_offset or read_length")
		}
		if offset > int64(len(data)) {
			return grpcError(codeOutOfRange, "read_offset %d is past the end of the object (%d bytes)", offset, len(data))
		}
		part := data[offset:]
		if length > 0 && length < int64(len(part)) {
			part = part[:length]
		}
		for first := true; first || len(part) > 0; first = false {
			n := min(len(part), maxReadChunk)
			rangeData := wire.Buf(nil).Msg(1, wire.Buf(nil).Bytes(1, part[:n]).Fixed32(2, crc32.Checksum(part[:n], castagnoli))).
				Msg(2, wire.Buf(nil).Int(1, offset).Int(2, int64(n)).Int(3, id)).Bool(3, n == len(part))
			if err := st.send(append(pending, wire.Buf(nil).Msg(6, rangeData)...)); err != nil {
				return err
			}
			pending = nil
			offset += int64(n)
			part = part[n:]
		}
	}
	if pending != nil {
		if err := st.send(pending); err != nil {
			return err
		}
	}
	for {
		if _, err := st.recv(); err != nil {
			return nil
		}
	}
}

type writeRequest struct {
	create    []byte
	takeover  []byte
	uploadID  bool
	offset    int64
	data      []byte
	crc       *uint32
	lookup    bool
	flush     bool
	finish    bool
	hasFields bool
}

func parseWriteRequest(msg []byte) (writeRequest, error) {
	var r writeRequest
	err := wire.Fields(msg, func(f wire.Field) error {
		switch f.Num {
		case 1:
			r.uploadID = true
		case 2:
			r.create = f.Data
		case 11:
			r.takeover = f.Data
		case 3:
			r.offset = int64(f.U)
		case 4:
			return wire.Fields(f.Data, func(d wire.Field) error {
				switch d.Num {
				case 1:
					r.data = d.Data
				case 2:
					v := uint32(d.U)
					r.crc = &v
				}
				return nil
			})
		case 6:
			return grpcError(codeInvalidArgument, "object_checksums are not supported by storagetest")
		case 7:
			r.lookup = f.U != 0
		case 8:
			r.flush = f.U != 0
		case 9:
			r.finish = f.U != 0
		}
		return nil
	})
	return r, err
}

type writer struct {
	s      *Server
	st     *grpcStream
	params url.Values
	bucket string
	name   string
	gen    int64
	epoch  uint64
}

func (s *Server) bidiWrite(st *grpcStream, params url.Values) error {
	w := &writer{s: s, st: st, params: params}
	s.mu.Lock()
	if t := params.Get("routing_token"); t != "" {
		s.routing = append(s.routing, t)
	}
	s.mu.Unlock()
	defer w.closeStream()
	for n := 1; ; n++ {
		msg, err := st.recv()
		if err == io.EOF {
			return w.finish()
		}
		if err != nil {
			return err
		}
		if err := s.grpcFault("BidiWriteObject", n, w.redirectDetail); err != nil {
			return err
		}
		req, err := parseWriteRequest(msg)
		if err != nil {
			return err
		}
		resp, err := w.handle(req, n == 1)
		if err != nil {
			return err
		}
		if resp != nil {
			if err := st.send(resp); err != nil {
				return err
			}
		}
	}
}

func (w *writer) redirectDetail(token string) (string, wire.Buf) {
	detail := wire.Buf(nil).Str(1, token)
	if obj := w.s.objects[w.bucket+"/"+w.name]; w.gen != 0 && obj != nil {
		detail = detail.Msg(2, wire.Buf(nil).Bytes(1, epochHandle(w.epoch))).Int(3, w.gen)
	}
	return writeRedirectURL, detail
}

func epochHandle(e uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, e)
}

func (w *writer) handle(req writeRequest, first bool) (wire.Buf, error) {
	s := w.s
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case first && req.create != nil:
		if err := w.create(req.create); err != nil {
			return nil, err
		}
	case first && req.takeover != nil:
		if err := w.takeover(req.takeover); err != nil {
			return nil, err
		}
	case first && req.uploadID:
		return nil, grpcError(codeInvalidArgument, "Zonal buckets are incompatible with resumable upload.")
	case first:
		return nil, grpcError(codeInvalidArgument, "the first message must carry write_object_spec or append_object_spec")
	case req.create != nil || req.takeover != nil || req.uploadID:
		return nil, grpcError(codeInvalidArgument, "write_object_spec, append_object_spec and upload_id are only allowed in the first message")
	}

	obj, err := w.object()
	if err != nil {
		return nil, err
	}
	if req.data != nil {
		if req.crc != nil && crc32.Checksum(req.data, castagnoli) != *req.crc {
			return nil, grpcError(codeInvalidArgument, "checksummed_data crc32c does not match its content")
		}
		if req.offset != int64(len(obj.Data)) {
			return nil, grpcError(codeInvalidArgument, "write_offset %d does not match the end of the written data at %d", req.offset, len(obj.Data))
		}
		obj.Data = append(obj.Data, req.data...)
	}
	if req.finish {
		obj.Finalized = time.Now().UTC()
		obj.statSize = int64(len(obj.Data))
	}
	switch {
	case first || req.finish:
		resp := wire.Buf(nil).Msg(2, objectProto(w.bucket, w.name, obj, true))
		if obj.Finalized.IsZero() {
			resp = resp.Msg(3, wire.Buf(nil).Bytes(1, epochHandle(w.epoch)))
		}
		return resp, nil
	case req.lookup:
		return wire.Buf(nil).Uint(1, uint64(len(obj.Data))), nil
	}
	return nil, nil
}

func (w *writer) object() (*Object, error) {
	obj := w.s.objects[w.bucket+"/"+w.name]
	if obj == nil || obj.Generation != w.gen {
		return nil, grpcError(codeNotFound, "No such object: %s/%s", w.bucket, w.name)
	}
	if obj.epoch != w.epoch {
		return nil, grpcError(codeFailedPrecond, "A different writer has become the exclusive writer of this object.")
	}
	if !obj.Finalized.IsZero() {
		return nil, grpcError(codeFailedPrecond, "The object has already been finalized.")
	}
	return obj, nil
}

func (w *writer) checkRouting(bucket string) error {
	if w.params.Get("bucket") != bucketPrefix+bucket {
		return grpcError(codeInvalidArgument, "x-goog-request-params bucket %q does not match %q", w.params.Get("bucket"), bucketPrefix+bucket)
	}
	return nil
}

func (w *writer) create(spec []byte) error {
	var res []byte
	var ifGen *int64
	appendable := false
	err := wire.Fields(spec, func(f wire.Field) error {
		switch f.Num {
		case 1:
			res = f.Data
		case 3:
			v := int64(f.U)
			ifGen = &v
		case 4, 5, 6, 7, 8:
			return grpcError(codeInvalidArgument, "WriteObjectSpec field %d is not supported by storagetest", f.Num)
		case 9:
			appendable = f.U != 0
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !appendable {
		return grpcError(codeInvalidArgument, "only appendable writes are supported")
	}
	var bucket, contentType, contentEncoding, contentDisposition string
	var metadata map[string]string
	err = wire.Fields(res, func(f wire.Field) error {
		switch f.Num {
		case 1:
			w.name = string(f.Data)
		case 2:
			bucket = string(f.Data)
		case 7:
			contentEncoding = string(f.Data)
		case 8:
			contentDisposition = string(f.Data)
		case 13:
			contentType = string(f.Data)
		case 22:
			var k, v string
			wire.Fields(f.Data, func(e wire.Field) error {
				if e.Num == 1 {
					k = string(e.Data)
				} else if e.Num == 2 {
					v = string(e.Data)
				}
				return nil
			})
			if metadata == nil {
				metadata = map[string]string{}
			}
			metadata[k] = v
		}
		return nil
	})
	if err != nil {
		return err
	}
	if err := w.checkRouting(strings.TrimPrefix(bucket, bucketPrefix)); err != nil {
		return err
	}
	w.bucket = strings.TrimPrefix(bucket, bucketPrefix)
	if !w.s.buckets[w.bucket] || w.name == "" {
		return grpcError(codeNotFound, "bucket %q does not exist", w.bucket)
	}
	w.s.calls = append(w.s.calls, "GRPC CREATE "+w.bucket+"/"+w.name)
	old := w.s.objects[w.bucket+"/"+w.name]
	if ifGen != nil && (old == nil && *ifGen != 0 || old != nil && old.Generation != *ifGen) {
		return errPrecondition()
	}
	w.s.generation++
	w.s.epoch++
	w.gen, w.epoch = w.s.generation, w.s.epoch
	w.s.objects[w.bucket+"/"+w.name] = &Object{ContentType: contentType, ContentEncoding: contentEncoding, ContentDisposition: contentDisposition, Metadata: metadata, Generation: w.gen, Created: time.Now().UTC(), epoch: w.epoch, Data: []byte{}}
	return nil
}

func (w *writer) takeover(spec []byte) error {
	var bucket string
	var handle []byte
	err := wire.Fields(spec, func(f wire.Field) error {
		switch f.Num {
		case 1:
			bucket = string(f.Data)
		case 2:
			w.name = string(f.Data)
		case 3:
			w.gen = int64(f.U)
		case 6:
			if t := string(f.Data); w.params.Get("routing_token") != t {
				return grpcError(codeInvalidArgument, "routing_token %q is not in x-goog-request-params", t)
			}
		case 7:
			wire.Fields(f.Data, func(h wire.Field) error {
				if h.Num == 1 {
					handle = bytes.Clone(h.Data)
				}
				return nil
			})
		}
		return nil
	})
	if err != nil {
		return err
	}
	w.bucket = strings.TrimPrefix(bucket, bucketPrefix)
	if err := w.checkRouting(w.bucket); err != nil {
		return err
	}
	obj := w.s.objects[w.bucket+"/"+w.name]
	resumed := obj != nil && bytes.Equal(handle, epochHandle(obj.epoch))
	w.s.calls = append(w.s.calls, fmt.Sprintf("GRPC TAKEOVER %s/%s resumed=%v", w.bucket, w.name, resumed))
	if obj == nil || w.gen != 0 && obj.Generation != w.gen {
		return grpcError(codeNotFound, "No such object: %s/%s", w.bucket, w.name)
	}
	if !obj.Finalized.IsZero() {
		return grpcError(codeFailedPrecond, "The object has already been finalized.")
	}
	w.gen = obj.Generation
	if !resumed {
		w.s.epoch++
		obj.epoch = w.s.epoch
	}
	w.epoch = obj.epoch
	return nil
}

func (w *writer) finish() error {
	w.s.mu.Lock()
	obj, err := w.object()
	var resp wire.Buf
	if err == nil {
		resp = wire.Buf(nil).Uint(1, uint64(len(obj.Data)))
	}
	w.s.mu.Unlock()
	if err != nil {
		return nil
	}
	return w.st.send(resp)
}

func (w *writer) closeStream() {
	w.s.mu.Lock()
	defer w.s.mu.Unlock()
	if obj := w.s.objects[w.bucket+"/"+w.name]; obj != nil && obj.Generation == w.gen && obj.epoch == w.epoch {
		obj.statSize = int64(len(obj.Data))
	}
}
