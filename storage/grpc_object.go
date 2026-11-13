package storage

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strings"

	"github.com/eeertekin/gcp/option"
	"github.com/eeertekin/gcp/storage/internal/wire"
)

const bucketResourcePrefix = "projects/_/buckets/"

var castagnoli = crc32.MakeTable(crc32.Castagnoli)

func NewGRPCClient(ctx context.Context, opts ...option.ClientOption) (*Client, error) {
	c, err := NewClient(ctx, opts...)
	if err != nil {
		return nil, err
	}
	var s option.Settings
	for _, o := range opts {
		o(&s)
	}
	hc := s.HTTPClient
	if hc == nil {
		hc = newGRPCHTTPClient(strings.HasPrefix(c.endpoint, "http://"))
	}
	c.grpc = &grpcClient{c: c, base: c.endpoint, http: hc}
	return c, nil
}

func parseObject(b []byte) (*ObjectAttrs, error) {
	a := &ObjectAttrs{}
	err := wire.Fields(b, func(f wire.Field) error {
		var err error
		switch f.Num {
		case 1:
			a.Name = string(f.Data)
		case 2:
			a.Bucket = strings.TrimPrefix(string(f.Data), bucketResourcePrefix)
		case 3:
			a.Generation = int64(f.U)
		case 6:
			a.Size = int64(f.U)
		case 13:
			a.ContentType = string(f.Data)
		case 14:
			a.Created, err = wire.ParseTimestamp(f.Data)
		case 36:
			a.Finalized, err = wire.ParseTimestamp(f.Data)
		case 22:
			var k, v string
			err = wire.Fields(f.Data, func(e wire.Field) error {
				switch e.Num {
				case 1:
					k = string(e.Data)
				case 2:
					v = string(e.Data)
				}
				return nil
			})
			if a.Metadata == nil {
				a.Metadata = map[string]string{}
			}
			a.Metadata[k] = v
		}
		return err
	})
	return a, err
}

func (o *ObjectHandle) conditionFields(b wire.Buf, field int) wire.Buf {
	if o.conds == nil {
		return b
	}
	switch {
	case o.conds.DoesNotExist:
		return b.OptInt(field, 0)
	case o.conds.GenerationMatch != 0:
		return b.OptInt(field, o.conds.GenerationMatch)
	}
	return b
}

func notFound(err error) error {
	if isNotFound(err) {
		return ErrObjectNotExist
	}
	return err
}

func (g *grpcClient) getObject(ctx context.Context, o *ObjectHandle) (*ObjectAttrs, error) {
	req := wire.Buf(nil).Str(1, bucketResourcePrefix+o.bucket).Str(2, o.name).Int(3, o.gen)
	req = o.conditionFields(req, 4)
	msg, err := g.unary(ctx, "GetObject", bucketParams(o.bucket, ""), req)
	if err != nil {
		return nil, notFound(err)
	}
	return parseObject(msg)
}

func (g *grpcClient) deleteObject(ctx context.Context, o *ObjectHandle) error {
	req := wire.Buf(nil).Str(1, bucketResourcePrefix+o.bucket).Str(2, o.name).Int(4, o.gen)
	req = o.conditionFields(req, 5)
	_, err := g.unary(ctx, "DeleteObject", bucketParams(o.bucket, ""), req)
	return notFound(err)
}

func (g *grpcClient) listObjects(ctx context.Context, bucket string, q Query, token string) (*objectPage, error) {
	req := wire.Buf(nil).Str(1, bucketResourcePrefix+bucket).Str(3, token).Str(4, q.Delimiter).Str(6, q.Prefix)
	msg, err := g.unary(ctx, "ListObjects", bucketParams(bucket, ""), req)
	if err != nil {
		return nil, err
	}
	page := &objectPage{}
	var prefixes []string
	err = wire.Fields(msg, func(f wire.Field) error {
		switch f.Num {
		case 1:
			a, err := parseObject(f.Data)
			if err != nil {
				return err
			}
			page.items = append(page.items, a)
		case 2:
			prefixes = append(prefixes, string(f.Data))
		case 3:
			page.next = string(f.Data)
		}
		return nil
	})
	for _, p := range prefixes {
		page.items = append(page.items, &ObjectAttrs{Prefix: p})
	}
	return page, err
}

type grpcReader struct {
	s   *grpcStream
	buf []byte
}

func (g *grpcClient) newRangeReader(ctx context.Context, o *ObjectHandle, offset, length int64) (*Reader, error) {
	req := wire.Buf(nil).Str(1, bucketResourcePrefix+o.bucket).Str(2, o.name).Int(3, o.gen).Int(4, offset)
	if length > 0 {
		req = req.Int(5, length)
	}
	req = o.conditionFields(req, 6)

	retry := g.c.Retry
	delay := retry.Initial
	for attempt := 1; ; attempt++ {
		r, err := g.openReader(ctx, o, req)
		if err == nil {
			return r, nil
		}
		if ok, _ := retryable(err); !ok || attempt >= retry.MaxAttempts || ctx.Err() != nil {
			return nil, notFound(err)
		}
		if err := sleep(ctx, jitter(delay)); err != nil {
			return nil, err
		}
		delay = retry.next(delay)
	}
}

func (g *grpcClient) openReader(ctx context.Context, o *ObjectHandle, req wire.Buf) (*Reader, error) {
	ctx, cancel := context.WithCancel(ctx)
	s, err := g.open(ctx, "ReadObject", bucketParams(o.bucket, ""))
	if err != nil {
		cancel()
		return nil, err
	}
	fail := func(err error) (*Reader, error) {
		s.close()
		cancel()
		return nil, err
	}
	if err := s.send(req); err != nil {
		return fail(s.status())
	}
	s.closeSend()
	r := &grpcReader{s: s}
	msg, err := s.recv()
	if err == io.EOF {
		return &Reader{body: r, cancel: cancel}, nil
	}
	if err != nil {
		return fail(err)
	}
	if err := r.take(msg); err != nil {
		return fail(err)
	}
	return &Reader{body: r, cancel: cancel}, nil
}

func (r *grpcReader) take(msg []byte) error {
	return wire.Fields(msg, func(f wire.Field) error {
		if f.Num != 1 {
			return nil
		}
		data, err := parseChecksummedData(f.Data)
		r.buf = append(r.buf, data...)
		return err
	})
}

func parseChecksummedData(b []byte) ([]byte, error) {
	var content []byte
	var crc *uint32
	err := wire.Fields(b, func(f wire.Field) error {
		switch f.Num {
		case 1:
			content = f.Data
		case 2:
			v := uint32(f.U)
			crc = &v
		}
		return nil
	})
	if err == nil && crc != nil && crc32.Checksum(content, castagnoli) != *crc {
		err = errors.New("storage: crc32c mismatch in gRPC object data")
	}
	return content, err
}

func (r *grpcReader) Read(p []byte) (int, error) {
	for len(r.buf) == 0 {
		msg, err := r.s.recv()
		if err == io.EOF {
			return 0, io.EOF
		}
		if err != nil {
			return 0, fmt.Errorf("storage: read object: %w", err)
		}
		if err := r.take(msg); err != nil {
			return 0, err
		}
	}
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

func (r *grpcReader) Close() error {
	r.s.close()
	return nil
}

var errAppendOnly = errors.New("storage: the gRPC client only supports appendable writes (set Writer.Append)")
