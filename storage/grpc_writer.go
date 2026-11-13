package storage

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync"

	"github.com/eeertekin/gcp/storage/internal/wire"
)

const maxWriteMessage = 2 << 20

type AppendableWriterOpts struct {
	ChunkSize       int
	FinalizeOnClose bool
}

func (o *ObjectHandle) NewWriterFromAppendableObject(ctx context.Context, opts *AppendableWriterOpts) (*Writer, int64, error) {
	if err := o.validate(); err != nil {
		return nil, 0, err
	}
	if o.c.grpc == nil {
		return nil, 0, errors.New("storage: appendable objects need a gRPC client (NewGRPCClient)")
	}
	if o.gen == 0 {
		return nil, 0, errors.New("storage: NewWriterFromAppendableObject needs ObjectHandle.Generation")
	}
	w := o.NewWriter(ctx)
	w.Append = true
	if opts != nil {
		w.FinalizeOnClose = opts.FinalizeOnClose
		if opts.ChunkSize > 0 {
			w.ChunkSize = opts.ChunkSize
		}
	}
	w.aw = &appendWriter{w: w, g: o.c.grpc, gen: o.gen}
	if err := w.aw.reconnect(); err != nil {
		return nil, 0, notFound(err)
	}
	return w, w.aw.sent, nil
}

type appendWriter struct {
	w            *Writer
	g            *grpcClient
	gen          int64
	handle       []byte
	routingToken string

	c       *writeConn
	sent    int64
	base    int64
	pending []byte
}

type writeConn struct {
	s *grpcStream

	mu        sync.Mutex
	changed   chan struct{}
	responses int
	persisted int64
	resource  *ObjectAttrs
	handle    []byte
	ended     bool
	err       error
}

func (c *writeConn) receive() {
	for {
		msg, err := c.s.recv()
		if err != nil {
			c.update(func() {
				c.ended = true
				if err != io.EOF {
					c.err = err
				}
			})
			return
		}
		var persisted int64 = -1
		var resource *ObjectAttrs
		var handle []byte
		perr := wire.Fields(msg, func(f wire.Field) error {
			var err error
			switch f.Num {
			case 1:
				persisted = int64(f.U)
			case 2:
				resource, err = parseObject(f.Data)
			case 3:
				handle = parseHandle(f.Data)
			}
			return err
		})
		c.update(func() {
			c.responses++
			if perr != nil && c.err == nil {
				c.err = perr
			}
			if resource != nil {
				c.resource = resource
				persisted = resource.Size
			}
			if persisted >= 0 {
				c.persisted = persisted
			}
			if handle != nil {
				c.handle = handle
			}
		})
	}
}

func (c *writeConn) update(fn func()) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fn()
	close(c.changed)
	c.changed = make(chan struct{})
}

func (c *writeConn) wait(ctx context.Context, done func() bool) error {
	for {
		c.mu.Lock()
		ok, ended, err, ch := done(), c.ended, c.err, c.changed
		c.mu.Unlock()
		switch {
		case err != nil:
			return err
		case ok:
			return nil
		case ended:
			return &transportError{err: errors.New("storage: gRPC write stream closed by the server")}
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (c *writeConn) streamError(ctx context.Context) error {
	if err := c.wait(ctx, func() bool { return c.ended }); err != nil {
		return err
	}
	return &transportError{err: errors.New("storage: gRPC write stream closed by the server")}
}

func (a *appendWriter) firstMessage() wire.Buf {
	o := a.w.o
	if a.gen == 0 {
		res := wire.Buf(nil).Str(1, o.name).Str(2, bucketResourcePrefix+o.bucket).Str(7, a.w.ContentEncoding).Str(13, a.w.ContentType)
		for k, v := range a.w.Metadata {
			res = res.Msg(22, wire.Buf(nil).Str(1, k).Str(2, v))
		}
		spec := wire.Buf(nil).Msg(1, res)
		spec = o.conditionFields(spec, 3)
		spec = spec.Bool(9, true)
		return wire.Buf(nil).Msg(2, spec)
	}
	spec := wire.Buf(nil).Str(1, bucketResourcePrefix+o.bucket).Str(2, o.name).Int(3, a.gen)
	if a.routingToken != "" {
		spec = spec.Str(6, a.routingToken)
	}
	if a.handle != nil {
		spec = spec.Msg(7, wire.Buf(nil).Bytes(1, a.handle))
	}
	return wire.Buf(nil).Msg(11, spec)
}

func (a *appendWriter) connectOnce() error {
	ctx := a.w.ctx
	s, err := a.g.open(ctx, "BidiWriteObject", bucketParams(a.w.o.bucket, a.routingToken))
	if err != nil {
		return err
	}
	c := &writeConn{s: s, changed: make(chan struct{})}
	go c.receive()
	if a.c != nil {
		a.c.s.close()
	}
	a.c = c

	if err := s.send(a.firstMessage().Bool(7, true).Bool(8, true)); err != nil {
		return c.streamError(ctx)
	}
	if err := c.wait(ctx, func() bool { return c.responses > 0 }); err != nil {
		return err
	}
	c.mu.Lock()
	persisted, resource, handle := c.persisted, c.resource, c.handle
	c.mu.Unlock()
	if resource != nil {
		a.gen = resource.Generation
	}
	if handle != nil {
		a.handle = handle
	}
	if persisted < a.base || persisted > a.sent && a.sent != 0 {
		return fmt.Errorf("storage: server persisted %d bytes of the appendable object, writer has sent %d", persisted, a.sent)
	}
	a.trim(persisted)
	resend := a.pending
	a.sent, a.base, a.pending = persisted, persisted, nil
	return a.sendData(resend, nil)
}

func (a *appendWriter) reconnect() error {
	for attempt := 1; ; attempt++ {
		err := a.connectOnce()
		var rd *redirectError
		if !errors.As(err, &rd) || attempt >= a.g.c.Retry.MaxAttempts {
			return err
		}
		a.routingToken = rd.routingToken
		if rd.handle != nil {
			a.handle = rd.handle
		}
		if rd.generation != 0 {
			a.gen = rd.generation
		}
	}
}

func (a *appendWriter) do(op func() error) error {
	if a.c == nil {
		if err := a.reconnect(); err != nil {
			return err
		}
	}
	for attempt := 1; ; attempt++ {
		err := op()
		var rd *redirectError
		if !errors.As(err, &rd) || attempt >= a.g.c.Retry.MaxAttempts {
			return err
		}
		a.routingToken = rd.routingToken
		if rd.handle != nil {
			a.handle = rd.handle
		}
		if rd.generation != 0 {
			a.gen = rd.generation
		}
		if err := a.reconnect(); err != nil {
			return err
		}
	}
}

func (a *appendWriter) trim(persisted int64) {
	if n := persisted - a.base; n > 0 && n <= int64(len(a.pending)) {
		a.pending = a.pending[n:]
		a.base = persisted
	}
}

func (a *appendWriter) sendData(data []byte, flags wire.Buf) error {
	for {
		n := min(len(data), maxWriteMessage)
		msg := wire.Buf(nil).Int(3, a.sent)
		if n > 0 {
			chunk := data[:n]
			msg = msg.Msg(4, wire.Buf(nil).Bytes(1, chunk).Fixed32(2, crc32.Checksum(chunk, castagnoli)))
		}
		last := n == len(data)
		if last {
			if flags == nil && n == 0 {
				return nil
			}
			msg = append(msg, flags...)
		}
		if err := a.c.s.send(msg); err != nil {
			return a.c.streamError(a.w.ctx)
		}
		a.pending = append(a.pending, data[:n]...)
		a.sent += int64(n)
		data = data[n:]
		if last {
			return nil
		}
	}
}

func (a *appendWriter) write(data []byte) error {
	return a.do(func() error { return a.sendData(data, nil) })
}

func (a *appendWriter) flush(data []byte) (int64, error) {
	var persisted int64
	err := a.do(func() error {
		c := a.c
		target := a.sent + int64(len(data))
		for {
			c.mu.Lock()
			after := c.responses
			c.mu.Unlock()
			if err := a.sendData(data, wire.Buf(nil).Bool(7, true).Bool(8, true)); err != nil {
				return err
			}
			data = nil
			if err := c.wait(a.w.ctx, func() bool { return c.responses > after }); err != nil {
				return err
			}
			c.mu.Lock()
			persisted = c.persisted
			c.mu.Unlock()
			a.trim(persisted)
			if persisted >= target {
				return nil
			}
		}
	})
	return persisted, err
}

func (a *appendWriter) close(data []byte, finalize bool) (*ObjectAttrs, error) {
	if !finalize {
		if _, err := a.flush(data); err != nil {
			return nil, err
		}
	}
	err := a.do(func() error {
		c := a.c
		if finalize {
			if err := a.sendData(data, wire.Buf(nil).Bool(9, true)); err != nil {
				return err
			}
			data = nil
		}
		c.s.closeSend()
		return c.wait(a.w.ctx, func() bool { return c.ended && (!finalize || c.resource != nil && !c.resource.Finalized.IsZero()) })
	})
	if a.c != nil {
		a.c.s.close()
	}
	if err != nil {
		return nil, err
	}
	a.c.mu.Lock()
	defer a.c.mu.Unlock()
	attrs := a.c.resource
	if attrs == nil {
		attrs = &ObjectAttrs{Bucket: a.w.o.bucket, Name: a.w.o.name, Generation: a.gen, ContentType: a.w.ContentType}
	}
	attrs.Size = a.sent
	return attrs, nil
}
