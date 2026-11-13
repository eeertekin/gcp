package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strconv"
	"strings"
)

const (
	minChunkSize     = 256 << 10
	defaultChunkSize = 16 << 20
)

var errWriterClosed = errors.New("storage: writer is closed")

// Writer buffers up to ChunkSize bytes. Objects that fit in a single chunk are
// sent in one request; larger objects use a resumable upload session.
type Writer struct {
	ContentType     string
	Metadata        map[string]string
	ChunkSize       int
	Append          bool
	FinalizeOnClose bool

	ctx     context.Context
	o       *ObjectHandle
	buf     []byte
	session string
	offset  int64
	closed  bool
	err     error
	attrs   *ObjectAttrs
	aw      *appendWriter
}

func (o *ObjectHandle) NewWriter(ctx context.Context) *Writer {
	return &Writer{ctx: ctx, o: o, ChunkSize: defaultChunkSize}
}

func (w *Writer) Attrs() *ObjectAttrs {
	return w.attrs
}

func (w *Writer) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.closed {
		return 0, errWriterClosed
	}
	if err := w.ctx.Err(); err != nil {
		w.err = err
		return 0, err
	}

	w.buf = append(w.buf, p...)
	chunk := w.chunkSize()
	if w.o.c.grpc != nil {
		if len(w.buf) >= chunk {
			aw, err := w.appendable()
			if err == nil {
				err = aw.write(w.buf)
			}
			if err != nil {
				w.err = err
				return 0, err
			}
			w.buf = w.buf[:0]
		}
		return len(p), nil
	}
	for len(w.buf) > chunk {
		if w.session == "" {
			if w.err = w.startSession(); w.err != nil {
				return 0, w.err
			}
		}
		if w.err = w.send(w.buf[:chunk], false); w.err != nil {
			return 0, w.err
		}
		w.buf = append(w.buf[:0], w.buf[chunk:]...)
	}
	return len(p), nil
}

func (w *Writer) Close() error {
	if w.closed {
		return w.err
	}
	w.closed = true
	if w.err != nil {
		if w.aw != nil && w.aw.c != nil {
			w.aw.c.s.close()
		}
		return w.err
	}

	switch {
	case w.o.c.grpc != nil:
		var aw *appendWriter
		if aw, w.err = w.appendable(); w.err == nil {
			w.attrs, w.err = aw.close(w.buf, w.FinalizeOnClose)
		}
	case w.session == "":
		w.err = w.uploadSingle()
	default:
		w.err = w.send(w.buf, true)
	}
	w.buf = nil
	return w.err
}

func (w *Writer) Flush() (int64, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.closed {
		return 0, errWriterClosed
	}
	if w.o.c.grpc == nil {
		return 0, errors.New("storage: Flush needs an appendable writer on a gRPC client (NewGRPCClient)")
	}
	aw, err := w.appendable()
	if err != nil {
		w.err = err
		return 0, err
	}
	n, err := aw.flush(w.buf)
	w.buf = w.buf[:0]
	w.err = err
	return n, err
}

func (w *Writer) appendable() (*appendWriter, error) {
	if w.aw != nil {
		return w.aw, nil
	}
	if err := w.o.validate(); err != nil {
		return nil, err
	}
	if !w.Append {
		return nil, errAppendOnly
	}
	w.aw = &appendWriter{w: w, g: w.o.c.grpc}
	return w.aw, nil
}

func (w *Writer) chunkSize() int {
	n := w.ChunkSize
	if n <= 0 {
		return defaultChunkSize
	}
	return (n + minChunkSize - 1) / minChunkSize * minChunkSize
}

func (w *Writer) contentType() string {
	if w.ContentType != "" {
		return w.ContentType
	}
	return http.DetectContentType(w.buf)
}

func (w *Writer) uploadURL(uploadType string) string {
	q := w.o.query(url.Values{"uploadType": {uploadType}, "name": {w.o.name}})
	q.Del("generation")
	return w.o.c.endpoint + "/upload/storage/v1/b/" + url.PathEscape(w.o.bucket) + "/o?" + q.Encode()
}

func (w *Writer) metadataJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"contentType": w.contentType(), "metadata": w.Metadata})
}

func (w *Writer) uploadSingle() error {
	if err := w.o.validate(); err != nil {
		return err
	}
	header := http.Header{"Content-Type": {w.contentType()}}
	body, uploadType := w.buf, "media"
	if len(w.Metadata) > 0 {
		meta, err := w.metadataJSON()
		if err != nil {
			return err
		}
		var b bytes.Buffer
		mw := multipart.NewWriter(&b)
		for _, part := range []struct {
			contentType string
			data        []byte
		}{{"application/json; charset=UTF-8", meta}, {w.contentType(), w.buf}} {
			pw, err := mw.CreatePart(textproto.MIMEHeader{"Content-Type": {part.contentType}})
			if err != nil {
				return err
			}
			pw.Write(part.data)
		}
		mw.Close()
		body, uploadType = b.Bytes(), "multipart"
		header.Set("Content-Type", "multipart/related; boundary="+mw.Boundary())
	}
	resp, _, err := w.o.c.call(w.ctx, http.MethodPost, w.uploadURL(uploadType), header, body)
	if err != nil {
		return err
	}
	return w.setAttrs(resp.body)
}

func (w *Writer) startSession() error {
	if err := w.o.validate(); err != nil {
		return err
	}
	header := http.Header{"X-Upload-Content-Type": {w.contentType()}}
	var meta []byte
	if len(w.Metadata) > 0 {
		var err error
		if meta, err = w.metadataJSON(); err != nil {
			return err
		}
		header.Set("Content-Type", "application/json; charset=UTF-8")
	}
	resp, _, err := w.o.c.call(w.ctx, http.MethodPost, w.uploadURL("resumable"), header, meta)
	if err != nil {
		return err
	}
	w.session = resp.header.Get("Location")
	if w.session == "" {
		return errors.New("storage: resumable upload response missing Location header")
	}
	return nil
}

// send uploads data starting at w.offset. Non-final chunks must be a multiple
// of 256 KiB. After a failed attempt the persisted offset is queried and the
// upload resumes from there.
func (w *Writer) send(data []byte, final bool) error {
	c := w.o.c
	start := w.offset
	end := start + int64(len(data))
	total := "*"
	if final {
		total = strconv.FormatInt(end, 10)
	}

	probe := false
	failures := 0
	delay := c.Retry.Initial
	for {
		var body []byte
		rng := "bytes */" + total
		if !probe && w.offset < end {
			body = data[w.offset-start:]
			rng = fmt.Sprintf("bytes %d-%d/%s", w.offset, end-1, total)
		}

		resp, err := c.roundTrip(w.ctx, http.MethodPut, w.session, http.Header{"Content-Range": {rng}}, body)
		if err == nil {
			switch {
			case resp.code == http.StatusOK || resp.code == http.StatusCreated:
				if !final {
					return errors.New("storage: upload finalized before all data was sent")
				}
				w.offset = end
				return w.setAttrs(resp.body)
			case resp.code == http.StatusPermanentRedirect:
				off, perr := persistedOffset(resp.header.Get("Range"))
				if perr != nil {
					return perr
				}
				if off < start || off > end {
					return fmt.Errorf("storage: server persisted %d bytes, expected between %d and %d", off, start, end)
				}
				progressed := off > w.offset
				w.offset = off
				if !final && off == end {
					return nil
				}
				if probe || progressed {
					probe = false
					failures = 0
					delay = c.Retry.Initial
					continue
				}
				err = fmt.Errorf("storage: upload made no progress at offset %d", off)
			default:
				err = parseAPIError(resp.code, resp.body)
				if retry, _ := retryable(err); !retry {
					return err
				}
			}
		} else if retry, _ := retryable(err); !retry {
			return err
		}

		failures++
		if failures >= c.Retry.MaxAttempts || w.ctx.Err() != nil {
			return err
		}
		if err := sleep(w.ctx, jitter(delay)); err != nil {
			return err
		}
		delay = c.Retry.next(delay)
		probe = true
	}
}

func persistedOffset(rng string) (int64, error) {
	if rng == "" {
		return 0, nil
	}
	_, last, ok := strings.Cut(strings.TrimPrefix(rng, "bytes="), "-")
	n, err := strconv.ParseInt(last, 10, 64)
	if !ok || err != nil {
		return 0, fmt.Errorf("storage: invalid Range header %q", rng)
	}
	return n + 1, nil
}

func (w *Writer) setAttrs(data []byte) error {
	var a ObjectAttrs
	if err := json.Unmarshal(data, &a); err != nil {
		return fmt.Errorf("storage: decode object: %w", err)
	}
	w.attrs = &a
	return nil
}
