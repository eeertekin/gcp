package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
	ContentType string
	ChunkSize   int

	ctx     context.Context
	o       *ObjectHandle
	buf     []byte
	session string
	offset  int64
	closed  bool
	err     error
	attrs   *ObjectAttrs
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
		return w.err
	}

	if w.session == "" {
		w.err = w.uploadSingle()
	} else {
		w.err = w.send(w.buf, true)
	}
	w.buf = nil
	return w.err
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
	q := url.Values{"uploadType": {uploadType}, "name": {w.o.name}}
	return w.o.c.endpoint + "/upload/storage/v1/b/" + url.PathEscape(w.o.bucket) + "/o?" + q.Encode()
}

func (w *Writer) uploadSingle() error {
	if err := w.o.validate(); err != nil {
		return err
	}
	header := http.Header{"Content-Type": {w.contentType()}}
	resp, _, err := w.o.c.call(w.ctx, http.MethodPost, w.uploadURL("media"), header, w.buf)
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
	resp, _, err := w.o.c.call(w.ctx, http.MethodPost, w.uploadURL("resumable"), header, nil)
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
