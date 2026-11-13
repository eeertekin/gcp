package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

type Reader struct {
	body   io.ReadCloser
	cancel context.CancelFunc
}

func (r *Reader) Read(p []byte) (int, error) { return r.body.Read(p) }

func (r *Reader) Close() error {
	err := r.body.Close()
	r.cancel()
	return err
}

func (o *ObjectHandle) NewReader(ctx context.Context) (*Reader, error) {
	return o.NewRangeReader(ctx, 0, -1)
}

func (o *ObjectHandle) NewRangeReader(ctx context.Context, offset, length int64) (*Reader, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	if offset < 0 {
		return nil, fmt.Errorf("storage: negative offset %d", offset)
	}
	if length == 0 {
		return &Reader{body: io.NopCloser(eofReader{}), cancel: func() {}}, nil
	}
	if o.c.grpc != nil {
		return o.c.grpc.newRangeReader(ctx, o, offset, length)
	}

	header := http.Header{}
	switch {
	case length > 0:
		header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	case offset > 0:
		header.Set("Range", fmt.Sprintf("bytes=%d-", offset))
	}
	u := withQuery(o.url(""), o.query(url.Values{"alt": {"media"}}))

	delay := o.c.Retry.Initial
	for attempt := 1; ; attempt++ {
		r, err := o.c.openStream(ctx, u, header)
		if err == nil {
			return r, nil
		}
		if isNotFound(err) {
			return nil, ErrObjectNotExist
		}
		if retry, _ := retryable(err); !retry || attempt >= o.c.Retry.MaxAttempts || ctx.Err() != nil {
			return nil, err
		}
		if err := sleep(ctx, jitter(delay)); err != nil {
			return nil, err
		}
		delay = o.c.Retry.next(delay)
	}
}

func (c *Client) openStream(ctx context.Context, u string, header http.Header) (*Reader, error) {
	ctx, cancel := context.WithCancel(ctx)
	req, err := c.newRequest(ctx, http.MethodGet, u, header, nil)
	if err != nil {
		cancel()
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		cancel()
		return nil, &transportError{err: err}
	}
	if resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		resp.Body.Close()
		return &Reader{body: io.NopCloser(eofReader{}), cancel: cancel}, nil
	}
	if !isSuccess(resp.StatusCode) {
		data, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		cancel()
		return nil, parseAPIError(resp.StatusCode, data)
	}
	return &Reader{body: resp.Body, cancel: cancel}, nil
}

type eofReader struct{}

func (eofReader) Read([]byte) (int, error) { return 0, io.EOF }
