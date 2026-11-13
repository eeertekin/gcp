package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
)

type Composer struct {
	ContentType string

	dst  *ObjectHandle
	srcs []*ObjectHandle
}

func (o *ObjectHandle) ComposerFrom(srcs ...*ObjectHandle) *Composer {
	return &Composer{dst: o, srcs: srcs}
}

func (c *Composer) Run(ctx context.Context) (*ObjectAttrs, error) {
	if err := c.dst.validate(); err != nil {
		return nil, err
	}
	if c.dst.c.grpc != nil {
		return nil, errors.New("storage: compose is not supported by the gRPC client")
	}
	type source struct {
		Name       string `json:"name"`
		Generation int64  `json:"generation,omitempty,string"`
	}
	var body struct {
		SourceObjects []source `json:"sourceObjects"`
		Destination   struct {
			ContentType string `json:"contentType,omitempty"`
		} `json:"destination"`
	}
	for _, s := range c.srcs {
		if err := s.validate(); err != nil {
			return nil, err
		}
		if s.bucket != c.dst.bucket {
			return nil, fmt.Errorf("storage: compose source %s/%s is not in bucket %s", s.bucket, s.name, c.dst.bucket)
		}
		body.SourceObjects = append(body.SourceObjects, source{Name: s.name, Generation: s.gen})
	}
	body.Destination.ContentType = c.ContentType
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	header := http.Header{"Content-Type": {"application/json"}}
	resp, _, err := c.dst.c.call(ctx, http.MethodPost, withQuery(c.dst.url("/compose"), c.dst.query(nil)), header, data)
	if err != nil {
		return nil, err
	}
	return decodeAttrs(resp.body)
}

type Copier struct {
	ObjectAttrs

	dst *ObjectHandle
	src *ObjectHandle
}

func (o *ObjectHandle) CopierFrom(src *ObjectHandle) *Copier {
	return &Copier{dst: o, src: src}
}

func (c *Copier) Run(ctx context.Context) (*ObjectAttrs, error) {
	if err := c.dst.validate(); err != nil {
		return nil, err
	}
	if err := c.src.validate(); err != nil {
		return nil, err
	}
	if c.dst.c.grpc != nil {
		return nil, errors.New("storage: copy is not supported by the gRPC client")
	}
	q := c.dst.query(nil)
	q.Del("generation")
	if c.src.gen != 0 {
		q.Set("sourceGeneration", fmt.Sprint(c.src.gen))
	}
	base := c.src.url("/rewriteTo/b/" + url.PathEscape(c.dst.bucket) + "/o/" + url.PathEscape(c.dst.name))
	body, err := json.Marshal(struct {
		ContentType        string            `json:"contentType,omitempty"`
		ContentEncoding    string            `json:"contentEncoding,omitempty"`
		ContentDisposition string            `json:"contentDisposition,omitempty"`
		Metadata           map[string]string `json:"metadata,omitempty"`
	}{c.ContentType, c.ContentEncoding, c.ContentDisposition, c.Metadata})
	if err != nil {
		return nil, err
	}
	header := http.Header{"Content-Type": {"application/json"}}
	for {
		resp, _, err := c.dst.c.call(ctx, http.MethodPost, withQuery(base, q), header, body)
		if isNotFound(err) {
			return nil, ErrObjectNotExist
		}
		if err != nil {
			return nil, err
		}
		var r struct {
			Done         bool            `json:"done"`
			RewriteToken string          `json:"rewriteToken"`
			Resource     json.RawMessage `json:"resource"`
		}
		if err := json.Unmarshal(resp.body, &r); err != nil {
			return nil, fmt.Errorf("storage: decode rewrite: %w", err)
		}
		if r.Done {
			return decodeAttrs(r.Resource)
		}
		q.Set("rewriteToken", r.RewriteToken)
	}
}
