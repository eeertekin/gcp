package storage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

type BucketHandle struct {
	c    *Client
	name string
}

func (c *Client) Bucket(name string) *BucketHandle {
	return &BucketHandle{c: c, name: name}
}

type ObjectHandle struct {
	c      *Client
	bucket string
	name   string
	gen    int64
	conds  *Conditions
}

func (b *BucketHandle) Object(name string) *ObjectHandle {
	return &ObjectHandle{c: b.c, bucket: b.name, name: name}
}

type Conditions struct {
	GenerationMatch int64
	DoesNotExist    bool
}

func (o *ObjectHandle) If(conds Conditions) *ObjectHandle {
	o2 := *o
	o2.conds = &conds
	return &o2
}

func (o *ObjectHandle) Generation(gen int64) *ObjectHandle {
	o2 := *o
	o2.gen = gen
	return &o2
}

type ObjectAttrs struct {
	Bucket             string            `json:"bucket"`
	Name               string            `json:"name"`
	ContentType        string            `json:"contentType"`
	ContentEncoding    string            `json:"contentEncoding"`
	ContentDisposition string            `json:"contentDisposition"`
	Size               int64             `json:"size,string"`
	Generation         int64             `json:"generation,string"`
	Metadata           map[string]string `json:"metadata"`
	Created            time.Time         `json:"timeCreated"`
	Finalized          time.Time         `json:"timeFinalized"`
	Prefix             string            `json:"-"`
}

func (o *ObjectHandle) validate() error {
	if o.bucket == "" {
		return errors.New("storage: bucket name is empty")
	}
	if o.name == "" {
		return errors.New("storage: object name is empty")
	}
	if o.conds != nil && o.conds.DoesNotExist && o.conds.GenerationMatch != 0 {
		return errors.New("storage: DoesNotExist and GenerationMatch are mutually exclusive")
	}
	return nil
}

func (o *ObjectHandle) url(suffix string) string {
	return o.c.endpoint + "/storage/v1/b/" + url.PathEscape(o.bucket) + "/o/" + url.PathEscape(o.name) + suffix
}

func (o *ObjectHandle) query(q url.Values) url.Values {
	if q == nil {
		q = url.Values{}
	}
	if o.gen != 0 {
		q.Set("generation", strconv.FormatInt(o.gen, 10))
	}
	if o.conds != nil {
		switch {
		case o.conds.DoesNotExist:
			q.Set("ifGenerationMatch", "0")
		case o.conds.GenerationMatch != 0:
			q.Set("ifGenerationMatch", strconv.FormatInt(o.conds.GenerationMatch, 10))
		}
	}
	return q
}

func withQuery(u string, q url.Values) string {
	if len(q) == 0 {
		return u
	}
	return u + "?" + q.Encode()
}

func (o *ObjectHandle) Attrs(ctx context.Context) (*ObjectAttrs, error) {
	if err := o.validate(); err != nil {
		return nil, err
	}
	if o.c.grpc != nil {
		return o.c.grpc.getObject(ctx, o)
	}
	resp, _, err := o.c.call(ctx, http.MethodGet, withQuery(o.url(""), o.query(nil)), nil, nil)
	if isNotFound(err) {
		return nil, ErrObjectNotExist
	}
	if err != nil {
		return nil, err
	}
	return decodeAttrs(resp.body)
}

func decodeAttrs(data []byte) (*ObjectAttrs, error) {
	var a ObjectAttrs
	if err := json.Unmarshal(data, &a); err != nil {
		return nil, fmt.Errorf("storage: decode object: %w", err)
	}
	return &a, nil
}

func (o *ObjectHandle) Delete(ctx context.Context) error {
	if err := o.validate(); err != nil {
		return err
	}
	if o.c.grpc != nil {
		return o.c.grpc.deleteObject(ctx, o)
	}
	_, applied, err := o.c.call(ctx, http.MethodDelete, withQuery(o.url(""), o.query(nil)), nil, nil)
	if isNotFound(err) {
		if applied {
			return nil
		}
		return ErrObjectNotExist
	}
	return err
}
