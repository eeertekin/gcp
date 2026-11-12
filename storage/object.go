package storage

import (
	"context"
	"errors"
	"net/http"
	"net/url"
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
}

func (b *BucketHandle) Object(name string) *ObjectHandle {
	return &ObjectHandle{c: b.c, bucket: b.name, name: name}
}

type ObjectAttrs struct {
	Bucket      string `json:"bucket"`
	Name        string `json:"name"`
	ContentType string `json:"contentType"`
	Size        int64  `json:"size,string"`
	Generation  int64  `json:"generation,string"`
}

func (o *ObjectHandle) validate() error {
	if o.bucket == "" {
		return errors.New("storage: bucket name is empty")
	}
	if o.name == "" {
		return errors.New("storage: object name is empty")
	}
	return nil
}

func (o *ObjectHandle) Delete(ctx context.Context) error {
	if err := o.validate(); err != nil {
		return err
	}
	u := o.c.endpoint + "/storage/v1/b/" + url.PathEscape(o.bucket) + "/o/" + url.PathEscape(o.name)
	_, applied, err := o.c.call(ctx, http.MethodDelete, u, nil, nil)
	if isNotFound(err) {
		if applied {
			return nil
		}
		return ErrObjectNotExist
	}
	return err
}
