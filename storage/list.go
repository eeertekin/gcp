package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"

	"github.com/eeertekin/gcp/iterator"
)

type Query struct {
	Prefix    string
	Delimiter string
}

type ObjectIterator struct {
	ctx   context.Context
	b     *BucketHandle
	q     Query
	items []*ObjectAttrs
	token string
	done  bool
}

func (b *BucketHandle) Objects(ctx context.Context, q *Query) *ObjectIterator {
	it := &ObjectIterator{ctx: ctx, b: b}
	if q != nil {
		it.q = *q
	}
	return it
}

func (it *ObjectIterator) Next() (*ObjectAttrs, error) {
	for len(it.items) == 0 {
		if it.done {
			return nil, iterator.Done
		}
		if err := it.fetch(); err != nil {
			return nil, err
		}
	}
	a := it.items[0]
	it.items = it.items[1:]
	return a, nil
}

type objectPage struct {
	items []*ObjectAttrs
	next  string
}

func (it *ObjectIterator) fetch() error {
	var page *objectPage
	var err error
	if it.b.c.grpc != nil {
		page, err = it.b.c.grpc.listObjects(it.ctx, it.b.name, it.q, it.token)
	} else {
		page, err = it.fetchJSON()
	}
	if err != nil {
		return err
	}
	it.items, it.token = page.items, page.next
	it.done = page.next == ""
	return nil
}

func (it *ObjectIterator) fetchJSON() (*objectPage, error) {
	q := url.Values{}
	if it.q.Prefix != "" {
		q.Set("prefix", it.q.Prefix)
	}
	if it.q.Delimiter != "" {
		q.Set("delimiter", it.q.Delimiter)
	}
	if it.token != "" {
		q.Set("pageToken", it.token)
	}
	u := it.b.c.endpoint + "/storage/v1/b/" + url.PathEscape(it.b.name) + "/o"
	resp, _, err := it.b.c.call(it.ctx, http.MethodGet, withQuery(u, q), nil, nil)
	if err != nil {
		return nil, err
	}
	var body struct {
		Items         []*ObjectAttrs `json:"items"`
		Prefixes      []string       `json:"prefixes"`
		NextPageToken string         `json:"nextPageToken"`
	}
	if err := json.Unmarshal(resp.body, &body); err != nil {
		return nil, fmt.Errorf("storage: decode object list: %w", err)
	}
	page := &objectPage{items: body.Items, next: body.NextPageToken}
	for _, p := range body.Prefixes {
		page.items = append(page.items, &ObjectAttrs{Prefix: p})
	}
	return page, nil
}
