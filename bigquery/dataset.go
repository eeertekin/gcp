package bigquery

import (
	"context"
	"net/http"
	"net/url"

	"github.com/eeertekin/gcp/iterator"
)

type DatasetMetadata struct {
	Name        string
	Description string
	Location    string
	FullID      string
	Labels      map[string]string
}

func (d *Dataset) Metadata(ctx context.Context) (*DatasetMetadata, error) {
	var out datasetResource
	if err := d.c.do(ctx, http.MethodGet, d.c.path(d.ProjectID, "datasets", d.DatasetID), nil, nil, &out); err != nil {
		return nil, err
	}
	return &DatasetMetadata{
		Name:        out.FriendlyName,
		Description: out.Description,
		Location:    out.Location,
		FullID:      out.ID,
		Labels:      out.Labels,
	}, nil
}

type pager[T any] struct {
	ctx   context.Context
	items []T
	token string
	done  bool
	err   error
	fetch func(ctx context.Context, token string) ([]T, string, error)
}

func (p *pager[T]) next() (T, error) {
	var zero T
	for len(p.items) == 0 {
		if p.err != nil {
			return zero, p.err
		}
		if p.done {
			return zero, iterator.Done
		}
		items, token, err := p.fetch(p.ctx, p.token)
		if err != nil {
			p.err = err
			return zero, err
		}
		p.items, p.token, p.done = items, token, token == ""
	}
	item := p.items[0]
	p.items = p.items[1:]
	return item, nil
}

func pageQuery(token string) url.Values {
	if token == "" {
		return nil
	}
	return url.Values{"pageToken": {token}}
}

type DatasetIterator struct {
	p pager[*Dataset]
}

func (c *Client) Datasets(ctx context.Context) *DatasetIterator {
	it := &DatasetIterator{}
	it.p = pager[*Dataset]{ctx: ctx, fetch: func(ctx context.Context, token string) ([]*Dataset, string, error) {
		var out struct {
			Datasets []struct {
				DatasetReference datasetReference `json:"datasetReference"`
			} `json:"datasets"`
			NextPageToken string `json:"nextPageToken"`
		}
		if err := c.do(ctx, http.MethodGet, c.path("", "datasets"), pageQuery(token), nil, &out); err != nil {
			return nil, "", err
		}
		ds := make([]*Dataset, len(out.Datasets))
		for i, d := range out.Datasets {
			ds[i] = c.DatasetInProject(d.DatasetReference.ProjectID, d.DatasetReference.DatasetID)
		}
		return ds, out.NextPageToken, nil
	}}
	return it
}

func (it *DatasetIterator) Next() (*Dataset, error) {
	return it.p.next()
}

type TableIterator struct {
	p pager[*Table]
}

func (d *Dataset) Tables(ctx context.Context) *TableIterator {
	it := &TableIterator{}
	it.p = pager[*Table]{ctx: ctx, fetch: func(ctx context.Context, token string) ([]*Table, string, error) {
		var out struct {
			Tables []struct {
				TableReference tableReference `json:"tableReference"`
			} `json:"tables"`
			NextPageToken string `json:"nextPageToken"`
		}
		if err := d.c.do(ctx, http.MethodGet, d.c.path(d.ProjectID, "datasets", d.DatasetID, "tables"), pageQuery(token), nil, &out); err != nil {
			return nil, "", err
		}
		ts := make([]*Table, len(out.Tables))
		for i, t := range out.Tables {
			ts[i] = d.Table(t.TableReference.TableID)
		}
		return ts, out.NextPageToken, nil
	}}
	return it
}

func (it *TableIterator) Next() (*Table, error) {
	return it.p.next()
}
