package bigquery

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
)

type Dataset struct {
	ProjectID string
	DatasetID string

	c *Client
}

func (c *Client) Dataset(id string) *Dataset {
	return c.DatasetInProject(c.projectID, id)
}

func (c *Client) DatasetInProject(projectID, datasetID string) *Dataset {
	return &Dataset{ProjectID: projectID, DatasetID: datasetID, c: c}
}

type Table struct {
	ProjectID string
	DatasetID string
	TableID   string

	c *Client
}

func (d *Dataset) Table(id string) *Table {
	return &Table{ProjectID: d.ProjectID, DatasetID: d.DatasetID, TableID: id, c: d.c}
}

func (t *Table) ref() tableReference {
	return tableReference{ProjectID: t.ProjectID, DatasetID: t.DatasetID, TableID: t.TableID}
}

func (t *Table) path() string {
	return fmt.Sprintf("/projects/%s/datasets/%s/tables/%s",
		url.PathEscape(t.ProjectID), url.PathEscape(t.DatasetID), url.PathEscape(t.TableID))
}

func (t *Table) Metadata(ctx context.Context) (*TableMetadata, error) {
	var out tableResource
	if err := t.c.do(ctx, http.MethodGet, t.path(), nil, nil, &out); err != nil {
		return nil, err
	}
	md := &TableMetadata{
		Description:      out.Description,
		TimePartitioning: timePartitioningFromWire(out.TimePartitioning),
	}
	if out.Schema != nil {
		md.Schema = schemaFromWire(out.Schema.Fields)
	}
	return md, nil
}

func (t *Table) Delete(ctx context.Context) error {
	applied, err := t.c.send(ctx, http.MethodDelete, t.path(), nil, nil, nil)
	if applied && IsNotFound(err) {
		return nil
	}
	return err
}

func (t *Table) Create(ctx context.Context, md *TableMetadata) error {
	body := tableResource{TableReference: t.ref()}
	if md != nil {
		body.Description = md.Description
		body.TimePartitioning = md.TimePartitioning.toWire()
		if md.Schema != nil {
			body.Schema = &wireSchema{Fields: md.Schema.toWire()}
		}
	}
	path := fmt.Sprintf("/projects/%s/datasets/%s/tables",
		url.PathEscape(t.ProjectID), url.PathEscape(t.DatasetID))
	applied, err := t.c.send(ctx, http.MethodPost, path, nil, body, nil)
	if applied && isConflict(err) {
		return nil
	}
	return err
}
