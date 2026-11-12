package bigquery_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/eeertekin/gcp/bigquery"
	"github.com/eeertekin/gcp/bigquery/bigquerytest"
	"github.com/eeertekin/gcp/googleapi"
)

func TestTableDelete(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "t")
	ctx := context.Background()
	table := c.Dataset("ds").Table("t")

	if err := table.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	if s.HasTable("ds", "t") {
		t.Fatal("table still exists")
	}

	err := table.Delete(ctx)
	if !strings.Contains(err.Error(), "404") {
		t.Fatalf("not-found error must mention 404 like googleapi errors do: %v", err)
	}
	if !bigquery.IsNotFound(err) {
		t.Fatalf("err = %v, want not found", err)
	}
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != http.StatusNotFound || len(apiErr.Errors) != 1 ||
		apiErr.Errors[0].Reason != "notFound" || apiErr.Message != "Not found: Table ds/t" ||
		!strings.Contains(apiErr.Body, `"status":"NOT_FOUND"`) {
		t.Fatalf("api error = %+v", apiErr)
	}
}

func TestTableCreate(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	ctx := context.Background()
	md := &bigquery.TableMetadata{
		Description: "events",
		Schema: bigquery.Schema{
			{Name: "id", Type: bigquery.IntegerFieldType, Required: true},
			{Name: "tags", Type: bigquery.StringFieldType, Repeated: true},
			{Name: "meta", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
				{Name: "source", Type: bigquery.StringFieldType, Description: "origin"},
			}},
		},
	}

	if err := c.Dataset("ds").Table("t").Create(ctx, md); err != nil {
		t.Fatal(err)
	}

	raw := s.Table("ds", "t")
	if bigquerytest.Dig(raw, "tableReference", "projectId") != bigquerytest.ProjectID || bigquerytest.Dig(raw, "tableReference", "tableId") != "t" {
		t.Fatalf("tableReference = %v", bigquerytest.Dig(raw, "tableReference"))
	}
	if bigquerytest.Dig(raw, "description") != "events" {
		t.Errorf("description = %v", bigquerytest.Dig(raw, "description"))
	}
	fields, _ := bigquerytest.Dig(raw, "schema", "fields").([]any)
	if len(fields) != 3 {
		t.Fatalf("schema fields = %v", fields)
	}
	if bigquerytest.Dig(fields[0], "type") != "INTEGER" || bigquerytest.Dig(fields[0], "mode") != "REQUIRED" {
		t.Errorf("field 0 = %v", fields[0])
	}
	if bigquerytest.Dig(fields[1], "mode") != "REPEATED" {
		t.Errorf("field 1 = %v", fields[1])
	}
	if _, ok := fields[2].(map[string]any)["mode"]; ok {
		t.Errorf("nullable field sent mode: %v", fields[2])
	}
	nested, _ := bigquerytest.Dig(fields[2], "fields").([]any)
	if len(nested) != 1 || bigquerytest.Dig(nested[0], "name") != "source" || bigquerytest.Dig(nested[0], "description") != "origin" {
		t.Errorf("nested fields = %v", nested)
	}

	err := c.Dataset("ds").Table("t").Create(ctx, md)
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != http.StatusConflict || len(apiErr.Errors) == 0 || apiErr.Errors[0].Reason != "duplicate" {
		t.Fatalf("err = %v, want 409 duplicate", err)
	}
	if bigquery.IsNotFound(err) {
		t.Fatal("409 reported as not found")
	}
}

func TestTableMetadata(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.SetTable("ds", "t", map[string]any{
		"description": "events",
		"schema": map[string]any{"fields": []any{
			map[string]any{"name": "timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
			map[string]any{"name": "tags", "type": "STRING", "mode": "REPEATED"},
			map[string]any{"name": "meta", "type": "RECORD", "mode": "NULLABLE", "fields": []any{
				map[string]any{"name": "source", "type": "STRING", "description": "origin"},
			}},
		}},
		"timePartitioning": map[string]any{"type": "HOUR", "field": "timestamp", "expirationMs": "7200000"},
		"numRows":          "42",
	})

	md, err := c.Dataset("ds").Table("t").Metadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if md.Description != "events" {
		t.Errorf("description = %q", md.Description)
	}
	tp := md.TimePartitioning
	if tp == nil || tp.Type != bigquery.HourPartitioningType || tp.Field != "timestamp" || tp.Expiration != 2*time.Hour || tp.RequirePartitionFilter {
		t.Fatalf("time partitioning = %+v", tp)
	}
	if len(md.Schema) != 3 {
		t.Fatalf("schema = %v", md.Schema)
	}
	if f := md.Schema[0]; f.Name != "timestamp" || f.Type != bigquery.TimestampFieldType || !f.Required || f.Repeated {
		t.Errorf("field 0 = %+v", f)
	}
	if f := md.Schema[1]; !f.Repeated || f.Required {
		t.Errorf("field 1 = %+v", f)
	}
	if f := md.Schema[2]; f.Required || f.Repeated || len(f.Schema) != 1 || f.Schema[0].Description != "origin" {
		t.Errorf("field 2 = %+v", f)
	}
	if calls := s.Calls(); len(calls) != 1 || calls[0] != "GET ds/t" {
		t.Errorf("calls = %v", calls)
	}
}

func TestTableMetadataUnpartitioned(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "t")

	md, err := c.Dataset("ds").Table("t").Metadata(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if md.TimePartitioning != nil || md.Schema != nil {
		t.Fatalf("metadata = %+v", md)
	}
}

func TestTableMetadataNotFound(t *testing.T) {
	_, c := bigquerytest.NewServer(t)

	md, err := c.Dataset("ds").Table("missing").Metadata(context.Background())
	if md != nil {
		t.Fatalf("metadata returned for missing table: %+v", md)
	}
	if !bigquery.IsNotFound(err) {
		t.Fatalf("err = %v, want not found", err)
	}
}

func TestTableCreateWithTimePartitioning(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	ctx := context.Background()
	table := c.Dataset("ds").Table("t")

	err := table.Create(ctx, &bigquery.TableMetadata{
		TimePartitioning: &bigquery.TimePartitioning{Type: bigquery.HourPartitioningType, Field: "timestamp", Expiration: time.Hour},
	})
	if err != nil {
		t.Fatal(err)
	}

	raw := bigquerytest.Dig(s.Table("ds", "t"), "timePartitioning")
	if bigquerytest.Dig(raw, "type") != "HOUR" || bigquerytest.Dig(raw, "field") != "timestamp" {
		t.Fatalf("timePartitioning = %v", raw)
	}
	if got := bigquerytest.Dig(raw, "expirationMs"); got != "3600000" {
		t.Fatalf("expirationMs = %#v, want int64 encoded as string", got)
	}

	md, err := table.Metadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if tp := md.TimePartitioning; tp == nil || tp.Type != bigquery.HourPartitioningType || tp.Expiration != time.Hour {
		t.Fatalf("round-trip time partitioning = %+v", tp)
	}
}

func TestTableCreateWithoutMetadata(t *testing.T) {
	s, c := bigquerytest.NewServer(t)

	if err := c.Dataset("ds").Table("t").Create(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"schema", "timePartitioning"} {
		if _, ok := s.Table("ds", "t")[k]; ok {
			t.Fatalf("%s sent without metadata: %v", k, s.Table("ds", "t"))
		}
	}
}
