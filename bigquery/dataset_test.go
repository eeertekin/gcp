package bigquery_test

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"testing"

	"github.com/eeertekin/gcp/bigquery"
	"github.com/eeertekin/gcp/bigquery/bigquerytest"
	"github.com/eeertekin/gcp/googleapi"
	"github.com/eeertekin/gcp/iterator"
)

func TestDatasetsAndTablesPaged(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("analytics", "events")
	s.AddTable("analytics", "users")
	s.AddTable("sales", "orders")
	s.AddDataset("empty")
	s.SetListPageSize(1)
	ctx := context.Background()

	var names []string
	it := c.Datasets(ctx)
	for {
		ds, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if ds.ProjectID != bigquerytest.ProjectID {
			t.Errorf("dataset %s in project %q", ds.DatasetID, ds.ProjectID)
		}
		names = append(names, ds.DatasetID)
	}
	if want := []string{"analytics", "empty", "sales"}; !reflect.DeepEqual(names, want) {
		t.Fatalf("datasets %v, want %v", names, want)
	}
	if _, err := it.Next(); !errors.Is(err, iterator.Done) {
		t.Fatalf("Next after Done: %v", err)
	}

	var tables []string
	tit := c.Dataset("analytics").Tables(ctx)
	for {
		tb, err := tit.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if tb.ProjectID != bigquerytest.ProjectID || tb.DatasetID != "analytics" {
			t.Errorf("table %+v", tb)
		}
		tables = append(tables, tb.TableID)
	}
	if want := []string{"events", "users"}; !reflect.DeepEqual(tables, want) {
		t.Fatalf("tables %v, want %v", tables, want)
	}
	if _, err := c.Dataset("empty").Tables(ctx).Next(); !errors.Is(err, iterator.Done) {
		t.Fatalf("empty dataset: %v", err)
	}

	want := []string{
		"LIST datasets", "LIST datasets", "LIST datasets",
		"LIST tables analytics", "LIST tables analytics",
		"LIST tables empty",
	}
	if calls := s.Calls(); !reflect.DeepEqual(calls, want) {
		t.Fatalf("calls %v, want %v (one request per page, none after the last)", calls, want)
	}
	for _, r := range s.ResultRequests() {
		t.Errorf("unexpected getQueryResults %+v", r)
	}
}

func TestTablesOfMissingDataset(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	it := c.Dataset("nope").Tables(context.Background())
	_, err := it.Next()
	var apiErr *googleapi.Error
	if !bigquery.IsNotFound(err) || !errors.As(err, &apiErr) || apiErr.Errors[0].Reason != "notFound" {
		t.Fatalf("err %v", err)
	}
	if _, again := it.Next(); again != err {
		t.Fatalf("iterator error not sticky: %v", again)
	}
	if n := s.Attempts(); n != 1 {
		t.Fatalf("%d requests; a failed iterator must not refetch", n)
	}
}

func TestDatasetMetadata(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddDataset("analytics")
	ctx := context.Background()

	md, err := c.Dataset("analytics").Metadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if md.Location != bigquerytest.Location || md.FullID != bigquerytest.ProjectID+":analytics" {
		t.Fatalf("metadata %+v", md)
	}

	_, err = c.Dataset("missing").Metadata(ctx)
	var apiErr *googleapi.Error
	if !bigquery.IsNotFound(err) || !errors.As(err, &apiErr) || apiErr.Code != http.StatusNotFound {
		t.Fatalf("err %v", err)
	}
	if want := []string{"GET dataset analytics", "GET dataset missing"}; !reflect.DeepEqual(s.Calls(), want) {
		t.Fatalf("calls %v", s.Calls())
	}
}
