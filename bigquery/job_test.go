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

func TestLoaderRun(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "t")
	ctx := context.Background()

	gcsRef := bigquery.NewGCSReference("gs://bucket/a-*.json", "gs://bucket/b.json")
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.IgnoreUnknownValues = true
	gcsRef.AllowJaggedRows = true
	gcsRef.MaxBadRecords = 1000
	gcsRef.Schema = bigquery.Schema{{Name: "id", Type: bigquery.IntegerFieldType}}

	loader := c.Dataset("ds").Table("t").LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteAppend
	loader.CreateDisposition = bigquery.CreateNever
	loader.JobID = "load-1"

	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID() != "load-1" || job.Location() != bigquerytest.Location {
		t.Fatalf("job id=%q location=%q", job.ID(), job.Location())
	}
	if job.LastStatus().Done() {
		t.Fatal("job reported done before polling")
	}

	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Done() || status.Err() != nil || job.LastStatus() != status {
		t.Fatalf("status = %+v err = %v", status, status.Err())
	}

	info, ok := s.Job("load-1")
	if !ok || info.Kind != "LOAD" || info.Polls < 1 || !info.Done {
		t.Fatalf("job info = %+v", info)
	}

	cfg := bigquerytest.Dig(info.Request, "configuration", "load")
	checks := map[string]any{
		"sourceFormat":        "NEWLINE_DELIMITED_JSON",
		"writeDisposition":    "WRITE_APPEND",
		"createDisposition":   "CREATE_NEVER",
		"ignoreUnknownValues": true,
		"allowJaggedRows":     true,
		"maxBadRecords":       float64(1000),
	}
	for k, v := range checks {
		if got := bigquerytest.Dig(cfg, k); got != v {
			t.Errorf("load.%s = %v, want %v", k, got, v)
		}
	}
	if uris, _ := bigquerytest.Dig(cfg, "sourceUris").([]any); len(uris) != 2 || uris[0] != "gs://bucket/a-*.json" || uris[1] != "gs://bucket/b.json" {
		t.Errorf("load.sourceUris = %v", bigquerytest.Dig(cfg, "sourceUris"))
	}
	if bigquerytest.Dig(cfg, "destinationTable", "projectId") != bigquerytest.ProjectID || bigquerytest.Dig(cfg, "destinationTable", "tableId") != "t" {
		t.Errorf("load.destinationTable = %v", bigquerytest.Dig(cfg, "destinationTable"))
	}
	if fields, _ := bigquerytest.Dig(cfg, "schema", "fields").([]any); len(fields) != 1 || bigquerytest.Dig(fields[0], "type") != "INTEGER" {
		t.Errorf("load.schema = %v", bigquerytest.Dig(cfg, "schema"))
	}
	if _, ok := cfg.(map[string]any)["autodetect"]; ok {
		t.Error("autodetect=false should be omitted")
	}
	if loc := bigquerytest.Dig(info.Request, "jobReference", "location"); loc != nil {
		t.Errorf("location sent without being configured: %v", loc)
	}
}

func TestLoaderOmitsUnsetOptions(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "t")

	loader := c.Dataset("ds").Table("t").LoaderFrom(bigquery.NewGCSReference("gs://bucket/a.json"))
	loader.JobID = "load-1"
	if _, err := loader.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	info, _ := s.Job("load-1")
	cfg, _ := bigquerytest.Dig(info.Request, "configuration", "load").(map[string]any)
	for _, k := range []string{"writeDisposition", "createDisposition", "schema", "sourceFormat", "maxBadRecords", "ignoreUnknownValues", "allowJaggedRows", "timePartitioning"} {
		if v, ok := cfg[k]; ok {
			t.Errorf("unset option %s sent as %v", k, v)
		}
	}
	if _, ok := bigquerytest.Dig(info.Request, "configuration").(map[string]any)["copy"]; ok {
		t.Error("nil copy config sent")
	}
}

func TestLoaderTimePartitioningFromTableMetadata(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.SetTable("ds", "events", map[string]any{
		"timePartitioning": map[string]any{"type": "DAY", "field": "created_at"},
	})
	ctx := context.Background()

	gcsRef := bigquery.NewGCSReference("gs://bucket/events.json")
	gcsRef.SourceFormat = bigquery.JSON
	gcsRef.IgnoreUnknownValues = true

	table := c.Dataset("ds").Table("events")
	tableMeta, _ := table.Metadata(ctx)

	loader := table.LoaderFrom(gcsRef)
	loader.WriteDisposition = bigquery.WriteAppend
	loader.TimePartitioning = &bigquery.TimePartitioning{
		Field:                  "timestamp",
		RequirePartitionFilter: false,
		Type:                   "HOUR",
	}
	if tableMeta != nil {
		loader.TimePartitioning.Type = tableMeta.TimePartitioning.Type
		loader.TimePartitioning.Field = tableMeta.TimePartitioning.Field
	}

	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Err() != nil {
		t.Fatalf("status.Err() = %v", status.Err())
	}

	info, _ := s.Job(job.ID())
	tp := bigquerytest.Dig(info.Request, "configuration", "load", "timePartitioning")
	if bigquerytest.Dig(tp, "type") != "DAY" || bigquerytest.Dig(tp, "field") != "created_at" {
		t.Fatalf("load.timePartitioning = %v", tp)
	}
	for _, k := range []string{"expirationMs", "requirePartitionFilter"} {
		if v, ok := tp.(map[string]any)[k]; ok {
			t.Errorf("unset %s sent as %v", k, v)
		}
	}
}

func TestLoaderTimePartitioningMismatch(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.SetTable("ds", "events", map[string]any{
		"timePartitioning": map[string]any{"type": "DAY", "field": "created_at"},
	})
	ctx := context.Background()

	loader := c.Dataset("ds").Table("events").LoaderFrom(bigquery.NewGCSReference("gs://bucket/events.json"))
	loader.TimePartitioning = &bigquery.TimePartitioning{Type: bigquery.HourPartitioningType, Field: "timestamp"}
	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var bqErr *bigquery.Error
	if !errors.As(status.Err(), &bqErr) || bqErr.Reason != "invalid" || !strings.Contains(bqErr.Message, "Incompatible table partitioning") {
		t.Fatalf("status.Err() = %v, want incompatible partitioning", status.Err())
	}
}

func TestLoaderTimePartitioningDefaults(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.SetTable("ds", "events", map[string]any{
		"timePartitioning": map[string]any{"type": "DAY", "field": "ts"},
	})
	ctx := context.Background()

	loader := c.Dataset("ds").Table("events").LoaderFrom(bigquery.NewGCSReference("gs://bucket/events.json"))
	loader.TimePartitioning = &bigquery.TimePartitioning{Field: "ts", Expiration: 48 * time.Hour, RequirePartitionFilter: true}
	job, err := loader.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status, err := job.Wait(ctx); err != nil || status.Err() != nil {
		t.Fatalf("wait err=%v status err=%v", err, status.Err())
	}

	info, _ := s.Job(job.ID())
	tp := bigquerytest.Dig(info.Request, "configuration", "load", "timePartitioning")
	checks := map[string]any{
		"type":                   "DAY",
		"field":                  "ts",
		"expirationMs":           "172800000",
		"requirePartitionFilter": true,
	}
	for k, v := range checks {
		if got := bigquerytest.Dig(tp, k); got != v {
			t.Errorf("load.timePartitioning.%s = %#v, want %#v", k, got, v)
		}
	}
}

func TestLoadJobFailureReportedInStatus(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "t")
	s.FailLoads(
		bigquery.Error{Reason: "invalid", Location: "a.json", Message: "row 1: bad value"},
		bigquery.Error{Reason: "invalid", Location: "a.json", Message: "row 7: bad value"},
	)
	ctx := context.Background()

	job, err := c.Dataset("ds").Table("t").LoaderFrom(bigquery.NewGCSReference("gs://bucket/a.json")).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatalf("job failure must be reported via status.Err(), not Wait error: %v", err)
	}
	if status.Err() == nil {
		t.Fatal("status.Err() = nil for failed job")
	}
	var bqErr *bigquery.Error
	if !errors.As(status.Err(), &bqErr) || bqErr.Reason != "invalid" || bqErr.Message != "Error while reading data" {
		t.Fatalf("status.Err() = %#v", status.Err())
	}
	if len(status.Errors) != 2 {
		t.Fatalf("status.Errors = %v", status.Errors)
	}
	if got, want := status.Errors[1].Error(), `{Location: "a.json"; Message: "row 7: bad value"; Reason: "invalid"}`; got != want {
		t.Fatalf("Errors[1].Error() = %s, want %s", got, want)
	}
}

func TestLoadJobMissingTable(t *testing.T) {
	_, c := bigquerytest.NewServer(t)
	ctx := context.Background()

	job, err := c.Dataset("ds").Table("missing").LoaderFrom(bigquery.NewGCSReference("gs://bucket/a.json")).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var bqErr *bigquery.Error
	if !errors.As(status.Err(), &bqErr) || bqErr.Reason != "notFound" {
		t.Fatalf("status.Err() = %v, want notFound", status.Err())
	}
}

func TestCopierRun(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "src")
	ctx := context.Background()
	ds := c.Dataset("ds")

	copier := ds.Table("dst").CopierFrom(ds.Table("src"))
	copier.WriteDisposition = bigquery.WriteTruncate
	job, err := copier.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil || status.Err() != nil {
		t.Fatalf("wait err=%v status err=%v", err, status.Err())
	}

	info, ok := s.Job(job.ID())
	if !ok || info.Kind != "COPY" || info.Polls < 1 {
		t.Fatalf("job info = %+v", info)
	}
	cfg := bigquerytest.Dig(info.Request, "configuration", "copy")
	if got := bigquerytest.Dig(cfg, "writeDisposition"); got != "WRITE_TRUNCATE" {
		t.Errorf("copy.writeDisposition = %v", got)
	}
	srcs, _ := bigquerytest.Dig(cfg, "sourceTables").([]any)
	if len(srcs) != 1 || bigquerytest.Dig(srcs[0], "tableId") != "src" || bigquerytest.Dig(srcs[0], "projectId") != bigquerytest.ProjectID {
		t.Errorf("copy.sourceTables = %v", srcs)
	}
	if bigquerytest.Dig(cfg, "destinationTable", "tableId") != "dst" {
		t.Errorf("copy.destinationTable = %v", bigquerytest.Dig(cfg, "destinationTable"))
	}
	if !s.HasTable("ds", "dst") {
		t.Fatal("copy destination missing")
	}
}

func TestCopierMultipleSources(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "a")
	s.AddTable("ds", "b")
	ds := c.Dataset("ds")

	job, err := ds.Table("dst").CopierFrom(ds.Table("a"), ds.Table("b")).Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	info, _ := s.Job(job.ID())
	if len(info.Sources) != 2 || info.Sources[0] != "ds/a" || info.Sources[1] != "ds/b" {
		t.Fatalf("sources = %v", info.Sources)
	}
}

func TestCopierMissingSource(t *testing.T) {
	_, c := bigquerytest.NewServer(t)
	ctx := context.Background()
	ds := c.Dataset("ds")

	job, err := ds.Table("dst").CopierFrom(ds.Table("missing")).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var bqErr *bigquery.Error
	if !errors.As(status.Err(), &bqErr) || bqErr.Reason != "notFound" {
		t.Fatalf("status.Err() = %v, want notFound", status.Err())
	}
}

func TestCopierWithoutSources(t *testing.T) {
	s, c := bigquerytest.NewServer(t)

	if _, err := c.Dataset("ds").Table("dst").CopierFrom().Run(context.Background()); err == nil {
		t.Fatal("expected error for copier without sources")
	}
	if calls := s.Calls(); len(calls) != 0 {
		t.Fatalf("API called: %v", calls)
	}
}

func TestRunDuplicateJobID(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "src")
	ds := c.Dataset("ds")

	copier := ds.Table("dst").CopierFrom(ds.Table("src"))
	copier.JobID = "fixed"
	if _, err := copier.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := copier.Run(context.Background())
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != http.StatusConflict {
		t.Fatalf("err = %v, want 409", err)
	}
}

func TestRunJobIDGeneration(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "src")
	ds := c.Dataset("ds")
	ctx := context.Background()

	a, err := ds.Table("dst").CopierFrom(ds.Table("src")).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ds.Table("dst").CopierFrom(ds.Table("src")).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID() == "" || a.ID() == b.ID() {
		t.Fatalf("generated job ids %q, %q", a.ID(), b.ID())
	}

	copier := ds.Table("dst").CopierFrom(ds.Table("src"))
	copier.JobID = "nightly"
	copier.AddJobIDSuffix = true
	suffixed, err := copier.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(suffixed.ID(), "nightly-") || len(suffixed.ID()) <= len("nightly-") {
		t.Fatalf("suffixed job id = %q", suffixed.ID())
	}
}

func TestJobLocation(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "src")
	ds := c.Dataset("ds")
	ctx := context.Background()

	c.Location = "US"
	job, err := ds.Table("dst").CopierFrom(ds.Table("src")).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := s.Job(job.ID())
	if loc := bigquerytest.Dig(info.Request, "jobReference", "location"); loc != "US" {
		t.Fatalf("client location not sent: %v", loc)
	}

	copier := ds.Table("dst2").CopierFrom(ds.Table("src"))
	copier.Location = "EU"
	job, err = copier.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	info, _ = s.Job(job.ID())
	if loc := bigquerytest.Dig(info.Request, "jobReference", "location"); loc != "EU" {
		t.Fatalf("job location override not sent: %v", loc)
	}
}

func TestWaitRespectsContext(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "src")
	s.HoldJobs()
	ds := c.Dataset("ds")

	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	job, err := ds.Table("dst").CopierFrom(ds.Table("src")).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = job.Wait(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want deadline exceeded", err)
	}
	if info, _ := s.Job(job.ID()); info.Polls < 1 {
		t.Fatalf("job never polled before deadline: %+v", info)
	}
}
