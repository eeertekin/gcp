package bigquery_test

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/eeertekin/gcp/bigquery"
	"github.com/eeertekin/gcp/bigquery/bigquerytest"
	"github.com/eeertekin/gcp/civil"
	"github.com/eeertekin/gcp/googleapi"
	"github.com/eeertekin/gcp/iterator"
)

func readAll(t *testing.T, it *bigquery.RowIterator) ([][]bigquery.Value, error) {
	t.Helper()
	var out [][]bigquery.Value
	for {
		var row []bigquery.Value
		err := it.Next(&row)
		if errors.Is(err, iterator.Done) {
			return out, nil
		}
		if err != nil {
			return out, err
		}
		out = append(out, row)
	}
}

func intRows(n int) [][]any {
	rows := make([][]any, n)
	for i := range rows {
		rows[i] = []any{strconv.Itoa(i + 1)}
	}
	return rows
}

func TestQueryFastPath(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.SetQuery("SELECT x FROM events", &bigquerytest.QueryResult{
		Schema: bigquery.Schema{{Name: "x", Type: bigquery.IntegerFieldType}},
		Rows:   intRows(2),
	})
	ctx := context.Background()

	q := c.Query("SELECT x FROM events")
	q.DefaultProjectID = c.Project()
	q.DefaultDatasetID = "analytics"
	job, err := q.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID() != "" {
		t.Fatalf("job id %q: BigQuery created no job for this query", job.ID())
	}
	status, err := job.Wait(ctx)
	if err != nil || !status.Done() || status.Err() != nil {
		t.Fatalf("status %+v err %v", status, err)
	}
	it, err := job.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := readAll(t, it)
	if err != nil || !reflect.DeepEqual(rows, [][]bigquery.Value{{int64(1)}, {int64(2)}}) {
		t.Fatalf("rows %v err %v", rows, err)
	}
	if err := job.Cancel(ctx); err != nil {
		t.Fatalf("cancel without a job: %v", err)
	}

	if calls := s.Calls(); !reflect.DeepEqual(calls, []string{"QUERY SELECT x FROM events"}) {
		t.Fatalf("calls %v: a completed jobs.query needs no further round trips", calls)
	}
	reqs := s.QueryRequests()
	if len(reqs) != 1 {
		t.Fatalf("%d query requests", len(reqs))
	}
	req := reqs[0]
	if dd := bigquerytest.Dig(req, "defaultDataset"); !reflect.DeepEqual(dd, map[string]any{"projectId": bigquerytest.ProjectID, "datasetId": "analytics"}) {
		t.Errorf("defaultDataset %v", dd)
	}
	if id, _ := req["requestId"].(string); !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(id) {
		t.Errorf("requestId %q is not a UUIDv4", id)
	}
	if ms, _ := req["timeoutMs"].(float64); ms != 10000 {
		t.Errorf("timeoutMs %v, want 10000 without a deadline", req["timeoutMs"])
	}
	if _, ok := req["location"]; ok {
		t.Errorf("location sent without one configured: %v", req["location"])
	}
}

func TestQueryPollsThenPagesWithPrefetch(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.SetQuery("SELECT x", &bigquerytest.QueryResult{
		Schema:   bigquery.Schema{{Name: "x", Type: bigquery.IntegerFieldType}},
		Rows:     intRows(8),
		PageSize: 3,
		Polls:    2,
	})
	c.Location = "EU"
	ctx := context.Background()

	job, err := c.Query("SELECT x").Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID() != "job-1" || job.Location() != "EU" {
		t.Fatalf("job %q in %q", job.ID(), job.Location())
	}
	if _, err := job.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	it, err := job.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(it.Schema) != 1 || it.Schema[0].Name != "x" || it.Schema[0].Type != bigquery.IntegerFieldType {
		t.Fatalf("schema %+v", it.Schema)
	}
	rows, err := readAll(t, it)
	if err != nil || len(rows) != 8 {
		t.Fatalf("%d rows, err %v", len(rows), err)
	}
	for i, r := range rows {
		if r[0] != int64(i+1) {
			t.Fatalf("row %d = %v", i, r)
		}
	}
	var row []bigquery.Value
	if err := it.Next(&row); !errors.Is(err, iterator.Done) {
		t.Fatalf("Next after Done: %v", err)
	}

	if loc := bigquerytest.Dig(s.QueryRequests()[0], "location"); loc != "EU" {
		t.Errorf("jobs.query location %v", loc)
	}
	var pages []string
	for i, r := range s.ResultRequests() {
		if r.JobID != "job-1" || r.Query.Get("location") != "EU" {
			t.Errorf("request %d: job %q location %q", i, r.JobID, r.Query.Get("location"))
		}
		polling := r.Query.Get("pageToken") == ""
		if ms, _ := strconv.Atoi(r.Query.Get("timeoutMs")); polling != (ms > 0 && ms <= 10000) {
			t.Errorf("request %d: timeoutMs %q; only polls for completion long-poll", i, r.Query.Get("timeoutMs"))
		}
		pages = append(pages, r.Query.Get("pageToken"))
	}
	if want := []string{"", "", "", "3", "6"}; !reflect.DeepEqual(pages, want) {
		t.Fatalf("getQueryResults pages %q, want %q (2 incomplete polls, completion, then each page once)", pages, want)
	}
}

func TestQueryWaitHonoursDeadlineThenCancel(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.SetPollDelay(5 * time.Second)
	s.SetQuery("SELECT slow", &bigquerytest.QueryResult{Schema: bigquery.Schema{{Name: "x", Type: bigquery.IntegerFieldType}}, Polls: 1000})

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	job, err := c.Query("SELECT slow").Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := job.Wait(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait err %v, want deadline exceeded", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Wait returned after %v; server-side long polls must stay inside the caller's deadline", d)
	}
	if ms, _ := s.QueryRequests()[0]["timeoutMs"].(float64); ms <= 0 || ms >= 300 {
		t.Errorf("jobs.query timeoutMs %v, want inside the 300ms deadline", ms)
	}
	for _, r := range s.ResultRequests() {
		if ms, _ := strconv.Atoi(r.Query.Get("timeoutMs")); ms <= 0 || ms >= 300 {
			t.Errorf("getQueryResults timeoutMs %q, want inside the 300ms deadline", r.Query.Get("timeoutMs"))
		}
	}

	if err := job.Cancel(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := s.Cancelled(); !reflect.DeepEqual(got, []string{"job-1"}) {
		t.Fatalf("cancelled %v", got)
	}
	_, err = job.Wait(context.Background())
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) || apiErr.Code != http.StatusBadRequest || apiErr.Errors[0].Reason != "stopped" {
		t.Fatalf("Wait after cancel: %v", err)
	}
}

func TestQueryRetryReusesRequestID(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.SetQuery("SELECT 1", &bigquerytest.QueryResult{Schema: bigquery.Schema{{Name: "x", Type: bigquery.IntegerFieldType}}, Rows: intRows(1)})
	s.InjectFaults(bigquerytest.Fault{Status: http.StatusServiceUnavailable, AfterApply: true, Match: "POST /projects/test-project/queries"})
	ctx := context.Background()

	job, err := c.Query("SELECT 1").Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	it, err := job.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rows, err := readAll(t, it); err != nil || len(rows) != 1 {
		t.Fatalf("rows %v err %v", rows, err)
	}
	reqs := s.QueryRequests()
	if len(reqs) != 2 || reqs[0]["requestId"] != reqs[1]["requestId"] {
		t.Fatalf("retried jobs.query must resend the same requestId: %v", reqs)
	}
	if n := s.QueryRuns(); n != 1 {
		t.Fatalf("query executed %d times, want 1", n)
	}

	if _, err := c.Query("SELECT 1").Run(ctx); err != nil {
		t.Fatal(err)
	}
	if reqs := s.QueryRequests(); reqs[2]["requestId"] == reqs[0]["requestId"] {
		t.Fatal("a new Run must use a new requestId")
	}
}

func TestQueryPageFailure(t *testing.T) {
	for _, tc := range []struct {
		name   string
		faults int
		rows   int
	}{
		{"retried", 1, 5},
		{"surfaced after retries", 5, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, c := bigquerytest.NewServer(t)
			s.SetQuery("SELECT x", &bigquerytest.QueryResult{
				Schema:   bigquery.Schema{{Name: "x", Type: bigquery.IntegerFieldType}},
				Rows:     intRows(5),
				PageSize: 2,
			})
			for range tc.faults {
				s.InjectFaults(bigquerytest.Fault{Status: http.StatusServiceUnavailable, Match: "pageToken=4"})
			}
			ctx := context.Background()
			job, err := c.Query("SELECT x").Run(ctx)
			if err != nil {
				t.Fatal(err)
			}
			it, err := job.Read(ctx)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := readAll(t, it)
			if len(rows) != tc.rows {
				t.Fatalf("got %d rows before the error, want %d", len(rows), tc.rows)
			}
			attempts := s.Attempts() - 2
			for _, r := range s.ResultRequests() {
				if p := r.Query.Get("pageToken"); p != "2" && p != "4" {
					t.Fatalf("unexpected getQueryResults page %q", p)
				}
			}
			if tc.faults < c.Retry.MaxAttempts {
				if err != nil || attempts != tc.faults+1 {
					t.Fatalf("err %v after %d attempts", err, attempts)
				}
				return
			}
			var apiErr *googleapi.Error
			if !errors.As(err, &apiErr) || apiErr.Code != http.StatusServiceUnavailable || attempts != c.Retry.MaxAttempts {
				t.Fatalf("err %v after %d attempts", err, attempts)
			}
			var row []bigquery.Value
			if again := it.Next(&row); again != err {
				t.Fatalf("iterator error is not sticky: %v", again)
			}
		})
	}
}

func TestQueryErrors(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.SetQuery("SELECT secret", &bigquerytest.QueryResult{ErrStatus: 403, ErrReason: "accessDenied", ErrMessage: "Access Denied: Table p:d.secret"})
	s.SetQuery("SELECT later", &bigquerytest.QueryResult{Polls: 1, ErrStatus: 400, ErrReason: "invalidQuery", ErrMessage: "Division by zero"})
	ctx := context.Background()

	reasonOf := func(err error) (int, string, string) {
		var apiErr *googleapi.Error
		if !errors.As(err, &apiErr) || len(apiErr.Errors) == 0 {
			t.Fatalf("not a googleapi error with a reason: %v", err)
		}
		return apiErr.Code, apiErr.Errors[0].Reason, apiErr.Message
	}

	_, err := c.Query("SELEC typo").Run(ctx)
	if code, r, _ := reasonOf(err); code != 400 || r != "invalidQuery" {
		t.Errorf("syntax error: %d %s", code, r)
	}
	_, err = c.Query("SELECT secret").Run(ctx)
	if code, r, msg := reasonOf(err); code != 403 || r != "accessDenied" || msg != "Access Denied: Table p:d.secret" {
		t.Errorf("access denied: %d %s %s", code, r, msg)
	}

	job, err := c.Query("SELECT later").Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, err = job.Wait(ctx)
	if code, r, msg := reasonOf(err); code != 400 || r != "invalidQuery" || msg != "Division by zero" {
		t.Errorf("job failure: %d %s %s", code, r, msg)
	}
	if _, err := job.Read(ctx); err == nil {
		t.Error("Read of a failed job succeeded")
	}

	q := c.Query("SELECT 1")
	q.JobID = "mine"
	if _, err := q.Run(ctx); err == nil || !strings.HasPrefix(err.Error(), "bigquery: ") {
		t.Errorf("JobID on Query: %v", err)
	}
	if n := s.QueryRuns(); n != 2 {
		t.Errorf("%d queries reached the server, want 2", n)
	}
}

func TestQueryDMLStatistics(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.SetQuery("DELETE FROM t WHERE true", &bigquerytest.QueryResult{DMLAffected: 12, Polls: 1})
	ctx := context.Background()

	job, err := c.Query("DELETE FROM t WHERE true").Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, err := job.Wait(ctx)
	if err != nil {
		t.Fatal(err)
	}
	qs, ok := status.Statistics.Details.(*bigquery.QueryStatistics)
	if !ok || qs.NumDMLAffectedRows != 12 {
		t.Fatalf("statistics %+v", status.Statistics)
	}
	it, err := job.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var row []bigquery.Value
	if len(it.Schema) != 0 || !errors.Is(it.Next(&row), iterator.Done) {
		t.Fatalf("DML result has schema %v", it.Schema)
	}
	if got := job.LastStatus(); got != status {
		t.Fatal("LastStatus differs from Wait result")
	}
}

func TestQueryDecodesValues(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	schema := bigquery.Schema{
		{Name: "i", Type: bigquery.IntegerFieldType, Required: true},
		{Name: "i64", Type: "INT64"},
		{Name: "f", Type: bigquery.FloatFieldType},
		{Name: "b", Type: bigquery.BooleanFieldType},
		{Name: "s", Type: bigquery.StringFieldType, MaxLength: 10},
		{Name: "by", Type: bigquery.BytesFieldType},
		{Name: "n", Type: bigquery.NumericFieldType, Precision: 10, Scale: 2},
		{Name: "bn", Type: bigquery.BigNumericFieldType},
		{Name: "d", Type: bigquery.DateFieldType},
		{Name: "t", Type: bigquery.TimeFieldType},
		{Name: "dt", Type: bigquery.DateTimeFieldType},
		{Name: "ts", Type: bigquery.TimestampFieldType},
		{Name: "iv", Type: bigquery.IntervalFieldType},
		{Name: "r", Type: bigquery.RangeFieldType, RangeElementType: &bigquery.RangeElementType{Type: bigquery.DateFieldType}},
		{Name: "j", Type: bigquery.JSONFieldType},
		{Name: "g", Type: bigquery.GeographyFieldType},
		{Name: "arr", Type: bigquery.IntegerFieldType, Repeated: true},
		{Name: "rec", Type: bigquery.RecordFieldType, Schema: bigquery.Schema{
			{Name: "name", Type: bigquery.StringFieldType},
			{Name: "tags", Type: bigquery.RecordFieldType, Repeated: true, Schema: bigquery.Schema{{Name: "k", Type: bigquery.StringFieldType}}},
		}},
	}
	ts := time.Date(2026, 3, 4, 5, 6, 7, 123456000, time.UTC)
	full := []any{
		"-7", "9007199254740993", "1.5", "true", "héllo \"q\"", "AAH/", "1.25", "0.123456789012345678901234567890123456789",
		"2026-01-02", "23:59:59.999999", "2026-12-31T08:00:00.000005", strconv.FormatInt(ts.UnixMicro(), 10),
		"-1-2 3 -4:5:6.5", "[2026-01-01, UNBOUNDED)", `{"k":[1,2]}`, "POINT(1 2)",
		[]any{"1", "2"}, []any{"Al", []any{[]any{"x"}, []any{nil}}},
	}
	nulls := make([]any, len(schema))
	nulls[0] = "0"
	nulls[16] = []any{}
	s.SetQuery("SELECT *", &bigquerytest.QueryResult{Schema: schema, Rows: [][]any{full, nulls}})
	ctx := context.Background()

	job, err := c.Query("SELECT *").Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	it, err := job.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if f := it.Schema[6]; f.Precision != 10 || f.Scale != 2 || it.Schema[4].MaxLength != 10 || !it.Schema[0].Required ||
		it.Schema[13].RangeElementType == nil || it.Schema[13].RangeElementType.Type != bigquery.DateFieldType ||
		!it.Schema[16].Repeated || len(it.Schema[17].Schema) != 2 || !it.Schema[17].Schema[1].Repeated {
		t.Fatalf("schema attributes lost: %+v", it.Schema)
	}
	rows, err := readAll(t, it)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows %v err %v", rows, err)
	}

	bn, _ := new(big.Rat).SetString("0.123456789012345678901234567890123456789")
	want := []bigquery.Value{
		int64(-7), int64(9007199254740993), 1.5, true, "héllo \"q\"", []byte{0, 1, 0xff}, big.NewRat(5, 4), bn,
		civil.Date{Year: 2026, Month: 1, Day: 2}, civil.Time{Hour: 23, Minute: 59, Second: 59, Nanosecond: 999999000},
		civil.DateTime{Date: civil.Date{Year: 2026, Month: 12, Day: 31}, Time: civil.Time{Hour: 8, Nanosecond: 5000}}, ts,
		&bigquery.IntervalValue{Years: -1, Months: -2, Days: 3, Hours: -4, Minutes: -5, Seconds: -6, SubSecondNanos: -500000000},
		&bigquery.RangeValue{Start: civil.Date{Year: 2026, Month: 1, Day: 1}},
		`{"k":[1,2]}`, "POINT(1 2)",
		[]bigquery.Value{int64(1), int64(2)},
		[]bigquery.Value{"Al", []bigquery.Value{[]bigquery.Value{"x"}, []bigquery.Value{nil}}},
	}
	for i, got := range rows[0] {
		w := want[i]
		switch w := w.(type) {
		case *big.Rat:
			if r, ok := got.(*big.Rat); !ok || r.Cmp(w) != 0 {
				t.Errorf("%s: %#v, want %s", schema[i].Name, got, w.RatString())
			}
		case time.Time:
			if g, ok := got.(time.Time); !ok || !g.Equal(w) || g.Location() != time.UTC {
				t.Errorf("%s: %#v, want %v UTC", schema[i].Name, got, w)
			}
		default:
			if !reflect.DeepEqual(got, w) {
				t.Errorf("%s: %#v, want %#v", schema[i].Name, got, w)
			}
		}
	}
	for i, got := range rows[1] {
		switch i {
		case 0:
			if got != int64(0) {
				t.Errorf("required i = %#v", got)
			}
		case 16:
			if !reflect.DeepEqual(got, []bigquery.Value{}) {
				t.Errorf("empty array = %#v", got)
			}
		default:
			if got != nil {
				t.Errorf("%s: NULL decoded as %#v", schema[i].Name, got)
			}
		}
	}

	if s := bigquery.NumericString(big.NewRat(5, 4)); s != "1.250000000" {
		t.Errorf("NumericString %q", s)
	}
	if s := bigquery.BigNumericString(big.NewRat(1, 3)); s != "0.33333333333333333333333333333333333333" {
		t.Errorf("BigNumericString %q", s)
	}
	if s := rows[0][12].(*bigquery.IntervalValue).String(); s != "-1-2 3 -4:5:6.5" {
		t.Errorf("IntervalValue.String %q", s)
	}
}

func TestQueryDecodeErrorIsSticky(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.SetQuery("SELECT bad", &bigquerytest.QueryResult{
		Schema: bigquery.Schema{{Name: "n", Type: bigquery.IntegerFieldType}},
		Rows:   [][]any{{"1"}, {"abc"}, {"3"}},
	})
	ctx := context.Background()
	job, err := c.Query("SELECT bad").Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	it, err := job.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := readAll(t, it)
	if len(rows) != 1 || err == nil || !strings.Contains(err.Error(), "bigquery: field n") {
		t.Fatalf("rows %v err %v", rows, err)
	}
	var row []bigquery.Value
	if again := it.Next(&row); again != err {
		t.Fatalf("decode error not sticky: %v", again)
	}
	var m map[string]bigquery.Value
	if err := it.Next(&m); err == nil {
		t.Fatal("unsupported destination accepted")
	}
}

func TestReadRequiresQueryJob(t *testing.T) {
	s, c := bigquerytest.NewServer(t)
	s.AddTable("ds", "src")
	ctx := context.Background()
	job, err := c.Dataset("ds").Table("dst").CopierFrom(c.Dataset("ds").Table("src")).Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := job.Read(ctx); err == nil || !strings.HasPrefix(err.Error(), "bigquery: ") {
		t.Fatalf("Read on a copy job: %v", err)
	}
	if status, err := job.Wait(ctx); err != nil || status.Err() != nil || status.Statistics != nil {
		t.Fatalf("copy job Wait: %+v %v", status, err)
	}
}
