package bigquery

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/eeertekin/gcp/iterator"
)

const queryPollTimeout = 10 * time.Second

var errNoJob = errors.New("bigquery: query is not complete and BigQuery returned no job reference")

type QueryConfig struct {
	Q                string
	DefaultProjectID string
	DefaultDatasetID string
}

type Query struct {
	JobIDConfig
	QueryConfig

	c *Client
}

func (c *Client) Query(q string) *Query {
	return &Query{QueryConfig: QueryConfig{Q: q}, c: c}
}

// Run uses jobs.query with JOB_CREATION_OPTIONAL: a short query can finish
// without BigQuery creating a job, so the returned Job may have an empty ID and
// already hold its first page of rows. The requestId makes retries idempotent.
func (q *Query) Run(ctx context.Context) (*Job, error) {
	if q.JobID != "" || q.AddJobIDSuffix {
		return nil, errors.New("bigquery: Query.Run does not support JobIDConfig.JobID")
	}
	c := q.c
	loc := q.Location
	if loc == "" {
		loc = c.Location
	}
	req := queryRequest{
		Query:           q.Q,
		Location:        loc,
		RequestID:       newRequestID(),
		JobCreationMode: "JOB_CREATION_OPTIONAL",
		TimeoutMs:       c.serverTimeout(ctx).Milliseconds(),
		FormatOptions:   &dataFormatOptions{UseInt64Timestamp: true},
	}
	if q.DefaultDatasetID != "" {
		project := q.DefaultProjectID
		if project == "" {
			project = c.projectID
		}
		req.DefaultDataset = &datasetReference{ProjectID: project, DatasetID: q.DefaultDatasetID}
	}

	var resp queryResponse
	if err := c.do(ctx, http.MethodPost, c.path("", "queries"), nil, req, &resp); err != nil {
		return nil, err
	}
	if !resp.JobComplete && resp.JobReference.JobID == "" {
		return nil, errNoJob
	}
	j := &Job{c: c, query: true, projectID: c.projectID, jobID: resp.JobReference.JobID, location: loc}
	if resp.JobReference.ProjectID != "" {
		j.projectID = resp.JobReference.ProjectID
	}
	if resp.JobReference.Location != "" {
		j.location = resp.JobReference.Location
	}
	if resp.JobComplete {
		j.completeQuery(&resp)
	}
	return j, nil
}

func (c *Client) serverTimeout(ctx context.Context) time.Duration {
	d := queryPollTimeout
	if dl, ok := ctx.Deadline(); ok {
		left := time.Until(dl)
		d = min(d, left-min(left/2, time.Second))
	}
	return max(d, time.Millisecond)
}

func (j *Job) completeQuery(resp *queryResponse) {
	j.first = resp
	j.lastStatus = &JobStatus{
		State:      Done,
		Statistics: &JobStatistics{Details: &QueryStatistics{NumDMLAffectedRows: resp.NumDMLAffectedRows}},
	}
}

func (j *Job) waitQuery(ctx context.Context) (*JobStatus, error) {
	for j.first == nil {
		resp, err := j.getQueryResults(ctx, "", j.c.serverTimeout(ctx))
		if err != nil {
			return nil, err
		}
		if resp.JobComplete {
			j.completeQuery(resp)
		}
	}
	return j.lastStatus, nil
}

func (j *Job) getQueryResults(ctx context.Context, pageToken string, timeout time.Duration) (*queryResponse, error) {
	if j.jobID == "" {
		return nil, errNoJob
	}
	q := url.Values{"formatOptions.useInt64Timestamp": {"true"}}
	if j.location != "" {
		q.Set("location", j.location)
	}
	if pageToken != "" {
		q.Set("pageToken", pageToken)
	}
	if timeout > 0 {
		q.Set("timeoutMs", strconv.FormatInt(timeout.Milliseconds(), 10))
	}
	var resp queryResponse
	if err := j.c.do(ctx, http.MethodGet, j.c.path(j.projectID, "queries", j.jobID), q, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (j *Job) Cancel(ctx context.Context) error {
	if j.jobID == "" {
		return nil
	}
	q := url.Values{}
	if j.location != "" {
		q.Set("location", j.location)
	}
	return j.c.do(ctx, http.MethodPost, j.c.path(j.projectID, "jobs", j.jobID, "cancel"), q, nil, nil)
}

func (j *Job) Read(ctx context.Context) (*RowIterator, error) {
	if !j.query {
		return nil, errors.New("bigquery: Read is only supported for query jobs")
	}
	if _, err := j.waitQuery(ctx); err != nil {
		return nil, err
	}
	if j.first.PageToken != "" && j.jobID == "" {
		return nil, errNoJob
	}
	it := &RowIterator{ctx: ctx, job: j, rows: j.first.Rows}
	if j.first.Schema != nil {
		it.Schema = schemaFromWire(j.first.Schema.Fields)
	}
	if j.first.PageToken != "" {
		it.prefetch(j.first.PageToken)
	}
	return it, nil
}

type pageResult struct {
	resp *queryResponse
	err  error
}

type RowIterator struct {
	Schema Schema

	ctx  context.Context
	job  *Job
	rows []wireRow
	idx  int
	next chan pageResult
	err  error
}

// prefetch requests the next page while the caller consumes the current one.
// The channel is buffered so an abandoned iterator does not leak the goroutine.
func (it *RowIterator) prefetch(token string) {
	ch := make(chan pageResult, 1)
	it.next = ch
	go func() {
		resp, err := it.job.getQueryResults(it.ctx, token, 0)
		ch <- pageResult{resp, err}
	}()
}

func (it *RowIterator) Next(dst any) error {
	out, ok := dst.(*[]Value)
	if !ok {
		return errors.New("bigquery: RowIterator.Next destination must be *[]Value")
	}
	row, err := it.nextRow()
	if err != nil {
		return err
	}
	vals, err := decodeRow(row, it.Schema)
	if err != nil {
		it.err = err
		return err
	}
	*out = vals
	return nil
}

func (it *RowIterator) nextRow() (wireRow, error) {
	if it.err != nil {
		return wireRow{}, it.err
	}
	for it.idx >= len(it.rows) {
		if it.next == nil {
			return wireRow{}, iterator.Done
		}
		var pr pageResult
		select {
		case pr = <-it.next:
		case <-it.ctx.Done():
			pr.err = it.ctx.Err()
		}
		it.next = nil
		if pr.err != nil {
			it.err = pr.err
			return wireRow{}, pr.err
		}
		it.rows, it.idx = pr.resp.Rows, 0
		if pr.resp.PageToken != "" {
			it.prefetch(pr.resp.PageToken)
		}
	}
	row := it.rows[it.idx]
	it.idx++
	return row, nil
}
