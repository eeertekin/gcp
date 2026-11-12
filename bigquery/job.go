package bigquery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

const (
	pollInitial = 100 * time.Millisecond
	pollMax     = 5 * time.Second
)

type JobIDConfig struct {
	JobID          string
	AddJobIDSuffix bool
	Location       string
}

type LoadSource interface {
	populateLoadConfig(*jobConfigurationLoad)
}

type GCSReference struct {
	URIs                []string
	SourceFormat        DataFormat
	AutoDetect          bool
	Schema              Schema
	MaxBadRecords       int64
	IgnoreUnknownValues bool
	AllowJaggedRows     bool
}

func NewGCSReference(uri ...string) *GCSReference {
	return &GCSReference{URIs: uri}
}

func (g *GCSReference) populateLoadConfig(lc *jobConfigurationLoad) {
	lc.SourceURIs = g.URIs
	lc.SourceFormat = g.SourceFormat
	lc.Autodetect = g.AutoDetect
	lc.MaxBadRecords = g.MaxBadRecords
	lc.IgnoreUnknownValues = g.IgnoreUnknownValues
	lc.AllowJaggedRows = g.AllowJaggedRows
	if len(g.Schema) > 0 {
		lc.Schema = &wireSchema{Fields: g.Schema.toWire()}
	}
}

type LoadConfig struct {
	Src               LoadSource
	Dst               *Table
	CreateDisposition TableCreateDisposition
	WriteDisposition  TableWriteDisposition
	TimePartitioning  *TimePartitioning
}

type Loader struct {
	JobIDConfig
	LoadConfig

	c *Client
}

func (t *Table) LoaderFrom(src LoadSource) *Loader {
	return &Loader{LoadConfig: LoadConfig{Src: src, Dst: t}, c: t.c}
}

func (l *Loader) Run(ctx context.Context) (*Job, error) {
	if l.Src == nil || l.Dst == nil {
		return nil, errors.New("bigquery: loader requires Src and Dst")
	}
	lc := &jobConfigurationLoad{
		DestinationTable:  l.Dst.ref(),
		CreateDisposition: l.CreateDisposition,
		WriteDisposition:  l.WriteDisposition,
		TimePartitioning:  l.TimePartitioning.toWire(),
	}
	l.Src.populateLoadConfig(lc)
	return l.c.insertJob(ctx, l.JobIDConfig, jobConfiguration{Load: lc})
}

type CopyConfig struct {
	Srcs              []*Table
	Dst               *Table
	CreateDisposition TableCreateDisposition
	WriteDisposition  TableWriteDisposition
}

type Copier struct {
	JobIDConfig
	CopyConfig

	c *Client
}

func (t *Table) CopierFrom(srcs ...*Table) *Copier {
	return &Copier{CopyConfig: CopyConfig{Srcs: srcs, Dst: t}, c: t.c}
}

func (cp *Copier) Run(ctx context.Context) (*Job, error) {
	if len(cp.Srcs) == 0 || cp.Dst == nil {
		return nil, errors.New("bigquery: copier requires Srcs and Dst")
	}
	cc := &jobConfigurationTableCopy{
		DestinationTable:  cp.Dst.ref(),
		CreateDisposition: cp.CreateDisposition,
		WriteDisposition:  cp.WriteDisposition,
	}
	for _, s := range cp.Srcs {
		cc.SourceTables = append(cc.SourceTables, s.ref())
	}
	return cp.c.insertJob(ctx, cp.JobIDConfig, jobConfiguration{Copy: cc})
}

type Job struct {
	c          *Client
	projectID  string
	jobID      string
	location   string
	lastStatus *JobStatus
}

func (c *Client) insertJob(ctx context.Context, idc JobIDConfig, cfg jobConfiguration) (*Job, error) {
	id := idc.JobID
	switch {
	case id == "":
		id = randomID()
	case idc.AddJobIDSuffix:
		id += "-" + randomID()
	}
	loc := idc.Location
	if loc == "" {
		loc = c.Location
	}

	body := jobResource{
		JobReference:  jobReference{ProjectID: c.projectID, JobID: id, Location: loc},
		Configuration: cfg,
	}
	var out jobResource
	applied, err := c.send(ctx, http.MethodPost, "/projects/"+url.PathEscape(c.projectID)+"/jobs", nil, body, &out)
	if applied && isConflict(err) {
		j := &Job{c: c, projectID: c.projectID, jobID: id, location: loc}
		if _, err := j.Status(ctx); err != nil {
			return nil, err
		}
		return j, nil
	}
	if err != nil {
		return nil, err
	}

	j := &Job{c: c, projectID: c.projectID, jobID: id, location: loc, lastStatus: statusFromResource(out.Status)}
	if out.JobReference.ProjectID != "" {
		j.projectID = out.JobReference.ProjectID
	}
	if out.JobReference.JobID != "" {
		j.jobID = out.JobReference.JobID
	}
	if out.JobReference.Location != "" {
		j.location = out.JobReference.Location
	}
	return j, nil
}

func (j *Job) ID() string {
	return j.jobID
}

func (j *Job) Location() string {
	return j.location
}

func (j *Job) LastStatus() *JobStatus {
	return j.lastStatus
}

func (j *Job) Status(ctx context.Context) (*JobStatus, error) {
	q := url.Values{}
	if j.location != "" {
		q.Set("location", j.location)
	}
	var out jobResource
	path := fmt.Sprintf("/projects/%s/jobs/%s", url.PathEscape(j.projectID), url.PathEscape(j.jobID))
	if err := j.c.do(ctx, http.MethodGet, path, q, nil, &out); err != nil {
		return nil, err
	}
	if j.location == "" {
		j.location = out.JobReference.Location
	}
	j.lastStatus = statusFromResource(out.Status)
	return j.lastStatus, nil
}

func (j *Job) Wait(ctx context.Context) (*JobStatus, error) {
	delay := pollInitial
	for j.lastStatus == nil || !j.lastStatus.Done() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		if _, err := j.Status(ctx); err != nil {
			return nil, err
		}
		delay = min(delay*2, pollMax)
	}
	return j.lastStatus, nil
}
