package bigquerytest

import (
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/eeertekin/gcp/bigquery"
)

func (s *Server) handleListDatasets(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	s.calls = append(s.calls, "LIST datasets")
	names := make([]string, 0, len(s.datasets))
	for name := range s.datasets {
		names = append(names, name)
	}
	slices.Sort(names)
	page, next, ok := s.listPage(w, names, r.URL.Query().Get("pageToken"))
	if !ok {
		return
	}
	items := make([]any, len(page))
	for i, name := range page {
		items[i] = map[string]any{
			"kind":             "bigquery#dataset",
			"id":               ProjectID + ":" + name,
			"datasetReference": map[string]any{"projectId": ProjectID, "datasetId": name},
			"location":         Location,
		}
	}
	out := map[string]any{"kind": "bigquery#datasetList", "datasets": items}
	if next != "" {
		out["nextPageToken"] = next
	}
	json.NewEncoder(w).Encode(out)
}

func (s *Server) handleGetDataset(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	name := r.PathValue("d")
	s.calls = append(s.calls, "GET dataset "+name)
	if !s.datasets[name] {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "notFound", "Not found: Dataset "+ProjectID+":"+name)
		return
	}
	json.NewEncoder(w).Encode(map[string]any{
		"kind":             "bigquery#dataset",
		"id":               ProjectID + ":" + name,
		"datasetReference": map[string]any{"projectId": ProjectID, "datasetId": name},
		"location":         Location,
	})
}

func (s *Server) handleListTables(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	ds := r.PathValue("d")
	s.calls = append(s.calls, "LIST tables "+ds)
	if !s.datasets[ds] {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "notFound", "Not found: Dataset "+ProjectID+":"+ds)
		return
	}
	var names []string
	for key := range s.tables {
		if d, t, _ := strings.Cut(key, "/"); d == ds {
			names = append(names, t)
		}
	}
	slices.Sort(names)
	page, next, ok := s.listPage(w, names, r.URL.Query().Get("pageToken"))
	if !ok {
		return
	}
	items := make([]any, len(page))
	for i, name := range page {
		items[i] = map[string]any{
			"kind":           "bigquery#table",
			"tableReference": map[string]any{"projectId": ProjectID, "datasetId": ds, "tableId": name},
		}
	}
	out := map[string]any{"kind": "bigquery#tableList", "tables": items, "totalItems": len(names)}
	if next != "" {
		out["nextPageToken"] = next
	}
	json.NewEncoder(w).Encode(out)
}

func (s *Server) listPage(w http.ResponseWriter, names []string, token string) (page []string, next string, ok bool) {
	start := 0
	if token != "" {
		n, err := strconv.Atoi(token)
		if err != nil || n < 0 || n > len(names) {
			writeAPIError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid", "bad pageToken "+token)
			return nil, "", false
		}
		start = n
	}
	end := len(names)
	if s.listPageSize > 0 && start+s.listPageSize < end {
		end = start + s.listPageSize
		next = strconv.Itoa(end)
	}
	return names[start:end], next, true
}

func (s *Server) handleQuery(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	var raw map[string]any
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid", err.Error())
		return
	}
	s.queryRequests = append(s.queryRequests, raw)

	if v, ok := raw["useLegacySql"].(bool); !ok || v {
		s.t.Errorf("bigquerytest: query: useLegacySql = %v; the REST default is legacy SQL, so false must be sent", raw["useLegacySql"])
	}
	if Dig(raw, "formatOptions", "useInt64Timestamp") != true {
		s.t.Errorf("bigquerytest: query: formatOptions = %v", raw["formatOptions"])
	}
	if raw["jobCreationMode"] != "JOB_CREATION_OPTIONAL" {
		s.t.Errorf("bigquerytest: query: jobCreationMode = %v", raw["jobCreationMode"])
	}
	reqID, _ := raw["requestId"].(string)
	if reqID == "" {
		s.t.Errorf("bigquerytest: query without requestId")
	}

	if qj, ok := s.byRequestID[reqID]; ok && reqID != "" {
		s.respondQuery(w, qj, "")
		return
	}

	sql, _ := raw["query"].(string)
	s.calls = append(s.calls, "QUERY "+sql)
	res, ok := s.queries[sql]
	if !ok {
		writeAPIError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalidQuery", "Syntax error: Unexpected identifier at [1:1]")
		return
	}
	loc, _ := raw["location"].(string)
	if loc == "" {
		loc = Location
	}
	qj := &queryJob{location: loc, res: res, pollsLeft: res.Polls}
	if res.Polls > 0 || (res.PageSize > 0 && len(res.Rows) > res.PageSize) {
		s.jobSeq++
		qj.id = fmt.Sprintf("job-%d", s.jobSeq)
		s.queryJobs[qj.id] = qj
	}
	s.byRequestID[reqID] = qj
	if qj.pollsLeft > 0 {
		json.NewEncoder(w).Encode(s.incomplete(qj))
		return
	}
	s.respondQuery(w, qj, "")
}

func (s *Server) handleQueryResults(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if !s.authorized(w, r) {
		s.mu.Unlock()
		return
	}
	id, q := r.PathValue("j"), r.URL.Query()
	s.resultRequests = append(s.resultRequests, ResultRequest{JobID: id, Query: q})
	s.calls = append(s.calls, "RESULTS "+id+" page="+q.Get("pageToken"))
	if q.Get("formatOptions.useInt64Timestamp") != "true" {
		s.t.Errorf("bigquerytest: getQueryResults without formatOptions.useInt64Timestamp=true: %v", q)
	}
	qj, ok := s.queryJobs[id]
	if !ok || q.Get("location") != qj.location {
		s.mu.Unlock()
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "notFound", "Not found: Job "+ProjectID+":"+id)
		return
	}
	if qj.cancelled {
		s.mu.Unlock()
		writeAPIError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "stopped", "Job execution was cancelled: User requested cancellation")
		return
	}
	if qj.pollsLeft > 0 {
		qj.pollsLeft--
		hold := s.pollDelay
		if ms, err := strconv.Atoi(q.Get("timeoutMs")); err == nil && time.Duration(ms)*time.Millisecond < hold {
			hold = time.Duration(ms) * time.Millisecond
		}
		out := s.incomplete(qj)
		s.mu.Unlock()
		select {
		case <-time.After(hold):
		case <-r.Context().Done():
			return
		}
		json.NewEncoder(w).Encode(out)
		return
	}
	defer s.mu.Unlock()
	s.respondQuery(w, qj, q.Get("pageToken"))
}

func (s *Server) handleCancelJob(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	id := r.PathValue("j")
	qj, ok := s.queryJobs[id]
	if !ok || r.URL.Query().Get("location") != qj.location {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "notFound", "Not found: Job "+ProjectID+":"+id)
		return
	}
	qj.cancelled = true
	s.cancelled = append(s.cancelled, id)
	json.NewEncoder(w).Encode(map[string]any{
		"kind": "bigquery#jobCancelResponse",
		"job": map[string]any{
			"jobReference": s.jobRef(qj),
			"status":       map[string]any{"state": "DONE"},
		},
	})
}

func (s *Server) jobRef(qj *queryJob) map[string]any {
	if qj.id == "" {
		return map[string]any{"projectId": ProjectID, "location": qj.location}
	}
	return map[string]any{"projectId": ProjectID, "jobId": qj.id, "location": qj.location}
}

func (s *Server) incomplete(qj *queryJob) map[string]any {
	return map[string]any{"kind": "bigquery#getQueryResultsResponse", "jobReference": s.jobRef(qj), "jobComplete": false}
}

func (s *Server) respondQuery(w http.ResponseWriter, qj *queryJob, token string) {
	res := qj.res
	if res.ErrStatus != 0 {
		writeAPIError(w, res.ErrStatus, strings.ToUpper(strings.ReplaceAll(http.StatusText(res.ErrStatus), " ", "_")), res.ErrReason, res.ErrMessage)
		return
	}
	start := 0
	if token != "" {
		n, err := strconv.Atoi(token)
		if err != nil || n <= 0 || n > len(res.Rows) {
			writeAPIError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid", "bad pageToken "+token)
			return
		}
		start = n
	}
	end := len(res.Rows)
	next := ""
	if res.PageSize > 0 && start+res.PageSize < end {
		end = start + res.PageSize
		next = strconv.Itoa(end)
	}
	rows := make([]any, 0, end-start)
	for _, row := range res.Rows[start:end] {
		rows = append(rows, encodeRow(row, res.Schema))
	}
	out := map[string]any{
		"kind":         "bigquery#queryResponse",
		"jobReference": s.jobRef(qj),
		"jobComplete":  true,
		"totalRows":    strconv.Itoa(len(res.Rows)),
		"rows":         rows,
	}
	if res.Schema != nil {
		out["schema"] = map[string]any{"fields": schemaJSON(res.Schema)}
	}
	if res.DMLAffected > 0 {
		out["numDmlAffectedRows"] = strconv.FormatInt(res.DMLAffected, 10)
	}
	if next != "" {
		out["pageToken"] = next
	}
	json.NewEncoder(w).Encode(out)
}

func encodeRow(row []any, schema bigquery.Schema) map[string]any {
	cells := make([]any, len(row))
	for i, v := range row {
		var f *bigquery.FieldSchema
		if i < len(schema) {
			f = schema[i]
		}
		cells[i] = map[string]any{"v": encodeCell(v, f)}
	}
	return map[string]any{"f": cells}
}

func encodeCell(v any, f *bigquery.FieldSchema) any {
	items, ok := v.([]any)
	if v == nil || !ok || f == nil {
		return v
	}
	if f.Repeated {
		elem := *f
		elem.Repeated = false
		out := make([]any, len(items))
		for i, item := range items {
			out[i] = map[string]any{"v": encodeCell(item, &elem)}
		}
		return out
	}
	return encodeRow(items, f.Schema)
}

func schemaJSON(schema bigquery.Schema) []any {
	out := make([]any, len(schema))
	for i, f := range schema {
		m := map[string]any{"name": f.Name, "type": string(f.Type), "mode": "NULLABLE"}
		switch {
		case f.Repeated:
			m["mode"] = "REPEATED"
		case f.Required:
			m["mode"] = "REQUIRED"
		}
		if len(f.Schema) > 0 {
			m["fields"] = schemaJSON(f.Schema)
		}
		if f.MaxLength > 0 {
			m["maxLength"] = strconv.FormatInt(f.MaxLength, 10)
		}
		if f.Precision > 0 {
			m["precision"] = strconv.FormatInt(f.Precision, 10)
		}
		if f.Scale > 0 {
			m["scale"] = strconv.FormatInt(f.Scale, 10)
		}
		if f.RangeElementType != nil {
			m["rangeElementType"] = map[string]any{"type": string(f.RangeElementType.Type)}
		}
		out[i] = m
	}
	return out
}
