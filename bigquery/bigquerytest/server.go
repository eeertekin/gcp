package bigquerytest

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eeertekin/gcp/bigquery"
	"github.com/eeertekin/gcp/option"
)

const (
	ProjectID   = "test-project"
	ClientEmail = "sa@test-project.iam.gserviceaccount.com"
	KeyID       = "kid-1"
	Location    = "US"
)

type JobInfo struct {
	ID          string
	Kind        string
	Sources     []string
	Destination string
	Polls       int
	Done        bool
	Request     map[string]any
}

type job struct {
	JobInfo
	result map[string]any
	errors []map[string]any
}

type Server struct {
	t     testing.TB
	pub   *rsa.PublicKey
	srv   *httptest.Server
	creds []byte

	mu          sync.Mutex
	calls       []string
	tokenCalls  int
	tokenTTL    int
	rejectToken bool
	lastToken   string
	tables      map[string]map[string]any
	jobs        map[string]*job
	holdJobs    bool
	loadErrors  []bigquery.Error
	attempts    int
	faults      []Fault
	tokenFaults []int
}

type Fault struct {
	Status     int
	Reason     string
	AfterApply bool
	Drop       bool
}

func NewServer(t testing.TB) (*Server, *bigquery.Client) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	s := &Server{
		t:        t,
		pub:      &key.PublicKey,
		tokenTTL: 3600,
		tables:   map[string]map[string]any{},
		jobs:     map[string]*job{},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /token", s.handleToken)
	mux.HandleFunc("GET /projects/{p}/datasets/{d}/tables/{t}", s.api(s.handleGetTable))
	mux.HandleFunc("DELETE /projects/{p}/datasets/{d}/tables/{t}", s.api(s.handleDeleteTable))
	mux.HandleFunc("POST /projects/{p}/datasets/{d}/tables", s.api(s.handleCreateTable))
	mux.HandleFunc("POST /projects/{p}/jobs", s.api(s.handleInsertJob))
	mux.HandleFunc("GET /projects/{p}/jobs/{j}", s.api(s.handleGetJob))
	s.srv = httptest.NewServer(mux)
	t.Cleanup(s.srv.Close)

	s.creds = s.credentials(key)

	c, err := bigquery.NewClient(context.Background(), ProjectID, s.ClientOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	c.Retry = bigquery.RetryPolicy{MaxAttempts: 5, Initial: time.Millisecond, Max: 10 * time.Millisecond}
	t.Cleanup(func() { c.Close() })
	return s, c
}

func (s *Server) InjectFaults(faults ...Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = append(s.faults, faults...)
}

func (s *Server) FailTokens(statuses ...int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokenFaults = append(s.tokenFaults, statuses...)
}

func (s *Server) Attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func (s *Server) api(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.attempts++
		var f *Fault
		if len(s.faults) > 0 {
			next := s.faults[0]
			s.faults = s.faults[1:]
			f = &next
		}
		s.mu.Unlock()

		if f == nil {
			h(w, r)
			return
		}
		if f.AfterApply {
			h(httptest.NewRecorder(), r)
		}
		if f.Drop {
			hj, ok := w.(http.Hijacker)
			if !ok {
				s.t.Errorf("bigquerytest: connection cannot be hijacked")
				return
			}
			if conn, _, err := hj.Hijack(); err == nil {
				conn.Close()
			}
			return
		}
		writeAPIError(w, f.Status, strings.ToUpper(strings.ReplaceAll(http.StatusText(f.Status), " ", "_")), f.Reason, "injected fault")
	}
}

func (s *Server) ClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithAuthCredentialsJSON(option.ServiceAccount, s.creds),
		option.WithEndpoint(s.srv.URL),
	}
}

func (s *Server) credentials(key *rsa.PrivateKey) []byte {
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		s.t.Fatal(err)
	}
	b, _ := json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     ProjectID,
		"private_key_id": KeyID,
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   ClientEmail,
		"token_uri":      s.srv.URL + "/token",
	})
	return b
}

func (s *Server) AddTable(dataset, table string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables[dataset+"/"+table] = map[string]any{}
}

func (s *Server) SetTable(dataset, table string, raw map[string]any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tables[dataset+"/"+table] = raw
}

func (s *Server) HasTable(dataset, table string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.tables[dataset+"/"+table]
	return ok
}

func (s *Server) Table(dataset, table string) map[string]any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tables[dataset+"/"+table]
}

func (s *Server) Calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func (s *Server) TokenCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokenCalls
}

func (s *Server) SetTokenTTL(seconds int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokenTTL = seconds
}

func (s *Server) RejectTokens() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rejectToken = true
}

func (s *Server) HoldJobs() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.holdJobs = true
}

func (s *Server) FailLoads(errs ...bigquery.Error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.loadErrors = errs
}

func (s *Server) Job(id string) (JobInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return JobInfo{}, false
	}
	return j.JobInfo, true
}

func (s *Server) Jobs() []JobInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]JobInfo, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j.JobInfo)
	}
	return out
}

func Dig(m any, keys ...string) any {
	for _, k := range keys {
		mm, ok := m.(map[string]any)
		if !ok {
			return nil
		}
		m = mm[k]
	}
	return m
}

func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokenCalls++

	if len(s.tokenFaults) > 0 {
		code := s.tokenFaults[0]
		s.tokenFaults = s.tokenFaults[1:]
		http.Error(w, `{"error":"temporarily_unavailable"}`, code)
		return
	}
	if s.rejectToken {
		http.Error(w, `{"error":"invalid_grant","error_description":"Invalid JWT Signature."}`, http.StatusBadRequest)
		return
	}
	if err := r.ParseForm(); err != nil {
		s.t.Errorf("bigquerytest: token: parse form: %s", err)
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	if gt := r.PostForm.Get("grant_type"); gt != "urn:ietf:params:oauth:grant-type:jwt-bearer" {
		s.t.Errorf("bigquerytest: token: grant_type = %q", gt)
	}
	if err := s.verifyJWT(r.PostForm.Get("assertion")); err != nil {
		s.t.Errorf("bigquerytest: token: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	s.lastToken = fmt.Sprintf("tok-%d", s.tokenCalls)
	json.NewEncoder(w).Encode(map[string]any{
		"access_token": s.lastToken,
		"expires_in":   s.tokenTTL,
		"token_type":   "Bearer",
	})
}

func (s *Server) verifyJWT(assertion string) error {
	parts := strings.Split(assertion, ".")
	if len(parts) != 3 {
		return fmt.Errorf("jwt has %d parts", len(parts))
	}
	enc := base64.RawURLEncoding

	sig, err := enc.DecodeString(parts[2])
	if err != nil {
		return fmt.Errorf("jwt signature encoding: %w", err)
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(s.pub, crypto.SHA256, sum[:], sig); err != nil {
		return fmt.Errorf("jwt signature invalid: %w", err)
	}

	var header map[string]string
	hb, _ := enc.DecodeString(parts[0])
	if err := json.Unmarshal(hb, &header); err != nil {
		return fmt.Errorf("jwt header: %w", err)
	}
	if header["alg"] != "RS256" || header["typ"] != "JWT" || header["kid"] != KeyID {
		return fmt.Errorf("jwt header = %v", header)
	}

	var claims struct {
		Iss   string `json:"iss"`
		Scope string `json:"scope"`
		Aud   string `json:"aud"`
		Iat   int64  `json:"iat"`
		Exp   int64  `json:"exp"`
	}
	cb, _ := enc.DecodeString(parts[1])
	if err := json.Unmarshal(cb, &claims); err != nil {
		return fmt.Errorf("jwt claims: %w", err)
	}
	if claims.Iss != ClientEmail {
		return fmt.Errorf("jwt iss = %q", claims.Iss)
	}
	if claims.Aud != s.srv.URL+"/token" {
		return fmt.Errorf("jwt aud = %q", claims.Aud)
	}
	if !strings.Contains(claims.Scope, "https://www.googleapis.com/auth/bigquery") {
		return fmt.Errorf("jwt scope = %q", claims.Scope)
	}
	if claims.Exp-claims.Iat != 3600 {
		return fmt.Errorf("jwt lifetime = %d", claims.Exp-claims.Iat)
	}
	if d := time.Now().Unix() - claims.Iat; d < -5 || d > 5 {
		return fmt.Errorf("jwt iat skew = %d", d)
	}
	return nil
}

func (s *Server) authorized(w http.ResponseWriter, r *http.Request) bool {
	if r.PathValue("p") != ProjectID {
		s.t.Errorf("bigquerytest: %s %s: project = %q", r.Method, r.URL.Path, r.PathValue("p"))
	}
	got := r.Header.Get("Authorization")
	if s.lastToken == "" || got != "Bearer "+s.lastToken {
		writeAPIError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "authError", "bad token "+got)
		return false
	}
	return true
}

func writeAPIError(w http.ResponseWriter, code int, status, reason, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": msg,
			"status":  status,
			"errors":  []any{map[string]any{"reason": reason, "message": msg}},
		},
	})
}

func (s *Server) handleGetTable(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	key := r.PathValue("d") + "/" + r.PathValue("t")
	s.calls = append(s.calls, "GET "+key)
	raw, ok := s.tables[key]
	if !ok {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "notFound", "Not found: Table "+key)
		return
	}
	out := map[string]any{
		"kind":           "bigquery#table",
		"tableReference": map[string]any{"projectId": ProjectID, "datasetId": r.PathValue("d"), "tableId": r.PathValue("t")},
	}
	for k, v := range raw {
		out[k] = v
	}
	json.NewEncoder(w).Encode(out)
}

func (s *Server) handleDeleteTable(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	key := r.PathValue("d") + "/" + r.PathValue("t")
	s.calls = append(s.calls, "DELETE "+key)
	if _, ok := s.tables[key]; !ok {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "notFound", "Not found: Table "+key)
		return
	}
	delete(s.tables, key)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) handleCreateTable(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "application/json" {
		s.t.Errorf("bigquerytest: create table: content-type = %q", ct)
	}

	var raw map[string]any
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		writeAPIError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid", err.Error())
		return
	}
	ref, _ := raw["tableReference"].(map[string]any)
	if ref["projectId"] != ProjectID || ref["datasetId"] != r.PathValue("d") {
		writeAPIError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid", fmt.Sprintf("bad tableReference %v", ref))
		return
	}
	key := tableKey(ref)
	s.calls = append(s.calls, "CREATE "+key)
	if _, ok := s.tables[key]; ok {
		writeAPIError(w, http.StatusConflict, "ALREADY_EXISTS", "duplicate", "Already Exists: Table "+key)
		return
	}
	s.tables[key] = raw
	json.NewEncoder(w).Encode(raw)
}

func tableKey(v any) string {
	m, _ := v.(map[string]any)
	return fmt.Sprintf("%s/%s", m["datasetId"], m["tableId"])
}

func (s *Server) handleInsertJob(w http.ResponseWriter, r *http.Request) {
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
	ref, _ := raw["jobReference"].(map[string]any)
	id, _ := ref["jobId"].(string)
	if id == "" || ref["projectId"] != ProjectID {
		writeAPIError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid", fmt.Sprintf("bad jobReference %v", ref))
		return
	}
	if _, ok := s.jobs[id]; ok {
		writeAPIError(w, http.StatusConflict, "ALREADY_EXISTS", "duplicate", "Already Exists: Job "+id)
		return
	}

	j := &job{JobInfo: JobInfo{ID: id, Request: raw}}
	switch {
	case Dig(raw, "configuration", "load") != nil:
		j.Kind = "LOAD"
		j.Destination = tableKey(Dig(raw, "configuration", "load", "destinationTable"))
		s.calls = append(s.calls, "LOAD "+j.Destination)
	case Dig(raw, "configuration", "copy") != nil:
		j.Kind = "COPY"
		srcs, _ := Dig(raw, "configuration", "copy", "sourceTables").([]any)
		if len(srcs) == 0 {
			writeAPIError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid", "copy requires sourceTables")
			return
		}
		for _, src := range srcs {
			j.Sources = append(j.Sources, tableKey(src))
		}
		j.Destination = tableKey(Dig(raw, "configuration", "copy", "destinationTable"))
		s.calls = append(s.calls, "COPY "+strings.Join(j.Sources, ",")+"->"+j.Destination)
	default:
		writeAPIError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "invalid", "unsupported job configuration")
		return
	}
	s.jobs[id] = j
	json.NewEncoder(w).Encode(s.jobJSON(j))
}

func (s *Server) handleGetJob(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	id := r.PathValue("j")
	j, ok := s.jobs[id]
	if loc := r.URL.Query().Get("location"); !ok || (loc != "" && loc != Location) {
		writeAPIError(w, http.StatusNotFound, "NOT_FOUND", "notFound", "Not found: Job "+id)
		return
	}

	j.Polls++
	if !j.Done && !s.holdJobs {
		s.finish(j)
	}
	json.NewEncoder(w).Encode(s.jobJSON(j))
}

func (s *Server) finish(j *job) {
	j.Done = true
	switch j.Kind {
	case "LOAD":
		dst, ok := s.tables[j.Destination]
		if !ok {
			j.result = map[string]any{"reason": "notFound", "message": "Not found: Table " + j.Destination}
			j.errors = []map[string]any{j.result}
			return
		}
		if want := Dig(j.Request, "configuration", "load", "timePartitioning"); want != nil {
			if have, got := partitionSpec(dst["timePartitioning"]), partitionSpec(want); have != got {
				j.result = map[string]any{"reason": "invalid", "message": "Incompatible table partitioning specification. Expects partitioning specification " + have + ", but input partitioning specification is " + got}
				j.errors = []map[string]any{j.result}
				return
			}
		}
		if len(s.loadErrors) > 0 {
			j.result = map[string]any{"reason": "invalid", "message": "Error while reading data"}
			for _, e := range s.loadErrors {
				j.errors = append(j.errors, map[string]any{"reason": e.Reason, "location": e.Location, "message": e.Message})
			}
		}
	case "COPY":
		for _, src := range j.Sources {
			if _, ok := s.tables[src]; !ok {
				j.result = map[string]any{"reason": "notFound", "message": "Not found: Table " + src}
				j.errors = []map[string]any{j.result}
				return
			}
		}
		s.tables[j.Destination] = s.tables[j.Sources[0]]
	}
}

func partitionSpec(tp any) string {
	if tp == nil {
		return "none"
	}
	typ, _ := Dig(tp, "type").(string)
	if typ == "" {
		typ = "DAY"
	}
	field, _ := Dig(tp, "field").(string)
	return fmt.Sprintf("interval(type:%s,field:%s)", strings.ToLower(typ), field)
}

func (s *Server) jobJSON(j *job) map[string]any {
	status := map[string]any{"state": "RUNNING"}
	if j.Done {
		status["state"] = "DONE"
		if j.result != nil {
			status["errorResult"] = j.result
			status["errors"] = j.errors
		}
	}
	return map[string]any{
		"kind":          "bigquery#job",
		"jobReference":  map[string]any{"projectId": ProjectID, "jobId": j.ID, "location": Location},
		"configuration": j.Request["configuration"],
		"status":        status,
	}
}
