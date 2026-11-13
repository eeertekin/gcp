package storagetest

import (
	"bytes"
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eeertekin/gcp/option"
	"github.com/eeertekin/gcp/storage"
)

const (
	Bucket           = "test-bucket"
	ProjectID        = "test-project"
	ClientEmail      = "sa@test-project.iam.gserviceaccount.com"
	KeyID            = "kid-1"
	UserClientID     = "user-client-id"
	UserClientSecret = "user-client-secret"
	UserRefreshToken = "user-refresh-token"
	chunkAlign       = 256 << 10
)

type Object struct {
	Data        []byte
	ContentType string
	Generation  int64
	Metadata    map[string]string
	Created     time.Time
	Finalized   time.Time

	epoch    uint64
	statSize int64
}

type session struct {
	bucket      string
	name        string
	contentType string
	metadata    map[string]string
	ifGen       string
	data        []byte
	done        *Object
}

type Fault struct {
	Status     int
	AfterApply bool
	Drop       bool
	// Partial makes a chunk upload persist only half of the sent bytes
	// (aligned to 256 KiB) and answer 308, as GCS is allowed to do.
	Partial bool
}

type Server struct {
	t     testing.TB
	pub   *rsa.PublicKey
	srv   *httptest.Server
	creds []byte

	mu           sync.Mutex
	calls        []string
	tokenCalls   int
	tokenTTL     int
	rejectToken  bool
	lastToken    string
	buckets      map[string]bool
	objects      map[string]*Object
	sessions     map[string]*session
	generation   int64
	attempts     int
	faults       []Fault
	grpcFaults   []GRPCFault
	tokenFaults  []int
	quotaProject string
	pageSize     int
	epoch        uint64
	routing      []string
}

func NewServer(t testing.TB) (*Server, *storage.Client) {
	t.Helper()

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	s := &Server{
		t:        t,
		pub:      &key.PublicKey,
		tokenTTL: 3600,
		buckets:  map[string]bool{Bucket: true},
		objects:  map[string]*Object{},
		sessions: map[string]*session{},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /token", s.handleToken)
	mux.HandleFunc("GET /computeMetadata/v1/project/project-id", s.handleMetadataProject)
	mux.HandleFunc("GET /computeMetadata/v1/instance/service-accounts/default/token", s.handleMetadataToken)
	mux.HandleFunc("GET /storage/v1/b/{b}/o", s.api(s.handleList))
	mux.HandleFunc("GET /storage/v1/b/{b}/o/{o}", s.api(s.handleGet))
	mux.HandleFunc("POST /storage/v1/b/{b}/o/{o}/compose", s.api(s.handleCompose))
	mux.HandleFunc("POST /storage/v1/b/{b}/o/{o}/rewriteTo/b/{db}/o/{do}", s.api(s.handleRewrite))
	mux.HandleFunc("POST /google.storage.v2.Storage/{method}", s.handleGRPC)
	mux.HandleFunc("DELETE /storage/v1/b/{b}/o/{o}", s.api(s.handleDelete))
	mux.HandleFunc("POST /upload/storage/v1/b/{b}/o", s.api(s.handleUpload))
	mux.HandleFunc("PUT /upload/storage/v1/b/{b}/o", s.api(s.handleChunk))
	mux.HandleFunc("GET /{b}/{o...}", s.handleSignedGet)
	mux.HandleFunc("PUT /{b}/{o...}", s.handleSignedPut)
	s.srv = httptest.NewUnstartedServer(mux)
	s.srv.Config.Protocols = new(http.Protocols)
	s.srv.Config.Protocols.SetHTTP1(true)
	s.srv.Config.Protocols.SetUnencryptedHTTP2(true)
	s.srv.Start()
	t.Cleanup(func() {
		s.srv.CloseClientConnections()
		s.srv.Close()
	})

	s.creds = s.credentials(key)

	c, err := storage.NewClient(context.Background(), s.ClientOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	c.Retry = storage.RetryPolicy{MaxAttempts: 5, Initial: time.Millisecond, Max: 10 * time.Millisecond}
	t.Cleanup(func() { c.Close() })
	return s, c
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

func (s *Server) RejectTokens() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rejectToken = true
}

func (s *Server) SetTokenTTL(seconds int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokenTTL = seconds
}

func (s *Server) AddObject(bucket, name string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.store(bucket, name, "", nil, data)
}

func (s *Server) SetPageSize(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pageSize = n
}

func (s *Server) Object(bucket, name string) (Object, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.objects[bucket+"/"+name]
	if !ok {
		return Object{}, false
	}
	return *o, true
}

func (s *Server) Calls() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func (s *Server) Attempts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

func (s *Server) TokenCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokenCalls
}

func (s *Server) Sessions() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions)
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
		if f.Partial {
			s.handlePartialChunk(w, r)
			return
		}
		if f.AfterApply {
			h(httptest.NewRecorder(), r)
		} else {
			io.Copy(io.Discard, r.Body)
		}
		if f.Drop {
			hj, ok := w.(http.Hijacker)
			if !ok {
				s.t.Errorf("storagetest: connection cannot be hijacked")
				return
			}
			if conn, _, err := hj.Hijack(); err == nil {
				conn.Close()
			}
			return
		}
		writeAPIError(w, f.Status, "injected", "injected fault")
	}
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
		s.t.Errorf("storagetest: token: parse form: %s", err)
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	switch gt := r.PostForm.Get("grant_type"); gt {
	case "urn:ietf:params:oauth:grant-type:jwt-bearer":
		if err := s.verifyJWT(r.PostForm.Get("assertion")); err != nil {
			s.t.Errorf("storagetest: token: %s", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	case "refresh_token":
		f := r.PostForm
		if f.Get("client_id") != UserClientID || f.Get("client_secret") != UserClientSecret || f.Get("refresh_token") != UserRefreshToken {
			s.t.Errorf("storagetest: token: bad refresh_token grant %v", f)
			http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
			return
		}
	default:
		s.t.Errorf("storagetest: token: grant_type = %q", gt)
		http.Error(w, `{"error":"unsupported_grant_type"}`, http.StatusBadRequest)
		return
	}
	s.issueToken(w)
}

func (s *Server) issueToken(w http.ResponseWriter) {
	s.lastToken = fmt.Sprintf("tok-%d", s.tokenCalls)
	json.NewEncoder(w).Encode(map[string]any{
		"access_token": s.lastToken,
		"expires_in":   s.tokenTTL,
		"token_type":   "Bearer",
	})
}

func (s *Server) handleMetadataProject(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Metadata-Flavor") != "Google" {
		s.t.Errorf("storagetest: metadata project-id without Metadata-Flavor header")
		http.Error(w, "missing Metadata-Flavor", http.StatusForbidden)
		return
	}
	io.WriteString(w, ProjectID)
}

func (s *Server) handleMetadataToken(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokenCalls++
	if r.Header.Get("Metadata-Flavor") != "Google" {
		s.t.Errorf("storagetest: metadata token without Metadata-Flavor header")
		http.Error(w, "missing Metadata-Flavor", http.StatusForbidden)
		return
	}
	if !strings.Contains(r.URL.Query().Get("scopes"), "https://www.googleapis.com/auth/devstorage.") {
		s.t.Errorf("storagetest: metadata token scopes = %q", r.URL.Query().Get("scopes"))
	}
	s.issueToken(w)
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
	if !strings.Contains(claims.Scope, "https://www.googleapis.com/auth/devstorage.") {
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
	s.quotaProject = r.Header.Get("X-Goog-User-Project")
	got := r.Header.Get("Authorization")
	if s.lastToken == "" || got != "Bearer "+s.lastToken {
		writeAPIError(w, http.StatusUnauthorized, "authError", "bad token "+got)
		return false
	}
	return true
}

func writeAPIError(w http.ResponseWriter, code int, reason, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": msg,
			"errors":  []any{map[string]any{"reason": reason, "message": msg}},
		},
	})
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	key := r.PathValue("b") + "/" + r.PathValue("o")
	s.calls = append(s.calls, "DELETE "+key)
	if !s.exists(w, r, key) || !s.precondition(w, r.URL.Query(), key) {
		return
	}
	delete(s.objects, key)
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) exists(w http.ResponseWriter, r *http.Request, key string) bool {
	o, ok := s.objects[key]
	if g := r.URL.Query().Get("generation"); ok && g != "" && g != strconv.FormatInt(o.Generation, 10) {
		ok = false
	}
	if !ok {
		writeAPIError(w, http.StatusNotFound, "notFound", "No such object: "+key)
	}
	return ok
}

func (s *Server) precondition(w http.ResponseWriter, q url.Values, key string) bool {
	v := q.Get("ifGenerationMatch")
	if v == "" {
		return true
	}
	if !generationMatches(s.objects[key], v) {
		writeAPIError(w, http.StatusPreconditionFailed, "conditionNotMet", "At least one of the pre-conditions you specified did not hold.")
		return false
	}
	return true
}

func generationMatches(o *Object, want string) bool {
	if o == nil {
		return want == "0"
	}
	return want == strconv.FormatInt(o.Generation, 10)
}

func (s *Server) handleUpload(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	bucket := r.PathValue("b")
	name := r.URL.Query().Get("name")
	if !s.buckets[bucket] {
		writeAPIError(w, http.StatusNotFound, "notFound", "The specified bucket does not exist.")
		return
	}
	if name == "" {
		writeAPIError(w, http.StatusBadRequest, "required", "Required parameter: name")
		return
	}

	q := r.URL.Query()
	if !s.precondition(w, q, bucket+"/"+name) {
		return
	}
	switch ut := q.Get("uploadType"); ut {
	case "media":
		s.calls = append(s.calls, "UPLOAD "+bucket+"/"+name)
		obj := s.store(bucket, name, r.Header.Get("Content-Type"), nil, body)
		s.writeObject(w, bucket, name, obj)
	case "multipart":
		meta, data, err := parseMultipart(r.Header.Get("Content-Type"), body)
		if err != nil {
			writeAPIError(w, http.StatusBadRequest, "invalid", err.Error())
			return
		}
		s.calls = append(s.calls, "MULTIPART "+bucket+"/"+name)
		obj := s.store(bucket, name, meta.ContentType, meta.Metadata, data)
		s.writeObject(w, bucket, name, obj)
	case "resumable":
		var meta objectMeta
		if len(body) != 0 {
			if err := json.Unmarshal(body, &meta); err != nil {
				writeAPIError(w, http.StatusBadRequest, "invalid", "bad metadata body: "+err.Error())
				return
			}
		}
		id := randomID()
		s.sessions[id] = &session{bucket: bucket, name: name, contentType: r.Header.Get("X-Upload-Content-Type"), metadata: meta.Metadata, ifGen: q.Get("ifGenerationMatch")}
		s.calls = append(s.calls, "SESSION "+bucket+"/"+name)
		q := url.Values{"uploadType": {"resumable"}, "name": {name}, "upload_id": {id}}
		w.Header().Set("Location", s.srv.URL+"/upload/storage/v1/b/"+url.PathEscape(bucket)+"/o?"+q.Encode())
		w.WriteHeader(http.StatusOK)
	default:
		writeAPIError(w, http.StatusBadRequest, "invalid", "unsupported uploadType "+ut)
	}
}

func (s *Server) store(bucket, name, contentType string, metadata map[string]string, data []byte) *Object {
	s.generation++
	now := time.Now().UTC()
	obj := &Object{Data: data, ContentType: contentType, Generation: s.generation, Metadata: metadata, Created: now, Finalized: now, statSize: int64(len(data))}
	s.objects[bucket+"/"+name] = obj
	return obj
}

type objectMeta struct {
	ContentType string            `json:"contentType"`
	Metadata    map[string]string `json:"metadata"`
}

func parseMultipart(contentType string, body []byte) (objectMeta, []byte, error) {
	var meta objectMeta
	mt, params, err := mime.ParseMediaType(contentType)
	if err != nil || mt != "multipart/related" {
		return meta, nil, fmt.Errorf("multipart upload with Content-Type %q", contentType)
	}
	mr := multipart.NewReader(bytes.NewReader(body), params["boundary"])
	var parts [][]byte
	for {
		p, err := mr.NextPart()
		if err == io.EOF {
			break
		}
		if err != nil {
			return meta, nil, err
		}
		data, err := io.ReadAll(p)
		if err != nil {
			return meta, nil, err
		}
		parts = append(parts, data)
	}
	if len(parts) != 2 {
		return meta, nil, fmt.Errorf("multipart upload has %d parts, want 2", len(parts))
	}
	if err := json.Unmarshal(parts[0], &meta); err != nil {
		return meta, nil, fmt.Errorf("multipart metadata: %w", err)
	}
	return meta, parts[1], nil
}

func objectJSON(bucket, name string, obj *Object) map[string]any {
	m := map[string]any{
		"kind":        "storage#object",
		"bucket":      bucket,
		"name":        name,
		"contentType": obj.ContentType,
		"size":        strconv.FormatInt(obj.size(), 10),
		"generation":  strconv.FormatInt(obj.Generation, 10),
		"timeCreated": obj.Created.Format(time.RFC3339Nano),
	}
	if !obj.Finalized.IsZero() {
		m["timeFinalized"] = obj.Finalized.Format(time.RFC3339Nano)
	}
	if len(obj.Metadata) > 0 {
		m["metadata"] = obj.Metadata
	}
	return m
}

func (o *Object) size() int64 {
	if o.Finalized.IsZero() {
		return o.statSize
	}
	return int64(len(o.Data))
}

func (s *Server) writeObject(w http.ResponseWriter, bucket, name string, obj *Object) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(objectJSON(bucket, name, obj))
}

type contentRange struct {
	first, last int64
	empty       bool
	total       int64
}

func parseContentRange(h string) (contentRange, error) {
	cr := contentRange{total: -1}
	spec, ok := strings.CutPrefix(h, "bytes ")
	if !ok {
		return cr, fmt.Errorf("bad Content-Range %q", h)
	}
	rng, total, ok := strings.Cut(spec, "/")
	if !ok {
		return cr, fmt.Errorf("bad Content-Range %q", h)
	}
	if total != "*" {
		n, err := strconv.ParseInt(total, 10, 64)
		if err != nil {
			return cr, fmt.Errorf("bad Content-Range total %q", h)
		}
		cr.total = n
	}
	if rng == "*" {
		cr.empty = true
		return cr, nil
	}
	a, b, ok := strings.Cut(rng, "-")
	first, err1 := strconv.ParseInt(a, 10, 64)
	last, err2 := strconv.ParseInt(b, 10, 64)
	if !ok || err1 != nil || err2 != nil || last < first {
		return cr, fmt.Errorf("bad Content-Range range %q", h)
	}
	cr.first, cr.last = first, last
	return cr, nil
}

func (s *Server) handleChunk(w http.ResponseWriter, r *http.Request) {
	s.chunk(w, r, false)
}

func (s *Server) handlePartialChunk(w http.ResponseWriter, r *http.Request) {
	s.chunk(w, r, true)
}

func (s *Server) chunk(w http.ResponseWriter, r *http.Request, partial bool) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.authorized(w, r) {
		return
	}
	sess, ok := s.sessions[r.URL.Query().Get("upload_id")]
	if !ok || sess.bucket != r.PathValue("b") || sess.name != r.URL.Query().Get("name") {
		writeAPIError(w, http.StatusNotFound, "notFound", "No such upload session")
		return
	}
	if sess.done != nil {
		s.writeObject(w, sess.bucket, sess.name, sess.done)
		return
	}

	h := r.Header.Get("Content-Range")
	cr, err := parseContentRange(h)
	if err != nil {
		writeAPIError(w, http.StatusBadRequest, "invalid", err.Error())
		return
	}
	s.calls = append(s.calls, "CHUNK "+h)

	if !cr.empty {
		if int64(len(body)) != cr.last-cr.first+1 {
			writeAPIError(w, http.StatusBadRequest, "invalid", fmt.Sprintf("Content-Range %q does not match body length %d", h, len(body)))
			return
		}
		if cr.first != int64(len(sess.data)) {
			writeAPIError(w, http.StatusBadRequest, "invalid", fmt.Sprintf("chunk starts at %d, persisted %d", cr.first, len(sess.data)))
			return
		}
		if cr.total < 0 && len(body)%chunkAlign != 0 {
			writeAPIError(w, http.StatusBadRequest, "invalid", fmt.Sprintf("non-final chunk of %d bytes is not a multiple of 256 KiB", len(body)))
			return
		}
		if cr.total >= 0 && cr.last+1 != cr.total {
			writeAPIError(w, http.StatusBadRequest, "invalid", fmt.Sprintf("final chunk ends at %d, total %d", cr.last+1, cr.total))
			return
		}
		if partial {
			body = body[:len(body)/2/chunkAlign*chunkAlign]
		}
		sess.data = append(sess.data, body...)
	}

	if !partial && cr.total >= 0 && int64(len(sess.data)) == cr.total {
		if sess.ifGen != "" && !generationMatches(s.objects[sess.bucket+"/"+sess.name], sess.ifGen) {
			writeAPIError(w, http.StatusPreconditionFailed, "conditionNotMet", "At least one of the pre-conditions you specified did not hold.")
			return
		}
		sess.done = s.store(sess.bucket, sess.name, sess.contentType, sess.metadata, sess.data)
		s.writeObject(w, sess.bucket, sess.name, sess.done)
		return
	}
	if len(sess.data) > 0 {
		w.Header().Set("Range", fmt.Sprintf("bytes=0-%d", len(sess.data)-1))
	}
	w.WriteHeader(http.StatusPermanentRedirect)
}

func (s *Server) URL() string {
	return s.srv.URL
}

func (s *Server) Credentials() []byte {
	return s.creds
}

func (s *Server) AuthorizedUserJSON() []byte {
	b, _ := json.Marshal(map[string]string{
		"type":             "authorized_user",
		"client_id":        UserClientID,
		"client_secret":    UserClientSecret,
		"refresh_token":    UserRefreshToken,
		"quota_project_id": ProjectID,
		"token_uri":        s.srv.URL + "/token",
	})
	return b
}

// Host is the host:port to use as GCE_METADATA_HOST.
func (s *Server) Host() string {
	return strings.TrimPrefix(s.srv.URL, "http://")
}

// QuotaProject is the X-Goog-User-Project header of the last API request.
func (s *Server) QuotaProject() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.quotaProject
}

func (s *Server) handleSignedGet(w http.ResponseWriter, r *http.Request) {
	if !s.verifySigned(w, r) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := r.PathValue("b") + "/" + r.PathValue("o")
	s.calls = append(s.calls, "SIGNED GET "+key)
	obj, ok := s.objects[key]
	if !ok {
		http.Error(w, "NoSuchKey", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", obj.ContentType)
	w.Write(obj.Data)
}

func (s *Server) handleSignedPut(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return
	}
	if !s.verifySigned(w, r) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	bucket, name := r.PathValue("b"), r.PathValue("o")
	s.calls = append(s.calls, "SIGNED PUT "+bucket+"/"+name)
	if !s.buckets[bucket] {
		http.Error(w, "NoSuchBucket", http.StatusNotFound)
		return
	}
	s.store(bucket, name, r.Header.Get("Content-Type"), nil, body)
}

// verifySigned independently rebuilds the V4 canonical request from what was
// received on the wire and checks it against the service account public key.
func (s *Server) verifySigned(w http.ResponseWriter, r *http.Request) bool {
	fail := func(code int, format string, args ...any) bool {
		msg := fmt.Sprintf(format, args...)
		s.mu.Lock()
		s.calls = append(s.calls, fmt.Sprintf("SIGNED REJECT %d %s", code, msg))
		s.mu.Unlock()
		http.Error(w, msg, code)
		return false
	}

	q := r.URL.Query()
	if a := q.Get("X-Goog-Algorithm"); a != "GOOG4-RSA-SHA256" {
		return fail(http.StatusBadRequest, "algorithm %q", a)
	}
	sig, err := hex.DecodeString(q.Get("X-Goog-Signature"))
	if err != nil || len(sig) == 0 {
		return fail(http.StatusBadRequest, "malformed signature")
	}
	date, err := time.Parse("20060102T150405Z", q.Get("X-Goog-Date"))
	if err != nil {
		return fail(http.StatusBadRequest, "X-Goog-Date: %v", err)
	}
	expires, err := strconv.Atoi(q.Get("X-Goog-Expires"))
	if err != nil || expires <= 0 || expires > 604800 {
		return fail(http.StatusBadRequest, "X-Goog-Expires %q", q.Get("X-Goog-Expires"))
	}
	if time.Now().After(date.Add(time.Duration(expires) * time.Second)) {
		return fail(http.StatusBadRequest, "ExpiredToken")
	}
	scope := date.Format("20060102") + "/auto/storage/goog4_request"
	if c := q.Get("X-Goog-Credential"); c != ClientEmail+"/"+scope {
		return fail(http.StatusForbidden, "credential %q", c)
	}

	signed := strings.Split(q.Get("X-Goog-SignedHeaders"), ";")
	if !slices.Contains(signed, "host") || !slices.IsSorted(signed) {
		return fail(http.StatusBadRequest, "signed headers %q", signed)
	}
	var headers strings.Builder
	for _, name := range signed {
		v := strings.Join(r.Header.Values(name), ",")
		if name == "host" {
			v = r.Host
			if h, _, err := net.SplitHostPort(v); err == nil {
				v = h
			}
		}
		headers.WriteString(name + ":" + strings.Join(strings.Fields(v), " ") + "\n")
	}

	enc := func(s string) string { return strings.ReplaceAll(url.QueryEscape(s), "+", "%20") }
	var pairs []string
	for k, vs := range q {
		if k == "X-Goog-Signature" {
			continue
		}
		for _, v := range vs {
			pairs = append(pairs, enc(k)+"\x00"+enc(v))
		}
	}
	slices.Sort(pairs)
	query := strings.ReplaceAll(strings.Join(pairs, "&"), "\x00", "=")

	path, _, _ := strings.Cut(r.RequestURI, "?")
	payload := "UNSIGNED-PAYLOAD"
	if slices.Contains(signed, "x-goog-content-sha256") {
		payload = r.Header.Get("X-Goog-Content-Sha256")
	}

	canonical := strings.Join([]string{r.Method, path, query, headers.String(), strings.Join(signed, ";"), payload}, "\n")
	sum := sha256.Sum256([]byte(canonical))
	toSign := "GOOG4-RSA-SHA256\n" + q.Get("X-Goog-Date") + "\n" + scope + "\n" + hex.EncodeToString(sum[:])
	digest := sha256.Sum256([]byte(toSign))
	if err := rsa.VerifyPKCS1v15(s.pub, crypto.SHA256, digest[:], sig); err != nil {
		return fail(http.StatusForbidden, "SignatureDoesNotMatch")
	}
	return true
}

func randomID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (s *Server) GRPCClient() *storage.Client {
	c, err := storage.NewGRPCClient(context.Background(), s.ClientOptions()...)
	if err != nil {
		s.t.Fatal(err)
	}
	c.Retry = storage.RetryPolicy{MaxAttempts: 5, Initial: time.Millisecond, Max: 10 * time.Millisecond}
	s.t.Cleanup(func() { c.Close() })
	return c
}
