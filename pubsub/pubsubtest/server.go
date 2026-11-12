package pubsubtest

import (
	"bytes"
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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eeertekin/gcp/option"
	"github.com/eeertekin/gcp/pubsub"
)

const (
	ProjectID        = "test-project"
	ClientEmail      = "sa@test-project.iam.gserviceaccount.com"
	KeyID            = "kid-1"
	UserClientID     = "user-client-id"
	UserClientSecret = "user-client-secret"
	UserRefreshToken = "user-refresh-token"
)

type Message struct {
	ID          string
	Data        []byte
	Attributes  map[string]string
	OrderingKey string
	PublishTime time.Time
}

type SubscriptionConfig struct {
	AckDeadline time.Duration
	DeadLetter  bool
}

// Fault is consumed by the first API call whose verb matches Verb ("" matches
// any verb: publish, pull, acknowledge, modifyAckDeadline).
type Fault struct {
	Verb       string
	Status     int
	State      string
	AfterApply bool
	Drop       bool
}

type delivery struct {
	msg         Message
	ackID       string
	attempts    int
	leasedUntil time.Time
}

type subscription struct {
	topic      string
	deadline   time.Duration
	deadLetter bool
	queue      []*delivery
	acked      []string
}

type Server struct {
	t     testing.TB
	pub   *rsa.PublicKey
	srv   *httptest.Server
	creds []byte

	mu           sync.Mutex
	tokenCalls   int
	lastToken    string
	quotaProject string
	topics       map[string][][]Message
	subs         map[string]*subscription
	nextID       int
	calls        map[string]int
	faults       []Fault
	hold         chan struct{}
	inflight     int
	maxInflight  int
	keyInflight  map[string]int
	maxPerKey    int
	pullWait     time.Duration
	notify       chan struct{}
}

var signingKey = sync.OnceValues(func() (*rsa.PrivateKey, error) {
	return rsa.GenerateKey(rand.Reader, 2048)
})

func NewServer(t testing.TB) (*Server, *pubsub.Client) {
	t.Helper()
	key, err := signingKey()
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{
		t:           t,
		pub:         &key.PublicKey,
		topics:      map[string][][]Message{},
		subs:        map[string]*subscription{},
		calls:       map[string]int{},
		keyInflight: map[string]int{},
		pullWait:    50 * time.Millisecond,
		notify:      make(chan struct{}),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /token", s.handleToken)
	mux.HandleFunc("GET /computeMetadata/v1/project/project-id", s.handleMetadataProject)
	mux.HandleFunc("GET /computeMetadata/v1/instance/service-accounts/default/token", s.handleMetadataToken)
	mux.HandleFunc("POST /v1/projects/{p}/topics/{rest}", s.api(s.handlePublish))
	mux.HandleFunc("POST /v1/projects/{p}/subscriptions/{rest}", s.api(s.handleSubscription))
	s.srv = httptest.NewServer(mux)
	t.Cleanup(s.srv.Close)

	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	s.creds, _ = json.Marshal(map[string]string{
		"type":           "service_account",
		"project_id":     ProjectID,
		"private_key_id": KeyID,
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   ClientEmail,
		"token_uri":      s.srv.URL + "/token",
	})

	c, err := pubsub.NewClient(context.Background(), pubsub.DetectProjectID, s.ClientOptions()...)
	if err != nil {
		t.Fatal(err)
	}
	c.Retry = pubsub.RetryPolicy{MaxAttempts: 5, Initial: time.Millisecond, Max: 10 * time.Millisecond}
	t.Cleanup(func() { c.Close() })
	return s, c
}

func (s *Server) ClientOptions() []option.ClientOption {
	return []option.ClientOption{
		option.WithAuthCredentialsJSON(option.ServiceAccount, s.creds),
		option.WithEndpoint(s.srv.URL),
	}
}

func (s *Server) URL() string {
	return s.srv.URL
}

// Host is the host:port to use as GCE_METADATA_HOST.
func (s *Server) Host() string {
	return strings.TrimPrefix(s.srv.URL, "http://")
}

func (s *Server) ServiceAccountJSON() []byte {
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

// QuotaProject is the X-Goog-User-Project header of the last API request.
func (s *Server) QuotaProject() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.quotaProject
}

func topicName(id string) string { return "projects/" + ProjectID + "/topics/" + id }
func subName(id string) string   { return "projects/" + ProjectID + "/subscriptions/" + id }

func (s *Server) CreateTopic(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topics[topicName(id)] = nil
}

func (s *Server) CreateSubscription(id, topicID string, cfg SubscriptionConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cfg.AckDeadline <= 0 {
		cfg.AckDeadline = 10 * time.Second
	}
	s.subs[subName(id)] = &subscription{topic: topicName(topicID), deadline: cfg.AckDeadline, deadLetter: cfg.DeadLetter}
}

func (s *Server) SetPullWait(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pullWait = d
}

func (s *Server) InjectFaults(faults ...Fault) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.faults = append(s.faults, faults...)
}

// HoldPublish blocks publish requests until the returned release is called.
func (s *Server) HoldPublish() (release func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := make(chan struct{})
	s.hold = ch
	var once sync.Once
	release = func() {
		once.Do(func() {
			s.mu.Lock()
			if s.hold == ch {
				s.hold = nil
			}
			s.mu.Unlock()
			close(ch)
		})
	}
	s.t.Cleanup(release)
	return release
}

// Publish stores messages directly, bypassing the client.
func (s *Server) Publish(topicID string, msgs ...Message) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids, err := s.store(topicName(topicID), msgs)
	if err != nil {
		s.t.Fatal(err)
	}
	return ids
}

func (s *Server) Batches(topicID string) [][]Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([][]Message(nil), s.topics[topicName(topicID)]...)
}

func (s *Server) Acked(subID string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.subs[subName(subID)].acked...)
}

func (s *Server) Unacked(subID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.subs[subName(subID)].queue)
}

// Available reports unacked messages that are not leased to a subscriber.
func (s *Server) Available(subID string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	n := 0
	for _, d := range s.subs[subName(subID)].queue {
		if !now.Before(d.leasedUntil) {
			n++
		}
	}
	return n
}

func (s *Server) Calls(verb string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls[verb]
}

func (s *Server) TokenCalls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tokenCalls
}

func (s *Server) MaxConcurrentPublishes() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxInflight
}

func (s *Server) MaxConcurrentPublishesPerKey() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxPerKey
}

func (s *Server) api(h func(w http.ResponseWriter, r *http.Request, name, verb string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rest := r.PathValue("rest")
		i := strings.LastIndex(rest, ":")
		if i < 0 {
			writeError(w, http.StatusNotFound, "NOT_FOUND", "unknown method")
			return
		}
		kind := "topics"
		if strings.Contains(r.URL.Path, "/subscriptions/") {
			kind = "subscriptions"
		}
		name := "projects/" + r.PathValue("p") + "/" + kind + "/" + rest[:i]
		verb := rest[i+1:]

		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
		if verb == "publish" {
			defer s.trackPublish(body)()
		}

		s.mu.Lock()
		s.calls[verb]++
		s.quotaProject = r.Header.Get("X-Goog-User-Project")
		authorized := s.lastToken != "" && r.Header.Get("Authorization") == "Bearer "+s.lastToken
		var f *Fault
		for j, cand := range s.faults {
			if cand.Verb == "" || cand.Verb == verb {
				f = &cand
				s.faults = append(s.faults[:j:j], s.faults[j+1:]...)
				break
			}
		}
		s.mu.Unlock()

		if !authorized {
			writeError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "bad token")
			return
		}
		if r.PathValue("p") != ProjectID {
			s.t.Errorf("pubsubtest: %s: project %q", r.URL.Path, r.PathValue("p"))
		}
		if f == nil {
			h(w, r, name, verb)
			return
		}
		if f.AfterApply {
			h(httptest.NewRecorder(), r, name, verb)
		}
		if f.Drop {
			if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
				conn.Close()
			}
			return
		}
		writeError(w, f.Status, f.State, "injected fault")
	}
}

func writeError(w http.ResponseWriter, code int, status, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]any{"code": code, "message": msg, "status": status},
	})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

type wireMessage struct {
	Data        string            `json:"data,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	OrderingKey string            `json:"orderingKey,omitempty"`
	MessageID   string            `json:"messageId,omitempty"`
	PublishTime string            `json:"publishTime,omitempty"`
}

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request, name, verb string) {
	if verb != "publish" {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "unknown method "+verb)
		return
	}
	var req struct {
		Messages []wireMessage `json:"messages"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	msgs := make([]Message, len(req.Messages))
	for i, m := range req.Messages {
		data, err := base64.StdEncoding.DecodeString(m.Data)
		if err != nil {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "data is not base64")
			return
		}
		if len(data) == 0 && len(m.Attributes) == 0 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "message must contain data or attributes")
			return
		}
		msgs[i] = Message{Data: data, Attributes: m.Attributes, OrderingKey: m.OrderingKey}
	}

	s.mu.Lock()
	ids, err := s.store(name, msgs)
	s.mu.Unlock()
	if err != nil {
		writeError(w, http.StatusNotFound, "NOT_FOUND", err.Error())
		return
	}
	writeJSON(w, map[string]any{"messageIds": ids})
}

// trackPublish records publish concurrency (overall and per ordering key) and
// blocks while HoldPublish is active. The returned func ends the tracking.
func (s *Server) trackPublish(body []byte) func() {
	var req struct {
		Messages []wireMessage `json:"messages"`
	}
	json.Unmarshal(body, &req)
	keys := map[string]bool{}
	for _, m := range req.Messages {
		if m.OrderingKey != "" {
			keys[m.OrderingKey] = true
		}
	}

	s.mu.Lock()
	hold := s.hold
	s.inflight++
	s.maxInflight = max(s.maxInflight, s.inflight)
	for k := range keys {
		s.keyInflight[k]++
		s.maxPerKey = max(s.maxPerKey, s.keyInflight[k])
	}
	s.mu.Unlock()
	if hold != nil {
		<-hold
	}
	return func() {
		s.mu.Lock()
		s.inflight--
		for k := range keys {
			s.keyInflight[k]--
		}
		s.mu.Unlock()
	}
}

func (s *Server) store(topic string, msgs []Message) ([]string, error) {
	if _, ok := s.topics[topic]; !ok {
		return nil, fmt.Errorf("Resource not found (resource=%s)", topic)
	}
	if len(msgs) == 0 {
		return nil, fmt.Errorf("no messages")
	}
	ids := make([]string, len(msgs))
	now := time.Now().UTC()
	for i := range msgs {
		s.nextID++
		msgs[i].ID = fmt.Sprintf("%d", s.nextID)
		msgs[i].PublishTime = now
		ids[i] = msgs[i].ID
		for _, sub := range s.subs {
			if sub.topic == topic {
				sub.queue = append(sub.queue, &delivery{msg: msgs[i]})
			}
		}
	}
	s.topics[topic] = append(s.topics[topic], msgs)
	s.wake()
	return ids, nil
}

func (s *Server) wake() {
	close(s.notify)
	s.notify = make(chan struct{})
}

func (s *Server) handleSubscription(w http.ResponseWriter, r *http.Request, name, verb string) {
	var req struct {
		MaxMessages        int      `json:"maxMessages"`
		AckIDs             []string `json:"ackIds"`
		AckDeadlineSeconds *int     `json:"ackDeadlineSeconds"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", err.Error())
		return
	}

	s.mu.Lock()
	sub, ok := s.subs[name]
	s.mu.Unlock()
	if !ok {
		writeError(w, http.StatusNotFound, "NOT_FOUND", "Resource not found (resource="+name+")")
		return
	}

	switch verb {
	case "pull":
		if req.MaxMessages <= 0 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "maxMessages must be positive")
			return
		}
		writeJSON(w, map[string]any{"receivedMessages": s.pull(r.Context(), sub, req.MaxMessages)})
	case "acknowledge":
		s.mu.Lock()
		for _, id := range req.AckIDs {
			for i, d := range sub.queue {
				if d.ackID == id {
					sub.acked = append(sub.acked, d.msg.ID)
					sub.queue = append(sub.queue[:i], sub.queue[i+1:]...)
					break
				}
			}
		}
		s.mu.Unlock()
		writeJSON(w, map[string]any{})
	case "modifyAckDeadline":
		if req.AckDeadlineSeconds == nil || *req.AckDeadlineSeconds < 0 || *req.AckDeadlineSeconds > 600 {
			writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "ackDeadlineSeconds must be within [0, 600]")
			return
		}
		until := time.Now().Add(time.Duration(*req.AckDeadlineSeconds) * time.Second)
		if *req.AckDeadlineSeconds == 0 {
			until = time.Time{}
		}
		s.mu.Lock()
		for _, id := range req.AckIDs {
			for _, d := range sub.queue {
				if d.ackID == id && time.Now().Before(d.leasedUntil) {
					d.leasedUntil = until
				}
			}
		}
		s.wake()
		s.mu.Unlock()
		writeJSON(w, map[string]any{})
	default:
		writeError(w, http.StatusNotFound, "NOT_FOUND", "unknown method "+verb)
	}
}

type receivedMessage struct {
	AckID           string      `json:"ackId"`
	Message         wireMessage `json:"message"`
	DeliveryAttempt int         `json:"deliveryAttempt,omitempty"`
}

func (s *Server) pull(ctx context.Context, sub *subscription, n int) []receivedMessage {
	deadline := time.NewTimer(s.pullWaitDuration())
	defer deadline.Stop()
	for {
		s.mu.Lock()
		now := time.Now()
		var out []receivedMessage
		for _, d := range sub.queue {
			if len(out) == n {
				break
			}
			if now.Before(d.leasedUntil) {
				continue
			}
			d.attempts++
			d.ackID = fmt.Sprintf("ack-%s-%d", d.msg.ID, d.attempts)
			d.leasedUntil = now.Add(sub.deadline)
			rm := receivedMessage{AckID: d.ackID, Message: wireMessage{
				Data:        base64.StdEncoding.EncodeToString(d.msg.Data),
				Attributes:  d.msg.Attributes,
				OrderingKey: d.msg.OrderingKey,
				MessageID:   d.msg.ID,
				PublishTime: d.msg.PublishTime.Format(time.RFC3339Nano),
			}}
			if sub.deadLetter {
				rm.DeliveryAttempt = d.attempts
			}
			out = append(out, rm)
		}
		notify := s.notify
		s.mu.Unlock()
		if len(out) > 0 {
			return out
		}
		select {
		case <-ctx.Done():
			return nil
		case <-deadline.C:
			return nil
		case <-notify:
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (s *Server) pullWaitDuration() time.Duration {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pullWait
}

func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokenCalls++
	if err := r.ParseForm(); err != nil {
		http.Error(w, "bad form", http.StatusBadRequest)
		return
	}
	switch gt := r.PostForm.Get("grant_type"); gt {
	case "urn:ietf:params:oauth:grant-type:jwt-bearer":
		if err := s.verifyJWT(r.PostForm.Get("assertion")); err != nil {
			s.t.Errorf("pubsubtest: token: %s", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	case "refresh_token":
		f := r.PostForm
		if f.Get("client_id") != UserClientID || f.Get("client_secret") != UserClientSecret || f.Get("refresh_token") != UserRefreshToken {
			s.t.Errorf("pubsubtest: token: bad refresh_token grant %v", f)
			http.Error(w, `{"error":"invalid_grant"}`, http.StatusBadRequest)
			return
		}
	default:
		s.t.Errorf("pubsubtest: token: grant_type = %q", gt)
		http.Error(w, `{"error":"unsupported_grant_type"}`, http.StatusBadRequest)
		return
	}
	s.issueToken(w)
}

func (s *Server) issueToken(w http.ResponseWriter) {
	s.lastToken = fmt.Sprintf("tok-%d", s.tokenCalls)
	writeJSON(w, map[string]any{"access_token": s.lastToken, "expires_in": 3600, "token_type": "Bearer"})
}

func (s *Server) handleMetadataProject(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Metadata-Flavor") != "Google" {
		s.t.Errorf("pubsubtest: metadata project-id without Metadata-Flavor header")
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
		s.t.Errorf("pubsubtest: metadata token without Metadata-Flavor header")
		http.Error(w, "missing Metadata-Flavor", http.StatusForbidden)
		return
	}
	if r.URL.Query().Get("scopes") != pubsub.ScopePubSub {
		s.t.Errorf("pubsubtest: metadata token scopes = %q", r.URL.Query().Get("scopes"))
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
		return err
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(s.pub, crypto.SHA256, sum[:], sig); err != nil {
		return fmt.Errorf("jwt signature invalid: %w", err)
	}
	var claims struct {
		Iss   string `json:"iss"`
		Scope string `json:"scope"`
		Aud   string `json:"aud"`
	}
	cb, _ := enc.DecodeString(parts[1])
	if err := json.Unmarshal(cb, &claims); err != nil {
		return err
	}
	if claims.Iss != ClientEmail || claims.Aud != s.srv.URL+"/token" || claims.Scope != pubsub.ScopePubSub {
		return fmt.Errorf("jwt claims = %+v", claims)
	}
	return nil
}
