package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/eeertekin/gcp/option"
)

const (
	defaultEndpoint = "https://pubsub.googleapis.com"
	DetectProjectID = "*detect-project-id*"
)

var ErrEmptyProjectID = errors.New("pubsub: projectID string is empty")

type Client struct {
	Retry RetryPolicy

	projectID string
	endpoint  string
	http      *http.Client
	tokens    *tokenSource
}

func NewClient(ctx context.Context, projectID string, opts ...option.ClientOption) (*Client, error) {
	var s option.Settings
	for _, o := range opts {
		o(&s)
	}

	creds := s.CredentialsJSON
	if len(creds) == 0 && s.CredentialsFile != "" {
		b, err := os.ReadFile(s.CredentialsFile)
		if err != nil {
			return nil, fmt.Errorf("pubsub: read credentials: %w", err)
		}
		creds = b
	}
	if len(creds) == 0 {
		return nil, errors.New("pubsub: credentials required: use option.WithAuthCredentialsJSON or option.WithAuthCredentialsFile")
	}

	var sa serviceAccount
	if err := json.Unmarshal(creds, &sa); err != nil {
		return nil, fmt.Errorf("pubsub: parse credentials: %w", err)
	}
	if s.CredentialsType != "" && string(s.CredentialsType) != sa.Type {
		return nil, fmt.Errorf("pubsub: credentials type %q does not match %q", sa.Type, s.CredentialsType)
	}
	if sa.Type != string(option.ServiceAccount) {
		return nil, fmt.Errorf("pubsub: unsupported credentials type %q", sa.Type)
	}

	if projectID == DetectProjectID {
		projectID = sa.ProjectID
	}
	if projectID == "" {
		return nil, ErrEmptyProjectID
	}

	// Pulls are long polls, so deadlines are set per request instead of on the client.
	hc := &http.Client{}
	ts, err := newTokenSource(sa, hc)
	if err != nil {
		return nil, err
	}

	endpoint := strings.TrimSuffix(s.Endpoint, "/")
	if endpoint == "" {
		endpoint = defaultEndpoint
	} else if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}

	return &Client{
		Retry:     defaultRetryPolicy(),
		projectID: projectID,
		endpoint:  endpoint,
		http:      hc,
		tokens:    ts,
	}, nil
}

func (c *Client) Project() string {
	return c.projectID
}

func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

type APIError struct {
	Code    int
	Status  string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("pubsub: Error %d: %s, %s", e.Code, e.Message, e.Status)
}

func parseAPIError(code int, data []byte) error {
	e := &APIError{Code: code, Message: strings.TrimSpace(string(data))}
	var env struct {
		Error struct {
			Message string `json:"message"`
			Status  string `json:"status"`
		} `json:"error"`
	}
	if json.Unmarshal(data, &env) == nil && env.Error.Message != "" {
		e.Message = env.Error.Message
		e.Status = env.Error.Status
	}
	return e
}

func resourcePath(name string) string {
	segs := strings.Split(name, "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return strings.Join(segs, "/")
}

// call sends a JSON request, retrying transient failures until the policy's
// attempts are used up or ctx is done.
func (c *Client) call(ctx context.Context, name, verb string, in, out any) error {
	payload, err := json.Marshal(in)
	if err != nil {
		return err
	}
	u := c.endpoint + "/v1/" + resourcePath(name) + ":" + verb

	delay := c.Retry.Initial
	for attempt := 1; ; attempt++ {
		err := c.attempt(ctx, u, payload, out)
		if err == nil || !retryable(err) || attempt >= c.Retry.MaxAttempts || ctx.Err() != nil {
			return err
		}
		if err := sleep(ctx, jitter(delay)); err != nil {
			return err
		}
		delay = c.Retry.next(delay)
	}
}

func (c *Client) attempt(ctx context.Context, u string, payload []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	token, err := c.tokens.Token(ctx)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return &transportError{err: err}
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return &transportError{err: err}
	}
	if resp.StatusCode >= 300 {
		return parseAPIError(resp.StatusCode, data)
	}
	if out != nil && len(data) > 0 {
		return json.Unmarshal(data, out)
	}
	return nil
}
