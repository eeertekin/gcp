package bigquery

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/eeertekin/gcp/option"
)

const (
	defaultEndpoint = "https://bigquery.googleapis.com/bigquery/v2"
	DetectProjectID = "*detect-project-id*"
)

type Client struct {
	Location string
	Retry    RetryPolicy

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
			return nil, fmt.Errorf("bigquery: read credentials: %w", err)
		}
		creds = b
	}
	if len(creds) == 0 {
		return nil, errors.New("bigquery: credentials required: use option.WithAuthCredentialsJSON or option.WithAuthCredentialsFile")
	}

	var sa serviceAccount
	if err := json.Unmarshal(creds, &sa); err != nil {
		return nil, fmt.Errorf("bigquery: parse credentials: %w", err)
	}
	if s.CredentialsType != "" && string(s.CredentialsType) != sa.Type {
		return nil, fmt.Errorf("bigquery: credentials type %q does not match %q", sa.Type, s.CredentialsType)
	}
	if sa.Type != string(option.ServiceAccount) {
		return nil, fmt.Errorf("bigquery: unsupported credentials type %q", sa.Type)
	}

	hc := &http.Client{Timeout: time.Minute}
	ts, err := newTokenSource(sa, hc)
	if err != nil {
		return nil, err
	}

	if projectID == "" || projectID == DetectProjectID {
		projectID = sa.ProjectID
	}
	if projectID == "" {
		return nil, errors.New("bigquery: project id is required")
	}

	endpoint := strings.TrimSuffix(s.Endpoint, "/")
	if endpoint == "" {
		endpoint = defaultEndpoint
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
	Reason  string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("bigquery: Error %d: %s, %s", e.Code, e.Message, e.Reason)
}

func IsNotFound(err error) bool {
	var e *APIError
	return errors.As(err, &e) && e.Code == http.StatusNotFound
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, in, out any) error {
	_, err := c.send(ctx, method, path, query, in, out)
	return err
}

// send retries transient failures. maybeApplied is true when an earlier failed
// attempt could have been processed by the server before its response was lost.
func (c *Client) send(ctx context.Context, method, path string, query url.Values, in, out any) (maybeApplied bool, err error) {
	var payload []byte
	if in != nil {
		if payload, err = json.Marshal(in); err != nil {
			return false, err
		}
	}

	u := c.endpoint + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	delay := c.Retry.Initial
	for attempt := 1; ; attempt++ {
		data, err := c.attempt(ctx, method, u, payload)
		if err == nil {
			if out != nil && len(data) > 0 {
				return maybeApplied, json.Unmarshal(data, out)
			}
			return maybeApplied, nil
		}

		retry, applied := retryable(err)
		if !retry || attempt >= c.Retry.MaxAttempts || ctx.Err() != nil {
			return maybeApplied, err
		}
		maybeApplied = maybeApplied || applied

		if err := sleep(ctx, jitter(delay)); err != nil {
			return maybeApplied, err
		}
		delay = c.Retry.next(delay)
	}
}

func (c *Client) attempt(ctx context.Context, method, u string, payload []byte) ([]byte, error) {
	var body io.Reader
	if payload != nil {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}

	token, err := c.tokens.Token(ctx)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, &transportError{err: err}
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, &transportError{err: err}
	}
	if resp.StatusCode >= 300 {
		return nil, parseAPIError(resp.StatusCode, data)
	}
	return data, nil
}

func parseAPIError(code int, data []byte) error {
	e := &APIError{Code: code, Message: strings.TrimSpace(string(data))}
	var env struct {
		Error struct {
			Message string `json:"message"`
			Status  string `json:"status"`
			Errors  []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"error"`
	}
	if json.Unmarshal(data, &env) == nil && env.Error.Message != "" {
		e.Message = env.Error.Message
		e.Status = env.Error.Status
		if len(env.Error.Errors) > 0 {
			e.Reason = env.Error.Errors[0].Reason
		}
	}
	return e
}

func randomID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}
