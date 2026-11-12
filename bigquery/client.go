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
	"strings"
	"time"

	"github.com/eeertekin/gcp/googleapi"
	"github.com/eeertekin/gcp/option"
)

const (
	defaultEndpoint = "https://bigquery.googleapis.com/bigquery/v2"
	DetectProjectID = "*detect-project-id*"
)

type Client struct {
	Location string
	Retry    RetryPolicy

	projectID    string
	quotaProject string
	endpoint     string
	http         *http.Client
	tokens       *tokenSource
}

func NewClient(ctx context.Context, projectID string, opts ...option.ClientOption) (*Client, error) {
	var s option.Settings
	for _, o := range opts {
		o(&s)
	}

	hc := s.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: time.Minute}
	}
	creds, err := findCredentials(ctx, s, hc)
	if err != nil {
		return nil, err
	}

	if projectID == "" || projectID == DetectProjectID {
		projectID = creds.projectID
	}
	if projectID == "" {
		return nil, errors.New("bigquery: project id is required")
	}

	endpoint := strings.TrimSuffix(s.Endpoint, "/")
	if endpoint == "" {
		endpoint = defaultEndpoint
	} else if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}

	return &Client{
		Retry:        defaultRetryPolicy(),
		projectID:    projectID,
		quotaProject: creds.quotaProject,
		endpoint:     endpoint,
		http:         hc,
		tokens:       creds.tokens,
	}, nil
}

func (c *Client) Project() string {
	return c.projectID
}

func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

func IsNotFound(err error) bool {
	var e *googleapi.Error
	return errors.As(err, &e) && e.Code == http.StatusNotFound
}

func reason(e *googleapi.Error) string {
	if len(e.Errors) > 0 {
		return e.Errors[0].Reason
	}
	return ""
}

func (c *Client) path(project string, parts ...string) string {
	if project == "" {
		project = c.projectID
	}
	p := "/projects/" + url.PathEscape(project)
	for _, part := range parts {
		p += "/" + url.PathEscape(part)
	}
	return p
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
				if err := json.Unmarshal(data, out); err != nil {
					return maybeApplied, fmt.Errorf("bigquery: decode response: %w", err)
				}
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
	if c.quotaProject != "" {
		req.Header.Set("X-Goog-User-Project", c.quotaProject)
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
		return nil, parseAPIError(resp.StatusCode, resp.Header, data)
	}
	return data, nil
}

func parseAPIError(code int, header http.Header, data []byte) error {
	e := &googleapi.Error{Code: code, Body: string(data), Header: header}
	var env struct {
		Error struct {
			Message string                `json:"message"`
			Errors  []googleapi.ErrorItem `json:"errors"`
		} `json:"error"`
	}
	if json.Unmarshal(data, &env) == nil && env.Error.Message != "" {
		e.Message = env.Error.Message
		e.Errors = env.Error.Errors
		return e
	}
	e.Message = strings.TrimSpace(string(data))
	if e.Message == "" {
		e.Message = http.StatusText(code)
	}
	return e
}

func randomID() string {
	b := make([]byte, 12)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func newRequestID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}
