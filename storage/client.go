package storage

import (
	"bytes"
	"context"
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/eeertekin/gcp/option"
)

const defaultEndpoint = "https://storage.googleapis.com"

var ErrObjectNotExist = errors.New("storage: object doesn't exist")

type Client struct {
	Retry RetryPolicy

	endpoint     string
	http         *http.Client
	tokens       *tokenSource
	quotaProject string
	email        string
	key          *rsa.PrivateKey
	grpc         *grpcClient
}

func NewClient(ctx context.Context, opts ...option.ClientOption) (*Client, error) {
	var s option.Settings
	for _, o := range opts {
		o(&s)
	}

	hc := s.HTTPClient
	if hc == nil {
		// No overall client timeout: large chunk uploads may legitimately take long.
		tr := http.DefaultTransport.(*http.Transport).Clone()
		tr.ResponseHeaderTimeout = time.Minute
		hc = &http.Client{Transport: tr}
	}

	creds, err := findCredentials(ctx, s, hc)
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
		Retry:        defaultRetryPolicy(),
		endpoint:     endpoint,
		http:         hc,
		tokens:       creds.tokens,
		quotaProject: creds.quotaProject,
		email:        creds.email,
		key:          creds.key,
	}, nil
}

func (c *Client) Close() error {
	c.http.CloseIdleConnections()
	return nil
}

type APIError struct {
	Code    int
	Reason  string
	Message string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("storage: Error %d: %s, %s", e.Code, e.Message, e.Reason)
}

func isNotFound(err error) bool {
	var e *APIError
	return errors.As(err, &e) && e.Code == http.StatusNotFound
}

func parseAPIError(code int, data []byte) error {
	e := &APIError{Code: code, Message: strings.TrimSpace(string(data))}
	var env struct {
		Error struct {
			Message string `json:"message"`
			Errors  []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"error"`
	}
	if json.Unmarshal(data, &env) == nil && env.Error.Message != "" {
		e.Message = env.Error.Message
		if len(env.Error.Errors) > 0 {
			e.Reason = env.Error.Errors[0].Reason
		}
	}
	return e
}

type response struct {
	code   int
	header http.Header
	body   []byte
}

func isSuccess(code int) bool { return code >= 200 && code < 300 }

// call retries transient failures. maybeApplied is true when an earlier failed
// attempt could have been processed by the server before its response was lost.
func (c *Client) call(ctx context.Context, method, u string, header http.Header, body []byte) (resp *response, maybeApplied bool, err error) {
	delay := c.Retry.Initial
	for attempt := 1; ; attempt++ {
		resp, err := c.roundTrip(ctx, method, u, header, body)
		if err == nil && !isSuccess(resp.code) {
			err = parseAPIError(resp.code, resp.body)
		}
		if err == nil {
			return resp, maybeApplied, nil
		}

		retry, applied := retryable(err)
		if !retry || attempt >= c.Retry.MaxAttempts || ctx.Err() != nil {
			return nil, maybeApplied, err
		}
		maybeApplied = maybeApplied || applied

		if err := sleep(ctx, jitter(delay)); err != nil {
			return nil, maybeApplied, err
		}
		delay = c.Retry.next(delay)
	}
}

func (c *Client) newRequest(ctx context.Context, method, u string, header http.Header, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, u, body)
	if err != nil {
		return nil, err
	}

	token, err := c.tokens.Token(ctx)
	if err != nil {
		return nil, err
	}
	for k, v := range header {
		req.Header[k] = v
	}
	req.Header.Set("Authorization", "Bearer "+token)
	if c.quotaProject != "" {
		req.Header.Set("X-Goog-User-Project", c.quotaProject)
	}
	return req, nil
}

func (c *Client) roundTrip(ctx context.Context, method, u string, header http.Header, body []byte) (*response, error) {
	req, err := c.newRequest(ctx, method, u, header, bytes.NewReader(body))
	if err != nil {
		return nil, err
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
	return &response{code: resp.StatusCode, header: resp.Header, body: data}, nil
}
