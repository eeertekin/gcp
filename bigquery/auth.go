package bigquery

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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/eeertekin/gcp/option"
)

const (
	defaultTokenURI     = "https://oauth2.googleapis.com/token"
	tokenScope          = "https://www.googleapis.com/auth/bigquery https://www.googleapis.com/auth/cloud-platform"
	defaultMetadataHost = "metadata.google.internal"
	metadataProbeLimit  = 3 * time.Second
)

type credentialsFile struct {
	Type           string `json:"type"`
	ProjectID      string `json:"project_id"`
	PrivateKeyID   string `json:"private_key_id"`
	PrivateKey     string `json:"private_key"`
	ClientEmail    string `json:"client_email"`
	TokenURI       string `json:"token_uri"`
	ClientID       string `json:"client_id"`
	ClientSecret   string `json:"client_secret"`
	RefreshToken   string `json:"refresh_token"`
	QuotaProjectID string `json:"quota_project_id"`
}

type credentials struct {
	projectID    string
	quotaProject string
	tokens       *tokenSource
}

// findCredentials follows Application Default Credentials order: explicit
// option, GOOGLE_APPLICATION_CREDENTIALS, the gcloud ADC file, then the
// metadata server.
func findCredentials(ctx context.Context, s option.Settings, hc *http.Client) (*credentials, error) {
	switch {
	case len(s.CredentialsJSON) > 0:
		return parseCredentials(s.CredentialsJSON, s.CredentialsType, hc)
	case s.CredentialsFile != "":
		data, err := os.ReadFile(s.CredentialsFile)
		if err != nil {
			return nil, fmt.Errorf("bigquery: read credentials: %w", err)
		}
		return parseCredentials(data, s.CredentialsType, hc)
	}

	if path := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS"); path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("bigquery: GOOGLE_APPLICATION_CREDENTIALS: %w", err)
		}
		return parseCredentials(data, "", hc)
	}

	if path := gcloudCredentialsPath(); path != "" {
		data, err := os.ReadFile(path)
		if err == nil {
			return parseCredentials(data, "", hc)
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("bigquery: read %s: %w", path, err)
		}
	}

	host := os.Getenv("GCE_METADATA_HOST")
	if host == "" {
		host = defaultMetadataHost
	}
	project, err := metadataProjectID(ctx, hc, host)
	if err != nil {
		return nil, fmt.Errorf("bigquery: no credentials: set GOOGLE_APPLICATION_CREDENTIALS, run `gcloud auth application-default login`, or run on Google Cloud (metadata server: %w)", err)
	}
	u := "http://" + host + "/computeMetadata/v1/instance/service-accounts/default/token?scopes=" +
		url.QueryEscape(strings.ReplaceAll(tokenScope, " ", ","))
	return &credentials{
		projectID: project,
		tokens: &tokenSource{tokenURI: u, http: hc, request: func(ctx context.Context) (*http.Request, error) {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
			if err == nil {
				req.Header.Set("Metadata-Flavor", "Google")
			}
			return req, err
		}},
	}, nil
}

func gcloudCredentialsPath() string {
	dir := os.Getenv("CLOUDSDK_CONFIG")
	if dir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		dir = filepath.Join(home, ".config", "gcloud")
	}
	return filepath.Join(dir, "application_default_credentials.json")
}

func metadataProjectID(ctx context.Context, hc *http.Client, host string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, metadataProbeLimit)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+host+"/computeMetadata/v1/project/project-id", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<10))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("project-id: %s", resp.Status)
	}
	return strings.TrimSpace(string(body)), nil
}

func parseCredentials(data []byte, want option.CredentialsType, hc *http.Client) (*credentials, error) {
	var f credentialsFile
	if err := json.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("bigquery: parse credentials: %w", err)
	}
	if want != "" && string(want) != f.Type {
		return nil, fmt.Errorf("bigquery: credentials type %q does not match %q", f.Type, want)
	}
	tokenURI := f.TokenURI
	if tokenURI == "" {
		tokenURI = defaultTokenURI
	}

	switch option.CredentialsType(f.Type) {
	case option.ServiceAccount:
		if f.ClientEmail == "" {
			return nil, errors.New("bigquery: credentials missing client_email")
		}
		block, _ := pem.Decode([]byte(f.PrivateKey))
		if block == nil {
			return nil, errors.New("bigquery: credentials private_key is not valid PEM")
		}
		key, err := parseRSAKey(block.Bytes)
		if err != nil {
			return nil, err
		}
		ts := &tokenSource{tokenURI: tokenURI, http: hc}
		ts.request = func(ctx context.Context) (*http.Request, error) {
			assertion, err := signJWT(key, f.PrivateKeyID, f.ClientEmail, tokenURI, time.Now())
			if err != nil {
				return nil, err
			}
			return tokenForm(ctx, tokenURI, url.Values{
				"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
				"assertion":  {assertion},
			})
		}
		return &credentials{projectID: f.ProjectID, tokens: ts}, nil

	case option.AuthorizedUser:
		if f.ClientID == "" || f.ClientSecret == "" || f.RefreshToken == "" {
			return nil, errors.New("bigquery: authorized_user credentials need client_id, client_secret and refresh_token")
		}
		ts := &tokenSource{tokenURI: tokenURI, http: hc}
		ts.request = func(ctx context.Context) (*http.Request, error) {
			return tokenForm(ctx, tokenURI, url.Values{
				"grant_type":    {"refresh_token"},
				"client_id":     {f.ClientID},
				"client_secret": {f.ClientSecret},
				"refresh_token": {f.RefreshToken},
			})
		}
		return &credentials{projectID: f.QuotaProjectID, quotaProject: f.QuotaProjectID, tokens: ts}, nil
	}
	return nil, fmt.Errorf("bigquery: unsupported credentials type %q", f.Type)
}

func parseRSAKey(der []byte) (*rsa.PrivateKey, error) {
	if k, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		rk, ok := k.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("bigquery: private key is not RSA")
		}
		return rk, nil
	}
	k, err := x509.ParsePKCS1PrivateKey(der)
	if err != nil {
		return nil, fmt.Errorf("bigquery: parse private key: %w", err)
	}
	return k, nil
}

func tokenForm(ctx context.Context, tokenURI string, form url.Values) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURI, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req, nil
}

func signJWT(key *rsa.PrivateKey, keyID, email, aud string, now time.Time) (string, error) {
	header, err := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": keyID})
	if err != nil {
		return "", err
	}
	claims, err := json.Marshal(map[string]any{
		"iss":   email,
		"scope": tokenScope,
		"aud":   aud,
		"iat":   now.Unix(),
		"exp":   now.Add(time.Hour).Unix(),
	})
	if err != nil {
		return "", err
	}

	enc := base64.RawURLEncoding
	unsigned := enc.EncodeToString(header) + "." + enc.EncodeToString(claims)
	sum := sha256.Sum256([]byte(unsigned))
	sig, err := rsa.SignPKCS1v15(rand.Reader, key, crypto.SHA256, sum[:])
	if err != nil {
		return "", fmt.Errorf("bigquery: sign jwt: %w", err)
	}
	return unsigned + "." + enc.EncodeToString(sig), nil
}

type tokenError struct {
	code int
	err  error
}

func (e *tokenError) Error() string { return e.err.Error() }
func (e *tokenError) Unwrap() error { return e.err }

type tokenSource struct {
	tokenURI string
	http     *http.Client
	request  func(ctx context.Context) (*http.Request, error)

	mu     sync.Mutex
	token  string
	expiry time.Time
}

func (ts *tokenSource) Token(ctx context.Context) (string, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.token != "" && time.Until(ts.expiry) > time.Minute {
		return ts.token, nil
	}

	now := time.Now()
	req, err := ts.request(ctx)
	if err != nil {
		return "", err
	}
	resp, err := ts.http.Do(req)
	if err != nil {
		return "", &tokenError{err: fmt.Errorf("bigquery: token exchange: %w", err)}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", &tokenError{err: fmt.Errorf("bigquery: token exchange: %w", err)}
	}
	if resp.StatusCode != http.StatusOK {
		return "", &tokenError{
			code: resp.StatusCode,
			err:  fmt.Errorf("bigquery: token exchange failed: %s: %s", resp.Status, strings.TrimSpace(string(body))),
		}
	}

	var tok struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		return "", fmt.Errorf("bigquery: token exchange: %w", err)
	}
	if tok.AccessToken == "" {
		return "", errors.New("bigquery: token exchange returned empty access_token")
	}

	ts.token = tok.AccessToken
	ts.expiry = now.Add(time.Duration(tok.ExpiresIn) * time.Second)
	return ts.token, nil
}
