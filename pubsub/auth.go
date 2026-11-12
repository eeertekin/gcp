package pubsub

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
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	defaultTokenURI = "https://oauth2.googleapis.com/token"
	ScopePubSub     = "https://www.googleapis.com/auth/pubsub"
)

type serviceAccount struct {
	Type         string `json:"type"`
	ProjectID    string `json:"project_id"`
	PrivateKeyID string `json:"private_key_id"`
	PrivateKey   string `json:"private_key"`
	ClientEmail  string `json:"client_email"`
	TokenURI     string `json:"token_uri"`
}

type tokenSource struct {
	email    string
	keyID    string
	tokenURI string
	key      *rsa.PrivateKey
	http     *http.Client

	mu     sync.Mutex
	token  string
	expiry time.Time
}

func newTokenSource(sa serviceAccount, hc *http.Client) (*tokenSource, error) {
	if sa.ClientEmail == "" {
		return nil, errors.New("pubsub: credentials missing client_email")
	}
	block, _ := pem.Decode([]byte(sa.PrivateKey))
	if block == nil {
		return nil, errors.New("pubsub: credentials private_key is not valid PEM")
	}
	key, err := parseRSAKey(block.Bytes)
	if err != nil {
		return nil, err
	}
	tokenURI := sa.TokenURI
	if tokenURI == "" {
		tokenURI = defaultTokenURI
	}
	return &tokenSource{
		email:    sa.ClientEmail,
		keyID:    sa.PrivateKeyID,
		tokenURI: tokenURI,
		key:      key,
		http:     hc,
	}, nil
}

func parseRSAKey(der []byte) (*rsa.PrivateKey, error) {
	if k, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		rk, ok := k.(*rsa.PrivateKey)
		if !ok {
			return nil, errors.New("pubsub: private key is not RSA")
		}
		return rk, nil
	}
	k, err := x509.ParsePKCS1PrivateKey(der)
	if err != nil {
		return nil, fmt.Errorf("pubsub: parse private key: %w", err)
	}
	return k, nil
}

type tokenError struct {
	code int
	err  error
}

func (e *tokenError) Error() string { return e.err.Error() }
func (e *tokenError) Unwrap() error { return e.err }

func (ts *tokenSource) Token(ctx context.Context) (string, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.token != "" && time.Until(ts.expiry) > time.Minute {
		return ts.token, nil
	}

	now := time.Now()
	assertion, err := ts.assertion(now)
	if err != nil {
		return "", err
	}

	form := url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"assertion":  {assertion},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ts.tokenURI, strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := ts.http.Do(req)
	if err != nil {
		return "", &tokenError{err: fmt.Errorf("pubsub: token exchange: %w", err)}
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", &tokenError{err: fmt.Errorf("pubsub: token exchange: %w", err)}
	}
	if resp.StatusCode != http.StatusOK {
		return "", &tokenError{
			code: resp.StatusCode,
			err:  fmt.Errorf("pubsub: token exchange failed: %s: %s", resp.Status, strings.TrimSpace(string(body))),
		}
	}

	var tok struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		return "", fmt.Errorf("pubsub: token exchange: %w", err)
	}
	if tok.AccessToken == "" {
		return "", errors.New("pubsub: token exchange returned empty access_token")
	}

	ts.token = tok.AccessToken
	ts.expiry = now.Add(time.Duration(tok.ExpiresIn) * time.Second)
	return ts.token, nil
}

func (ts *tokenSource) assertion(now time.Time) (string, error) {
	header, err := json.Marshal(map[string]string{"alg": "RS256", "typ": "JWT", "kid": ts.keyID})
	if err != nil {
		return "", err
	}
	claims, err := json.Marshal(map[string]any{
		"iss":   ts.email,
		"scope": ScopePubSub,
		"aud":   ts.tokenURI,
		"iat":   now.Unix(),
		"exp":   now.Add(time.Hour).Unix(),
	})
	if err != nil {
		return "", err
	}

	enc := base64.RawURLEncoding
	unsigned := enc.EncodeToString(header) + "." + enc.EncodeToString(claims)
	sum := sha256.Sum256([]byte(unsigned))
	sig, err := rsa.SignPKCS1v15(rand.Reader, ts.key, crypto.SHA256, sum[:])
	if err != nil {
		return "", fmt.Errorf("pubsub: sign jwt: %w", err)
	}
	return unsigned + "." + enc.EncodeToString(sig), nil
}
