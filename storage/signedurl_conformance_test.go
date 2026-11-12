package storage

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/eeertekin/gcp/option"
)

// Test vectors from github.com/googleapis/conformance-tests/storage/v1.
type v4Case struct {
	Description          string            `json:"description"`
	Bucket               string            `json:"bucket"`
	Object               string            `json:"object"`
	Method               string            `json:"method"`
	Expiration           int               `json:"expiration"`
	Timestamp            time.Time         `json:"timestamp"`
	Headers              map[string]string `json:"headers"`
	QueryParameters      map[string]string `json:"queryParameters"`
	Scheme               string            `json:"scheme"`
	URLStyle             string            `json:"urlStyle"`
	BucketBoundHostname  string            `json:"bucketBoundHostname"`
	Hostname             string            `json:"hostname"`
	ClientEndpoint       string            `json:"clientEndpoint"`
	EmulatorHostname     string            `json:"emulatorHostname"`
	UniverseDomain       string            `json:"universeDomain"`
	ExpectedURL          string            `json:"expectedUrl"`
	ExpectedStringToSign string            `json:"expectedStringToSign"`
}

func TestSignedURLConformance(t *testing.T) {
	var data struct {
		Cases []v4Case `json:"signingV4Tests"`
	}
	readJSON(t, "testdata/v4_signatures.json", &data)
	var sa serviceAccount
	saJSON := readJSON(t, "testdata/test_service_account.not-a-test.json", &sa)
	key, err := signer(&SignedURLOptions{PrivateKey: []byte(sa.PrivateKey)})
	if err != nil {
		t.Fatal(err)
	}

	if len(data.Cases) < 25 {
		t.Fatalf("only %d conformance cases loaded", len(data.Cases))
	}
	for _, tc := range data.Cases {
		t.Run(tc.Description, func(t *testing.T) {
			if tc.UniverseDomain != "" {
				t.Skip("universe domains are not supported")
			}
			if tc.EmulatorHostname != "" && tc.ClientEndpoint == "" && tc.Hostname == "" {
				t.Skip("STORAGE_EMULATOR_HOST is not supported")
			}

			var stringToSign string
			opts := &SignedURLOptions{
				GoogleAccessID: sa.ClientEmail,
				SignBytes: func(p []byte) ([]byte, error) {
					stringToSign = string(p)
					return key(p)
				},
				Method:   tc.Method,
				Expires:  tc.Timestamp.Add(time.Duration(tc.Expiration) * time.Second),
				Insecure: tc.Scheme == "http",
				Hostname: tc.Hostname,
				Scheme:   SigningSchemeV4,
			}
			for k, v := range tc.Headers {
				opts.Headers = append(opts.Headers, k+":"+v)
			}
			if len(tc.QueryParameters) > 0 {
				opts.QueryParameters = url.Values{}
				for k, v := range tc.QueryParameters {
					opts.QueryParameters.Set(k, v)
				}
			}
			switch tc.URLStyle {
			case "", "PATH_STYLE":
			case "VIRTUAL_HOSTED_STYLE":
				opts.Style = VirtualHostedStyle()
			case "BUCKET_BOUND_HOSTNAME":
				opts.Style = BucketBoundHostname(tc.BucketBoundHostname)
			default:
				t.Fatalf("unknown url style %q", tc.URLStyle)
			}

			endpoint := ""
			if tc.ClientEndpoint != "" {
				c, err := NewClient(context.Background(),
					option.WithAuthCredentialsJSON(option.ServiceAccount, saJSON),
					option.WithEndpoint(tc.ClientEndpoint))
				if err != nil {
					t.Fatal(err)
				}
				endpoint = c.endpoint
			}

			got, err := signURL(tc.Bucket, tc.Object, opts, endpoint, tc.Timestamp)
			if err != nil {
				t.Fatal(err)
			}
			if stringToSign != tc.ExpectedStringToSign {
				t.Errorf("string to sign:\n%s\nwant:\n%s", stringToSign, tc.ExpectedStringToSign)
			}
			if got != tc.ExpectedURL {
				t.Errorf("url:\n%s\nwant:\n%s", got, tc.ExpectedURL)
			}
		})
	}
}

func readJSON(t *testing.T, path string, v any) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return b
}
