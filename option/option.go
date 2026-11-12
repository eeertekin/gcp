package option

import "net/http"

type CredentialsType string

const (
	ServiceAccount CredentialsType = "service_account"
	AuthorizedUser CredentialsType = "authorized_user"
)

type Settings struct {
	CredentialsJSON []byte
	CredentialsFile string
	CredentialsType CredentialsType
	Endpoint        string
	HTTPClient      *http.Client
}

type ClientOption func(*Settings)

func WithAuthCredentialsJSON(credType CredentialsType, json []byte) ClientOption {
	return func(s *Settings) {
		s.CredentialsType = credType
		s.CredentialsJSON = json
	}
}

func WithAuthCredentialsFile(credType CredentialsType, filename string) ClientOption {
	return func(s *Settings) {
		s.CredentialsType = credType
		s.CredentialsFile = filename
	}
}

func WithEndpoint(url string) ClientOption {
	return func(s *Settings) {
		s.Endpoint = url
	}
}

// WithHTTPClient replaces the transport only. Unlike the official option, the
// client still authenticates every request and token exchange through it.
func WithHTTPClient(client *http.Client) ClientOption {
	return func(s *Settings) {
		s.HTTPClient = client
	}
}
