package option

type CredentialsType string

const ServiceAccount CredentialsType = "service_account"

type Settings struct {
	CredentialsJSON []byte
	CredentialsFile string
	CredentialsType CredentialsType
	Endpoint        string
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
