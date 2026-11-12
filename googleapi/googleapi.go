package googleapi

import (
	"fmt"
	"net/http"
	"strings"
)

type ErrorItem struct {
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

type Error struct {
	Code    int
	Message string
	Errors  []ErrorItem
	Body    string
	Header  http.Header
}

func (e *Error) Error() string {
	if len(e.Errors) == 0 && e.Message == "" {
		return fmt.Sprintf("googleapi: got HTTP response code %d with body: %v", e.Code, e.Body)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "googleapi: Error %d: %s", e.Code, e.Message)
	if len(e.Errors) == 1 && e.Errors[0].Message == e.Message {
		fmt.Fprintf(&b, ", %s", e.Errors[0].Reason)
		return b.String()
	}
	if len(e.Errors) > 0 {
		b.WriteString("\nMore details:\n")
		for _, item := range e.Errors {
			fmt.Fprintf(&b, "Reason: %s, Message: %s\n", item.Reason, item.Message)
		}
	}
	return b.String()
}
