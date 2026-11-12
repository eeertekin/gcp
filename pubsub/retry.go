package pubsub

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"time"
)

type RetryPolicy struct {
	MaxAttempts int
	Initial     time.Duration
	Max         time.Duration
}

func defaultRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 10, Initial: 100 * time.Millisecond, Max: 30 * time.Second}
}

type transportError struct {
	err error
}

func (e *transportError) Error() string { return e.err.Error() }
func (e *transportError) Unwrap() error { return e.err }

func retryable(err error) bool {
	var tErr *transportError
	var tokErr *tokenError
	var apiErr *APIError
	switch {
	case errors.As(err, &tErr):
		return true
	case errors.As(err, &tokErr):
		return tokErr.code == 0 || tokErr.code == http.StatusTooManyRequests || tokErr.code >= 500
	case errors.As(err, &apiErr):
		switch apiErr.Status {
		case "ABORTED", "CANCELLED", "DEADLINE_EXCEEDED", "INTERNAL", "RESOURCE_EXHAUSTED", "UNAVAILABLE", "UNKNOWN":
			return true
		case "":
			switch apiErr.Code {
			case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway,
				http.StatusServiceUnavailable, http.StatusGatewayTimeout:
				return true
			}
		}
	}
	return false
}

func (p RetryPolicy) next(d time.Duration) time.Duration {
	d *= 2
	if p.Max > 0 && d > p.Max {
		d = p.Max
	}
	return d
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return d/2 + rand.N(d/2+1)
}

func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
