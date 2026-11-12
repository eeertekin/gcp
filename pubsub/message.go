package pubsub

import (
	"context"
	"time"
)

type Message struct {
	ID              string
	Data            []byte
	Attributes      map[string]string
	PublishTime     time.Time
	DeliveryAttempt *int
	OrderingKey     string

	ack *ackHandler
}

// Ack acknowledges the message. Calls after the first Ack or Nack are ignored,
// as are calls on messages that did not come from Receive.
func (m *Message) Ack() {
	if m.ack != nil {
		m.ack.done(true)
	}
}

// Nack asks the server to redeliver the message as soon as possible.
func (m *Message) Nack() {
	if m.ack != nil {
		m.ack.done(false)
	}
}

type PublishResult struct {
	ready    chan struct{}
	serverID string
	err      error
}

func newPublishResult() *PublishResult {
	return &PublishResult{ready: make(chan struct{})}
}

func (r *PublishResult) set(id string, err error) {
	r.serverID, r.err = id, err
	close(r.ready)
}

func (r *PublishResult) Ready() <-chan struct{} { return r.ready }

func (r *PublishResult) Get(ctx context.Context) (serverID string, err error) {
	select {
	case <-r.ready:
		return r.serverID, r.err
	default:
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-r.ready:
		return r.serverID, r.err
	}
}
