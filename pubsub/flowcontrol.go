package pubsub

import (
	"context"
	"errors"
	"sync"
)

type LimitExceededBehavior int

const (
	FlowControlIgnore LimitExceededBehavior = iota
	FlowControlBlock
	FlowControlSignalError
)

var (
	ErrFlowControllerMaxOutstandingMessages = errors.New("pubsub: MaxOutstandingMessages flow controller limit exceeded")
	ErrFlowControllerMaxOutstandingBytes    = errors.New("pubsub: MaxOutstandingBytes flow control limit exceeded")
)

type FlowControlSettings struct {
	MaxOutstandingMessages int
	MaxOutstandingBytes    int
	LimitExceededBehavior  LimitExceededBehavior
}

// flowController methods are no-ops on a nil receiver, which represents
// disabled flow control.
type flowController struct {
	maxCount int
	maxBytes int
	signal   bool

	mu      sync.Mutex
	count   int
	bytes   int
	changed chan struct{}
}

func newFlowController(maxCount, maxBytes int, signal bool) *flowController {
	if maxCount <= 0 && maxBytes <= 0 {
		return nil
	}
	return &flowController{maxCount: maxCount, maxBytes: maxBytes, signal: signal, changed: make(chan struct{})}
}

func (f *flowController) acquire(ctx context.Context, size int) error {
	if f == nil {
		return nil
	}
	if f.maxBytes > 0 && size > f.maxBytes {
		return ErrFlowControllerMaxOutstandingBytes
	}
	f.mu.Lock()
	for {
		countOK := f.maxCount <= 0 || f.count < f.maxCount
		bytesOK := f.maxBytes <= 0 || f.bytes+size <= f.maxBytes
		if countOK && bytesOK {
			f.count++
			f.bytes += size
			f.mu.Unlock()
			return nil
		}
		if f.signal {
			f.mu.Unlock()
			if !countOK {
				return ErrFlowControllerMaxOutstandingMessages
			}
			return ErrFlowControllerMaxOutstandingBytes
		}
		ch := f.changed
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
		f.mu.Lock()
	}
}

// room blocks until the controller is below both limits and returns how many
// more messages fit (-1 when the message count is unlimited).
func (f *flowController) room(ctx context.Context) (int, error) {
	if f == nil {
		return -1, nil
	}
	f.mu.Lock()
	for {
		countOK := f.maxCount <= 0 || f.count < f.maxCount
		bytesOK := f.maxBytes <= 0 || f.bytes < f.maxBytes
		if countOK && bytesOK {
			n := -1
			if f.maxCount > 0 {
				n = f.maxCount - f.count
			}
			f.mu.Unlock()
			return n, nil
		}
		ch := f.changed
		f.mu.Unlock()
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-ch:
		}
		f.mu.Lock()
	}
}

// add records a message unconditionally; messages already pulled from the
// server cannot be refused.
func (f *flowController) add(size int) {
	if f == nil {
		return
	}
	f.mu.Lock()
	f.count++
	f.bytes += size
	f.mu.Unlock()
}

func (f *flowController) release(size int) {
	if f == nil {
		return
	}
	f.mu.Lock()
	f.count--
	f.bytes -= size
	close(f.changed)
	f.changed = make(chan struct{})
	f.mu.Unlock()
}
