package pubsub

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	maxPullMessages  = 1000
	maxAckIDsPerCall = 500
	ackFlushInterval = 100 * time.Millisecond
	pullTimeout      = 90 * time.Second
	finalFlushLimit  = 30 * time.Second
	defaultLease     = 60 * time.Second
	minLease         = 10 * time.Second
	maxLease         = 600 * time.Second
)

var errReceiveInProgress = errors.New("pubsub: Receive already in progress for this subscriber")

type ShutdownBehavior int

const (
	ShutdownBehaviorWaitForProcessing ShutdownBehavior = iota
	ShutdownBehaviorNackImmediately
)

// ShutdownOptions: nil waits for all callbacks. Timeout 0 returns
// immediately and a negative Timeout waits forever, regardless of Behavior.
type ShutdownOptions struct {
	Timeout  time.Duration
	Behavior ShutdownBehavior
}

type ReceiveSettings struct {
	MaxExtension               time.Duration
	MaxDurationPerAckExtension time.Duration
	MinDurationPerAckExtension time.Duration
	MaxOutstandingMessages     int
	MaxOutstandingBytes        int
	NumGoroutines              int
	ShutdownOptions            *ShutdownOptions
}

var DefaultReceiveSettings = ReceiveSettings{
	MaxExtension:           60 * time.Minute,
	MaxOutstandingMessages: 1000,
	MaxOutstandingBytes:    1e9,
	NumGoroutines:          1,
}

type Subscriber struct {
	ReceiveSettings ReceiveSettings

	c     *Client
	name  string
	lease time.Duration

	mu     sync.Mutex
	active bool
}

func (c *Client) Subscriber(nameOrID string) *Subscriber {
	name := nameOrID
	if len(strings.Split(nameOrID, "/")) != 4 {
		name = fmt.Sprintf("projects/%s/subscriptions/%s", c.projectID, nameOrID)
	}
	return &Subscriber{c: c, name: name, ReceiveSettings: DefaultReceiveSettings}
}

func (s *Subscriber) ID() string {
	return s.name[strings.LastIndex(s.name, "/")+1:]
}

func (s *Subscriber) String() string {
	return s.name
}

// Receive pulls messages and calls f concurrently for each of them; messages
// sharing an ordering key are handled one at a time, in order. It blocks until
// ctx is done (returning nil) or the service returns a non-retryable error.
func (s *Subscriber) Receive(ctx context.Context, f func(context.Context, *Message)) error {
	s.mu.Lock()
	if s.active {
		s.mu.Unlock()
		return errReceiveInProgress
	}
	s.active = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.active = false
		s.mu.Unlock()
	}()

	rs := s.ReceiveSettings
	maxMsgs, maxBytes := rs.MaxOutstandingMessages, rs.MaxOutstandingBytes
	if maxMsgs == 0 {
		maxMsgs = DefaultReceiveSettings.MaxOutstandingMessages
	}
	if maxBytes == 0 {
		maxBytes = DefaultReceiveSettings.MaxOutstandingBytes
	}
	if rs.MaxExtension == 0 {
		rs.MaxExtension = DefaultReceiveSettings.MaxExtension
	}
	if rs.NumGoroutines < 1 {
		rs.NumGoroutines = 1
	}

	bg, cancelBG := context.WithCancel(context.Background())
	defer cancelBG()
	r := &receiver{
		c:            s.c,
		name:         s.name,
		f:            f,
		lease:        s.leaseDuration(rs),
		maxExtension: rs.MaxExtension,
		fc:           newFlowController(max(maxMsgs, 0), max(maxBytes, 0), false),
		bg:           bg,
		leased:       map[string]*ackHandler{},
		ordered:      map[string][]*Message{},
		kick:         make(chan struct{}, 1),
	}
	return r.run(ctx, rs)
}

func (s *Subscriber) leaseDuration(rs ReceiveSettings) time.Duration {
	if s.lease > 0 {
		return s.lease
	}
	d := defaultLease
	if rs.MaxDurationPerAckExtension > 0 {
		d = min(d, rs.MaxDurationPerAckExtension)
	}
	if rs.MinDurationPerAckExtension > 0 {
		d = max(d, rs.MinDurationPerAckExtension)
	}
	return min(max(d, minLease), maxLease)
}

type receiver struct {
	c            *Client
	name         string
	f            func(context.Context, *Message)
	lease        time.Duration
	maxExtension time.Duration
	fc           *flowController
	bg           context.Context
	cbCtx        context.Context
	callbacks    sync.WaitGroup
	kick         chan struct{}

	mu       sync.Mutex
	leased   map[string]*ackHandler
	ordered  map[string][]*Message
	acks     []string
	nacks    []string
	stopping bool
	closed   bool
}

type ackHandler struct {
	r        *receiver
	ackID    string
	size     int
	received time.Time
	finished bool
	released bool
}

func (h *ackHandler) done(ack bool) {
	h.r.finish(h, ack)
}

func (r *receiver) run(ctx context.Context, rs ReceiveSettings) error {
	cbCtx, cancelCB := context.WithCancel(ctx)
	defer cancelCB()
	r.cbCtx = cbCtx
	pullCtx, stopPull := context.WithCancel(ctx)
	defer stopPull()

	var (
		fatalOnce sync.Once
		fatal     error
		pullers   sync.WaitGroup
	)
	for range rs.NumGoroutines {
		pullers.Add(1)
		go func() {
			defer pullers.Done()
			if err := r.pullLoop(pullCtx); err != nil {
				fatalOnce.Do(func() {
					fatal = err
					stopPull()
					cancelCB()
				})
			}
		}()
	}

	stopKeepAlive := make(chan struct{})
	keepAliveDone := make(chan struct{})
	go r.keepAlive(stopKeepAlive, keepAliveDone)
	stopAcker := make(chan struct{})
	ackerDone := make(chan struct{})
	go r.acker(stopAcker, ackerDone)

	pullers.Wait()
	r.mu.Lock()
	r.stopping = true
	r.mu.Unlock()
	r.shutdown(rs.ShutdownOptions)

	r.mu.Lock()
	r.closed = true
	r.mu.Unlock()
	close(stopKeepAlive)
	<-keepAliveDone
	close(stopAcker)
	<-ackerDone
	return fatal
}

func (r *receiver) shutdown(opts *ShutdownOptions) {
	waitAll := make(chan struct{})
	go func() {
		r.callbacks.Wait()
		close(waitAll)
	}()

	switch {
	case opts == nil || opts.Timeout < 0:
		<-waitAll
	case opts.Timeout == 0:
	case opts.Behavior == ShutdownBehaviorNackImmediately:
		r.mu.Lock()
		pending := make([]*ackHandler, 0, len(r.leased))
		for _, h := range r.leased {
			pending = append(pending, h)
		}
		r.mu.Unlock()
		for _, h := range pending {
			r.finish(h, false)
		}
	default:
		t := time.NewTimer(opts.Timeout)
		defer t.Stop()
		select {
		case <-waitAll:
		case <-t.C:
		}
	}
}

func (r *receiver) pullLoop(ctx context.Context) error {
	delay := r.c.Retry.Initial
	for {
		room, err := r.fc.room(ctx)
		if err != nil {
			return nil
		}
		n := maxPullMessages
		if room > 0 {
			n = min(n, room)
		}
		msgs, err := r.pull(ctx, n)
		if ctx.Err() != nil {
			r.nackUndelivered(msgs)
			return nil
		}
		if err != nil {
			if !retryable(err) {
				return err
			}
			if sleep(ctx, jitter(delay)) != nil {
				return nil
			}
			delay = r.c.Retry.next(delay)
			continue
		}
		delay = r.c.Retry.Initial
		r.deliver(msgs)
	}
}

type receivedMessage struct {
	AckID   string `json:"ackId"`
	Message struct {
		Data        string            `json:"data"`
		Attributes  map[string]string `json:"attributes"`
		MessageID   string            `json:"messageId"`
		PublishTime time.Time         `json:"publishTime"`
		OrderingKey string            `json:"orderingKey"`
	} `json:"message"`
	DeliveryAttempt int `json:"deliveryAttempt"`
}

func (r *receiver) pull(ctx context.Context, n int) ([]receivedMessage, error) {
	ctx, cancel := context.WithTimeout(ctx, pullTimeout)
	defer cancel()
	var resp struct {
		ReceivedMessages []receivedMessage `json:"receivedMessages"`
	}
	err := r.c.call(ctx, r.name, "pull", map[string]int{"maxMessages": n}, &resp)
	return resp.ReceivedMessages, err
}

func (r *receiver) nackUndelivered(msgs []receivedMessage) {
	if len(msgs) == 0 {
		return
	}
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.AckID
	}
	r.mu.Lock()
	r.nacks = append(r.nacks, ids...)
	r.mu.Unlock()
}

func (r *receiver) deliver(received []receivedMessage) {
	if len(received) == 0 {
		return
	}
	now := time.Now()
	ids := make([]string, 0, len(received))
	msgs := make([]*Message, 0, len(received))
	for _, rm := range received {
		data, err := base64.StdEncoding.DecodeString(rm.Message.Data)
		if err != nil {
			r.mu.Lock()
			r.nacks = append(r.nacks, rm.AckID)
			r.mu.Unlock()
			continue
		}
		m := &Message{
			ID:          rm.Message.MessageID,
			Data:        data,
			Attributes:  rm.Message.Attributes,
			PublishTime: rm.Message.PublishTime,
			OrderingKey: rm.Message.OrderingKey,
		}
		if rm.DeliveryAttempt > 0 {
			attempt := rm.DeliveryAttempt
			m.DeliveryAttempt = &attempt
		}
		m.ack = &ackHandler{r: r, ackID: rm.AckID, size: messageSize(m), received: now}
		ids = append(ids, rm.AckID)
		msgs = append(msgs, m)
	}

	r.mu.Lock()
	for _, m := range msgs {
		r.leased[m.ack.ackID] = m.ack
		r.fc.add(m.ack.size)
	}
	r.mu.Unlock()
	go r.modack(ids, r.lease)

	for _, m := range msgs {
		if m.OrderingKey == "" {
			r.callbacks.Add(1)
			go func() {
				defer r.callbacks.Done()
				r.f(r.cbCtx, m)
			}()
			continue
		}
		r.mu.Lock()
		if q, busy := r.ordered[m.OrderingKey]; busy {
			r.ordered[m.OrderingKey] = append(q, m)
			r.mu.Unlock()
			continue
		}
		r.ordered[m.OrderingKey] = nil
		r.callbacks.Add(1)
		r.mu.Unlock()
		go r.runOrdered(m)
	}
}

func (r *receiver) runOrdered(m *Message) {
	defer r.callbacks.Done()
	key := m.OrderingKey
	for m != nil {
		r.f(r.cbCtx, m)

		var skipped []*Message
		r.mu.Lock()
		q := r.ordered[key]
		switch {
		case len(q) == 0:
			delete(r.ordered, key)
			m = nil
		case r.stopping:
			skipped = q
			delete(r.ordered, key)
			m = nil
		default:
			m = q[0]
			r.ordered[key] = q[1:]
		}
		r.mu.Unlock()
		for _, s := range skipped {
			s.Nack()
		}
	}
}

func (r *receiver) finish(h *ackHandler, ack bool) {
	r.mu.Lock()
	if h.finished {
		r.mu.Unlock()
		return
	}
	h.finished = true
	delete(r.leased, h.ackID)
	release := !h.released
	h.released = true
	if !r.closed {
		if ack {
			r.acks = append(r.acks, h.ackID)
		} else {
			r.nacks = append(r.nacks, h.ackID)
		}
		if len(r.acks)+len(r.nacks) >= maxAckIDsPerCall {
			select {
			case r.kick <- struct{}{}:
			default:
			}
		}
	}
	r.mu.Unlock()
	if release {
		r.fc.release(h.size)
	}
}

// keepAlive extends leases until MaxExtension has passed since receipt; a
// negative MaxExtension disables extension beyond the initial lease.
func (r *receiver) keepAlive(stop, done chan struct{}) {
	defer close(done)
	t := time.NewTicker(r.lease * 3 / 4)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		now := time.Now()
		var ids []string
		var expired []*ackHandler
		r.mu.Lock()
		for id, h := range r.leased {
			if r.maxExtension < 0 || now.Sub(h.received) >= r.maxExtension {
				delete(r.leased, id)
				h.released = true
				expired = append(expired, h)
				continue
			}
			ids = append(ids, id)
		}
		r.mu.Unlock()
		for _, h := range expired {
			r.fc.release(h.size)
		}
		r.modack(ids, r.lease)
	}
}

func (r *receiver) acker(stop, done chan struct{}) {
	defer close(done)
	t := time.NewTicker(ackFlushInterval)
	defer t.Stop()
	for {
		select {
		case <-t.C:
		case <-r.kick:
		case <-stop:
			ctx, cancel := context.WithTimeout(r.bg, finalFlushLimit)
			r.flush(ctx)
			cancel()
			return
		}
		r.flush(r.bg)
	}
}

func (r *receiver) flush(ctx context.Context) {
	r.mu.Lock()
	acks, nacks := r.acks, r.nacks
	r.acks, r.nacks = nil, nil
	r.mu.Unlock()

	for c := range chunks(acks) {
		r.c.call(ctx, r.name, "acknowledge", map[string]any{"ackIds": c}, nil)
	}
	for c := range chunks(nacks) {
		r.c.call(ctx, r.name, "modifyAckDeadline", map[string]any{"ackIds": c, "ackDeadlineSeconds": 0}, nil)
	}
}

func (r *receiver) modack(ids []string, d time.Duration) {
	secs := max(int(d/time.Second), 1)
	for c := range chunks(ids) {
		r.c.call(r.bg, r.name, "modifyAckDeadline", map[string]any{"ackIds": c, "ackDeadlineSeconds": secs}, nil)
	}
}

func chunks(ids []string) func(yield func([]string) bool) {
	return func(yield func([]string) bool) {
		for len(ids) > 0 {
			n := min(len(ids), maxAckIDsPerCall)
			if !yield(ids[:n]) {
				return
			}
			ids = ids[n:]
		}
	}
}
