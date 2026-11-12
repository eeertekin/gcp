package pubsub

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"time"
)

const (
	MaxPublishRequestCount = 1000
	MaxPublishRequestBytes = 1e7
)

var (
	ErrOversizedMessage = errors.New("pubsub: message is larger than MaxPublishRequestBytes")
	ErrPublisherStopped = errors.New("pubsub: Stop has been called for this publisher")

	errPublisherOrderingNotEnabled = errors.New("Publisher.EnableMessageOrdering=false, but an OrderingKey was set in Message. Please remove the OrderingKey or turn on Publisher.EnableMessageOrdering")
)

type ErrPublishingPaused struct {
	OrderingKey string
}

func (e ErrPublishingPaused) Error() string {
	return fmt.Sprintf("pubsub: Publishing for ordering key, %s, paused due to previous error. Call topic.ResumePublish(orderingKey) before resuming publishing", e.OrderingKey)
}

type PublishSettings struct {
	DelayThreshold      time.Duration
	CountThreshold      int
	ByteThreshold       int
	NumGoroutines       int
	Timeout             time.Duration
	FlowControlSettings FlowControlSettings
}

var DefaultPublishSettings = PublishSettings{
	DelayThreshold: 10 * time.Millisecond,
	CountThreshold: 100,
	ByteThreshold:  1e6,
	Timeout:        60 * time.Second,
	FlowControlSettings: FlowControlSettings{
		MaxOutstandingMessages: 1000,
		MaxOutstandingBytes:    -1,
		LimitExceededBehavior:  FlowControlIgnore,
	},
}

// Publisher batches messages per ordering key. Batches without an ordering key
// are sent concurrently (up to NumGoroutines); batches for the same ordering
// key are sent one at a time, in order.
type Publisher struct {
	PublishSettings       PublishSettings
	EnableMessageOrdering bool

	c    *Client
	name string

	mu          sync.Mutex
	cond        *sync.Cond
	started     bool
	stopped     bool
	settings    PublishSettings
	fc          *flowController
	sem         chan struct{}
	keys        map[string]*keyState
	outstanding int
}

type pendingMessage struct {
	msg  *Message
	res  *PublishResult
	size int
}

type keyState struct {
	batch    []*pendingMessage
	bytes    int
	gen      int
	timer    *time.Timer
	queue    [][]*pendingMessage
	inflight bool
	paused   bool
}

func (c *Client) Publisher(topicNameOrID string) *Publisher {
	name := topicNameOrID
	if len(strings.Split(topicNameOrID, "/")) != 4 {
		name = fmt.Sprintf("projects/%s/topics/%s", c.projectID, topicNameOrID)
	}
	return &Publisher{c: c, name: name, PublishSettings: DefaultPublishSettings}
}

func (p *Publisher) ID() string {
	return p.name[strings.LastIndex(p.name, "/")+1:]
}

func (p *Publisher) String() string {
	return p.name
}

func (p *Publisher) start() {
	if p.started {
		return
	}
	p.started = true
	p.cond = sync.NewCond(&p.mu)
	p.keys = map[string]*keyState{}

	s := p.PublishSettings
	d := DefaultPublishSettings
	if s.DelayThreshold <= 0 {
		s.DelayThreshold = d.DelayThreshold
	}
	if s.CountThreshold <= 0 {
		s.CountThreshold = d.CountThreshold
	}
	s.CountThreshold = min(s.CountThreshold, MaxPublishRequestCount)
	if s.ByteThreshold <= 0 {
		s.ByteThreshold = d.ByteThreshold
	}
	s.ByteThreshold = min(s.ByteThreshold, MaxPublishRequestBytes)
	if s.Timeout <= 0 {
		s.Timeout = d.Timeout
	}
	if s.NumGoroutines <= 0 {
		s.NumGoroutines = 25 * runtime.GOMAXPROCS(0)
	}
	p.settings = s
	p.sem = make(chan struct{}, s.NumGoroutines)

	fcs := s.FlowControlSettings
	if fcs.LimitExceededBehavior != FlowControlIgnore {
		p.fc = newFlowController(fcs.MaxOutstandingMessages, fcs.MaxOutstandingBytes, fcs.LimitExceededBehavior == FlowControlSignalError)
	}
}

// Publish queues msg for publishing and returns immediately. The ctx only
// bounds waiting for flow control; the request itself is bounded by
// PublishSettings.Timeout.
func (p *Publisher) Publish(ctx context.Context, msg *Message) *PublishResult {
	r := newPublishResult()
	key := msg.OrderingKey
	if !p.EnableMessageOrdering && key != "" {
		r.set("", errPublisherOrderingNotEnabled)
		return r
	}
	size := messageSize(msg)
	if size > MaxPublishRequestBytes {
		r.set("", ErrOversizedMessage)
		return r
	}

	p.mu.Lock()
	p.start()
	if err := p.rejectLocked(key); err != nil {
		p.mu.Unlock()
		r.set("", err)
		return r
	}
	fc := p.fc
	p.mu.Unlock()

	if err := fc.acquire(ctx, size); err != nil {
		p.mu.Lock()
		if key != "" {
			p.key(key).paused = true
		}
		p.mu.Unlock()
		r.set("", err)
		return r
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.rejectLocked(key); err != nil {
		fc.release(size)
		r.set("", err)
		return r
	}

	ks := p.key(key)
	if ks.bytes+size > MaxPublishRequestBytes {
		p.cut(key, ks)
	}
	ks.batch = append(ks.batch, &pendingMessage{msg: msg, res: r, size: size})
	ks.bytes += size
	switch {
	case len(ks.batch) >= p.settings.CountThreshold || ks.bytes >= p.settings.ByteThreshold:
		p.cut(key, ks)
	case ks.timer == nil:
		gen := ks.gen
		ks.timer = time.AfterFunc(p.settings.DelayThreshold, func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			if p.keys[key] == ks && ks.gen == gen {
				p.cut(key, ks)
			}
		})
	}
	return r
}

func (p *Publisher) rejectLocked(key string) error {
	if p.stopped {
		return ErrPublisherStopped
	}
	if ks := p.keys[key]; ks != nil && ks.paused {
		return ErrPublishingPaused{OrderingKey: key}
	}
	return nil
}

func (p *Publisher) key(key string) *keyState {
	ks := p.keys[key]
	if ks == nil {
		ks = &keyState{}
		p.keys[key] = ks
	}
	return ks
}

func (p *Publisher) cut(key string, ks *keyState) {
	if ks.timer != nil {
		ks.timer.Stop()
		ks.timer = nil
	}
	if len(ks.batch) == 0 {
		return
	}
	b := ks.batch
	ks.batch, ks.bytes = nil, 0
	ks.gen++
	p.outstanding++
	if key != "" && ks.inflight {
		ks.queue = append(ks.queue, b)
		return
	}
	if key != "" {
		ks.inflight = true
	}
	go p.send(key, b)
}

func (p *Publisher) send(key string, b []*pendingMessage) {
	p.sem <- struct{}{}
	ids, err := p.publish(b)
	<-p.sem

	for i, m := range b {
		p.fc.release(m.size)
		if err != nil {
			m.res.set("", err)
		} else {
			m.res.set(ids[i], nil)
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.outstanding--
	defer p.cond.Broadcast()
	if key == "" {
		return
	}

	ks := p.keys[key]
	switch {
	case err != nil:
		ks.paused = true
		ks.inflight = false
		paused := ErrPublishingPaused{OrderingKey: key}
		for _, q := range ks.queue {
			p.outstanding--
			for _, m := range q {
				p.fc.release(m.size)
				m.res.set("", paused)
			}
		}
		ks.queue = nil
		for _, m := range ks.batch {
			p.fc.release(m.size)
			m.res.set("", paused)
		}
		ks.batch, ks.bytes = nil, 0
		if ks.timer != nil {
			ks.timer.Stop()
			ks.timer = nil
		}
	case len(ks.queue) > 0:
		next := ks.queue[0]
		ks.queue = ks.queue[1:]
		go p.send(key, next)
	default:
		ks.inflight = false
		if len(ks.batch) == 0 {
			delete(p.keys, key)
		}
	}
}

type wireMessage struct {
	Data        string            `json:"data,omitempty"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	OrderingKey string            `json:"orderingKey,omitempty"`
}

func (p *Publisher) publish(b []*pendingMessage) ([]string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), p.settings.Timeout)
	defer cancel()

	req := struct {
		Messages []wireMessage `json:"messages"`
	}{Messages: make([]wireMessage, len(b))}
	for i, m := range b {
		req.Messages[i] = wireMessage{
			Data:        base64.StdEncoding.EncodeToString(m.msg.Data),
			Attributes:  m.msg.Attributes,
			OrderingKey: m.msg.OrderingKey,
		}
	}
	var resp struct {
		MessageIDs []string `json:"messageIds"`
	}
	if err := p.c.call(ctx, p.name, "publish", req, &resp); err != nil {
		return nil, err
	}
	if len(resp.MessageIDs) != len(b) {
		return nil, fmt.Errorf("pubsub: publish returned %d message ids for %d messages", len(resp.MessageIDs), len(b))
	}
	return resp.MessageIDs, nil
}

// Flush sends all queued messages and waits until every outstanding batch
// has completed.
func (p *Publisher) Flush() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.flushLocked()
}

func (p *Publisher) flushLocked() {
	if !p.started {
		return
	}
	for k, ks := range p.keys {
		p.cut(k, ks)
	}
	for p.outstanding > 0 {
		p.cond.Wait()
	}
}

// Stop flushes outstanding messages; later calls to Publish fail with
// ErrPublisherStopped.
func (p *Publisher) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stopped = true
	p.flushLocked()
}

func (p *Publisher) ResumePublish(orderingKey string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.started {
		return
	}
	if ks := p.keys[orderingKey]; ks != nil {
		ks.paused = false
		if !ks.inflight && len(ks.batch) == 0 {
			delete(p.keys, orderingKey)
		}
	}
}

// messageSize is the encoded protobuf size of a PubsubMessage, which is what
// the service counts against request limits.
func messageSize(m *Message) int {
	field := func(n int) int { return 1 + varintSize(n) + n }
	size := 0
	if len(m.Data) > 0 {
		size += field(len(m.Data))
	}
	for k, v := range m.Attributes {
		size += field(field(len(k)) + field(len(v)))
	}
	if m.OrderingKey != "" {
		size += field(len(m.OrderingKey))
	}
	return size
}

func varintSize(n int) int {
	s := 1
	for n >= 0x80 {
		n >>= 7
		s++
	}
	return s
}
