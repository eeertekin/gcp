package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/eeertekin/gcp/pubsub"
	"github.com/eeertekin/gcp/pubsub/pubsubtest"
)

func batchSizes(batches [][]pubsubtest.Message) []int {
	sizes := make([]int, len(batches))
	for i, b := range batches {
		sizes[i] = len(b)
	}
	slices.Sort(sizes)
	return sizes
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestNames(t *testing.T) {
	_, c := pubsubtest.NewServer(t)
	if c.Project() != pubsubtest.ProjectID {
		t.Fatalf("Project() = %q", c.Project())
	}
	p := c.Publisher("orders")
	if p.ID() != "orders" || p.String() != "projects/test-project/topics/orders" {
		t.Fatalf("publisher id=%q name=%q", p.ID(), p.String())
	}
	if p := c.Publisher("projects/other/topics/x"); p.String() != "projects/other/topics/x" || p.ID() != "x" {
		t.Fatalf("full topic name rewritten: %q", p.String())
	}
	s := c.Subscriber("workers")
	if s.ID() != "workers" || s.String() != "projects/test-project/subscriptions/workers" {
		t.Fatalf("subscriber id=%q name=%q", s.ID(), s.String())
	}
	if s := c.Subscriber("projects/other/subscriptions/y"); s.String() != "projects/other/subscriptions/y" {
		t.Fatalf("full subscription name rewritten: %q", s.String())
	}
}

func TestPublish(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	defer p.Stop()

	ctx := context.Background()
	r := p.Publish(ctx, &pubsub.Message{Data: []byte("hello"), Attributes: map[string]string{"k": "v"}})
	id, err := r.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.Ready():
	default:
		t.Fatal("Ready not closed after Get")
	}

	b := srv.Batches("t")
	if len(b) != 1 || len(b[0]) != 1 {
		t.Fatalf("batches = %v", b)
	}
	m := b[0][0]
	if m.ID != id || string(m.Data) != "hello" || m.Attributes["k"] != "v" {
		t.Fatalf("stored %+v, result id %q", m, id)
	}

	if _, err := p.Publish(ctx, &pubsub.Message{Attributes: map[string]string{"only": "attrs"}}).Get(ctx); err != nil {
		t.Fatalf("attributes-only message: %v", err)
	}
	if srv.TokenCalls() != 1 {
		t.Fatalf("token fetched %d times, want cached", srv.TokenCalls())
	}
}

func TestPublishBatchesByCount(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	p.PublishSettings.CountThreshold = 5
	p.PublishSettings.DelayThreshold = time.Hour

	ctx := context.Background()
	var results []*pubsub.PublishResult
	for i := range 12 {
		results = append(results, p.Publish(ctx, &pubsub.Message{Data: fmt.Appendf(nil, "m%d", i)}))
	}
	waitFor(t, "two full batches", func() bool { return len(srv.Batches("t")) == 2 })
	select {
	case <-results[11].Ready():
		t.Fatal("partial batch sent before Flush despite huge DelayThreshold")
	case <-time.After(50 * time.Millisecond):
	}

	p.Flush()
	if got := batchSizes(srv.Batches("t")); !slices.Equal(got, []int{2, 5, 5}) {
		t.Fatalf("batch sizes = %v", got)
	}
	ids := map[string]bool{}
	for _, r := range results {
		id, err := r.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		ids[id] = true
	}
	if len(ids) != 12 {
		t.Fatalf("%d distinct ids", len(ids))
	}
}

func TestPublishBatchesByDelay(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	p.PublishSettings.DelayThreshold = 50 * time.Millisecond
	defer p.Stop()

	ctx := context.Background()
	start := time.Now()
	var results []*pubsub.PublishResult
	for range 3 {
		results = append(results, p.Publish(ctx, &pubsub.Message{Data: []byte("x")}))
	}
	for _, r := range results {
		if _, err := r.Get(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if el := time.Since(start); el < 50*time.Millisecond {
		t.Fatalf("batch sent after %s, before DelayThreshold", el)
	}
	if got := batchSizes(srv.Batches("t")); !slices.Equal(got, []int{3}) {
		t.Fatalf("batch sizes = %v", got)
	}
}

func TestPublishBatchesByBytes(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	p.PublishSettings.ByteThreshold = 1000
	p.PublishSettings.DelayThreshold = time.Hour

	ctx := context.Background()
	for range 10 {
		p.Publish(ctx, &pubsub.Message{Data: make([]byte, 300)})
	}
	p.Flush()
	if got := batchSizes(srv.Batches("t")); !slices.Equal(got, []int{2, 4, 4}) {
		t.Fatalf("batch sizes = %v", got)
	}
}

func TestPublishSplitsAtRequestLimit(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	p.PublishSettings.ByteThreshold = 1e9
	p.PublishSettings.CountThreshold = 5000
	p.PublishSettings.DelayThreshold = time.Hour

	ctx := context.Background()
	for range 3 {
		p.Publish(ctx, &pubsub.Message{Data: make([]byte, 4e6)})
	}
	for range 1500 {
		p.Publish(ctx, &pubsub.Message{Data: []byte("s")})
	}
	p.Flush()
	for _, b := range srv.Batches("t") {
		total := 0
		for _, m := range b {
			total += len(m.Data)
		}
		if len(b) > pubsub.MaxPublishRequestCount || total > pubsub.MaxPublishRequestBytes {
			t.Fatalf("batch of %d messages / %d bytes exceeds request limits", len(b), total)
		}
	}
	n := 0
	for _, b := range srv.Batches("t") {
		n += len(b)
	}
	if n != 1503 {
		t.Fatalf("published %d messages", n)
	}
}

func TestPublishOversized(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	defer p.Stop()

	ctx := context.Background()
	_, err := p.Publish(ctx, &pubsub.Message{Data: make([]byte, pubsub.MaxPublishRequestBytes)}).Get(ctx)
	if !errors.Is(err, pubsub.ErrOversizedMessage) {
		t.Fatalf("err = %v", err)
	}
	if srv.Calls("publish") != 0 {
		t.Fatal("oversized message reached the server")
	}
}

func TestPublishStop(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	p.PublishSettings.DelayThreshold = time.Hour

	ctx := context.Background()
	var results []*pubsub.PublishResult
	for range 3 {
		results = append(results, p.Publish(ctx, &pubsub.Message{Data: []byte("x")}))
	}
	p.Stop()
	for _, r := range results {
		select {
		case <-r.Ready():
		default:
			t.Fatal("Stop returned before pending messages were published")
		}
		if _, err := r.Get(ctx); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := p.Publish(ctx, &pubsub.Message{Data: []byte("late")}).Get(ctx); !errors.Is(err, pubsub.ErrPublisherStopped) {
		t.Fatalf("publish after Stop: %v", err)
	}
	p.Stop()
	p.Flush()
	c.Publisher("t").Stop()
}

func TestPublishOrderingNotEnabled(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	defer p.Stop()

	ctx := context.Background()
	if _, err := p.Publish(ctx, &pubsub.Message{Data: []byte("x"), OrderingKey: "k"}).Get(ctx); err == nil {
		t.Fatal("expected error for ordering key without EnableMessageOrdering")
	}
	if srv.Calls("publish") != 0 {
		t.Fatal("message reached the server")
	}
}

func TestPublishOrdering(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	p.EnableMessageOrdering = true
	p.PublishSettings.CountThreshold = 1

	release := srv.HoldPublish()
	ctx := context.Background()
	keys := []string{"a", "b", "c", ""}
	var results []*pubsub.PublishResult
	for i := range 40 {
		results = append(results, p.Publish(ctx, &pubsub.Message{
			Data:        fmt.Appendf(nil, "%d", i),
			OrderingKey: keys[i%len(keys)],
		}))
	}
	waitFor(t, "concurrent publishes", func() bool { return srv.MaxConcurrentPublishes() >= 13 })
	time.Sleep(50 * time.Millisecond)
	if n := srv.MaxConcurrentPublishesPerKey(); n != 1 {
		t.Fatalf("%d concurrent publishes for one ordering key", n)
	}
	release()
	p.Flush()

	if n := srv.MaxConcurrentPublishesPerKey(); n != 1 {
		t.Fatalf("%d concurrent publishes for one ordering key", n)
	}
	for _, r := range results {
		if _, err := r.Get(ctx); err != nil {
			t.Fatal(err)
		}
	}
	last := map[string]int{}
	for _, b := range srv.Batches("t") {
		for _, m := range b {
			if m.OrderingKey == "" {
				continue
			}
			var seq int
			fmt.Sscan(string(m.Data), &seq)
			if prev, ok := last[m.OrderingKey]; ok && seq < prev {
				t.Fatalf("key %q: message %d stored after %d", m.OrderingKey, seq, prev)
			}
			last[m.OrderingKey] = seq
		}
	}
	if len(last) != 3 {
		t.Fatalf("keys seen: %v", last)
	}
}

func TestPublishOrderingPauseAndResume(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	p.EnableMessageOrdering = true
	p.PublishSettings.CountThreshold = 1
	defer p.Stop()

	ctx := context.Background()
	release := srv.HoldPublish()
	first := p.Publish(ctx, &pubsub.Message{Data: []byte("1"), OrderingKey: "a"})
	queued := p.Publish(ctx, &pubsub.Message{Data: []byte("2"), OrderingKey: "a"})
	waitFor(t, "first publish in flight", func() bool { return srv.MaxConcurrentPublishes() == 1 })
	srv.InjectFaults(pubsubtest.Fault{Verb: "publish", Status: 400, State: "INVALID_ARGUMENT"})
	release()

	_, err := first.Get(ctx)
	var apiErr *pubsub.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != 400 || apiErr.Status != "INVALID_ARGUMENT" {
		t.Fatalf("first: %v", err)
	}
	var paused pubsub.ErrPublishingPaused
	if _, err := queued.Get(ctx); !errors.As(err, &paused) || paused.OrderingKey != "a" {
		t.Fatalf("queued: %v", err)
	}
	if _, err := p.Publish(ctx, &pubsub.Message{Data: []byte("3"), OrderingKey: "a"}).Get(ctx); !errors.As(err, &paused) {
		t.Fatalf("publish on paused key: %v", err)
	}
	if _, err := p.Publish(ctx, &pubsub.Message{Data: []byte("b"), OrderingKey: "b"}).Get(ctx); err != nil {
		t.Fatalf("other key affected by pause: %v", err)
	}
	if _, err := p.Publish(ctx, &pubsub.Message{Data: []byte("u")}).Get(ctx); err != nil {
		t.Fatalf("unordered affected by pause: %v", err)
	}

	p.ResumePublish("a")
	if _, err := p.Publish(ctx, &pubsub.Message{Data: []byte("4"), OrderingKey: "a"}).Get(ctx); err != nil {
		t.Fatalf("after ResumePublish: %v", err)
	}
	for _, b := range srv.Batches("t") {
		for _, m := range b {
			if string(m.Data) == "2" || string(m.Data) == "3" {
				t.Fatalf("message %s published although it failed", m.Data)
			}
		}
	}
}

func TestPublishUnorderedErrorDoesNotPause(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	defer p.Stop()

	ctx := context.Background()
	srv.InjectFaults(pubsubtest.Fault{Verb: "publish", Status: 403, State: "PERMISSION_DENIED"})
	if _, err := p.Publish(ctx, &pubsub.Message{Data: []byte("x")}).Get(ctx); err == nil {
		t.Fatal("expected error")
	}
	if _, err := p.Publish(ctx, &pubsub.Message{Data: []byte("y")}).Get(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPublishRetriesTransientErrors(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	defer p.Stop()

	srv.InjectFaults(
		pubsubtest.Fault{Verb: "publish", Status: 503, State: "UNAVAILABLE"},
		pubsubtest.Fault{Verb: "publish", Status: 429, State: "RESOURCE_EXHAUSTED"},
		pubsubtest.Fault{Verb: "publish", Drop: true},
	)
	ctx := context.Background()
	id, err := p.Publish(ctx, &pubsub.Message{Data: []byte("x")}).Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if srv.Calls("publish") != 4 {
		t.Fatalf("publish calls = %d", srv.Calls("publish"))
	}
	b := srv.Batches("t")
	if len(b) != 1 || b[0][0].ID != id {
		t.Fatalf("batches = %v", b)
	}
}

func TestPublishGivesUpAfterMaxAttempts(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	defer p.Stop()

	for range 10 {
		srv.InjectFaults(pubsubtest.Fault{Verb: "publish", Status: 503, State: "UNAVAILABLE"})
	}
	ctx := context.Background()
	_, err := p.Publish(ctx, &pubsub.Message{Data: []byte("x")}).Get(ctx)
	var apiErr *pubsub.APIError
	if !errors.As(err, &apiErr) || apiErr.Code != 503 {
		t.Fatalf("err = %v", err)
	}
	if srv.Calls("publish") != c.Retry.MaxAttempts {
		t.Fatalf("publish calls = %d, want %d", srv.Calls("publish"), c.Retry.MaxAttempts)
	}
}

func TestPublishPermanentError(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	p := c.Publisher("missing")
	p.PublishSettings.DelayThreshold = time.Hour

	ctx := context.Background()
	results := []*pubsub.PublishResult{
		p.Publish(ctx, &pubsub.Message{Data: []byte("1")}),
		p.Publish(ctx, &pubsub.Message{Data: []byte("2")}),
	}
	p.Stop()
	for _, r := range results {
		_, err := r.Get(ctx)
		var apiErr *pubsub.APIError
		if !errors.As(err, &apiErr) || apiErr.Code != 404 || apiErr.Status != "NOT_FOUND" {
			t.Fatalf("err = %v", err)
		}
	}
	if srv.Calls("publish") != 1 {
		t.Fatalf("publish calls = %d, permanent errors must not be retried", srv.Calls("publish"))
	}
}

func TestPublishTimeout(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	p.PublishSettings.Timeout = 100 * time.Millisecond

	srv.HoldPublish()
	ctx := context.Background()
	start := time.Now()
	_, err := p.Publish(ctx, &pubsub.Message{Data: []byte("x")}).Get(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
	if el := time.Since(start); el > 2*time.Second {
		t.Fatalf("timeout took %s", el)
	}
}

func TestPublishResultGetContext(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")

	release := srv.HoldPublish()
	r := p.Publish(context.Background(), &pubsub.Message{Data: []byte("x")})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := r.Get(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Get with expired ctx: %v", err)
	}
	release()
	if _, err := r.Get(context.Background()); err != nil {
		t.Fatal(err)
	}
	p.Stop()
}

func TestPublishNumGoroutines(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	p.PublishSettings.CountThreshold = 1
	p.PublishSettings.NumGoroutines = 2

	release := srv.HoldPublish()
	ctx := context.Background()
	var results []*pubsub.PublishResult
	for range 6 {
		results = append(results, p.Publish(ctx, &pubsub.Message{Data: []byte("x")}))
	}
	waitFor(t, "2 publishes in flight", func() bool { return srv.MaxConcurrentPublishes() == 2 })
	time.Sleep(50 * time.Millisecond)
	if n := srv.MaxConcurrentPublishes(); n != 2 {
		t.Fatalf("%d concurrent publishes, NumGoroutines is 2", n)
	}
	release()
	p.Stop()
	for _, r := range results {
		if _, err := r.Get(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if srv.MaxConcurrentPublishes() != 2 {
		t.Fatalf("%d concurrent publishes, NumGoroutines is 2", srv.MaxConcurrentPublishes())
	}
}

func TestPublishFlowControlSignalError(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	p.PublishSettings.CountThreshold = 1
	p.PublishSettings.FlowControlSettings = pubsub.FlowControlSettings{
		MaxOutstandingMessages: 1,
		MaxOutstandingBytes:    -1,
		LimitExceededBehavior:  pubsub.FlowControlSignalError,
	}
	defer p.Stop()

	ctx := context.Background()
	release := srv.HoldPublish()
	first := p.Publish(ctx, &pubsub.Message{Data: []byte("1")})
	if _, err := p.Publish(ctx, &pubsub.Message{Data: []byte("2")}).Get(ctx); !errors.Is(err, pubsub.ErrFlowControllerMaxOutstandingMessages) {
		t.Fatalf("second publish: %v", err)
	}
	release()
	if _, err := first.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Publish(ctx, &pubsub.Message{Data: []byte("3")}).Get(ctx); err != nil {
		t.Fatalf("after capacity freed: %v", err)
	}
}

func TestPublishFlowControlBytes(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	p.PublishSettings.FlowControlSettings = pubsub.FlowControlSettings{
		MaxOutstandingMessages: -1,
		MaxOutstandingBytes:    100,
		LimitExceededBehavior:  pubsub.FlowControlSignalError,
	}
	defer p.Stop()

	ctx := context.Background()
	if _, err := p.Publish(ctx, &pubsub.Message{Data: make([]byte, 200)}).Get(ctx); !errors.Is(err, pubsub.ErrFlowControllerMaxOutstandingBytes) {
		t.Fatalf("err = %v", err)
	}
	if _, err := p.Publish(ctx, &pubsub.Message{Data: make([]byte, 50)}).Get(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPublishFlowControlBlock(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")
	p.PublishSettings.CountThreshold = 1
	p.PublishSettings.FlowControlSettings = pubsub.FlowControlSettings{
		MaxOutstandingMessages: 1,
		MaxOutstandingBytes:    -1,
		LimitExceededBehavior:  pubsub.FlowControlBlock,
	}
	defer p.Stop()

	ctx := context.Background()
	release := srv.HoldPublish()
	first := p.Publish(ctx, &pubsub.Message{Data: []byte("1")})

	done := make(chan *pubsub.PublishResult)
	go func() { done <- p.Publish(ctx, &pubsub.Message{Data: []byte("2")}) }()
	select {
	case <-done:
		t.Fatal("Publish did not block on flow control")
	case <-time.After(100 * time.Millisecond):
	}
	release()
	second := <-done
	for _, r := range []*pubsub.PublishResult{first, second} {
		if _, err := r.Get(ctx); err != nil {
			t.Fatal(err)
		}
	}

	defer srv.HoldPublish()()
	p.Publish(ctx, &pubsub.Message{Data: []byte("3")})
	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := p.Publish(short, &pubsub.Message{Data: []byte("4")}).Get(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked publish with expiring ctx: %v", err)
	}
}

func TestPublishConcurrent(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	p := c.Publisher("t")

	ctx := context.Background()
	var (
		mu  sync.Mutex
		ids = map[string]bool{}
		wg  sync.WaitGroup
	)
	for g := range 20 {
		wg.Go(func() {
			for i := range 50 {
				id, err := p.Publish(ctx, &pubsub.Message{Data: fmt.Appendf(nil, "%d-%d", g, i)}).Get(ctx)
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				ids[id] = true
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	p.Stop()
	if len(ids) != 1000 {
		t.Fatalf("%d distinct ids", len(ids))
	}
	seen := map[string]bool{}
	for _, b := range srv.Batches("t") {
		for _, m := range b {
			if seen[string(m.Data)] {
				t.Fatalf("duplicate %s", m.Data)
			}
			seen[string(m.Data)] = true
		}
	}
	if len(seen) != 1000 {
		t.Fatalf("stored %d messages", len(seen))
	}
}
