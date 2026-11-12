package pubsub_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/eeertekin/gcp/pubsub"
	"github.com/eeertekin/gcp/pubsub/pubsubtest"
)

type receiving struct {
	cancel context.CancelFunc
	done   chan error
}

func startReceive(t *testing.T, s *pubsub.Subscriber, f func(context.Context, *pubsub.Message)) *receiving {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	r := &receiving{cancel: cancel, done: make(chan error, 1)}
	go func() { r.done <- s.Receive(ctx, f) }()
	t.Cleanup(cancel)
	return r
}

func (r *receiving) wait(t *testing.T) error {
	t.Helper()
	select {
	case err := <-r.done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Receive did not return")
		return nil
	}
}

func (r *receiving) stop(t *testing.T) error {
	t.Helper()
	r.cancel()
	return r.wait(t)
}

func newSubscription(t *testing.T, cfg pubsubtest.SubscriptionConfig) (*pubsubtest.Server, *pubsub.Subscriber) {
	t.Helper()
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("t")
	srv.CreateSubscription("s", "t", cfg)
	return srv, c.Subscriber("s")
}

func recv[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
		var zero T
		return zero
	}
}

func TestReceiveAck(t *testing.T) {
	srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
	ids := srv.Publish("t",
		pubsubtest.Message{Data: []byte("a"), Attributes: map[string]string{"k": "1"}},
		pubsubtest.Message{Data: []byte("b")},
		pubsubtest.Message{Data: []byte("c")},
	)

	got := make(chan *pubsub.Message, 3)
	r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) {
		got <- m
		m.Ack()
	})
	byID := map[string]*pubsub.Message{}
	for range 3 {
		m := recv(t, got)
		byID[m.ID] = m
	}
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}

	m := byID[ids[0]]
	if m == nil || string(m.Data) != "a" || m.Attributes["k"] != "1" {
		t.Fatalf("message %s = %+v", ids[0], m)
	}
	if m.PublishTime.IsZero() || time.Since(m.PublishTime) > time.Minute {
		t.Fatalf("PublishTime = %s", m.PublishTime)
	}
	if m.DeliveryAttempt != nil {
		t.Fatalf("DeliveryAttempt set without dead lettering: %d", *m.DeliveryAttempt)
	}
	if string(byID[ids[1]].Data) != "b" || string(byID[ids[2]].Data) != "c" {
		t.Fatal("payload mismatch")
	}
	if n := len(srv.Acked("s")); n != 3 {
		t.Fatalf("acked %d, final flush must send pending acks before Receive returns", n)
	}
	if srv.Unacked("s") != 0 {
		t.Fatalf("%d messages left", srv.Unacked("s"))
	}
}

func TestReceiveNackRedelivers(t *testing.T) {
	srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{DeadLetter: true})
	srv.Publish("t", pubsubtest.Message{Data: []byte("x")})

	attempts := make(chan int, 4)
	r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) {
		attempts <- *m.DeliveryAttempt
		if *m.DeliveryAttempt == 1 {
			m.Nack()
			return
		}
		m.Ack()
	})
	if a := recv(t, attempts); a != 1 {
		t.Fatalf("first attempt = %d", a)
	}
	if a := recv(t, attempts); a != 2 {
		t.Fatalf("second attempt = %d", a)
	}
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if len(srv.Acked("s")) != 1 || srv.Unacked("s") != 0 {
		t.Fatalf("acked=%v unacked=%d", srv.Acked("s"), srv.Unacked("s"))
	}
}

func TestReceiveAlreadyInProgress(t *testing.T) {
	srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
	got := make(chan string, 1)
	f := func(ctx context.Context, m *pubsub.Message) {
		got <- string(m.Data)
		m.Ack()
	}
	r := startReceive(t, sub, f)
	waitFor(t, "first pull", func() bool { return srv.Calls("pull") > 0 })
	if err := sub.Receive(context.Background(), f); err == nil {
		t.Fatal("concurrent Receive on the same subscriber must fail")
	}
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}

	// Let the server finish the abandoned long poll so it cannot lease the next message.
	time.Sleep(100 * time.Millisecond)
	srv.Publish("t", pubsubtest.Message{Data: []byte("again")})
	r = startReceive(t, sub, f)
	if d := recv(t, got); d != "again" {
		t.Fatalf("got %q", d)
	}
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
}

func TestReceiveFatalErrors(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		_, c := pubsubtest.NewServer(t)
		var called atomic.Bool
		err := c.Subscriber("missing").Receive(context.Background(), func(context.Context, *pubsub.Message) { called.Store(true) })
		var apiErr *pubsub.APIError
		if !errors.As(err, &apiErr) || apiErr.Code != 404 || apiErr.Status != "NOT_FOUND" {
			t.Fatalf("err = %v", err)
		}
		if called.Load() {
			t.Fatal("callback called")
		}
	})

	t.Run("permission denied mid-stream", func(t *testing.T) {
		srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
		srv.Publish("t", pubsubtest.Message{Data: []byte("x")})
		got := make(chan struct{}, 1)
		r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) {
			m.Ack()
			got <- struct{}{}
		})
		recv(t, got)
		srv.InjectFaults(pubsubtest.Fault{Verb: "pull", Status: 403, State: "PERMISSION_DENIED"})
		err := r.wait(t)
		var apiErr *pubsub.APIError
		if !errors.As(err, &apiErr) || apiErr.Code != 403 {
			t.Fatalf("err = %v", err)
		}
		if len(srv.Acked("s")) != 1 {
			t.Fatal("ack lost when Receive failed")
		}
	})
}

func TestReceiveRetriesTransientPullErrors(t *testing.T) {
	srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
	for range 8 {
		srv.InjectFaults(pubsubtest.Fault{Verb: "pull", Status: 503, State: "UNAVAILABLE"})
	}
	srv.InjectFaults(pubsubtest.Fault{Verb: "pull", Drop: true})
	srv.Publish("t", pubsubtest.Message{Data: []byte("x")})

	got := make(chan string, 1)
	r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) {
		m.Ack()
		got <- string(m.Data)
	})
	if d := recv(t, got); d != "x" {
		t.Fatalf("got %q", d)
	}
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if srv.Calls("pull") < 10 {
		t.Fatalf("pull calls = %d", srv.Calls("pull"))
	}
}

func TestReceiveMaxOutstandingMessages(t *testing.T) {
	srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
	sub.ReceiveSettings.MaxOutstandingMessages = 2
	for i := range 10 {
		srv.Publish("t", pubsubtest.Message{Data: fmt.Appendf(nil, "%d", i)})
	}

	var active, peak, done atomic.Int32
	finished := make(chan struct{})
	r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) {
		n := active.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		active.Add(-1)
		m.Ack()
		if done.Add(1) == 10 {
			close(finished)
		}
	})
	recv(t, finished)
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if p := peak.Load(); p > 2 {
		t.Fatalf("%d callbacks ran concurrently, MaxOutstandingMessages is 2", p)
	}
	if len(srv.Acked("s")) != 10 {
		t.Fatalf("acked %d", len(srv.Acked("s")))
	}
}

func TestReceiveUnlimitedOutstanding(t *testing.T) {
	srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
	sub.ReceiveSettings.MaxOutstandingMessages = -1
	sub.ReceiveSettings.MaxOutstandingBytes = -1
	const total = 1500
	msgs := make([]pubsubtest.Message, total)
	for i := range msgs {
		msgs[i] = pubsubtest.Message{Data: []byte("x")}
	}
	srv.Publish("t", msgs...)

	var started atomic.Int32
	all := make(chan struct{})
	r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) {
		if started.Add(1) == total {
			close(all)
		}
		select {
		case <-all:
			m.Ack()
		case <-ctx.Done():
		}
	})
	recv(t, all)
	waitFor(t, "all acks", func() bool { return len(srv.Acked("s")) == total })
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
}

func TestReceiveCallbackContextCanceled(t *testing.T) {
	srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
	srv.Publish("t", pubsubtest.Message{Data: []byte("x")})

	started := make(chan struct{})
	r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) {
		close(started)
		<-ctx.Done()
		m.Ack()
	})
	recv(t, started)
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if len(srv.Acked("s")) != 1 {
		t.Fatal("ack from canceled callback was not flushed")
	}
}

func TestReceiveShutdown(t *testing.T) {
	block := func(started chan struct{}, release chan struct{}) func(context.Context, *pubsub.Message) {
		return func(ctx context.Context, m *pubsub.Message) {
			close(started)
			<-release
			m.Ack()
		}
	}

	t.Run("default waits for callbacks", func(t *testing.T) {
		t.Parallel()
		srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
		srv.Publish("t", pubsubtest.Message{Data: []byte("x")})
		started := make(chan struct{})
		r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) {
			close(started)
			<-ctx.Done()
			time.Sleep(200 * time.Millisecond)
			m.Ack()
		})
		recv(t, started)
		start := time.Now()
		if err := r.stop(t); err != nil {
			t.Fatal(err)
		}
		if el := time.Since(start); el < 200*time.Millisecond {
			t.Fatalf("Receive returned after %s, before the callback finished", el)
		}
		if len(srv.Acked("s")) != 1 {
			t.Fatal("ack lost")
		}
	})

	t.Run("timeout zero returns immediately", func(t *testing.T) {
		t.Parallel()
		srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
		sub.ReceiveSettings.ShutdownOptions = &pubsub.ShutdownOptions{Timeout: 0}
		srv.Publish("t", pubsubtest.Message{Data: []byte("x")})
		started, release := make(chan struct{}), make(chan struct{})
		r := startReceive(t, sub, block(started, release))
		recv(t, started)
		start := time.Now()
		if err := r.stop(t); err != nil {
			t.Fatal(err)
		}
		if el := time.Since(start); el > 500*time.Millisecond {
			t.Fatalf("Receive took %s to return", el)
		}
		close(release)
		time.Sleep(150 * time.Millisecond)
		if len(srv.Acked("s")) != 0 {
			t.Fatal("ack after Receive returned must be ignored")
		}
	})

	t.Run("nack immediately", func(t *testing.T) {
		t.Parallel()
		srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
		sub.ReceiveSettings.ShutdownOptions = &pubsub.ShutdownOptions{Timeout: 5 * time.Second, Behavior: pubsub.ShutdownBehaviorNackImmediately}
		srv.Publish("t", pubsubtest.Message{Data: []byte("x")})
		started, release := make(chan struct{}), make(chan struct{})
		defer close(release)
		r := startReceive(t, sub, block(started, release))
		recv(t, started)
		if srv.Available("s") != 0 {
			t.Fatal("message should be leased while processing")
		}
		start := time.Now()
		if err := r.stop(t); err != nil {
			t.Fatal(err)
		}
		if el := time.Since(start); el > time.Second {
			t.Fatalf("Receive took %s to return", el)
		}
		if srv.Available("s") != 1 {
			t.Fatal("outstanding message was not nacked on shutdown")
		}
	})

	t.Run("wait with timeout", func(t *testing.T) {
		t.Parallel()
		srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
		sub.ReceiveSettings.ShutdownOptions = &pubsub.ShutdownOptions{Timeout: 150 * time.Millisecond}
		srv.Publish("t", pubsubtest.Message{Data: []byte("x")})
		started, release := make(chan struct{}), make(chan struct{})
		defer close(release)
		r := startReceive(t, sub, block(started, release))
		recv(t, started)
		start := time.Now()
		if err := r.stop(t); err != nil {
			t.Fatal(err)
		}
		if el := time.Since(start); el < 150*time.Millisecond || el > time.Second {
			t.Fatalf("Receive returned after %s, want ~150ms", el)
		}
	})
}

func TestReceiveOrdering(t *testing.T) {
	srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
	keys := []string{"a", "b", "c", ""}
	const total = 80
	var msgs []pubsubtest.Message
	for i := range total {
		msgs = append(msgs, pubsubtest.Message{Data: fmt.Appendf(nil, "%d", i), OrderingKey: keys[i%len(keys)]})
	}
	srv.Publish("t", msgs...)

	var (
		mu       sync.Mutex
		busy     = map[string]bool{}
		last     = map[string]int{}
		active   int
		peak     int
		count    int
		finished = make(chan struct{})
	)
	r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) {
		var seq int
		fmt.Sscan(string(m.Data), &seq)
		mu.Lock()
		if m.OrderingKey != "" {
			if busy[m.OrderingKey] {
				t.Errorf("key %q: concurrent callbacks", m.OrderingKey)
			}
			if prev, ok := last[m.OrderingKey]; ok && seq < prev {
				t.Errorf("key %q: %d delivered after %d", m.OrderingKey, seq, prev)
			}
			busy[m.OrderingKey] = true
			last[m.OrderingKey] = seq
		}
		active++
		peak = max(peak, active)
		mu.Unlock()

		time.Sleep(2 * time.Millisecond)

		mu.Lock()
		busy[m.OrderingKey] = false
		active--
		count++
		if count == total {
			close(finished)
		}
		mu.Unlock()
		m.Ack()
	})
	recv(t, finished)
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if peak < 4 {
		t.Fatalf("peak concurrency %d: different keys should be processed in parallel", peak)
	}
	if len(srv.Acked("s")) != total {
		t.Fatalf("acked %d", len(srv.Acked("s")))
	}
}

func TestReceiveOrderedQueueNackedOnShutdown(t *testing.T) {
	srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
	var msgs []pubsubtest.Message
	for i := range 20 {
		msgs = append(msgs, pubsubtest.Message{Data: fmt.Appendf(nil, "%d", i), OrderingKey: "k"})
	}
	srv.Publish("t", msgs...)

	var calls atomic.Int32
	started := make(chan struct{})
	r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-ctx.Done()
		time.Sleep(100 * time.Millisecond)
		m.Ack()
	})
	recv(t, started)
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("callback ran %d times after shutdown began", n)
	}
	if len(srv.Acked("s")) != 1 || srv.Available("s") != 19 {
		t.Fatalf("acked=%d available=%d, queued ordered messages must be nacked", len(srv.Acked("s")), srv.Available("s"))
	}
}

func TestReceiveLeaseExtension(t *testing.T) {
	t.Parallel()
	srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{AckDeadline: time.Second, DeadLetter: true})
	pubsub.SetLeaseForTest(sub, time.Second)
	srv.Publish("t", pubsubtest.Message{Data: []byte("slow")})

	var calls atomic.Int32
	done := make(chan struct{})
	r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) {
		if calls.Add(1) > 1 {
			t.Errorf("redelivered (attempt %d) while being processed", *m.DeliveryAttempt)
			return
		}
		time.Sleep(2500 * time.Millisecond)
		m.Ack()
		close(done)
	})
	recv(t, done)
	time.Sleep(200 * time.Millisecond)
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if len(srv.Acked("s")) != 1 || calls.Load() != 1 {
		t.Fatalf("acked=%d calls=%d", len(srv.Acked("s")), calls.Load())
	}
	if srv.Calls("modifyAckDeadline") < 3 {
		t.Fatalf("modifyAckDeadline calls = %d", srv.Calls("modifyAckDeadline"))
	}
}

func TestReceiveMaxExtension(t *testing.T) {
	t.Parallel()
	srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{AckDeadline: time.Second, DeadLetter: true})
	pubsub.SetLeaseForTest(sub, time.Second)
	sub.ReceiveSettings.MaxExtension = 300 * time.Millisecond
	sub.ReceiveSettings.MaxOutstandingMessages = 1
	srv.Publish("t", pubsubtest.Message{Data: []byte("stuck")})

	release := make(chan struct{})
	redelivered := make(chan struct{})
	r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) {
		if *m.DeliveryAttempt == 1 {
			<-release
			m.Ack()
			return
		}
		m.Ack()
		close(redelivered)
	})
	recv(t, redelivered)
	close(release)
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if len(srv.Acked("s")) != 1 || srv.Unacked("s") != 0 {
		t.Fatalf("acked=%d unacked=%d", len(srv.Acked("s")), srv.Unacked("s"))
	}
}

func TestReceiveAcksAreBatched(t *testing.T) {
	srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
	const total = 300
	msgs := make([]pubsubtest.Message, total)
	for i := range msgs {
		msgs[i] = pubsubtest.Message{Data: []byte("x")}
	}
	srv.Publish("t", msgs...)

	r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) { m.Ack() })
	waitFor(t, "all acks", func() bool { return len(srv.Acked("s")) == total })
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if n := srv.Calls("acknowledge"); n > total/10 {
		t.Fatalf("%d acknowledge calls for %d messages", n, total)
	}
}

func TestReceiveDoubleAckIgnored(t *testing.T) {
	srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
	srv.Publish("t", pubsubtest.Message{Data: []byte("x")})

	var calls atomic.Int32
	r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) {
		calls.Add(1)
		m.Ack()
		m.Nack()
		m.Ack()
	})
	waitFor(t, "ack", func() bool { return len(srv.Acked("s")) == 1 })
	time.Sleep(200 * time.Millisecond)
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || srv.Unacked("s") != 0 {
		t.Fatalf("calls=%d unacked=%d", calls.Load(), srv.Unacked("s"))
	}

	m := &pubsub.Message{Data: []byte("local")}
	m.Ack()
	m.Nack()
}

func TestReceiveNumGoroutines(t *testing.T) {
	srv, sub := newSubscription(t, pubsubtest.SubscriptionConfig{})
	srv.SetPullWait(time.Second)
	sub.ReceiveSettings.NumGoroutines = 4

	r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) { m.Ack() })
	waitFor(t, "4 pulls", func() bool { return srv.Calls("pull") >= 4 })
	time.Sleep(100 * time.Millisecond)
	if n := srv.Calls("pull"); n != 4 {
		t.Fatalf("pull calls = %d, want 4 concurrent long polls", n)
	}
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}
}
