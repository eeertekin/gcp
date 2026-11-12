package pubsub_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/eeertekin/gcp/pubsub"
	"github.com/eeertekin/gcp/pubsub/pubsubtest"
)

func TestPublishSubscribe(t *testing.T) {
	srv, c := pubsubtest.NewServer(t)
	srv.CreateTopic("events")
	srv.CreateSubscription("workers", "events", pubsubtest.SubscriptionConfig{})

	p := c.Publisher("events")
	p.EnableMessageOrdering = true
	ctx := context.Background()

	const total = 600
	keys := []string{"", "", "u1", "u2", "u3"}
	var results []*pubsub.PublishResult
	for i := range total {
		results = append(results, p.Publish(ctx, &pubsub.Message{
			Data:        fmt.Appendf(nil, "%d", i),
			Attributes:  map[string]string{"seq": fmt.Sprint(i)},
			OrderingKey: keys[i%len(keys)],
		}))
	}
	p.Stop()
	for _, r := range results {
		if _, err := r.Get(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if len(srv.Batches("events")) >= total/2 {
		t.Fatalf("%d publish requests for %d messages, expected batching", len(srv.Batches("events")), total)
	}

	sub := c.Subscriber("workers")
	sub.ReceiveSettings.MaxOutstandingMessages = 50

	var (
		mu       sync.Mutex
		seen     = map[string]int{}
		last     = map[string]int{}
		finished = make(chan struct{})
	)
	r := startReceive(t, sub, func(ctx context.Context, m *pubsub.Message) {
		var seq int
		fmt.Sscan(string(m.Data), &seq)
		mu.Lock()
		defer mu.Unlock()
		if m.Attributes["seq"] != string(m.Data) {
			t.Errorf("attributes %v for data %s", m.Attributes, m.Data)
		}
		if m.OrderingKey != "" {
			if prev, ok := last[m.OrderingKey]; ok && seq < prev {
				t.Errorf("key %q: %d after %d", m.OrderingKey, seq, prev)
			}
			last[m.OrderingKey] = seq
		}
		seen[string(m.Data)]++
		m.Ack()
		if len(seen) == total && seen[string(m.Data)] == 1 {
			close(finished)
		}
	})
	recv(t, finished)
	if err := r.stop(t); err != nil {
		t.Fatal(err)
	}

	for i := range total {
		if n := seen[fmt.Sprint(i)]; n != 1 {
			t.Fatalf("message %d received %d times", i, n)
		}
	}
	if len(srv.Acked("workers")) != total || srv.Unacked("workers") != 0 {
		t.Fatalf("acked=%d unacked=%d", len(srv.Acked("workers")), srv.Unacked("workers"))
	}
}
