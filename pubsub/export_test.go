package pubsub

import "time"

func SetLeaseForTest(s *Subscriber, d time.Duration) {
	s.lease = d
}
