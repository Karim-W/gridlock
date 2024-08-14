package subscriber

import "time"

func (s *SubscriberImpl) SetPullFrequency(frequency time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ticker.Stop()

	s.ticker = time.NewTicker(frequency)
}
